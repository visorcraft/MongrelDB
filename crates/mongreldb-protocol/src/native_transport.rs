//! Native gRPC transport over multiplexed HTTP/2 with certificate validation.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tonic::transport::{
    Certificate, Channel, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig,
};

use crate::native;

#[derive(Debug, Clone)]
pub struct NativeRpcServerConfig {
    pub address: SocketAddr,
    pub certificate_pem: Vec<u8>,
    pub private_key_pem: Vec<u8>,
    pub client_ca_pem: Option<Vec<u8>>,
    pub max_concurrent_streams: u32,
    pub max_in_flight_per_connection: usize,
    pub request_timeout: Duration,
    pub keepalive_interval: Duration,
    pub keepalive_timeout: Duration,
}

impl NativeRpcServerConfig {
    fn tls(&self) -> ServerTlsConfig {
        let mut tls = ServerTlsConfig::new().identity(Identity::from_pem(
            self.certificate_pem.clone(),
            self.private_key_pem.clone(),
        ));
        if let Some(client_ca) = &self.client_ca_pem {
            tls = tls.client_ca_root(Certificate::from_pem(client_ca.clone()));
        }
        tls
    }
}

pub struct NativeRpcServices<A, S, Q, T, C, D, H> {
    pub auth: A,
    pub session: S,
    pub query: Q,
    pub transaction: T,
    pub catalog: C,
    pub admin: D,
    pub health: H,
}

/// Real native listener. All seven adapters are supplied from one canonical
/// runtime so HTTP/Kit/native transports can share behavior.
pub struct NativeRpcServer {
    config: NativeRpcServerConfig,
}

impl NativeRpcServer {
    pub fn new(config: NativeRpcServerConfig) -> Self {
        Self { config }
    }

    pub async fn serve_with_shutdown<A, S, Q, T, C, D, H, F>(
        self,
        services: NativeRpcServices<A, S, Q, T, C, D, H>,
        shutdown: F,
    ) -> Result<(), tonic::transport::Error>
    where
        A: native::auth_service_server::AuthService,
        S: native::session_service_server::SessionService,
        Q: native::query_service_server::QueryService,
        T: native::transaction_service_server::TransactionService,
        C: native::catalog_service_server::CatalogService,
        D: native::admin_service_server::AdminService,
        H: native::health_service_server::HealthService,
        F: Future<Output = ()>,
    {
        Server::builder()
            .tls_config(self.config.tls())?
            .max_concurrent_streams(Some(self.config.max_concurrent_streams.max(1)))
            .concurrency_limit_per_connection(self.config.max_in_flight_per_connection.max(1))
            .timeout(self.config.request_timeout)
            .http2_keepalive_interval(Some(self.config.keepalive_interval))
            .http2_keepalive_timeout(Some(self.config.keepalive_timeout))
            .add_service(native::auth_service_server::AuthServiceServer::new(
                services.auth,
            ))
            .add_service(native::session_service_server::SessionServiceServer::new(
                services.session,
            ))
            .add_service(native::query_service_server::QueryServiceServer::new(
                services.query,
            ))
            .add_service(
                native::transaction_service_server::TransactionServiceServer::new(
                    services.transaction,
                ),
            )
            .add_service(native::catalog_service_server::CatalogServiceServer::new(
                services.catalog,
            ))
            .add_service(native::admin_service_server::AdminServiceServer::new(
                services.admin,
            ))
            .add_service(native::health_service_server::HealthServiceServer::new(
                services.health,
            ))
            .serve_with_shutdown(self.config.address, shutdown)
            .await
    }
}

#[derive(Clone)]
pub struct NativeRpcClientConfig {
    pub endpoint: String,
    pub domain_name: String,
    pub ca_certificate_pem: Vec<u8>,
    pub client_identity_pem: Option<(Vec<u8>, Vec<u8>)>,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub max_in_flight: usize,
    pub tcp_keepalive: Duration,
    pub http2_keepalive_interval: Duration,
}

#[derive(Clone)]
pub struct NativeRpcConnection {
    channel: Channel,
}

impl NativeRpcConnection {
    pub async fn connect(config: &NativeRpcClientConfig) -> Result<Self, tonic::transport::Error> {
        let mut tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(config.ca_certificate_pem.clone()))
            .domain_name(config.domain_name.clone());
        if let Some((certificate, key)) = &config.client_identity_pem {
            tls = tls.identity(Identity::from_pem(certificate.clone(), key.clone()));
        }
        let channel = Endpoint::from_shared(config.endpoint.clone())?
            .tls_config(tls)?
            .connect_timeout(config.connect_timeout)
            .timeout(config.request_timeout)
            .concurrency_limit(config.max_in_flight.max(1))
            .tcp_keepalive(Some(config.tcp_keepalive))
            .http2_keep_alive_interval(config.http2_keepalive_interval)
            .keep_alive_while_idle(false)
            .connect()
            .await?;
        Ok(Self { channel })
    }

    pub fn client(&self) -> NativeRpcClient {
        NativeRpcClient::new(self.channel.clone())
    }
}

/// Seven generated clients sharing one multiplexed HTTP/2 channel.
#[derive(Clone)]
pub struct NativeRpcClient {
    channel: Channel,
}

impl NativeRpcClient {
    pub fn new(channel: Channel) -> Self {
        Self { channel }
    }

    pub fn auth(&self) -> native::auth_service_client::AuthServiceClient<Channel> {
        native::auth_service_client::AuthServiceClient::new(self.channel.clone())
    }

    pub fn session(&self) -> native::session_service_client::SessionServiceClient<Channel> {
        native::session_service_client::SessionServiceClient::new(self.channel.clone())
    }

    pub fn query(&self) -> native::query_service_client::QueryServiceClient<Channel> {
        native::query_service_client::QueryServiceClient::new(self.channel.clone())
    }

    pub fn transaction(
        &self,
    ) -> native::transaction_service_client::TransactionServiceClient<Channel> {
        native::transaction_service_client::TransactionServiceClient::new(self.channel.clone())
    }

    pub fn catalog(&self) -> native::catalog_service_client::CatalogServiceClient<Channel> {
        native::catalog_service_client::CatalogServiceClient::new(self.channel.clone())
    }

    pub fn admin(&self) -> native::admin_service_client::AdminServiceClient<Channel> {
        native::admin_service_client::AdminServiceClient::new(self.channel.clone())
    }

    pub fn health(&self) -> native::health_service_client::HealthServiceClient<Channel> {
        native::health_service_client::HealthServiceClient::new(self.channel.clone())
    }
}

/// Small round-robin connection pool. Each connection still multiplexes many
/// concurrent streams.
pub struct NativeRpcClientPool {
    clients: Vec<NativeRpcClient>,
    next: AtomicUsize,
}

impl NativeRpcClientPool {
    pub async fn connect(
        config: NativeRpcClientConfig,
        connections: usize,
    ) -> Result<Arc<Self>, tonic::transport::Error> {
        let mut clients = Vec::with_capacity(connections.max(1));
        for _ in 0..connections.max(1) {
            clients.push(NativeRpcConnection::connect(&config).await?.client());
        }
        Ok(Arc::new(Self {
            clients,
            next: AtomicUsize::new(0),
        }))
    }

    pub fn client(&self) -> NativeRpcClient {
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        self.clients[index].clone()
    }
}
