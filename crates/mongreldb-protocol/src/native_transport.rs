//! Native gRPC transport over multiplexed HTTP/2 with certificate validation.

use std::future::Future;
use std::io::{BufReader, Error, ErrorKind};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::OwnedSemaphorePermit;
use tokio_rustls::rustls::pki_types::CertificateDer;
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{RootCertStore, ServerConfig};
use tokio_rustls::TlsAcceptor;
use tonic::transport::{
    server::Connected, Certificate, Channel, ClientTlsConfig, Endpoint, Identity, Server,
};

use crate::native;

#[derive(Debug, thiserror::Error)]
pub enum NativeRpcTransportError {
    #[error("native RPC I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("native RPC transport error: {0}")]
    Tonic(#[from] tonic::transport::Error),
}

#[derive(Debug, Clone)]
pub struct NativeRpcServerConfig {
    pub address: SocketAddr,
    pub certificate_pem: Vec<u8>,
    pub private_key_pem: Vec<u8>,
    pub client_ca_pem: Option<Vec<u8>>,
    pub max_connections: usize,
    pub max_concurrent_streams: u32,
    pub max_in_flight_per_connection: usize,
    pub request_timeout: Duration,
    pub idle_timeout: Duration,
    pub keepalive_interval: Duration,
    pub keepalive_timeout: Duration,
}

impl NativeRpcServerConfig {
    fn tls13_acceptor(&self) -> Result<TlsAcceptor, Error> {
        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
        let certificates =
            rustls_pemfile::certs(&mut BufReader::new(self.certificate_pem.as_slice()))
                .collect::<Result<Vec<CertificateDer<'static>>, _>>()?;
        let private_key =
            rustls_pemfile::private_key(&mut BufReader::new(self.private_key_pem.as_slice()))?
                .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "TLS private key is missing"))?;
        let builder =
            ServerConfig::builder_with_protocol_versions(&[&tokio_rustls::rustls::version::TLS13]);
        let builder = match &self.client_ca_pem {
            None => builder.with_no_client_auth(),
            Some(client_ca) => {
                let mut roots = RootCertStore::empty();
                roots.add_parsable_certificates(
                    rustls_pemfile::certs(&mut BufReader::new(client_ca.as_slice()))
                        .collect::<Result<Vec<_>, _>>()?,
                );
                let verifier = WebPkiClientVerifier::builder(roots.into())
                    .build()
                    .map_err(|error| Error::new(ErrorKind::InvalidInput, error))?;
                builder.with_client_cert_verifier(verifier)
            }
        };
        let mut config = builder
            .with_single_cert(certificates, private_key)
            .map_err(|error| Error::new(ErrorKind::InvalidInput, error))?;
        config.alpn_protocols = vec![b"h2".to_vec()];
        Ok(TlsAcceptor::from(Arc::new(config)))
    }
}

struct Tls13Connection {
    stream: tokio_rustls::server::TlsStream<TcpStream>,
    peer: SocketAddr,
    idle_timeout: Duration,
    idle: Pin<Box<tokio::time::Sleep>>,
    _permit: OwnedSemaphorePermit,
}

impl Tls13Connection {
    fn poll_idle(&mut self, cx: &mut Context<'_>) -> std::io::Result<()> {
        if self.idle.as_mut().poll(cx).is_ready() {
            Err(Error::new(
                ErrorKind::TimedOut,
                "native RPC connection idle timeout",
            ))
        } else {
            Ok(())
        }
    }

    fn reset_idle(&mut self) {
        self.idle
            .as_mut()
            .reset(tokio::time::Instant::now() + self.idle_timeout);
    }
}

impl Connected for Tls13Connection {
    type ConnectInfo = SocketAddr;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.peer
    }
}

impl AsyncRead for Tls13Connection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if let Err(error) = self.poll_idle(cx) {
            return Poll::Ready(Err(error));
        }
        let before = buffer.filled().len();
        let result = Pin::new(&mut self.stream).poll_read(cx, buffer);
        if matches!(&result, Poll::Ready(Ok(()))) && buffer.filled().len() > before {
            self.reset_idle();
        }
        result
    }
}

impl AsyncWrite for Tls13Connection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if let Err(error) = self.poll_idle(cx) {
            return Poll::Ready(Err(error));
        }
        let result = Pin::new(&mut self.stream).poll_write(cx, buffer);
        if matches!(&result, Poll::Ready(Ok(written)) if *written > 0) {
            self.reset_idle();
        }
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
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
    ) -> Result<(), NativeRpcTransportError>
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
        let listener = tokio::net::TcpListener::bind(self.config.address).await?;
        let acceptor = self.config.tls13_acceptor()?;
        let idle_timeout = self.config.idle_timeout;
        let permits = Arc::new(tokio::sync::Semaphore::new(
            self.config.max_connections.max(1),
        ));
        let handshake_timeout = self.config.keepalive_timeout;
        let (connections, incoming) =
            tokio::sync::mpsc::channel(self.config.max_connections.max(1));
        let accept_task = tokio::spawn(async move {
            loop {
                let (stream, peer) = listener.accept().await?;
                let permit = match Arc::clone(&permits).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => continue,
                };
                let acceptor = acceptor.clone();
                let connections = connections.clone();
                tokio::spawn(async move {
                    let accepted = tokio::time::timeout(handshake_timeout, acceptor.accept(stream))
                        .await
                        .map_err(|_| Error::new(ErrorKind::TimedOut, "TLS handshake timed out"))
                        .and_then(|result| result.map_err(Error::other))
                        .map(|stream| Tls13Connection {
                            stream,
                            peer,
                            idle_timeout,
                            idle: Box::pin(tokio::time::sleep(idle_timeout)),
                            _permit: permit,
                        });
                    let _ = connections.send(accepted).await;
                });
            }
            #[allow(unreachable_code)]
            Ok::<(), Error>(())
        });
        let result = Server::builder()
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
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::ReceiverStream::new(incoming),
                shutdown,
            )
            .await;
        accept_task.abort();
        result.map_err(Into::into)
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
