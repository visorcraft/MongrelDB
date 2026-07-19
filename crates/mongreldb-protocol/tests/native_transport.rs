use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use mongreldb_protocol::native;
use mongreldb_protocol::native_transport::{
    NativeRpcClientConfig, NativeRpcConnection, NativeRpcServer, NativeRpcServerConfig,
    NativeRpcServices,
};
use mongreldb_protocol::{validate_native_context, NATIVE_API_MAJOR};
use tonic::{Request, Response, Status};

#[derive(Clone, Copy)]
struct TestServices;

fn unimplemented<T>() -> Result<Response<T>, Status> {
    Err(Status::unimplemented("test service"))
}

#[tonic::async_trait]
impl native::auth_service_server::AuthService for TestServices {
    async fn authenticate(
        &self,
        _: Request<native::AuthenticateRequest>,
    ) -> Result<Response<native::AuthenticateResponse>, Status> {
        unimplemented()
    }

    async fn begin_scram(
        &self,
        _: Request<native::BeginScramRequest>,
    ) -> Result<Response<native::BeginScramResponse>, Status> {
        unimplemented()
    }

    async fn finish_scram(
        &self,
        _: Request<native::FinishScramRequest>,
    ) -> Result<Response<native::FinishScramResponse>, Status> {
        unimplemented()
    }
}

#[tonic::async_trait]
impl native::session_service_server::SessionService for TestServices {
    async fn open_session(
        &self,
        _: Request<native::OpenSessionRequest>,
    ) -> Result<Response<native::OpenSessionResponse>, Status> {
        unimplemented()
    }

    async fn close_session(
        &self,
        _: Request<native::CloseSessionRequest>,
    ) -> Result<Response<native::Empty>, Status> {
        unimplemented()
    }
}

#[tonic::async_trait]
impl native::query_service_server::QueryService for TestServices {
    type ExecuteStreamStream = Pin<
        Box<
            dyn tonic::codegen::tokio_stream::Stream<Item = Result<native::ArrowFrame, Status>>
                + Send,
        >,
    >;

    async fn prepare(
        &self,
        _: Request<native::PrepareRequest>,
    ) -> Result<Response<native::PrepareResponse>, Status> {
        unimplemented()
    }

    async fn execute(
        &self,
        _: Request<native::ExecuteRequest>,
    ) -> Result<Response<native::ExecuteResponse>, Status> {
        unimplemented()
    }

    async fn execute_stream(
        &self,
        _: Request<native::ExecuteRequest>,
    ) -> Result<Response<Self::ExecuteStreamStream>, Status> {
        unimplemented()
    }

    async fn cancel_query(
        &self,
        _: Request<native::CancelQueryRequest>,
    ) -> Result<Response<native::Empty>, Status> {
        unimplemented()
    }

    async fn get_query_status(
        &self,
        _: Request<native::GetQueryStatusRequest>,
    ) -> Result<Response<native::QueryStatusResponse>, Status> {
        unimplemented()
    }
}

#[tonic::async_trait]
impl native::transaction_service_server::TransactionService for TestServices {
    async fn begin(
        &self,
        _: Request<native::BeginTransactionRequest>,
    ) -> Result<Response<native::BeginTransactionResponse>, Status> {
        unimplemented()
    }

    async fn commit(
        &self,
        _: Request<native::TransactionRequest>,
    ) -> Result<Response<native::Empty>, Status> {
        unimplemented()
    }

    async fn rollback(
        &self,
        _: Request<native::TransactionRequest>,
    ) -> Result<Response<native::Empty>, Status> {
        unimplemented()
    }
}

#[tonic::async_trait]
impl native::catalog_service_server::CatalogService for TestServices {
    async fn get_schema(
        &self,
        _: Request<native::GetSchemaRequest>,
    ) -> Result<Response<native::GetSchemaResponse>, Status> {
        unimplemented()
    }

    async fn create_table(
        &self,
        _: Request<native::CreateTableRequest>,
    ) -> Result<Response<native::CreateTableResponse>, Status> {
        unimplemented()
    }
}

#[tonic::async_trait]
impl native::admin_service_server::AdminService for TestServices {
    async fn execute_admin(
        &self,
        _: Request<native::ExecuteAdminRequest>,
    ) -> Result<Response<native::Empty>, Status> {
        unimplemented()
    }
}

#[tonic::async_trait]
impl native::health_service_server::HealthService for TestServices {
    async fn status(
        &self,
        request: Request<native::HealthRequest>,
    ) -> Result<Response<native::HealthResponse>, Status> {
        validate_native_context(request.get_ref().context.as_ref())?;
        Ok(Response::new(native::HealthResponse {
            serving: true,
            detail: "ready".into(),
        }))
    }
}

fn client_config(
    port: u16,
    domain_name: &str,
    ca_certificate_pem: Vec<u8>,
) -> NativeRpcClientConfig {
    NativeRpcClientConfig {
        endpoint: format!("https://127.0.0.1:{port}"),
        domain_name: domain_name.into(),
        ca_certificate_pem,
        client_identity_pem: None,
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(2),
        max_in_flight: 16,
        tcp_keepalive: Duration::from_secs(30),
        http2_keepalive_interval: Duration::from_secs(10),
    }
}

#[tokio::test]
async fn real_tls_http2_listener_validates_ca_and_hostname() {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let certificate_pem = certified.cert.pem().into_bytes();
    let private_key_pem = certified.key_pair.serialize_pem().into_bytes();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap();
    drop(socket);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        NativeRpcServer::new(NativeRpcServerConfig {
            address,
            certificate_pem: certificate_pem.clone(),
            private_key_pem,
            client_ca_pem: None,
            max_connections: 1,
            max_concurrent_streams: 32,
            max_in_flight_per_connection: 32,
            request_timeout: Duration::from_secs(2),
            idle_timeout: Duration::from_millis(100),
            keepalive_interval: Duration::from_secs(10),
            keepalive_timeout: Duration::from_secs(2),
        })
        .serve_with_shutdown(
            NativeRpcServices {
                auth: TestServices,
                session: TestServices,
                query: TestServices,
                transaction: TestServices,
                catalog: TestServices,
                admin: TestServices,
                health: TestServices,
            },
            async move {
                let _ = shutdown_rx.await;
            },
        ),
    );

    let valid_config = client_config(address.port(), "localhost", certificate_pem);
    let mut connection = None;
    for _ in 0..50 {
        match NativeRpcConnection::connect(&valid_config).await {
            Ok(connected) => {
                connection = Some(connected);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
    let connection = connection.expect("native TLS listener did not start");
    let response = connection
        .client()
        .health()
        .status(native::HealthRequest {
            context: Some(native::RequestContext {
                version: Some(native::ApiVersion {
                    major: NATIVE_API_MAJOR,
                    minor: 0,
                }),
                request_id: "health-1".into(),
                deadline_unix_micros: 0,
                idempotency_key: String::new(),
            }),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(response.serving);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let replacement = NativeRpcConnection::connect(&valid_config).await.unwrap();
    assert!(
        replacement
            .client()
            .health()
            .status(native::HealthRequest {
                context: Some(native::RequestContext {
                    version: Some(native::ApiVersion {
                        major: NATIVE_API_MAJOR,
                        minor: 0,
                    }),
                    request_id: "health-after-idle".into(),
                    deadline_unix_micros: 0,
                    idempotency_key: String::new(),
                }),
            })
            .await
            .unwrap()
            .into_inner()
            .serving
    );

    assert!(NativeRpcConnection::connect(&client_config(
        address.port(),
        "wrong.example",
        certified.cert.pem().into_bytes(),
    ))
    .await
    .is_err());
    let wrong_ca = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    assert!(NativeRpcConnection::connect(&client_config(
        address.port(),
        "localhost",
        wrong_ca.cert.pem().into_bytes(),
    ))
    .await
    .is_err());

    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    roots.add_parsable_certificates(
        rustls_pemfile::certs(&mut std::io::BufReader::new(
            certified.cert.pem().as_bytes(),
        ))
        .collect::<Result<Vec<_>, _>>()
        .unwrap(),
    );
    let mut tls12 = tokio_rustls::rustls::ClientConfig::builder_with_protocol_versions(&[
        &tokio_rustls::rustls::version::TLS12,
    ])
    .with_root_certificates(roots)
    .with_no_client_auth();
    tls12.alpn_protocols = vec![b"h2".to_vec()];
    let tcp = tokio::net::TcpStream::connect(address).await.unwrap();
    let domain = tokio_rustls::rustls::pki_types::ServerName::try_from("localhost").unwrap();
    assert!(
        tokio_rustls::TlsConnector::from(Arc::new(tls12))
            .connect(domain, tcp)
            .await
            .is_err(),
        "TLS 1.2 must be rejected"
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap().unwrap();
}
