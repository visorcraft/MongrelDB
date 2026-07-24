#![cfg(feature = "native-rpc")]
use std::sync::Arc;
use std::time::Duration;

use arrow::array::Int64Array;
use mongreldb_client::native::NativeClient;
use mongreldb_client::AsyncMongrelClient;
use mongreldb_core::{ColumnDef, ColumnFlags, Database, Schema, TypeId};
use mongreldb_protocol::native_transport::{
    NativeRpcClientConfig, NativeRpcServer, NativeRpcServerConfig, NativeRpcServices,
};
use mongreldb_server::native::NativeExternalAuth;
use mongreldb_server::{build_app_with_sessions_and_control, SessionStore};
use tempfile::tempdir;

#[tokio::test]
async fn packaged_http_and_native_clients_have_query_and_write_parity() {
    let directory = tempdir().unwrap();
    let database = Arc::new(Database::create(directory.path()).unwrap());
    let sessions = Arc::new(SessionStore::new(8, Duration::from_secs(60)));
    let (app, control) = build_app_with_sessions_and_control(
        Arc::clone(&database),
        std::iter::empty(),
        None,
        None,
        false,
        Arc::clone(&sessions),
    );

    let http_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_address = http_listener.local_addr().unwrap();
    let (http_shutdown_tx, http_shutdown_rx) = tokio::sync::oneshot::channel();
    let http_server = tokio::spawn(async move {
        axum::serve(http_listener, app)
            .with_graceful_shutdown(async move {
                let _ = http_shutdown_rx.await;
            })
            .await
    });

    let runtime = control
        .native_runtime(Arc::clone(&database), Arc::clone(&sessions))
        .with_external_auth(NativeExternalAuth::new());
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let certificate_pem = certified.cert.pem().into_bytes();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let native_address = socket.local_addr().unwrap();
    drop(socket);
    let (native_shutdown_tx, native_shutdown_rx) = tokio::sync::oneshot::channel();
    let native_server = tokio::spawn(
        NativeRpcServer::new(NativeRpcServerConfig {
            address: native_address,
            certificate_pem: certificate_pem.clone(),
            private_key_pem: certified.key_pair.serialize_pem().into_bytes(),
            client_ca_pem: None,
            max_connections: 16,
            max_concurrent_streams: 16,
            max_in_flight_per_connection: 16,
            request_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(30),
            keepalive_interval: Duration::from_secs(30),
            keepalive_timeout: Duration::from_secs(5),
        })
        .serve_with_shutdown(
            NativeRpcServices {
                auth: runtime.clone(),
                session: runtime.clone(),
                query: runtime.clone(),
                transaction: runtime.clone(),
                catalog: runtime.clone(),
                admin: runtime.clone(),
                health: runtime,
            },
            async move {
                let _ = native_shutdown_rx.await;
            },
        ),
    );

    let native_config = NativeRpcClientConfig {
        endpoint: format!("https://127.0.0.1:{}", native_address.port()),
        domain_name: "localhost".into(),
        ca_certificate_pem: certificate_pem,
        client_identity_pem: None,
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(30),
        max_in_flight: 16,
        tcp_keepalive: Duration::from_secs(30),
        http2_keepalive_interval: Duration::from_secs(30),
    };
    let mut native = None;
    for _ in 0..100 {
        match NativeClient::connect(native_config.clone(), 2, *sessions.database_id().as_bytes())
            .await
        {
            Ok(client) => {
                native = Some(client.authenticate_anonymous().await.unwrap());
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
    let native = native.expect("native listener did not start");
    let http =
        AsyncMongrelClient::new(&format!("http://127.0.0.1:{}", http_address.port())).unwrap();

    native
        .create_table(
            "adapter_items",
            &Schema {
                columns: vec![ColumnDef {
                    id: 1,
                    name: "id".into(),
                    ty: TypeId::Int64,
                    flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                    default_value: None,
                    embedding_source: None,
                }],
                ..Schema::default()
            },
        )
        .await
        .unwrap();
    http.sql_write_idempotent("INSERT INTO adapter_items VALUES (1)", "adapter-http-write")
        .await
        .unwrap();
    native
        .execute(
            "INSERT INTO adapter_items VALUES (2)",
            Some("adapter-native-write"),
        )
        .await
        .unwrap();

    let sql = "SELECT id FROM adapter_items ORDER BY id";
    let http_batches = http.sql(sql).await.unwrap();
    let native_batches = native.execute(sql, None).await.unwrap().batches;
    assert_eq!(int64_values(&http_batches), vec![1, 2]);
    assert_eq!(int64_values(&native_batches), int64_values(&http_batches));

    assert!(http
        .sql("SELECT * FROM missing_adapter_table")
        .await
        .is_err());
    assert!(native
        .execute("SELECT * FROM missing_adapter_table", None)
        .await
        .is_err());

    native.close().await.unwrap();
    http_shutdown_tx.send(()).unwrap();
    native_shutdown_tx.send(()).unwrap();
    http_server.await.unwrap().unwrap();
    native_server.await.unwrap().unwrap();
}

fn int64_values(batches: &[arrow::record_batch::RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect()
}
