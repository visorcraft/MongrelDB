use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;

use mongreldb_client::native::NativeClient;
use mongreldb_core::Database;
use mongreldb_mysql_wire::{serve, MysqlWireConfig};
use mongreldb_protocol::native_transport::{
    NativeRpcClientConfig, NativeRpcServer, NativeRpcServerConfig, NativeRpcServices,
};
use mongreldb_server::{build_app_with_sessions_and_control, SessionStore};
use mysql_async::prelude::Queryable;
use mysql_async::{OptsBuilder, Pool, SslOpts};
use tempfile::tempdir;

#[tokio::test]
async fn external_mysql_driver_uses_tls_auth_query_prepare_and_transactions() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_user("wire-user", "wire-password").unwrap();
    db.set_user_admin("wire-user", true).unwrap();
    let db = Arc::new(db);
    let sessions = Arc::new(SessionStore::new(16, Duration::from_secs(60)));
    let (_, control) = build_app_with_sessions_and_control(
        Arc::clone(&db),
        std::iter::empty(),
        None,
        None,
        false,
        Arc::clone(&sessions),
    );
    let runtime = control.native_runtime(db, Arc::clone(&sessions));
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let certificate_pem = certificate.cert.pem().into_bytes();
    let private_key_pem = certificate.key_pair.serialize_pem().into_bytes();

    let native_socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let native_address = native_socket.local_addr().unwrap();
    drop(native_socket);
    let (native_shutdown_tx, native_shutdown_rx) = tokio::sync::oneshot::channel();
    let native_server = tokio::spawn(
        NativeRpcServer::new(NativeRpcServerConfig {
            address: native_address,
            certificate_pem: certificate_pem.clone(),
            private_key_pem: private_key_pem.clone(),
            client_ca_pem: None,
            max_connections: 16,
            max_concurrent_streams: 16,
            max_in_flight_per_connection: 16,
            request_timeout: Duration::from_secs(5),
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
        ca_certificate_pem: certificate_pem.clone(),
        client_identity_pem: None,
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(5),
        max_in_flight: 16,
        tcp_keepalive: Duration::from_secs(30),
        http2_keepalive_interval: Duration::from_secs(30),
    };
    let native_client = {
        let mut connected = None;
        for _ in 0..100 {
            if let Ok(client) =
                NativeClient::connect(native_config.clone(), 2, *sessions.database_id().as_bytes())
                    .await
            {
                connected = Some(client);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        connected.expect("native listener did not start")
    };

    let wire_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let wire_address = wire_listener.local_addr().unwrap();
    let (wire_shutdown_tx, wire_shutdown_rx) = tokio::sync::oneshot::channel();
    let wire_server = tokio::spawn(serve(
        wire_listener,
        MysqlWireConfig {
            certificate_pem: certificate_pem.clone(),
            private_key_pem,
            database_name: "main".into(),
            max_connections: 8,
            handshake_timeout: Duration::from_secs(2),
        },
        native_client,
        async move {
            let _ = wire_shutdown_rx.await;
        },
    ));

    let options = OptsBuilder::default()
        .ip_or_hostname("localhost")
        .resolved_ips(Some(vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]))
        .tcp_port(wire_address.port())
        .user(Some("wire-user"))
        .pass(Some("wire-password"))
        .db_name(Some("main"))
        .prefer_socket(false)
        .ssl_opts(Some(
            SslOpts::default()
                .with_root_certs(vec![certificate_pem.clone().into()])
                .with_disable_built_in_roots(true),
        ))
        .max_allowed_packet(Some(16 * 1024 * 1024))
        .wait_timeout(Some(60));
    let pool = Pool::new(options);
    let mut connection = pool.get_conn().await.unwrap();
    connection
        .query_drop("CREATE TABLE wire_items (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    connection
        .exec_drop(
            "INSERT INTO wire_items (id, name) VALUES (?, ?)",
            (1_i64, "one"),
        )
        .await
        .unwrap();
    let rows: Vec<(i64, String)> = connection
        .exec("SELECT id, name FROM wire_items WHERE id = ?", (1_i64,))
        .await
        .unwrap();
    assert_eq!(rows, vec![(1, "one".into())]);

    connection.query_drop("START TRANSACTION").await.unwrap();
    connection
        .query_drop("INSERT INTO wire_items VALUES (2, 'rolled back')")
        .await
        .unwrap();
    connection.query_drop("ROLLBACK").await.unwrap();
    let count: Option<u64> = connection
        .query_first("SELECT COUNT(*) FROM wire_items")
        .await
        .unwrap();
    assert_eq!(count, Some(1));
    drop(connection);
    pool.disconnect().await.unwrap();

    let wrong_password = Pool::new(
        OptsBuilder::default()
            .ip_or_hostname("localhost")
            .resolved_ips(Some(vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]))
            .tcp_port(wire_address.port())
            .user(Some("wire-user"))
            .pass(Some("wrong-password"))
            .db_name(Some("main"))
            .prefer_socket(false)
            .ssl_opts(Some(
                SslOpts::default()
                    .with_root_certs(vec![certificate_pem.clone().into()])
                    .with_disable_built_in_roots(true),
            ))
            .max_allowed_packet(Some(16 * 1024 * 1024))
            .wait_timeout(Some(60)),
    );
    assert!(wrong_password.get_conn().await.is_err());
    wrong_password.disconnect().await.unwrap();

    let plaintext = Pool::new(
        OptsBuilder::default()
            .ip_or_hostname("localhost")
            .resolved_ips(Some(vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]))
            .tcp_port(wire_address.port())
            .user(Some("wire-user"))
            .pass(Some("wire-password"))
            .db_name(Some("main"))
            .prefer_socket(false)
            .max_allowed_packet(Some(16 * 1024 * 1024))
            .wait_timeout(Some(60)),
    );
    assert!(plaintext.get_conn().await.is_err());
    plaintext.disconnect().await.unwrap();

    wire_shutdown_tx.send(()).unwrap();
    wire_server.await.unwrap().unwrap();
    native_shutdown_tx.send(()).unwrap();
    native_server.await.unwrap().unwrap();
}
