use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use mongreldb_client::native::NativeClient;
use mongreldb_core::{CancellationReason, Database, ExecutionControl};
use mongreldb_migrate_mysql::{
    apply_target_schema, copy_snapshot_controlled, cutover_with, migrate, validate_target,
    CheckpointStore, MigrationCheckpoint, MigrationOptions, MigrationStage, MysqlSource,
    SourceFkAction,
};
use mongreldb_protocol::native_transport::{
    NativeRpcClientConfig, NativeRpcServer, NativeRpcServerConfig, NativeRpcServices,
};
use mongreldb_server::{build_app_with_sessions_and_control, SessionStore};
use mysql_async::prelude::Queryable;
use mysql_async::{Opts, OptsBuilder, Pool, SslOpts};
use tempfile::tempdir;

fn source_options() -> Option<Opts> {
    let host = std::env::var("MONGRELDB_TEST_MYSQL_HOST").ok()?;
    let port = std::env::var("MONGRELDB_TEST_MYSQL_PORT")
        .ok()?
        .parse()
        .ok()?;
    let password = std::env::var("MONGRELDB_TEST_MYSQL_PASSWORD").ok()?;
    let ca = PathBuf::from(std::env::var("MONGRELDB_TEST_MYSQL_CA").ok()?);
    Some(
        OptsBuilder::default()
            .ip_or_hostname(host)
            .resolved_ips(Some(vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]))
            .tcp_port(port)
            .user(Some("root"))
            .pass(Some(password))
            .db_name(Some("source"))
            .prefer_socket(false)
            .ssl_opts(Some(
                SslOpts::default()
                    .with_root_certs(vec![ca.into()])
                    .with_disable_built_in_roots(true),
            ))
            .max_allowed_packet(Some(16 * 1024 * 1024))
            .wait_timeout(Some(60))
            .into(),
    )
}

#[tokio::test]
async fn real_mysql_snapshot_concurrent_binlog_restart_cutover_and_rollback_guard() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let Some(source_options) = source_options() else {
        eprintln!("skipped: real MySQL environment is not configured");
        return;
    };
    let source_pool = Pool::new(source_options.clone());
    let mut source_admin = source_pool.get_conn().await.unwrap();
    source_admin
        .query_drop("DROP TABLE IF EXISTS b_migrate_children")
        .await
        .unwrap();
    source_admin
        .query_drop("DROP TABLE IF EXISTS a_migrate_parents")
        .await
        .unwrap();
    source_admin
        .query_drop("DROP TABLE IF EXISTS migrate_items")
        .await
        .unwrap();
    source_admin
        .query_drop("CREATE TABLE a_migrate_parents (id BIGINT PRIMARY KEY) ENGINE=InnoDB")
        .await
        .unwrap();
    source_admin
        .query_drop(
            "CREATE TABLE b_migrate_children (\
             id BIGINT PRIMARY KEY, parent_id BIGINT NULL, \
             CONSTRAINT fk_migrate_parent FOREIGN KEY (parent_id) \
             REFERENCES a_migrate_parents (id) \
             ON DELETE CASCADE ON UPDATE SET NULL) ENGINE=InnoDB",
        )
        .await
        .unwrap();
    source_admin
        .query_drop("INSERT INTO a_migrate_parents VALUES (1)")
        .await
        .unwrap();
    source_admin
        .query_drop("INSERT INTO b_migrate_children VALUES (1, 1)")
        .await
        .unwrap();
    source_admin
        .query_drop(
            "CREATE TABLE migrate_items (id BIGINT AUTO_INCREMENT PRIMARY KEY, \
             name VARCHAR(100) NOT NULL, \
             external_id VARCHAR(50), UNIQUE KEY uq_external_id (external_id), \
             KEY idx_name (name)) \
             ENGINE=InnoDB",
        )
        .await
        .unwrap();
    let rows = (1_i64..=2_000)
        .map(|id| (id, format!("item-{id}")))
        .collect::<Vec<_>>();
    source_admin
        .exec_batch("INSERT INTO migrate_items (id, name) VALUES (?, ?)", rows)
        .await
        .unwrap();
    drop(source_admin);

    let target_dir = tempdir().unwrap();
    let target_db = Arc::new(Database::create(target_dir.path()).unwrap());
    let sessions = Arc::new(SessionStore::new(8, Duration::from_secs(60)));
    let (_, server_control) = build_app_with_sessions_and_control(
        Arc::clone(&target_db),
        std::iter::empty(),
        None,
        None,
        false,
        Arc::clone(&sessions),
    );
    let runtime = server_control.native_runtime(Arc::clone(&target_db), Arc::clone(&sessions));
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let certificate_pem = certificate.cert.pem().into_bytes();
    let native_socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let native_address = native_socket.local_addr().unwrap();
    drop(native_socket);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let native_server = tokio::spawn(
        NativeRpcServer::new(NativeRpcServerConfig {
            address: native_address,
            certificate_pem: certificate_pem.clone(),
            private_key_pem: certificate.key_pair.serialize_pem().into_bytes(),
            client_ca_pem: None,
            max_connections: 8,
            max_concurrent_streams: 8,
            max_in_flight_per_connection: 8,
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
                let _ = shutdown_rx.await;
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
        max_in_flight: 8,
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
    let target = native_client.authenticate_anonymous().await.unwrap();

    let source = MysqlSource::from_opts(source_options.clone()).unwrap();
    let schema = source.introspect().await.unwrap();
    assert!(schema
        .tables
        .iter()
        .any(|table| table.name == "migrate_items"));
    let items = schema
        .tables
        .iter()
        .find(|table| table.name == "migrate_items")
        .unwrap();
    assert_eq!(items.unique_keys[0].name, "uq_external_id");
    assert_eq!(items.unique_keys[0].columns, vec!["external_id"]);
    assert!(items.columns[0].auto_increment);
    assert_eq!(items.secondary_indexes[0].name, "idx_name");
    assert_eq!(items.secondary_indexes[0].columns, vec!["name"]);
    let child = schema
        .tables
        .iter()
        .find(|table| table.name == "b_migrate_children")
        .unwrap();
    assert_eq!(child.foreign_keys[0].on_delete, SourceFkAction::Cascade);
    assert_eq!(child.foreign_keys[0].on_update, SourceFkAction::SetNull);
    let schema_only_dir = tempdir().unwrap();
    let schema_only_store = CheckpointStore::new(schema_only_dir.path().join("schema-only.json"));
    let schema_only = migrate(
        &source,
        &target,
        &schema_only_store,
        &MigrationOptions {
            schema_only: true,
            ..MigrationOptions::default()
        },
        &ExecutionControl::with_timeout(Duration::from_secs(60)),
        || Ok(()),
    )
    .await
    .unwrap();
    assert_eq!(schema_only.stage, MigrationStage::Succeeded);
    let child_schema = target_db
        .table("b_migrate_children")
        .unwrap()
        .lock()
        .schema()
        .clone();
    assert_eq!(
        child_schema.constraints.foreign_keys[0].on_delete,
        mongreldb_core::constraint::FkAction::Cascade
    );
    assert_eq!(
        child_schema.constraints.foreign_keys[0].on_update,
        mongreldb_core::constraint::FkAction::SetNull
    );
    assert_eq!(
        target
            .execute("SELECT COUNT(*) AS count FROM migrate_items", None)
            .await
            .unwrap()
            .batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0),
        0
    );
    let mut snapshot = source.begin_consistent_snapshot().await.unwrap();
    let snapshot_position = snapshot.position.clone();
    let checkpoint_dir = tempdir().unwrap();
    let store = CheckpointStore::new(checkpoint_dir.path().join("migration.json"));
    let mut checkpoint = MigrationCheckpoint::new(&source, &schema, snapshot.position.clone());
    store.persist(&checkpoint).unwrap();
    apply_target_schema(&schema, &target, &mut checkpoint, &store)
        .await
        .unwrap();

    let writer_pool = Pool::new(source_options.clone());
    let writer = tokio::spawn(async move {
        let mut connection = writer_pool.get_conn().await.unwrap();
        connection
            .query_drop("INSERT INTO migrate_items (id, name) VALUES (2001, 'late')")
            .await
            .unwrap();
        connection
            .query_drop("UPDATE migrate_items SET id = 2003, name = 'updated' WHERE id = 2")
            .await
            .unwrap();
        connection
            .query_drop("DELETE FROM migrate_items WHERE id = 3")
            .await
            .unwrap();
        drop(connection);
        writer_pool.disconnect().await.unwrap();
    });
    let control = ExecutionControl::with_timeout(Duration::from_secs(60));
    copy_snapshot_controlled(
        &mut snapshot,
        &schema,
        &target,
        &mut checkpoint,
        &store,
        25,
        &control,
    )
    .await
    .unwrap();
    snapshot.commit().await.unwrap();
    writer.await.unwrap();
    validate_target(&schema, &target, &mut checkpoint, &store, &control)
        .await
        .unwrap();

    let options = MigrationOptions {
        replica_server_id: 4_294_000_002,
        ..MigrationOptions::default()
    };
    source
        .catch_up_controlled(
            &schema,
            &target,
            &mut checkpoint,
            &store,
            &control,
            &options,
        )
        .await
        .unwrap();
    let caught_up_position = checkpoint.last_binlog_position.clone();
    let applied_transactions = checkpoint.applied_transactions.clone();
    checkpoint.last_binlog_position = snapshot_position;
    store.persist(&checkpoint).unwrap();
    source
        .catch_up_controlled(
            &schema,
            &target,
            &mut checkpoint,
            &store,
            &control,
            &options,
        )
        .await
        .unwrap();
    assert_eq!(checkpoint.last_binlog_position, caught_up_position);
    assert_eq!(checkpoint.applied_transactions, applied_transactions);
    let mut source_admin = source_pool.get_conn().await.unwrap();
    source_admin
        .query_drop("INSERT INTO migrate_items (id, name) VALUES (2002, 'after-restart')")
        .await
        .unwrap();
    drop(source_admin);

    let refused_dir = tempdir().unwrap();
    let refused_store = CheckpointStore::new(refused_dir.path().join("refused.json"));
    let mut refused_checkpoint = checkpoint.clone();
    refused_store.persist(&refused_checkpoint).unwrap();
    let refused_control = ExecutionControl::new(None);
    refused_control.cancel(CancellationReason::ClientRequest);
    let published = std::cell::Cell::new(false);
    assert!(cutover_with(
        &source,
        &target,
        &mut refused_checkpoint,
        &refused_store,
        &options,
        &refused_control,
        || {
            published.set(true);
            Ok(())
        },
    )
    .await
    .is_err());
    assert!(!published.get());

    cutover_with(
        &source,
        &target,
        &mut checkpoint,
        &store,
        &options,
        &control,
        || Ok(()),
    )
    .await
    .unwrap();
    assert_eq!(checkpoint.stage, MigrationStage::RollbackWindow);
    assert_eq!(
        checkpoint.target_row_counts.get("migrate_items"),
        Some(&2_001)
    );
    for (id, expected) in [(2, 0), (2003, 1)] {
        let count = target
            .execute(
                format!("SELECT COUNT(*) AS count FROM migrate_items WHERE id = {id}"),
                None,
            )
            .await
            .unwrap()
            .batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, expected, "unexpected row count for id {id}");
    }
    assert!(mongreldb_migrate_mysql::rollback_with(&mut checkpoint, &store, 1, || Ok(())).is_err());

    target.close().await.unwrap();
    source.disconnect().await.unwrap();
    source_pool.disconnect().await.unwrap();
    shutdown_tx.send(()).unwrap();
    native_server.await.unwrap().unwrap();
}
