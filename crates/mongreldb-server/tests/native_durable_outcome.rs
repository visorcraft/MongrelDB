//! P0.6 — native durable-outcome protocol parity.
//!
//! A native client must always determine whether an execution committed during
//! the query-status retention window, via structural fields on execute,
//! get_query_status, and cancel_query (no string parsing).

use std::sync::{Arc, Barrier};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mongreldb_core::{ColumnDef, ColumnFlags, Database, Schema, TypeId};
use mongreldb_protocol::native;
use mongreldb_protocol::native_transport::{
    NativeRpcClientConfig, NativeRpcConnection, NativeRpcServer, NativeRpcServerConfig,
    NativeRpcServices,
};
use mongreldb_protocol::NATIVE_API_MAJOR;
use mongreldb_query::SqlTestHookPoint;
use mongreldb_server::{build_app_with_sessions_and_control, SessionStore};
use tempfile::tempdir;

fn context(deadline_unix_micros: u64) -> Option<native::RequestContext> {
    Some(native::RequestContext {
        version: Some(native::ApiVersion {
            major: NATIVE_API_MAJOR,
            minor: 0,
        }),
        request_id: "test".into(),
        deadline_unix_micros,
        idempotency_key: String::new(),
    })
}

fn session_token_hex(session_id: &[u8]) -> String {
    session_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

async fn start_native(
    db: Arc<Database>,
    sessions: Arc<SessionStore>,
) -> (
    NativeRpcConnection,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<
        Result<(), mongreldb_protocol::native_transport::NativeRpcTransportError>,
    >,
) {
    let (_, control) = build_app_with_sessions_and_control(
        Arc::clone(&db),
        std::iter::empty(),
        None,
        None,
        false,
        Arc::clone(&sessions),
    );
    let runtime = control.native_runtime(Arc::clone(&db), Arc::clone(&sessions));
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let certificate_pem = certified.cert.pem().into_bytes();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap();
    drop(socket);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        NativeRpcServer::new(NativeRpcServerConfig {
            address,
            certificate_pem: certificate_pem.clone(),
            private_key_pem: certified.key_pair.serialize_pem().into_bytes(),
            client_ca_pem: None,
            max_connections: 32,
            max_concurrent_streams: 32,
            max_in_flight_per_connection: 32,
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
    let config = NativeRpcClientConfig {
        endpoint: format!("https://127.0.0.1:{}", address.port()),
        domain_name: "localhost".into(),
        ca_certificate_pem: certificate_pem,
        client_identity_pem: None,
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(30),
        max_in_flight: 32,
        tcp_keepalive: Duration::from_secs(30),
        http2_keepalive_interval: Duration::from_secs(30),
    };
    let mut connection = None;
    for _ in 0..100 {
        match NativeRpcConnection::connect(&config).await {
            Ok(connected) => {
                connection = Some(connected);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
    (
        connection.expect("native listener did not start"),
        shutdown_tx,
        server,
    )
}

async fn open_anonymous_session(
    client: &mongreldb_protocol::native_transport::NativeRpcClient,
    sessions: &SessionStore,
) -> Vec<u8> {
    let auth = client
        .auth()
        .authenticate(native::AuthenticateRequest {
            context: context(0),
            credential: None,
        })
        .await
        .unwrap()
        .into_inner();
    client
        .session()
        .open_session(native::OpenSessionRequest {
            context: context(0),
            identity: auth.identity,
            database_id: sessions.database_id().as_bytes().to_vec(),
            auth_token: auth.auth_token,
        })
        .await
        .unwrap()
        .into_inner()
        .session_id
}

fn create_items_table(db: &Database) {
    db.create_table(
        "items",
        Schema {
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
    .unwrap();
}

/// P0.6-X2: lost streaming response after commit; status recovery shows committed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_streaming_response_after_commit_status_recovery() {
    use futures::StreamExt;

    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    create_items_table(&db);
    let sessions = Arc::new(SessionStore::new(8, Duration::from_secs(60)));
    let (connection, shutdown_tx, server) = start_native(Arc::clone(&db), Arc::clone(&sessions)).await;
    let client = connection.client();
    let session_id = open_anonymous_session(&client, &sessions).await;

    let query_id = vec![0x22; 16];
    // Multi-statement: first statement commits the insert; final statement streams.
    let mut stream = client
        .query()
        .execute_stream(native::ExecuteRequest {
            context: context(0),
            session_id: session_id.clone(),
            query_id: query_id.clone(),
            command: Some(native::execute_request::Command::Sql(
                "INSERT INTO items (id) VALUES (99); SELECT id FROM items".into(),
            )),
            parameters: Vec::new(),
        })
        .await
        .unwrap()
        .into_inner();

    // Drain the stream fully (client received frames), then "lose" the terminal
    // by relying solely on get_query_status for the durable outcome.
    let mut saw_eos = false;
    while let Some(frame) = stream.next().await {
        let frame = frame.unwrap();
        if frame.end_of_stream {
            saw_eos = true;
            break;
        }
    }
    assert!(saw_eos, "stream must terminate");
    drop(stream);

    let status = client
        .query()
        .get_query_status(native::GetQueryStatusRequest {
            context: context(0),
            query_id: query_id.clone(),
            session_id: session_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    let status_durable = status.durable.expect("status carries DurableOutcome");
    assert!(
        status_durable.committed,
        "lost stream terminal still recovers committed: {status_durable:?}"
    );
    assert!(
        status_durable.committed_statements >= 1,
        "insert must count as committed statement: {status_durable:?}"
    );
    assert!(
        status_durable.terminal_state == "committed"
            || status_durable.terminal_state == "completed"
            || status.phase == native::QueryPhase::Completed as i32,
        "terminal/phase after stream: durable={status_durable:?} phase={}",
        status.phase
    );

    let _ = shutdown_tx.send(());
    let _ = server.await;
}

/// P0.6-X1: lost buffered write response; status recovery shows committed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_buffered_write_status_recovery_shows_committed() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    create_items_table(&db);
    let sessions = Arc::new(SessionStore::new(8, Duration::from_secs(60)));
    let (connection, shutdown_tx, server) = start_native(Arc::clone(&db), Arc::clone(&sessions)).await;
    let client = connection.client();
    let session_id = open_anonymous_session(&client, &sessions).await;

    let query_id = vec![0x11; 16];
    let execute = client
        .query()
        .execute(native::ExecuteRequest {
            context: context(0),
            session_id: session_id.clone(),
            query_id: query_id.clone(),
            command: Some(native::execute_request::Command::Sql(
                "INSERT INTO items (id) VALUES (1)".into(),
            )),
            parameters: Vec::new(),
        })
        .await
        .unwrap()
        .into_inner();

    let durable = execute.durable.expect("execute carries DurableOutcome");
    assert!(execute.committed);
    assert!(durable.committed);
    assert_eq!(durable.committed_statements, 1);
    assert_eq!(durable.terminal_state, "committed");
    assert!(durable.last_commit_epoch.is_some());
    // Prefer optional epoch over legacy zero-as-missing field.
    assert_eq!(durable.last_commit_epoch, Some(execute.commit_epoch));
    assert_ne!(execute.commit_epoch, 0);

    // Simulate a lost execute response: recover solely via get_query_status.
    let status = client
        .query()
        .get_query_status(native::GetQueryStatusRequest {
            context: context(0),
            query_id: query_id.clone(),
            session_id: session_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    let status_durable = status.durable.expect("status carries DurableOutcome");
    assert!(status_durable.committed);
    assert_eq!(status_durable.committed_statements, 1);
    assert_eq!(status_durable.terminal_state, "committed");
    assert_eq!(
        status_durable.last_commit_epoch,
        durable.last_commit_epoch
    );
    assert_eq!(status.phase, native::QueryPhase::Completed as i32);

    let _ = shutdown_tx.send(());
    let _ = server.await;
}

/// P0.6-X3: cancel during commit-critical returns TooLate + durable snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_too_late_returns_structured_outcome() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    create_items_table(&db);
    let sessions = Arc::new(SessionStore::new(8, Duration::from_secs(60)));
    let (connection, shutdown_tx, server) = start_native(Arc::clone(&db), Arc::clone(&sessions)).await;
    let client = connection.client();
    let session_id = open_anonymous_session(&client, &sessions).await;

    let entry = sessions
        .get(&session_token_hex(&session_id), "anonymous")
        .expect("session entry");
    let barrier = Arc::new(Barrier::new(2));
    let hook_barrier = Arc::clone(&barrier);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    entry.session().set_test_hook(Some(Arc::new(move |point| {
        if point == SqlTestHookPoint::InsideCommitCritical {
            let _ = entered_tx.send(());
            hook_barrier.wait();
        }
    })));

    let query_id = vec![0x22; 16];
    let execute_client = client.clone();
    let execute_session = session_id.clone();
    let execute_qid = query_id.clone();
    let execute_task = tokio::spawn(async move {
        execute_client
            .query()
            .execute(native::ExecuteRequest {
                context: context(0),
                session_id: execute_session,
                query_id: execute_qid,
                command: Some(native::execute_request::Command::Sql(
                    "INSERT INTO items (id) VALUES (1)".into(),
                )),
                parameters: Vec::new(),
            })
            .await
    });

    tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
        .await
        .unwrap();

    let cancel = client
        .query()
        .cancel_query(native::CancelQueryRequest {
            context: context(0),
            query_id: query_id.clone(),
            session_id: session_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(cancel.outcome, native::CancelOutcome::TooLate as i32);
    let _cancel_durable = cancel.durable.expect("cancel carries DurableOutcome");

    barrier.wait();
    let execute = execute_task.await.unwrap().unwrap().into_inner();
    assert!(execute.committed);
    let execute_durable = execute.durable.expect("execute durable after commit");
    assert!(execute_durable.committed);
    assert_eq!(execute_durable.terminal_state, "committed");

    entry.session().set_test_hook(None);
    let _ = shutdown_tx.send(());
    let _ = server.await;
}

/// P0.6-X4: cancel after finish returns AlreadyFinished with terminal receipt.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_already_finished_returns_terminal_receipt() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    create_items_table(&db);
    let sessions = Arc::new(SessionStore::new(8, Duration::from_secs(60)));
    let (connection, shutdown_tx, server) = start_native(Arc::clone(&db), Arc::clone(&sessions)).await;
    let client = connection.client();
    let session_id = open_anonymous_session(&client, &sessions).await;

    let query_id = vec![0x33; 16];
    let execute = client
        .query()
        .execute(native::ExecuteRequest {
            context: context(0),
            session_id: session_id.clone(),
            query_id: query_id.clone(),
            command: Some(native::execute_request::Command::Sql(
                "INSERT INTO items (id) VALUES (7)".into(),
            )),
            parameters: Vec::new(),
        })
        .await
        .unwrap()
        .into_inner();
    let execute_durable = execute.durable.expect("execute durable");
    assert!(execute_durable.committed);

    let cancel = client
        .query()
        .cancel_query(native::CancelQueryRequest {
            context: context(0),
            query_id: query_id.clone(),
            session_id: session_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        cancel.outcome,
        native::CancelOutcome::AlreadyFinished as i32
    );
    let durable = cancel.durable.expect("cancel durable terminal receipt");
    assert!(durable.committed);
    assert_eq!(durable.committed_statements, 1);
    assert_eq!(durable.terminal_state, "committed");
    assert_eq!(durable.last_commit_epoch, execute_durable.last_commit_epoch);
    assert_eq!(
        durable.first_commit_statement_index,
        execute_durable.first_commit_statement_index
    );

    let _ = shutdown_tx.send(());
    let _ = server.await;
}

/// P0.6-X: cancel accepted path returns CancelOutcome + durable snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_accepted_returns_outcome_and_durable() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    create_items_table(&db);
    let sessions = Arc::new(SessionStore::new(8, Duration::from_secs(60)));
    let (connection, shutdown_tx, server) = start_native(Arc::clone(&db), Arc::clone(&sessions)).await;
    let client = connection.client();
    let session_id = open_anonymous_session(&client, &sessions).await;

    let entry = sessions
        .get(&session_token_hex(&session_id), "anonymous")
        .expect("session entry");
    let barrier = Arc::new(Barrier::new(2));
    let hook_barrier = Arc::clone(&barrier);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    entry.session().set_test_hook(Some(Arc::new(move |point| {
        if point == SqlTestHookPoint::Planning {
            let _ = entered_tx.send(());
            hook_barrier.wait();
        }
    })));

    let query_id = vec![0x44; 16];
    let execute_client = client.clone();
    let execute_session = session_id.clone();
    let execute_qid = query_id.clone();
    let execute_task = tokio::spawn(async move {
        execute_client
            .query()
            .execute(native::ExecuteRequest {
                context: context(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_micros() as u64
                        + 30_000_000,
                ),
                session_id: execute_session,
                query_id: execute_qid,
                command: Some(native::execute_request::Command::Sql("SELECT 1".into())),
                parameters: Vec::new(),
            })
            .await
    });

    tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
        .await
        .unwrap();

    let cancel = client
        .query()
        .cancel_query(native::CancelQueryRequest {
            context: context(0),
            query_id: query_id.clone(),
            session_id: session_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(cancel.outcome, native::CancelOutcome::Accepted as i32);
    let durable = cancel.durable.expect("cancel durable");
    assert!(!durable.committed);
    assert_eq!(durable.committed_statements, 0);
    assert!(durable.last_commit_epoch.is_none());

    barrier.wait();
    let _ = execute_task.await;
    entry.session().set_test_hook(None);
    let _ = shutdown_tx.send(());
    let _ = server.await;
}

/// ID: P0.6-X7 Native and HTTP durable outcome objects match on structural fields.
/// ID: P0.6-X8 Rust client exposes durable fields structurally (no string parsing).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p06_x7_native_and_http_durable_outcome_fields_match() {
    use axum::body::{to_bytes, Body};
    use axum::http::Request;
    use serde_json::{json, Value};
    use tower::ServiceExt;

    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    create_items_table(&db);
    let sessions = Arc::new(SessionStore::new(8, Duration::from_secs(60)));
    let (app, control) = build_app_with_sessions_and_control(
        Arc::clone(&db),
        std::iter::empty(),
        None,
        None,
        false,
        Arc::clone(&sessions),
    );

    // HTTP path: durable write receipt + GET /queries/{id} (same registry as native).
    let query_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let http_insert = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/sql")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "sql": "INSERT INTO items (id) VALUES (42)",
                        "query_id": query_id,
                        "idempotency_key": "p06-x7-http",
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(http_insert.status(), axum::http::StatusCode::OK);
    let http_insert_body: Value =
        serde_json::from_slice(&to_bytes(http_insert.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    // Idempotent durable receipt (structural fields, not string parsing).
    assert_eq!(http_insert_body["status"], "committed");
    assert_eq!(http_insert_body["outcome"]["committed"], true);
    assert_eq!(http_insert_body["outcome"]["committed_statements"], 1);
    assert!(http_insert_body["outcome"]["last_commit_epoch"].is_number());

    let http_status = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/queries/{query_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(http_status.status(), axum::http::StatusCode::OK);
    let http_status_body: Value =
        serde_json::from_slice(&to_bytes(http_status.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    assert_eq!(http_status_body["outcome"]["committed"], true);
    assert_eq!(http_status_body["outcome"]["committed_statements"], 1);
    assert_eq!(
        http_status_body["outcome"]["last_commit_epoch"],
        http_insert_body["outcome"]["last_commit_epoch"]
    );

    // Native path against the same AppState control surface.
    let runtime = control.native_runtime(Arc::clone(&db), Arc::clone(&sessions));
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let certificate_pem = certified.cert.pem().into_bytes();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap();
    drop(socket);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        NativeRpcServer::new(NativeRpcServerConfig {
            address,
            certificate_pem: certificate_pem.clone(),
            private_key_pem: certified.key_pair.serialize_pem().into_bytes(),
            client_ca_pem: None,
            max_connections: 32,
            max_concurrent_streams: 32,
            max_in_flight_per_connection: 32,
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
    let config = NativeRpcClientConfig {
        endpoint: format!("https://127.0.0.1:{}", address.port()),
        domain_name: "localhost".into(),
        ca_certificate_pem: certificate_pem,
        client_identity_pem: None,
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(30),
        max_in_flight: 32,
        tcp_keepalive: Duration::from_secs(30),
        http2_keepalive_interval: Duration::from_secs(30),
    };
    let mut connection = None;
    for _ in 0..100 {
        if let Ok(c) = NativeRpcConnection::connect(&config).await {
            connection = Some(c);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let client = connection.expect("native ready").client();
    let session_id = open_anonymous_session(&client, &sessions).await;
    let native_qid = vec![0xBB; 16];
    let execute = client
        .query()
        .execute(native::ExecuteRequest {
            context: context(0),
            session_id: session_id.clone(),
            query_id: native_qid.clone(),
            command: Some(native::execute_request::Command::Sql(
                "INSERT INTO items (id) VALUES (43)".into(),
            )),
            parameters: Vec::new(),
        })
        .await
        .unwrap()
        .into_inner();
    let durable = execute.durable.expect("native durable");
    // Structural parity: both surfaces expose committed / committed_statements /
    // last_commit_epoch without string parsing (P0.6-X7 / X9).
    assert!(durable.committed);
    assert_eq!(durable.committed_statements, 1);
    assert!(durable.last_commit_epoch.is_some());
    assert_eq!(
        durable.committed,
        http_status_body["outcome"]["committed"].as_bool().unwrap()
    );
    assert_eq!(
        durable.committed_statements as u64,
        http_status_body["outcome"]["committed_statements"]
            .as_u64()
            .unwrap()
    );
    assert_eq!(durable.terminal_state, "committed");
    // serialization_state is a structural string enum field (not a freeform blob).
    assert!(
        matches!(
            durable.serialization_state.as_str(),
            "not_started" | "in_progress" | "succeeded" | "failed" | ""
        ),
        "serialization_state must be structural: {:?}",
        durable.serialization_state
    );
    assert!(
        !durable.serialization_state.is_empty(),
        "native durable must populate serialization_state"
    );

    let _ = shutdown_tx.send(());
    let _ = server.await;
}

/// ID: P0.6-X8 High-level native client exposes durable fields structurally.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn high_level_client_exposes_durable_structurally() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    create_items_table(&db);
    let sessions = Arc::new(SessionStore::new(8, Duration::from_secs(60)));
    let (_, control) = build_app_with_sessions_and_control(
        Arc::clone(&db),
        std::iter::empty(),
        None,
        None,
        false,
        Arc::clone(&sessions),
    );
    let runtime = control.native_runtime(Arc::clone(&db), Arc::clone(&sessions));
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let certificate_pem = certified.cert.pem().into_bytes();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap();
    drop(socket);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        NativeRpcServer::new(NativeRpcServerConfig {
            address,
            certificate_pem: certificate_pem.clone(),
            private_key_pem: certified.key_pair.serialize_pem().into_bytes(),
            client_ca_pem: None,
            max_connections: 32,
            max_concurrent_streams: 32,
            max_in_flight_per_connection: 32,
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
    let config = NativeRpcClientConfig {
        endpoint: format!("https://127.0.0.1:{}", address.port()),
        domain_name: "localhost".into(),
        ca_certificate_pem: certificate_pem,
        client_identity_pem: None,
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(30),
        max_in_flight: 32,
        tcp_keepalive: Duration::from_secs(30),
        http2_keepalive_interval: Duration::from_secs(30),
    };
    let mut ready = false;
    for _ in 0..100 {
        if NativeRpcConnection::connect(&config).await.is_ok() {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(ready);

    let client = mongreldb_client::native::NativeClient::connect(
        config,
        1,
        *sessions.database_id().as_bytes(),
    )
    .await
    .unwrap()
    .authenticate_anonymous()
    .await
    .unwrap();

    let result = client
        .execute("INSERT INTO items (id) VALUES (99)", None)
        .await
        .unwrap();
    assert!(result.committed);
    assert!(result.durable.committed);
    assert_eq!(result.durable.terminal_state, "committed");
    assert!(result.durable.last_commit_epoch.is_some());
    assert_eq!(result.commit_epoch, result.durable.last_commit_epoch);

    let status = client.query_status(&result.query_id).await.unwrap();
    assert_eq!(status.phase, native::QueryPhase::Completed as i32);
    assert!(status.durable.committed);
    assert_eq!(status.durable.terminal_state, "committed");
    assert_eq!(
        status.durable.last_commit_epoch,
        result.durable.last_commit_epoch
    );

    let cancel = client.cancel(&result.query_id).await.unwrap();
    assert_eq!(cancel.outcome, native::CancelOutcome::AlreadyFinished);
    assert!(cancel.durable.committed);
    assert_eq!(cancel.durable.terminal_state, "committed");

    let _ = shutdown_tx.send(());
    let _ = server.await;
}
