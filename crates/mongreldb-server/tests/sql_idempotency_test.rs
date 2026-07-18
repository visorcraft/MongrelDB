use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use mongreldb_core::{ColumnDef, ColumnFlags, Database, Permission, Schema, TypeId};
use mongreldb_query::SqlTestHookPoint;
use mongreldb_server::{build_app, build_app_full, build_app_with_sessions, SessionStore};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;
use tempfile::tempdir;
use tower::ServiceExt;

fn request(body: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/sql")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn authenticated_request(body: Value, authorization: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/sql")
        .header("content-type", "application/json")
        .header("authorization", authorization)
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn status_request(query_id: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(format!("/queries/{query_id}"))
        .body(Body::empty())
        .unwrap()
}

fn session_request(path: &str, body: Value, session_id: Option<&str>) -> Request<Body> {
    let mut request = Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json");
    if let Some(session_id) = session_id {
        request = request.header("x-session-id", session_id);
    }
    request.body(Body::from(body.to_string())).unwrap()
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn database() -> (tempfile::TempDir, Arc<Database>) {
    let directory = tempdir().unwrap();
    let database = Arc::new(Database::create(directory.path()).unwrap());
    database
        .create_table(
            "items",
            Schema {
                columns: vec![
                    ColumnDef {
                        id: 1,
                        name: "id".into(),
                        ty: TypeId::Int64,
                        flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                        default_value: None,
                        embedding_source: None,
                    },
                    ColumnDef {
                        id: 2,
                        name: "value".into(),
                        ty: TypeId::Int64,
                        flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                        default_value: None,
                        embedding_source: None,
                    },
                ],
                ..Schema::default()
            },
        )
        .unwrap();
    (directory, database)
}

async fn count(app: axum::Router) -> i64 {
    let response = app
        .oneshot(request(json!({
            "sql": "SELECT count(*) AS n FROM items",
        })))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    json_body(response).await[0]["n"].as_i64().unwrap()
}

#[tokio::test]
async fn committed_write_replays_receipt_without_reexecution_and_survives_restart() {
    let (directory, database) = database();
    let app = build_app(Arc::clone(&database));
    let first_id = "11111111111111111111111111111111";
    let first = app
        .clone()
        .oneshot(request(json!({
            "sql": "INSERT INTO items (id) VALUES (1)",
            "query_id": first_id,
            "idempotency_key": "write-key",
        })))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(first.headers()["idempotency-replayed"], "false");
    assert_eq!(first.headers()["idempotency-persisted"], "true");
    let first = json_body(first).await;
    assert_eq!(first["query_id"], first_id);
    assert_eq!(first["original_query_id"], first_id);
    assert_eq!(first["status"], "committed");
    assert_eq!(first["server_state"], "completed");
    assert_eq!(first["cancel_outcome"], "already_finished");
    assert_eq!(first["cancellation_reason"], "none");
    assert_eq!(first["idempotency_replayed"], false);
    assert_eq!(first["outcome"]["committed_statements"], 1);
    assert!(first["outcome"]["last_commit_epoch"].is_number());
    assert_eq!(
        first["outcome"]["last_commit_epoch_text"],
        first["outcome"]["last_commit_epoch"]
            .as_u64()
            .unwrap()
            .to_string()
    );
    assert_eq!(
        first["last_commit_epoch_text"],
        first["last_commit_epoch"].as_u64().unwrap().to_string()
    );
    assert_eq!(count(app.clone()).await, 1);

    let restarted = build_app(Arc::clone(&database));
    let replay_id = "22222222222222222222222222222222";
    let replay = restarted
        .clone()
        .oneshot(request(json!({
            "sql": "  INSERT  INTO items (id) VALUES (1) -- same request\n",
            "query_id": replay_id,
            "idempotency_key": "write-key",
        })))
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(replay.headers()["idempotency-replayed"], "true");
    let replay = json_body(replay).await;
    assert_eq!(replay["query_id"], replay_id);
    assert_eq!(replay["original_query_id"], first_id);
    assert_eq!(replay["idempotency_replayed"], true);
    assert_eq!(replay["outcome"], first["outcome"]);
    let replay_status = restarted
        .clone()
        .oneshot(status_request(replay_id))
        .await
        .unwrap();
    assert_eq!(replay_status.status(), StatusCode::OK);
    let replay_status = json_body(replay_status).await;
    assert_eq!(replay_status["status"], replay["status"]);
    assert_eq!(replay_status["terminal_state"], replay["status"]);
    assert_eq!(replay_status["state"], replay["server_state"]);
    assert_eq!(replay_status["server_state"], replay["server_state"]);
    assert_eq!(
        replay_status["cancellation_reason"],
        replay["cancellation_reason"]
    );
    assert_eq!(replay_status["committed"], replay["committed"]);
    assert_eq!(replay_status["outcome"], replay["outcome"]);
    assert_eq!(count(restarted.clone()).await, 1);

    let mismatch = restarted
        .clone()
        .oneshot(request(json!({
            "sql": "INSERT INTO items (id) VALUES (2)",
            "query_id": "33333333333333333333333333333333",
            "idempotency_key": "write-key",
        })))
        .await
        .unwrap();
    assert_eq!(mismatch.status(), StatusCode::CONFLICT);
    assert_eq!(
        json_body(mismatch).await["error"]["code"],
        "IDEMPOTENCY_KEY_REUSE_MISMATCH"
    );
    assert_eq!(count(restarted.clone()).await, 1);

    let semantics_mismatch = restarted
        .oneshot(request(json!({
            "sql": "INSERT INTO items (id) VALUES (1)",
            "query_id": "44444444444444444444444444444444",
            "idempotency_key": "write-key",
            "max_output_rows": 1,
        })))
        .await
        .unwrap();
    assert_eq!(semantics_mismatch.status(), StatusCode::CONFLICT);
    assert_eq!(
        json_body(semantics_mismatch).await["error"]["code"],
        "IDEMPOTENCY_KEY_REUSE_MISMATCH"
    );

    let receipt_files: Vec<_> = std::fs::read_dir(directory.path().join("_sql_idempotency"))
        .unwrap()
        .flatten()
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .ends_with(".receipt.json")
        })
        .collect();
    assert_eq!(receipt_files.len(), 1);
    let receipt = std::fs::read_to_string(receipt_files[0].path()).unwrap();
    assert!(!receipt.contains("write-key"));
    assert!(!receipt.contains("INSERT INTO"));
    assert!(!receipt.contains("anonymous"));
}

#[tokio::test]
async fn successful_noop_write_replays_without_later_mutation() {
    let (_directory, database) = database();
    let app = build_app(database);
    let statement = "UPDATE items SET value = 7 WHERE id = 9";
    let first = app
        .clone()
        .oneshot(request(json!({
            "sql": statement,
            "query_id": "10101010101010101010101010101010",
            "idempotency_key": "noop-update",
        })))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(first.headers()["idempotency-persisted"], "true");
    let first = json_body(first).await;
    assert_eq!(first["status"], "completed");
    assert_eq!(first["committed"], false);
    assert_eq!(first["outcome"]["committed"], false);
    assert_eq!(first["outcome"]["committed_statements"], 0);
    assert_eq!(first["outcome"]["last_commit_epoch"], Value::Null);

    let insert = app
        .clone()
        .oneshot(request(json!({
            "sql": "INSERT INTO items (id, value) VALUES (9, 0)",
        })))
        .await
        .unwrap();
    assert_eq!(insert.status(), StatusCode::OK);

    let replay = app
        .clone()
        .oneshot(request(json!({
            "sql": statement,
            "query_id": "20202020202020202020202020202020",
            "idempotency_key": "noop-update",
        })))
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(replay.headers()["idempotency-replayed"], "true");
    let replay = json_body(replay).await;
    assert_eq!(
        replay["original_query_id"],
        "10101010101010101010101010101010"
    );
    assert_eq!(replay["outcome"], first["outcome"]);

    let value = app
        .oneshot(request(json!({
            "sql": "SELECT value FROM items WHERE id = 9",
        })))
        .await
        .unwrap();
    assert_eq!(value.status(), StatusCode::OK);
    assert_eq!(json_body(value).await[0]["value"], 0);
}

#[tokio::test]
async fn read_keys_are_rejected_and_session_semantics_are_bound() {
    let (directory, database) = database();
    let app = build_app(database);

    let first_read = app
        .clone()
        .oneshot(request(json!({
            "sql": "SELECT count(*) AS n FROM items",
            "idempotency_key": "read-key",
        })))
        .await
        .unwrap();
    assert_eq!(first_read.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        json_body(first_read).await["error"]["code"],
        "IDEMPOTENCY_REQUIRES_SINGLE_WRITE"
    );
    let insert = app
        .clone()
        .oneshot(request(
            json!({ "sql": "INSERT INTO items (id) VALUES (1)" }),
        ))
        .await
        .unwrap();
    assert_eq!(insert.status(), StatusCode::OK);
    let second_read = app
        .clone()
        .oneshot(request(json!({
            "sql": "SELECT count(*) AS n FROM items",
            "idempotency_key": "read-key",
        })))
        .await
        .unwrap();
    assert_eq!(second_read.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        json_body(second_read).await["error"]["code"],
        "IDEMPOTENCY_REQUIRES_SINGLE_WRITE"
    );

    let first = app
        .clone()
        .oneshot(request(json!({
            "sql": "INSERT INTO items (id) VALUES (2)",
            "idempotency_key": "session-key",
        })))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let opened = app
        .clone()
        .oneshot(session_request("/sessions", Value::Null, None))
        .await
        .unwrap();
    let session_id = json_body(opened).await["session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let mismatch = app
        .clone()
        .oneshot(session_request(
            "/sql",
            json!({
                "sql": "INSERT INTO items (id) VALUES (2)",
                "idempotency_key": "session-key",
            }),
            Some(&session_id),
        ))
        .await
        .unwrap();
    assert_eq!(mismatch.status(), StatusCode::CONFLICT);
    assert_eq!(
        json_body(mismatch).await["error"]["code"],
        "IDEMPOTENCY_KEY_REUSE_MISMATCH"
    );

    let begin = app
        .clone()
        .oneshot(session_request(
            "/sql",
            json!({ "sql": "BEGIN" }),
            Some(&session_id),
        ))
        .await
        .unwrap();
    assert_eq!(begin.status(), StatusCode::OK);
    let transaction_key = app
        .clone()
        .oneshot(session_request(
            "/sql",
            json!({
                "sql": "INSERT INTO items (id) VALUES (3)",
                "idempotency_key": "transaction-key",
            }),
            Some(&session_id),
        ))
        .await
        .unwrap();
    assert_eq!(transaction_key.status(), StatusCode::CONFLICT);
    assert_eq!(
        json_body(transaction_key).await["error"]["code"],
        "IDEMPOTENCY_UNSUPPORTED_IN_TRANSACTION"
    );
    let rollback = app
        .clone()
        .oneshot(session_request(
            "/sql",
            json!({ "sql": "ROLLBACK" }),
            Some(&session_id),
        ))
        .await
        .unwrap();
    assert_eq!(rollback.status(), StatusCode::OK);

    let multi = app
        .clone()
        .oneshot(request(json!({
            "sql": "INSERT INTO items (id) VALUES (3); INSERT INTO items (id) VALUES (4)",
            "idempotency_key": "multi-key",
        })))
        .await
        .unwrap();
    assert_eq!(multi.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        json_body(multi).await["error"]["code"],
        "IDEMPOTENCY_REQUIRES_SINGLE_WRITE"
    );
    let receipt_directory = directory.path().join("_sql_idempotency");
    let receipt_count = std::fs::read_dir(&receipt_directory)
        .map(|entries| entries.count())
        .unwrap_or(0);
    for (index, sql) in [
        "NOTIFY jobs, 'ready'",
        "LISTEN jobs",
        "ATTACH DATABASE 'other.db' AS other",
        "DETACH DATABASE other",
        "SELECT * FROM items",
        "SHOW TABLES",
        "EXPLAIN SELECT 1",
        "PRAGMA table_info(items)",
    ]
    .into_iter()
    .enumerate()
    {
        let response = app
            .clone()
            .oneshot(request(json!({
                "sql": sql,
                "idempotency_key": format!("transient-{index}"),
            })))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{sql}");
        assert_eq!(
            json_body(response).await["error"]["code"],
            "IDEMPOTENCY_REQUIRES_SINGLE_WRITE",
            "{sql}"
        );
    }
    assert_eq!(
        std::fs::read_dir(receipt_directory)
            .map(|entries| entries.count())
            .unwrap_or(0),
        receipt_count
    );
    assert_eq!(count(app).await, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idempotency_transaction_check_waits_for_the_session_lock() {
    let (_directory, database) = database();
    let sessions = Arc::new(SessionStore::new(8, Duration::from_secs(60)));
    let app = build_app_with_sessions(
        database,
        std::iter::empty(),
        None,
        None,
        false,
        Arc::clone(&sessions),
    );
    let opened = app
        .clone()
        .oneshot(session_request("/sessions", Value::Null, None))
        .await
        .unwrap();
    let session_id = json_body(opened).await["session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let entry = sessions.get(&session_id, "anonymous").unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let hook_barrier = Arc::clone(&barrier);
    let planning_fired = Arc::new(AtomicBool::new(false));
    let hook_planning_fired = Arc::clone(&planning_fired);
    let (planning_tx, planning_rx) = std::sync::mpsc::channel();
    let check_fired = Arc::new(AtomicBool::new(false));
    let hook_check_fired = Arc::clone(&check_fired);
    entry.session().set_test_hook(Some(Arc::new(move |point| {
        if point == SqlTestHookPoint::Planning && !hook_planning_fired.swap(true, Ordering::AcqRel)
        {
            planning_tx.send(()).unwrap();
            hook_barrier.wait();
        }
        if point == SqlTestHookPoint::BeforeServerIdempotencyCheck {
            hook_check_fired.store(true, Ordering::Release);
        }
    })));

    let begin = tokio::spawn(app.clone().oneshot(session_request(
        "/sql",
        json!({ "sql": "BEGIN" }),
        Some(&session_id),
    )));
    tokio::task::spawn_blocking(move || planning_rx.recv().unwrap())
        .await
        .unwrap();
    let write = tokio::spawn(app.clone().oneshot(session_request(
        "/sql",
        json!({
            "sql": "INSERT INTO items (id) VALUES (99)",
            "idempotency_key": "concurrent-transaction-key",
            "query_id": "99999999999999999999999999999999",
        }),
        Some(&session_id),
    )));
    loop {
        let status = app
            .clone()
            .oneshot(status_request("99999999999999999999999999999999"))
            .await
            .unwrap();
        if status.status() == StatusCode::OK {
            break;
        }
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!check_fired.load(Ordering::Acquire));

    barrier.wait();
    assert_eq!(begin.await.unwrap().unwrap().status(), StatusCode::OK);
    let write = write.await.unwrap().unwrap();
    assert_eq!(write.status(), StatusCode::CONFLICT);
    assert_eq!(
        json_body(write).await["error"]["code"],
        "IDEMPOTENCY_UNSUPPORTED_IN_TRANSACTION"
    );
    assert!(check_fired.load(Ordering::Acquire));
    let rollback = app
        .oneshot(session_request(
            "/sql",
            json!({ "sql": "ROLLBACK" }),
            Some(&session_id),
        ))
        .await
        .unwrap();
    assert_eq!(rollback.status(), StatusCode::OK);
}

#[tokio::test]
async fn write_keys_are_scoped_to_authenticated_owner() {
    let directory = tempdir().unwrap();
    let database =
        Arc::new(Database::create_with_credentials(directory.path(), "admin", "pw").unwrap());
    database
        .create_table(
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
    for user in ["alice", "bob"] {
        database.create_user(user, "pw").unwrap();
    }
    database.create_role("writer").unwrap();
    database
        .grant_permission(
            "writer",
            Permission::Insert {
                table: "items".into(),
            },
        )
        .unwrap();
    for user in ["alice", "bob"] {
        database.grant_role(user, "writer").unwrap();
    }
    let app = build_app_full(database, std::iter::empty(), None, None, true);
    for (authorization, id) in [("Basic YWxpY2U6cHc=", 1), ("Basic Ym9iOnB3", 2)] {
        let response = app
            .clone()
            .oneshot(authenticated_request(
                json!({
                    "sql": format!("INSERT INTO items (id) VALUES ({id})"),
                    "idempotency_key": "shared-key",
                }),
                authorization,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["idempotency-replayed"], "false");
    }
    let replay = app
        .oneshot(authenticated_request(
            json!({
                "sql": "INSERT INTO items (id) VALUES (1)",
                "idempotency_key": "shared-key",
            }),
            "Basic YWxpY2U6cHc=",
        ))
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(replay.headers()["idempotency-replayed"], "true");
    let files = std::fs::read_dir(directory.path().join("_sql_idempotency"))
        .unwrap()
        .flatten()
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .ends_with(".receipt.json")
        })
        .count();
    assert_eq!(files, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_routers_execute_same_owner_key_once() {
    let (_directory, database) = database();
    let check_database = Arc::clone(&database);
    let first_app = build_app(Arc::clone(&database));
    let second_app = build_app(database);
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let first_barrier = Arc::clone(&barrier);
    let first = tokio::spawn(async move {
        first_barrier.wait().await;
        first_app
            .oneshot(request(json!({
                "sql": "INSERT INTO items (id) VALUES (77)",
                "query_id": "77777777777777777777777777777771",
                "idempotency_key": "cross-router-key",
            })))
            .await
            .unwrap()
    });
    let second_barrier = Arc::clone(&barrier);
    let second = tokio::spawn(async move {
        second_barrier.wait().await;
        second_app
            .oneshot(request(json!({
                "sql": "INSERT INTO items (id) VALUES (77)",
                "query_id": "77777777777777777777777777777772",
                "idempotency_key": "cross-router-key",
            })))
            .await
            .unwrap()
    });
    barrier.wait().await;
    let responses = [first.await.unwrap(), second.await.unwrap()];
    assert!(responses
        .iter()
        .all(|response| response.status() == StatusCode::OK));
    assert_eq!(
        responses
            .iter()
            .filter(|response| response.headers()["idempotency-replayed"] == "false")
            .count(),
        1
    );
    assert_eq!(
        responses
            .iter()
            .filter(|response| response.headers()["idempotency-replayed"] == "true")
            .count(),
        1
    );
    let check = build_app(check_database);
    assert_eq!(count(check).await, 1);
}

#[tokio::test]
async fn durable_intent_after_crash_returns_unknown_without_execution() {
    let (directory, database) = database();
    let owner = "anonymous";
    let key = "crash-key";
    let statement = "INSERT INTO items (id) VALUES (1)";
    let mut scope = Sha256::new();
    scope.update(b"mongreldb-sql-idempotency-v2\0");
    scope.update((owner.len() as u64).to_le_bytes());
    scope.update(owner.as_bytes());
    scope.update((key.len() as u64).to_le_bytes());
    scope.update(key.as_bytes());
    let scope_hash: [u8; 32] = scope.finalize().into();
    let hex_scope: String = scope_hash
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let hash = |bytes: &[u8]| -> [u8; 32] { Sha256::digest(bytes).into() };
    let request_semantics = serde_json::to_vec(&json!({
        "format": "json",
        "max_output_rows": 1,
        "max_output_bytes": 1024,
    }))
    .unwrap();
    let directory_path = directory.path().join("_sql_idempotency");
    std::fs::create_dir_all(&directory_path).unwrap();
    std::fs::write(
        directory_path.join(format!("{hex_scope}.intent.json")),
        serde_json::to_vec(&json!({
            "version": 4,
            "scope_hash": scope_hash,
            "owner_hash": hash(owner.as_bytes()),
            "created_at_ms": 1,
            "binding": {
                "sql_fingerprint": mongreldb_query::normalized_sql_fingerprint(statement),
                "parameter_hash": hash(b"[]"),
                "request_semantics_hash": hash(&request_semantics),
                "session_semantics_hash": hash(b"ephemeral"),
                "expires_after_ms": 86_400_000,
            }
        }))
        .unwrap(),
    )
    .unwrap();

    let app = build_app(database);
    let response = app
        .clone()
        .oneshot(request(json!({
            "sql": statement,
            "idempotency_key": key,
            "max_output_rows": 1,
            "max_output_bytes": 1024,
        })))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let response = json_body(response).await;
    assert_eq!(response["status"], "outcome_unknown");
    assert_eq!(response["committed"], Value::Null);
    assert_eq!(response["retryable"], false);
    assert_eq!(response["error"]["code"], "QUERY_OUTCOME_UNKNOWN");
    let status = app
        .clone()
        .oneshot(status_request(response["query_id"].as_str().unwrap()))
        .await
        .unwrap();
    assert_eq!(status.status(), StatusCode::OK);
    let status = json_body(status).await;
    assert_eq!(status["status"], "outcome_unknown");
    assert_eq!(status["committed"], Value::Null);
    assert_eq!(status["outcome"]["committed"], Value::Null);
    assert_eq!(status["terminal_error"]["code"], "QUERY_OUTCOME_UNKNOWN");
    assert_eq!(count(app).await, 0);
}
