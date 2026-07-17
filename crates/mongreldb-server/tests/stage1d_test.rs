//! Stage 1D server integration tests (spec section 10.4):
//!
//! - S1D-004: the canonical session record (principal, database, transaction
//!   state, prepared statements, settings, read-your-writes token, last
//!   activity) tracked by the daemon's session store.
//! - S1D-005: prepared statements bind SQL text + parameter types + catalog
//!   version + schema versions; incompatible catalog/schema changes
//!   invalidate + replan; a stale plan never executes silently
//!   (`SchemaVersionMismatch` when replanning is impossible).
//! - S1D-006: queue wait and response serialization count toward the request
//!   deadline (injected delays via the existing SQL test hooks).
//! - S1D-007: the request-bytes bound rejects over-limit bodies with a
//!   structured 4xx.
//! - Structured errors carry the stable `ErrorCategory` taxonomy
//!   (`category` + `category_code`).

use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::Database;
use mongreldb_protocol::request::AuthenticatedIdentity;
use mongreldb_protocol::session::TransactionState;
use mongreldb_server::{build_app_with_sessions, SessionStore};
use serde_json::{json, Value};
use std::sync::Arc;
use tempfile::tempdir;
use tempfile::TempDir;

/// File-wide server knob: one in-flight SQL execution so the queue-wait
/// deadline test can occupy the single permit deterministically. Set once
/// before any app in this file is built; every test here issues strictly
/// sequential SQL except the queue test, which wants exactly this.
fn env_defaults() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        std::env::set_var("MONGRELDB_SQL_MAX_CONCURRENT", "1");
    });
}

fn items_schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![ColumnDef {
            id: 1,
            name: "id".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            default_value: None,
        }],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

/// Spin up a daemon over a fresh DB with an `items(id int64 pk)` table and an
/// externally-owned session store (so tests can read the canonical session
/// record). Returns the TempDir (must stay alive), the store, and the bound
/// address.
async fn setup() -> (TempDir, Arc<SessionStore>, std::net::SocketAddr) {
    env_defaults();
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("items", items_schema()).unwrap();
    let sessions = Arc::new(SessionStore::new(64, std::time::Duration::from_secs(300)));
    let app = build_app_with_sessions(
        db,
        std::iter::empty::<Arc<dyn mongreldb_query::ExternalTableModule>>(),
        None,
        None,
        false,
        Arc::clone(&sessions),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (dir, sessions, addr)
}

async fn open_session(client: &reqwest::Client, addr: &std::net::SocketAddr) -> String {
    let response = client
        .post(format!("http://{addr}/sessions"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    response
        .json::<Value>()
        .await
        .unwrap()
        .get("session_id")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string()
}

async fn sql(
    client: &reqwest::Client,
    addr: &std::net::SocketAddr,
    session: Option<&str>,
    body: Value,
) -> (reqwest::StatusCode, Value) {
    let mut request = client.post(format!("http://{addr}/sql")).json(&body);
    if let Some(session) = session {
        request = request.header("X-Session-ID", session);
    }
    let response = request.send().await.unwrap();
    let status = response.status();
    let body = response.json::<Value>().await.unwrap_or(Value::Null);
    (status, body)
}

fn record(store: &SessionStore, token: &str) -> mongreldb_protocol::session::Session {
    store
        .get(token, "anonymous")
        .expect("session must be live")
        .protocol_record()
}

#[tokio::test]
async fn session_record_tracks_the_s1d_004_model() {
    let (_dir, store, addr) = setup().await;
    let client = reqwest::Client::new();
    let token = open_session(&client, &addr).await;

    let created = record(&store, &token);
    assert_eq!(created.session_id.to_string(), token);
    assert_eq!(created.principal, AuthenticatedIdentity::Credentialless);
    assert_eq!(created.current_database, store.database_id());
    assert_eq!(created.transaction_state, TransactionState::Idle);
    assert!(created.prepared_statements.is_empty());
    assert!(created.settings.is_empty());
    assert_eq!(created.read_your_writes_token, None);
    assert!(created.last_activity_unix_micros > 0);

    // BEGIN opens the session's transaction staging → Active on the record.
    let (status, _) = sql(&client, &addr, Some(&token), json!({ "sql": "BEGIN" })).await;
    assert_eq!(status, 200);
    let active = record(&store, &token);
    assert!(
        active.transaction_state.is_active(),
        "an open transaction must be reflected on the session record"
    );

    // A staged insert keeps the transaction open; COMMIT closes it and
    // advances the read-your-writes token.
    let (status, _) = sql(
        &client,
        &addr,
        Some(&token),
        json!({ "sql": "INSERT INTO items (id) VALUES (1)" }),
    )
    .await;
    assert_eq!(status, 200);
    assert!(record(&store, &token).transaction_state.is_active());

    let (status, _) = sql(&client, &addr, Some(&token), json!({ "sql": "COMMIT" })).await;
    assert_eq!(status, 200);
    let committed = record(&store, &token);
    assert_eq!(committed.transaction_state, TransactionState::Idle);
    assert!(
        committed.read_your_writes_token.is_some(),
        "a durable commit must advance the read-your-writes token"
    );
    assert!(
        committed.last_activity_unix_micros >= active.last_activity_unix_micros,
        "last activity must move forward"
    );
}

#[tokio::test]
async fn prepared_statement_replans_on_schema_change_and_never_runs_stale() {
    let (_dir, store, addr) = setup().await;
    let client = reqwest::Client::new();
    let token = open_session(&client, &addr).await;

    // Prepare through the registry-tracked endpoint.
    let response = client
        .post(format!("http://{addr}/sessions/{token}/prepare"))
        .json(&json!({ "name": "q", "sql": "SELECT * FROM items" }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let prepared_body = response.json::<Value>().await.unwrap();
    let statement_id = prepared_body.get("statement_id").unwrap().as_u64().unwrap();
    assert!(statement_id >= 1);

    let binding_before = record(&store, &token)
        .prepared_statements
        .values()
        .next()
        .cloned()
        .expect("prepare must record a binding on the session record");
    assert_eq!(binding_before.statement_id.get(), statement_id);
    assert_eq!(binding_before.sql, "SELECT * FROM items");
    assert!(binding_before.parameter_types.is_empty());

    let (status, _) = sql(
        &client,
        &addr,
        Some(&token),
        json!({ "sql": "INSERT INTO items (id) VALUES (7)" }),
    )
    .await;
    assert_eq!(status, 200);

    // Execute once: the cached plan serves the original column name.
    let response = client
        .post(format!("http://{addr}/sessions/{token}/execute"))
        .json(&json!({ "name": "q", "params": [] }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let rows = response.json::<Value>().await.unwrap();
    assert!(rows.as_array().unwrap()[0].get("id").is_some());

    // Incompatible schema change through a *different* connection: rename the
    // column the plan expanded at prepare time.
    let (status, _) = sql(
        &client,
        &addr,
        None,
        json!({ "sql": "ALTER TABLE items RENAME COLUMN id TO item_id" }),
    )
    .await;
    assert_eq!(status, 200);

    // Execute again: the binding detects the catalog change, invalidates, and
    // replans — the result reflects the NEW schema (no stale plan).
    let response = client
        .post(format!("http://{addr}/sessions/{token}/execute"))
        .json(&json!({ "name": "q", "params": [] }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let rows = response.json::<Value>().await.unwrap();
    let first = &rows.as_array().unwrap()[0];
    assert!(
        first.get("item_id").is_some(),
        "replanned execution must serve the renamed column: {rows}"
    );
    assert!(
        first.get("id").is_none(),
        "a stale plan would serve `id`: {rows}"
    );

    let binding_after = record(&store, &token)
        .prepared_statements
        .values()
        .next()
        .cloned()
        .unwrap();
    assert!(
        binding_after.catalog_version != binding_before.catalog_version,
        "replanning must rebind to the new catalog version"
    );
}

#[tokio::test]
async fn prepared_statement_reports_schema_version_mismatch_when_unplannable() {
    let (_dir, store, addr) = setup().await;
    let client = reqwest::Client::new();
    let token = open_session(&client, &addr).await;

    let response = client
        .post(format!("http://{addr}/sessions/{token}/prepare"))
        .json(&json!({ "name": "q", "sql": "SELECT id FROM items" }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    // Drop the table through another connection: the statement can no longer
    // be planned at all.
    let (status, _) = sql(&client, &addr, None, json!({ "sql": "DROP TABLE items" })).await;
    assert_eq!(status, 200);

    let response = client
        .post(format!("http://{addr}/sessions/{token}/execute"))
        .json(&json!({ "name": "q", "params": [] }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 409);
    let body = response.json::<Value>().await.unwrap();
    let error = body.get("error").unwrap();
    assert_eq!(error.get("code").unwrap(), "SCHEMA_VERSION_MISMATCH");
    assert_eq!(error.get("category").unwrap(), "schema version mismatch");
    assert_eq!(error.get("category_code").unwrap(), 16);

    // The invalidated binding is gone; the session record stays consistent.
    assert!(
        record(&store, &token).prepared_statements.is_empty(),
        "an unplannable statement must be invalidated, not retained"
    );
}

#[tokio::test]
async fn prepared_statement_parameter_contract_is_enforced() {
    let (_dir, _store, addr) = setup().await;
    let client = reqwest::Client::new();
    let token = open_session(&client, &addr).await;
    let (status, _) = sql(
        &client,
        &addr,
        Some(&token),
        json!({ "sql": "INSERT INTO items (id) VALUES (5)" }),
    )
    .await;
    assert_eq!(status, 200);

    // Unknown declared parameter types are rejected at prepare time.
    let response = client
        .post(format!("http://{addr}/sessions/{token}/prepare"))
        .json(&json!({ "name": "bad", "sql": "SELECT 1", "param_types": ["DATE"] }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);

    let response = client
        .post(format!("http://{addr}/sessions/{token}/prepare"))
        .json(&json!({
            "name": "by_id",
            "sql": "SELECT id FROM items WHERE id = $1",
            "param_types": ["INT64"],
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    // A mismatched parameter list fails rather than coercing silently.
    let response = client
        .post(format!("http://{addr}/sessions/{token}/execute"))
        .json(&json!({ "name": "by_id", "params": ["5"] }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    let body = response.json::<Value>().await.unwrap();
    assert_eq!(
        body.pointer("/error/code").unwrap(),
        "PREPARED_PARAMETER_MISMATCH"
    );

    // The declared list executes.
    let response = client
        .post(format!("http://{addr}/sessions/{token}/execute"))
        .json(&json!({ "name": "by_id", "params": [5] }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let rows = response.json::<Value>().await.unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 1);

    // Deallocate drops the binding; executing afterwards is a plain 404.
    let response = client
        .delete(format!("http://{addr}/sessions/{token}/statements/by_id"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let response = client
        .post(format!("http://{addr}/sessions/{token}/execute"))
        .json(&json!({ "name": "by_id", "params": [5] }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 404);
}

#[tokio::test]
async fn request_body_over_limit_is_rejected_with_structured_413() {
    let (_dir, _store, addr) = setup().await;
    let client = reqwest::Client::new();
    // Default request-bytes bound: 2 MiB. Exceed it with a comment tail.
    let huge = format!("SELECT 1 /* {} */", "x".repeat(3 * 1024 * 1024));
    let response = client
        .post(format!("http://{addr}/sql"))
        .json(&json!({ "sql": huge }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 413);
    let body = response.json::<Value>().await.unwrap();
    let error = body.get("error").unwrap();
    assert_eq!(error.get("code").unwrap(), "REQUEST_BODY_TOO_LARGE");
    assert_eq!(error.get("category").unwrap(), "resource exhausted");
    assert_eq!(error.get("category_code").unwrap(), 18);

    // The same statement within the bound is served normally.
    let (status, _) = sql(&client, &addr, None, json!({ "sql": "SELECT 1" })).await;
    assert_eq!(status, 200);
}

// Multi-thread runtime: the planning hook blocks one worker thread on the
// release latch; the queued request must be scheduled on another.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deadline_counts_queue_wait() {
    let (_dir, store, addr) = setup().await;
    let client = reqwest::Client::new();
    let slow_session = open_session(&client, &addr).await;
    let queued_session = open_session(&client, &addr).await;

    // Occupy the single SQL admission permit (MONGRELDB_SQL_MAX_CONCURRENT=1)
    // with an arrow-stream request parked in its planning hook; the streaming
    // path holds the server permit from admission through serialization. The
    // hook signals `parked` from inside Planning, then holds the permit on a
    // latch until the test releases it: no fixed sleeps, so the park can never
    // elapse before the queued request is dispatched (the old 500 ms sleep
    // raced the poll loop under load and flaked).
    let parked = Arc::new(tokio::sync::Notify::new());
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
    let holder = store.get(&slow_session, "anonymous").unwrap();
    let parked_hook = Arc::clone(&parked);
    let release_hook = Arc::clone(&release_rx);
    holder.session().set_test_hook(Some(Arc::new(move |point| {
        if point == mongreldb_query::SqlTestHookPoint::Planning {
            parked_hook.notify_one();
            // Parked inside Planning with the admission permit held until the
            // test releases the latch. A dropped sender (test panic) releases
            // too, so the server is never wedged.
            let _ = release_hook.lock().unwrap().recv();
        }
    })));

    let slow_client = client.clone();
    let slow = tokio::spawn(async move {
        slow_client
            .post(format!("http://{addr}/sql"))
            .header("X-Session-ID", &slow_session)
            .json(&json!({
                "sql": "SELECT id FROM items",
                "format": "arrow-stream",
                "timeout_ms": 10_000,
            }))
            .send()
            .await
            .unwrap()
    });

    // Wait until the first query holds the permit (deterministic: the hook
    // only fires once planning is entered with the permit already held). The
    // timeout is a failure bound, never part of the synchronization.
    tokio::time::timeout(std::time::Duration::from_secs(30), parked.notified())
        .await
        .expect("slow request must reach its planning hook");

    // Queue behind it with a deadline shorter than the park: the wait must
    // count (S1D-006). The queued request announces itself at the admission
    // queue through its own session hook just before blocking on the
    // semaphore, so the test observes it queued rather than assuming it.
    let queued = Arc::new(tokio::sync::Notify::new());
    let queued_holder = store.get(&queued_session, "anonymous").unwrap();
    let queued_hook = Arc::clone(&queued);
    queued_holder
        .session()
        .set_test_hook(Some(Arc::new(move |point| {
            if point == mongreldb_query::SqlTestHookPoint::WaitingForSqlPermit {
                queued_hook.notify_one();
            }
        })));
    let queued_request = tokio::spawn(async move {
        sql(
            &client,
            &addr,
            Some(&queued_session),
            json!({ "sql": "SELECT id FROM items", "timeout_ms": 100 }),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(30), queued.notified())
        .await
        .expect("queued request must reach the admission queue");

    // The queued request's deadline expires while it waits for the permit the
    // parked request still holds; only after that terminal response arrives is
    // the park released.
    let (status, body) = queued_request.await.unwrap();
    assert_eq!(status, 504, "queue wait must expire the deadline: {body}");
    assert_eq!(body.pointer("/error/code").unwrap(), "DEADLINE_EXCEEDED");
    assert_eq!(
        body.pointer("/error/category").unwrap(),
        "deadline exceeded"
    );
    assert_eq!(body.pointer("/error/category_code").unwrap(), 10);

    release_tx.send(()).unwrap();
    let slow_response = slow.await.unwrap();
    assert_eq!(slow_response.status(), 200);
}

#[tokio::test]
async fn deadline_counts_response_serialization() {
    let (_dir, store, addr) = setup().await;
    let client = reqwest::Client::new();
    let token = open_session(&client, &addr).await;
    let (status, _) = sql(
        &client,
        &addr,
        Some(&token),
        json!({ "sql": "INSERT INTO items (id) VALUES (1)" }),
    )
    .await;
    assert_eq!(status, 200);

    // Stall the buffered-response serializer past the request deadline; the
    // per-chunk cancellation checkpoint must surface DEADLINE_EXCEEDED.
    let entry = store.get(&token, "anonymous").unwrap();
    entry.session().set_test_hook(Some(Arc::new(move |point| {
        if point == mongreldb_query::SqlTestHookPoint::BeforeSerializationBatch {
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
    })));
    let (status, body) = sql(
        &client,
        &addr,
        Some(&token),
        json!({ "sql": "SELECT id FROM items", "timeout_ms": 100 }),
    )
    .await;
    assert_eq!(
        status, 504,
        "serialization time must count toward the deadline: {body}"
    );
    assert_eq!(body.pointer("/error/code").unwrap(), "DEADLINE_EXCEEDED");
    assert_eq!(body.pointer("/error/category_code").unwrap(), 10);
}

#[tokio::test]
async fn query_errors_carry_the_error_taxonomy() {
    let (_dir, _store, addr) = setup().await;
    let client = reqwest::Client::new();
    for id in 1..=3 {
        let (status, _) = sql(
            &client,
            &addr,
            None,
            json!({ "sql": format!("INSERT INTO items (id) VALUES ({id})") }),
        )
        .await;
        assert_eq!(status, 200);
    }

    // Result-limit breach → ResourceExhausted on the structured error object.
    let (status, body) = sql(
        &client,
        &addr,
        None,
        json!({ "sql": "SELECT id FROM items", "max_output_rows": 1 }),
    )
    .await;
    assert_eq!(status, 413);
    let error = body.get("error").unwrap();
    assert_eq!(error.get("code").unwrap(), "RESULT_LIMIT_EXCEEDED");
    assert_eq!(error.get("category").unwrap(), "resource exhausted");
    assert_eq!(error.get("category_code").unwrap(), 18);

    // Unknown table → the core Schema/NotFound mapping surfaces a category.
    let (status, body) = sql(
        &client,
        &addr,
        None,
        json!({ "sql": "SELECT id FROM missing_table" }),
    )
    .await;
    assert!(status.is_client_error() || status.is_server_error());
    assert!(
        body.pointer("/error/category_code").is_some(),
        "every structured query error must carry the taxonomy: {body}"
    );
}
