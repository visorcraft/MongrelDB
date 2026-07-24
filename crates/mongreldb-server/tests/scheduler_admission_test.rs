#![cfg(feature = "cluster")] // scheduler parent admission on the SQL path is cluster-gated
//! S1E-002 / S4A: hierarchical scheduler admission on the SQL path.
//!
//! Under an artificially tiny InteractiveSql max_queue, excess concurrent
//! SQL requests receive ResourceExhausted (category code 18), not hang.

use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::Database;
use mongreldb_server::{build_app_with_sessions, SessionStore};
use serde_json::{json, Value};
use std::sync::Arc;
use std::sync::Once;
use tempfile::tempdir;
use tempfile::TempDir;

fn env_scheduler_tiny() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // Outer node cap high enough that the class queue is the bottleneck.
        std::env::set_var("MONGRELDB_SQL_MAX_CONCURRENT", "32");
        std::env::set_var("MONGRELDB_SCHEDULER_INTERACTIVE_SQL_MAX_QUEUE", "1");
        std::env::set_var("MONGRELDB_SCHEDULER_INTERACTIVE_SQL_MAX_CONCURRENCY", "1");
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
            embedding_source: None,
        }],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

async fn setup() -> (TempDir, Arc<SessionStore>, std::net::SocketAddr) {
    env_scheduler_tiny();
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

/// Hold one InteractiveSql concurrency slot in Planning; fill the single
/// queue slot; the next SQL request must fail closed with ResourceExhausted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_queue_full_returns_resource_exhausted() {
    let (_dir, store, addr) = setup().await;
    let client = reqwest::Client::new();
    let holder_session = open_session(&client, &addr).await;
    let queued_session = open_session(&client, &addr).await;
    let overflow_session = open_session(&client, &addr).await;

    let parked = Arc::new(tokio::sync::Notify::new());
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
    let holder = store.get(&holder_session, "anonymous").unwrap();
    let parked_hook = Arc::clone(&parked);
    let release_hook = Arc::clone(&release_rx);
    holder.session().set_test_hook(Some(Arc::new(move |point| {
        if point == mongreldb_query::SqlTestHookPoint::Planning {
            parked_hook.notify_one();
            let _ = release_hook.lock().unwrap().recv();
        }
    })));

    let holder_client = client.clone();
    let holder_req = tokio::spawn(async move {
        holder_client
            .post(format!("http://{addr}/sql"))
            .header("X-Session-ID", &holder_session)
            .json(&json!({
                "sql": "SELECT id FROM items",
                "timeout_ms": 30_000,
            }))
            .send()
            .await
            .unwrap()
    });

    tokio::time::timeout(std::time::Duration::from_secs(30), parked.notified())
        .await
        .expect("holder must reach Planning with admission held");

    // Second request enqueues behind the holder (max_queue=1).
    let enqueued = Arc::new(tokio::sync::Notify::new());
    let queued_holder = store.get(&queued_session, "anonymous").unwrap();
    let enqueued_hook = Arc::clone(&enqueued);
    queued_holder
        .session()
        .set_test_hook(Some(Arc::new(move |point| {
            if point == mongreldb_query::SqlTestHookPoint::WaitingForSqlPermit {
                enqueued_hook.notify_one();
            }
        })));
    let queued_client = client.clone();
    let queued_req = tokio::spawn(async move {
        queued_client
            .post(format!("http://{addr}/sql"))
            .header("X-Session-ID", &queued_session)
            .json(&json!({
                "sql": "SELECT 1",
                "timeout_ms": 30_000,
            }))
            .send()
            .await
            .unwrap()
    });

    tokio::time::timeout(std::time::Duration::from_secs(30), enqueued.notified())
        .await
        .expect("queued request must reach WaitingForSqlPermit");
    // Give the second request time to pass the outer semaphore and enter the
    // hierarchical queue (WaitingForSqlPermit fires before both).
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Third request: queue is full → ResourceExhausted, not hang.
    let overflow = client
        .post(format!("http://{addr}/sql"))
        .header("X-Session-ID", &overflow_session)
        .json(&json!({
            "sql": "SELECT 1",
            "timeout_ms": 5_000,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        overflow.status().as_u16(),
        503,
        "overflow SQL must be rejected with 503"
    );
    let body: Value = overflow.json().await.unwrap();
    let error = body.get("error").expect("structured error object");
    assert_eq!(
        error.get("category").and_then(|v| v.as_str()),
        Some("resource exhausted"),
        "body={body}"
    );
    assert_eq!(
        error.get("category_code").and_then(|v| v.as_u64()),
        Some(18),
        "body={body}"
    );

    // Release holder so the queue drains cleanly.
    drop(release_tx);
    let holder_status = holder_req.await.unwrap().status();
    assert!(holder_status.is_success() || holder_status.as_u16() == 200);
    let _ = queued_req.await;
}
