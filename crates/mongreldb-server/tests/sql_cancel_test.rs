use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use mongreldb_core::{ColumnDef, ColumnFlags, Database, Schema, TypeId};
use mongreldb_query::SqlTestHookPoint;
use mongreldb_server::{
    build_app, build_app_with_sessions, build_app_with_sessions_and_control, SessionStore,
};
use serde_json::{json, Value};
use std::sync::{Arc, Barrier};
use std::time::Duration;
use tempfile::tempdir;
use tower::ServiceExt;

fn request(method: &str, path: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn sql_returns_supplied_and_generated_query_ids() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let app = build_app(db);
    let supplied = "00112233445566778899aabbccddeeff";
    let response = app
        .clone()
        .oneshot(request(
            "POST",
            "/sql",
            json!({ "sql": "SELECT 1", "query_id": supplied }),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-mongreldb-query-id"], supplied);

    let status = app
        .clone()
        .oneshot(request("GET", &format!("/queries/{supplied}"), Value::Null))
        .await
        .unwrap();
    assert_eq!(status.status(), StatusCode::OK);
    let status = json_body(status).await;
    assert_eq!(status["state"], "completed");
    assert!(status["trace"]["queue_duration_us"].is_number());
    assert!(status["trace"]["planning_duration_us"].is_number());
    assert!(status["trace"]["execution_duration_us"].is_number());
    assert!(status["trace"]["serialization_duration_us"].is_number());
    assert_eq!(status["trace"]["commit_fence_outcome"], "not_reached");
    assert!(status.get("sql").is_none());

    let generated = app
        .oneshot(request("POST", "/sql", json!({ "sql": "SELECT 1" })))
        .await
        .unwrap();
    assert_eq!(generated.status(), StatusCode::OK);
    let generated_id = generated.headers()["x-mongreldb-query-id"]
        .to_str()
        .unwrap();
    assert_eq!(generated_id.len(), 32);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn active_query_can_be_inspected_cancelled_and_duplicate_id_is_rejected() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let sessions = Arc::new(SessionStore::new(8, Duration::from_secs(60)));
    let app = build_app_with_sessions(
        db,
        std::iter::empty(),
        None,
        None,
        false,
        Arc::clone(&sessions),
    );

    let opened = app
        .clone()
        .oneshot(request("POST", "/sessions", Value::Null))
        .await
        .unwrap();
    let session_id = json_body(opened).await["session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let entry = sessions.get(&session_id, "anonymous").unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let hook_barrier = Arc::clone(&barrier);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    entry.session.set_test_hook(Some(Arc::new(move |point| {
        if point == SqlTestHookPoint::Planning {
            let _ = entered_tx.send(());
            hook_barrier.wait();
        }
    })));

    let query_id = "ffeeddccbbaa99887766554433221100";
    let mut sql_request = request(
        "POST",
        "/sql",
        json!({ "sql": "SELECT 1", "query_id": query_id }),
    );
    sql_request
        .headers_mut()
        .insert("x-session-id", session_id.parse().unwrap());
    let sql_task = tokio::spawn(app.clone().oneshot(sql_request));
    tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
        .await
        .unwrap();

    let status = app
        .clone()
        .oneshot(request("GET", &format!("/queries/{query_id}"), Value::Null))
        .await
        .unwrap();
    assert_eq!(status.status(), StatusCode::OK);
    assert_eq!(json_body(status).await["state"], "planning");

    let mut duplicate = request(
        "POST",
        "/sql",
        json!({ "sql": "SELECT 2", "query_id": query_id }),
    );
    duplicate
        .headers_mut()
        .insert("x-session-id", session_id.parse().unwrap());
    let duplicate = app.clone().oneshot(duplicate).await.unwrap();
    assert_eq!(duplicate.status(), StatusCode::CONFLICT);
    assert_eq!(
        json_body(duplicate).await["error"]["code"],
        "QUERY_ID_CONFLICT"
    );

    let cancelled = app
        .oneshot(request(
            "POST",
            &format!("/queries/{query_id}/cancel"),
            Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(cancelled.status(), StatusCode::ACCEPTED);
    barrier.wait();

    let response = sql_task.await.unwrap().unwrap();
    assert_eq!(response.status().as_u16(), 499);
    assert_eq!(
        json_body(response).await["error"]["code"],
        "QUERY_CANCELLED"
    );
}

#[tokio::test]
async fn timeout_validation_and_header_precedence_are_stable() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let app = build_app(db);

    let invalid = app
        .clone()
        .oneshot(request(
            "POST",
            "/sql",
            json!({ "sql": "SELECT 1", "timeout_ms": 0 }),
        ))
        .await
        .unwrap();
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);

    let body_id = "0123456789abcdef0123456789abcdef";
    let mut body_wins = request(
        "POST",
        "/sql",
        json!({ "sql": "SELECT 1", "query_id": body_id, "timeout_ms": 1000 }),
    );
    body_wins.headers_mut().insert(
        "x-mongreldb-query-id",
        "ffffffffffffffffffffffffffffffff".parse().unwrap(),
    );
    body_wins
        .headers_mut()
        .insert("x-mongreldb-timeout-ms", "0".parse().unwrap());
    let response = app.oneshot(body_wins).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-mongreldb-query-id"], body_id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn buffered_serialization_is_cancellable() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let sessions = Arc::new(SessionStore::new(8, Duration::from_secs(60)));
    let app = build_app_with_sessions(
        db,
        std::iter::empty(),
        None,
        None,
        false,
        Arc::clone(&sessions),
    );
    let opened = app
        .clone()
        .oneshot(request("POST", "/sessions", Value::Null))
        .await
        .unwrap();
    let session_id = json_body(opened).await["session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let entry = sessions.get(&session_id, "anonymous").unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let hook_barrier = Arc::clone(&barrier);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    entry.session.set_test_hook(Some(Arc::new(move |point| {
        if point == SqlTestHookPoint::BeforeSerializationBatch {
            let _ = entered_tx.send(());
            hook_barrier.wait();
        }
    })));

    let query_id = "11112222333344445555666677778888";
    let mut sql_request = request(
        "POST",
        "/sql",
        json!({ "sql": "SELECT 1", "query_id": query_id }),
    );
    sql_request
        .headers_mut()
        .insert("x-session-id", session_id.parse().unwrap());
    let sql_task = tokio::spawn(app.clone().oneshot(sql_request));
    tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
        .await
        .unwrap();

    let status = app
        .clone()
        .oneshot(request("GET", &format!("/queries/{query_id}"), Value::Null))
        .await
        .unwrap();
    assert_eq!(json_body(status).await["state"], "serializing");
    let cancelled = app
        .oneshot(request(
            "POST",
            &format!("/queries/{query_id}/cancel"),
            Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(cancelled.status(), StatusCode::ACCEPTED);
    barrier.wait();
    let response = sql_task.await.unwrap().unwrap();
    assert_eq!(response.status().as_u16(), 499);
    assert_eq!(
        json_body(response).await["error"]["code"],
        "QUERY_CANCELLED"
    );
}

#[tokio::test]
async fn dropping_arrow_stream_cancels_and_cleans_registry_entry() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let app = build_app(db);
    let query_id = "9999aaaabbbbccccddddeeeeffff0000";
    let response = app
        .clone()
        .oneshot(request(
            "POST",
            "/sql",
            json!({
                "sql": "SELECT 1",
                "format": "arrow-stream",
                "query_id": query_id
            }),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    drop(response);

    let status = app
        .oneshot(request("GET", &format!("/queries/{query_id}"), Value::Null))
        .await
        .unwrap();
    assert_eq!(status.status(), StatusCode::OK);
    assert_eq!(json_body(status).await["state"], "cancelled");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closing_session_cancels_active_query_without_session_lock() {
    let directory = tempdir().unwrap();
    let database = Arc::new(Database::create(directory.path()).unwrap());
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
        .oneshot(request("POST", "/sessions", Value::Null))
        .await
        .unwrap();
    let session_id = json_body(opened).await["session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let entry = sessions.get(&session_id, "anonymous").unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let hook_barrier = Arc::clone(&barrier);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    entry.session.set_test_hook(Some(Arc::new(move |point| {
        if point == SqlTestHookPoint::Planning {
            entered_tx.send(()).unwrap();
            hook_barrier.wait();
        }
    })));

    let query_id = "1234567890abcdef1234567890abcdef";
    let mut sql_request = request(
        "POST",
        "/sql",
        json!({ "sql": "SELECT 1", "query_id": query_id }),
    );
    sql_request
        .headers_mut()
        .insert("x-session-id", session_id.parse().unwrap());
    let sql_task = tokio::spawn(app.clone().oneshot(sql_request));
    tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
        .await
        .unwrap();

    let close_task = tokio::spawn(
        app.clone()
            .oneshot(request("DELETE", &format!("/sessions/{session_id}"), Value::Null)),
    );
    loop {
        let status = app
            .clone()
            .oneshot(request("GET", &format!("/queries/{query_id}"), Value::Null))
            .await
            .unwrap();
        let phase = json_body(status).await["state"].as_str().unwrap().to_owned();
        if phase == "cancelling" {
            break;
        }
        tokio::task::yield_now().await;
    }
    barrier.wait();
    assert_eq!(close_task.await.unwrap().unwrap().status(), StatusCode::OK);
    let response = sql_task.await.unwrap().unwrap();
    assert_eq!(response.status().as_u16(), 499);
    assert_eq!(json_body(response).await["error"]["code"], "QUERY_CANCELLED");
    assert!(sessions.get(&session_id, "anonymous").is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_planning_and_execution_are_cancellable() {
    let directory = tempdir().unwrap();
    let database = Arc::new(Database::create(directory.path()).unwrap());
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
        .oneshot(request("POST", "/sessions", Value::Null))
        .await
        .unwrap();
    let session_id = json_body(opened).await["session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let entry = sessions.get(&session_id, "anonymous").unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let hook_barrier = Arc::clone(&barrier);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    entry.session.set_test_hook(Some(Arc::new(move |point| {
        if point == SqlTestHookPoint::Planning {
            entered_tx.send(()).unwrap();
            hook_barrier.wait();
        }
    })));
    let prepare_id = "aaaabbbbccccddddeeeeffff00001111";
    let prepare_task = tokio::spawn(app.clone().oneshot(request(
        "POST",
        &format!("/sessions/{session_id}/prepare"),
        json!({ "name": "one", "sql": "SELECT 1", "query_id": prepare_id }),
    )));
    tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
        .await
        .unwrap();
    assert_eq!(
        app.clone()
            .oneshot(request(
                "POST",
                &format!("/queries/{prepare_id}/cancel"),
                Value::Null,
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::ACCEPTED
    );
    barrier.wait();
    assert_eq!(prepare_task.await.unwrap().unwrap().status().as_u16(), 499);

    entry.session.set_test_hook(None);
    assert_eq!(
        app.clone()
            .oneshot(request(
                "POST",
                &format!("/sessions/{session_id}/prepare"),
                json!({ "name": "one", "sql": "SELECT 1" }),
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    let barrier = Arc::new(Barrier::new(2));
    let hook_barrier = Arc::clone(&barrier);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    entry.session.set_test_hook(Some(Arc::new(move |point| {
        if point == SqlTestHookPoint::BeforeSerializationBatch {
            entered_tx.send(()).unwrap();
            hook_barrier.wait();
        }
    })));
    let execute_id = "11110000ffffeeeeddddccccbbbbaaaa";
    let execute_task = tokio::spawn(app.clone().oneshot(request(
        "POST",
        &format!("/sessions/{session_id}/execute"),
        json!({ "name": "one", "params": [], "query_id": execute_id }),
    )));
    tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
        .await
        .unwrap();
    assert_eq!(
        app.oneshot(request(
            "POST",
            &format!("/queries/{execute_id}/cancel"),
            Value::Null,
        ))
        .await
        .unwrap()
        .status(),
        StatusCode::ACCEPTED
    );
    barrier.wait();
    assert_eq!(execute_task.await.unwrap().unwrap().status().as_u16(), 499);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_cancels_reads_and_rejects_new_sql() {
    let directory = tempdir().unwrap();
    let database = Arc::new(Database::create(directory.path()).unwrap());
    let sessions = Arc::new(SessionStore::new(8, Duration::from_secs(60)));
    let (app, control) = build_app_with_sessions_and_control(
        database,
        std::iter::empty(),
        None,
        None,
        false,
        Arc::clone(&sessions),
    );
    let opened = app
        .clone()
        .oneshot(request("POST", "/sessions", Value::Null))
        .await
        .unwrap();
    let session_id = json_body(opened).await["session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let entry = sessions.get(&session_id, "anonymous").unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let hook_barrier = Arc::clone(&barrier);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    entry.session.set_test_hook(Some(Arc::new(move |point| {
        if point == SqlTestHookPoint::Planning {
            entered_tx.send(()).unwrap();
            hook_barrier.wait();
        }
    })));

    let query_id = "12341234123412341234123412341234";
    let mut sql_request = request(
        "POST",
        "/sql",
        json!({ "sql": "SELECT 1", "query_id": query_id }),
    );
    sql_request
        .headers_mut()
        .insert("x-session-id", session_id.parse().unwrap());
    let sql_task = tokio::spawn(app.clone().oneshot(sql_request));
    tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
        .await
        .unwrap();
    let shutdown_task = tokio::spawn(async move { control.shutdown().await });
    loop {
        let status = app
            .clone()
            .oneshot(request("GET", &format!("/queries/{query_id}"), Value::Null))
            .await
            .unwrap();
        if json_body(status).await["state"] == "cancelling" {
            break;
        }
        tokio::task::yield_now().await;
    }
    barrier.wait();
    assert_eq!(shutdown_task.await.unwrap(), 0);
    assert_eq!(sql_task.await.unwrap().unwrap().status().as_u16(), 499);
    assert!(sessions.is_empty());
    assert_eq!(
        app.oneshot(request("POST", "/sql", json!({ "sql": "SELECT 1" })))
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_fence_returns_too_late_and_preserves_commit_after_response_drop() {
    let directory = tempdir().unwrap();
    let database = Arc::new(Database::create(directory.path()).unwrap());
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
                }],
                ..Schema::default()
            },
        )
        .unwrap();
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
        .oneshot(request("POST", "/sessions", Value::Null))
        .await
        .unwrap();
    let session_id = json_body(opened).await["session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let entry = sessions.get(&session_id, "anonymous").unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let hook_barrier = Arc::clone(&barrier);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    entry.session.set_test_hook(Some(Arc::new(move |point| {
        if point == SqlTestHookPoint::InsideCommitCritical {
            entered_tx.send(()).unwrap();
            hook_barrier.wait();
        }
    })));

    let query_id = "abcdabcdabcdabcdabcdabcdabcdabcd";
    let mut sql_request = request(
        "POST",
        "/sql",
        json!({ "sql": "INSERT INTO items (id) VALUES (1)", "query_id": query_id }),
    );
    sql_request
        .headers_mut()
        .insert("x-session-id", session_id.parse().unwrap());
    let sql_task = tokio::spawn(app.clone().oneshot(sql_request));
    tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
        .await
        .unwrap();

    let cancel = app
        .clone()
        .oneshot(request(
            "POST",
            &format!("/queries/{query_id}/cancel"),
            Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(cancel.status(), StatusCode::CONFLICT);
    assert_eq!(json_body(cancel).await["error"]["code"], "CANCEL_TOO_LATE");
    barrier.wait();
    let response = sql_task.await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    drop(response);

    let status = app
        .clone()
        .oneshot(request("GET", &format!("/queries/{query_id}"), Value::Null))
        .await
        .unwrap();
    let status = json_body(status).await;
    assert_eq!(status["state"], "completed");
    assert_eq!(status["committed"], true);
    assert_eq!(status["trace"]["commit_fence_outcome"], "commit_won");

    entry.session.set_test_hook(None);
    let mut count = request(
        "POST",
        "/sql",
        json!({ "sql": "SELECT count(*) AS n FROM items" }),
    );
    count
        .headers_mut()
        .insert("x-session-id", session_id.parse().unwrap());
    let count = app.oneshot(count).await.unwrap();
    assert_eq!(count.status(), StatusCode::OK);
    assert_eq!(json_body(count).await[0]["n"], 1);
}
