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

#[tokio::test]
async fn compact_status_preserves_durable_outcome_after_detailed_eviction() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table(
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
    let app = build_app(db);
    let finished_id = "ffffffffffffffffffffffffffffffff";
    assert_eq!(
        app.clone()
            .oneshot(request(
                "POST",
                "/sql",
                json!({ "sql": "INSERT INTO items (id) VALUES (1)", "query_id": finished_id }),
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let detailed = app
        .clone()
        .oneshot(request(
            "GET",
            &format!("/queries/{finished_id}"),
            Value::Null,
        ))
        .await
        .unwrap();
    let detailed = json_body(detailed).await;
    let commit_epoch = detailed["last_commit_epoch_text"].clone();
    assert_eq!(detailed["committed"], true);

    for value in 1..=2_048_u64 {
        let query_id = format!("{value:032x}");
        assert_eq!(
            app.clone()
                .oneshot(request(
                    "POST",
                    "/sql",
                    json!({ "sql": "SELECT 1", "query_id": query_id }),
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }

    let compact_status = app
        .clone()
        .oneshot(request(
            "GET",
            &format!("/queries/{finished_id}"),
            Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(compact_status.status(), StatusCode::OK);
    let compact_status = json_body(compact_status).await;
    assert_eq!(compact_status["detail"], "compact");
    assert_eq!(compact_status["state"], "completed");
    assert_eq!(compact_status["cancel_outcome"], "already_finished");
    assert_eq!(compact_status["committed"], true);
    assert_eq!(compact_status["last_commit_epoch_text"], commit_epoch);
    let cancel = app
        .oneshot(request(
            "POST",
            &format!("/queries/{finished_id}/cancel"),
            Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(cancel.status(), StatusCode::OK);
    let cancel = json_body(cancel).await;
    assert_eq!(cancel["cancel_outcome"], "already_finished");
    assert_eq!(cancel["code"], "QUERY_ALREADY_FINISHED");
    assert_eq!(cancel["committed"], true);
    assert_eq!(cancel["last_commit_epoch_text"], commit_epoch);
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
    entry.session().set_test_hook(Some(Arc::new(move |point| {
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
    let cancelled = json_body(cancelled).await;
    assert_eq!(cancelled["query_id"], query_id);
    assert_eq!(cancelled["status"], "running");
    assert_eq!(cancelled["server_state"], "cancelling");
    assert_eq!(cancelled["cancel_outcome"], "accepted");
    assert_eq!(cancelled["cancellation_reason"], "client_request");
    assert_eq!(cancelled["committed"], false);
    assert_eq!(cancelled["committed_statements"], 0);
    assert!(cancelled["last_commit_epoch"].is_null());
    assert!(cancelled["last_commit_epoch_text"].is_null());
    assert_eq!(cancelled["outcome"]["committed"], false);
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

#[tokio::test]
async fn per_request_output_limits_must_be_positive_and_clamp_to_server_limits() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let app = build_app(db);

    let invalid = app
        .clone()
        .oneshot(request(
            "POST",
            "/sql",
            json!({ "sql": "SELECT 1", "max_output_rows": 0 }),
        ))
        .await
        .unwrap();
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    let invalid = json_body(invalid).await;
    assert_eq!(invalid["status"], "failed_before_commit");
    assert_eq!(invalid["error"]["code"], "INVALID_QUERY_OPTIONS");

    let clamped = app
        .oneshot(request(
            "POST",
            "/sql",
            json!({
                "sql": "SELECT 1 AS value",
                "max_output_rows": u64::MAX,
                "max_output_bytes": u64::MAX,
            }),
        ))
        .await
        .unwrap();
    assert_eq!(clamped.status(), StatusCode::OK);
}

#[tokio::test]
async fn buffered_output_limit_preserves_and_reports_earlier_commits() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table(
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
    let app = build_app(Arc::clone(&db));
    let query_id = "45454545454545454545454545454545";

    let response = app
        .clone()
        .oneshot(request(
            "POST",
            "/sql",
            json!({
                "sql": "INSERT INTO items (id) VALUES (1); INSERT INTO items (id) VALUES (2); SELECT id FROM items ORDER BY id",
                "query_id": query_id,
                "max_output_rows": 1,
            }),
        ))
        .await
        .unwrap();
    let response_status = response.status();
    assert_eq!(response.headers()["x-mongreldb-query-id"], query_id);
    let body = json_body(response).await;
    assert_eq!(
        response_status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "response body: {body}"
    );
    assert_eq!(body["status"], "partially_committed");
    assert_eq!(body["error"]["code"], "RESULT_LIMIT_EXCEEDED");
    assert_eq!(body["outcome"]["committed"], true);
    assert_eq!(body["outcome"]["committed_statements"], 2);

    let status = app
        .clone()
        .oneshot(request("GET", &format!("/queries/{query_id}"), Value::Null))
        .await
        .unwrap();
    assert_eq!(status.status(), StatusCode::OK);
    let status = json_body(status).await;
    assert_eq!(status["status"], "partially_committed");
    assert_eq!(status["terminal_error"]["code"], "RESULT_LIMIT_EXCEEDED");
    assert_eq!(status["outcome"], body["outcome"]);

    let count = app
        .oneshot(request(
            "POST",
            "/sql",
            json!({ "sql": "SELECT count(*) AS n FROM items" }),
        ))
        .await
        .unwrap();
    assert_eq!(count.status(), StatusCode::OK);
    assert_eq!(json_body(count).await[0]["n"], 2);
}

#[tokio::test]
async fn arrow_stream_output_limits_are_visible_through_query_status() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table(
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
    let app = build_app(db);
    let setup = app
        .clone()
        .oneshot(request(
            "POST",
            "/sql",
            json!({
                "sql": "INSERT INTO items (id) VALUES (1); INSERT INTO items (id) VALUES (2)",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(setup.status(), StatusCode::OK);

    let row_limit_id = "56565656565656565656565656565656";
    let row_limited = app
        .clone()
        .oneshot(request(
            "POST",
            "/sql",
            json!({
                "sql": "SELECT id FROM items ORDER BY id",
                "format": "arrow-stream",
                "query_id": row_limit_id,
                "max_output_rows": 1,
            }),
        ))
        .await
        .unwrap();
    assert_eq!(row_limited.status(), StatusCode::OK);
    assert!(to_bytes(row_limited.into_body(), usize::MAX).await.is_err());
    let status = app
        .clone()
        .oneshot(request(
            "GET",
            &format!("/queries/{row_limit_id}"),
            Value::Null,
        ))
        .await
        .unwrap();
    let status = json_body(status).await;
    assert_eq!(status["status"], "failed_before_commit");
    assert_eq!(status["terminal_error"]["code"], "RESULT_LIMIT_EXCEEDED");
    assert_eq!(status["terminal_error"]["category"], "result_limit");

    let byte_limit_id = "67676767676767676767676767676767";
    let byte_limited = app
        .clone()
        .oneshot(request(
            "POST",
            "/sql",
            json!({
                "sql": "SELECT id FROM items ORDER BY id",
                "format": "arrow-stream",
                "query_id": byte_limit_id,
                "max_output_bytes": 1,
            }),
        ))
        .await
        .unwrap();
    assert_eq!(byte_limited.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let body = json_body(byte_limited).await;
    assert_eq!(body["error"]["code"], "RESULT_LIMIT_EXCEEDED");
    let status = app
        .oneshot(request(
            "GET",
            &format!("/queries/{byte_limit_id}"),
            Value::Null,
        ))
        .await
        .unwrap();
    let status = json_body(status).await;
    assert_eq!(status["terminal_error"]["code"], "RESULT_LIMIT_EXCEEDED");
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
    entry.session().set_test_hook(Some(Arc::new(move |point| {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paginated_json_deserialization_is_cancellable() {
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
    entry.session().set_test_hook(Some(Arc::new(move |point| {
        if point == SqlTestHookPoint::DuringPaginationDeserialization {
            let _ = entered_tx.send(());
            hook_barrier.wait();
        }
    })));

    let query_id = "12121212121212121212121212121212";
    let mut sql_request = request(
        "POST",
        "/sql",
        json!({
            "sql": "SELECT 1 AS value",
            "query_id": query_id,
            "pagination": {
                "page_size_rows": 1,
                "projection": ["value"]
            }
        }),
    );
    sql_request
        .headers_mut()
        .insert("x-session-id", session_id.parse().unwrap());
    let sql_task = tokio::spawn(app.clone().oneshot(sql_request));
    tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
        .await
        .unwrap();

    let cancelled = app
        .clone()
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
    let status = app
        .oneshot(request("GET", &format!("/queries/{query_id}"), Value::Null))
        .await
        .unwrap();
    let status = json_body(status).await;
    assert_eq!(status["state"], "cancelled");
    assert_eq!(status["outcome"]["serialization"], "failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_at_final_serialization_boundary_never_returns_clean_success() {
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

    for (query_id, body) in [
        (
            "10101010101010101010101010101010",
            json!({ "sql": "SELECT 1 AS value", "query_id": "10101010101010101010101010101010" }),
        ),
        (
            "20202020202020202020202020202020",
            json!({
                "sql": "SELECT 1 AS value",
                "query_id": "20202020202020202020202020202020",
                "pagination": { "page_size_rows": 1, "projection": ["value"] }
            }),
        ),
    ] {
        let hook_point = if query_id.starts_with('2') {
            SqlTestHookPoint::AfterPageResponseSerialization
        } else {
            SqlTestHookPoint::AfterSerialization
        };
        let barrier = Arc::new(Barrier::new(2));
        let hook_barrier = Arc::clone(&barrier);
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        entry.session().set_test_hook(Some(Arc::new(move |point| {
            if point == hook_point {
                entered_tx.send(()).unwrap();
                hook_barrier.wait();
            }
        })));
        let mut sql_request = request("POST", "/sql", body);
        sql_request
            .headers_mut()
            .insert("x-session-id", session_id.parse().unwrap());
        let sql_task = tokio::spawn(app.clone().oneshot(sql_request));
        tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
            .await
            .unwrap();

        let cancelled = app
            .clone()
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
        let status = app
            .clone()
            .oneshot(request("GET", &format!("/queries/{query_id}"), Value::Null))
            .await
            .unwrap();
        let status = json_body(status).await;
        assert_eq!(status["state"], "cancelled");
        assert_ne!(status["state"], "completed");
    }

    entry.session().set_test_hook(Some(Arc::new(|point| {
        if point == SqlTestHookPoint::AfterPageResponseSerialization {
            std::thread::sleep(Duration::from_millis(25));
        }
    })));
    let timeout_query_id = "25252525252525252525252525252525";
    let mut timeout_request = request(
        "POST",
        "/sql",
        json!({
            "sql": "SELECT 1 AS value",
            "query_id": timeout_query_id,
            "timeout_ms": 1,
            "pagination": { "page_size_rows": 1, "projection": ["value"] }
        }),
    );
    timeout_request
        .headers_mut()
        .insert("x-session-id", session_id.parse().unwrap());
    let timeout_response = app.clone().oneshot(timeout_request).await.unwrap();
    assert_eq!(timeout_response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(
        json_body(timeout_response).await["error"]["code"],
        "DEADLINE_EXCEEDED"
    );

    let barrier = Arc::new(Barrier::new(2));
    let hook_barrier = Arc::clone(&barrier);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    entry.session().set_test_hook(Some(Arc::new(move |point| {
        if point == SqlTestHookPoint::AfterSerialization {
            entered_tx.send(()).unwrap();
            hook_barrier.wait();
        }
    })));
    let query_id = "30303030303030303030303030303030";
    let mut sql_request = request(
        "POST",
        "/sql",
        json!({
            "sql": "SELECT 1 AS value",
            "query_id": query_id,
            "format": "arrow-stream"
        }),
    );
    sql_request
        .headers_mut()
        .insert("x-session-id", session_id.parse().unwrap());
    let response = app.clone().oneshot(sql_request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body_task = tokio::spawn(async move { to_bytes(response.into_body(), usize::MAX).await });
    tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
        .await
        .unwrap();

    let cancelled = app
        .clone()
        .oneshot(request(
            "POST",
            &format!("/queries/{query_id}/cancel"),
            Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(cancelled.status(), StatusCode::ACCEPTED);
    barrier.wait();
    assert!(body_task.await.unwrap().is_err());
    let status = app
        .oneshot(request("GET", &format!("/queries/{query_id}"), Value::Null))
        .await
        .unwrap();
    let status = json_body(status).await;
    assert_eq!(status["state"], "cancelled");
    assert_eq!(status["terminal_state"], "cancelled_before_commit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn buffered_cancel_after_earlier_commit_reports_durable_outcome() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table(
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
    entry.session().set_test_hook(Some(Arc::new(move |point| {
        if point == SqlTestHookPoint::BeforeSerializationBatch {
            let _ = entered_tx.send(());
            hook_barrier.wait();
        }
    })));

    let query_id = "12121212121212121212121212121212";
    let mut sql_request = request(
        "POST",
        "/sql",
        json!({
            "sql": "INSERT INTO items (id) VALUES (1); SELECT 1 AS value",
            "query_id": query_id,
        }),
    );
    sql_request
        .headers_mut()
        .insert("x-session-id", session_id.parse().unwrap());
    let sql_task = tokio::spawn(app.clone().oneshot(sql_request));
    tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
        .await
        .unwrap();

    let cancelled = app
        .clone()
        .oneshot(request(
            "POST",
            &format!("/queries/{query_id}/cancel"),
            Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(cancelled.status(), StatusCode::ACCEPTED);
    let cancelled = json_body(cancelled).await;
    assert_eq!(cancelled["query_id"], query_id);
    assert_eq!(cancelled["status"], "committed");
    assert!(cancelled["terminal_state"].is_null());
    assert_eq!(cancelled["server_state"], "cancelling");
    assert_eq!(cancelled["cancel_outcome"], "accepted");
    assert_eq!(cancelled["cancellation_reason"], "client_request");
    assert_eq!(cancelled["committed"], true);
    assert_eq!(cancelled["committed_statements"], 1);
    assert!(cancelled["last_commit_epoch"].is_number());
    assert_eq!(
        cancelled["last_commit_epoch_text"],
        cancelled["last_commit_epoch"].as_u64().unwrap().to_string()
    );
    assert_eq!(cancelled["outcome"]["committed"], true);
    barrier.wait();
    let response = sql_task.await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body = json_body(response).await;
    assert_eq!(body["status"], "cancelled_after_commit");
    assert_eq!(body["terminal_state"], "cancelled_after_commit");
    assert_eq!(body["server_state"], "cancelled");
    assert_eq!(body["cancellation_reason"], "client_request");
    assert_eq!(body["committed"], true);
    assert_eq!(body["committed_statements"], 1);
    assert!(body["last_commit_epoch"].is_number());
    assert_eq!(
        body["last_commit_epoch_text"],
        body["last_commit_epoch"].as_u64().unwrap().to_string()
    );
    assert_eq!(body["error"]["code"], "QUERY_CANCELLED_AFTER_COMMIT");
    assert_eq!(body["cancel_outcome"], "accepted");
    assert_eq!(body["outcome"]["committed"], true);
    assert_eq!(body["outcome"]["committed_statements"], 1);

    let status = app
        .clone()
        .oneshot(request("GET", &format!("/queries/{query_id}"), Value::Null))
        .await
        .unwrap();
    let status = json_body(status).await;
    assert_eq!(status["status"], "cancelled_after_commit");
    assert_eq!(status["terminal_state"], "cancelled_after_commit");
    assert_eq!(status["cancel_outcome"], "already_finished");
    assert_eq!(status["state"], "cancelled");
    assert_eq!(status["server_state"], "cancelled");
    assert_eq!(status["cancellation_reason"], "client_request");
    assert_eq!(status["outcome"], body["outcome"]);

    entry.session().set_test_hook(None);
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

#[tokio::test]
async fn dropping_arrow_stream_cancels_and_cleans_registry_entry() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table(
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
        .clone()
        .oneshot(request("GET", &format!("/queries/{query_id}"), Value::Null))
        .await
        .unwrap();
    assert_eq!(status.status(), StatusCode::OK);
    assert_eq!(json_body(status).await["state"], "cancelled");

    let committed_query_id = "8989aaaabbbbccccddddeeeeffff0000";
    let response = app
        .clone()
        .oneshot(request(
            "POST",
            "/sql",
            json!({
                "sql": "INSERT INTO items (id) VALUES (1); SELECT 1 AS value",
                "format": "arrow-stream",
                "query_id": committed_query_id,
            }),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    drop(response);
    let status = app
        .clone()
        .oneshot(request(
            "GET",
            &format!("/queries/{committed_query_id}"),
            Value::Null,
        ))
        .await
        .unwrap();
    let status = json_body(status).await;
    assert_eq!(status["status"], "cancelled_after_commit");
    assert_eq!(status["outcome"]["committed"], true);
    assert_eq!(status["outcome"]["committed_statements"], 1);
    assert!(status["outcome"]["last_commit_epoch"].is_number());

    let count = app
        .oneshot(request(
            "POST",
            "/sql",
            json!({ "sql": "SELECT count(*) AS n FROM items" }),
        ))
        .await
        .unwrap();
    assert_eq!(count.status(), StatusCode::OK);
    assert_eq!(json_body(count).await[0]["n"], 1);
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
    entry.session().set_test_hook(Some(Arc::new(move |point| {
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

    let close_task = tokio::spawn(app.clone().oneshot(request(
        "DELETE",
        &format!("/sessions/{session_id}"),
        Value::Null,
    )));
    loop {
        let status = app
            .clone()
            .oneshot(request("GET", &format!("/queries/{query_id}"), Value::Null))
            .await
            .unwrap();
        let phase = json_body(status).await["state"]
            .as_str()
            .unwrap()
            .to_owned();
        if phase == "cancelling" {
            break;
        }
        tokio::task::yield_now().await;
    }
    barrier.wait();
    assert_eq!(close_task.await.unwrap().unwrap().status(), StatusCode::OK);
    let response = sql_task.await.unwrap().unwrap();
    assert_eq!(response.status().as_u16(), 499);
    assert_eq!(
        json_body(response).await["error"]["code"],
        "QUERY_CANCELLED"
    );
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
    entry.session().set_test_hook(Some(Arc::new(move |point| {
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

    entry.session().set_test_hook(None);
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
    entry.session().set_test_hook(Some(Arc::new(move |point| {
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
    entry.session().set_test_hook(Some(Arc::new(move |point| {
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
    entry.session().set_test_hook(Some(Arc::new(move |point| {
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
    assert_eq!(status["status"], "committed");
    assert_eq!(status["committed"], true);
    assert_eq!(status["outcome"]["committed"], true);
    assert_eq!(status["outcome"]["committed_statements"], 1);
    assert_eq!(status["trace"]["commit_fence_outcome"], "commit_won");

    entry.session().set_test_hook(None);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_after_autocommit_reports_committed_outcome() {
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
    entry.session().set_test_hook(Some(Arc::new(move |point| {
        if point == SqlTestHookPoint::AfterDurableCommit {
            entered_tx.send(()).unwrap();
            hook_barrier.wait();
        }
    })));

    let query_id = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";
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

    let before_cancel = app
        .clone()
        .oneshot(request("GET", &format!("/queries/{query_id}"), Value::Null))
        .await
        .unwrap();
    let before_cancel = json_body(before_cancel).await;
    assert_eq!(before_cancel["state"], "executing");
    assert_eq!(before_cancel["outcome"]["committed"], true);

    let cancel = app
        .clone()
        .oneshot(request(
            "POST",
            &format!("/queries/{query_id}/cancel"),
            Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(cancel.status(), StatusCode::ACCEPTED);
    barrier.wait();

    let response = sql_task.await.unwrap().unwrap();
    let response_status = response.status();
    let response = json_body(response).await;
    assert_eq!(response_status, StatusCode::CONFLICT, "{response}");
    assert_eq!(response["status"], "cancelled_after_commit");
    assert_eq!(response["error"]["code"], "QUERY_CANCELLED_AFTER_COMMIT");
    assert_eq!(response["outcome"]["committed"], true);
    assert_eq!(response["outcome"]["committed_statements"], 1);

    entry.session().set_test_hook(None);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deadline_after_autocommit_reports_committed_outcome() {
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
    entry.session().set_test_hook(Some(Arc::new(move |point| {
        if point == SqlTestHookPoint::AfterDurableCommit {
            entered_tx.send(()).unwrap();
            hook_barrier.wait();
        }
    })));

    let query_id = "dededededededededededededededede";
    let mut sql_request = request(
        "POST",
        "/sql",
        json!({
            "sql": "INSERT INTO items (id) VALUES (1)",
            "query_id": query_id,
            "timeout_ms": 1000,
        }),
    );
    sql_request
        .headers_mut()
        .insert("x-session-id", session_id.parse().unwrap());
    let sql_task = tokio::spawn(app.clone().oneshot(sql_request));
    tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    barrier.wait();

    let response = sql_task.await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body = json_body(response).await;
    assert_eq!(body["status"], "deadline_after_commit");
    assert_eq!(body["error"]["code"], "DEADLINE_AFTER_COMMIT");
    assert_eq!(body["outcome"]["committed"], true);
    assert_eq!(body["outcome"]["committed_statements"], 1);

    let status = app
        .clone()
        .oneshot(request("GET", &format!("/queries/{query_id}"), Value::Null))
        .await
        .unwrap();
    let status = json_body(status).await;
    assert_eq!(status["status"], "deadline_after_commit");
    assert_eq!(status["outcome"], body["outcome"]);

    entry.session().set_test_hook(None);
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
