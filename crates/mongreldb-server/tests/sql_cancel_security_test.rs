use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use mongreldb_core::Database;
use mongreldb_query::SqlTestHookPoint;
use mongreldb_server::{build_app_with_sessions, SessionStore};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;
use tempfile::tempdir;
use tower::ServiceExt;

fn request(method: &str, path: &str, body: Value, auth: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json")
        .header("authorization", auth)
        .body(Body::from(body.to_string()))
        .unwrap()
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_control_is_owner_or_admin_only_and_never_exposes_sql() {
    let directory = tempdir().unwrap();
    let database = Arc::new(Database::create_with_credentials(directory.path(), "admin", "pw").unwrap());
    database.create_user("alice", "pw").unwrap();
    database.create_user("bob", "pw").unwrap();
    let sessions = Arc::new(SessionStore::new(8, Duration::from_secs(60)));
    let app = build_app_with_sessions(
        database,
        std::iter::empty(),
        None,
        None,
        true,
        Arc::clone(&sessions),
    );
    let alice = "Basic YWxpY2U6cHc=";
    let bob = "Basic Ym9iOnB3";
    let admin = "Basic YWRtaW46cHc=";

    let opened = app
        .clone()
        .oneshot(request("POST", "/sessions", Value::Null, alice))
        .await
        .unwrap();
    let session_id = json_body(opened).await["session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let entry = sessions.get(&session_id, "alice").unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let hook_barrier = Arc::clone(&barrier);
    let fired = Arc::new(AtomicBool::new(false));
    let hook_fired = Arc::clone(&fired);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    entry.session.set_test_hook(Some(Arc::new(move |point| {
        if point == SqlTestHookPoint::Planning && !hook_fired.swap(true, Ordering::AcqRel) {
            entered_tx.send(()).unwrap();
            hook_barrier.wait();
        }
    })));

    let query_id = "0123456789abcdef0123456789abcdef";
    let mut sql = request(
        "POST",
        "/sql",
        json!({ "sql": "SELECT 1", "query_id": query_id }),
        alice,
    );
    sql.headers_mut()
        .insert("x-session-id", session_id.parse().unwrap());
    let sql_task = tokio::spawn(app.clone().oneshot(sql));
    tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
        .await
        .unwrap();

    for method_path in [
        ("GET", format!("/queries/{query_id}")),
        ("POST", format!("/queries/{query_id}/cancel")),
    ] {
        let response = app
            .clone()
            .oneshot(request(method_path.0, &method_path.1, Value::Null, bob))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    let owner_status = app
        .clone()
        .oneshot(request(
            "GET",
            &format!("/queries/{query_id}"),
            Value::Null,
            alice,
        ))
        .await
        .unwrap();
    assert_eq!(owner_status.status(), StatusCode::OK);
    let owner_status = json_body(owner_status).await;
    assert_eq!(owner_status["operation"], "SELECT");
    assert!(owner_status.get("sql").is_none());

    let admin_status = app
        .clone()
        .oneshot(request(
            "GET",
            &format!("/queries/{query_id}"),
            Value::Null,
            admin,
        ))
        .await
        .unwrap();
    assert_eq!(admin_status.status(), StatusCode::OK);

    let cancelled = app
        .oneshot(request(
            "POST",
            &format!("/queries/{query_id}/cancel"),
            Value::Null,
            admin,
        ))
        .await
        .unwrap();
    assert_eq!(cancelled.status(), StatusCode::ACCEPTED);
    barrier.wait();
    let response = sql_task.await.unwrap().unwrap();
    assert_eq!(response.status().as_u16(), 499);
}
