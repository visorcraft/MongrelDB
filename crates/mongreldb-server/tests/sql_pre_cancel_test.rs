use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use mongreldb_core::Database;
use mongreldb_server::{build_app, build_app_full};
use serde_json::{json, Value};
use std::sync::Arc;
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
async fn cancel_before_registration_stops_before_sql_parsing() {
    let directory = tempdir().unwrap();
    let app = build_app(Arc::new(Database::create(directory.path()).unwrap()));
    let query_id = "01010101010101010101010101010101";

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
    assert_eq!(json_body(cancel).await["cancel_outcome"], "pre_cancelled");

    let status = app
        .clone()
        .oneshot(request("GET", &format!("/queries/{query_id}"), Value::Null))
        .await
        .unwrap();
    assert_eq!(status.status(), StatusCode::OK);
    let status = json_body(status).await;
    assert_eq!(status["state"], "pre_cancelled");
    assert_eq!(status["status"], "cancelled_before_start");
    assert_eq!(status["terminal_error"]["code"], "QUERY_CANCELLED");

    let sql = app
        .clone()
        .oneshot(request(
            "POST",
            "/sql",
            json!({
                "query_id": query_id,
                "sql": "this is deliberately invalid SQL",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(sql.status().as_u16(), 499);
    let sql = json_body(sql).await;
    assert_eq!(sql["status"], "cancelled_before_commit");
    assert_eq!(sql["error"]["code"], "QUERY_CANCELLED");
    assert_eq!(sql["outcome"]["committed"], false);

    let status = app
        .clone()
        .oneshot(request("GET", &format!("/queries/{query_id}"), Value::Null))
        .await
        .unwrap();
    assert_eq!(status.status(), StatusCode::OK);
    let status = json_body(status).await;
    assert_eq!(status["state"], "cancelled");
    assert_eq!(status["status"], "cancelled_before_commit");

    let reused = app
        .oneshot(request(
            "POST",
            "/sql",
            json!({ "query_id": query_id, "sql": "SELECT 1" }),
        ))
        .await
        .unwrap();
    assert_eq!(reused.status(), StatusCode::CONFLICT);
    assert_eq!(
        json_body(reused).await["error"]["code"],
        "QUERY_ID_CONFLICT"
    );
}

#[tokio::test]
async fn pre_cancel_is_bound_to_authenticated_owner() {
    let directory = tempdir().unwrap();
    let database =
        Arc::new(Database::create_with_credentials(directory.path(), "admin", "pw").unwrap());
    database.create_user("alice", "pw").unwrap();
    database.create_user("bob", "pw").unwrap();
    let app = build_app_full(database, std::iter::empty(), None, None, true);
    let query_id = "02020202020202020202020202020202";

    let mut cancel = request("POST", &format!("/queries/{query_id}/cancel"), Value::Null);
    cancel
        .headers_mut()
        .insert("authorization", "Basic YWxpY2U6cHc=".parse().unwrap());
    let cancel = app.clone().oneshot(cancel).await.unwrap();
    assert_eq!(cancel.status(), StatusCode::ACCEPTED);

    let mut alice_status = request("GET", &format!("/queries/{query_id}"), Value::Null);
    alice_status
        .headers_mut()
        .insert("authorization", "Basic YWxpY2U6cHc=".parse().unwrap());
    let alice_status = app.clone().oneshot(alice_status).await.unwrap();
    assert_eq!(alice_status.status(), StatusCode::OK);
    assert_eq!(json_body(alice_status).await["state"], "pre_cancelled");

    let mut bob_status = request("GET", &format!("/queries/{query_id}"), Value::Null);
    bob_status
        .headers_mut()
        .insert("authorization", "Basic Ym9iOnB3".parse().unwrap());
    assert_eq!(
        app.clone().oneshot(bob_status).await.unwrap().status(),
        StatusCode::NOT_FOUND
    );

    let mut admin_status = request("GET", &format!("/queries/{query_id}"), Value::Null);
    admin_status
        .headers_mut()
        .insert("authorization", "Basic YWRtaW46cHc=".parse().unwrap());
    let admin_status = app.clone().oneshot(admin_status).await.unwrap();
    assert_eq!(admin_status.status(), StatusCode::OK);
    assert_eq!(json_body(admin_status).await["state"], "pre_cancelled");

    let mut sql = request(
        "POST",
        "/sql",
        json!({ "query_id": query_id, "sql": "SELECT 1 AS value" }),
    );
    sql.headers_mut()
        .insert("authorization", "Basic Ym9iOnB3".parse().unwrap());
    let sql = app.oneshot(sql).await.unwrap();
    assert_eq!(sql.status(), StatusCode::CONFLICT);
    assert_eq!(json_body(sql).await["error"]["code"], "QUERY_ID_CONFLICT");
}

#[tokio::test]
async fn pre_cancel_is_bound_to_session_when_header_is_supplied() {
    let directory = tempdir().unwrap();
    let app = build_app(Arc::new(Database::create(directory.path()).unwrap()));
    let first = json_body(
        app.clone()
            .oneshot(request("POST", "/sessions", Value::Null))
            .await
            .unwrap(),
    )
    .await["session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let second = json_body(
        app.clone()
            .oneshot(request("POST", "/sessions", Value::Null))
            .await
            .unwrap(),
    )
    .await["session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let query_id = "03030303030303030303030303030303";

    let mut cancel = request("POST", &format!("/queries/{query_id}/cancel"), Value::Null);
    cancel
        .headers_mut()
        .insert("x-session-id", first.parse().unwrap());
    assert_eq!(
        app.clone().oneshot(cancel).await.unwrap().status(),
        StatusCode::ACCEPTED
    );

    assert_eq!(
        app.clone()
            .oneshot(request("GET", &format!("/queries/{query_id}"), Value::Null))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );

    let mut wrong_status = request("GET", &format!("/queries/{query_id}"), Value::Null);
    wrong_status
        .headers_mut()
        .insert("x-session-id", second.parse().unwrap());
    assert_eq!(
        app.clone().oneshot(wrong_status).await.unwrap().status(),
        StatusCode::NOT_FOUND
    );

    let mut matching_status = request("GET", &format!("/queries/{query_id}"), Value::Null);
    matching_status
        .headers_mut()
        .insert("x-session-id", first.parse().unwrap());
    let matching_status = app.clone().oneshot(matching_status).await.unwrap();
    assert_eq!(matching_status.status(), StatusCode::OK);
    assert_eq!(json_body(matching_status).await["state"], "pre_cancelled");

    let mut sql = request(
        "POST",
        "/sql",
        json!({ "query_id": query_id, "sql": "SELECT 1 AS value" }),
    );
    sql.headers_mut()
        .insert("x-session-id", second.parse().unwrap());
    let sql = app.oneshot(sql).await.unwrap();
    assert_eq!(sql.status(), StatusCode::CONFLICT);
    assert_eq!(json_body(sql).await["error"]["code"], "QUERY_ID_CONFLICT");
}
