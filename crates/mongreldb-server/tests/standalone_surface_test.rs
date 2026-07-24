//! Negative test for the default (standalone) build: with the `cluster`
//! feature off, the cluster admin surface must be absent from the router and
//! cluster admin SQL must fall through to the ordinary SQL path (which
//! rejects it), never to `cluster_admin::try_admin_sql`. The whole file is
//! compiled out under `--features cluster`, where the opposite holds.
#![cfg(not(feature = "cluster"))]

use mongreldb_core::Database;
use mongreldb_server::build_app;
use std::sync::Arc;
use tempfile::tempdir;
use tower::ServiceExt;

async fn request(
    app: axum::Router,
    method: &str,
    uri: &str,
    body: Option<serde_json::Value>,
) -> (u16, serde_json::Value) {
    let mut builder = axum::http::Request::builder().method(method).uri(uri);
    let body = match body {
        Some(json) => {
            builder = builder.header("content-type", "application/json");
            axum::body::Body::from(json.to_string())
        }
        None => axum::body::Body::empty(),
    };
    let resp = app.oneshot(builder.body(body).unwrap()).await.unwrap();
    let status = resp.status().as_u16();
    let bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, value)
}

#[tokio::test]
async fn standalone_build_omits_cluster_admin_surface() {
    let dir = tempdir().unwrap();
    let app = build_app(Arc::new(Database::create(dir.path()).unwrap()));

    // The cluster admin routes are not registered in a standalone build.
    let (status, _) = request(app.clone(), "GET", "/admin/cluster/status", None).await;
    assert_eq!(status, 404);

    // `SHOW CLUSTER` is not intercepted as admin SQL: it takes the ordinary
    // SQL path and is rejected there, so the response must not carry the
    // cluster admin `command` envelope.
    let (status, body) = request(
        app,
        "POST",
        "/sql",
        Some(serde_json::json!({"sql": "SHOW CLUSTER"})),
    )
    .await;
    assert_ne!(status, 200);
    assert!(
        body.get("command").is_none(),
        "SHOW CLUSTER must not produce a cluster admin response: {body}"
    );
}
