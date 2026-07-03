use mongreldb_core::Database;
use mongreldb_server::build_app;
use std::sync::Arc;
use tempfile::tempdir;
use tower::ServiceExt;

#[tokio::test]
async fn procedure_endpoints_create_list_call_and_drop() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let app = build_app(db);

    let create_body = serde_json::json!({
        "name": "users",
        "columns": [
            {"id": 1, "name": "id", "ty": "int64", "primary_key": true},
            {"id": 2, "name": "status", "ty": "bytes", "primary_key": false}
        ]
    });
    request(&app, "POST", "/tables", Some(create_body)).await;
    request(
        &app,
        "POST",
        "/txn",
        Some(serde_json::json!({
            "ops": [{"table": "users", "op": "put", "cells": [1, 1, 2, "active"]}]
        })),
    )
    .await;

    let spec = serde_json::json!({
        "name": "read_users",
        "version": 1,
        "mode": "read_only",
        "params": [],
        "body": {
            "steps": [{
                "kind": "native_query",
                "id": "read",
                "table": "users",
                "conditions": [],
                "projection": [1, 2],
                "limit": 10
            }],
            "return_value": { "kind": "step_rows", "value": "read" }
        },
        "checksum": "",
        "created_epoch": 0,
        "updated_epoch": 0
    });
    let created = request(
        &app,
        "POST",
        "/procedures",
        Some(serde_json::json!({ "procedure": spec })),
    )
    .await;
    assert_eq!(created["procedure"]["name"], "read_users");

    let listed = request(&app, "GET", "/procedures", None).await;
    assert_eq!(listed["procedures"][0]["name"], "read_users");

    let called = request(
        &app,
        "POST",
        "/kit/procedures/read_users/call",
        Some(serde_json::json!({ "args": {} })),
    )
    .await;
    assert_eq!(called["status"], "ok");
    assert!(called["result"].to_string().contains("active"));

    request(&app, "DELETE", "/procedures/read_users", None).await;
    let listed = request(&app, "GET", "/procedures", None).await;
    assert_eq!(listed["procedures"].as_array().unwrap().len(), 0);
}

async fn request(
    app: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<serde_json::Value>,
) -> serde_json::Value {
    let mut builder = axum::http::Request::builder().method(method).uri(uri);
    if body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    let body = body
        .map(|v| axum::body::Body::from(v.to_string()))
        .unwrap_or_else(axum::body::Body::empty);
    let resp = app
        .clone()
        .oneshot(builder.body(body).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    if body.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&body).unwrap()
    }
}
