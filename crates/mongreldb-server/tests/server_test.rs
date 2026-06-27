//! P6.1 — multi-table HTTP server integration test.

use mongreldb_core::Database;
use mongreldb_server::build_app;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn multi_table_server_endpoints() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let app = build_app(db);

    // Use tower's oneshot to test the router in-process.
    use tower::ServiceExt;

    // Health check.
    let resp = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("GET")
                .uri("/health")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Create a table.
    let create_body = serde_json::json!({
        "name": "users",
        "columns": [
            {"id": 1, "name": "id", "ty": "int64", "primary_key": true},
            {"id": 2, "name": "name", "ty": "bytes", "primary_key": false},
        ]
    });
    let resp = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/tables")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // List tables.
    let resp = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("GET")
                .uri("/tables")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024).await.unwrap();
    let tables: Vec<String> = serde_json::from_slice(&body).unwrap();
    assert!(tables.iter().any(|t| t == "users"), "users table exists");

    // Put a row.
    let put_body = serde_json::json!({
        "row": [1, 42, 2, "Alice"]
    });
    let resp = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/tables/users/put")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(put_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Commit.
    let resp = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/tables/users/commit")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Count.
    let resp = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("GET")
                .uri("/tables/users/count")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["count"], 1);

    // Atomic txn.
    let txn_body = serde_json::json!({
        "ops": [
            {"table": "users", "op": "put", "cells": [1, 99, 2, "Bob"]},
        ]
    });
    let resp = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/txn")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(txn_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Verify the txn row is visible.
    let resp = app
        .oneshot(
            axum::http::Request::builder()
                .method("GET")
                .uri("/tables/users/count")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["count"], 2);
}
