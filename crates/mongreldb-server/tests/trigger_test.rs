use mongreldb_core::{
    ColumnDef, Database, StoredTrigger, TriggerCell, TriggerDefinition, TriggerEvent,
    TriggerProgram, TriggerStep, TriggerTarget, TriggerTiming, TriggerValue,
};
use mongreldb_server::build_app;
use std::sync::Arc;
use tempfile::tempdir;
use tower::ServiceExt;

#[tokio::test]
async fn trigger_endpoints_create_execute_describe_replace_and_drop() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let app = build_app(db);

    request(
        &app,
        "POST",
        "/tables",
        Some(serde_json::json!({
            "name": "users",
            "columns": [
                {"id": 1, "name": "id", "ty": "int64", "primary_key": true},
                {"id": 2, "name": "name", "ty": "bytes", "primary_key": false}
            ]
        })),
        200,
    )
    .await;
    request(
        &app,
        "POST",
        "/tables",
        Some(serde_json::json!({
            "name": "audit",
            "columns": [
                {"id": 1, "name": "id", "ty": "int64", "primary_key": true},
                {"id": 2, "name": "user_id", "ty": "int64", "primary_key": false}
            ]
        })),
        200,
    )
    .await;

    let trigger = audit_trigger("users_ai", "users");
    let created = request(
        &app,
        "POST",
        "/triggers",
        Some(serde_json::json!({ "trigger": trigger })),
        200,
    )
    .await;
    assert_eq!(created["trigger"]["name"], "users_ai");

    request(
        &app,
        "POST",
        "/txn",
        Some(serde_json::json!({
            "ops": [{"table": "users", "op": "put", "cells": [1, 7, 2, "alice"]}]
        })),
        200,
    )
    .await;
    let audit_count = request(&app, "GET", "/tables/audit/count", None, 200).await;
    assert_eq!(audit_count["count"], 1);

    let listed = request(&app, "GET", "/triggers", None, 200).await;
    assert_eq!(listed["triggers"][0]["name"], "users_ai");

    let described = request(&app, "GET", "/triggers/users_ai", None, 200).await;
    assert_eq!(described["trigger"]["name"], "users_ai");

    let replaced = request(
        &app,
        "PUT",
        "/triggers/users_ai",
        Some(serde_json::json!({ "trigger": audit_trigger("ignored", "users") })),
        200,
    )
    .await;
    assert_eq!(replaced["trigger"]["name"], "users_ai");
    assert_eq!(replaced["trigger"]["version"], 2);

    request(&app, "DELETE", "/triggers/users_ai", None, 200).await;
    let listed = request(&app, "GET", "/triggers", None, 200).await;
    assert_eq!(listed["triggers"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn trigger_endpoint_errors_are_enveloped() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let app = build_app(db);

    let missing = request(&app, "GET", "/triggers/nope", None, 404).await;
    assert_eq!(missing["error"]["code"], "TRIGGER_NOT_FOUND");

    let invalid = request(
        &app,
        "POST",
        "/triggers",
        Some(serde_json::json!({ "trigger": audit_trigger("missing_target", "missing") })),
        400,
    )
    .await;
    assert_eq!(invalid["error"]["code"], "TRIGGER_VALIDATION");
}

#[tokio::test]
async fn trigger_endpoint_ddl_is_idempotent_by_key() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let app = build_app(db);

    request(
        &app,
        "POST",
        "/tables",
        Some(serde_json::json!({
            "name": "users",
            "columns": [
                {"id": 1, "name": "id", "ty": "int64", "primary_key": true}
            ]
        })),
        200,
    )
    .await;
    request(
        &app,
        "POST",
        "/tables",
        Some(serde_json::json!({
            "name": "audit",
            "columns": [
                {"id": 1, "name": "id", "ty": "int64", "primary_key": true},
                {"id": 2, "name": "user_id", "ty": "int64", "primary_key": false}
            ]
        })),
        200,
    )
    .await;

    let body = serde_json::json!({
        "idempotency_key": "trigger-create-k",
        "trigger": audit_trigger("users_ai", "users")
    });
    let created = request(&app, "POST", "/triggers", Some(body.clone()), 200).await;
    let replayed = request(&app, "POST", "/triggers", Some(body), 200).await;
    assert_eq!(created, replayed);

    let replace_body = serde_json::json!({
        "idempotency_key": "trigger-replace-k",
        "trigger": audit_trigger("ignored", "users")
    });
    let replaced = request(
        &app,
        "PUT",
        "/triggers/users_ai",
        Some(replace_body.clone()),
        200,
    )
    .await;
    let replayed = request(
        &app,
        "PUT",
        "/triggers/users_ai",
        Some(replace_body),
        200,
    )
    .await;
    assert_eq!(replaced, replayed);
    assert_eq!(replaced["trigger"]["version"], 2);

    let dropped = request_with_idempotency_key(
        &app,
        "DELETE",
        "/triggers/users_ai",
        None,
        "trigger-drop-k",
        200,
    )
    .await;
    let replayed = request_with_idempotency_key(
        &app,
        "DELETE",
        "/triggers/users_ai",
        None,
        "trigger-drop-k",
        200,
    )
    .await;
    assert_eq!(dropped, replayed);
}

fn audit_trigger(name: &str, source_table: &str) -> StoredTrigger {
    StoredTrigger::new(
        name,
        TriggerDefinition {
            target: TriggerTarget::Table(source_table.into()),
            timing: TriggerTiming::After,
            event: TriggerEvent::Insert,
            update_of: Vec::new(),
            target_columns: Vec::<ColumnDef>::new(),
            when: None,
            program: TriggerProgram {
                steps: vec![TriggerStep::Insert {
                    table: "audit".into(),
                    cells: vec![
                        TriggerCell {
                            column_id: 1,
                            value: TriggerValue::NewColumn(1),
                        },
                        TriggerCell {
                            column_id: 2,
                            value: TriggerValue::NewColumn(1),
                        },
                    ],
                }],
            },
        },
        0,
    )
    .unwrap()
}

async fn request(
    app: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<serde_json::Value>,
    expected_status: u16,
) -> serde_json::Value {
    request_inner(app, method, uri, body, None, expected_status).await
}

async fn request_with_idempotency_key(
    app: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<serde_json::Value>,
    idempotency_key: &str,
    expected_status: u16,
) -> serde_json::Value {
    request_inner(
        app,
        method,
        uri,
        body,
        Some(idempotency_key),
        expected_status,
    )
    .await
}

async fn request_inner(
    app: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<serde_json::Value>,
    idempotency_key: Option<&str>,
    expected_status: u16,
) -> serde_json::Value {
    let mut builder = axum::http::Request::builder().method(method).uri(uri);
    if body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    if let Some(idempotency_key) = idempotency_key {
        builder = builder.header("Idempotency-Key", idempotency_key);
    }
    let body = body
        .map(|v| axum::body::Body::from(v.to_string()))
        .unwrap_or_else(axum::body::Body::empty);
    let resp = app
        .clone()
        .oneshot(builder.body(body).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), expected_status);
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    if body.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&body).unwrap()
    }
}
