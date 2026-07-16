use mongreldb_core::procedure::{
    ProcedureBody, ProcedureCell, ProcedureMode, ProcedureParam, ProcedureStep, ProcedureValue,
    StoredProcedure,
};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{Database, Permission};
use mongreldb_server::{build_app, build_app_full};
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
    assert_eq!(called["committed"], false);
    assert!(called["epoch"].is_null());
    assert!(called["epoch_text"].is_null());
    assert!(called["result"].to_string().contains("active"));

    let dropped = request(&app, "DELETE", "/procedures/read_users", None).await;
    assert_eq!(dropped.as_object().unwrap().len(), 3);
    assert_eq!(dropped["status"], "committed");
    let epoch = dropped["epoch"].as_u64().unwrap();
    assert_eq!(dropped["epoch_text"], epoch.to_string());
    let listed = request(&app, "GET", "/procedures", None).await;
    assert_eq!(listed["procedures"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn procedure_ddl_preserves_durable_commit_outcome() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table(
        "users",
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
    std::fs::rename(dir.path().join("CATALOG"), dir.path().join("CATALOG.saved")).unwrap();
    std::fs::create_dir(dir.path().join("CATALOG")).unwrap();
    let procedure = StoredProcedure::new(
        "read_users",
        ProcedureMode::ReadOnly,
        Vec::new(),
        ProcedureBody {
            steps: vec![ProcedureStep::NativeQuery {
                id: "read".into(),
                table: "users".into(),
                conditions: Vec::new(),
                projection: Some(vec![1]),
                limit: Some(1),
            }],
            return_value: ProcedureValue::StepRows("read".into()),
        },
        0,
    )
    .unwrap();
    let (status, body) = request_status(
        &app,
        "POST",
        "/procedures",
        Some(serde_json::json!({ "procedure": procedure })),
    )
    .await;
    assert_eq!(status, 409, "body: {body}");
    assert_eq!(body["status"], "committed");
    assert_eq!(body["committed"], true);
    assert_eq!(body["retryable"], false);
    assert_eq!(body["error"]["code"], "COMMIT_OUTCOME");
    let epoch = body["epoch"].as_u64().unwrap();
    assert_eq!(body["epoch_text"], epoch.to_string());
    assert!(db.procedure("read_users").is_some());
}

#[tokio::test]
async fn procedure_call_idempotency_restarts_and_rejects_mismatch() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table(
        "users",
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
    db.create_procedure(
        StoredProcedure::new(
            "insert_user",
            ProcedureMode::ReadWrite,
            vec![ProcedureParam {
                name: "id".into(),
                ty: TypeId::Int64,
                nullable: false,
                default: None,
            }],
            ProcedureBody {
                steps: vec![ProcedureStep::Put {
                    id: "write".into(),
                    table: "users".into(),
                    cells: vec![ProcedureCell {
                        column_id: 1,
                        value: ProcedureValue::Param("id".into()),
                    }],
                    returning: true,
                }],
                return_value: ProcedureValue::StepRow("write".into()),
            },
            0,
        )
        .unwrap(),
    )
    .unwrap();
    let app = build_app(Arc::clone(&db));
    let body = serde_json::json!({
        "args": {"id": 1},
        "idempotency_key": "procedure-key"
    });
    let first = request(
        &app,
        "POST",
        "/kit/procedures/insert_user/call",
        Some(body.clone()),
    )
    .await;
    assert_eq!(first["committed"], true);
    let epoch = first["epoch"].as_u64().unwrap();
    assert_eq!(first["epoch_text"], epoch.to_string());
    let restarted = build_app(Arc::clone(&db));
    let replay = request(
        &restarted,
        "POST",
        "/procedures/insert_user/call",
        Some(body),
    )
    .await;
    assert_eq!(first, replay);

    let (status, mismatch) = request_status(
        &app,
        "POST",
        "/kit/procedures/insert_user/call",
        Some(serde_json::json!({
            "args": {"id": 2},
            "idempotency_key": "procedure-key"
        })),
    )
    .await;
    assert_eq!(status, 409);
    assert_eq!(mismatch["error"]["code"], "IDEMPOTENCY_KEY_REUSE_MISMATCH");
    assert_eq!(
        db.table("users")
            .unwrap()
            .lock()
            .visible_rows(db.snapshot().0)
            .unwrap()
            .len(),
        1
    );

    let (status, invalid) = request_status(
        &app,
        "POST",
        "/kit/procedures/insert_user/call",
        Some(serde_json::json!({"args": {"id": 3}, "idempotency_key": ""})),
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(invalid["error"]["code"], "INVALID_IDEMPOTENCY_KEY");

    let original = db.procedure("insert_user").unwrap();
    db.create_or_replace_procedure(
        StoredProcedure::new(
            "insert_user",
            ProcedureMode::ReadWrite,
            vec![ProcedureParam {
                name: "id".into(),
                ty: TypeId::Int64,
                nullable: false,
                default: None,
            }],
            ProcedureBody {
                steps: vec![ProcedureStep::Put {
                    id: "replacement-write".into(),
                    table: "users".into(),
                    cells: vec![ProcedureCell {
                        column_id: 1,
                        value: ProcedureValue::Param("id".into()),
                    }],
                    returning: true,
                }],
                return_value: ProcedureValue::StepRow("replacement-write".into()),
            },
            0,
        )
        .unwrap(),
    )
    .unwrap();
    let stale_call = db
        .call_procedure_as_bound(
            &original,
            std::collections::HashMap::from([("id".into(), mongreldb_core::Value::Int64(2))]),
            None,
        )
        .unwrap_err();
    assert!(matches!(
        stale_call,
        mongreldb_core::MongrelError::Conflict(_)
    ));
    let (status, replaced) = request_status(
        &app,
        "POST",
        "/kit/procedures/insert_user/call",
        Some(serde_json::json!({
            "args": {"id": 1},
            "idempotency_key": "procedure-key"
        })),
    )
    .await;
    assert_eq!(status, 409);
    assert_eq!(replaced["error"]["code"], "IDEMPOTENCY_KEY_REUSE_MISMATCH");
    assert!(replaced.get("result").is_none());

    db.drop_procedure("insert_user").unwrap();
    let (status, dropped) = request_status(
        &app,
        "POST",
        "/kit/procedures/insert_user/call",
        Some(serde_json::json!({
            "args": {"id": 1},
            "idempotency_key": "procedure-key"
        })),
    )
    .await;
    assert_eq!(status, 404);
    assert_eq!(dropped["error"]["code"], "PROCEDURE_NOT_FOUND");
    assert!(dropped.get("result").is_none());
    assert_eq!(
        db.table("users")
            .unwrap()
            .lock()
            .visible_rows(db.snapshot().0)
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn procedure_replay_reauthorizes_and_user_recreation_cannot_inherit_receipt() {
    let directory = tempdir().unwrap();
    let db = Arc::new(Database::create_with_credentials(directory.path(), "admin", "pw").unwrap());
    db.create_table(
        "users",
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
    db.create_procedure(
        StoredProcedure::new(
            "insert_user",
            ProcedureMode::ReadWrite,
            vec![ProcedureParam {
                name: "id".into(),
                ty: TypeId::Int64,
                nullable: false,
                default: None,
            }],
            ProcedureBody {
                steps: vec![ProcedureStep::Put {
                    id: "write".into(),
                    table: "users".into(),
                    cells: vec![ProcedureCell {
                        column_id: 1,
                        value: ProcedureValue::Param("id".into()),
                    }],
                    returning: true,
                }],
                return_value: ProcedureValue::StepRow("write".into()),
            },
            0,
        )
        .unwrap(),
    )
    .unwrap();
    db.create_user("alice", "pw").unwrap();
    db.create_role("procedure_caller").unwrap();
    db.grant_permission("procedure_caller", Permission::All)
        .unwrap();
    db.grant_role("alice", "procedure_caller").unwrap();
    let app = build_app_full(Arc::clone(&db), std::iter::empty(), None, None, true);
    let body = serde_json::json!({
        "args": {"id": 71},
        "idempotency_key": "procedure-auth-key"
    });
    let (status, first) = request_status_auth(
        &app,
        "POST",
        "/kit/procedures/insert_user/call",
        Some(body.clone()),
        "Basic YWxpY2U6cHc=",
    )
    .await;
    assert_eq!(status, 200, "body: {first}");
    assert!(first.get("result").is_some());

    db.revoke_role("alice", "procedure_caller").unwrap();
    let (status, revoked) = request_status_auth(
        &app,
        "POST",
        "/kit/procedures/insert_user/call",
        Some(body.clone()),
        "Basic YWxpY2U6cHc=",
    )
    .await;
    assert_eq!(status, 403, "body: {revoked}");
    assert!(revoked.get("result").is_none());

    db.drop_user("alice").unwrap();
    db.create_user("alice", "pw2").unwrap();
    let (status, recreated) = request_status_auth(
        &app,
        "POST",
        "/kit/procedures/insert_user/call",
        Some(body),
        "Basic YWxpY2U6cHcy",
    )
    .await;
    assert_eq!(status, 403, "body: {recreated}");
    assert!(recreated.get("result").is_none());
    assert_eq!(
        db.table("users")
            .unwrap()
            .lock()
            .visible_rows(db.snapshot().0)
            .unwrap()
            .len(),
        1
    );
}

async fn request(
    app: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<serde_json::Value>,
) -> serde_json::Value {
    let (status, body) = request_status(app, method, uri, body).await;
    assert_eq!(status, 200);
    body
}

async fn request_status(
    app: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<serde_json::Value>,
) -> (u16, serde_json::Value) {
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
    let status = resp.status().as_u16();
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let body = if body.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&body).unwrap()
    };
    (status, body)
}

async fn request_status_auth(
    app: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<serde_json::Value>,
    authorization: &str,
) -> (u16, serde_json::Value) {
    let mut builder = axum::http::Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", authorization);
    if body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    let body = body
        .map(|value| axum::body::Body::from(value.to_string()))
        .unwrap_or_else(axum::body::Body::empty);
    let response = app
        .clone()
        .oneshot(builder.body(body).unwrap())
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let body = if body.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&body).unwrap()
    };
    (status, body)
}
