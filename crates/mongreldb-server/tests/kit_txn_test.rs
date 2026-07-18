//! `/kit/txn` + `/kit/schema` typed-server integration tests.

use mongreldb_core::constraint::{
    CheckConstraint, CheckExpr, FkAction, ForeignKey, TableConstraints, UniqueConstraint,
};
use mongreldb_core::schema::*;
use mongreldb_core::{Database, Permission, Value};
use mongreldb_server::{build_app, build_app_full, build_app_with_config};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tempfile::tempdir;
use tower::ServiceExt;

fn col(id: u16, name: &str, ty: TypeId, flags: ColumnFlags) -> ColumnDef {
    ColumnDef {
        id,
        name: name.into(),
        ty,
        flags,
        default_value: None,
        embedding_source: None,
    }
}

fn users_schema() -> Schema {
    let mut cons = TableConstraints::default();
    cons.uniques.push(UniqueConstraint {
        id: 1,
        name: "email_unique".into(),
        columns: vec![1],
    });
    cons.checks.push(CheckConstraint {
        id: 2,
        name: "age_nonneg".into(),
        expr: CheckExpr::Or(
            Box::new(CheckExpr::IsNull(2)),
            Box::new(CheckExpr::Ge(
                Box::new(CheckExpr::Col(2)),
                Box::new(CheckExpr::Lit(Value::Int64(0))),
            )),
        ),
    });
    Schema {
        schema_id: 0,
        columns: vec![
            col(
                0,
                "id",
                TypeId::Int64,
                ColumnFlags::empty()
                    .with(ColumnFlags::PRIMARY_KEY)
                    .with(ColumnFlags::AUTO_INCREMENT),
            ),
            col(
                1,
                "email",
                TypeId::Bytes,
                ColumnFlags::empty().with(ColumnFlags::NULLABLE),
            ),
            col(
                2,
                "age",
                TypeId::Int64,
                ColumnFlags::empty().with(ColumnFlags::NULLABLE),
            ),
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: cons,
        clustered: false,
    }
}

fn orders_schema() -> Schema {
    let mut cons = TableConstraints::default();
    cons.foreign_keys.push(ForeignKey {
        id: 3,
        name: "uid_fk".into(),
        columns: vec![11],
        ref_table: "users".into(),
        ref_columns: vec![0],
        on_delete: FkAction::Restrict,
        on_update: FkAction::Restrict,
    });
    Schema {
        schema_id: 0,
        columns: vec![
            col(
                10,
                "oid",
                TypeId::Int64,
                ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            ),
            col(
                11,
                "uid",
                TypeId::Int64,
                ColumnFlags::empty().with(ColumnFlags::NULLABLE),
            ),
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: cons,
        clustered: false,
    }
}

async fn setup() -> (tempfile::TempDir, axum::Router) {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("users", users_schema()).unwrap();
    db.create_table("orders", orders_schema()).unwrap();
    let app = build_app(db);
    (dir, app)
}

async fn post(app: axum::Router, uri: &str, body: serde_json::Value) -> (u16, serde_json::Value) {
    let resp = app
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let bytes = axum::body::to_bytes(resp.into_body(), 8 * 1024 * 1024)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, v)
}

async fn post_auth(
    app: axum::Router,
    uri: &str,
    body: serde_json::Value,
    authorization: &str,
) -> (u16, serde_json::Value) {
    let resp = app
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .header("authorization", authorization)
                .body(axum::body::Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let bytes = axum::body::to_bytes(resp.into_body(), 8 * 1024 * 1024)
        .await
        .unwrap();
    let value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, value)
}

async fn get(app: axum::Router, uri: &str) -> (u16, serde_json::Value) {
    let resp = app
        .oneshot(
            axum::http::Request::builder()
                .method("GET")
                .uri(uri)
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let bytes = axum::body::to_bytes(resp.into_body(), 8 * 1024 * 1024)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, v)
}

async fn setup_shared() -> (tempfile::TempDir, Arc<Database>, axum::Router) {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("users", users_schema()).unwrap();
    db.create_table("orders", orders_schema()).unwrap();
    let app = build_app(Arc::clone(&db));
    (dir, db, app)
}

#[tokio::test]
async fn idempotency_persists_across_server_restart() {
    let dir = tempdir().unwrap();
    let body = serde_json::json!({
        "idempotency_key": "persist-k",
        "ops": [{"put": {"table": "users", "cells": [0, 7, 1, "p@x", 2, 1]}}]
    });
    let epoch1 = {
        let db = Arc::new(Database::create(dir.path()).unwrap());
        db.create_table("users", users_schema()).unwrap();
        db.create_table("orders", orders_schema()).unwrap();
        let (status, response) = post(build_app(db), "/kit/txn", body.clone()).await;
        assert_eq!(status, 200);
        assert_eq!(
            response["epoch_text"],
            response["epoch"].as_u64().unwrap().to_string()
        );
        response["epoch"].as_u64().unwrap()
    };

    let db = Arc::new(Database::open(dir.path()).unwrap());
    let (s2, v2) = post(build_app(Arc::clone(&db)), "/kit/txn", body).await;
    assert_eq!(s2, 200);
    assert_eq!(
        v2["epoch"].as_u64().unwrap(),
        epoch1,
        "idempotent replay after restart returns durable epoch"
    );
    // And the row was not double-inserted.
    let snap = db.snapshot().0;
    let n = db
        .table("users")
        .unwrap()
        .lock()
        .visible_rows(snap)
        .unwrap()
        .len();
    assert_eq!(n, 1);
}

#[tokio::test]
async fn idempotency_rejects_payload_mismatch_and_malformed_keys() {
    let (_dir, db, app) = setup_shared().await;
    let first = serde_json::json!({
        "idempotency_key": "bound-key",
        "ops": [{"put": {"table": "users", "cells": [0, 1, 1, "a@x", 2, 1]}}]
    });
    assert_eq!(post(app.clone(), "/kit/txn", first).await.0, 200);
    let mismatch = serde_json::json!({
        "idempotency_key": "bound-key",
        "ops": [{"put": {"table": "users", "cells": [0, 2, 1, "b@x", 2, 2]}}]
    });
    let (status, body) = post(app.clone(), "/kit/txn", mismatch).await;
    assert_eq!(status, 409);
    assert_eq!(body["error"]["code"], "IDEMPOTENCY_KEY_REUSE_MISMATCH");

    for key in [String::new(), "x".repeat(257)] {
        let invalid = serde_json::json!({
            "idempotency_key": key,
            "ops": [{"put": {"table": "users", "cells": [0, 3, 1, "c@x", 2, 3]}}]
        });
        let (status, body) = post(app.clone(), "/kit/txn", invalid).await;
        assert_eq!(status, 400);
        assert_eq!(body["error"]["code"], "INVALID_IDEMPOTENCY_KEY");
    }
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
async fn receipt_publication_failure_returns_unknown_without_reexecution() {
    let (dir, db, app) = setup_shared().await;
    let key = "receipt-failure";
    let scope = idempotency_scope("anonymous", key);
    let receipt_path = dir
        .path()
        .join("_idem")
        .join(format!("{scope}.receipt.json"));
    db.__set_catalog_commit_hook(move || {
        if !receipt_path.exists() {
            std::fs::create_dir(&receipt_path).unwrap();
        }
    });
    let body = serde_json::json!({
        "idempotency_key": key,
        "ops": [{"put": {"table": "users", "cells": [0, 5, 1, "once@x", 2, 5]}}]
    });
    for _ in 0..2 {
        let (status, response) = post(app.clone(), "/kit/txn", body.clone()).await;
        assert_eq!(status, 409, "body: {response}");
        assert_eq!(response["error"]["code"], "QUERY_OUTCOME_UNKNOWN");
        assert!(response["committed"].is_null());
    }
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

fn idempotency_scope(owner: &str, key: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"mongreldb-server-idempotency-v2\0");
    digest.update((owner.len() as u64).to_le_bytes());
    digest.update(owner.as_bytes());
    digest.update((key.len() as u64).to_le_bytes());
    digest.update(key.as_bytes());
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_routers_execute_same_owner_key_once() {
    let (_dir, db, first_app) = setup_shared().await;
    let second_app = build_app(Arc::clone(&db));
    let body = serde_json::json!({
        "idempotency_key": "concurrent-key",
        "ops": [{"put": {"table": "users", "cells": [0, 9, 1, "once@x", 2, 9]}}]
    });
    let (first, second) = tokio::join!(
        post(first_app, "/kit/txn", body.clone()),
        post(second_app, "/kit/txn", body),
    );
    assert_eq!(first.0, 200, "body: {}", first.1);
    assert_eq!(second.0, 200, "body: {}", second.1);
    assert_eq!(first.1, second.1);
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
async fn idempotency_keys_are_scoped_to_authenticated_owner() {
    let directory = tempdir().unwrap();
    let db = Arc::new(Database::create_with_credentials(directory.path(), "admin", "pw").unwrap());
    db.create_table("users", users_schema()).unwrap();
    for user in ["alice", "bob"] {
        db.create_user(user, "pw").unwrap();
    }
    db.create_role("writer").unwrap();
    db.grant_permission(
        "writer",
        Permission::Insert {
            table: "users".into(),
        },
    )
    .unwrap();
    for user in ["alice", "bob"] {
        db.grant_role(user, "writer").unwrap();
    }
    let app = build_app_full(Arc::clone(&db), std::iter::empty(), None, None, true);
    for (authorization, id) in [("Basic YWxpY2U6cHc=", 11), ("Basic Ym9iOnB3", 12)] {
        let body = serde_json::json!({
            "idempotency_key": "shared-key",
            "ops": [{"put": {"table": "users", "cells": [0, id, 1, format!("{id}@x"), 2, id]}}]
        });
        let (status, response) = post_auth(app.clone(), "/kit/txn", body, authorization).await;
        assert_eq!(status, 200, "body: {response}");
    }
    assert_eq!(
        db.table("users")
            .unwrap()
            .lock()
            .visible_rows(db.snapshot().0)
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn idempotent_replay_reauthorizes_and_user_recreation_cannot_inherit_receipt() {
    let directory = tempdir().unwrap();
    let db = Arc::new(Database::create_with_credentials(directory.path(), "admin", "pw").unwrap());
    db.create_table("users", users_schema()).unwrap();
    db.create_user("alice", "pw").unwrap();
    db.create_role("writer").unwrap();
    for permission in [
        Permission::Insert {
            table: "users".into(),
        },
        Permission::Select {
            table: "users".into(),
        },
    ] {
        db.grant_permission("writer", permission).unwrap();
    }
    db.grant_role("alice", "writer").unwrap();
    let app = build_app_full(Arc::clone(&db), std::iter::empty(), None, None, true);
    let body = serde_json::json!({
        "idempotency_key": "authorization-bound-key",
        "ops": [{"put": {
            "table": "users",
            "cells": [0, 31, 1, "private@x", 2, 31],
            "returning": true
        }}]
    });
    let (status, first) =
        post_auth(app.clone(), "/kit/txn", body.clone(), "Basic YWxpY2U6cHc=").await;
    assert_eq!(status, 200, "body: {first}");
    assert!(first["results"].to_string().contains("private@x"));

    db.create_role("unrelated_security_change").unwrap();
    let (status, changed_security) =
        post_auth(app.clone(), "/kit/txn", body.clone(), "Basic YWxpY2U6cHc=").await;
    assert_eq!(status, 409, "body: {changed_security}");
    assert_eq!(
        changed_security["error"]["code"],
        "IDEMPOTENCY_KEY_REUSE_MISMATCH"
    );
    assert!(changed_security.get("results").is_none());

    db.revoke_role("alice", "writer").unwrap();
    let (status, revoked) =
        post_auth(app.clone(), "/kit/txn", body.clone(), "Basic YWxpY2U6cHc=").await;
    assert_eq!(status, 403, "body: {revoked}");
    assert!(revoked.get("results").is_none());

    db.drop_user("alice").unwrap();
    db.create_user("alice", "pw2").unwrap();
    let (status, recreated) = post_auth(app, "/kit/txn", body, "Basic YWxpY2U6cHcy").await;
    assert_eq!(status, 403, "body: {recreated}");
    assert!(recreated.get("results").is_none());
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
async fn returning_rows_require_current_select_permission_before_receipt_lookup() {
    let directory = tempdir().unwrap();
    let db = Arc::new(Database::create_with_credentials(directory.path(), "admin", "pw").unwrap());
    db.create_table("users", users_schema()).unwrap();
    db.create_user("alice", "pw").unwrap();
    db.create_role("writer").unwrap();
    db.grant_permission(
        "writer",
        Permission::Insert {
            table: "users".into(),
        },
    )
    .unwrap();
    db.grant_role("alice", "writer").unwrap();
    let app = build_app_full(Arc::clone(&db), std::iter::empty(), None, None, true);
    let body = serde_json::json!({
        "idempotency_key": "returning-key",
        "ops": [{"put": {
            "table": "users",
            "cells": [0, 41, 1, "hidden@x", 2, 41],
            "returning": true
        }}]
    });
    let (status, denied) = post_auth(app, "/kit/txn", body, "Basic YWxpY2U6cHc=").await;
    assert_eq!(status, 403, "body: {denied}");
    assert!(denied.get("results").is_none());
    assert!(db
        .table("users")
        .unwrap()
        .lock()
        .visible_rows(db.snapshot().0)
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn table_recreation_cannot_replay_old_returning_rows() {
    let directory = tempdir().unwrap();
    let db = Arc::new(Database::create(directory.path()).unwrap());
    db.create_table("users", users_schema()).unwrap();
    let old_identity = db.table_identity("users").unwrap();
    let app = build_app(Arc::clone(&db));
    let body = serde_json::json!({
        "idempotency_key": "table-generation-key",
        "ops": [{"put": {
            "table": "users",
            "cells": [0, 51, 1, "old-table@x", 2, 51],
            "returning": true
        }}]
    });
    let (status, first) = post(app.clone(), "/kit/txn", body.clone()).await;
    assert_eq!(status, 200, "body: {first}");
    assert!(first["results"].to_string().contains("old-table@x"));

    db.drop_table("users").unwrap();
    db.create_table("users", users_schema()).unwrap();
    let mut stale_transaction = db.begin();
    let stale = stale_transaction
        .put_returning_bound(
            "users",
            old_identity.0,
            old_identity.1,
            vec![(0, Value::Int64(52))],
        )
        .unwrap_err();
    assert!(matches!(stale, mongreldb_core::MongrelError::Conflict(_)));
    let (status, replay) = post(app, "/kit/txn", body).await;
    assert_eq!(status, 409, "body: {replay}");
    assert_eq!(replay["error"]["code"], "IDEMPOTENCY_KEY_REUSE_MISMATCH");
    assert!(replay.get("results").is_none());
    assert!(db
        .table("users")
        .unwrap()
        .lock()
        .visible_rows(db.snapshot().0)
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn rotated_bearer_token_cannot_inherit_old_receipt() {
    let directory = tempdir().unwrap();
    let db = Arc::new(Database::create(directory.path()).unwrap());
    db.create_table("users", users_schema()).unwrap();
    let body = serde_json::json!({
        "idempotency_key": "bearer-generation-key",
        "ops": [{"put": {
            "table": "users",
            "cells": [0, 61, 1, "token-a@x", 2, 61],
            "returning": true
        }}]
    });
    let first_app = build_app_with_config(
        Arc::clone(&db),
        std::iter::empty(),
        Some("token-a".into()),
        None,
    );
    let (status, first) = post_auth(first_app, "/kit/txn", body.clone(), "Bearer token-a").await;
    assert_eq!(status, 200, "body: {first}");

    let rotated = build_app_with_config(
        Arc::clone(&db),
        std::iter::empty(),
        Some("token-b".into()),
        None,
    );
    let (status, second) = post_auth(rotated, "/kit/txn", body, "Bearer token-b").await;
    assert_eq!(status, 409, "body: {second}");
    assert_ne!(second, first);
    assert!(second.get("results").is_none());
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
async fn schema_endpoint_returns_constraints() {
    let (_d, app) = setup().await;
    let (s, v) = get(app, "/kit/schema/users").await;
    assert_eq!(s, 200);
    assert_eq!(v["constraints"]["uniques"][0]["name"], "email_unique");
    assert_eq!(v["constraints"]["checks"][0]["name"], "age_nonneg");
    assert_eq!(v["columns"][0]["auto_increment"], true);
}

#[tokio::test]
async fn kit_static_defaults_preserve_scalar_types_and_dynamic_expr() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let app = build_app(Arc::clone(&db));
    let body = serde_json::json!({
        "name": "defaults",
        "columns": [
            {"id": 0, "name": "id", "ty": "int64", "primary_key": true},
            {"id": 1, "name": "label", "ty": "varchar", "default_value": "draft"},
            {"id": 2, "name": "count", "ty": "int64", "default_value": 7},
            {"id": 3, "name": "enabled", "ty": "bool", "default_value": true},
            {"id": 4, "name": "note", "ty": "varchar", "nullable": true, "default_value": null},
            {"id": 5, "name": "literal_now", "ty": "varchar", "default_value": "now"},
            {"id": 6, "name": "literal_uuid", "ty": "varchar", "default_value": "uuid"},
            {"id": 7, "name": "created", "ty": "varchar", "default_expr": "now"}
        ]
    });
    let (status, _) = post(app.clone(), "/kit/create_table", body).await;
    assert_eq!(status, 200);

    let schema = db.table("defaults").unwrap().lock().schema().clone();
    assert!(matches!(
        schema.columns[1].default_value.as_ref(),
        Some(DefaultExpr::Static(Value::Bytes(value))) if value == b"draft"
    ));
    assert!(matches!(
        schema.columns[2].default_value.as_ref(),
        Some(DefaultExpr::Static(Value::Int64(7)))
    ));
    assert!(matches!(
        schema.columns[3].default_value.as_ref(),
        Some(DefaultExpr::Static(Value::Bool(true)))
    ));
    assert!(matches!(
        schema.columns[4].default_value.as_ref(),
        Some(DefaultExpr::Static(Value::Null))
    ));
    assert!(matches!(
        schema.columns[5].default_value.as_ref(),
        Some(DefaultExpr::Static(Value::Bytes(value))) if value == b"now"
    ));
    assert!(matches!(
        schema.columns[6].default_value.as_ref(),
        Some(DefaultExpr::Static(Value::Bytes(value))) if value == b"uuid"
    ));
    assert!(matches!(
        schema.columns[7].default_value.as_ref(),
        Some(DefaultExpr::Now)
    ));

    let (status, response) = post(
        app,
        "/kit/txn",
        serde_json::json!({"ops":[{"put":{"table":"defaults","cells":[0,1],"returning":true}}]}),
    )
    .await;
    assert_eq!(status, 200, "{response}");
    assert!(response["results"][0]["row"].is_array());
}

#[tokio::test]
async fn kit_txn_put_with_auto_inc_and_returning() {
    let (_d, app) = setup().await;
    // Put with auto-inc PK omitted; returning the row.
    let body = serde_json::json!({
        "ops": [
            {"put": {"table": "users", "cells": [1, "a@x", 2, 30], "returning": true}},
        ]
    });
    let (s, v) = post(app, "/kit/txn", body).await;
    assert_eq!(s, 200, "body: {v}");
    assert_eq!(v["status"], "committed");
    assert!(v["epoch"].as_u64().unwrap() > 0);
    let r = &v["results"][0];
    assert_eq!(r["kind"], "put");
    assert!(r["auto_inc"].as_i64().unwrap() >= 1);
    // Returned row carries the assigned auto-inc id in column 0.
    assert!(r["row"].as_array().unwrap().contains(&serde_json::json!(1)));
}

#[tokio::test]
async fn kit_txn_check_violation_aborts_batch() {
    let (_d, app) = setup().await;
    let body = serde_json::json!({
        "ops": [
            {"put": {"table": "users", "cells": [1, "a@x", 2, 30]}},
            {"put": {"table": "users", "cells": [1, "b@x", 2, -5]}},
        ]
    });
    let (s, v) = post(app, "/kit/txn", body).await;
    assert_eq!(s, 409, "body: {v}");
    assert_eq!(v["status"], "aborted");
    assert_eq!(v["error"]["code"], "CHECK_VIOLATION");
    // Atomicity: nothing committed.
}

#[tokio::test]
async fn kit_txn_unique_violation_across_batches() {
    let (_d, app) = setup().await;
    let b1 = serde_json::json!({"ops": [{"put": {"table": "users", "cells": [1, "dup@x", 2, 1]}}]});
    let (s, _) = post(app.clone(), "/kit/txn", b1).await;
    assert_eq!(s, 200);
    let b2 = serde_json::json!({"ops": [{"put": {"table": "users", "cells": [0, 99, 1, "dup@x", 2, 2]}}]});
    let (s, v) = post(app, "/kit/txn", b2).await;
    assert_eq!(s, 409, "body: {v}");
    assert_eq!(v["error"]["code"], "UNIQUE_VIOLATION");
}

#[tokio::test]
async fn kit_txn_fk_insert_violation() {
    let (_d, app) = setup().await;
    let body = serde_json::json!({
        "ops": [{"put": {"table": "orders", "cells": [10, 1, 11, 9999]}}]
    });
    let (s, v) = post(app, "/kit/txn", body).await;
    assert_eq!(s, 409);
    assert_eq!(v["error"]["code"], "FK_VIOLATION");
}

#[tokio::test]
async fn kit_txn_upsert_and_delete_by_pk() {
    let (_d, app) = setup().await;
    // Insert a user with explicit id.
    let b =
        serde_json::json!({"ops": [{"put": {"table": "users", "cells": [0, 5, 1, "u@x", 2, 20]}}]});
    let (s, _) = post(app.clone(), "/kit/txn", b).await;
    assert_eq!(s, 200);
    // Upsert same PK → DO NOTHING (unchanged).
    let b = serde_json::json!({"ops": [{"upsert": {"table": "users", "cells": [0, 5, 1, "u@x"], "returning": true}}]});
    let (s, v) = post(app.clone(), "/kit/txn", b).await;
    assert_eq!(s, 200, "body: {v}");
    assert_eq!(v["results"][0]["action"], "unchanged");
    // Upsert same PK → DO UPDATE (age → 21).
    let b = serde_json::json!({"ops": [{"upsert": {"table": "users", "cells": [0, 5, 1, "u@x"], "update_cells": [2, 21], "returning": true}}]});
    let (s, v) = post(app.clone(), "/kit/txn", b).await;
    assert_eq!(s, 200, "body: {v}");
    assert_eq!(v["results"][0]["action"], "updated");
    assert!(v["results"][0]["row"]
        .as_array()
        .unwrap()
        .contains(&serde_json::json!(21)));
    // Delete by PK.
    let b = serde_json::json!({"ops": [{"delete_by_pk": {"table": "users", "pk": 5}}]});
    let (s, v) = post(app.clone(), "/kit/txn", b).await;
    assert_eq!(s, 200, "body: {v}");
    assert_eq!(v["results"][0]["kind"], "deleted");
}

#[tokio::test]
async fn kit_txn_idempotent_replay() {
    let (_d, app) = setup().await;
    let b = serde_json::json!({
        "idempotency_key": "k1",
        "ops": [{"put": {"table": "users", "cells": [0, 1, 1, "a@x", 2, 1]}}]
    });
    let (s1, v1) = post(app.clone(), "/kit/txn", b.clone()).await;
    assert_eq!(s1, 200);
    let epoch1 = v1["epoch"].as_u64().unwrap();
    // Replay with same key → same response, no new write.
    let (s2, v2) = post(app, "/kit/txn", b).await;
    assert_eq!(s2, 200);
    assert_eq!(
        v2["epoch"].as_u64().unwrap(),
        epoch1,
        "replay returns cached epoch"
    );
}

#[tokio::test]
async fn kit_txn_structural_error_has_op_index() {
    let (_d, app) = setup().await;
    // Unknown column id → BAD_REQUEST with op_index 0.
    let b = serde_json::json!({"ops": [{"put": {"table": "users", "cells": [99, 1]}}]});
    let (s, v) = post(app, "/kit/txn", b).await;
    assert_eq!(s, 400);
    assert_eq!(v["error"]["code"], "BAD_REQUEST");
    assert_eq!(v["error"]["op_index"], 0);
}

#[tokio::test]
async fn kit_query_pk_range_and_projection() {
    let (_d, app) = setup().await;
    // Seed: users with explicit ids 1..=3 (ages 30/40/50), emails a/b/c.
    let b = serde_json::json!({"ops": [
        {"put": {"table": "users", "cells": [0, 1, 1, "a@x", 2, 30]}},
        {"put": {"table": "users", "cells": [0, 2, 1, "b@x", 2, 40]}},
        {"put": {"table": "users", "cells": [0, 3, 1, "c@x", 2, 50]}},
    ]});
    let (s, _) = post(app.clone(), "/kit/txn", b).await;
    assert_eq!(s, 200);

    // PK exact lookup → one row, with its physical row_id.
    let q = serde_json::json!({"table": "users", "conditions": [{"pk": {"value": 2}}]});
    let (s, v) = post(app.clone(), "/kit/query", q).await;
    assert_eq!(s, 200, "body: {v}");
    assert_eq!(v["rows"].as_array().unwrap().len(), 1);
    assert!(v["rows"][0]["row_id"].is_string(), "row_id returned");
    // cells carry id 0 → 2 (the PK we asked for).
    let cells = v["rows"][0]["cells"].as_array().unwrap();
    assert!(cells.contains(&serde_json::json!(2)));

    // Range 35..=55 on age (col 2) → users 2 and 3.
    let q = serde_json::json!({"table": "users",
        "conditions": [{"range": {"column_id": 2, "lo": 35, "hi": 55}}],
        "projection": [0, 2]});
    let (s, v) = post(app.clone(), "/kit/query", q).await;
    assert_eq!(s, 200, "body: {v}");
    assert_eq!(v["rows"].as_array().unwrap().len(), 2);
    // Projection: only cols 0 and 2 appear (no email col 1).
    for r in v["rows"].as_array().unwrap() {
        let ids: Vec<&serde_json::Value> =
            r["cells"].as_array().unwrap().iter().step_by(2).collect();
        assert!(ids.contains(&&serde_json::json!(0)) || ids.contains(&&serde_json::json!(2)));
        assert!(
            !ids.iter().any(|x| **x == serde_json::json!(1u64)),
            "email not projected"
        );
    }

    // Limit truncates.
    let q = serde_json::json!({"table": "users", "limit": 1});
    let (s, v) = post(app.clone(), "/kit/query", q).await;
    assert_eq!(s, 200);
    assert_eq!(v["rows"].as_array().unwrap().len(), 1);
    assert_eq!(v["truncated"], true);

    // Offset is applied before the per-request result cap.
    let q = serde_json::json!({"table": "users", "limit": 1, "offset": 2});
    let (s, v) = post(app.clone(), "/kit/query", q).await;
    assert_eq!(s, 200);
    assert_eq!(v["rows"].as_array().unwrap().len(), 1);
    assert!(v["rows"][0]["cells"]
        .as_array()
        .unwrap()
        .contains(&serde_json::json!(3)));

    // Empty conditions ⇒ all rows.
    let q = serde_json::json!({"table": "users"});
    let (s, v) = post(app, "/kit/query", q).await;
    assert_eq!(s, 200);
    assert_eq!(v["rows"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn kit_query_cursor_pins_snapshot_and_request() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("users", users_schema()).unwrap();
    let app = build_app(Arc::clone(&db));
    let seed = serde_json::json!({"ops": [
        {"put": {"table": "users", "cells": [0, 1, 1, "a@x", 2, 30]}},
        {"put": {"table": "users", "cells": [0, 2, 1, "b@x", 2, 40]}},
        {"put": {"table": "users", "cells": [0, 3, 1, "c@x", 2, 50]}},
    ]});
    assert_eq!(post(app.clone(), "/kit/txn", seed).await.0, 200);

    let first = serde_json::json!({"table": "users", "projection": [0], "limit": 2});
    let (status, first) = post(app.clone(), "/kit/query", first).await;
    assert_eq!(status, 200, "body: {first}");
    assert_eq!(first["rows"].as_array().unwrap().len(), 2);
    let cursor = first["next_cursor"].as_str().unwrap().to_string();

    let insert = serde_json::json!({"ops": [
        {"put": {"table": "users", "cells": [0, 4, 1, "d@x", 2, 60]}}
    ]});
    assert_eq!(post(app.clone(), "/kit/txn", insert).await.0, 200);

    let second = serde_json::json!({
        "table": "users",
        "projection": [0],
        "limit": 2,
        "cursor": cursor,
    });
    let (status, second) = post(app.clone(), "/kit/query", second).await;
    assert_eq!(status, 409, "body: {second}");
    assert_eq!(second["error"]["code"], "CURSOR_STALE");

    let mismatched = serde_json::json!({
        "table": "users",
        "projection": [0, 2],
        "limit": 2,
        "cursor": first["next_cursor"],
    });
    let (status, body) = post(app, "/kit/query", mismatched).await;
    assert_eq!(status, 400);
    assert_eq!(body["error"]["code"], "BAD_REQUEST");
}

#[tokio::test]
async fn kit_create_table_self_services_constraints_over_http() {
    // A remote client provisions a constraint-bearing table entirely over HTTP
    // (no out-of-band create): unique + check + auto_increment, then exercises
    // the constraints through /kit/txn.
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let app = build_app(Arc::clone(&db));

    let body = serde_json::json!({
        "name": "accounts",
        "columns": [
            {"id": 0, "name": "id", "ty": "int64", "primary_key": true, "auto_increment": true},
            {"id": 1, "name": "email", "ty": "bytes", "nullable": true},
            {"id": 2, "name": "level", "ty": "int64", "nullable": true},
            {"id": 3, "name": "label", "ty": "varchar", "nullable": false, "default_value": "draft"},
        ],
        "constraints": {
            "uniques": [{"id": 1, "name": "email_unique", "columns": [1]}],
            "checks": [{"id": 2, "name": "level_nonneg", "expr":
                {"Or": [{"IsNull": 2}, {"Ge": [{"Col": 2}, {"Lit": {"Int64": 0}}]}]}}]
        }
    });
    let (s, v) = post(app.clone(), "/kit/create_table", body).await;
    assert_eq!(s, 200, "create_table body: {v}");
    assert!(v["table_id"].as_u64().is_some());

    // Schema metadata round-trips the constraints + auto_increment flag.
    let (s, v) = get(app.clone(), "/kit/schema/accounts").await;
    assert_eq!(s, 200);
    assert_eq!(v["columns"][0]["auto_increment"], true);
    assert_eq!(v["constraints"]["uniques"][0]["name"], "email_unique");
    assert_eq!(v["constraints"]["checks"][0]["name"], "level_nonneg");

    // A valid insert commits (auto-inc assigned).
    let (s, v) = post(
        app.clone(),
        "/kit/txn",
        serde_json::json!({"ops":[{"put":{"table":"accounts","cells":[1,"a@x",2,5],"returning":true}}]}),
    )
    .await;
    assert_eq!(s, 200, "body: {v}");
    assert!(v["results"][0]["auto_inc"].as_i64().unwrap() >= 1);
    let (s, v) = post(
        app.clone(),
        "/kit/query",
        serde_json::json!({"table":"accounts"}),
    )
    .await;
    assert_eq!(s, 200, "query body: {v}");
    assert!(v["rows"][0]["cells"]
        .as_array()
        .unwrap()
        .windows(2)
        .any(|pair| pair == [serde_json::json!(3), serde_json::json!("draft")]));

    // Duplicate email → UNIQUE_VIOLATION (constraint enforced end-to-end).
    let (s, v) = post(
        app.clone(),
        "/kit/txn",
        serde_json::json!({"ops":[{"put":{"table":"accounts","cells":[0,9,1,"a@x"]}}]}),
    )
    .await;
    assert_eq!(s, 409);
    assert_eq!(v["error"]["code"], "UNIQUE_VIOLATION");

    // CHECK violation → CHECK_VIOLATION.
    let (s, v) = post(
        app,
        "/kit/txn",
        serde_json::json!({"ops":[{"put":{"table":"accounts","cells":[1,"b@x",2,-1]}}]}),
    )
    .await;
    assert_eq!(s, 409);
    assert_eq!(v["error"]["code"], "CHECK_VIOLATION");
}
