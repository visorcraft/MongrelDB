//! `/kit/txn` + `/kit/schema` typed-server integration tests.

use mongreldb_core::constraint::{
    CheckConstraint, CheckExpr, FkAction, ForeignKey, TableConstraints, UniqueConstraint,
};
use mongreldb_core::schema::*;
use mongreldb_core::{Database, Value};
use mongreldb_server::build_app;
use std::sync::Arc;
use tempfile::tempdir;
use tower::ServiceExt;

fn col(id: u16, name: &str, ty: TypeId, flags: ColumnFlags) -> ColumnDef {
    ColumnDef {
        id,
        name: name.into(),
        ty,
        flags,            default_value: None,
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
    // Commit a batch with key K on app1, then rebuild a fresh app2 on the SAME
    // database dir (simulating a daemon restart). The replayed K must return
    // the original committed response from the on-disk `_idem/` store.
    let (_dir, db, app1) = setup_shared().await;
    let body = serde_json::json!({
        "idempotency_key": "persist-k",
        "ops": [{"put": {"table": "users", "cells": [0, 7, 1, "p@x", 2, 1]}}]
    });
    let (s1, v1) = post(app1, "/kit/txn", body.clone()).await;
    assert_eq!(s1, 200);
    let epoch1 = v1["epoch"].as_u64().unwrap();

    // Fresh server instance on the same db (new IdempotencyStore, empty memory).
    let app2 = build_app(Arc::clone(&db));
    let (s2, v2) = post(app2, "/kit/txn", body).await;
    assert_eq!(s2, 200);
    assert_eq!(
        v2["epoch"].as_u64().unwrap(),
        epoch1,
        "idempotent replay after restart returns cached epoch from disk"
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
async fn schema_endpoint_returns_constraints() {
    let (_d, app) = setup().await;
    let (s, v) = get(app, "/kit/schema/users").await;
    assert_eq!(s, 200);
    assert_eq!(v["constraints"]["uniques"][0]["name"], "email_unique");
    assert_eq!(v["constraints"]["checks"][0]["name"], "age_nonneg");
    assert_eq!(v["columns"][0]["auto_increment"], true);
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

    // Empty conditions ⇒ all rows.
    let q = serde_json::json!({"table": "users"});
    let (s, v) = post(app, "/kit/query", q).await;
    assert_eq!(s, 200);
    assert_eq!(v["rows"].as_array().unwrap().len(), 3);
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
