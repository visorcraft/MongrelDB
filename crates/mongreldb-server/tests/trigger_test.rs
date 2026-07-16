use mongreldb_core::{
    ColumnDef, ColumnFlags, Database, IndexDef, IndexKind, Permission, Schema, StoredTrigger,
    TriggerCell, TriggerCondition, TriggerDefinition, TriggerEvent, TriggerExpr, TriggerProgram,
    TriggerRaiseAction, TriggerStep, TriggerTarget, TriggerTiming, TriggerValue, TypeId, Value,
};
use mongreldb_server::{build_app, build_app_full};
use std::sync::Arc;
use tempfile::tempdir;
use tower::ServiceExt;

#[tokio::test]
async fn trigger_endpoints_create_execute_describe_replace_and_drop() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let app = build_app(Arc::clone(&db));

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
async fn trigger_ddl_preserves_durable_commit_outcome() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    create_table_with_bitmap_indexes(
        &db,
        "users",
        vec![ColumnDef {
            id: 1,
            name: "id".into(),
            ty: TypeId::Int64,
            flags: pk_flags(),
            default_value: None,
        }],
        Vec::new(),
    );
    create_table_with_bitmap_indexes(
        &db,
        "audit",
        vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: pk_flags(),
                default_value: None,
            },
            ColumnDef {
                id: 2,
                name: "user_id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
        ],
        Vec::new(),
    );
    let app = build_app(Arc::clone(&db));
    std::fs::rename(dir.path().join("CATALOG"), dir.path().join("CATALOG.saved")).unwrap();
    std::fs::create_dir(dir.path().join("CATALOG")).unwrap();
    let body = request(
        &app,
        "POST",
        "/triggers",
        Some(serde_json::json!({ "trigger": audit_trigger("users_ai", "users") })),
        409,
    )
    .await;
    assert_eq!(body["status"], "committed");
    assert_eq!(body["committed"], true);
    assert_eq!(body["retryable"], false);
    assert_eq!(body["error"]["code"], "COMMIT_OUTCOME");
    let epoch = body["epoch"].as_u64().unwrap();
    assert_eq!(body["epoch_text"], epoch.to_string());
    assert!(db.trigger("users_ai").is_some());
}

#[tokio::test]
async fn trigger_endpoint_ddl_is_idempotent_by_key() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let app = build_app(Arc::clone(&db));

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
    let replayed = request(&app, "POST", "/triggers", Some(body.clone()), 200).await;
    assert_eq!(created, replayed);
    let restarted = build_app(db);
    let replayed_after_restart = request(&restarted, "POST", "/triggers", Some(body), 200).await;
    assert_eq!(created, replayed_after_restart);

    let mismatch = request(
        &app,
        "POST",
        "/triggers",
        Some(serde_json::json!({
            "idempotency_key": "trigger-create-k",
            "trigger": audit_trigger("other_ai", "users")
        })),
        409,
    )
    .await;
    assert_eq!(mismatch["error"]["code"], "IDEMPOTENCY_KEY_REUSE_MISMATCH");

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
    let replayed = request(&app, "PUT", "/triggers/users_ai", Some(replace_body), 200).await;
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

#[tokio::test]
async fn trigger_endpoint_rejects_invalid_idempotency_keys_before_mutation() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let app = build_app(db);
    let body = serde_json::json!({
        "idempotency_key": "body-key",
        "trigger": audit_trigger("users_ai", "users")
    });

    let mismatch = request_with_idempotency_key(
        &app,
        "POST",
        "/triggers",
        Some(body.clone()),
        "header-key",
        400,
    )
    .await;
    assert_eq!(mismatch["error"]["code"], "INVALID_IDEMPOTENCY_KEY");

    let invalid_header = axum::http::HeaderValue::from_bytes(&[0xff]).unwrap();
    let invalid = request_inner(
        &app,
        "POST",
        "/triggers",
        Some(body),
        Some(invalid_header),
        400,
    )
    .await;
    assert_eq!(invalid["error"]["code"], "INVALID_IDEMPOTENCY_KEY");

    let listed = request(&app, "GET", "/triggers", None, 200).await;
    assert_eq!(listed["triggers"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn trigger_replay_reauthorizes_and_user_recreation_cannot_inherit_receipt() {
    let directory = tempdir().unwrap();
    let db = Arc::new(Database::create_with_credentials(directory.path(), "admin", "pw").unwrap());
    create_table_with_bitmap_indexes(
        &db,
        "users",
        vec![col_def(1, "id", TypeId::Int64, pk_flags())],
        Vec::new(),
    );
    create_table_with_bitmap_indexes(
        &db,
        "audit",
        vec![
            col_def(1, "id", TypeId::Int64, pk_flags()),
            col_def(2, "user_id", TypeId::Int64, ColumnFlags::empty()),
        ],
        Vec::new(),
    );
    db.create_user("alice", "pw").unwrap();
    db.create_role("ddl_writer").unwrap();
    db.grant_permission("ddl_writer", Permission::Ddl).unwrap();
    db.grant_role("alice", "ddl_writer").unwrap();
    let app = build_app_full(Arc::clone(&db), std::iter::empty(), None, None, true);
    let body = serde_json::json!({
        "idempotency_key": "trigger-auth-key",
        "trigger": audit_trigger("users_ai", "users")
    });
    let first = request_auth(
        &app,
        "POST",
        "/triggers",
        Some(body.clone()),
        "Basic YWxpY2U6cHc=",
        200,
    )
    .await;
    assert_eq!(first["trigger"]["version"], 1);

    db.revoke_role("alice", "ddl_writer").unwrap();
    let revoked = request_auth(
        &app,
        "POST",
        "/triggers",
        Some(body.clone()),
        "Basic YWxpY2U6cHc=",
        403,
    )
    .await;
    assert!(revoked.get("trigger").is_none());

    db.drop_user("alice").unwrap();
    db.create_user("alice", "pw2").unwrap();
    let recreated = request_auth(
        &app,
        "POST",
        "/triggers",
        Some(body),
        "Basic YWxpY2U6cHcy",
        403,
    )
    .await;
    assert!(recreated.get("trigger").is_none());
    assert_eq!(db.triggers().len(), 1);
    assert_eq!(db.trigger("users_ai").unwrap().version, 1);
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

fn col_def(id: u16, name: &str, ty: TypeId, flags: ColumnFlags) -> ColumnDef {
    ColumnDef {
        id,
        name: name.into(),
        ty,
        flags,
        default_value: None,
    }
}

fn pk_flags() -> ColumnFlags {
    ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY)
}

fn create_table_with_bitmap_indexes(
    db: &Database,
    name: &str,
    columns: Vec<ColumnDef>,
    bitmap_columns: Vec<u16>,
) {
    let indexes = bitmap_columns
        .into_iter()
        .map(|column_id| IndexDef {
            name: format!("idx_{column_id}"),
            column_id,
            kind: IndexKind::Bitmap,
            predicate: None,
            options: Default::default(),
        })
        .collect();
    let schema = Schema {
        schema_id: 0,
        columns,
        indexes,
        colocation: Vec::new(),
        constraints: Default::default(),
        clustered: false,
    };
    db.create_table(name, schema).unwrap();
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
        Some(axum::http::HeaderValue::from_str(idempotency_key).unwrap()),
        expected_status,
    )
    .await
}

async fn request_inner(
    app: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<serde_json::Value>,
    idempotency_key: Option<axum::http::HeaderValue>,
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
    let status = resp.status().as_u16();
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(status, expected_status);
    if body.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null)
    }
}

async fn request_auth(
    app: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<serde_json::Value>,
    authorization: &str,
    expected_status: u16,
) -> serde_json::Value {
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
    assert_eq!(status, expected_status);
    if body.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null)
    }
}

async fn kit_txn(app: &axum::Router, ops: Vec<serde_json::Value>) -> serde_json::Value {
    request(
        app,
        "POST",
        "/kit/txn",
        Some(serde_json::json!({ "ops": ops })),
        200,
    )
    .await
}

async fn kit_query(
    app: &axum::Router,
    table: &str,
    conditions: Vec<serde_json::Value>,
) -> Vec<serde_json::Value> {
    let body = serde_json::json!({
        "table": table,
        "conditions": conditions
    });
    let resp = request(app, "POST", "/kit/query", Some(body), 200).await;
    resp["rows"].as_array().unwrap().clone()
}

fn cell_value(row: &serde_json::Value, column_id: u16) -> Option<&serde_json::Value> {
    let cells = row["cells"].as_array()?;
    cells.chunks(2).find_map(|chunk| {
        if chunk[0].as_u64()? as u16 == column_id {
            Some(&chunk[1])
        } else {
            None
        }
    })
}

#[tokio::test]
async fn trigger_when_clause_ranges_and_booleans() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let app = build_app(db);

    request(
        &app,
        "POST",
        "/tables",
        Some(serde_json::json!({
            "name": "scores",
            "columns": [
                {"id": 1, "name": "id", "ty": "int64", "primary_key": true},
                {"id": 2, "name": "value", "ty": "int64", "primary_key": false},
                {"id": 3, "name": "category", "ty": "bytes", "primary_key": false, "nullable": true}
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
                {"id": 2, "name": "value", "ty": "int64", "primary_key": false},
                {"id": 3, "name": "category", "ty": "bytes", "primary_key": false, "nullable": true}
            ]
        })),
        200,
    )
    .await;

    let trigger = StoredTrigger::new(
        "scores_ai",
        TriggerDefinition {
            target: TriggerTarget::Table("scores".into()),
            timing: TriggerTiming::After,
            event: TriggerEvent::Insert,
            update_of: Vec::new(),
            target_columns: Vec::<ColumnDef>::new(),
            when: Some(TriggerExpr::Or {
                left: Box::new(TriggerExpr::And {
                    left: Box::new(TriggerExpr::Gt {
                        left: TriggerValue::NewColumn(2),
                        right: TriggerValue::Literal(Value::Int64(0)),
                    }),
                    right: Box::new(TriggerExpr::Lte {
                        left: TriggerValue::NewColumn(2),
                        right: TriggerValue::Literal(Value::Int64(100)),
                    }),
                }),
                right: Box::new(TriggerExpr::Not(Box::new(TriggerExpr::IsNull(
                    TriggerValue::NewColumn(3),
                )))),
            }),
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
                            value: TriggerValue::NewColumn(2),
                        },
                        TriggerCell {
                            column_id: 3,
                            value: TriggerValue::NewColumn(3),
                        },
                    ],
                }],
            },
        },
        0,
    )
    .unwrap();
    request(
        &app,
        "POST",
        "/triggers",
        Some(serde_json::json!({ "trigger": trigger })),
        200,
    )
    .await;

    request(
        &app,
        "POST",
        "/txn",
        Some(serde_json::json!({
            "ops": [
                {"table": "scores", "op": "put", "cells": [1, 1, 2, 50, 3, "a"]},
                {"table": "scores", "op": "put", "cells": [1, 2, 2, 200, 3, "b"]},
                {"table": "scores", "op": "put", "cells": [1, 3, 2, -1, 3, null]}
            ]
        })),
        200,
    )
    .await;

    let audit_count = request(&app, "GET", "/tables/audit/count", None, 200).await;
    assert_eq!(audit_count["count"], 2);
}

#[tokio::test]
async fn trigger_select_foreach_raises_on_children() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let app = build_app(db);

    request(
        &app,
        "POST",
        "/tables",
        Some(serde_json::json!({
            "name": "parents",
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
            "name": "children",
            "columns": [
                {"id": 1, "name": "id", "ty": "int64", "primary_key": true},
                {"id": 2, "name": "parent_id", "ty": "int64", "primary_key": false}
            ]
        })),
        200,
    )
    .await;

    let trigger = StoredTrigger::new(
        "parents_bd",
        TriggerDefinition {
            target: TriggerTarget::Table("parents".into()),
            timing: TriggerTiming::Before,
            event: TriggerEvent::Delete,
            update_of: Vec::new(),
            target_columns: Vec::<ColumnDef>::new(),
            when: None,
            program: TriggerProgram {
                steps: vec![
                    TriggerStep::Select {
                        id: "kids".into(),
                        table: "children".into(),
                        conditions: vec![TriggerCondition::Eq {
                            column_id: 2,
                            value: TriggerValue::OldColumn(1),
                        }],
                    },
                    TriggerStep::Foreach {
                        id: "kids".into(),
                        steps: vec![TriggerStep::Raise {
                            action: TriggerRaiseAction::Abort,
                            message: TriggerValue::Literal(Value::Bytes(
                                b"child rows exist".to_vec(),
                            )),
                        }],
                    },
                ],
            },
        },
        0,
    )
    .unwrap();
    request(
        &app,
        "POST",
        "/triggers",
        Some(serde_json::json!({ "trigger": trigger })),
        200,
    )
    .await;

    kit_txn(
        &app,
        vec![
            serde_json::json!({"put": {"table": "parents", "cells": [1, 1]}}),
            serde_json::json!({"put": {"table": "children", "cells": [1, 1, 2, 1]}}),
        ],
    )
    .await;

    let err = request(
        &app,
        "POST",
        "/kit/txn",
        Some(serde_json::json!({
            "ops": [{"delete_by_pk": {"table": "parents", "pk": 1}}]
        })),
        409,
    )
    .await;
    assert!(err["error"]["message"]
        .as_str()
        .unwrap()
        .contains("child rows exist"));

    kit_txn(
        &app,
        vec![serde_json::json!({"delete_by_pk": {"table": "children", "pk": 1}})],
    )
    .await;
    kit_txn(
        &app,
        vec![serde_json::json!({"delete_by_pk": {"table": "parents", "pk": 1}})],
    )
    .await;

    let parents_count = request(&app, "GET", "/tables/parents/count", None, 200).await;
    assert_eq!(parents_count["count"], 0);
}

#[tokio::test]
async fn trigger_delete_where_cleans_up_related_rows() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let app = build_app(Arc::clone(&db));

    create_table_with_bitmap_indexes(
        &db,
        "parents",
        vec![col_def(1, "id", TypeId::Int64, pk_flags())],
        Vec::new(),
    );
    create_table_with_bitmap_indexes(
        &db,
        "logs",
        vec![
            col_def(1, "id", TypeId::Int64, pk_flags()),
            col_def(2, "parent_id", TypeId::Int64, ColumnFlags::empty()),
        ],
        vec![2],
    );

    let trigger = StoredTrigger::new(
        "parents_ad",
        TriggerDefinition {
            target: TriggerTarget::Table("parents".into()),
            timing: TriggerTiming::After,
            event: TriggerEvent::Delete,
            update_of: Vec::new(),
            target_columns: Vec::<ColumnDef>::new(),
            when: None,
            program: TriggerProgram {
                steps: vec![TriggerStep::DeleteWhere {
                    table: "logs".into(),
                    conditions: vec![TriggerCondition::Eq {
                        column_id: 2,
                        value: TriggerValue::OldColumn(1),
                    }],
                }],
            },
        },
        0,
    )
    .unwrap();
    request(
        &app,
        "POST",
        "/triggers",
        Some(serde_json::json!({ "trigger": trigger })),
        200,
    )
    .await;

    kit_txn(
        &app,
        vec![
            serde_json::json!({"put": {"table": "parents", "cells": [1, 1]}}),
            serde_json::json!({"put": {"table": "parents", "cells": [1, 2]}}),
            serde_json::json!({"put": {"table": "logs", "cells": [1, 1, 2, 1]}}),
            serde_json::json!({"put": {"table": "logs", "cells": [1, 2, 2, 1]}}),
            serde_json::json!({"put": {"table": "logs", "cells": [1, 3, 2, 2]}}),
        ],
    )
    .await;

    kit_txn(
        &app,
        vec![serde_json::json!({"delete_by_pk": {"table": "parents", "pk": 1}})],
    )
    .await;

    let logs_count = request(&app, "GET", "/tables/logs/count", None, 200).await;
    assert_eq!(logs_count["count"], 1);

    let rows = kit_query(
        &app,
        "logs",
        vec![serde_json::json!({"bitmap_eq": {"column_id": 2, "value": 2}})],
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(cell_value(&rows[0], 1).unwrap().as_u64().unwrap(), 3);
}

#[tokio::test]
async fn trigger_update_where_cascades_value() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let app = build_app(Arc::clone(&db));

    create_table_with_bitmap_indexes(
        &db,
        "parents",
        vec![
            col_def(1, "id", TypeId::Int64, pk_flags()),
            col_def(2, "status", TypeId::Int64, ColumnFlags::empty()),
        ],
        Vec::new(),
    );
    create_table_with_bitmap_indexes(
        &db,
        "children",
        vec![
            col_def(1, "id", TypeId::Int64, pk_flags()),
            col_def(2, "parent_id", TypeId::Int64, ColumnFlags::empty()),
            col_def(3, "status", TypeId::Int64, ColumnFlags::empty()),
        ],
        vec![2],
    );

    let trigger = StoredTrigger::new(
        "parents_au",
        TriggerDefinition {
            target: TriggerTarget::Table("parents".into()),
            timing: TriggerTiming::After,
            event: TriggerEvent::Update,
            update_of: Vec::new(),
            target_columns: Vec::<ColumnDef>::new(),
            when: None,
            program: TriggerProgram {
                steps: vec![TriggerStep::UpdateWhere {
                    table: "children".into(),
                    conditions: vec![TriggerCondition::Eq {
                        column_id: 2,
                        value: TriggerValue::OldColumn(1),
                    }],
                    cells: vec![TriggerCell {
                        column_id: 3,
                        value: TriggerValue::NewColumn(2),
                    }],
                }],
            },
        },
        0,
    )
    .unwrap();
    request(
        &app,
        "POST",
        "/triggers",
        Some(serde_json::json!({ "trigger": trigger })),
        200,
    )
    .await;

    kit_txn(
        &app,
        vec![
            serde_json::json!({"put": {"table": "parents", "cells": [1, 1, 2, 0]}}),
            serde_json::json!({"put": {"table": "parents", "cells": [1, 2, 2, 0]}}),
            serde_json::json!({"put": {"table": "children", "cells": [1, 1, 2, 1, 3, 0]}}),
            serde_json::json!({"put": {"table": "children", "cells": [1, 2, 2, 1, 3, 0]}}),
            serde_json::json!({"put": {"table": "children", "cells": [1, 3, 2, 2, 3, 0]}}),
        ],
    )
    .await;

    kit_txn(
        &app,
        vec![serde_json::json!({"upsert": {
            "table": "parents",
            "cells": [1, 1, 2, 1],
            "update_cells": [2, 1]
        }})],
    )
    .await;

    let rows = kit_query(
        &app,
        "children",
        vec![serde_json::json!({"bitmap_eq": {"column_id": 2, "value": 1}})],
    )
    .await;
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert_eq!(cell_value(row, 3).unwrap().as_i64().unwrap(), 1);
    }

    let rows = kit_query(
        &app,
        "children",
        vec![serde_json::json!({"bitmap_eq": {"column_id": 2, "value": 2}})],
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(cell_value(&rows[0], 3).unwrap().as_i64().unwrap(), 0);
}
