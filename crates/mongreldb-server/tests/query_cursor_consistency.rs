use mongreldb_core::{
    schema::{ColumnDef, ColumnFlags, Schema, TypeId},
    AlterColumn, ColumnMask, Database, MaskStrategy, Permission, PolicyCommand, RowPolicy,
    SecurityCatalog, SecurityExpr, Value,
};
use mongreldb_server::{build_app, build_app_full};
use std::sync::Arc;
use tempfile::{tempdir, TempDir};
use tower::ServiceExt;

fn schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
            },
            ColumnDef {
                id: 2,
                name: "value".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
        ],
        ..Schema::default()
    }
}

async fn post(
    app: axum::Router,
    path: &str,
    body: serde_json::Value,
    authorization: Option<&str>,
) -> (u16, serde_json::Value) {
    let mut request = axum::http::Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json");
    if let Some(authorization) = authorization {
        request = request.header("authorization", authorization);
    }
    let response = app
        .oneshot(
            request
                .body(axum::body::Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = serde_json::from_slice(&bytes).unwrap_or_else(
        |_| serde_json::json!({"message": String::from_utf8_lossy(&bytes).into_owned()}),
    );
    (status, body)
}

async fn setup() -> (TempDir, Arc<Database>, axum::Router) {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("docs", schema()).unwrap();
    let app = build_app(Arc::clone(&db));
    let rows = (1..=5)
        .map(|id| {
            serde_json::json!({
                "put": {"table": "docs", "cells": [1, id, 2, format!("v{id}")]}
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(
        post(
            app.clone(),
            "/kit/txn",
            serde_json::json!({"ops": rows}),
            None
        )
        .await
        .0,
        200
    );
    (dir, db, app)
}

async fn first_cursor(app: axum::Router, authorization: Option<&str>) -> String {
    let (status, body) = post(
        app.clone(),
        "/kit/query",
        serde_json::json!({"table": "docs", "projection": [1, 2], "limit": 2}),
        authorization,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    body["next_cursor"].as_str().unwrap().to_string()
}

async fn continue_query(
    app: axum::Router,
    cursor: &str,
    authorization: Option<&str>,
) -> (u16, serde_json::Value) {
    post(
        app,
        "/kit/query",
        serde_json::json!({
            "table": "docs", "projection": [1, 2], "limit": 2, "cursor": cursor
        }),
        authorization,
    )
    .await
}

#[tokio::test]
async fn query_cursor_continues_without_duplicates_when_generation_is_unchanged() {
    let (_dir, _db, app) = setup().await;
    let cursor = first_cursor(app.clone(), None).await;
    let (status, body) = post(
        app.clone(),
        "/kit/query",
        serde_json::json!({
            "table": "docs", "projection": [2, 1], "limit": 2, "cursor": cursor
        }),
        None,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_ne!(rows[0]["row_id"], rows[1]["row_id"]);
}

#[tokio::test]
async fn query_cursor_fails_stale_after_insert_update_or_delete() {
    for mutation in [
        serde_json::json!({"put": {"table": "docs", "cells": [1, 6, 2, "v6"]}}),
        serde_json::json!({"upsert": {
            "table": "docs", "cells": [1, 3, 2, "v3"], "update_cells": [2, "changed"]
        }}),
        serde_json::json!({"delete_by_pk": {"table": "docs", "pk": 3}}),
    ] {
        let (_dir, _db, app) = setup().await;
        let cursor = first_cursor(app.clone(), None).await;
        assert_eq!(
            post(
                app.clone(),
                "/kit/txn",
                serde_json::json!({"ops": [mutation]}),
                None
            )
            .await
            .0,
            200
        );
        let (status, body) = post(
            app,
            "/kit/query",
            serde_json::json!({
                "table": "docs", "projection": [1, 2], "limit": 2, "cursor": cursor
            }),
            None,
        )
        .await;
        assert_eq!(status, 409, "{body}");
        assert_eq!(body["error"]["code"], "CURSOR_STALE");
    }
}

#[tokio::test]
async fn query_cursor_survives_or_fails_typed_after_flush_checkpoint_and_compaction() {
    for operation in ["flush", "checkpoint", "compact"] {
        let (_dir, db, app) = setup().await;
        db.table("docs").unwrap().lock().force_flush().unwrap();
        if operation == "compact" {
            db.transaction(|transaction| {
                transaction.put(
                    "docs",
                    vec![
                        (1, mongreldb_core::Value::Int64(6)),
                        (2, mongreldb_core::Value::Bytes(b"v6".to_vec())),
                    ],
                )?;
                Ok(())
            })
            .unwrap();
            db.table("docs").unwrap().lock().force_flush().unwrap();
        }
        let cursor = first_cursor(app.clone(), None).await;
        let (before_status, before_body) = continue_query(app.clone(), &cursor, None).await;
        assert_eq!(before_status, 200, "{operation}: {before_body}");
        match operation {
            "flush" => {
                db.table("docs").unwrap().lock().flush().unwrap();
            }
            "checkpoint" => db.checkpoint().unwrap(),
            "compact" => db.table("docs").unwrap().lock().compact().unwrap(),
            _ => unreachable!(),
        }
        let (status, body) = continue_query(app, &cursor, None).await;
        if operation == "flush" || operation == "checkpoint" {
            assert_eq!(status, 200, "{operation}: {body}");
            assert_eq!(body["rows"], before_body["rows"]);
            assert_eq!(body["truncated"], before_body["truncated"]);
        } else {
            assert_eq!(status, 409, "{operation}: {body}");
            assert_eq!(body["error"]["code"], "CURSOR_STALE");
        }
    }
}

#[tokio::test]
async fn query_cursor_is_rejected_after_close_and_reopen() {
    let (dir, db, app) = setup().await;
    let cursor = first_cursor(app.clone(), None).await;
    drop(app);
    db.close().unwrap();
    drop(db);
    let reopened = Arc::new(Database::open(dir.path()).unwrap());
    let (status, body) = continue_query(build_app(reopened), &cursor, None).await;
    assert_eq!(status, 400, "{body}");
}

#[tokio::test]
async fn query_cursor_fails_stale_after_schema_or_security_catalog_change() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create_with_credentials(dir.path(), "admin", "pw").unwrap());
    db.create_table("docs", schema()).unwrap();
    db.transaction(|transaction| {
        for id in 1..=5 {
            transaction.put(
                "docs",
                vec![
                    (1, mongreldb_core::Value::Int64(id)),
                    (2, mongreldb_core::Value::Bytes(vec![id as u8])),
                ],
            )?;
        }
        Ok(())
    })
    .unwrap();
    let app = build_app_full(Arc::clone(&db), std::iter::empty(), None, None, true);
    let admin = "Basic YWRtaW46cHc=";
    let cursor = first_cursor(app.clone(), Some(admin)).await;
    db.alter_column("docs", "value", AlterColumn::rename("payload"))
        .unwrap();
    let (status, body) = post(
        app.clone(),
        "/kit/query",
        serde_json::json!({
            "table": "docs", "projection": [1, 2], "limit": 2, "cursor": cursor
        }),
        Some(admin),
    )
    .await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"]["code"], "CURSOR_STALE");

    let cursor = first_cursor(app.clone(), Some(admin)).await;
    db.set_security_catalog(SecurityCatalog {
        rls_tables: Vec::new(),
        policies: Vec::new(),
        masks: vec![ColumnMask {
            name: "redact".into(),
            table: "docs".into(),
            column: 2,
            strategy: MaskStrategy::Redact {
                replacement: "***".into(),
            },
            exempt_subjects: Vec::new(),
        }],
    })
    .unwrap();
    let (status, body) = post(
        app,
        "/kit/query",
        serde_json::json!({
            "table": "docs", "projection": [1, 2], "limit": 2, "cursor": cursor
        }),
        Some(admin),
    )
    .await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"]["code"], "CURSOR_STALE");
}

#[tokio::test]
async fn query_cursor_fails_stale_after_rls_add_change_remove_and_mask_change() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create_with_credentials(dir.path(), "admin", "pw").unwrap());
    db.create_table("docs", schema()).unwrap();
    db.transaction(|transaction| {
        for id in 1..=5 {
            transaction.put(
                "docs",
                vec![
                    (1, Value::Int64(id)),
                    (2, Value::Bytes(format!("v{id}").into_bytes())),
                ],
            )?;
        }
        Ok(())
    })
    .unwrap();
    let app = build_app_full(Arc::clone(&db), std::iter::empty(), None, None, true);
    let admin = "Basic YWRtaW46cHc=";
    let policy = |value| RowPolicy {
        name: "visible".into(),
        table: "docs".into(),
        command: PolicyCommand::Select,
        subjects: vec!["public".into()],
        permissive: true,
        using: Some(SecurityExpr::ColumnEqValue {
            column: 1,
            value: Value::Int64(value),
        }),
        with_check: None,
    };
    let catalogs = [
        SecurityCatalog {
            rls_tables: vec!["docs".into()],
            policies: vec![policy(1)],
            masks: Vec::new(),
        },
        SecurityCatalog {
            rls_tables: vec!["docs".into()],
            policies: vec![policy(2)],
            masks: Vec::new(),
        },
        SecurityCatalog::default(),
        SecurityCatalog {
            masks: vec![ColumnMask {
                name: "mask".into(),
                table: "docs".into(),
                column: 2,
                strategy: MaskStrategy::Null,
                exempt_subjects: Vec::new(),
            }],
            ..SecurityCatalog::default()
        },
        SecurityCatalog {
            masks: vec![ColumnMask {
                name: "mask".into(),
                table: "docs".into(),
                column: 2,
                strategy: MaskStrategy::Redact {
                    replacement: "***".into(),
                },
                exempt_subjects: Vec::new(),
            }],
            ..SecurityCatalog::default()
        },
    ];
    for catalog in catalogs {
        let cursor = first_cursor(app.clone(), Some(admin)).await;
        db.set_security_catalog(catalog).unwrap();
        let (status, body) = continue_query(app.clone(), &cursor, Some(admin)).await;
        assert_eq!(status, 409, "{body}");
        assert_eq!(body["error"]["code"], "CURSOR_STALE");
    }
}

#[tokio::test]
async fn query_cursor_preserves_first_page_ttl_time() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table(
        "docs",
        Schema {
            schema_id: 1,
            columns: vec![
                ColumnDef {
                    id: 1,
                    name: "id".into(),
                    ty: TypeId::Int64,
                    flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                    default_value: None,
                },
                ColumnDef {
                    id: 2,
                    name: "created_at".into(),
                    ty: TypeId::TimestampNanos,
                    flags: ColumnFlags::empty(),
                    default_value: None,
                },
            ],
            ..Schema::default()
        },
    )
    .unwrap();
    db.set_table_ttl("docs", "created_at", 10_000_000_000)
        .unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    db.transaction(|transaction| {
        for id in 1..=4 {
            transaction.put(
                "docs",
                vec![
                    (1, mongreldb_core::Value::Int64(id)),
                    (2, mongreldb_core::Value::Int64(now - 9_500_000_000)),
                ],
            )?;
        }
        Ok(())
    })
    .unwrap();
    let app = build_app(db);
    let (status, first) = post(
        app.clone(),
        "/kit/query",
        serde_json::json!({"table": "docs", "projection": [1], "limit": 2}),
        None,
    )
    .await;
    assert_eq!(status, 200, "{first}");
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    let (status, second) = post(
        app,
        "/kit/query",
        serde_json::json!({
            "table": "docs", "projection": [1], "limit": 2,
            "cursor": first["next_cursor"]
        }),
        None,
    )
    .await;
    assert_eq!(status, 200, "{second}");
    assert_eq!(second["rows"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn query_cursor_is_mac_request_principal_and_server_bound() {
    let (_dir, db, app) = setup().await;
    let cursor = first_cursor(app.clone(), None).await;

    let mut tampered = cursor.clone().into_bytes();
    let index = tampered.len() / 3;
    tampered[index] = if tampered[index] == b'0' { b'1' } else { b'0' };
    let (status, body) = post(
        app.clone(),
        "/kit/query",
        serde_json::json!({
            "table": "docs", "projection": [1, 2], "limit": 2,
            "cursor": String::from_utf8(tampered).unwrap()
        }),
        None,
    )
    .await;
    assert_eq!(status, 400, "{body}");

    let (status, body) = post(
        app.clone(),
        "/kit/query",
        serde_json::json!({
            "table": "docs", "projection": [1], "limit": 2, "cursor": cursor
        }),
        None,
    )
    .await;
    assert_eq!(status, 400, "{body}");

    let other_server = build_app(db);
    let (status, body) = post(
        other_server,
        "/kit/query",
        serde_json::json!({
            "table": "docs", "projection": [1, 2], "limit": 2, "cursor": cursor
        }),
        None,
    )
    .await;
    assert_eq!(status, 400, "{body}");
}

#[tokio::test]
async fn query_cursor_fails_stale_after_security_or_principal_change() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create_with_credentials(dir.path(), "admin", "pw").unwrap());
    db.create_table("docs", schema()).unwrap();
    db.create_user("alice", "pw").unwrap();
    db.create_user("bob", "pw").unwrap();
    db.create_role("reader").unwrap();
    db.grant_permission(
        "reader",
        Permission::Select {
            table: "docs".into(),
        },
    )
    .unwrap();
    db.grant_role("alice", "reader").unwrap();
    db.grant_role("bob", "reader").unwrap();
    db.transaction(|transaction| {
        for id in 1..=5 {
            transaction.put(
                "docs",
                vec![
                    (1, mongreldb_core::Value::Int64(id)),
                    (2, mongreldb_core::Value::Bytes(vec![id as u8])),
                ],
            )?;
        }
        Ok(())
    })
    .unwrap();
    let app = build_app_full(Arc::clone(&db), std::iter::empty(), None, None, true);
    let alice = "Basic YWxpY2U6cHc=";
    let bob = "Basic Ym9iOnB3";
    let cursor = first_cursor(app.clone(), Some(alice)).await;

    let (status, body) = post(
        app.clone(),
        "/kit/query",
        serde_json::json!({
            "table": "docs", "projection": [1, 2], "limit": 2, "cursor": cursor
        }),
        Some(bob),
    )
    .await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"]["code"], "CURSOR_STALE");

    db.revoke_role("alice", "reader").unwrap();
    let (status, body) = post(
        app,
        "/kit/query",
        serde_json::json!({
            "table": "docs", "projection": [1, 2], "limit": 2, "cursor": cursor
        }),
        Some(alice),
    )
    .await;
    assert_eq!(status, 403, "{body}");
    assert!(body.get("rows").is_none());
}

#[tokio::test]
async fn query_cursor_rechecks_projection_grants_owner_and_admin_state() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create_with_credentials(dir.path(), "admin", "pw").unwrap());
    db.create_table("docs", schema()).unwrap();
    db.transaction(|transaction| {
        for id in 1..=5 {
            transaction.put(
                "docs",
                vec![
                    (1, Value::Int64(id)),
                    (2, Value::Bytes(format!("v{id}").into_bytes())),
                ],
            )?;
        }
        Ok(())
    })
    .unwrap();
    for user in ["alice", "carol", "dave"] {
        db.create_user(user, "pw").unwrap();
    }
    db.create_role("columns").unwrap();
    let both_columns = Permission::SelectColumns {
        table: "docs".into(),
        columns: vec!["id".into(), "value".into()],
    };
    db.grant_permission("columns", both_columns.clone())
        .unwrap();
    db.grant_role("alice", "columns").unwrap();
    db.create_role("reader").unwrap();
    db.grant_permission(
        "reader",
        Permission::Select {
            table: "docs".into(),
        },
    )
    .unwrap();
    db.grant_role("carol", "reader").unwrap();
    db.grant_role("dave", "reader").unwrap();
    let app = build_app_full(Arc::clone(&db), std::iter::empty(), None, None, true);

    let alice = "Basic YWxpY2U6cHc=";
    let cursor = first_cursor(app.clone(), Some(alice)).await;
    db.revoke_permission("columns", both_columns).unwrap();
    db.grant_permission(
        "columns",
        Permission::SelectColumns {
            table: "docs".into(),
            columns: vec!["id".into()],
        },
    )
    .unwrap();
    let (status, body) = continue_query(app.clone(), &cursor, Some(alice)).await;
    assert_eq!(status, 403, "{body}");
    assert!(body.get("rows").is_none());

    let carol = "Basic Y2Fyb2w6cHc=";
    let cursor = first_cursor(app.clone(), Some(carol)).await;
    db.drop_user("carol").unwrap();
    let (status, body) = continue_query(app.clone(), &cursor, Some(carol)).await;
    assert_eq!(status, 401, "{body}");
    assert!(body.get("rows").is_none());

    let dave = "Basic ZGF2ZTpwdw==";
    let cursor = first_cursor(app.clone(), Some(dave)).await;
    db.set_user_admin("dave", true).unwrap();
    let (status, body) = continue_query(app.clone(), &cursor, Some(dave)).await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"]["code"], "CURSOR_STALE");
    let cursor = first_cursor(app.clone(), Some(dave)).await;
    db.set_user_admin("dave", false).unwrap();
    let (status, body) = continue_query(app, &cursor, Some(dave)).await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"]["code"], "CURSOR_STALE");
}
