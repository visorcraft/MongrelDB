//! P6.1 — multi-table HTTP server integration test.

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use arrow::ipc::reader::FileReader;
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::TableProviderFilterPushDown;
use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{Database, ModuleCapabilities};
use mongreldb_query::{
    ExternalModuleDescriptor, ExternalPlan, ExternalPlanRequest, ExternalScan, ExternalTable,
    ExternalTableModule, ModuleConnectCtx,
};
use mongreldb_server::{build_app, build_app_with_external_modules};
use std::io::Cursor;
use std::sync::Arc;
use tempfile::tempdir;
use tower::ServiceExt;

struct ServerRowsModule;

impl ExternalTableModule for ServerRowsModule {
    fn name(&self) -> &str {
        "server_rows"
    }

    fn descriptor(&self) -> ExternalModuleDescriptor {
        ExternalModuleDescriptor {
            schema: Schema {
                schema_id: 0,
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
                        name: "label".into(),
                        ty: TypeId::Bytes,
                        flags: ColumnFlags::empty(),
                        default_value: None,
                    },
                ],
                indexes: Vec::new(),
                colocation: Vec::new(),
                constraints: Default::default(),
                clustered: false,
            },
            hidden_columns: Vec::new(),
            capabilities: ModuleCapabilities {
                read_only: true,
                deterministic: true,
                trigger_safe: true,
                ..ModuleCapabilities::default()
            },
        }
    }

    fn connect(
        &self,
        _ctx: &ModuleConnectCtx<'_>,
        _entry: &mongreldb_core::ExternalTableEntry,
    ) -> mongreldb_query::Result<Arc<dyn ExternalTable>> {
        Ok(Arc::new(ServerRowsTable {
            schema: Arc::new(ArrowSchema::new(vec![
                Field::new("id", ArrowDataType::Int64, false),
                Field::new("label", ArrowDataType::Utf8, false),
            ])),
        }))
    }
}

#[derive(Debug)]
struct ServerRowsTable {
    schema: Arc<ArrowSchema>,
}

impl ExternalTable for ServerRowsTable {
    fn schema(&self) -> Arc<ArrowSchema> {
        self.schema.clone()
    }

    fn plan(&self, request: &ExternalPlanRequest<'_>) -> DFResult<ExternalPlan> {
        Ok(ExternalPlan::new(
            request
                .filters
                .iter()
                .map(|_| TableProviderFilterPushDown::Unsupported)
                .collect(),
            Some(2),
            1.0,
            false,
        ))
    }

    fn scan(&self, request: &ExternalPlanRequest<'_>) -> DFResult<ExternalScan> {
        let full_columns = vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["one", "two"])) as ArrayRef,
        ];
        let (schema, columns) = if let Some(projection) = request.projection.as_deref() {
            let fields = projection
                .iter()
                .map(|idx| self.schema.field(*idx).clone())
                .collect::<Vec<_>>();
            let columns = projection
                .iter()
                .map(|idx| full_columns[*idx].clone())
                .collect::<Vec<_>>();
            (Arc::new(ArrowSchema::new(fields)), columns)
        } else {
            (self.schema.clone(), full_columns)
        };
        let batch = RecordBatch::try_new(schema.clone(), columns)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        Ok(ExternalScan {
            schema,
            batches: vec![batch],
        })
    }
}

#[tokio::test]
async fn multi_table_server_endpoints() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let app = build_app(db);

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
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
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
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
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
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["count"], 2);
}

#[tokio::test]
async fn sql_endpoint_uses_startup_external_module_allowlist() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let modules: Vec<Arc<dyn ExternalTableModule>> = vec![Arc::new(ServerRowsModule)];
    let app = build_app_with_external_modules(db, modules);

    use tower::ServiceExt;

    let create_body = serde_json::json!({
        "sql": "CREATE VIRTUAL TABLE app_rows USING server_rows"
    });
    let resp = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/sql")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let query_body = serde_json::json!({
        "sql": "SELECT label FROM app_rows ORDER BY id",
        "format": "arrow",
    });
    let resp = app
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/sql")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(query_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let reader = FileReader::try_new(Cursor::new(body), None).unwrap();
    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let labels = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(labels.value(0), "one");
    assert_eq!(labels.value(1), "two");
}

async fn get_json(app: axum::Router, uri: &str) -> (u16, serde_json::Value) {
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

async fn put_json(
    app: axum::Router,
    uri: &str,
    body: serde_json::Value,
) -> (u16, serde_json::Value) {
    let resp = app
        .oneshot(
            axum::http::Request::builder()
                .method("PUT")
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

async fn post_json(
    app: axum::Router,
    uri: &str,
    body: serde_json::Value,
) -> (u16, serde_json::Value) {
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

#[tokio::test]
async fn history_retention_get_returns_exact_shape() {
    std::env::remove_var("MONGRELDB_HISTORY_RETENTION_EPOCHS");
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let app = build_app(db);
    let (status, body) = get_json(app, "/history/retention").await;
    assert_eq!(status, 200);
    assert_eq!(body.as_object().unwrap().len(), 2);
    assert_eq!(body["history_retention_epochs"], 1024);
    assert_eq!(body["earliest_retained_epoch"], 0);
}

#[tokio::test]
async fn history_retention_put_returns_exact_shape_and_persists() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let app = build_app(Arc::clone(&db));
    let (status, body) = put_json(
        app,
        "/history/retention",
        serde_json::json!({"history_retention_epochs": 7}),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body.as_object().unwrap().len(), 2);
    assert_eq!(body["history_retention_epochs"], 7);
    assert_eq!(body["earliest_retained_epoch"], 0);
    assert_eq!(db.history_retention_epochs(), 7);

    drop(db);
    let reopened = Arc::new(Database::open(dir.path()).unwrap());
    assert_eq!(reopened.history_retention_epochs(), 7);
}

#[tokio::test]
async fn history_retention_put_rejects_non_u64() {
    let dir = tempdir().unwrap();
    let app = build_app(Arc::new(Database::create(dir.path()).unwrap()));
    for value in [
        serde_json::json!(-1),
        serde_json::json!(1.5),
        serde_json::json!("7"),
    ] {
        let (status, _) = put_json(
            app.clone(),
            "/history/retention",
            serde_json::json!({"history_retention_epochs": value}),
        )
        .await;
        assert_eq!(status, 400, "value {value} should be rejected");
    }
}

#[tokio::test]
async fn history_retention_cannot_restore_lost_history() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table(
        "items",
        Schema {
            schema_id: 0,
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
                    ty: TypeId::Int64,
                    flags: ColumnFlags::empty(),
                    default_value: None,
                },
            ],
            indexes: Vec::new(),
            colocation: Vec::new(),
            constraints: Default::default(),
            clustered: false,
        },
    )
    .unwrap();

    let app = build_app(Arc::clone(&db));

    // Establish initial retention window.
    let (status, _) = put_json(
        app.clone(),
        "/history/retention",
        serde_json::json!({"history_retention_epochs": 2}),
    )
    .await;
    assert_eq!(status, 200);

    // Advance visible epochs so that some history is pruned.
    for value in [1, 2, 3, 4] {
        let (status, _) = post_json(
            app.clone(),
            "/tables/items/put",
            serde_json::json!({"row": [1, 1, 2, value]}),
        )
        .await;
        assert_eq!(status, 200);
        let (status, _) =
            post_json(app.clone(), "/tables/items/commit", serde_json::json!({})).await;
        assert_eq!(status, 200);
    }

    let (status, before) = get_json(app.clone(), "/history/retention").await;
    assert_eq!(status, 200);
    assert_eq!(before.as_object().unwrap().len(), 2);
    let earliest_before = before["earliest_retained_epoch"].as_u64().unwrap();
    assert!(earliest_before > 0, "history should have been pruned");

    // Expanding the window cannot restore already-pruned epochs.
    let (status, after) = put_json(
        app.clone(),
        "/history/retention",
        serde_json::json!({"history_retention_epochs": 100}),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(after.as_object().unwrap().len(), 2);
    assert_eq!(
        after["earliest_retained_epoch"].as_u64().unwrap(),
        earliest_before,
        "earliest retained epoch must not move backward"
    );
}
