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
                        flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),            default_value: None,
                    },
                    ColumnDef {
                        id: 2,
                        name: "label".into(),
                        ty: TypeId::Bytes,
                        flags: ColumnFlags::empty(),            default_value: None,
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
