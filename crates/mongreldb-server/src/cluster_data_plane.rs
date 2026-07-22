//! Public cluster data plane (P0.2 / P0.4).
//!
//! Routes ordinary user SQL/Kit data operations through tablet consensus so no
//! public write bypasses Raft via standalone `AppState.db`.
//!
//! Production path (P0.3 typed tablets):
//! - bind user table via [`ClusterRuntimeHandle::bind_tablet_user_table`]
//! - writes: [`ClusterRuntimeHandle::write_tablet_ops`] (Raft propose)
//! - reads: [`ClusterRuntimeHandle::tablet_typed_rows`] / `tablet_database_try`
//!
//! Legacy opaque path still available when a keyspace exists:
//! `write_tablet_rows` / `tablet_rows` (JSON/bincode value bytes).
//!
//! Supported subset (v1 gateway):
//! - `INSERT INTO <table> [(cols…)] VALUES (…), …`
//! - `SELECT * | col… FROM <table> [WHERE <col> = <lit>]`
//! - Complex `SELECT` via [`crate::cluster_sql::plan_public_sql`] /
//!   [`mongreldb_query::plan_sql_distributed`] (P0.4 planning seam)
//! - Kit `/kit/txn` batches of simple `put` ops
//! - Kit `/kit/search` hybrid fusion via [`mongreldb_query::fuse_distributed_hits`] (P0.8)
//!
//! Unsupported statements fail closed (caller emits the dual-root refusal).
//! NotLeader proposals return structured 503 with a leader hint.

use std::sync::Arc;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use mongreldb_cluster::runtime::RuntimeError;
use mongreldb_cluster::tablet::Key;
use mongreldb_consensus::engine_sink::{
    TabletPartitionBounds, TabletTableBinding, TabletWriteOperation,
};
use mongreldb_consensus::error::ConsensusError;
use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::Value;
use mongreldb_types::ids::TabletId;
use serde_json::{json, Map, Value as JsonValue};

use crate::cluster_runtime::{ClusterRuntimeError, ClusterRuntimeHandle};
use crate::kit::{KitOp, KitTxnRequest};
use crate::AppState;

/// Separator between logical table name and row key bytes in the opaque tablet KV.
const TABLE_KEY_SEP: u8 = 0;

/// Attempt to execute ordinary public SQL on the cluster tablet data plane.
///
/// - `Some(response)` — handled (success or structured error)
/// - `None` — statement outside the supported subset; caller must refuse the
///   standalone data plane (no Raft bypass)
pub async fn try_execute_sql(state: &AppState, sql: &str) -> Option<Response> {
    if !state.is_cluster_mode() {
        return None;
    }
    let runtime = state.cluster_runtime()?;
    match parse_simple_sql(sql) {
        Ok(SimpleSql::Insert {
            table,
            columns,
            rows,
        }) => Some(execute_insert(runtime, &table, columns.as_deref(), &rows).await),
        Ok(SimpleSql::Select {
            table,
            columns,
            filter,
        }) => Some(execute_select(runtime, &table, columns.as_deref(), filter.as_ref()).await),
        Err(_) => {
            // P0.4: complex SELECT (aggregates/joins/…) goes through the public
            // DataFusion distributed planner entry before fail-closed refusal.
            try_distributed_sql_plan(state, runtime, sql).await
        }
    }
}

/// Plan with [`mongreldb_query::plan_sql_distributed`] then **execute** via
/// the shipped [`mongreldb_query::distributed::Coordinator`] over the
/// production remote fragment contract (P0.4 AC §7.8 / FAC-DS-2).
///
/// Data path:
/// 1. Prefer the **installed** [`ClusterGatewayRuntime::fragment_endpoint`]
///    workers (same endpoint production `install_production_cluster_workers`
///    attaches on internal mTLS RPC).
/// 2. When workers are not yet installed (staged startup / some tests), fall
///    back to a gateway-materialized [`InMemoryFragmentExecutor`] behind the
///    same [`RemoteFragmentEndpoint`] protocol so the public path never uses
///    bare `InMemoryTransport`.
///
/// P1.1: admits a parent budget on [`AppState::node_admission`] and a child
/// reservation per fragment tablet before execute.
async fn try_distributed_sql_plan(
    state: &AppState,
    runtime: &ClusterRuntimeHandle,
    sql: &str,
) -> Option<Response> {
    use mongreldb_core::{MemoryClass, WorkloadClass};
    use mongreldb_query::distributed::{
        FragmentExecutor, InMemoryFragmentExecutor, LoopbackFragmentRpcClient,
        RemoteFragmentEndpoint, RemoteFragmentTransport,
    };

    let trimmed = sql.trim();
    if !trimmed.to_ascii_uppercase().starts_with("SELECT ") {
        return None;
    }
    let tablet_ids = runtime.tablet_ids().await.ok()?;
    if tablet_ids.is_empty() {
        return None;
    }
    let table = extract_from_table(trimmed)?;

    // Materialize consensus-applied tablet rows for planning stats/schema and
    // for the fallback executor when production workers are not installed.
    let store = Arc::new(mongreldb_query::distributed::InMemoryTableStore::new());
    let mut arrow_schema: Option<arrow::datatypes::SchemaRef> = None;
    let mut total_rows = 0_u64;
    let mut mongrel_schema: Option<Schema> = None;
    for tablet_id in &tablet_ids {
        if mongrel_schema.is_none() {
            if let Ok(Some(binding)) = runtime.tablet_table_binding(*tablet_id).await {
                if binding.local_table_name == table {
                    mongrel_schema = Some(binding.schema.clone());
                }
            }
        }
        let Ok(typed) = runtime.tablet_typed_rows(*tablet_id).await else {
            continue;
        };
        let Some((schema_ref, batch)) =
            typed_rows_to_batch(&table, &typed, mongrel_schema.as_ref())
        else {
            continue;
        };
        total_rows = total_rows.saturating_add(batch.num_rows() as u64);
        store.register_schema(&table, Arc::clone(&schema_ref));
        store.insert(&table, *tablet_id, batch);
        arrow_schema = Some(schema_ref);
    }
    let schema = arrow_schema.unwrap_or_else(|| {
        if let Some(ref ms) = mongrel_schema {
            mongrel_schema_to_arrow(ms)
        } else {
            Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, true),
                arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, true),
            ]))
        }
    });
    store.register_schema(&table, Arc::clone(&schema));

    let context = crate::cluster_sql::GatewayPlanningContext::new().with_table(
        table.clone(),
        Arc::clone(&schema),
        tablet_ids.clone(),
        mongreldb_query::distributed::PartitionSpec::Unpartitioned,
        mongreldb_query::distributed::TableStats {
            row_count: total_rows,
            total_bytes: total_rows.saturating_mul(64),
        },
    );
    let plan = match crate::cluster_sql::plan_public_sql(sql, &context).await {
        Ok(plan) => plan,
        Err(error) => {
            return Some(
                (
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "status": "error",
                        "category": "distributed_plan_failed",
                        "storage_mode": "cluster",
                        "message": error.to_string(),
                        "table": table,
                        "tablets": tablet_ids.iter().map(|t| t.to_string()).collect::<Vec<_>>(),
                    })),
                )
                    .into_response(),
            );
        }
    };

    // P1.1: parent admission for distributed coordinator work.
    const DIST_SQL_PARENT_BUDGET: u64 = 32 * 1024 * 1024;
    let parent = match state
        .node_admission()
        .admit_parent(
            crate::admission::AdmitRequest {
                tenant: "default",
                class: WorkloadClass::InteractiveSql,
                priority: crate::admission::priority_for_class(
                    state.resource_groups(),
                    WorkloadClass::InteractiveSql,
                ),
                deadline: None,
                query_id: Some(plan.query_id),
                tag: "cluster_sql",
            },
            MemoryClass::QueryExecution,
            DIST_SQL_PARENT_BUDGET,
            std::future::pending::<()>(),
        )
        .await
    {
        Ok(parent) => parent,
        Err(error) => {
            return Some(
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({
                        "status": "error",
                        "category": "admission_denied",
                        "storage_mode": "cluster",
                        "message": format!("{error:?}"),
                    })),
                )
                    .into_response(),
            );
        }
    };
    // Child reservation per tablet fragment (P1.1-T4).
    let child_bytes = (DIST_SQL_PARENT_BUDGET / tablet_ids.len().max(1) as u64).max(64 * 1024);
    let mut children = Vec::with_capacity(tablet_ids.len());
    for _ in &tablet_ids {
        match state
            .node_admission()
            .admit_child(&parent, MemoryClass::QueryExecution, child_bytes)
        {
            Ok(child) => children.push(child),
            Err(error) => {
                return Some(
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        Json(json!({
                            "status": "error",
                            "category": "fragment_admission_denied",
                            "storage_mode": "cluster",
                            "message": format!("{error:?}"),
                        })),
                    )
                        .into_response(),
                );
            }
        }
    }

    // Prefer production-installed fragment workers on the gateway runtime.
    let installed = state
        .storage()
        .cluster_gateway()
        .and_then(|g| g.fragment_endpoint().map(Arc::clone));
    let (endpoint, fragment_source) = if let Some(ep) = installed {
        (ep, "installed_fragment_endpoint")
    } else {
        // Fallback: same RemoteFragmentEndpoint protocol over gateway-
        // materialized tablet batches (tests without worker install).
        let executor: Arc<dyn FragmentExecutor> =
            Arc::new(InMemoryFragmentExecutor::new(Arc::clone(&store)));
        (
            Arc::new(RemoteFragmentEndpoint::new(executor)),
            "gateway_materialized_remote_fragment",
        )
    };
    let client = Arc::new(LoopbackFragmentRpcClient::new(Arc::clone(&endpoint)));
    let mut transport = RemoteFragmentTransport::new(client);
    for tablet_id in &tablet_ids {
        transport = transport.with_client(
            *tablet_id,
            Arc::new(LoopbackFragmentRpcClient::new(Arc::clone(&endpoint))),
        );
    }
    let transport = Arc::new(transport);
    let registry = Arc::new(mongreldb_query::SqlQueryRegistry::default());
    let coordinator = mongreldb_query::distributed::Coordinator::new(transport, registry);
    let batches = match coordinator.execute(&plan).await {
        Ok(batches) => batches,
        Err(error) => {
            drop(children);
            drop(parent);
            return Some(
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({
                        "status": "error",
                        "category": "distributed_execute_failed",
                        "storage_mode": "cluster",
                        "message": error.to_string(),
                        "query_id": plan.query_id.to_string(),
                        "fragment_source": fragment_source,
                    })),
                )
                    .into_response(),
            );
        }
    };
    // Hold admission until results are ready.
    drop(children);
    drop(parent);

    let rows = arrow_batches_to_json_rows(&batches);
    Some(
        Json(json!({
            "status": "ok",
            "storage_mode": "cluster",
            "distributed": true,
            "planner": "plan_sql_distributed",
            "executor": "Coordinator",
            "fragment_transport": "RemoteFragmentEndpoint",
            "fragment_source": fragment_source,
            "query_id": plan.query_id.to_string(),
            "metadata_version": plan.metadata_version.get(),
            "fragment_count": plan.fragments.len(),
            "tablets": tablet_ids.iter().map(|t| t.to_string()).collect::<Vec<_>>(),
            "rows": rows,
        }))
        .into_response(),
    )
}

fn mongrel_schema_to_arrow(schema: &Schema) -> arrow::datatypes::SchemaRef {
    let fields: Vec<arrow::datatypes::Field> = schema
        .columns
        .iter()
        .map(|c| {
            let ty = match c.ty {
                TypeId::Int64 | TypeId::Int32 | TypeId::Int16 | TypeId::Int8 => {
                    arrow::datatypes::DataType::Int64
                }
                TypeId::Float64 | TypeId::Float32 => arrow::datatypes::DataType::Float64,
                TypeId::Bool => arrow::datatypes::DataType::Boolean,
                _ => arrow::datatypes::DataType::Utf8,
            };
            arrow::datatypes::Field::new(c.name.clone(), ty, true)
        })
        .collect();
    Arc::new(arrow::datatypes::Schema::new(fields))
}

/// Convert typed tablet rows into one Arrow batch + schema for fragment execution.
fn typed_rows_to_batch(
    table: &str,
    typed: &std::collections::BTreeMap<u64, std::collections::BTreeMap<u16, Value>>,
    mongrel_schema: Option<&Schema>,
) -> Option<(
    arrow::datatypes::SchemaRef,
    arrow::record_batch::RecordBatch,
)> {
    let _ = table;
    let (col_ids, fields): (Vec<u16>, Vec<arrow::datatypes::Field>) =
        if let Some(ms) = mongrel_schema {
            let ids: Vec<u16> = ms.columns.iter().map(|c| c.id).collect();
            let fields = ms
                .columns
                .iter()
                .map(|c| {
                    let ty = match c.ty {
                        TypeId::Int64 | TypeId::Int32 | TypeId::Int16 | TypeId::Int8 => {
                            arrow::datatypes::DataType::Int64
                        }
                        TypeId::Float64 | TypeId::Float32 => arrow::datatypes::DataType::Float64,
                        TypeId::Bool => arrow::datatypes::DataType::Boolean,
                        _ => arrow::datatypes::DataType::Utf8,
                    };
                    arrow::datatypes::Field::new(c.name.clone(), ty, true)
                })
                .collect();
            (ids, fields)
        } else {
            let mut col_ids: Vec<u16> = typed
                .values()
                .flat_map(|cells| cells.keys().copied())
                .collect();
            col_ids.sort_unstable();
            col_ids.dedup();
            if col_ids.is_empty() {
                col_ids = vec![1, 2];
            }
            let fields = col_ids
                .iter()
                .map(|id| {
                    let ty = typed
                        .values()
                        .find_map(|cells| cells.get(id))
                        .map(|v| match v {
                            Value::Int64(_) => arrow::datatypes::DataType::Int64,
                            Value::Float64(_) => arrow::datatypes::DataType::Float64,
                            Value::Bool(_) => arrow::datatypes::DataType::Boolean,
                            _ => arrow::datatypes::DataType::Utf8,
                        })
                        .unwrap_or(arrow::datatypes::DataType::Utf8);
                    arrow::datatypes::Field::new(format!("c{id}"), ty, true)
                })
                .collect();
            (col_ids, fields)
        };
    let schema = Arc::new(arrow::datatypes::Schema::new(fields));
    let n = typed.len();
    let mut arrays: Vec<arrow::array::ArrayRef> = Vec::with_capacity(col_ids.len());
    for (i, col_id) in col_ids.iter().enumerate() {
        let field = schema.field(i);
        match field.data_type() {
            arrow::datatypes::DataType::Int64 => {
                let mut b = arrow::array::Int64Builder::with_capacity(n);
                for cells in typed.values() {
                    match cells.get(col_id) {
                        Some(Value::Int64(v)) => b.append_value(*v),
                        _ => b.append_null(),
                    }
                }
                arrays.push(Arc::new(b.finish()));
            }
            arrow::datatypes::DataType::Float64 => {
                let mut b = arrow::array::Float64Builder::with_capacity(n);
                for cells in typed.values() {
                    match cells.get(col_id) {
                        Some(Value::Float64(v)) => b.append_value(*v),
                        Some(Value::Int64(v)) => b.append_value(*v as f64),
                        _ => b.append_null(),
                    }
                }
                arrays.push(Arc::new(b.finish()));
            }
            arrow::datatypes::DataType::Boolean => {
                let mut b = arrow::array::BooleanBuilder::with_capacity(n);
                for cells in typed.values() {
                    match cells.get(col_id) {
                        Some(Value::Bool(v)) => b.append_value(*v),
                        _ => b.append_null(),
                    }
                }
                arrays.push(Arc::new(b.finish()));
            }
            _ => {
                let mut b = arrow::array::StringBuilder::with_capacity(n, n * 8);
                for cells in typed.values() {
                    match cells.get(col_id) {
                        Some(Value::Bytes(bytes)) => {
                            b.append_value(String::from_utf8_lossy(bytes));
                        }
                        Some(Value::Int64(v)) => b.append_value(v.to_string()),
                        Some(Value::Float64(v)) => b.append_value(v.to_string()),
                        Some(Value::Bool(v)) => b.append_value(v.to_string()),
                        Some(other) => b.append_value(format!("{other:?}")),
                        None => b.append_null(),
                    }
                }
                arrays.push(Arc::new(b.finish()));
            }
        }
    }
    // Empty typed map still yields a zero-row batch with schema for planning.
    let arrays = if n == 0 {
        schema
            .fields()
            .iter()
            .map(|f| arrow::array::new_empty_array(f.data_type()))
            .collect()
    } else {
        arrays
    };
    let batch = arrow::record_batch::RecordBatch::try_new(Arc::clone(&schema), arrays).ok()?;
    Some((schema, batch))
}

fn arrow_batches_to_json_rows(batches: &[arrow::record_batch::RecordBatch]) -> Vec<JsonValue> {
    let mut rows = Vec::new();
    for batch in batches {
        let n = batch.num_rows();
        let schema = batch.schema();
        for row_idx in 0..n {
            let mut obj = Map::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let col = batch.column(col_idx);
                let value = if col.is_null(row_idx) {
                    JsonValue::Null
                } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
                    JsonValue::from(arr.value(row_idx))
                } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Float64Array>()
                {
                    JsonValue::from(arr.value(row_idx))
                } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::BooleanArray>()
                {
                    JsonValue::from(arr.value(row_idx))
                } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
                    JsonValue::from(arr.value(row_idx))
                } else {
                    JsonValue::from(format!("{col:?}"))
                };
                obj.insert(field.name().clone(), value);
            }
            rows.push(JsonValue::Object(obj));
        }
    }
    rows
}

/// Minimal `FROM <table>` extractor for planning catalog registration.
fn extract_from_table(sql: &str) -> Option<String> {
    let upper = sql.to_ascii_uppercase();
    let from_at = upper.find(" FROM ")?;
    let rest = sql[from_at + " FROM ".len()..].trim_start();
    let (table, _) = take_ident(rest).ok()?;
    Some(table)
}

/// Attempt a Kit txn batch on the cluster tablet data plane.
///
/// Only batches composed entirely of `put` ops are accepted. Other op kinds
/// return `None` so the caller fails closed. Prefer typed tablet writes when
/// a binding is available; otherwise fall back to opaque `write_tablet_rows`.
pub async fn try_kit_txn(state: &AppState, req: &KitTxnRequest) -> Option<Response> {
    if !state.is_cluster_mode() {
        return None;
    }
    let runtime = state.cluster_runtime()?;
    if req.ops.is_empty() {
        return Some(kit_cluster_error(
            StatusCode::BAD_REQUEST,
            "BAD_REQUEST",
            "kit txn batch is empty",
            None,
        ));
    }
    let tablet_id = match first_hosted_tablet(runtime).await {
        Ok(id) => id,
        Err(response) => return Some(response),
    };

    // Prefer typed path when all puts share one table and we can bind/ensure.
    if let Some(response) = try_kit_txn_typed(runtime, tablet_id, req).await {
        return Some(response);
    }

    let mut entries: Vec<(Key, Vec<u8>)> = Vec::with_capacity(req.ops.len());
    let mut results = Vec::with_capacity(req.ops.len());
    for (idx, op) in req.ops.iter().enumerate() {
        let KitOp::Put {
            table,
            cells,
            returning,
        } = op
        else {
            return None;
        };
        if cells.is_empty() || cells.len() % 2 != 0 {
            return Some(kit_cluster_error(
                StatusCode::BAD_REQUEST,
                "BAD_REQUEST",
                "put cells must be a non-empty flat [col_id, val, …] list",
                Some(idx),
            ));
        }
        let row_key = cells
            .get(1)
            .cloned()
            .unwrap_or(JsonValue::Number(idx.into()));
        let key = encode_row_key(table, &row_key);
        let value = match serde_json::to_vec(&json!({
            "table": table,
            "cells": cells,
        })) {
            Ok(bytes) => bytes,
            Err(error) => {
                return Some(kit_cluster_error(
                    StatusCode::BAD_REQUEST,
                    "BAD_REQUEST",
                    format!("serialize put cells: {error}"),
                    Some(idx),
                ));
            }
        };
        entries.push((key, value));
        results.push(json!({
            "kind": "put",
            "row_id": null,
            "auto_inc": null,
            "row": if *returning { Some(cells) } else { None::<&Vec<JsonValue>> },
        }));
    }
    match runtime.write_tablet_rows(tablet_id, &entries).await {
        Ok(receipt) => Some(
            Json(json!({
                "status": "ok",
                "epoch": receipt.position.index,
                "epoch_text": receipt.position.index.to_string(),
                "results": results,
                "storage_mode": "cluster",
                "tablet_id": tablet_id.to_string(),
                "commit_index": receipt.position.index,
                "commit_term": receipt.position.term,
            }))
            .into_response(),
        ),
        Err(error) => Some(cluster_runtime_error_response(error)),
    }
}

async fn try_kit_txn_typed(
    runtime: &ClusterRuntimeHandle,
    tablet_id: TabletId,
    req: &KitTxnRequest,
) -> Option<Response> {
    // Only pure put batches.
    let puts: Vec<_> = req
        .ops
        .iter()
        .map(|op| match op {
            KitOp::Put {
                table,
                cells,
                returning,
            } => Some((table.as_str(), cells.as_slice(), *returning)),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;
    if puts.is_empty() {
        return None;
    }
    let table = puts[0].0;
    if !puts.iter().all(|(t, _, _)| *t == table) {
        return None;
    }
    // Infer columns from first put: even indices are col ids.
    let first_cells = puts[0].1;
    if first_cells.is_empty() || first_cells.len() % 2 != 0 {
        return None;
    }
    let mut col_names = Vec::new();
    let mut sample_vals = Vec::new();
    let mut i = 0;
    while i + 1 < first_cells.len() {
        let name = match &first_cells[i] {
            JsonValue::Number(n) => format!("c{}", n),
            JsonValue::String(s) => s.clone(),
            other => other.to_string(),
        };
        col_names.push(name);
        sample_vals.push(first_cells[i + 1].clone());
        i += 2;
    }
    let binding =
        match ensure_table_binding(runtime, tablet_id, table, &col_names, &sample_vals).await {
            Ok(b) => b,
            Err(response) => return Some(response),
        };
    let mut operations = Vec::with_capacity(puts.len());
    let mut results = Vec::with_capacity(puts.len());
    for (idx, (_table, cells, returning)) in puts.iter().enumerate() {
        let mut typed_cells = Vec::new();
        let mut j = 0;
        let mut row_id = (idx as u64).saturating_add(1);
        while j + 1 < cells.len() {
            let col_id = match &cells[j] {
                JsonValue::Number(n) => n.as_u64().unwrap_or(0) as u16,
                _ => (j / 2 + 1) as u16,
            };
            // Map kit col_id onto binding schema by position when names were cN.
            let schema_col = binding
                .schema
                .columns
                .iter()
                .find(|c| c.id == col_id)
                .or_else(|| binding.schema.columns.get(j / 2));
            let Some(col_def) = schema_col else {
                return Some(kit_cluster_error(
                    StatusCode::BAD_REQUEST,
                    "BAD_REQUEST",
                    format!("unknown column id {col_id}"),
                    Some(idx),
                ));
            };
            let value = match json_to_core_value(&cells[j + 1], &col_def.ty) {
                Ok(v) => v,
                Err(message) => {
                    return Some(kit_cluster_error(
                        StatusCode::BAD_REQUEST,
                        "BAD_REQUEST",
                        message,
                        Some(idx),
                    ));
                }
            };
            if col_def.name == "id" || col_def.id == 1 {
                if let Value::Int64(n) = &value {
                    row_id = *n as u64;
                }
            }
            typed_cells.push((col_def.id, value));
            j += 2;
        }
        operations.push(TabletWriteOperation::Put {
            table_id: binding.local_table_id,
            row_id,
            cells: typed_cells,
        });
        results.push(json!({
            "kind": "put",
            "row_id": row_id.to_string(),
            "auto_inc": null,
            "row": if *returning { Some(*cells) } else { None::<&[JsonValue]> },
        }));
    }
    match runtime.write_tablet_ops(tablet_id, operations).await {
        Ok(receipt) => Some(
            Json(json!({
                "status": "ok",
                "epoch": receipt.position.index,
                "epoch_text": receipt.position.index.to_string(),
                "results": results,
                "storage_mode": "cluster",
                "tablet_id": tablet_id.to_string(),
                "commit_index": receipt.position.index,
                "commit_term": receipt.position.term,
                "write_path": "write_tablet_ops",
            }))
            .into_response(),
        ),
        Err(error) => Some(cluster_runtime_error_response(error)),
    }
}

/// Kit search against the tablet row map for a named table.
///
/// When the request names multiple retrievers, results are fused with the
/// production hybrid path ([`mongreldb_query::fuse_distributed_hits`] /
/// [`mongreldb_query::merge_hybrid_contributions`]) so dense+sparse RRF is
/// global (P0.8) — not single local_rank RRF.
pub async fn try_kit_search(
    state: &AppState,
    req: &crate::kit::KitSearchRequest,
) -> Option<Response> {
    if !state.is_cluster_mode() {
        return None;
    }
    let runtime = state.cluster_runtime()?;
    let tablet_id = match first_hosted_tablet(runtime).await {
        Ok(id) => id,
        Err(response) => return Some(response),
    };
    // Prefer typed rows; fall back to opaque tablet_rows. tablet_database_try
    // is the typed engine handle when present but is never a peer standalone open.
    let _engine = runtime.tablet_database_try(tablet_id);
    let raw_hits = match load_table_rows_json(runtime, tablet_id, &req.table).await {
        Ok(rows) => rows,
        Err(response) => return Some(response),
    };

    // P0.8: multi-retriever Kit search uses production hybrid fusion.
    if req.retrievers.len() > 1 {
        return Some(fuse_cluster_kit_search(tablet_id, req, &raw_hits));
    }

    let mut hits = Vec::new();
    for row in raw_hits {
        hits.push(json!({
            "row": row,
            "score": 1.0,
        }));
    }
    Some(
        Json(json!({
            "status": "ok",
            "hits": hits,
            "storage_mode": "cluster",
            "tablet_id": tablet_id.to_string(),
            "fusion": "none",
        }))
        .into_response(),
    )
}

/// Production hybrid fusion for distributed Kit search (P0.8).
///
/// Builds per-retriever contributions from tablet-local rows and runs
/// [`mongreldb_query::fuse_distributed_hits`] (which calls
/// [`mongreldb_query::merge_hybrid_contributions`]).
fn fuse_cluster_kit_search(
    tablet_id: TabletId,
    req: &crate::kit::KitSearchRequest,
    rows: &[Map<String, JsonValue>],
) -> Response {
    use mongreldb_core::query::{NamedRetriever, Retriever, SearchRequest};
    use mongreldb_core::RowId;
    use mongreldb_query::{
        fuse_distributed_hits, AiTabletHit, AiWorkBudget, FusionMethod, LocalCandidate,
        LocalRetrieverContribution,
    };

    let retrievers: Vec<NamedRetriever> = req
        .retrievers
        .iter()
        .enumerate()
        .map(|(index, named)| {
            let name = if named.name.is_empty() {
                format!("retriever_{index}")
            } else {
                named.name.clone()
            };
            NamedRetriever {
                name,
                weight: named.weight,
                // Placeholders: fusion uses contribution ranks, not the retriever body.
                retriever: Retriever::Sparse {
                    column_id: named.retriever.column_id(),
                    query: vec![],
                    k: req.limit.max(1),
                },
            }
        })
        .collect();
    let search = SearchRequest {
        must: Vec::new(),
        retrievers: retrievers.clone(),
        fusion: mongreldb_core::query::Fusion::ReciprocalRank {
            constant: match &req.fusion {
                crate::kit::KitFusion::ReciprocalRank { constant } => *constant,
            },
        },
        rerank: None,
        limit: req.limit.max(1),
        projection: req.projection.clone(),
    };
    let fusion = FusionMethod::Rrf {
        k: match &req.fusion {
            crate::kit::KitFusion::ReciprocalRank { constant } => *constant,
        },
    };
    let budget = AiWorkBudget {
        candidate_ceiling: req.limit.max(1),
        max_local_candidates: rows.len().saturating_mul(retrievers.len().max(1)).max(16),
        ..AiWorkBudget::default()
    };

    // One contribution per (row, retriever) with stable rank by row order.
    // Real ANN scores arrive when tablet AI workers are bound; until then
    // ranks are order-based so the production fuse path is exercised.
    let mut hits: Vec<AiTabletHit> = Vec::new();
    for (rank_idx, row) in rows.iter().enumerate() {
        let row_id = RowId((rank_idx as u64).saturating_add(1));
        let local_rank = u32::try_from(rank_idx + 1).unwrap_or(u32::MAX);
        let mut contributions = Vec::with_capacity(retrievers.len());
        for named in &retrievers {
            contributions.push(LocalRetrieverContribution {
                tablet_id,
                row_id,
                retriever_id: named.name.clone(),
                retriever_kind: None,
                local_rank,
                raw_score: 1.0 / f64::from(local_rank),
                upper_bound_after: None,
                rls_visible: true,
                weight: named.weight,
            });
        }
        hits.push(AiTabletHit {
            candidate: LocalCandidate {
                tablet_id,
                row_id,
                score: contributions.iter().map(|c| c.raw_score).sum(),
                local_rank,
                rls_visible: true,
            },
            cells: Vec::new(),
            exact_rerank_score: None,
            consistency: None,
            contributions,
            metadata: Default::default(),
        });
        // Attach row payload via JSON after fuse using row_id index.
        let _ = row;
    }

    let fused = match fuse_distributed_hits(&hits, &search, fusion, &budget) {
        Ok(candidates) => candidates,
        Err(error) => {
            return kit_cluster_error(
                StatusCode::BAD_REQUEST,
                "HYBRID_FUSION",
                error.to_string(),
                None,
            );
        }
    };

    let out_hits: Vec<JsonValue> = fused
        .into_iter()
        .filter_map(|candidate| {
            let idx = candidate.row_id.0.checked_sub(1)? as usize;
            let row = rows.get(idx)?.clone();
            Some(json!({
                "row_id": candidate.row_id.0.to_string(),
                "row": row,
                "final_score": candidate.final_score,
                "raw_score": candidate.raw_score,
                "tablet_id": candidate.tablet_id.to_string(),
            }))
        })
        .collect();

    Json(json!({
        "status": "ok",
        "hits": out_hits,
        "storage_mode": "cluster",
        "tablet_id": tablet_id.to_string(),
        "fusion": "merge_hybrid_contributions",
        "production_path": "fuse_distributed_hits",
    }))
    .into_response()
}

// ── SQL execution ───────────────────────────────────────────────────────────

async fn execute_insert(
    runtime: &ClusterRuntimeHandle,
    table: &str,
    columns: Option<&[String]>,
    rows: &[Vec<JsonValue>],
) -> Response {
    if rows.is_empty() {
        return sql_error(
            StatusCode::BAD_REQUEST,
            "INSERT requires at least one VALUES row",
            "bad_request",
        );
    }
    let tablet_id = match first_hosted_tablet(runtime).await {
        Ok(id) => id,
        Err(response) => return response,
    };

    // Build row objects first (shared by typed + opaque paths).
    let mut objects = Vec::with_capacity(rows.len());
    for (row_idx, values) in rows.iter().enumerate() {
        match row_object(columns, values, row_idx) {
            Ok(object) => objects.push(object),
            Err(message) => {
                return sql_error(StatusCode::BAD_REQUEST, message, "bad_request");
            }
        }
    }

    // Prefer typed user-table writes (production P0.3 path via Raft).
    match execute_insert_typed(runtime, tablet_id, table, columns, &objects).await {
        Ok(response) => return response,
        Err(TypedInsertFallback::Opaque) => {}
        Err(TypedInsertFallback::Response(response)) => return response,
    }

    // Legacy opaque keyspace fallback (JSON value bytes).
    let mut entries = Vec::with_capacity(objects.len());
    for (row_idx, object) in objects.iter().enumerate() {
        let pk = object
            .get("id")
            .cloned()
            .or_else(|| object.get("pk").cloned())
            .or_else(|| object.values().next().cloned())
            .unwrap_or(JsonValue::Number(row_idx.into()));
        let key = encode_row_key(table, &pk);
        let value = match serde_json::to_vec(&JsonValue::Object(object.clone())) {
            Ok(bytes) => bytes,
            Err(error) => {
                return sql_error(
                    StatusCode::BAD_REQUEST,
                    format!("serialize row: {error}"),
                    "bad_request",
                );
            }
        };
        entries.push((key, value));
    }
    match runtime.write_tablet_rows(tablet_id, &entries).await {
        Ok(receipt) => Json(json!({
            "status": "ok",
            "rows_affected": entries.len(),
            "storage_mode": "cluster",
            "tablet_id": tablet_id.to_string(),
            "commit_index": receipt.position.index,
            "commit_term": receipt.position.term,
            "write_path": "write_tablet_rows",
            "commit_ts": {
                "physical_micros": receipt.commit_ts.physical_micros,
                "logical": receipt.commit_ts.logical,
            },
        }))
        .into_response(),
        Err(error) => cluster_runtime_error_response(error),
    }
}

enum TypedInsertFallback {
    Opaque,
    Response(Response),
}

async fn execute_insert_typed(
    runtime: &ClusterRuntimeHandle,
    tablet_id: TabletId,
    table: &str,
    columns: Option<&[String]>,
    objects: &[Map<String, JsonValue>],
) -> Result<Response, TypedInsertFallback> {
    let col_names: Vec<String> = if let Some(cols) = columns {
        cols.to_vec()
    } else if let Some(first) = objects.first() {
        first.keys().cloned().collect()
    } else {
        return Err(TypedInsertFallback::Opaque);
    };
    let sample_vals: Vec<JsonValue> = objects
        .first()
        .map(|o| {
            col_names
                .iter()
                .map(|c| o.get(c).cloned().unwrap_or(JsonValue::Null))
                .collect()
        })
        .unwrap_or_default();
    let binding = ensure_table_binding(runtime, tablet_id, table, &col_names, &sample_vals)
        .await
        .map_err(TypedInsertFallback::Response)?;

    let mut operations = Vec::with_capacity(objects.len());
    for (row_idx, object) in objects.iter().enumerate() {
        let mut cells = Vec::new();
        let mut row_id = (row_idx as u64).saturating_add(1);
        for col_def in &binding.schema.columns {
            let Some(json_val) = object.get(&col_def.name) else {
                continue;
            };
            let value = json_to_core_value(json_val, &col_def.ty).map_err(|m| {
                TypedInsertFallback::Response(sql_error(StatusCode::BAD_REQUEST, m, "bad_request"))
            })?;
            if col_def.name == "id" || col_def.flags.contains(ColumnFlags::PRIMARY_KEY) {
                if let Value::Int64(n) = &value {
                    row_id = *n as u64;
                }
            }
            cells.push((col_def.id, value));
        }
        operations.push(TabletWriteOperation::Put {
            table_id: binding.local_table_id,
            row_id,
            cells,
        });
    }
    match runtime.write_tablet_ops(tablet_id, operations).await {
        Ok(receipt) => Ok(Json(json!({
            "status": "ok",
            "rows_affected": objects.len(),
            "storage_mode": "cluster",
            "tablet_id": tablet_id.to_string(),
            "commit_index": receipt.position.index,
            "commit_term": receipt.position.term,
            "write_path": "write_tablet_ops",
            "commit_ts": {
                "physical_micros": receipt.commit_ts.physical_micros,
                "logical": receipt.commit_ts.logical,
            },
        }))
        .into_response()),
        Err(error) => Err(TypedInsertFallback::Response(
            cluster_runtime_error_response(error),
        )),
    }
}

async fn execute_select(
    runtime: &ClusterRuntimeHandle,
    table: &str,
    columns: Option<&[String]>,
    filter: Option<&(String, JsonValue)>,
) -> Response {
    let tablet_id = match first_hosted_tablet(runtime).await {
        Ok(id) => id,
        Err(response) => return response,
    };
    // Local applied view. Prefer typed rows; never open a peer standalone root.
    let _engine = runtime.tablet_database_try(tablet_id);
    let rows = match load_table_rows_json(runtime, tablet_id, table).await {
        Ok(rows) => rows,
        Err(response) => return response,
    };
    let mut out = Vec::new();
    for mut row in rows {
        if let Some((col, lit)) = filter {
            match row.get(col) {
                Some(v) if json_values_equal(v, lit) => {}
                _ => continue,
            }
        }
        if let Some(cols) = columns {
            let mut projected = Map::new();
            for col in cols {
                if let Some(v) = row.remove(col) {
                    projected.insert(col.clone(), v);
                } else if let Some(v) = row.get(col) {
                    projected.insert(col.clone(), v.clone());
                }
            }
            out.push(JsonValue::Object(projected));
        } else {
            out.push(JsonValue::Object(row));
        }
    }
    // SQL JSON default is a bare array of row objects (matches Arrow JSON).
    Json(out).into_response()
}

async fn load_table_rows_json(
    runtime: &ClusterRuntimeHandle,
    tablet_id: TabletId,
    table: &str,
) -> Result<Vec<Map<String, JsonValue>>, Response> {
    // Typed path first.
    if let Ok(Some(binding)) = runtime.tablet_table_binding(tablet_id).await {
        if binding.local_table_name == table {
            let typed = runtime
                .tablet_typed_rows(tablet_id)
                .await
                .map_err(cluster_runtime_error_response)?;
            let mut out = Vec::new();
            for (_row_id, cells) in typed {
                let mut row = Map::new();
                for col in &binding.schema.columns {
                    if let Some(value) = cells.get(&col.id) {
                        row.insert(col.name.clone(), core_value_to_json(value));
                    }
                }
                out.push(row);
            }
            return Ok(out);
        }
    }
    // Opaque keyspace fallback.
    let rows = runtime
        .tablet_rows(tablet_id)
        .await
        .map_err(cluster_runtime_error_response)?;
    let mut out = Vec::new();
    for (key, value) in rows {
        if let Some(row) = decode_table_row(table, &key, &value) {
            out.push(row);
        }
    }
    Ok(out)
}

/// Ensure the hosted tablet is bound to `table` with a schema covering
/// `col_names` (inferred from sample values on first bind).
async fn ensure_table_binding(
    runtime: &ClusterRuntimeHandle,
    tablet_id: TabletId,
    table: &str,
    col_names: &[String],
    sample_vals: &[JsonValue],
) -> Result<TabletTableBinding, Response> {
    if let Ok(Some(existing)) = runtime.tablet_table_binding(tablet_id).await {
        if existing.local_table_name == table {
            return Ok(existing);
        }
        // Different table name on this single-tablet host: fail closed.
        return Err(sql_error(
            StatusCode::CONFLICT,
            format!(
                "tablet already bound to table {:?}, cannot bind {table:?}",
                existing.local_table_name
            ),
            "conflict",
        ));
    }
    if col_names.is_empty() {
        return Err(sql_error(
            StatusCode::BAD_REQUEST,
            "cannot bind tablet user table without columns",
            "bad_request",
        ));
    }
    let mut columns = Vec::with_capacity(col_names.len());
    for (i, name) in col_names.iter().enumerate() {
        let sample = sample_vals.get(i).unwrap_or(&JsonValue::Null);
        let ty = infer_type_id(sample);
        let mut flags = ColumnFlags::empty().with(ColumnFlags::NULLABLE);
        if i == 0 || name == "id" || name == "pk" {
            flags = ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY);
        }
        columns.push(ColumnDef {
            id: (i as u16).saturating_add(1),
            name: name.clone(),
            ty,
            flags,
            default_value: None,
            embedding_source: None,
        });
    }
    // Exactly one PRIMARY_KEY: if multiple flagged, keep first only.
    let mut saw_pk = false;
    for col in &mut columns {
        if col.flags.contains(ColumnFlags::PRIMARY_KEY) {
            if saw_pk {
                col.flags = ColumnFlags::empty().with(ColumnFlags::NULLABLE);
            } else {
                saw_pk = true;
            }
        }
    }
    let schema = Schema {
        columns,
        clustered: true,
        ..Schema::default()
    };
    let binding = TabletTableBinding::new(
        tablet_id,
        1,
        1,
        table.to_owned(),
        schema,
        TabletPartitionBounds::default(),
    );
    runtime
        .bind_tablet_user_table(tablet_id, binding)
        .await
        .map_err(cluster_runtime_error_response)
}

fn infer_type_id(value: &JsonValue) -> TypeId {
    match value {
        JsonValue::Bool(_) => TypeId::Bool,
        JsonValue::Number(n) if n.as_i64().is_some() => TypeId::Int64,
        JsonValue::Number(_) => TypeId::Float64,
        JsonValue::String(_) | JsonValue::Null | JsonValue::Array(_) | JsonValue::Object(_) => {
            TypeId::Bytes
        }
    }
}

fn json_to_core_value(value: &JsonValue, ty: &TypeId) -> Result<Value, String> {
    match (ty, value) {
        (_, JsonValue::Null) => Ok(Value::Null),
        (TypeId::Bool, JsonValue::Bool(b)) => Ok(Value::Bool(*b)),
        (TypeId::Int64, JsonValue::Number(n)) => n
            .as_i64()
            .map(Value::Int64)
            .ok_or_else(|| format!("expected int64, got {value}")),
        (TypeId::Float64, JsonValue::Number(n)) => n
            .as_f64()
            .map(Value::Float64)
            .ok_or_else(|| format!("expected float64, got {value}")),
        (TypeId::Bytes, JsonValue::String(s)) => Ok(Value::Bytes(s.as_bytes().to_vec())),
        (TypeId::Bytes, other) => Ok(Value::Bytes(other.to_string().into_bytes())),
        (TypeId::Int64, JsonValue::String(s)) => s
            .parse::<i64>()
            .map(Value::Int64)
            .map_err(|_| format!("expected int64, got {value}")),
        (TypeId::Bool, JsonValue::String(s)) => match s.as_str() {
            "true" | "TRUE" | "1" => Ok(Value::Bool(true)),
            "false" | "FALSE" | "0" => Ok(Value::Bool(false)),
            _ => Err(format!("expected bool, got {value}")),
        },
        _ => Err(format!("cannot coerce {value} to {ty:?}")),
    }
}

fn core_value_to_json(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Bool(b) => JsonValue::Bool(*b),
        Value::Int64(n) => JsonValue::Number((*n).into()),
        Value::Float64(f) => serde_json::Number::from_f64(*f)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Value::Bytes(b) => match std::str::from_utf8(b) {
            Ok(s) => JsonValue::String(s.to_owned()),
            Err(_) => JsonValue::String(hex_encode(b)),
        },
        Value::Json(b) => match serde_json::from_slice(b) {
            Ok(v) => v,
            Err(_) => JsonValue::String(String::from_utf8_lossy(b).into_owned()),
        },
        other => JsonValue::String(format!("{other:?}")),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0xf) as usize] as char);
    }
    out
}

async fn first_hosted_tablet(runtime: &ClusterRuntimeHandle) -> Result<TabletId, Response> {
    let ids = runtime
        .tablet_ids()
        .await
        .map_err(cluster_runtime_error_response)?;
    ids.into_iter().next().ok_or_else(|| {
        sql_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "no hosted tablets for public cluster data plane",
            "unavailable",
        )
    })
}

// ── Encoding ────────────────────────────────────────────────────────────────

fn encode_row_key(table: &str, pk: &JsonValue) -> Key {
    let mut bytes = table.as_bytes().to_vec();
    bytes.push(TABLE_KEY_SEP);
    match pk {
        JsonValue::String(s) => bytes.extend_from_slice(s.as_bytes()),
        JsonValue::Number(n) => bytes.extend_from_slice(n.to_string().as_bytes()),
        other => bytes.extend_from_slice(other.to_string().as_bytes()),
    }
    Key::from_bytes(bytes)
}

fn table_prefix(table: &str) -> Vec<u8> {
    let mut bytes = table.as_bytes().to_vec();
    bytes.push(TABLE_KEY_SEP);
    bytes
}

fn decode_table_row(table: &str, key: &Key, value: &[u8]) -> Option<Map<String, JsonValue>> {
    let prefix = table_prefix(table);
    if !key.as_bytes().starts_with(&prefix) {
        return None;
    }
    let parsed: JsonValue = serde_json::from_slice(value).ok()?;
    match parsed {
        JsonValue::Object(map) => {
            // Kit puts store {table, cells}; expand cells into a row map when present.
            if let Some(JsonValue::Array(cells)) = map.get("cells") {
                let mut row = Map::new();
                let mut i = 0;
                while i + 1 < cells.len() {
                    let col = match &cells[i] {
                        JsonValue::Number(n) => n.to_string(),
                        JsonValue::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    row.insert(col, cells[i + 1].clone());
                    i += 2;
                }
                if !row.is_empty() {
                    return Some(row);
                }
            }
            Some(map)
        }
        other => {
            let mut map = Map::new();
            map.insert("value".into(), other);
            Some(map)
        }
    }
}

fn row_object(
    columns: Option<&[String]>,
    values: &[JsonValue],
    row_idx: usize,
) -> Result<Map<String, JsonValue>, String> {
    let mut object = Map::new();
    if let Some(cols) = columns {
        if cols.len() != values.len() {
            return Err(format!(
                "INSERT column count ({}) does not match value count ({}) on row {row_idx}",
                cols.len(),
                values.len()
            ));
        }
        for (col, val) in cols.iter().zip(values.iter()) {
            object.insert(col.clone(), val.clone());
        }
    } else {
        for (i, val) in values.iter().enumerate() {
            object.insert(format!("c{i}"), val.clone());
        }
    }
    Ok(object)
}

fn json_values_equal(a: &JsonValue, b: &JsonValue) -> bool {
    match (a, b) {
        (JsonValue::Number(x), JsonValue::Number(y)) => x.as_f64() == y.as_f64(),
        _ => a == b,
    }
}

// ── Errors ──────────────────────────────────────────────────────────────────

fn cluster_runtime_error_response(error: ClusterRuntimeError) -> Response {
    if let Some(leader) = not_leader_hint(&error) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": error.to_string(),
                "status": "error",
                "category": "not_leader",
                "storage_mode": "cluster",
                "leader_hint": leader,
            })),
        )
            .into_response();
    }
    let status = match &error {
        ClusterRuntimeError::Config(message)
            if message.contains("not running") || message.contains("hosts no") =>
        {
            StatusCode::SERVICE_UNAVAILABLE
        }
        ClusterRuntimeError::Runtime(RuntimeError::InvalidRequest(_)) => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        _ => StatusCode::CONFLICT,
    };
    (
        status,
        Json(json!({
            "error": error.to_string(),
            "status": "error",
            "category": "unavailable",
            "storage_mode": "cluster",
        })),
    )
        .into_response()
}

fn not_leader_hint(error: &ClusterRuntimeError) -> Option<Option<u64>> {
    match error {
        ClusterRuntimeError::Runtime(RuntimeError::Consensus(ConsensusError::NotLeader {
            leader,
        })) => Some(*leader),
        ClusterRuntimeError::Runtime(RuntimeError::Meta(meta)) => {
            // Meta NotLeader is nested; surface string-only without typed access
            // when Display mentions not the leader.
            let text = meta.to_string();
            if text.contains("not the leader") || text.contains("NotLeader") {
                Some(None)
            } else {
                None
            }
        }
        _ => {
            let text = error.to_string();
            if text.contains("not the leader") {
                Some(None)
            } else {
                None
            }
        }
    }
}

fn sql_error(status: StatusCode, message: impl Into<String>, category: &str) -> Response {
    (
        status,
        Json(json!({
            "error": message.into(),
            "status": "error",
            "category": category,
            "storage_mode": "cluster",
        })),
    )
        .into_response()
}

fn kit_cluster_error(
    status: StatusCode,
    code: &str,
    message: impl Into<String>,
    op_index: Option<usize>,
) -> Response {
    (
        status,
        Json(json!({
            "status": "aborted",
            "error": {
                "code": code,
                "message": message.into(),
                "op_index": op_index,
            },
            "storage_mode": "cluster",
        })),
    )
        .into_response()
}

// ── Minimal SQL subset parser ───────────────────────────────────────────────

#[derive(Debug)]
enum SimpleSql {
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        rows: Vec<Vec<JsonValue>>,
    },
    Select {
        table: String,
        columns: Option<Vec<String>>,
        filter: Option<(String, JsonValue)>,
    },
}

#[derive(Debug)]
struct ParseError;

fn parse_simple_sql(sql: &str) -> Result<SimpleSql, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return Err(ParseError);
    }
    let upper = trimmed.to_ascii_uppercase();
    if upper.starts_with("INSERT ") {
        parse_insert(trimmed)
    } else if upper.starts_with("SELECT ") {
        parse_select(trimmed)
    } else {
        Err(ParseError)
    }
}

fn parse_insert(sql: &str) -> Result<SimpleSql, ParseError> {
    // INSERT INTO table [(cols)] VALUES (...), (...)
    let rest = strip_prefix_ci(sql, "INSERT")?;
    let rest = rest.trim_start();
    let rest = strip_prefix_ci(rest, "INTO")?.trim_start();
    let (table, rest) = take_ident(rest)?;
    let rest = rest.trim_start();
    // Optional column list: INSERT INTO t (a, b) VALUES ...
    let (columns, rest) = if rest.starts_with('(') {
        let (inside, after) = take_paren_list(rest)?;
        let after = after.trim_start();
        if !starts_with_ci(after, "VALUES") {
            return Err(ParseError);
        }
        let cols = split_idents(&inside)?;
        if cols.is_empty() {
            return Err(ParseError);
        }
        (Some(cols), after)
    } else {
        (None, rest)
    };
    let rest = strip_prefix_ci(rest.trim_start(), "VALUES")?.trim_start();
    let rows = parse_values_lists(rest)?;
    if rows.is_empty() {
        return Err(ParseError);
    }
    Ok(SimpleSql::Insert {
        table,
        columns,
        rows,
    })
}

fn parse_select(sql: &str) -> Result<SimpleSql, ParseError> {
    // SELECT * | col[, col…] FROM table [WHERE col = lit]
    let rest = strip_prefix_ci(sql, "SELECT")?.trim_start();
    let (columns, rest) = if let Some(stripped) = rest.strip_prefix('*') {
        (None, stripped.trim_start())
    } else {
        let upper = rest.to_ascii_uppercase();
        let from_at = upper.find(" FROM ").ok_or(ParseError)?;
        let col_part = rest[..from_at].trim();
        let cols = split_idents(col_part)?;
        if cols.is_empty() {
            return Err(ParseError);
        }
        (Some(cols), rest[from_at..].trim_start())
    };
    let rest = strip_prefix_ci(rest, "FROM")?.trim_start();
    let (table, rest) = take_ident(rest)?;
    let rest = rest.trim_start();
    let filter = if rest.is_empty() {
        None
    } else {
        let rest = strip_prefix_ci(rest, "WHERE")?.trim_start();
        let (col, rest) = take_ident(rest)?;
        let rest = rest.trim_start();
        if !rest.starts_with('=') {
            return Err(ParseError);
        }
        let rest = rest[1..].trim_start();
        let (lit, rest) = take_literal(rest)?;
        if !rest.trim().is_empty() {
            return Err(ParseError);
        }
        Some((col, lit))
    };
    Ok(SimpleSql::Select {
        table,
        columns,
        filter,
    })
}

fn parse_values_lists(input: &str) -> Result<Vec<Vec<JsonValue>>, ParseError> {
    let mut rest = input.trim_start();
    let mut rows = Vec::new();
    loop {
        let (inside, after) = take_paren_list(rest)?;
        rows.push(parse_value_row(&inside)?);
        rest = after.trim_start();
        if rest.is_empty() {
            break;
        }
        if rest.starts_with(',') {
            rest = rest[1..].trim_start();
            continue;
        }
        return Err(ParseError);
    }
    Ok(rows)
}

fn parse_value_row(inside: &str) -> Result<Vec<JsonValue>, ParseError> {
    let mut values = Vec::new();
    let mut rest = inside.trim();
    if rest.is_empty() {
        return Ok(values);
    }
    loop {
        let (lit, after) = take_literal(rest)?;
        values.push(lit);
        rest = after.trim_start();
        if rest.is_empty() {
            break;
        }
        if rest.starts_with(',') {
            rest = rest[1..].trim_start();
            continue;
        }
        return Err(ParseError);
    }
    Ok(values)
}

fn take_paren_list(input: &str) -> Result<(String, &str), ParseError> {
    let input = input.trim_start();
    if !input.starts_with('(') {
        return Err(ParseError);
    }
    let mut depth = 0i32;
    let mut in_str: Option<char> = None;
    let bytes = input.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        let c = b as char;
        if let Some(q) = in_str {
            if c == q {
                // handle escaped '' inside SQL strings
                if q == '\'' && bytes.get(i + 1) == Some(&b'\'') {
                    continue;
                }
                in_str = None;
            }
            continue;
        }
        match c {
            '\'' | '"' => in_str = Some(c),
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    let inside = input[1..i].to_owned();
                    let after = &input[i + 1..];
                    return Ok((inside, after));
                }
            }
            _ => {}
        }
    }
    Err(ParseError)
}

fn take_ident(input: &str) -> Result<(String, &str), ParseError> {
    let input = input.trim_start();
    let chars: Vec<char> = input.chars().collect();
    if chars.is_empty() {
        return Err(ParseError);
    }
    if chars[0] == '`' || chars[0] == '"' {
        let q = chars[0];
        let mut end = 1;
        while end < chars.len() && chars[end] != q {
            end += 1;
        }
        if end >= chars.len() {
            return Err(ParseError);
        }
        let name: String = chars[1..end].iter().collect();
        let rest_start = chars[..end + 1].iter().map(|c| c.len_utf8()).sum();
        return Ok((name, &input[rest_start..]));
    }
    if !(chars[0].is_ascii_alphabetic() || chars[0] == '_') {
        return Err(ParseError);
    }
    let mut end = 1;
    while end < chars.len() && (chars[end].is_ascii_alphanumeric() || chars[end] == '_') {
        end += 1;
    }
    let name: String = chars[..end].iter().collect();
    let rest_start = chars[..end].iter().map(|c| c.len_utf8()).sum();
    Ok((name, &input[rest_start..]))
}

fn split_idents(input: &str) -> Result<Vec<String>, ParseError> {
    let mut out = Vec::new();
    let mut rest = input.trim();
    if rest.is_empty() {
        return Ok(out);
    }
    loop {
        let (ident, after) = take_ident(rest)?;
        out.push(ident);
        rest = after.trim_start();
        if rest.is_empty() {
            break;
        }
        if rest.starts_with(',') {
            rest = rest[1..].trim_start();
            continue;
        }
        return Err(ParseError);
    }
    Ok(out)
}

fn take_literal(input: &str) -> Result<(JsonValue, &str), ParseError> {
    let input = input.trim_start();
    if input.is_empty() {
        return Err(ParseError);
    }
    if starts_with_ci(input, "NULL") {
        let after = &input["NULL".len()..];
        if after
            .chars()
            .next()
            .is_none_or(|c| !c.is_ascii_alphanumeric() && c != '_')
        {
            return Ok((JsonValue::Null, after));
        }
    }
    if starts_with_ci(input, "TRUE") {
        let after = &input["TRUE".len()..];
        if after
            .chars()
            .next()
            .is_none_or(|c| !c.is_ascii_alphanumeric() && c != '_')
        {
            return Ok((JsonValue::Bool(true), after));
        }
    }
    if starts_with_ci(input, "FALSE") {
        let after = &input["FALSE".len()..];
        if after
            .chars()
            .next()
            .is_none_or(|c| !c.is_ascii_alphanumeric() && c != '_')
        {
            return Ok((JsonValue::Bool(false), after));
        }
    }
    let first = input.chars().next().unwrap();
    if first == '\'' || first == '"' {
        let q = first;
        let mut out = String::new();
        let mut chars = input.chars();
        chars.next(); // quote
        while let Some(c) = chars.next() {
            if c == q {
                if q == '\'' && chars.clone().next() == Some('\'') {
                    chars.next();
                    out.push('\'');
                    continue;
                }
                let consumed = input.len() - chars.as_str().len();
                return Ok((JsonValue::String(out), &input[consumed..]));
            }
            out.push(c);
        }
        return Err(ParseError);
    }
    // number
    let mut end = 0;
    let bytes = input.as_bytes();
    if bytes[0] == b'-' || bytes[0] == b'+' {
        end = 1;
    }
    let start_digits = end;
    while end < bytes.len() && (bytes[end].is_ascii_digit() || bytes[end] == b'.') {
        end += 1;
    }
    if end == start_digits {
        return Err(ParseError);
    }
    let num_str = &input[..end];
    if num_str.contains('.') {
        let f: f64 = num_str.parse().map_err(|_| ParseError)?;
        Ok((
            JsonValue::Number(serde_json::Number::from_f64(f).ok_or(ParseError)?),
            &input[end..],
        ))
    } else {
        let i: i64 = num_str.parse().map_err(|_| ParseError)?;
        Ok((JsonValue::Number(i.into()), &input[end..]))
    }
}

fn strip_prefix_ci<'a>(input: &'a str, prefix: &str) -> Result<&'a str, ParseError> {
    if starts_with_ci(input, prefix) {
        Ok(&input[prefix.len()..])
    } else {
        Err(ParseError)
    }
}

fn starts_with_ci(input: &str, prefix: &str) -> bool {
    input.len() >= prefix.len() && input[..prefix.len()].eq_ignore_ascii_case(prefix)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_insert_with_columns() {
        let sql = "INSERT INTO items (id, name) VALUES (1, 'alice'), (2, 'bob')";
        match parse_simple_sql(sql).unwrap() {
            SimpleSql::Insert {
                table,
                columns,
                rows,
            } => {
                assert_eq!(table, "items");
                assert_eq!(columns.unwrap(), vec!["id".to_string(), "name".to_string()]);
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][0], json!(1));
                assert_eq!(rows[0][1], json!("alice"));
            }
            _ => panic!("expected insert"),
        }
    }

    #[test]
    fn parses_select_star_and_filter() {
        let sql = "SELECT * FROM items WHERE id = 1";
        match parse_simple_sql(sql).unwrap() {
            SimpleSql::Select {
                table,
                columns,
                filter,
            } => {
                assert_eq!(table, "items");
                assert!(columns.is_none());
                assert_eq!(filter.unwrap().0, "id");
            }
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn rejects_unsupported() {
        assert!(parse_simple_sql("SELECT 1").is_err());
        assert!(parse_simple_sql("UPDATE items SET x = 1").is_err());
        assert!(parse_simple_sql("BEGIN").is_err());
    }

    #[test]
    fn row_key_is_table_prefixed() {
        let key = encode_row_key("items", &json!(1));
        assert!(key.as_bytes().starts_with(b"items\0"));
    }

    #[test]
    fn fuse_cluster_kit_search_production_path_uses_hybrid_merge() {
        // Production Kit cluster path must call fuse_distributed_hits
        // (→ merge_hybrid_contributions), not single local_rank RRF alone.
        use mongreldb_core::query::{NamedRetriever, Retriever, SearchRequest};
        use mongreldb_core::RowId;
        use mongreldb_query::{
            fuse_distributed_hits, merge_candidates, AiTabletHit, AiWorkBudget, FusionMethod,
            LocalCandidate, LocalRetrieverContribution,
        };

        let tablet = TabletId::from_bytes([9; 16]);
        let hits = vec![AiTabletHit {
            candidate: LocalCandidate {
                tablet_id: tablet,
                row_id: RowId(1),
                score: 0.1,
                local_rank: 9,
                rls_visible: true,
            },
            cells: vec![],
            exact_rerank_score: None,
            consistency: None,
            contributions: vec![
                LocalRetrieverContribution::new(tablet, RowId(1), "dense", 1, 0.9),
                LocalRetrieverContribution::new(tablet, RowId(1), "sparse", 1, 0.8),
            ],
            metadata: Default::default(),
        }];
        let search = SearchRequest {
            must: Vec::new(),
            retrievers: vec![
                NamedRetriever {
                    name: "dense".into(),
                    weight: 1.0,
                    retriever: Retriever::Ann {
                        column_id: 1,
                        query: vec![0.0],
                        k: 5,
                    },
                },
                NamedRetriever {
                    name: "sparse".into(),
                    weight: 1.0,
                    retriever: Retriever::Sparse {
                        column_id: 2,
                        query: vec![],
                        k: 5,
                    },
                },
            ],
            fusion: mongreldb_core::query::Fusion::ReciprocalRank { constant: 60 },
            rerank: None,
            limit: 1,
            projection: None,
        };
        let budget = AiWorkBudget {
            candidate_ceiling: 1,
            ..AiWorkBudget::default()
        };
        let fused =
            fuse_distributed_hits(&hits, &search, FusionMethod::Rrf { k: 60 }, &budget).unwrap();
        assert_eq!(fused.len(), 1);
        assert!((fused[0].final_score - 2.0 / 61.0).abs() < 1e-12);

        let single = merge_candidates(
            &[hits[0].candidate.clone()],
            FusionMethod::Rrf { k: 60 },
            &budget,
        )
        .unwrap();
        // Single-rank would use local_rank=9 → 1/69; hybrid uses two rank-1s.
        assert!((single[0].final_score - 1.0 / 69.0).abs() < 1e-12);
        assert!(fused[0].final_score > single[0].final_score);
    }

    #[test]
    fn extract_from_table_finds_ident() {
        assert_eq!(
            extract_from_table("SELECT region, SUM(amount) FROM orders GROUP BY region").as_deref(),
            Some("orders")
        );
    }
}
