//! mongreldb-server — a long-lived process holding a multi-table `Database`
//! open, serving SQL + table-qualified native APIs over HTTP.
//!
//! Endpoints:
//!   GET    /health                    → 200 OK
//!   GET    /tables                    → ["t1", "t2", ...]
//!   POST   /tables                    → create table
//!   DELETE /tables/{name}              → drop table
//!   POST   /tables/{name}/put          → upsert one row
//!   POST   /tables/{name}/count        → { "count": N }
//!   POST   /tables/{name}/commit       → { "epoch": N }
//!   POST   /sql                       → Arrow IPC bytes
//!   POST   /txn                       → atomic cross-table transaction
//!
//! Usage: `mongreldb-server <db_dir> [port]`

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::header;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Json;
use mongreldb_core::schema::{Schema, TypeId};
use mongreldb_core::{Database, Value};
use mongreldb_query::{ExternalTableModule, MongrelSession};
use serde::Deserialize;
use serde_json::json;

mod kit;
mod procedure;
mod trigger;

struct AppState {
    db: Arc<Database>,
    idem: kit::IdempotencyStore,
    external_modules: Vec<Arc<dyn ExternalTableModule>>,
}

pub fn build_app(db: Arc<Database>) -> axum::Router {
    build_app_with_external_modules(db, std::iter::empty::<Arc<dyn ExternalTableModule>>())
}

pub fn build_app_with_external_modules(
    db: Arc<Database>,
    external_modules: impl IntoIterator<Item = Arc<dyn ExternalTableModule>>,
) -> axum::Router {
    let state = Arc::new(AppState {
        idem: kit::IdempotencyStore::new(db.root()),
        db,
        external_modules: external_modules.into_iter().collect(),
    });
    axum::Router::new()
        .route("/health", get(health))
        .route("/tables", get(list_tables).post(create_table))
        .route("/tables/{name}", axum::routing::delete(drop_table))
        .route("/tables/{name}/put", post(put_row))
        .route("/tables/{name}/count", get(count))
        .route("/tables/{name}/commit", post(commit))
        .route("/sql", post(sql))
        .route("/txn", post(txn))
        .route("/procedures", get(procedure::list).post(procedure::create))
        .route(
            "/procedures/{name}",
            get(procedure::describe)
                .put(procedure::replace)
                .delete(procedure::drop_procedure),
        )
        .route("/procedures/{name}/call", post(procedure::call))
        .route("/triggers", get(trigger::list).post(trigger::create))
        .route(
            "/triggers/{name}",
            get(trigger::describe)
                .put(trigger::replace)
                .delete(trigger::drop_trigger),
        )
        // Typed Kit-aware surface (authoritative validation + constraints).
        .route("/kit/schema", get(kit::schema_all))
        .route("/kit/schema/{table}", get(kit::schema_one))
        .route("/kit/txn", post(kit::kit_txn))
        .route("/kit/query", post(kit::kit_query))
        .route("/kit/create_table", post(kit::kit_create_table))
        .route("/kit/procedures/{name}/call", post(procedure::kit_call))
        .route("/compact", post(compact_all))
        .route("/tables/{name}/compact", post(compact_table))
        .with_state(state)
}

/// Launch the §5.9 background auto-compaction sweep (run-count cost trigger).
/// One OS thread, sleeping `interval` between sweeps; each tick locks each
/// table individually and calls `Table::maybe_compact`. Best-effort: a
/// compaction error is logged and never aborts the sweep.
pub fn spawn_auto_compactor(db: Arc<Database>) {
    std::thread::Builder::new()
        .name("mongreldb-auto-compact".into())
        .spawn(move || loop {
            std::thread::sleep(std::time::Duration::from_secs(30));
            for name in db.table_names() {
                let Ok(handle) = db.table(&name) else {
                    continue;
                };
                let mut t = handle.lock();
                let before = t.run_count();
                match t.maybe_compact() {
                    Ok(true) => {
                        eprintln!(
                            "[auto-compact] {name}: {} runs -> {}",
                            before,
                            t.run_count()
                        );
                    }
                    Ok(false) => {}
                    Err(e) => {
                        eprintln!("[auto-compact] {name}: compaction failed: {e}");
                    }
                }
            }
        })
        .expect("spawn auto-compact thread");
}

async fn health() -> StatusCode {
    StatusCode::OK
}

/// `POST /compact` — compact all mounted tables.
async fn compact_all(State(state): State<Arc<AppState>>) -> (StatusCode, Json<serde_json::Value>) {
    match state.db.compact() {
        Ok((compacted, skipped)) => (
            StatusCode::OK,
            Json(json!({
                "status": "ok",
                "compacted": compacted,
                "skipped": skipped,
            })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "status": "error", "message": format!("{e}") })),
        ),
    }
}

/// `POST /tables/{name}/compact` — compact a single table.
async fn compact_table(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    match state.db.compact_table(&name) {
        Ok(true) => (
            StatusCode::OK,
            Json(json!({ "status": "compacted", "table": name })),
        ),
        Ok(false) => (
            StatusCode::OK,
            Json(json!({ "status": "skipped", "table": name, "reason": "fewer than 2 runs" })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "status": "error", "table": name, "message": format!("{e}") })),
        ),
    }
}

#[derive(Deserialize)]
struct CreateTableRequest {
    name: String,
    columns: Vec<ColumnDefJson>,
}

#[derive(Deserialize)]
struct ColumnDefJson {
    id: u16,
    name: String,
    ty: String,
    primary_key: bool,
    #[serde(default)]
    nullable: bool,
}

async fn create_table(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateTableRequest>,
) -> Response {
    let mut columns = Vec::new();
    for c in &req.columns {
        let ty = match c.ty.as_str() {
            "int64" | "bigint" => TypeId::Int64,
            "float64" | "double" => TypeId::Float64,
            "bytes" | "varchar" | "text" => TypeId::Bytes,
            "bool" => TypeId::Bool,
            other => {
                return (StatusCode::BAD_REQUEST, format!("unknown type: {other}")).into_response()
            }
        };
        let mut flags = mongreldb_core::schema::ColumnFlags::empty();
        if c.primary_key {
            flags = flags.with(mongreldb_core::schema::ColumnFlags::PRIMARY_KEY);
        }
        if c.nullable {
            flags = flags.with(mongreldb_core::schema::ColumnFlags::NULLABLE);
        }
        columns.push(mongreldb_core::schema::ColumnDef {
            id: c.id,
            name: c.name.clone(),
            ty,
            flags,
        });
    }
    let schema = Schema {
        schema_id: 0,
        columns,
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
    };
    if let Err(msg) = validate_table_name(&req.name) {
        return (StatusCode::BAD_REQUEST, msg).into_response();
    }
    match state.db.create_table(&req.name, schema) {
        Ok(id) => Json(json!({ "table_id": id })).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn list_tables(State(state): State<Arc<AppState>>) -> Json<Vec<String>> {
    Json(state.db.table_names())
}

async fn drop_table(State(state): State<Arc<AppState>>, Path(name): Path<String>) -> Response {
    match state.db.drop_table(&name) {
        Ok(_) => StatusCode::OK.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Deserialize)]
struct PutRequest {
    row: Vec<serde_json::Value>,
}

pub(crate) fn json_to_value(v: &serde_json::Value, expected: TypeId) -> Value {
    match (v, expected) {
        (serde_json::Value::Number(n), TypeId::Float64) => {
            n.as_f64().map(Value::Float64).unwrap_or(Value::Null)
        }
        (serde_json::Value::Number(n), TypeId::Int64) => {
            n.as_i64().map(Value::Int64).unwrap_or(Value::Null)
        }
        (serde_json::Value::String(s), TypeId::Bytes) => Value::Bytes(s.as_bytes().to_vec()),
        (serde_json::Value::Bool(b), TypeId::Bool) => Value::Bool(*b),
        (serde_json::Value::Null, _) => Value::Null,
        // Lenient fallbacks for unknown/loosely-typed JSON.
        (serde_json::Value::Number(n), _) => {
            if let Some(i) = n.as_i64() {
                Value::Int64(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float64(f)
            } else {
                Value::Null
            }
        }
        (serde_json::Value::String(s), _) => Value::Bytes(s.as_bytes().to_vec()),
        (serde_json::Value::Bool(b), _) => Value::Bool(*b),
        _ => Value::Null,
    }
}

/// Parse a flat JSON array `[col_id, val, col_id, val, ...]` into typed cell
/// pairs, validating the schema. Returns `Err(message)` on any malformed pair.
fn parse_cells(
    row: &[serde_json::Value],
    schema: &mongreldb_core::schema::Schema,
) -> Result<Vec<(u16, Value)>, String> {
    if row.len() & 1 != 0 {
        return Err("row must be an even-length array of [col_id, value] pairs".into());
    }
    let mut out = Vec::with_capacity(row.len() / 2);
    for chunk in row.chunks(2) {
        let col_id = chunk[0]
            .as_u64()
            .ok_or("column id must be a non-negative integer")? as u16;
        let expected = schema
            .columns
            .iter()
            .find(|c| c.id == col_id)
            .map(|c| c.ty)
            .ok_or_else(|| format!("unknown column id {col_id}"))?;
        let val = json_to_value(&chunk[1], expected);
        out.push((col_id, val));
    }
    Ok(out)
}

/// Basic validation for a table name: non-empty and no path separators.
pub(crate) fn validate_table_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("table name must not be empty".into());
    }
    if name.contains('/') || name.contains('\\') || name.contains('\0') {
        return Err("table name contains invalid characters".into());
    }
    Ok(())
}

async fn put_row(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(req): Json<PutRequest>,
) -> Response {
    let handle = match state.db.table(&name) {
        Ok(h) => h,
        Err(e) => return (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    };
    let mut g = handle.lock();
    let schema = g.schema().clone();
    let row = match parse_cells(&req.row, &schema) {
        Ok(r) => r,
        Err(msg) => return (StatusCode::BAD_REQUEST, msg).into_response(),
    };
    match g.put(row) {
        Ok(rid) => Json(json!({ "row_id": rid.0.to_string() })).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn count(State(state): State<Arc<AppState>>, Path(name): Path<String>) -> Response {
    let handle = match state.db.table(&name) {
        Ok(h) => h,
        Err(e) => return (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    };
    Json(json!({ "count": handle.lock().count() })).into_response()
}

async fn commit(State(state): State<Arc<AppState>>, Path(name): Path<String>) -> Response {
    let handle = match state.db.table(&name) {
        Ok(h) => h,
        Err(e) => return (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    };
    let mut g = handle.lock();
    match g.commit() {
        Ok(epoch) => Json(json!({ "epoch": epoch.0 })).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Deserialize)]
struct SqlRequest {
    sql: String,
}

async fn sql(State(state): State<Arc<AppState>>, Json(req): Json<SqlRequest>) -> Response {
    let session = match MongrelSession::open_with_external_modules(
        Arc::clone(&state.db),
        state.external_modules.iter().cloned(),
    ) {
        Ok(s) => s,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    match session.run(&req.sql).await {
        Ok(batches) => {
            if batches.is_empty() {
                return StatusCode::OK.into_response();
            }
            let schema = batches[0].schema();
            let mut buf = Vec::new();
            let mut writer =
                arrow::ipc::writer::FileWriter::try_new(&mut buf, schema.as_ref()).unwrap();
            for b in &batches {
                let _ = writer.write(b);
            }
            let _ = writer.finish();
            drop(writer);
            (
                [(header::CONTENT_TYPE, "application/vnd.apache.arrow.file")],
                buf,
            )
                .into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Deserialize)]
struct TxnOp {
    table: String,
    op: String,
    cells: Option<Vec<serde_json::Value>>,
    row_id: Option<u64>,
}

#[derive(Deserialize)]
struct TxnRequest {
    ops: Vec<TxnOp>,
}

async fn txn(State(state): State<Arc<AppState>>, Json(req): Json<TxnRequest>) -> Response {
    // Pre-validate every op against the live schemas before entering the
    // transaction, so malformed input returns 400 without consuming an epoch
    // or poisoning a txn.
    let mut parsed: Vec<(String, TxnAction)> = Vec::with_capacity(req.ops.len());
    for op in &req.ops {
        match op.op.as_str() {
            "put" => {
                let cells_json = match op.cells.as_ref() {
                    Some(c) if !c.is_empty() => c,
                    _ => {
                        return (StatusCode::BAD_REQUEST, "put op requires non-empty cells")
                            .into_response()
                    }
                };
                let handle = match state.db.table(&op.table) {
                    Ok(h) => h,
                    Err(e) => return (StatusCode::NOT_FOUND, e.to_string()).into_response(),
                };
                let schema = handle.lock().schema().clone();
                let cells = match parse_cells(cells_json, &schema) {
                    Ok(c) => c,
                    Err(msg) => return (StatusCode::BAD_REQUEST, msg).into_response(),
                };
                parsed.push((op.table.clone(), TxnAction::Put(cells)));
            }
            "delete" => {
                let rid = match op.row_id {
                    Some(r) => r,
                    None => {
                        return (StatusCode::BAD_REQUEST, "delete op requires row_id")
                            .into_response()
                    }
                };
                parsed.push((op.table.clone(), TxnAction::Delete(rid)));
            }
            other => {
                return (StatusCode::BAD_REQUEST, format!("unknown op: {other}")).into_response()
            }
        }
    }

    let result = state.db.transaction(|t| {
        for (table, action) in &parsed {
            match action {
                TxnAction::Put(cells) => {
                    t.put(table, cells.clone())?;
                }
                TxnAction::Delete(rid) => {
                    t.delete(table, mongreldb_core::RowId(*rid))?;
                }
            }
        }
        Ok(())
    });
    match result {
        Ok(_) => Json(json!({ "status": "committed" })).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

enum TxnAction {
    Put(Vec<(u16, Value)>),
    Delete(u64),
}
