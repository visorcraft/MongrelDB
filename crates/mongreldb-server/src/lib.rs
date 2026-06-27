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

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::header;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Json;
use mongreldb_core::schema::{Schema, TypeId};
use mongreldb_core::{Database, Value};
use mongreldb_query::MongrelSession;
use serde::{Deserialize, Serialize};
use serde_json::json;

struct AppState {
    db: Arc<Database>,
}

pub fn build_app(db: Arc<Database>) -> axum::Router {
    let state = Arc::new(AppState { db });
    axum::Router::new()
        .route("/health", get(health))
        .route("/tables", get(list_tables).post(create_table))
        .route("/tables/{name}", axum::routing::delete(drop_table))
        .route("/tables/{name}/put", post(put_row))
        .route("/tables/{name}/count", get(count))
        .route("/tables/{name}/commit", post(commit))
        .route("/sql", post(sql))
        .route("/txn", post(txn))
        .with_state(state)
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: {} <db_dir> [port]", args[0]);
        std::process::exit(1);
    }
    let db_dir = &args[1];
    let port: u16 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(8453);

    let db = Database::open(db_dir).unwrap_or_else(|e| {
        eprintln!("failed to open {db_dir}: {e}");
        std::process::exit(1);
    });
    let app = build_app(Arc::new(db));

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    eprintln!("mongreldb-server listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn health() -> StatusCode {
    StatusCode::OK
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
                return (StatusCode::BAD_REQUEST, format!("unknown type: {other}"))
                    .into_response()
            }
        };
        let mut flags = mongreldb_core::schema::ColumnFlags::empty();
        if c.primary_key {
            flags = flags.with(mongreldb_core::schema::ColumnFlags::PRIMARY_KEY);
        }
        columns.push(mongreldb_core::schema::ColumnDef {
            id: c.id,
            name: c.name.clone(),
            ty,
            flags,
        });
    }
    let schema = Schema {
        schema_id: 1,
        columns,
        indexes: vec![],
        colocation: vec![],
    };
    match state.db.create_table(&req.name, schema) {
        Ok(id) => Json(json!({ "table_id": id })).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn list_tables(State(state): State<Arc<AppState>>) -> Json<Vec<String>> {
    Json(state.db.table_names())
}

async fn drop_table(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Response {
    match state.db.drop_table(&name) {
        Ok(_) => StatusCode::OK.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Deserialize)]
struct PutRequest {
    row: Vec<serde_json::Value>,
}

fn json_to_value(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int64(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float64(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::Bytes(s.as_bytes().to_vec()),
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Null => Value::Null,
        _ => Value::Null,
    }
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
    let row: Vec<(u16, Value)> = req
        .row
        .chunks(2)
        .filter_map(|chunk| {
            let col_id = chunk.first()?.as_u64()? as u16;
            let val = json_to_value(chunk.get(1)?);
            Some((col_id, val))
        })
        .collect();
    match g.put(row) {
        Ok(rid) => Json(json!({ "row_id": rid.0.to_string() })).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn count(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Response {
    let handle = match state.db.table(&name) {
        Ok(h) => h,
        Err(e) => return (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    };
    Json(json!({ "count": handle.lock().count() })).into_response()
}

async fn commit(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Response {
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

async fn sql(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SqlRequest>,
) -> Response {
    let session = match MongrelSession::open(Arc::clone(&state.db)) {
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
            let mut writer = arrow::ipc::writer::FileWriter::try_new(&mut buf, schema.as_ref())
                .unwrap();
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

async fn txn(
    State(state): State<Arc<AppState>>,
    Json(req): Json<TxnRequest>,
) -> Response {
    let result = state.db.transaction(|t| {
        for op in &req.ops {
            match op.op.as_str() {
                "put" => {
                    let cells: Vec<(u16, Value)> = op
                        .cells
                        .as_ref()
                        .unwrap()
                        .chunks(2)
                        .filter_map(|chunk| {
                            let col_id = chunk.first()?.as_u64()? as u16;
                            let val = json_to_value(chunk.get(1)?);
                            Some((col_id, val))
                        })
                        .collect();
                    t.put(&op.table, cells)?;
                }
                "delete" => {
                    if let Some(rid) = op.row_id {
                        t.delete(&op.table, mongreldb_core::RowId(rid))?;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    });
    match result {
        Ok(_) => Json(json!({ "status": "committed" })).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}
