//! mongreldb-server (Phase 19.3) — a long-lived process holding a `Db` open
//! with all indexes warm, serving SQL + native APIs over HTTP.
//!
//! Endpoints:
//!   GET  /health        → 200 OK
//!   GET  /count         → { "count": N }
//!   POST /sql           → { "sql": "..." } → Arrow IPC bytes
//!   POST /put           → { "row": [[col_id, val], ...] } → { "row_id": N }
//!   POST /delete        → { "row_id": N } → 200 OK
//!   POST /commit        → { "epoch": N }
//!   POST /query         → { "conditions": [...], "projection": [...] } → Arrow IPC
//!
//! Usage: `mongreldb-server <table_dir> [port]`

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::header;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Json;
use mongreldb_core::columnar::NativeColumn;
use mongreldb_core::query::{Condition, Query};
use mongreldb_core::schema::TypeId;
use mongreldb_core::{Db, Value};
use mongreldb_query::MongrelSession;
use serde::{Deserialize, Serialize};

struct AppState {
    session: MongrelSession,
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: {} <table_dir> [port]", args[0]);
        std::process::exit(1);
    }
    let table_dir = &args[1];
    let port: u16 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(8453);

    let db = Db::open(table_dir).unwrap_or_else(|e| {
        eprintln!("failed to open {table_dir}: {e}");
        std::process::exit(1);
    });
    let session = MongrelSession::new(db);
    session.register("t").await.unwrap();

    let state = Arc::new(AppState { session });
    let app = axum::Router::new()
        .route("/health", get(health))
        .route("/count", get(count))
        .route("/sql", post(sql))
        .route("/put", post(put_row))
        .route("/delete", post(delete_row))
        .route("/commit", post(commit))
        .route("/query", post(query))
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    eprintln!("mongreldb-server listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn health() -> &'static str {
    "ok"
}

async fn count(State(state): State<Arc<AppState>>) -> Json<CountResp> {
    let count = state.session.db().lock().unwrap().count();
    Json(CountResp { count })
}

#[derive(Serialize)]
struct CountResp {
    count: u64,
}

#[derive(Deserialize)]
struct SqlReq {
    sql: String,
}

async fn sql(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SqlReq>,
) -> Response {
    match state.session.run(&req.sql).await {
        Ok(batches) => arrow_response(&batches),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Deserialize)]
struct PutReq {
    row: Vec<(u16, serde_json::Value)>,
}

fn json_to_value(v: serde_json::Value) -> Value {
    match v {
        serde_json::Value::Number(n) if n.is_i64() => Value::Int64(n.as_i64().unwrap()),
        serde_json::Value::Number(n) => Value::Float64(n.as_f64().unwrap()),
        serde_json::Value::String(s) => Value::Bytes(s.into_bytes()),
        serde_json::Value::Bool(b) => Value::Bool(b),
        serde_json::Value::Null => Value::Null,
        other => Value::Bytes(other.to_string().into_bytes()),
    }
}

#[derive(Serialize)]
struct PutResp {
    row_id: String,
}

async fn put_row(
    State(state): State<Arc<AppState>>,
    Json(req): Json<PutReq>,
) -> Response {
    let row: Vec<(u16, Value)> = req.row.into_iter().map(|(id, v)| (id, json_to_value(v))).collect();
    match state.session.db().lock().unwrap().put(row) {
        Ok(rid) => Json(PutResp {
            row_id: rid.0.to_string(),
        })
        .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Deserialize)]
struct DeleteReq {
    row_id: u64,
}

async fn delete_row(
    State(state): State<Arc<AppState>>,
    Json(req): Json<DeleteReq>,
) -> Response {
    match state
        .session
        .db()
        .lock()
        .unwrap()
        .delete(mongreldb_core::RowId(req.row_id))
    {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn commit(State(state): State<Arc<AppState>>) -> Response {
    match state.session.db().lock().unwrap().commit() {
        Ok(epoch) => Json(serde_json::json!({ "epoch": epoch.0 })).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Deserialize)]
struct QueryReq {
    conditions: Vec<JsonCondition>,
    projection: Option<Vec<u16>>,
}

#[derive(Deserialize)]
struct JsonCondition {
    kind: String,
    column_id: u16,
    value: Option<serde_json::Value>,
    lo: Option<f64>,
    hi: Option<f64>,
}

async fn query(
    State(state): State<Arc<AppState>>,
    Json(req): Json<QueryReq>,
) -> Response {
    let mut q = Query::new();
    for c in &req.conditions {
        let cond = match c.kind.as_str() {
            "bitmap_eq" => Condition::BitmapEq {
                column_id: c.column_id,
                value: json_to_bytes(c.value.as_ref()),
            },
            "range" => Condition::Range {
                column_id: c.column_id,
                lo: c.lo.unwrap_or(f64::MIN) as i64,
                hi: c.hi.unwrap_or(f64::MAX) as i64,
            },
            "range_f64" => Condition::RangeF64 {
                column_id: c.column_id,
                lo: c.lo.unwrap_or(f64::MIN),
                lo_inclusive: true,
                hi: c.hi.unwrap_or(f64::MAX),
                hi_inclusive: true,
            },
            _ => continue,
        };
        q = q.and(cond);
    }
    let proj = req.projection;
    let result = {
        let mut db = state.session.db().lock().unwrap();
        let snap = db.snapshot();
        db.query_columns_native_cached(&q.conditions, proj.as_deref(), snap)
    };
    match result {
        Ok(Some(cols)) => native_to_arrow_response(&cols),
        Ok(None) => {
            match state.session.run("SELECT * FROM t").await {
                Ok(batches) => arrow_response(&batches),
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
            }
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

fn json_to_bytes(v: Option<&serde_json::Value>) -> Vec<u8> {
    match v {
        Some(serde_json::Value::String(s)) => s.as_bytes().to_vec(),
        Some(other) => other.to_string().into_bytes(),
        None => Vec::new(),
    }
}

fn arrow_response(batches: &[arrow::record_batch::RecordBatch]) -> Response {
    if batches.is_empty() {
        return (StatusCode::OK, Vec::new()).into_response();
    }
    let schema = batches[0].schema();
    let mut buf = Vec::new();
    {
        let mut writer =
            arrow::ipc::writer::FileWriter::try_new(&mut buf, schema.as_ref()).unwrap();
        for b in batches {
            let _ = writer.write(b);
        }
        let _ = writer.finish();
    }
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/vnd.apache.arrow.file")],
        buf,
    )
        .into_response()
}

fn native_to_arrow_response(cols: &[(u16, NativeColumn)]) -> Response {
    let schema_ref = state_schema(cols);
    let arrays: Vec<arrow::array::ArrayRef> = cols
        .iter()
        .map(|(_, col)| {
            let ty = match col {
                NativeColumn::Int64 { .. } => TypeId::Int64,
                NativeColumn::Float64 { .. } => TypeId::Float64,
                _ => TypeId::Bytes,
            };
            mongreldb_query::arrow_conv::native_to_array(ty, col).unwrap()
        })
        .collect();
    let batch = arrow::record_batch::RecordBatch::try_new(schema_ref, arrays).unwrap();
    arrow_response(std::slice::from_ref(&batch))
}

fn state_schema(cols: &[(u16, NativeColumn)]) -> arrow::datatypes::SchemaRef {
    let fields: Vec<arrow::datatypes::Field> = cols
        .iter()
        .map(|(id, col)| {
            let dt = match col {
                NativeColumn::Int64 { .. } => arrow::datatypes::DataType::Int64,
                NativeColumn::Float64 { .. } => arrow::datatypes::DataType::Float64,
                NativeColumn::Bool { .. } => arrow::datatypes::DataType::Boolean,
                NativeColumn::Bytes { .. } => arrow::datatypes::DataType::Utf8,
            };
            arrow::datatypes::Field::new(format!("c{id}"), dt, true)
        })
        .collect();
    Arc::new(arrow::datatypes::Schema::new(fields))
}
