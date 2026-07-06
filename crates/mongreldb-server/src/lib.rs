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
    auth_token: Option<String>,
    /// When true, authenticate via catalog users (HTTP Basic auth).
    user_auth: bool,
}

pub fn build_app(db: Arc<Database>) -> axum::Router {
    build_app_with_config(db, std::iter::empty::<Arc<dyn ExternalTableModule>>(), None, None)
}

pub fn build_app_with_external_modules(
    db: Arc<Database>,
    external_modules: impl IntoIterator<Item = Arc<dyn ExternalTableModule>>,
) -> axum::Router {
    build_app_with_config(db, external_modules, None, None)
}

/// Build the daemon router with optional auth token and max-connections limit.
pub fn build_app_with_config(
    db: Arc<Database>,
    external_modules: impl IntoIterator<Item = Arc<dyn ExternalTableModule>>,
    auth_token: Option<String>,
    max_connections: Option<usize>,
) -> axum::Router {
    build_app_full(db, external_modules, auth_token, max_connections, false)
}

/// Build the daemon router with full auth configuration including user-based auth.
pub fn build_app_full(
    db: Arc<Database>,
    external_modules: impl IntoIterator<Item = Arc<dyn ExternalTableModule>>,
    auth_token: Option<String>,
    max_connections: Option<usize>,
    user_auth: bool,
) -> axum::Router {
    let state = Arc::new(AppState {
        idem: kit::IdempotencyStore::new(db.root()),
        db,
        external_modules: external_modules.into_iter().collect(),
        auth_token,
        user_auth,
    });
    let router = axum::Router::new()
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
        .route("/wal/stream", get(wal_stream))
        .route("/events", get(events_stream))
        .with_state(state.clone());

    // Apply auth middleware if token auth or user auth is enabled.
    if state.auth_token.is_some() || state.user_auth {
        router.layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ))
    } else {
        router
    }
    .layer({
        // Apply connection limit if configured.
        if let Some(max) = max_connections {
            Some(tower::limit::ConcurrencyLimitLayer::new(max))
        } else {
            None
        }
    })

/// Auth middleware supporting three modes:
/// 1. **Token** (`--auth-token <token>`): checks `Authorization: Bearer <token>`.
/// 2. **User auth** (`--auth-users`): checks `Authorization: Basic <base64(user:pass)>`
///    against catalog users (Argon2id-verified). Injects a `Principal` into
///    request extensions.
/// 3. **Both**: token OR valid user credentials accepted.
async fn auth_middleware(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, axum::http::StatusCode> {
    let header = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    // Mode 1: Token auth (Bearer).
    if let Some(token) = &state.auth_token {
        if let Some(provided) = header.strip_prefix("Bearer ") {
            if provided == token {
                return Ok(next.run(req).await);
            }
        }
    }

    // Mode 2: User auth (Basic).
    if state.user_auth {
        if let Some(encoded) = header.strip_prefix("Basic ") {
            if let Ok(decoded) = base64_decode(encoded) {
                if let Ok(creds) = std::str::from_utf8(&decoded) {
                    if let Some((username, password)) = creds.split_once(':') {
                        if let Ok(Some(_user)) = state.db.verify_user(username, password) {
                            // Inject the principal for permission checks.
                            if let Some(principal) = state.db.resolve_principal(username) {
                                req.extensions_mut().insert(principal);
                            }
                            return Ok(next.run(req).await);
                        }
                    }
                }
            }
        }
    }

    Err(axum::http::StatusCode::UNAUTHORIZED)
}

/// Minimal Base64 decoder (no extra dep).
fn base64_decode(input: &str) -> Result<Vec<u8>, ()> {
    const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let input: Vec<u8> = input.bytes().filter(|&b| b != b'\n' && b != b'\r' && b != b' ').collect();
    let mut out = Vec::with_capacity(input.len() * 3 / 4);
    let mut buf = 0u32;
    let mut bits = 0u32;
    for &b in &input {
        if b == b'=' {
            break;
        }
        let val = TABLE.iter().position(|&t| t == b).ok_or(())? as u32;
        buf = (buf << 6) | val;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    Ok(out)
}

/// `GET /wal/stream?since=<seq>` — stream committed WAL records as
/// newline-delimited JSON for replication followers. Each line is a JSON
/// object `{ "seq": N, "txn_id": N, "op": {...} }`.
async fn wal_stream(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    axum::extract::Query(params): axum::extract::Query<WalStreamParams>,
) -> Result<Response, StatusCode> {
    let db_root = state.db.root().to_path_buf();
    let since = params.since.unwrap_or(0);

    // Read all committed WAL records with seq > since and stream as NDJSON.
    let body = tokio::task::spawn_blocking(move || -> Result<String, String> {
        let records = mongreldb_core::wal::SharedWal::replay(&db_root).map_err(|e| e.to_string())?;
        let mut out = String::new();
        for record in records.iter().filter(|r| r.seq.0 > since) {
            if let Ok(json) = serde_json::to_string(record) {
                out.push_str(&json);
                out.push('\n');
            }
        }
        Ok(out)
    })
    .await
    .map_err(|_e| StatusCode::INTERNAL_SERVER_ERROR)?;

    let body = body.map_err(|e| {
        eprintln!("wal_stream error: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok((
        [
            (header::CONTENT_TYPE, "application/x-ndjson".to_string()),
            (header::CACHE_CONTROL, "no-cache".to_string()),
        ],
        body,
    )
        .into_response())
}

#[derive(serde::Deserialize)]
struct WalStreamParams {
    since: Option<u64>,
}

/// `GET /events` — stream change-data-capture events (from NOTIFY/LISTEN and
/// committed Put/Delete operations) as newline-delimited JSON. Each line is
/// a JSON `ChangeEvent { channel, table, op, epoch, message }`.
async fn events_stream(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
) -> Result<Response, StatusCode> {
    let mut rx = state.db.subscribe_changes();
    let body = tokio::task::spawn_blocking(move || {
        // Drain the current backlog (non-blocking).
        let mut out = String::new();
        while let Ok(event) = rx.try_recv() {
            if let Ok(json) = serde_json::to_string(&event) {
                out.push_str(&json);
                out.push('\n');
            }
        }
        out
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok((
        [
            (header::CONTENT_TYPE, "application/x-ndjson".to_string()),
            (header::CACHE_CONTROL, "no-cache".to_string()),
        ],
        body,
    )
        .into_response())
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
        clustered: false,
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

#[cfg(test)]
mod auth_tests {
    use super::*;
    use mongreldb_core::Database;
    use tempfile::tempdir;

    #[tokio::test]
    async fn auth_rejects_missing_token() {
        let dir = tempdir().unwrap();
        let db = Arc::new(Database::create(dir.path()).unwrap());
        let app = build_app_with_config(db, std::iter::empty(), Some("secret".into()), None);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        // Give the server a moment to start.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let client = reqwest::Client::new();
        let resp = client
            .get(format!("http://{addr}/health"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 401);
    }

    #[tokio::test]
    async fn auth_accepts_valid_token() {
        let dir = tempdir().unwrap();
        let db = Arc::new(Database::create(dir.path()).unwrap());
        let app = build_app_with_config(db, std::iter::empty(), Some("secret".into()), None);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let client = reqwest::Client::new();
        let resp = client
            .get(format!("http://{addr}/health"))
            .header("Authorization", "Bearer secret")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    #[tokio::test]
    async fn no_auth_when_token_unset() {
        let dir = tempdir().unwrap();
        let db = Arc::new(Database::create(dir.path()).unwrap());
        let app = build_app_with_config(db, std::iter::empty(), None, None);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let client = reqwest::Client::new();
        let resp = client
            .get(format!("http://{addr}/health"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }
}

#[cfg(test)]
mod wal_stream_tests {
    use super::*;
    use mongreldb_core::Database;
    use tempfile::tempdir;

    #[tokio::test]
    async fn wal_stream_returns_records_after_commit() {
        let dir = tempdir().unwrap();
        let db = Arc::new(Database::create(dir.path()).unwrap());
        let table_schema = mongreldb_core::schema::Schema {
            schema_id: 1,
            columns: vec![mongreldb_core::schema::ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: mongreldb_core::schema::ColumnFlags::empty()
                    .with(mongreldb_core::schema::ColumnFlags::PRIMARY_KEY),
            }],
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        };
        db.create_table("items", table_schema).unwrap();
        // Write a row to generate WAL records.
        let handle = db.table("items").unwrap();
        handle.lock().put(vec![(1, Value::Int64(1))]).unwrap();
        handle.lock().flush().unwrap();

        let app = build_app(db);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let resp = reqwest::get(format!("http://{addr}/wal/stream")).await.unwrap();
        assert_eq!(resp.status(), 200);
        let body = resp.text().await.unwrap();
        // Should contain at least one record (the flush commit).
        assert!(!body.is_empty(), "wal_stream should return records");
        assert!(body.contains("seq"), "response should contain seq field");
    }
}
