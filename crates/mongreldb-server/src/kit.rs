//! Typed Kit-aware server endpoints that sit on top of the engine's
//! transactional commit path. These give remote clients an authoritative
//! surface (validation + constraints + sequence allocation executed server-side
//! inside one core transaction) rather than SQL passthrough.
//!
//! Routes:
//!   GET  /kit/schema            → all tables' schema/constraint metadata
//!   GET  /kit/schema/{table}    → one table's metadata (404 if absent)
//!   POST /kit/txn               → typed atomic batch (see [`KitTxnRequest`])
//!
//! Enforcement: every `/kit/txn` runs inside [`Database::transaction`], so the
//! engine's declarative constraints (unique / FK / check) are enforced
//! authoritatively at commit — concurrent conflicting writers cannot both
//! commit, and a violating batch is rejected atomically (no partial commit).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use mongreldb_core::schema::{ColumnFlags, Schema};
use mongreldb_core::txn::{UpsertAction, UpsertActionKind};
use mongreldb_core::{Database, MongrelError, RowId, Value};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as Jval};

use crate::json_to_value;
use crate::AppState;

/// Per-server idempotency store: idempotency key → committed response. A
/// best-effort in-memory cache (single-process daemon); a persistent store is
/// the "full version" item. Per-key locks serialize truly-concurrent identical
/// retries so a key is applied exactly once.
pub struct IdempotencyStore {
    committed: Mutex<HashMap<String, KitTxnResponse>>,
    in_flight: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

impl IdempotencyStore {
    pub fn new() -> Self {
        Self {
            committed: Mutex::new(HashMap::new()),
            in_flight: Mutex::new(HashMap::new()),
        }
    }

    /// Acquire the per-key lock handle (creating it if necessary). Callers hold
    /// this lock across execution so two concurrent identical-key requests run
    /// strictly one-after-the-other.
    fn key_lock(&self, key: &str) -> Arc<Mutex<()>> {
        self.in_flight
            .lock()
            .unwrap()
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    fn get(&self, key: &str) -> Option<KitTxnResponse> {
        self.committed.lock().unwrap().get(key).cloned()
    }

    fn store(&self, key: String, resp: KitTxnResponse) {
        self.committed.lock().unwrap().insert(key, resp);
    }
}

impl Default for IdempotencyStore {
    fn default() -> Self {
        Self::new()
    }
}

// ── Request models ──────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct KitTxnRequest {
    /// Optional idempotency key. A retried request with the same key returns the
    /// original committed response exactly once.
    #[serde(default)]
    pub idempotency_key: Option<String>,
    pub ops: Vec<KitOp>,
}

/// One typed operation in a `/kit/txn` batch (externally tagged: `{"put": …}`).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KitOp {
    Put {
        table: String,
        /// Flat `[col_id, val, col_id, val, …]` cells.
        cells: Vec<Json>,
        /// When true, the per-op result carries the full committed row.
        #[serde(default)]
        returning: bool,
    },
    Upsert {
        table: String,
        cells: Vec<Json>,
        /// Cells applied on conflict (absent ⇒ DO NOTHING).
        update_cells: Option<Vec<Json>>,
        #[serde(default)]
        returning: bool,
    },
    Delete {
        table: String,
        row_id: u64,
    },
    DeleteByPk {
        table: String,
        pk: Json,
    },
}

// ── Response models ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct KitTxnResponse {
    pub status: String,
    pub epoch: u64,
    pub results: Vec<KitOpResult>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum KitOpResult {
    Put {
        row_id: Option<String>,
        auto_inc: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        row: Option<Vec<Json>>,
    },
    Upsert {
        action: String,
        auto_inc: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        row: Option<Vec<Json>>,
    },
    Deleted,
    NotFound,
}

/// Typed error envelope returned on a rejected batch.
#[derive(Debug, Serialize)]
pub struct KitErrorEnvelope {
    pub status: String,
    pub error: KitError,
}

#[derive(Debug, Serialize)]
pub struct KitError {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub op_index: Option<usize>,
}

impl KitError {
    fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            message: message.into(),
            op_index: None,
        }
    }
    fn with_op(mut self, idx: usize) -> Self {
        self.op_index = Some(idx);
        self
    }
}

/// Map an engine error from the commit path to a typed error code.
pub fn error_code(e: &MongrelError) -> &'static str {
    match e {
        MongrelError::Conflict(_) => {
            let m = format!("{e}");
            if m.contains("UNIQUE") {
                "UNIQUE_VIOLATION"
            } else if m.contains("FOREIGN KEY") {
                "FK_VIOLATION"
            } else {
                "CONFLICT"
            }
        }
        MongrelError::InvalidArgument(_) => {
            let m = format!("{e}");
            if m.contains("CHECK constraint") {
                "CHECK_VIOLATION"
            } else {
                "BAD_REQUEST"
            }
        }
        MongrelError::NotFound(_) => "NOT_FOUND",
        _ => "INTERNAL",
    }
}

// ── Handlers ────────────────────────────────────────────────────────────────

pub async fn schema_all(State(state): State<Arc<AppState>>) -> Response {
    let names = state.db.table_names();
    let mut tables = serde_json::Map::new();
    for name in &names {
        if let Ok(h) = state.db.table(name) {
            tables.insert(name.clone(), schema_descriptor(h.lock().schema()));
        }
    }
    Json(json!({ "tables": serde_json::Value::Object(tables) })).into_response()
}

pub async fn schema_one(
    State(state): State<Arc<AppState>>,
    Path(table): Path<String>,
) -> Response {
    match state.db.table(&table) {
        Ok(h) => Json(schema_descriptor(h.lock().schema())).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

fn schema_descriptor(schema: &Schema) -> Json {
    let columns: Vec<Json> = schema
        .columns
        .iter()
        .map(|c| {
            json!({
                "id": c.id,
                "name": c.name,
                "ty": type_name(c.ty),
                "primary_key": c.flags.contains(ColumnFlags::PRIMARY_KEY),
                "nullable": c.flags.contains(ColumnFlags::NULLABLE),
                "auto_increment": c.flags.contains(ColumnFlags::AUTO_INCREMENT),
            })
        })
        .collect();
    let uniques: Vec<Json> = schema
        .constraints
        .uniques
        .iter()
        .map(|u| json!({ "id": u.id, "name": u.name, "columns": u.columns }))
        .collect();
    let fks: Vec<Json> = schema
        .constraints
        .foreign_keys
        .iter()
        .map(|f| {
            json!({
                "id": f.id,
                "name": f.name,
                "columns": f.columns,
                "ref_table": f.ref_table,
                "ref_columns": f.ref_columns,
                "on_delete": format!("{:?}", f.on_delete).to_lowercase(),
            })
        })
        .collect();
    let checks: Vec<Json> = schema
        .constraints
        .checks
        .iter()
        .map(|c| json!({ "id": c.id, "name": c.name }))
        .collect();
    json!({
        "schema_id": schema.schema_id,
        "columns": columns,
        "constraints": { "uniques": uniques, "foreign_keys": fks, "checks": checks },
    })
}

fn type_name(ty: mongreldb_core::schema::TypeId) -> &'static str {
    use mongreldb_core::schema::TypeId::*;
    match ty {
        Bool => "bool",
        Int8 => "int8",
        Int16 => "int16",
        Int32 => "int32",
        Int64 => "int64",
        UInt8 => "uint8",
        UInt16 => "uint16",
        UInt32 => "uint32",
        UInt64 => "uint64",
        Float32 => "float32",
        Float64 => "float64",
        TimestampNanos => "timestamp_nanos",
        Date32 => "date32",
        Bytes => "bytes",
        Embedding { .. } => "embedding",
    }
}

pub async fn kit_txn(
    State(state): State<Arc<AppState>>,
    Json(req): Json<KitTxnRequest>,
) -> Response {
    run_kit_txn(&state, req).await
}

async fn run_kit_txn(state: &AppState, req: KitTxnRequest) -> Response {
    // Idempotency: if a key is present, serialize same-key requests and return
    // any previously-committed response verbatim.
    if let Some(key) = req.idempotency_key.as_deref() {
        if let Some(cached) = state.idem.get(key) {
            return Json(cached).into_response();
        }
        let lock = state.idem.key_lock(key);
        // Hold the per-key lock across execution.
        let _g = lock.lock().unwrap();
        // Re-check after acquiring — a concurrent peer may have committed.
        if let Some(cached) = state.idem.get(key) {
            return Json(cached).into_response();
        }
        let resp = execute_kit_txn(state, &req);
        if let Ok(out) = &resp {
            state.idem.store(key.to_string(), out.clone());
        }
        return resp.into_response();
    }
    execute_kit_txn(state, &req).into_response()
}

fn execute_kit_txn(state: &AppState, req: &KitTxnRequest) -> Result<KitTxnResponse, Response> {
    // 1. Structural pre-validation: resolve each op against the live schemas and
    //    parse cells into typed Values. This gives per-op error attribution
    //    (op_index) for malformed input BEFORE consuming an epoch.
    enum Action {
        Put { table: String, cells: Vec<(u16, Value)> },
        Upsert {
            table: String,
            cells: Vec<(u16, Value)>,
            update_cells: Option<Vec<(u16, Value)>>,
        },
        Delete { table: String, row_id: RowId },
        DeleteByPk { table: String, key: Value },
    }
    struct Parsed {
        returning: bool,
        action: Action,
    }

    let mut parsed: Vec<Parsed> = Vec::with_capacity(req.ops.len());
    for (i, op) in req.ops.iter().enumerate() {
        match op {
            KitOp::Put {
                table,
                cells,
                returning,
            } => {
                let schema = lookup_schema(state, table).map_err(|e| op_error(i, e))?;
                let cells =
                    parse_cells(cells, &schema).map_err(|m| op_error_msg(i, "BAD_REQUEST", m))?;
                parsed.push(Parsed {
                    returning: *returning,
                    action: Action::Put {
                        table: table.clone(),
                        cells,
                    },
                });
            }
            KitOp::Upsert {
                table,
                cells,
                update_cells,
                returning,
            } => {
                let schema = lookup_schema(state, table).map_err(|e| op_error(i, e))?;
                let cells =
                    parse_cells(cells, &schema).map_err(|m| op_error_msg(i, "BAD_REQUEST", m))?;
                let upd = match update_cells {
                    Some(uc) => Some(
                        parse_cells(uc, &schema)
                            .map_err(|m| op_error_msg(i, "BAD_REQUEST", m))?,
                    ),
                    None => None,
                };
                parsed.push(Parsed {
                    returning: *returning,
                    action: Action::Upsert {
                        table: table.clone(),
                        cells,
                        update_cells: upd,
                    },
                });
            }
            KitOp::Delete { table, row_id } => {
                lookup_schema(state, table).map_err(|e| op_error(i, e))?;
                parsed.push(Parsed {
                    returning: false,
                    action: Action::Delete {
                        table: table.clone(),
                        row_id: RowId(*row_id),
                    },
                });
            }
            KitOp::DeleteByPk { table, pk } => {
                let schema = lookup_schema(state, table).map_err(|e| op_error(i, e))?;
                let key = pk_value(pk, &schema).map_err(|m| op_error_msg(i, "BAD_REQUEST", m))?;
                parsed.push(Parsed {
                    returning: false,
                    action: Action::DeleteByPk {
                        table: table.clone(),
                        key,
                    },
                });
            }
        }
    }

    // 2. Execute the whole batch inside ONE core transaction. Constraint
    //    enforcement (unique / FK / check) is authoritative at commit; any
    //    violation aborts the entire batch atomically (no partial commit).
    let db = Arc::clone(&state.db);
    let outcome: mongreldb_core::Result<Vec<KitOpResult>> = db.transaction(|t| {
        let mut results: Vec<KitOpResult> = Vec::with_capacity(parsed.len());
        for p in &parsed {
            match &p.action {
                Action::Put { table, cells } => {
                    let pr = t.put_returning(table, cells.clone())?;
                    results.push(KitOpResult::Put {
                        // The engine allocates physical row ids at commit, so the
                        // id is not surfaced for batch puts. The returned row
                        // carries the PK (and any auto_inc value), which is how
                        // typed clients identify a logical row.
                        row_id: None,
                        auto_inc: pr.auto_inc,
                        row: if p.returning {
                            Some(row_to_json(&pr.row))
                        } else {
                            None
                        },
                    });
                }
                Action::Upsert {
                    table,
                    cells,
                    update_cells,
                } => {
                    let action = match update_cells {
                        Some(uc) => UpsertAction::DoUpdate(uc.clone()),
                        None => UpsertAction::DoNothing,
                    };
                    let ur = t.upsert(table, cells.clone(), action)?;
                    let action_str = match ur.action {
                        UpsertActionKind::Inserted => "inserted",
                        UpsertActionKind::Updated => "updated",
                        UpsertActionKind::Unchanged => "unchanged",
                    };
                    results.push(KitOpResult::Upsert {
                        action: action_str.to_string(),
                        auto_inc: ur.auto_inc,
                        row: if p.returning {
                            Some(row_to_json(&ur.row))
                        } else {
                            None
                        },
                    });
                }
                Action::Delete { table, row_id } => {
                    t.delete(table, *row_id)?;
                    results.push(KitOpResult::Deleted);
                }
                Action::DeleteByPk { table, key } => {
                    let handle = db.table(table)?;
                    let rid = handle.lock().lookup_pk(&key.encode_key());
                    match rid {
                        Some(r) => {
                            t.delete(table, r)?;
                            results.push(KitOpResult::Deleted);
                        }
                        None => results.push(KitOpResult::NotFound),
                    }
                }
            }
        }
        Ok(results)
    });

    let results = match outcome {
        Ok(r) => r,
        Err(e) => {
            let code = error_code(&e);
            return Err((
                StatusCode::CONFLICT,
                Json(KitErrorEnvelope {
                    status: "aborted".into(),
                    error: KitError::new(code, format!("{e}")),
                }),
            )
                .into_response());
        }
    };

    let epoch = state.db.visible_epoch().0;
    Ok(KitTxnResponse {
        status: "committed".into(),
        epoch,
        results,
    })
}

// Helpers ---------------------------------------------------------------------

fn op_error(i: usize, e: MongrelError) -> Response {
    let code = error_code(&e);
    (
        StatusCode::BAD_REQUEST,
        Json(KitErrorEnvelope {
            status: "aborted".into(),
            error: KitError::new(code, format!("{e}")).with_op(i),
        }),
    )
        .into_response()
}

fn op_error_msg(i: usize, code: &str, msg: String) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(KitErrorEnvelope {
            status: "aborted".into(),
            error: KitError::new(code, msg).with_op(i),
        }),
    )
        .into_response()
}

fn lookup_schema(state: &AppState, table: &str) -> std::result::Result<Schema, MongrelError> {
    let h = state.db.table(table)?;
    Ok(h.lock().schema().clone())
}

/// Parse a flat `[col_id, val, …]` cell array against a schema. Reuses the
/// engine Value coercion from the crate root.
fn parse_cells(row: &[Json], schema: &Schema) -> std::result::Result<Vec<(u16, Value)>, String> {
    if row.len() % 2 != 0 {
        return Err("cells must be an even-length [col_id, value, …] array".into());
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
        out.push((col_id, json_to_value(&chunk[1], expected)));
    }
    Ok(out)
}

/// Coerce a PK JSON value against the table's declared primary-key column.
fn pk_value(pk: &Json, schema: &Schema) -> std::result::Result<Value, String> {
    let pk_col = schema.primary_key().ok_or("table has no primary_key column")?;
    Ok(json_to_value(pk, pk_col.ty))
}

fn row_to_json(row: &mongreldb_core::txn::OwnedRow) -> Vec<Json> {
    let mut out: Vec<Json> = Vec::with_capacity(row.columns.len() * 2);
    for (id, v) in &row.columns {
        out.push(json!(id));
        out.push(value_to_json(v));
    }
    out
}

pub(crate) fn value_to_json(v: &Value) -> Json {
    match v {
        Value::Int64(n) => json!(n),
        Value::Float64(f) => serde_json::Number::from_f64(*f)
            .map(Json::Number)
            .unwrap_or(Json::Null),
        Value::Bytes(b) => Json::String(String::from_utf8_lossy(b).into_owned()),
        Value::Bool(b) => Json::Bool(*b),
        Value::Null => Json::Null,
        Value::Embedding(v) => {
            let arr: Vec<Json> = v
                .iter()
                .map(|x| {
                    serde_json::Number::from_f64(*x)
                        .map(Json::Number)
                        .unwrap_or(Json::Null)
                })
                .collect();
            Json::Array(arr)
        }
    }
}
