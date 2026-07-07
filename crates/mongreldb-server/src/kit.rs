//! Typed Kit-aware server endpoints that sit on top of the engine's
//! transactional commit path. These give remote clients an authoritative
//! surface (validation + constraints executed server-side inside one core
//! transaction) rather than SQL passthrough.
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
use mongreldb_core::constraint::TableConstraints;
use mongreldb_core::query::{Condition, Query};
use mongreldb_core::schema::{
    ColumnDef, ColumnFlags, DefaultExpr, Schema, TypeId,
};
use mongreldb_core::txn::{UpsertAction, UpsertActionKind};
use mongreldb_core::{MongrelError, RowId, Value};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as Jval};

use crate::json_to_value;
use crate::{validate_table_name, AppState};

/// Per-server idempotency store: idempotency key → committed response, backed
/// by an on-disk `<root>/_idem/` directory so retry-after-restart (not just
/// retry-after-timeout) still returns the original committed response exactly
/// once. The in-memory map is a hot cache; a miss falls through to disk. Per-key
/// locks serialize truly-concurrent identical retries.
pub struct IdempotencyStore {
    dir: std::path::PathBuf,
    committed: Mutex<HashMap<String, KitTxnResponse>>,
    json_committed: Mutex<HashMap<String, Jval>>,
    in_flight: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

impl IdempotencyStore {
    /// Open (or create) the store rooted at `<root>/_idem/`. Best-effort: a
    /// failure to create the directory is not fatal — the store simply behaves
    /// as in-memory-only (disk reads/writes become no-ops on error).
    pub fn new(root: &std::path::Path) -> Self {
        let dir = root.join("_idem");
        let _ = std::fs::create_dir_all(&dir);
        Self {
            dir,
            committed: Mutex::new(HashMap::new()),
            json_committed: Mutex::new(HashMap::new()),
            in_flight: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn key_lock(&self, key: &str) -> Arc<Mutex<()>> {
        self.in_flight
            .lock()
            .unwrap()
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    fn path_for(&self, key: &str) -> std::path::PathBuf {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        key.hash(&mut h);
        self.dir.join(format!("{:016x}.json", h.finish()))
    }

    fn get(&self, key: &str) -> Option<KitTxnResponse> {
        if let Some(v) = self.committed.lock().unwrap().get(key).cloned() {
            return Some(v);
        }
        // Disk fallback (persisted across daemon restarts).
        let path = self.path_for(key);
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(_) => return None,
        };
        match serde_json::from_slice::<KitTxnResponse>(&bytes) {
            Ok(v) => {
                self.committed
                    .lock()
                    .unwrap()
                    .insert(key.to_string(), v.clone());
                Some(v)
            }
            Err(_) => None,
        }
    }

    fn store(&self, key: String, resp: KitTxnResponse) {
        // Atomic write: tmp file in the same dir, then rename.
        let path = self.path_for(&key);
        if let Ok(bytes) = serde_json::to_vec(&resp) {
            let tmp = path.with_extension("json.tmp");
            if std::fs::write(&tmp, &bytes).is_ok() {
                let _ = std::fs::rename(&tmp, &path);
            }
        }
        self.committed.lock().unwrap().insert(key, resp);
    }

    pub(crate) fn get_json(&self, key: &str) -> Option<Jval> {
        if let Some(v) = self.json_committed.lock().unwrap().get(key).cloned() {
            return Some(v);
        }
        let path = self.path_for(key);
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(_) => return None,
        };
        match serde_json::from_slice::<Jval>(&bytes) {
            Ok(v) => {
                self.json_committed
                    .lock()
                    .unwrap()
                    .insert(key.to_string(), v.clone());
                Some(v)
            }
            Err(_) => None,
        }
    }

    pub(crate) fn store_json(&self, key: String, resp: Jval) {
        let path = self.path_for(&key);
        if let Ok(bytes) = serde_json::to_vec(&resp) {
            let tmp = path.with_extension("json.tmp");
            if std::fs::write(&tmp, &bytes).is_ok() {
                let _ = std::fs::rename(&tmp, &path);
            }
        }
        self.json_committed.lock().unwrap().insert(key, resp);
    }
}

// ── Request models ──────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct KitTxnRequest {
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
        cells: Vec<Jval>,
        #[serde(default)]
        returning: bool,
    },
    Upsert {
        table: String,
        cells: Vec<Jval>,
        /// Cells applied on conflict (absent ⇒ DO NOTHING).
        update_cells: Option<Vec<Jval>>,
        #[serde(default)]
        returning: bool,
    },
    Delete {
        table: String,
        row_id: u64,
    },
    DeleteByPk {
        table: String,
        pk: Jval,
    },
}

// ── Response models ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KitTxnResponse {
    pub status: String,
    pub epoch: u64,
    pub results: Vec<KitOpResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum KitOpResult {
    Put {
        /// The engine allocates physical row ids at commit, so this is `None`
        /// for batch puts. The returned `row` carries the PK (and any auto_inc),
        /// which is how typed clients identify a logical row.
        row_id: Option<String>,
        auto_inc: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        row: Option<Vec<Jval>>,
    },
    Upsert {
        action: String,
        auto_inc: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        row: Option<Vec<Jval>>,
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
            if is_trigger_error(&m) {
                "TRIGGER_VALIDATION"
            } else if m.contains("UNIQUE") {
                "UNIQUE_VIOLATION"
            } else if m.contains("FOREIGN KEY") {
                "FK_VIOLATION"
            } else {
                "CONFLICT"
            }
        }
        MongrelError::InvalidArgument(_) => {
            let m = format!("{e}");
            if is_trigger_error(&m) {
                "TRIGGER_VALIDATION"
            } else if m.contains("CHECK constraint") {
                "CHECK_VIOLATION"
            } else {
                "BAD_REQUEST"
            }
        }
        MongrelError::NotFound(_) => "NOT_FOUND",
        _ => "INTERNAL",
    }
}

fn is_trigger_error(message: &str) -> bool {
    message.contains("trigger ")
        || message.contains("Trigger ")
        || message.contains("external trigger bridge")
}

// ── Metadata handlers ───────────────────────────────────────────────────────

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

pub async fn schema_one(State(state): State<Arc<AppState>>, Path(table): Path<String>) -> Response {
    match state.db.table(&table) {
        Ok(h) => Json(schema_descriptor(h.lock().schema())).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

fn schema_descriptor(schema: &Schema) -> Jval {
    let columns: Vec<Jval> = schema
        .columns
        .iter()
        .map(|c| {
            json!({
                "id": c.id,
                "name": c.name,
                "ty": type_name(&c.ty),
                "primary_key": c.flags.contains(ColumnFlags::PRIMARY_KEY),
                "nullable": c.flags.contains(ColumnFlags::NULLABLE),
                "auto_increment": c.flags.contains(ColumnFlags::AUTO_INCREMENT),
            })
        })
        .collect();
    let uniques: Vec<Jval> = schema
        .constraints
        .uniques
        .iter()
        .map(|u| json!({ "id": u.id, "name": u.name, "columns": u.columns }))
        .collect();
    let fks: Vec<Jval> = schema
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
    let checks: Vec<Jval> = schema
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

fn type_name(ty: &mongreldb_core::schema::TypeId) -> &'static str {
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
        Date64 => "date64",
        Time64 => "time64",
        Interval => "interval",
        Decimal128 { .. } => "decimal128",
        Uuid => "uuid",
        Json => "json",
        Array { .. } => "array",
        Enum { .. } => "enum",
    }
}

fn parse_type_name(s: &str) -> std::result::Result<TypeId, String> {
    use mongreldb_core::schema::TypeId::*;
    Ok(match s {
        "bool" => Bool,
        "int8" | "i8" => Int8,
        "int16" | "i16" => Int16,
        "int32" | "i32" => Int32,
        "int64" | "i64" | "bigint" => Int64,
        "uint8" | "u8" => UInt8,
        "uint16" | "u16" => UInt16,
        "uint32" | "u32" => UInt32,
        "uint64" | "u64" => UInt64,
        "float32" | "f32" => Float32,
        "float64" | "f64" | "double" => Float64,
        "timestamp_nanos" | "timestamp" => TimestampNanos,
        "date32" | "date" => Date32,
        "bytes" | "varchar" | "text" | "string" => Bytes,
        other if other.starts_with("embedding") => Embedding { dim: 0 },
        other => return Err(format!("unknown type: {other}")),
    })
}

// ── Typed DDL: POST /kit/create_table ───────────────────────────────────────
//
// A constraint-aware table creator: the full ColumnFlags surface (nullable /
// primary_key / auto_increment / encrypted / encrypted_indexable) plus the
// engine's declarative TableConstraints (unique / FK / check). This lets a
// remote client self-provision a constraint-bearing table entirely over HTTP —
// the legacy `/tables` route only maps `primary_key`.

#[derive(Debug, Deserialize)]
pub struct KitCreateTableRequest {
    pub name: String,
    pub columns: Vec<KitColumnDef>,
    #[serde(default)]
    pub constraints: TableConstraints,
}

#[derive(Debug, Deserialize)]
pub struct KitColumnDef {
    pub id: u16,
    pub name: String,
    pub ty: String,
    #[serde(default)]
    pub primary_key: bool,
    #[serde(default)]
    pub nullable: bool,
    #[serde(default)]
    pub auto_increment: bool,
    #[serde(default)]
    pub encrypted: bool,
    #[serde(default)]
    pub encrypted_indexable: bool,
    #[serde(default)]
    pub enum_variants: Option<Vec<String>>,
    #[serde(default)]
    pub default_expr: Option<String>,
}

/// Convert a KitColumnDef's default_expr field into an engine DefaultExpr.
#[allow(clippy::result_large_err)]
fn kit_default_expr(
    c: &KitColumnDef,
    _ty: &TypeId,
) -> std::result::Result<Option<DefaultExpr>, axum::response::Response> {
    match c.default_expr.as_deref() {
        Some("now") => Ok(Some(DefaultExpr::Now)),
        Some("uuid") => Ok(Some(DefaultExpr::Uuid)),
        Some(other) => Err((
            StatusCode::BAD_REQUEST,
            Json(KitErrorEnvelope {
                status: "aborted".into(),
                error: KitError::new("BAD_REQUEST", format!("unknown default_expr \"{other}\"")),
            }),
        )
            .into_response()),
        None => Ok(None),
    }
}

pub async fn kit_create_table(
    State(state): State<Arc<AppState>>,
    Json(req): Json<KitCreateTableRequest>,
) -> Response {
    if let Err(msg) = validate_table_name(&req.name) {
        return (
            StatusCode::BAD_REQUEST,
            Json(KitErrorEnvelope {
                status: "aborted".into(),
                error: KitError::new("BAD_REQUEST", msg),
            }),
        )
            .into_response();
    }
    let mut columns = Vec::with_capacity(req.columns.len());
    for c in &req.columns {
        let ty = if c.ty == "enum" {
            let variants = c.enum_variants.clone().unwrap_or_default();
            if variants.is_empty() {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(KitErrorEnvelope {
                        status: "aborted".into(),
                        error: KitError::new("BAD_REQUEST", "enum column requires non-empty enum_variants"),
                    }),
                )
                    .into_response();
            }
            TypeId::Enum {
                variants: variants.into(),
            }
        } else {
            match parse_type_name(&c.ty) {
                Ok(t) => t,
                Err(msg) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(KitErrorEnvelope {
                            status: "aborted".into(),
                            error: KitError::new("BAD_REQUEST", msg),
                        }),
                    )
                        .into_response();
                }
            }
        };
        let mut flags = ColumnFlags::empty();
        if c.primary_key {
            flags = flags.with(ColumnFlags::PRIMARY_KEY);
        }
        if c.nullable {
            flags = flags.with(ColumnFlags::NULLABLE);
        }
        if c.auto_increment {
            flags = flags.with(ColumnFlags::AUTO_INCREMENT);
        }
        if c.encrypted {
            flags = flags.with(ColumnFlags::ENCRYPTED);
        }
        if c.encrypted_indexable {
            flags = flags.with(ColumnFlags::ENCRYPTED_INDEXABLE);
        }
        columns.push(ColumnDef {
            id: c.id,
            name: c.name.clone(),
            ty: ty.clone(),
            flags,
            default_value: match kit_default_expr(c, &ty) {
                Ok(v) => v,
                Err(resp) => return resp,
            },
        });
    }
    let schema = Schema {
        schema_id: 0,
        columns,
        indexes: vec![],
        colocation: vec![],
        constraints: req.constraints,
        clustered: false,
    };
    match state.db.create_table(&req.name, schema) {
        Ok(id) => Json(json!({ "table_id": id })).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(KitErrorEnvelope {
                status: "aborted".into(),
                error: KitError::new("BAD_REQUEST", format!("{e}")),
            }),
        )
            .into_response(),
    }
}

// ── Typed transaction handler ───────────────────────────────────────────────

pub async fn kit_txn(
    State(state): State<Arc<AppState>>,
    Json(req): Json<KitTxnRequest>,
) -> Response {
    // Idempotency: if a key is present, serialize same-key requests and return
    // any previously-committed response verbatim.
    if let Some(key) = req.idempotency_key.clone() {
        if let Some(cached) = state.idem.get(&key) {
            return Json(cached).into_response();
        }
        let lock = state.idem.key_lock(&key);
        let _g = lock.lock().unwrap();
        if let Some(cached) = state.idem.get(&key) {
            return Json(cached).into_response();
        }
        let resp = execute_kit_txn(&state, &req);
        if let Ok(out) = &resp {
            state.idem.store(key.clone(), out.clone());
        }
        return txn_response(resp);
    }
    txn_response(execute_kit_txn(&state, &req))
}

/// Convert a `Result<KitTxnResponse, Response>` (Ok = committed batch, Err =
/// pre-built error Response) into a single axum `Response`.
fn txn_response(r: Result<KitTxnResponse, Response>) -> Response {
    match r {
        Ok(resp) => Json(resp).into_response(),
        Err(resp) => resp,
    }
}

// ── Native typed query endpoint (/kit/query) ────────────────────────────────
//
// A row-ID- and typed-cell-returning native query over the engine's `Condition`
// primitives (PK / bitmap equality / range / ANN / sparse / FM / MinHash / null
// tests). This is the native counterpart to SQL reads: it returns physical row
// ids (SQL hides them) and exposes ANN/sparse/MinHash conditions with typed
// results. Conditions intersect in the row-id space; only survivors decode.

#[derive(Debug, Deserialize)]
pub struct KitQueryRequest {
    pub table: String,
    #[serde(default)]
    pub conditions: Vec<JsonCondition>,
    /// Projected column ids. Omit / empty ⇒ all columns.
    #[serde(default)]
    pub projection: Option<Vec<u16>>,
    /// Cap on the number of returned rows (after intersection).
    #[serde(default)]
    pub limit: Option<usize>,
}

/// A condition over the row-id space, mirroring `mongreldb_core::query::Condition`
/// in a JSON-friendly, externally-tagged shape.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsonCondition {
    Pk {
        value: Jval,
    },
    BitmapEq {
        column_id: u16,
        value: Jval,
    },
    BitmapIn {
        column_id: u16,
        values: Vec<Jval>,
    },
    Range {
        column_id: u16,
        lo: i64,
        hi: i64,
    },
    RangeF64 {
        column_id: u16,
        lo: f64,
        lo_inclusive: bool,
        hi: f64,
        hi_inclusive: bool,
    },
    IsNull {
        column_id: u16,
    },
    IsNotNull {
        column_id: u16,
    },
    FmContains {
        column_id: u16,
        pattern: String,
    },
    FmContainsAll {
        column_id: u16,
        patterns: Vec<String>,
    },
    Ann {
        column_id: u16,
        query: Vec<f32>,
        k: usize,
    },
    SparseMatch {
        column_id: u16,
        query: Vec<(u32, f32)>,
        k: usize,
    },
    MinHashSimilar {
        column_id: u16,
        query: Vec<u64>,
        k: usize,
    },
}

#[derive(Debug, Serialize)]
pub struct KitQueryResponse {
    pub rows: Vec<KitRow>,
    pub truncated: bool,
}

#[derive(Debug, Serialize)]
pub struct KitRow {
    pub row_id: String,
    pub cells: Vec<Jval>,
}

pub async fn kit_query(
    State(state): State<Arc<AppState>>,
    Json(req): Json<KitQueryRequest>,
) -> Response {
    let handle = match state.db.table(&req.table) {
        Ok(h) => h,
        Err(e) => return (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    };
    let schema = handle.lock().schema().clone();

    // Translate JSON conditions → engine Conditions.
    let mut q = Query::new();
    for c in &req.conditions {
        match parse_condition(c, &schema) {
            Ok(cond) => q = q.and(cond),
            Err(msg) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(KitErrorEnvelope {
                        status: "aborted".into(),
                        error: KitError::new("BAD_REQUEST", msg),
                    }),
                )
                    .into_response();
            }
        }
    }

    let projection: Option<std::collections::HashSet<u16>> =
        req.projection.as_ref().map(|p| p.iter().copied().collect());

    let limit = req.limit.unwrap_or(usize::MAX);
    let rows = match handle.lock().query(&q) {
        Ok(r) => r,
        Err(e) => {
            return (crate::status_for_error(&e), e.to_string()).into_response();
        }
    };
    let mut out: Vec<KitRow> = Vec::new();
    let mut truncated = false;
    for r in rows {
        if out.len() >= limit {
            truncated = true;
            break;
        }
        let cells: Vec<Jval> = match &projection {
            Some(proj) => schema
                .columns
                .iter()
                .filter(|c| proj.contains(&c.id))
                .filter_map(|c| {
                    r.columns
                        .get(&c.id)
                        .map(|v| vec![json!(c.id), value_to_json(v)])
                })
                .flatten()
                .collect(),
            None => schema
                .columns
                .iter()
                .filter_map(|c| {
                    r.columns
                        .get(&c.id)
                        .map(|v| vec![json!(c.id), value_to_json(v)])
                })
                .flatten()
                .collect(),
        };
        out.push(KitRow {
            row_id: r.row_id.0.to_string(),
            cells,
        });
    }
    Json(KitQueryResponse {
        rows: out,
        truncated,
    })
    .into_response()
}

fn parse_condition(c: &JsonCondition, schema: &Schema) -> std::result::Result<Condition, String> {
    Ok(match c {
        JsonCondition::Pk { value } => {
            let pk = schema.primary_key().ok_or("table has no primary key")?;
            Condition::Pk(json_to_value(value, &pk.ty).encode_key())
        }
        JsonCondition::BitmapEq { column_id, value } => {
            let ty = col_type(schema, *column_id)?;
            Condition::BitmapEq {
                column_id: *column_id,
                value: json_to_value(value, &ty).encode_key(),
            }
        }
        JsonCondition::BitmapIn { column_id, values } => {
            let ty = col_type(schema, *column_id)?;
            Condition::BitmapIn {
                column_id: *column_id,
                values: values
                    .iter()
                    .map(|v| json_to_value(v, &ty).encode_key())
                    .collect(),
            }
        }
        JsonCondition::Range { column_id, lo, hi } => Condition::Range {
            column_id: *column_id,
            lo: *lo,
            hi: *hi,
        },
        JsonCondition::RangeF64 {
            column_id,
            lo,
            lo_inclusive,
            hi,
            hi_inclusive,
        } => Condition::RangeF64 {
            column_id: *column_id,
            lo: *lo,
            lo_inclusive: *lo_inclusive,
            hi: *hi,
            hi_inclusive: *hi_inclusive,
        },
        JsonCondition::IsNull { column_id } => Condition::IsNull {
            column_id: *column_id,
        },
        JsonCondition::IsNotNull { column_id } => Condition::IsNotNull {
            column_id: *column_id,
        },
        JsonCondition::FmContains { column_id, pattern } => Condition::FmContains {
            column_id: *column_id,
            pattern: pattern.as_bytes().to_vec(),
        },
        JsonCondition::FmContainsAll {
            column_id,
            patterns,
        } => Condition::FmContainsAll {
            column_id: *column_id,
            patterns: patterns.iter().map(|s| s.as_bytes().to_vec()).collect(),
        },
        JsonCondition::Ann {
            column_id,
            query,
            k,
        } => Condition::Ann {
            column_id: *column_id,
            query: query.clone(),
            k: *k,
        },
        JsonCondition::SparseMatch {
            column_id,
            query,
            k,
        } => Condition::SparseMatch {
            column_id: *column_id,
            query: query.clone(),
            k: *k,
        },
        JsonCondition::MinHashSimilar {
            column_id,
            query,
            k,
        } => Condition::MinHashSimilar {
            column_id: *column_id,
            query: query.clone(),
            k: *k,
        },
    })
}

fn col_type(
    schema: &Schema,
    column_id: u16,
) -> std::result::Result<mongreldb_core::schema::TypeId, String> {
    schema
        .columns
        .iter()
        .find(|c| c.id == column_id)
        .map(|c| c.ty.clone())
        .ok_or_else(|| format!("unknown column id {column_id}"))
}

#[allow(clippy::result_large_err)]
fn execute_kit_txn(state: &AppState, req: &KitTxnRequest) -> Result<KitTxnResponse, Response> {
    // 1. Structural pre-validation: resolve each op against the live schemas and
    //    parse cells into typed Values. This gives per-op error attribution
    //    (op_index) for malformed input BEFORE consuming an epoch.
    enum Action {
        Put {
            table: String,
            cells: Vec<(u16, Value)>,
        },
        Upsert {
            table: String,
            cells: Vec<(u16, Value)>,
            update_cells: Option<Vec<(u16, Value)>>,
        },
        Delete {
            table: String,
            row_id: RowId,
        },
        DeleteByPk {
            table: String,
            key: Value,
        },
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
                        parse_cells(uc, &schema).map_err(|m| op_error_msg(i, "BAD_REQUEST", m))?,
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
                    let rid = {
                        let mut guard = handle.lock();
                        // A deferred bulk load leaves HOT unbuilt; complete it
                        // before the point lookup (Phase 14.7 lazy contract).
                        guard.ensure_indexes_complete()?;
                        guard.lookup_pk(&key.encode_key())
                    };
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
    let g = h.lock();
    Ok(g.schema().clone())
}

/// Parse a flat `[col_id, val, …]` cell array against a schema.
fn parse_cells(row: &[Jval], schema: &Schema) -> std::result::Result<Vec<(u16, Value)>, String> {
    #[allow(clippy::manual_is_multiple_of)]
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
            .map(|c| c.ty.clone())
            .ok_or_else(|| format!("unknown column id {col_id}"))?;
        out.push((col_id, json_to_value(&chunk[1], &expected)));
    }
    Ok(out)
}

/// Coerce a PK JSON value against the table's declared primary-key column.
fn pk_value(pk: &Jval, schema: &Schema) -> std::result::Result<Value, String> {
    let pk_col = schema
        .primary_key()
        .ok_or("table has no primary_key column")?;
    Ok(json_to_value(pk, &pk_col.ty))
}

fn row_to_json(row: &mongreldb_core::txn::OwnedRow) -> Vec<Jval> {
    let mut out: Vec<Jval> = Vec::with_capacity(row.columns.len() * 2);
    for (id, v) in &row.columns {
        out.push(json!(id));
        out.push(value_to_json(v));
    }
    out
}

pub(crate) fn value_to_json(v: &Value) -> Jval {
    match v {
        Value::Int64(n) => json!(n),
        Value::Float64(f) => serde_json::Number::from_f64(*f)
            .map(Jval::Number)
            .unwrap_or(Jval::Null),
        Value::Bytes(b) => Jval::String(String::from_utf8_lossy(b).into_owned()),
        Value::Bool(b) => Jval::Bool(*b),
        Value::Null => Jval::Null,
        Value::Embedding(v) => {
            let arr: Vec<Jval> = v
                .iter()
                .map(|x| {
                    serde_json::Number::from_f64(*x as f64)
                        .map(Jval::Number)
                        .unwrap_or(Jval::Null)
                })
                .collect();
            Jval::Array(arr)
        }
        Value::Decimal(d) => Jval::String(d.to_string()),
        Value::Uuid(_) | Value::Json(_) => Jval::Null,
        Value::Interval { .. } => Jval::Null,
    }
}
