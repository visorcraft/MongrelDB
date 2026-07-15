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
use hmac::{Hmac, Mac};
use mongreldb_core::constraint::TableConstraints;
use mongreldb_core::query::{
    AnnRerankRequest, Condition, Fusion, NamedRetriever, Query, Retriever, RetrieverScore,
    SearchRequest, SetMember, SetSimilarityRequest, VectorMetric,
};
use mongreldb_core::schema::{
    ColumnDef, ColumnFlags, DefaultExpr, IndexDef, IndexKind, Schema, TypeId,
};
use mongreldb_core::txn::{UpsertAction, UpsertActionKind};
use mongreldb_core::{MongrelError, RowId, Value};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as Jval};
use sha2::{Digest, Sha256};

use crate::json_to_value;
use crate::{request_principal, validate_table_name, AppState, OptionalPrincipal};

const DEFAULT_AI_DEADLINE_MS: u64 = 30_000;
const MAX_AI_DEADLINE_MS: u64 = 60_000;
const DEFAULT_AI_WORK: usize = 1_000_000;
const MAX_AI_WORK: usize = 1_000_000;
const AI_CANCELLATION_GRACE: std::time::Duration = std::time::Duration::from_millis(100);

fn max_ai_fused_candidates() -> usize {
    std::env::var("MONGRELDB_AI_MAX_FUSED_CANDIDATES")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value: &usize| *value > 0 && *value <= mongreldb_core::query::MAX_FUSED_CANDIDATES)
        .unwrap_or(mongreldb_core::query::MAX_FUSED_CANDIDATES)
}

fn ai_execution_options(
    deadline_ms: Option<u64>,
    max_work: Option<usize>,
) -> Result<
    (
        std::time::Duration,
        mongreldb_core::query::AiExecutionContext,
    ),
    MongrelError,
> {
    let deadline_ms = deadline_ms.unwrap_or(DEFAULT_AI_DEADLINE_MS);
    if deadline_ms == 0 || deadline_ms > MAX_AI_DEADLINE_MS {
        return Err(MongrelError::InvalidArgument(format!(
            "deadline_ms must be between 1 and {MAX_AI_DEADLINE_MS}"
        )));
    }
    let max_work = max_work.unwrap_or(DEFAULT_AI_WORK);
    if max_work == 0 || max_work > MAX_AI_WORK {
        return Err(MongrelError::InvalidArgument(format!(
            "max_work must be between 1 and {MAX_AI_WORK}"
        )));
    }
    let duration = std::time::Duration::from_millis(deadline_ms);
    Ok((
        duration,
        mongreldb_core::query::AiExecutionContext::with_limits(
            duration,
            max_work,
            max_ai_fused_candidates(),
        ),
    ))
}

struct CancelOnDrop(Option<mongreldb_core::query::AiExecutionContext>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(context) = &self.0 {
            context.cancel();
        }
    }
}

async fn run_ai<T, F>(
    state: Arc<AppState>,
    timeout: std::time::Duration,
    context: mongreldb_core::query::AiExecutionContext,
    work: F,
) -> Result<T, MongrelError>
where
    T: Send + 'static,
    F: FnOnce(&mongreldb_core::query::AiExecutionContext) -> Result<T, MongrelError>
        + Send
        + 'static,
{
    let started = std::time::Instant::now();
    let permit = tokio::time::timeout(timeout, state.ai_semaphore.clone().acquire_owned())
        .await
        .map_err(|_| MongrelError::DeadlineExceeded)?
        .map_err(|_| MongrelError::Cancelled)?;
    let remaining = timeout.saturating_sub(started.elapsed());
    if remaining.is_zero() {
        return Err(MongrelError::DeadlineExceeded);
    }
    let worker_context = context.clone();
    let mut cancel = CancelOnDrop(Some(context.clone()));
    let mut task = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work(&worker_context)
    });
    let result = match tokio::time::timeout(remaining, &mut task).await {
        Ok(result) => {
            result.map_err(|error| MongrelError::Other(format!("AI worker failed: {error}")))?
        }
        Err(_) => {
            context.cancel();
            if tokio::time::timeout(AI_CANCELLATION_GRACE, &mut task)
                .await
                .is_err()
            {
                eprintln!("AI worker exceeded cancellation grace");
            }
            Err(MongrelError::DeadlineExceeded)
        }
    };
    cancel.0 = None;
    result
}

#[allow(clippy::too_many_arguments)]
fn retry_authorized_context<T, F>(
    state: &AppState,
    table: &str,
    principal: Option<&mongreldb_core::Principal>,
    required_columns: &[u16],
    required_permissions: &[mongreldb_core::Permission],
    context: &mongreldb_core::query::AiExecutionContext,
    snapshot_override: Option<mongreldb_core::Snapshot>,
    read: F,
) -> Result<T, MongrelError>
where
    F: FnMut(
        &mongreldb_core::Table,
        mongreldb_core::Snapshot,
        Option<&mongreldb_core::security::CandidateAuthorization<'_>>,
        Option<&mongreldb_core::Principal>,
    ) -> Result<T, MongrelError>,
{
    retry_authorized_context_stamped(
        state,
        table,
        principal,
        required_columns,
        required_permissions,
        context,
        snapshot_override,
        read,
    )
    .map(|(result, _)| result)
}

#[allow(clippy::too_many_arguments)]
fn retry_authorized_context_stamped<T, F>(
    state: &AppState,
    table: &str,
    principal: Option<&mongreldb_core::Principal>,
    required_columns: &[u16],
    required_permissions: &[mongreldb_core::Permission],
    context: &mongreldb_core::query::AiExecutionContext,
    snapshot_override: Option<mongreldb_core::Snapshot>,
    read: F,
) -> Result<(T, mongreldb_core::AuthorizedReadStamp), MongrelError>
where
    F: FnMut(
        &mongreldb_core::Table,
        mongreldb_core::Snapshot,
        Option<&mongreldb_core::security::CandidateAuthorization<'_>>,
        Option<&mongreldb_core::Principal>,
    ) -> Result<T, MongrelError>,
{
    let catalog_bound = principal
        .is_some_and(|principal| state.db.resolve_principal(&principal.username).is_some());
    state.db.with_authorized_scored_read_context_at_stamped(
        table,
        principal,
        catalog_bound,
        Some(&mongreldb_core::ReadAuthorization {
            operation: mongreldb_core::ColumnOperation::Select,
            columns: required_columns.to_vec(),
            permissions: required_permissions.to_vec(),
        }),
        Some(context),
        snapshot_override,
        read,
    )
}

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

    /// Invalidate all cached idempotency entries. Called when a table is
    /// dropped, because any cached transaction may reference the dropped
    /// table. Replaying such a cached response would silently report success
    /// without applying the transaction to the new (empty) table.
    pub(crate) fn clear(&self) {
        self.committed.lock().unwrap().clear();
        self.json_committed.lock().unwrap().clear();
        // Best-effort disk cleanup.
        if let Ok(entries) = std::fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_some_and(|ext| ext == "json") {
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
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
        MongrelError::CursorStale(_) => "CURSOR_STALE",
        MongrelError::CursorExpired => "CURSOR_EXPIRED",
        _ => "INTERNAL",
    }
}

fn is_trigger_error(message: &str) -> bool {
    message.contains("trigger ")
        || message.contains("Trigger ")
        || message.contains("external trigger bridge")
}

// ── Metadata handlers ───────────────────────────────────────────────────────

pub async fn schema_all(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
) -> Response {
    let principal = request_principal(&state, &principal);
    let names = state.db.table_names();
    let mut tables = serde_json::Map::new();
    for name in &names {
        if let Ok(schema) = visible_schema(&state, name, principal.as_ref()) {
            tables.insert(name.clone(), schema_descriptor(&schema));
        }
    }
    Json(json!({ "tables": serde_json::Value::Object(tables) })).into_response()
}

pub async fn schema_one(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Path(table): Path<String>,
) -> Response {
    let principal = request_principal(&state, &principal);
    match visible_schema(&state, &table, principal.as_ref()) {
        Ok(schema) => Json(schema_descriptor(&schema)).into_response(),
        Err(error) => kit_core_error(&error),
    }
}

fn visible_schema(
    state: &AppState,
    table: &str,
    principal: Option<&mongreldb_core::Principal>,
) -> mongreldb_core::Result<Schema> {
    let allowed = state.db.select_column_ids_for(table, principal)?;
    let mut schema = state.db.table(table)?.lock().schema().clone();
    let restricted = allowed.len() != schema.columns.len();
    schema.columns.retain(|column| allowed.contains(&column.id));
    schema
        .indexes
        .retain(|index| allowed.contains(&index.column_id));
    schema
        .constraints
        .uniques
        .retain(|unique| unique.columns.iter().all(|column| allowed.contains(column)));
    schema.constraints.foreign_keys.retain(|foreign_key| {
        !restricted
            && foreign_key
                .columns
                .iter()
                .all(|column| allowed.contains(column))
    });
    if restricted {
        schema.constraints.checks.clear();
    }
    Ok(schema)
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
                "on_update": format!("{:?}", f.on_update).to_lowercase(),
            })
        })
        .collect();
    let checks: Vec<Jval> = schema
        .constraints
        .checks
        .iter()
        .map(|c| json!({ "id": c.id, "name": c.name }))
        .collect();
    let indexes: Vec<Jval> = schema
        .indexes
        .iter()
        .map(|index| {
            json!({
                "name": index.name,
                "column_id": index.column_id,
                "kind": index_kind_name(index.kind),
                "predicate": index.predicate,
                "options": index.options,
            })
        })
        .collect();
    json!({
        "schema_id": schema.schema_id,
        "columns": columns,
        "indexes": indexes,
        "constraints": { "uniques": uniques, "foreign_keys": fks, "checks": checks },
    })
}

fn index_kind_name(kind: IndexKind) -> &'static str {
    match kind {
        IndexKind::Bitmap => "bitmap",
        IndexKind::FmIndex => "fm_index",
        IndexKind::Ann => "ann",
        IndexKind::LearnedRange => "learned_range",
        IndexKind::MinHash => "minhash",
        IndexKind::Sparse => "sparse",
    }
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
        // embedding(N) or embedding<N> — fixed-dimension vector column.
        other if other.starts_with("embedding") => {
            let rest = other.trim_start_matches("embedding").trim();
            let dim_str = rest
                .trim_start_matches(['(', '<'])
                .trim_end_matches([')', '>'])
                .trim();
            let dim: u32 = dim_str.parse().map_err(|_| {
                format!("invalid embedding dimension '{dim_str}'; expected embedding(<dim>)")
            })?;
            Embedding { dim }
        }
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
    pub indexes: Vec<KitIndexDef>,
    #[serde(default)]
    pub constraints: TableConstraints,
}

#[derive(Debug, Deserialize)]
pub struct KitIndexDef {
    pub name: String,
    pub column_id: u16,
    pub kind: String,
    #[serde(default)]
    pub options: mongreldb_core::schema::IndexOptions,
}

fn kit_index_kind(kind: &str) -> std::result::Result<IndexKind, String> {
    match kind {
        "bitmap" => Ok(IndexKind::Bitmap),
        "fm" | "fm_index" => Ok(IndexKind::FmIndex),
        "ann" | "hnsw" => Ok(IndexKind::Ann),
        "learned_range" | "range" => Ok(IndexKind::LearnedRange),
        "minhash" | "lsh" => Ok(IndexKind::MinHash),
        "sparse" => Ok(IndexKind::Sparse),
        _ => Err(format!("unknown index kind: {kind}")),
    }
}

fn validate_index_type(kind: IndexKind, ty: &TypeId) -> bool {
    match kind {
        IndexKind::Ann => matches!(ty, TypeId::Embedding { .. }),
        IndexKind::Sparse | IndexKind::MinHash | IndexKind::FmIndex => {
            matches!(ty, TypeId::Bytes)
        }
        IndexKind::LearnedRange => matches!(
            ty,
            TypeId::Int8
                | TypeId::Int16
                | TypeId::Int32
                | TypeId::Int64
                | TypeId::UInt8
                | TypeId::UInt16
                | TypeId::UInt32
                | TypeId::UInt64
                | TypeId::Float32
                | TypeId::Float64
                | TypeId::TimestampNanos
                | TypeId::Date32
                | TypeId::Date64
                | TypeId::Time64
        ),
        IndexKind::Bitmap => !matches!(ty, TypeId::Embedding { .. }),
    }
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
    // `default_expr` accepts the dynamic `now` / `uuid` discriminators.
    #[serde(default)]
    pub default_expr: Option<String>,
    // `default_value` accepts a static JSON scalar, including explicit null.
    #[serde(default)]
    pub default_value: KitStaticDefault,
}

/// Presence-aware default value. `None` means the key was omitted; `Some(Null)`
/// means the caller explicitly requested a static JSON null default.
#[derive(Debug, Default)]
pub struct KitStaticDefault(pub Option<Jval>);

impl<'de> Deserialize<'de> for KitStaticDefault {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self(Some(Jval::deserialize(deserializer)?)))
    }
}

/// Convert a KitColumnDef's default fields into an engine DefaultExpr.
#[allow(clippy::result_large_err)]
fn kit_default_expr(
    c: &KitColumnDef,
    ty: &TypeId,
) -> std::result::Result<Option<DefaultExpr>, axum::response::Response> {
    if let Some(expr) = c.default_expr.as_deref() {
        return match expr {
            "now" => Ok(Some(DefaultExpr::Now)),
            "uuid" => Ok(Some(DefaultExpr::Uuid)),
            other => Err((
                StatusCode::BAD_REQUEST,
                Json(KitErrorEnvelope {
                    status: "aborted".into(),
                    error: KitError::new(
                        "BAD_REQUEST",
                        format!("unknown default_expr \"{other}\""),
                    ),
                }),
            )
                .into_response()),
        };
    }
    let Some(value) = c.default_value.0.as_ref() else {
        return Ok(None);
    };
    if let (Jval::String(value), TypeId::Enum { variants }) = (value, ty) {
        if !variants.iter().any(|variant| variant == value) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(KitErrorEnvelope {
                    status: "aborted".into(),
                    error: KitError::new(
                        "BAD_REQUEST",
                        format!("default enum value \"{value}\" is not declared"),
                    ),
                }),
            )
                .into_response());
        }
    }
    Ok(Some(DefaultExpr::Static(json_to_value(value, ty))))
}

pub async fn kit_create_table(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Json(req): Json<KitCreateTableRequest>,
) -> Response {
    if let Err(error) = state.db.require_for(
        request_principal(&state, &principal).as_ref(),
        &mongreldb_core::Permission::Ddl,
    ) {
        return kit_core_error(&error);
    }
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
                        error: KitError::new(
                            "BAD_REQUEST",
                            "enum column requires non-empty enum_variants",
                        ),
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
    let mut names = std::collections::HashSet::new();
    let mut indexes = Vec::with_capacity(req.indexes.len());
    for index in &req.indexes {
        if !names.insert(&index.name) {
            return kit_bad_request(format!("duplicate index name: {}", index.name));
        }
        let Some(column) = columns.iter().find(|column| column.id == index.column_id) else {
            return kit_bad_request(format!(
                "index {} references unknown column {}",
                index.name, index.column_id
            ));
        };
        let kind = match kit_index_kind(&index.kind) {
            Ok(kind) => kind,
            Err(message) => return kit_bad_request(message),
        };
        if !validate_index_type(kind, &column.ty) {
            return kit_bad_request(format!(
                "index {} kind {} is incompatible with column {} type {}",
                index.name,
                index.kind,
                index.column_id,
                type_name(&column.ty)
            ));
        }
        indexes.push(IndexDef {
            name: index.name.clone(),
            column_id: index.column_id,
            kind,
            predicate: None,
            options: index.options.clone(),
        });
    }
    let schema = Schema {
        schema_id: 0,
        columns,
        indexes,
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

fn kit_bad_request(message: String) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(KitErrorEnvelope {
            status: "aborted".into(),
            error: KitError::new("BAD_REQUEST", message),
        }),
    )
        .into_response()
}

fn kit_core_error(error: &MongrelError) -> Response {
    let (status, code) = match error {
        MongrelError::InvalidArgument(_)
        | MongrelError::Schema(_)
        | MongrelError::ColumnNotFound(_) => (StatusCode::BAD_REQUEST, "BAD_REQUEST"),
        MongrelError::AuthRequired | MongrelError::InvalidCredentials { .. } => {
            (StatusCode::UNAUTHORIZED, "AUTH_REQUIRED")
        }
        MongrelError::PermissionDenied { .. } => (StatusCode::FORBIDDEN, "PERMISSION_DENIED"),
        MongrelError::NotFound(_) => (StatusCode::NOT_FOUND, "NOT_FOUND"),
        MongrelError::Conflict(_) => (StatusCode::CONFLICT, "CONFLICT"),
        MongrelError::CursorStale(_) => (StatusCode::CONFLICT, "CURSOR_STALE"),
        MongrelError::CursorExpired => (StatusCode::GONE, "CURSOR_EXPIRED"),
        MongrelError::DeadlineExceeded => (StatusCode::GATEWAY_TIMEOUT, "DEADLINE_EXCEEDED"),
        MongrelError::WorkBudgetExceeded => (StatusCode::TOO_MANY_REQUESTS, "WORK_BUDGET_EXCEEDED"),
        MongrelError::Cancelled => (
            StatusCode::from_u16(499).expect("499 is valid"),
            "CANCELLED",
        ),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL"),
    };
    (
        status,
        Json(KitErrorEnvelope {
            status: "aborted".into(),
            error: KitError::new(code, error.to_string()),
        }),
    )
        .into_response()
}

// ── Typed transaction handler ───────────────────────────────────────────────

pub async fn kit_txn(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Json(req): Json<KitTxnRequest>,
) -> Response {
    let principal = request_principal(&state, &principal);
    // Idempotency: if a key is present, serialize same-key requests and return
    // any previously-committed response verbatim.
    if let Some(key) = req.idempotency_key.clone().map(|key| {
        format!(
            "{}:{key}",
            principal
                .as_ref()
                .map(|principal| principal.username.as_str())
                .unwrap_or("anonymous")
        )
    }) {
        if let Some(cached) = state.idem.get(&key) {
            return Json(cached).into_response();
        }
        let lock = state.idem.key_lock(&key);
        let _g = lock.lock().unwrap();
        if let Some(cached) = state.idem.get(&key) {
            return Json(cached).into_response();
        }
        let resp = execute_kit_txn(&state, &req, principal.clone());
        if let Ok(out) = &resp {
            state.idem.store(key.clone(), out.clone());
        }
        return txn_response(resp);
    }
    txn_response(execute_kit_txn(&state, &req, principal))
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
// primitives (PK / bitmap equality / range / FM / null tests). This is the
// native counterpart to SQL reads: it returns physical row ids (SQL hides
// them). Conditions intersect in the row-id space; only survivors decode.

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
    /// Number of matching rows to skip before applying `limit`.
    #[serde(default)]
    pub offset: usize,
    /// Signed, generation-bound continuation token returned by a previous page.
    #[serde(default)]
    pub cursor: Option<String>,
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
    #[serde(rename = "minhash_similar", alias = "min_hash_similar")]
    MinHashSimilar {
        column_id: u16,
        query: Vec<u64>,
        k: usize,
    },
    #[serde(rename = "minhash_similar_members")]
    MinHashSimilarMembers {
        column_id: u16,
        members: Vec<Jval>,
        k: usize,
    },
}

#[derive(Debug, Serialize)]
pub struct KitQueryResponse {
    pub rows: Vec<KitRow>,
    pub truncated: bool,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct KitRow {
    pub row_id: String,
    pub cells: Vec<Jval>,
}

const CURSOR_TTL_NANOS: i64 = 5 * 60 * 1_000_000_000;
const CURSOR_CLOCK_SKEW_NANOS: i64 = 5 * 1_000_000_000;

#[derive(Debug, Clone, Copy)]
struct KitCursorBinding {
    returned_count: u64,
    table_id: u64,
    schema_id: u64,
    data_generation: u64,
    security_version: u64,
    query_time_nanos: i64,
    issued_at_nanos: i64,
    expires_at_nanos: i64,
    principal_hash: [u8; 32],
    request_hash: [u8; 32],
}

#[derive(Debug, Clone, Copy)]
struct KitQueryCursor {
    epoch: u64,
    row_id: u64,
    binding: KitCursorBinding,
}

fn cursor_now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

fn cursor_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(HEX[(byte >> 4) as usize]));
        out.push(char::from(HEX[(byte & 0x0f) as usize]));
    }
    out
}

fn cursor_unhex<const N: usize>(value: &str) -> Option<[u8; N]> {
    if value.len() != N * 2 {
        return None;
    }
    let mut out = [0u8; N];
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(out)
}

fn cursor_principal_hash(principal: Option<&mongreldb_core::Principal>) -> [u8; 32] {
    let mut hash = Sha256::new();
    match principal {
        Some(principal) => {
            hash.update([1]);
            hash.update((principal.username.len() as u64).to_le_bytes());
            hash.update(principal.username.as_bytes());
        }
        None => hash.update([0]),
    }
    hash.finalize().into()
}

fn cursor_sign(key: &[u8; 32], payload: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts 32-byte keys");
    mac.update(payload.as_bytes());
    cursor_hex(&mac.finalize().into_bytes())
}

fn cursor_verified_payload<'a>(
    value: &'a str,
    key: &[u8; 32],
    kind: &str,
) -> Result<&'a str, MongrelError> {
    let invalid = || MongrelError::InvalidArgument(format!("invalid {kind} cursor"));
    let (payload, tag) = value.rsplit_once(':').ok_or_else(&invalid)?;
    let tag = cursor_unhex::<32>(tag).ok_or_else(&invalid)?;
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts 32-byte keys");
    mac.update(payload.as_bytes());
    mac.verify_slice(&tag).map_err(|_| invalid())?;
    Ok(payload)
}

fn cursor_binding_fields(binding: KitCursorBinding) -> String {
    format!(
        "{}:{}:{}:{}:{}:{}:{}:{}:{}:{}",
        binding.returned_count,
        binding.table_id,
        binding.schema_id,
        binding.data_generation,
        binding.security_version,
        binding.query_time_nanos,
        binding.issued_at_nanos,
        binding.expires_at_nanos,
        cursor_hex(&binding.principal_hash),
        cursor_hex(&binding.request_hash),
    )
}

fn parse_cursor_binding(
    parts: &[&str],
    start: usize,
    kind: &str,
) -> Result<KitCursorBinding, MongrelError> {
    let invalid = || MongrelError::InvalidArgument(format!("invalid {kind} cursor"));
    if parts.len() != start + 10 {
        return Err(invalid());
    }
    let binding = KitCursorBinding {
        returned_count: parts[start].parse().map_err(|_| invalid())?,
        table_id: parts[start + 1].parse().map_err(|_| invalid())?,
        schema_id: parts[start + 2].parse().map_err(|_| invalid())?,
        data_generation: parts[start + 3].parse().map_err(|_| invalid())?,
        security_version: parts[start + 4].parse().map_err(|_| invalid())?,
        query_time_nanos: parts[start + 5].parse().map_err(|_| invalid())?,
        issued_at_nanos: parts[start + 6].parse().map_err(|_| invalid())?,
        expires_at_nanos: parts[start + 7].parse().map_err(|_| invalid())?,
        principal_hash: cursor_unhex(parts[start + 8]).ok_or_else(&invalid)?,
        request_hash: cursor_unhex(parts[start + 9]).ok_or_else(&invalid)?,
    };
    let lifetime = binding
        .expires_at_nanos
        .checked_sub(binding.issued_at_nanos)
        .ok_or_else(&invalid)?;
    let now = cursor_now_nanos();
    if lifetime <= 0
        || lifetime > CURSOR_TTL_NANOS
        || binding.issued_at_nanos > now.saturating_add(CURSOR_CLOCK_SKEW_NANOS)
    {
        return Err(invalid());
    }
    if now > binding.expires_at_nanos {
        return Err(MongrelError::CursorExpired);
    }
    Ok(binding)
}

fn new_cursor_binding(
    stamp: mongreldb_core::AuthorizedReadStamp,
    principal: Option<&mongreldb_core::Principal>,
    request_hash: [u8; 32],
    query_time_nanos: i64,
    returned_count: u64,
) -> KitCursorBinding {
    let issued_at_nanos = cursor_now_nanos();
    KitCursorBinding {
        returned_count,
        table_id: stamp.table_id,
        schema_id: stamp.schema_id,
        data_generation: stamp.data_generation,
        security_version: stamp.security_version,
        query_time_nanos,
        issued_at_nanos,
        expires_at_nanos: issued_at_nanos.saturating_add(CURSOR_TTL_NANOS),
        principal_hash: cursor_principal_hash(principal),
        request_hash,
    }
}

fn validate_cursor_identity(
    binding: KitCursorBinding,
    principal: Option<&mongreldb_core::Principal>,
    request_hash: [u8; 32],
) -> Result<(), MongrelError> {
    if binding.request_hash != request_hash {
        return Err(MongrelError::InvalidArgument(
            "cursor does not match request".into(),
        ));
    }
    if binding.principal_hash != cursor_principal_hash(principal) {
        return Err(MongrelError::CursorStale("cursor principal changed".into()));
    }
    Ok(())
}

fn validate_cursor_stamp(
    binding: KitCursorBinding,
    epoch: u64,
    stamp: mongreldb_core::AuthorizedReadStamp,
) -> Result<(), MongrelError> {
    if binding.table_id != stamp.table_id
        || binding.schema_id != stamp.schema_id
        || binding.data_generation != stamp.data_generation
        || binding.security_version != stamp.security_version
        || epoch != stamp.snapshot.epoch.0
    {
        return Err(MongrelError::CursorStale(
            "table, schema, index, or security generation changed".into(),
        ));
    }
    Ok(())
}

fn parse_kit_query_cursor(value: &str, key: &[u8; 32]) -> Result<KitQueryCursor, MongrelError> {
    if !value.starts_with("q2:") {
        return Err(MongrelError::CursorStale(
            "unsupported query cursor version".into(),
        ));
    }
    let payload = cursor_verified_payload(value, key, "query")?;
    let parts: Vec<_> = payload.split(':').collect();
    let invalid = || MongrelError::InvalidArgument("invalid query cursor".into());
    if parts.first() != Some(&"q2") {
        return Err(invalid());
    }
    Ok(KitQueryCursor {
        epoch: parts
            .get(1)
            .ok_or_else(&invalid)?
            .parse()
            .map_err(|_| invalid())?,
        row_id: parts
            .get(2)
            .ok_or_else(&invalid)?
            .parse()
            .map_err(|_| invalid())?,
        binding: parse_cursor_binding(&parts, 3, "query")?,
    })
}

fn format_kit_query_cursor(cursor: KitQueryCursor, key: &[u8; 32]) -> String {
    let payload = format!(
        "q2:{}:{}:{}",
        cursor.epoch,
        cursor.row_id,
        cursor_binding_fields(cursor.binding)
    );
    format!("{payload}:{}", cursor_sign(key, &payload))
}

#[derive(Debug, Deserialize)]
pub struct KitRetrieveRequest {
    pub table: String,
    pub retriever: JsonRetriever,
    #[serde(default)]
    pub deadline_ms: Option<u64>,
    #[serde(default)]
    pub max_work: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct KitAnnRerankRequest {
    pub table: String,
    pub column_id: u16,
    pub query: Vec<f32>,
    pub candidate_k: usize,
    pub limit: usize,
    pub metric: KitVectorMetric,
    #[serde(default)]
    pub deadline_ms: Option<u64>,
    #[serde(default)]
    pub max_work: Option<usize>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KitVectorMetric {
    Cosine,
    DotProduct,
    Euclidean,
}

impl From<KitVectorMetric> for VectorMetric {
    fn from(metric: KitVectorMetric) -> Self {
        match metric {
            KitVectorMetric::Cosine => Self::Cosine,
            KitVectorMetric::DotProduct => Self::DotProduct,
            KitVectorMetric::Euclidean => Self::Euclidean,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsonRetriever {
    Ann {
        column_id: u16,
        query: Vec<f32>,
        k: usize,
    },
    Sparse {
        column_id: u16,
        query: Vec<(u32, f32)>,
        k: usize,
    },
    MinHash {
        column_id: u16,
        members: Vec<Jval>,
        k: usize,
    },
}

impl JsonRetriever {
    fn column_id(&self) -> u16 {
        match self {
            Self::Ann { column_id, .. }
            | Self::Sparse { column_id, .. }
            | Self::MinHash { column_id, .. } => *column_id,
        }
    }

    fn to_core(&self) -> Result<Retriever, String> {
        Ok(match self {
            Self::Ann {
                column_id,
                query,
                k,
            } => Retriever::Ann {
                column_id: *column_id,
                query: query.clone(),
                k: *k,
            },
            Self::Sparse {
                column_id,
                query,
                k,
            } => Retriever::Sparse {
                column_id: *column_id,
                query: query.clone(),
                k: *k,
            },
            Self::MinHash {
                column_id,
                members,
                k,
            } => Retriever::MinHash {
                column_id: *column_id,
                members: members.iter().map(set_member).collect::<Result<_, _>>()?,
                k: *k,
            },
        })
    }
}

fn set_member(value: &Jval) -> Result<SetMember, String> {
    match value {
        Jval::String(value) => Ok(SetMember::String(value.clone())),
        Jval::Number(value) => Ok(SetMember::Number(value.clone())),
        Jval::Bool(value) => Ok(SetMember::Boolean(*value)),
        _ => Err("set member must be a string, number, or boolean".into()),
    }
}

fn retriever_score_json(score: RetrieverScore) -> Jval {
    match score {
        RetrieverScore::AnnHammingDistance(value) => {
            json!({"kind":"ann_hamming_distance","value":value})
        }
        RetrieverScore::SparseDotProduct(value) => {
            json!({"kind":"sparse_dot_product","value":value})
        }
        RetrieverScore::MinHashEstimatedJaccard(value) => {
            json!({"kind":"minhash_estimated_jaccard","value":value})
        }
    }
}

pub async fn kit_ai_metrics(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
) -> Response {
    let principal = request_principal(&state, &principal);
    if let Err(error) = state
        .db
        .require_for(principal.as_ref(), &mongreldb_core::Permission::Admin)
    {
        return kit_core_error(&error);
    }
    let stats = state.db.rls_cache_stats();
    Json(json!({
        "rls_cache": {
            "entries": stats.entries,
            "bytes": stats.bytes,
            "hits": stats.hits,
            "misses": stats.misses,
            "evictions": stats.evictions,
            "build_nanos": stats.build_nanos,
            "rows_evaluated": stats.rows_evaluated,
        }
    }))
    .into_response()
}

pub async fn kit_retrieve(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Json(req): Json<KitRetrieveRequest>,
) -> Response {
    let (timeout, context) = match ai_execution_options(req.deadline_ms, req.max_work) {
        Ok(options) => options,
        Err(error) => return kit_core_error(&error),
    };
    let principal = request_principal(&state, &principal);
    let column_id = req.retriever.column_id();
    if let Err(error) = state.db.require_columns_for(
        &req.table,
        mongreldb_core::ColumnOperation::Select,
        &[column_id],
        principal.as_ref(),
    ) {
        return kit_core_error(&error);
    }
    let retriever = match req.retriever.to_core() {
        Ok(retriever) => retriever,
        Err(message) => return kit_bad_request(message),
    };
    let table_name = req.table;
    let worker_state = Arc::clone(&state);
    let result = run_ai(state, timeout, context, move |context| {
        retry_authorized_context(
            &worker_state,
            &table_name,
            principal.as_ref(),
            &[column_id],
            &[],
            context,
            None,
            |table, snapshot, authorization, _| {
                table.retrieve_at_with_candidate_authorization_on_generation(
                    &retriever,
                    snapshot,
                    authorization,
                    Some(context),
                )
            },
        )
    })
    .await;
    match result {
        Ok(hits) => Json(json!({
            "hits": hits.into_iter().map(|hit| json!({
                "row_id": hit.row_id.0.to_string(),
                "rank": hit.rank,
                "score": retriever_score_json(hit.score)
            })).collect::<Vec<_>>()
        }))
        .into_response(),
        Err(error) => kit_core_error(&error),
    }
}

pub async fn kit_ann_rerank(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Json(req): Json<KitAnnRerankRequest>,
) -> Response {
    let (timeout, context) = match ai_execution_options(req.deadline_ms, req.max_work) {
        Ok(options) => options,
        Err(error) => return kit_core_error(&error),
    };
    let principal = request_principal(&state, &principal);
    if let Err(error) = state.db.require_columns_for(
        &req.table,
        mongreldb_core::ColumnOperation::Select,
        &[req.column_id],
        principal.as_ref(),
    ) {
        return kit_core_error(&error);
    }
    let request = AnnRerankRequest {
        column_id: req.column_id,
        query: req.query,
        candidate_k: req.candidate_k,
        limit: req.limit,
        metric: req.metric.into(),
    };
    let table_name = req.table;
    let worker_state = Arc::clone(&state);
    match run_ai(state, timeout, context, move |context| {
        retry_authorized_context(
            &worker_state,
            &table_name,
            principal.as_ref(),
            &[request.column_id],
            &[],
            context,
            None,
            |table, snapshot, authorization, _| {
                table.ann_rerank_at_with_candidate_authorization_on_generation(
                    &request,
                    snapshot,
                    authorization,
                    Some(context),
                )
            },
        )
    })
    .await
    {
        Ok(hits) => Json(json!({
            "hits": hits.into_iter().map(|hit| json!({
                "row_id": hit.row_id.0.to_string(),
                "hamming_distance": hit.hamming_distance,
                "exact_score": hit.exact_score,
            })).collect::<Vec<_>>()
        }))
        .into_response(),
        Err(error) => kit_core_error(&error),
    }
}

#[derive(Debug, Deserialize)]
pub struct KitSearchRequest {
    pub table: String,
    #[serde(default)]
    pub must: Vec<JsonCondition>,
    pub retrievers: Vec<KitNamedRetriever>,
    pub fusion: KitFusion,
    #[serde(default)]
    pub rerank: Option<KitRerank>,
    pub limit: usize,
    #[serde(default)]
    pub projection: Option<Vec<u16>>,
    #[serde(default)]
    pub deadline_ms: Option<u64>,
    #[serde(default)]
    pub max_work: Option<usize>,
    #[serde(default)]
    pub explain: bool,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Clone, Copy)]
struct KitSearchCursor {
    epoch: u64,
    final_score: f64,
    row_id: u64,
    binding: KitCursorBinding,
}

fn parse_kit_search_cursor(value: &str, key: &[u8; 32]) -> Result<KitSearchCursor, MongrelError> {
    if !value.starts_with("s2:") {
        return Err(MongrelError::CursorStale(
            "unsupported search cursor version".into(),
        ));
    }
    let payload = cursor_verified_payload(value, key, "search")?;
    let invalid = || MongrelError::InvalidArgument("invalid search cursor".into());
    let parts: Vec<_> = payload.split(':').collect();
    if parts.first() != Some(&"s2") {
        return Err(invalid());
    }
    let epoch = parts
        .get(1)
        .ok_or_else(&invalid)?
        .parse()
        .map_err(|_| invalid())?;
    let score_bits =
        u64::from_str_radix(parts.get(2).ok_or_else(&invalid)?, 16).map_err(|_| invalid())?;
    let row_id = parts
        .get(3)
        .ok_or_else(&invalid)?
        .parse()
        .map_err(|_| invalid())?;
    let final_score = f64::from_bits(score_bits);
    if !final_score.is_finite() {
        return Err(invalid());
    }
    Ok(KitSearchCursor {
        epoch,
        final_score,
        row_id,
        binding: parse_cursor_binding(&parts, 4, "search")?,
    })
}

fn format_kit_search_cursor(cursor: KitSearchCursor, key: &[u8; 32]) -> String {
    let payload = format!(
        "s2:{}:{:016x}:{}:{}",
        cursor.epoch,
        cursor.final_score.to_bits(),
        cursor.row_id,
        cursor_binding_fields(cursor.binding)
    );
    format!("{payload}:{}", cursor_sign(key, &payload))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KitRerank {
    ExactVector {
        embedding_column: u16,
        query: Vec<f32>,
        metric: KitVectorMetric,
        candidate_limit: usize,
        #[serde(default = "default_retriever_weight")]
        weight: f64,
    },
}

#[derive(Debug, Deserialize)]
pub struct KitNamedRetriever {
    pub name: String,
    #[serde(default = "default_retriever_weight")]
    pub weight: f64,
    #[serde(flatten)]
    pub retriever: JsonRetriever,
}

fn default_retriever_weight() -> f64 {
    1.0
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KitFusion {
    ReciprocalRank { constant: u32 },
}

fn execute_kit_search(
    state: &AppState,
    principal: Option<&mongreldb_core::Principal>,
    req: &KitSearchRequest,
    context: &mongreldb_core::query::AiExecutionContext,
) -> Result<Jval, MongrelError> {
    let handle = state.db.table(&req.table)?;
    let schema = mongreldb_core::lock_table_with_context(&handle, Some(context))?
        .schema()
        .clone();
    let mut required = req
        .projection
        .clone()
        .unwrap_or_else(|| schema.columns.iter().map(|column| column.id).collect());
    for condition in &req.must {
        match condition {
            JsonCondition::Pk { .. } => {
                if let Some(primary_key) = schema.primary_key() {
                    required.push(primary_key.id);
                }
            }
            JsonCondition::BitmapEq { column_id, .. }
            | JsonCondition::BitmapIn { column_id, .. }
            | JsonCondition::Range { column_id, .. }
            | JsonCondition::RangeF64 { column_id, .. }
            | JsonCondition::IsNull { column_id }
            | JsonCondition::IsNotNull { column_id }
            | JsonCondition::FmContains { column_id, .. }
            | JsonCondition::FmContainsAll { column_id, .. }
            | JsonCondition::Ann { column_id, .. }
            | JsonCondition::SparseMatch { column_id, .. }
            | JsonCondition::MinHashSimilar { column_id, .. }
            | JsonCondition::MinHashSimilarMembers { column_id, .. } => required.push(*column_id),
        }
    }
    required.extend(
        req.retrievers
            .iter()
            .map(|retriever| retriever.retriever.column_id()),
    );
    if let Some(KitRerank::ExactVector {
        embedding_column, ..
    }) = &req.rerank
    {
        required.push(*embedding_column);
    }
    required.sort_unstable();
    required.dedup();
    state.db.require_columns_for(
        &req.table,
        mongreldb_core::ColumnOperation::Select,
        &required,
        principal,
    )?;
    let must = req
        .must
        .iter()
        .map(|condition| parse_condition(condition, &schema))
        .collect::<Result<Vec<_>, _>>()
        .map_err(MongrelError::InvalidArgument)?;
    let retrievers = req
        .retrievers
        .iter()
        .map(|retriever| {
            Ok(NamedRetriever {
                name: retriever.name.clone(),
                weight: retriever.weight,
                retriever: retriever
                    .retriever
                    .to_core()
                    .map_err(MongrelError::InvalidArgument)?,
            })
        })
        .collect::<Result<Vec<_>, MongrelError>>()?;
    let projection_work = req
        .projection
        .as_ref()
        .map_or(schema.columns.len(), Vec::len);
    let mut estimated_work = retrievers
        .iter()
        .filter(|named| named.weight != 0.0)
        .try_fold(
            req.must
                .len()
                .checked_add(projection_work)
                .ok_or(MongrelError::WorkBudgetExceeded)?,
            |total, named| {
                let k = match &named.retriever {
                    Retriever::Ann { k, .. }
                    | Retriever::Sparse { k, .. }
                    | Retriever::MinHash { k, .. } => *k,
                };
                total.checked_add(k).ok_or(MongrelError::WorkBudgetExceeded)
            },
        )?;
    if let Some(KitRerank::ExactVector {
        candidate_limit, ..
    }) = &req.rerank
    {
        estimated_work = estimated_work
            .checked_add(*candidate_limit)
            .ok_or(MongrelError::WorkBudgetExceeded)?;
    }
    if estimated_work > context.work_limit() {
        return Err(MongrelError::WorkBudgetExceeded);
    }
    let fusion = match &req.fusion {
        KitFusion::ReciprocalRank { constant } => Fusion::ReciprocalRank {
            constant: *constant,
        },
    };
    let request = SearchRequest {
        must,
        retrievers,
        fusion,
        rerank: req.rerank.as_ref().map(|rerank| match rerank {
            KitRerank::ExactVector {
                embedding_column,
                query,
                metric,
                candidate_limit,
                weight,
            } => mongreldb_core::query::Rerank::ExactVector {
                embedding_column: *embedding_column,
                query: query.clone(),
                metric: (*metric).into(),
                candidate_limit: *candidate_limit,
                weight: *weight,
            },
        }),
        limit: req.limit,
        projection: req.projection.clone(),
    };
    let request_hash = mongreldb_core::query::canonical_search_cursor_hash(&req.table, &request);
    let cursor = req
        .cursor
        .as_deref()
        .map(|value| parse_kit_search_cursor(value, &state.cursor_mac_key))
        .transpose()?;
    if let Some(cursor) = cursor {
        validate_cursor_identity(cursor.binding, principal, request_hash)?;
    }
    let epoch = cursor
        .map(|cursor| cursor.epoch)
        .unwrap_or_else(|| state.db.visible_epoch().0);
    let (snapshot, _snapshot_guard) = state.db.snapshot_at_owned(mongreldb_core::Epoch(epoch))?;
    let search_after = cursor
        .map(|cursor| {
            Ok::<_, MongrelError>(mongreldb_core::query::SearchAfter {
                final_score: cursor.final_score,
                row_id: RowId(cursor.row_id),
                returned_count: usize::try_from(cursor.binding.returned_count)
                    .map_err(|_| MongrelError::InvalidArgument("invalid search cursor".into()))?,
            })
        })
        .transpose()?;
    let read_context = cursor.map_or_else(
        || context.clone(),
        |cursor| context.with_query_time_nanos(cursor.binding.query_time_nanos),
    );
    let (result, trace) = mongreldb_core::trace::QueryTrace::capture(|| {
        retry_authorized_context_stamped(
            state,
            &req.table,
            principal,
            &required,
            if req.explain {
                &[mongreldb_core::Permission::Admin]
            } else {
                &[]
            },
            &read_context,
            Some(snapshot),
            |table, snapshot, authorization, effective_principal| {
                let mut hits = table.search_at_with_candidate_authorization_on_generation_after(
                    &request,
                    snapshot,
                    authorization,
                    Some(&read_context),
                    search_after,
                )?;
                state
                    .db
                    .mask_search_hits_for(&req.table, &mut hits, effective_principal)?;
                Ok(hits)
            },
        )
    });
    let (hits, stamp) = result?;
    if let Some(cursor) = cursor {
        validate_cursor_stamp(cursor.binding, epoch, stamp)?;
    }
    read_context.checkpoint()?;
    let hit_count = u64::try_from(hits.len())
        .map_err(|_| MongrelError::InvalidArgument("search result count overflow".into()))?;
    let next_cursor = if hits.len() == req.limit {
        hits.last().map(|hit| {
            let binding = match cursor {
                Some(cursor) => KitCursorBinding {
                    returned_count: cursor.binding.returned_count.saturating_add(hit_count),
                    ..cursor.binding
                },
                None => new_cursor_binding(
                    stamp,
                    principal,
                    request_hash,
                    read_context.query_time_nanos(),
                    hit_count,
                ),
            };
            format_kit_search_cursor(
                KitSearchCursor {
                    epoch,
                    final_score: hit.final_score,
                    row_id: hit.row_id.0,
                    binding,
                },
                &state.cursor_mac_key,
            )
        })
    } else {
        None
    };
    let mut response = json!({
        "next_cursor": next_cursor,
        "hits": hits.into_iter().map(|hit| json!({
            "row_id": hit.row_id.0.to_string(),
            "cells": hit.cells.into_iter().flat_map(|(column_id, value)| [json!(column_id), value_to_json(&value)]).collect::<Vec<_>>(),
            "components": hit.components.into_iter().map(|component| json!({
                "retriever_name": component.retriever_name,
                "rank": component.rank,
                "raw_score": retriever_score_json(component.raw_score),
                "contribution": component.contribution,
            })).collect::<Vec<_>>(),
            "fused_score": hit.fused_score,
            "exact_rerank_score": hit.exact_rerank_score,
            "final_score": hit.final_score,
            "final_rank": hit.final_rank,
        })).collect::<Vec<_>>()
    });
    if req.explain {
        response["trace"] = json!({
            "authorization_nanos": trace.authorization_nanos,
            "rls_cache_hit": trace.rls_cache_hit,
            "rls_rows_evaluated": trace.rls_rows_evaluated,
            "rls_policy_columns_decoded": trace.rls_policy_columns_decoded,
            "authorization_retries": trace.authorization_retries,
            "hard_filter_nanos": trace.hard_filter_nanos,
            "ann_candidate_nanos": trace.ann_candidate_nanos,
            "ann_candidate_cap_hit": trace.ann_candidate_cap_hit,
            "sparse_candidate_nanos": trace.sparse_candidate_nanos,
            "minhash_candidate_nanos": trace.minhash_candidate_nanos,
            "candidate_count": trace.candidate_count,
            "union_size": trace.union_size,
            "fusion_nanos": trace.fusion_nanos,
            "projection_nanos": trace.projection_nanos,
            "projection_rows": trace.projection_rows,
            "projection_cells": trace.projection_cells,
            "work_consumed": trace.work_consumed,
            "total_nanos": trace.total_nanos,
        });
    }
    Ok(response)
}

pub async fn kit_search(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Json(req): Json<KitSearchRequest>,
) -> Response {
    let principal = request_principal(&state, &principal);
    if req.explain {
        if let Err(error) = state
            .db
            .require_for(principal.as_ref(), &mongreldb_core::Permission::Admin)
        {
            return kit_core_error(&error);
        }
    }
    let (timeout, context) = match ai_execution_options(req.deadline_ms, req.max_work) {
        Ok(options) => options,
        Err(error) => return kit_core_error(&error),
    };
    let worker_state = Arc::clone(&state);
    match run_ai(state, timeout, context, move |context| {
        execute_kit_search(&worker_state, principal.as_ref(), &req, context)
    })
    .await
    {
        Ok(response) => Json(response).into_response(),
        Err(error) => kit_core_error(&error),
    }
}

#[derive(Debug, Deserialize)]
pub struct KitSetSimilarityRequest {
    pub table: String,
    pub column_id: u16,
    pub members: Vec<Jval>,
    pub candidate_k: usize,
    pub min_jaccard: f32,
    pub limit: usize,
    #[serde(default)]
    pub deadline_ms: Option<u64>,
    #[serde(default)]
    pub max_work: Option<usize>,
}

pub async fn kit_set_similarity(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Json(req): Json<KitSetSimilarityRequest>,
) -> Response {
    let (timeout, context) = match ai_execution_options(req.deadline_ms, req.max_work) {
        Ok(options) => options,
        Err(error) => return kit_core_error(&error),
    };
    let principal = request_principal(&state, &principal);
    if let Err(error) = state.db.require_columns_for(
        &req.table,
        mongreldb_core::ColumnOperation::Select,
        &[req.column_id],
        principal.as_ref(),
    ) {
        return kit_core_error(&error);
    }
    let members = match req.members.iter().map(set_member).collect::<Result<_, _>>() {
        Ok(members) => members,
        Err(message) => return kit_bad_request(message),
    };
    let request = SetSimilarityRequest {
        column_id: req.column_id,
        members,
        candidate_k: req.candidate_k,
        min_jaccard: req.min_jaccard,
        limit: req.limit,
    };
    let table_name = req.table;
    let worker_state = Arc::clone(&state);
    let result = run_ai(state, timeout, context, move |context| {
        retry_authorized_context(
            &worker_state,
            &table_name,
            principal.as_ref(),
            &[request.column_id],
            &[],
            context,
            None,
            |table, snapshot, authorization, _| {
                table.set_similarity_at_with_candidate_authorization_on_generation(
                    &request,
                    snapshot,
                    authorization,
                    Some(context),
                )
            },
        )
    })
    .await;
    match result {
        Ok(hits) => Json(json!({
            "hits": hits.into_iter().map(|hit| json!({
                "row_id": hit.row_id.0.to_string(),
                "estimated_jaccard": hit.estimated_jaccard,
                "exact_jaccard": hit.exact_jaccard,
            })).collect::<Vec<_>>()
        }))
        .into_response(),
        Err(error) => kit_core_error(&error),
    }
}

pub async fn kit_query(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Json(req): Json<KitQueryRequest>,
) -> Response {
    if req.cursor.is_some() && req.offset != 0 {
        return kit_bad_request("offset cannot be combined with cursor".into());
    }
    if req.conditions.iter().any(|condition| {
        matches!(
            condition,
            JsonCondition::Ann { .. }
                | JsonCondition::SparseMatch { .. }
                | JsonCondition::MinHashSimilar { .. }
                | JsonCondition::MinHashSimilarMembers { .. }
        )
    }) {
        return kit_bad_request(
            "ranked AI conditions are not available on /kit/query; use /kit/retrieve or /kit/search"
                .into(),
        );
    }
    if req.offset > mongreldb_core::query::MAX_QUERY_OFFSET {
        return kit_bad_request(format!(
            "offset exceeds {}",
            mongreldb_core::query::MAX_QUERY_OFFSET
        ));
    }
    let principal = request_principal(&state, &principal);
    let handle = match state.db.table(&req.table) {
        Ok(h) => h,
        Err(error) => return kit_core_error(&error),
    };
    let schema = handle.lock().schema().clone();
    let allowed = match state
        .db
        .select_column_ids_for(&req.table, principal.as_ref())
    {
        Ok(allowed) => allowed,
        Err(error) => {
            return kit_core_error(&error);
        }
    };
    let projection_ids = req
        .projection
        .as_ref()
        .filter(|projection| !projection.is_empty())
        .cloned()
        .unwrap_or_else(|| allowed.clone());
    if projection_ids.len() > mongreldb_core::query::MAX_PROJECTION_COLUMNS {
        return kit_bad_request(format!(
            "projection exceeds {} columns",
            mongreldb_core::query::MAX_PROJECTION_COLUMNS
        ));
    }
    let mut required = projection_ids.clone();
    for condition in &req.conditions {
        match condition {
            JsonCondition::Pk { .. } => {
                if let Some(primary_key) = schema.primary_key() {
                    required.push(primary_key.id);
                }
            }
            JsonCondition::BitmapEq { column_id, .. }
            | JsonCondition::BitmapIn { column_id, .. }
            | JsonCondition::Range { column_id, .. }
            | JsonCondition::RangeF64 { column_id, .. }
            | JsonCondition::IsNull { column_id }
            | JsonCondition::IsNotNull { column_id }
            | JsonCondition::FmContains { column_id, .. }
            | JsonCondition::FmContainsAll { column_id, .. }
            | JsonCondition::Ann { column_id, .. }
            | JsonCondition::SparseMatch { column_id, .. }
            | JsonCondition::MinHashSimilar { column_id, .. }
            | JsonCondition::MinHashSimilarMembers { column_id, .. } => required.push(*column_id),
        }
    }
    required.sort_unstable();
    required.dedup();
    if let Err(error) = state.db.require_columns_for(
        &req.table,
        mongreldb_core::ColumnOperation::Select,
        &required,
        principal.as_ref(),
    ) {
        return kit_core_error(&error);
    }

    // Translate JSON conditions → engine Conditions.
    let conditions = match req
        .conditions
        .iter()
        .map(|condition| parse_condition(condition, &schema))
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(conditions) => conditions,
        Err(message) => return kit_bad_request(message),
    };
    let limit = req.limit.unwrap_or(mongreldb_core::query::MAX_FINAL_LIMIT);
    if limit == 0 || limit > mongreldb_core::query::MAX_FINAL_LIMIT {
        return kit_bad_request(format!(
            "limit must be between 1 and {}",
            mongreldb_core::query::MAX_FINAL_LIMIT
        ));
    }
    let request_hash = mongreldb_core::query::canonical_query_cursor_hash(
        &req.table,
        &conditions,
        Some(&projection_ids),
    );
    let cursor = match req
        .cursor
        .as_deref()
        .map(|value| parse_kit_query_cursor(value, &state.cursor_mac_key))
        .transpose()
    {
        Ok(cursor) => cursor,
        Err(error) => return kit_core_error(&error),
    };
    if let Some(cursor) = cursor {
        if let Err(error) =
            validate_cursor_identity(cursor.binding, principal.as_ref(), request_hash)
        {
            return kit_core_error(&error);
        }
    }
    let epoch = cursor
        .map(|cursor| cursor.epoch)
        .unwrap_or_else(|| state.db.visible_epoch().0);
    let (snapshot, _snapshot_guard) = match state.db.snapshot_at_owned(mongreldb_core::Epoch(epoch))
    {
        Ok(snapshot) => snapshot,
        Err(error) => return kit_core_error(&error),
    };
    let fetch_limit = limit
        .saturating_add(1)
        .min(mongreldb_core::query::MAX_FINAL_LIMIT);
    let q = Query {
        conditions,
        limit: Some(fetch_limit),
        offset: req.offset,
    };
    let after_row_id = cursor.map(|cursor| RowId(cursor.row_id));
    let query_time_nanos =
        cursor.map_or_else(cursor_now_nanos, |cursor| cursor.binding.query_time_nanos);

    let projection = projection_ids
        .into_iter()
        .collect::<std::collections::HashSet<_>>();
    let principal_catalog_bound = principal
        .as_ref()
        .is_some_and(|principal| state.db.resolve_principal(&principal.username).is_some());
    let (rows, stamp) = match state.db.with_authorized_read_context_stamped(
        &req.table,
        principal.as_ref(),
        principal_catalog_bound,
        Some(&mongreldb_core::ReadAuthorization {
            operation: mongreldb_core::ColumnOperation::Select,
            columns: required,
            permissions: Vec::new(),
        }),
        None,
        Some(snapshot),
        |table, snapshot, allowed, effective_principal| {
            let rows = table.query_at_with_allowed_after_at_time(
                &q,
                snapshot,
                allowed,
                after_row_id,
                query_time_nanos,
            )?;
            state
                .db
                .secure_rows_for(&req.table, rows, effective_principal)
        },
    ) {
        Ok(rows) => rows,
        Err(error) => return kit_core_error(&error),
    };
    if let Some(cursor) = cursor {
        if let Err(error) = validate_cursor_stamp(cursor.binding, epoch, stamp) {
            return kit_core_error(&error);
        }
    }
    let truncated = rows.len() > limit
        || (limit == mongreldb_core::query::MAX_FINAL_LIMIT && rows.len() == limit);
    let mut out: Vec<KitRow> = Vec::with_capacity(rows.len().min(limit));
    let mut last_row_id = None;
    for r in rows.into_iter().take(limit) {
        last_row_id = Some(r.row_id);
        let cells: Vec<Jval> = schema
            .columns
            .iter()
            .filter(|column| projection.contains(&column.id))
            .filter_map(|column| {
                r.columns
                    .get(&column.id)
                    .map(|value| vec![json!(column.id), value_to_json(value)])
            })
            .flatten()
            .collect();
        out.push(KitRow {
            row_id: r.row_id.0.to_string(),
            cells,
        });
    }
    let next_cursor = if truncated {
        last_row_id.map(|row_id| {
            let returned_count = u64::try_from(out.len()).unwrap_or(u64::MAX);
            let binding = match cursor {
                Some(cursor) => KitCursorBinding {
                    returned_count: cursor.binding.returned_count.saturating_add(returned_count),
                    ..cursor.binding
                },
                None => new_cursor_binding(
                    stamp,
                    principal.as_ref(),
                    request_hash,
                    query_time_nanos,
                    returned_count,
                ),
            };
            format_kit_query_cursor(
                KitQueryCursor {
                    epoch,
                    row_id: row_id.0,
                    binding,
                },
                &state.cursor_mac_key,
            )
        })
    } else {
        None
    };
    Json(KitQueryResponse {
        rows: out,
        truncated,
        next_cursor,
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
        JsonCondition::MinHashSimilarMembers {
            column_id,
            members,
            k,
        } => Condition::MinHashSimilar {
            column_id: *column_id,
            query: members
                .iter()
                .map(mongreldb_core::index::minhash_member_hash_v1)
                .collect::<Result<Vec<_>, _>>()
                .map_err(str::to_string)?,
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
fn execute_kit_txn(
    state: &AppState,
    req: &KitTxnRequest,
    principal: Option<mongreldb_core::Principal>,
) -> Result<KitTxnResponse, Response> {
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
    let mut transaction = db.begin_as(principal);
    let outcome: mongreldb_core::Result<Vec<KitOpResult>> = (|| {
        let mut results: Vec<KitOpResult> = Vec::with_capacity(parsed.len());
        for p in &parsed {
            match &p.action {
                Action::Put { table, cells } => {
                    let pr = transaction.put_returning(table, cells.clone())?;
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
                    let ur = transaction.upsert(table, cells.clone(), action)?;
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
                    transaction.delete(table, *row_id)?;
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
                            transaction.delete(table, r)?;
                            results.push(KitOpResult::Deleted);
                        }
                        None => results.push(KitOpResult::NotFound),
                    }
                }
            }
        }
        transaction.commit()?;
        Ok(results)
    })();

    let results = match outcome {
        Ok(r) => r,
        Err(e) => {
            let code = error_code(&e);
            let status = match crate::status_for_error(&e) {
                StatusCode::INTERNAL_SERVER_ERROR => StatusCode::CONFLICT,
                status => status,
            };
            return Err((
                status,
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
        let raw_col_id = chunk[0]
            .as_u64()
            .ok_or("column id must be a non-negative integer")?;
        let col_id = u16::try_from(raw_col_id).map_err(|_| "column id must fit u16")?;
        let column = schema
            .columns
            .iter()
            .find(|c| c.id == col_id)
            .ok_or_else(|| format!("unknown column id {col_id}"))?;
        out.push((
            col_id,
            kit_value(&chunk[1], column, &schema.indexes)
                .map_err(|message| format!("column {col_id}: {message}"))?,
        ));
    }
    Ok(out)
}

fn kit_value(value: &Jval, column: &ColumnDef, indexes: &[IndexDef]) -> Result<Value, String> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    match &column.ty {
        TypeId::Embedding { dim } => {
            let values = value
                .as_array()
                .ok_or("embedding must be a numeric array")?;
            if values.len() != *dim as usize {
                return Err(format!(
                    "embedding dimension must be {dim}, got {}",
                    values.len()
                ));
            }
            Ok(values
                .iter()
                .map(|value| {
                    let value = value.as_f64().ok_or("embedding value must be numeric")?;
                    let value = value as f32;
                    value
                        .is_finite()
                        .then_some(value)
                        .ok_or("embedding value must be finite f32")
                })
                .collect::<Result<Vec<_>, _>>()
                .map(Value::Embedding)?)
        }
        TypeId::Bytes
            if indexes
                .iter()
                .any(|index| index.column_id == column.id && index.kind == IndexKind::Sparse) =>
        {
            let pairs = value
                .as_array()
                .ok_or("sparse vector must be an array of [token_id, weight]")?;
            let mut terms = std::collections::BTreeMap::<u32, f32>::new();
            for pair in pairs {
                let pair = pair
                    .as_array()
                    .filter(|pair| pair.len() == 2)
                    .ok_or("sparse term must be [token_id, weight]")?;
                let token = pair[0]
                    .as_u64()
                    .and_then(|token| u32::try_from(token).ok())
                    .ok_or("sparse token_id must fit u32")?;
                let weight = pair[1].as_f64().ok_or("sparse weight must be numeric")? as f32;
                if !weight.is_finite() {
                    return Err("sparse weight must be finite f32".into());
                }
                *terms.entry(token).or_default() += weight;
            }
            if terms.values().any(|weight| !weight.is_finite()) {
                return Err("summed sparse weight must be finite f32".into());
            }
            bincode::serialize(&terms.into_iter().collect::<Vec<_>>())
                .map(Value::Bytes)
                .map_err(|error| error.to_string())
        }
        TypeId::Bytes
            if indexes
                .iter()
                .any(|index| index.column_id == column.id && index.kind == IndexKind::MinHash) =>
        {
            let members = value.as_array().ok_or("set must be an array")?;
            if members
                .iter()
                .any(|member| !matches!(member, Jval::String(_) | Jval::Number(_) | Jval::Bool(_)))
            {
                return Err("set members must be strings, numbers, or booleans".into());
            }
            serde_json::to_vec(members)
                .map(Value::Bytes)
                .map_err(|error| error.to_string())
        }
        _ => Ok(json_to_value(value, &column.ty)),
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn column(id: u16, ty: TypeId) -> ColumnDef {
        ColumnDef {
            id,
            name: format!("c{id}"),
            ty,
            flags: ColumnFlags::empty(),
            default_value: None,
        }
    }

    #[test]
    fn ai_wire_values_are_strict_and_canonical() {
        let embedding = column(1, TypeId::Embedding { dim: 2 });
        assert_eq!(
            kit_value(&json!([1, -1]), &embedding, &[]).unwrap(),
            Value::Embedding(vec![1.0, -1.0])
        );
        assert!(kit_value(&json!([1]), &embedding, &[])
            .unwrap_err()
            .contains("dimension"));

        let sparse = column(2, TypeId::Bytes);
        let sparse_index = IndexDef {
            name: "sparse".into(),
            column_id: 2,
            kind: IndexKind::Sparse,
            predicate: None,
            options: Default::default(),
        };
        let Value::Bytes(encoded) = kit_value(
            &json!([[2, 1.0], [1, 2.0], [2, 3.0]]),
            &sparse,
            &[sparse_index],
        )
        .unwrap() else {
            panic!("expected bytes")
        };
        assert_eq!(
            bincode::deserialize::<Vec<(u32, f32)>>(&encoded).unwrap(),
            vec![(1, 2.0), (2, 4.0)]
        );

        let set = column(3, TypeId::Bytes);
        let set_index = IndexDef {
            name: "set".into(),
            column_id: 3,
            kind: IndexKind::MinHash,
            predicate: None,
            options: Default::default(),
        };
        assert_eq!(
            kit_value(
                &json!(["a", 1, true]),
                &set,
                std::slice::from_ref(&set_index),
            )
            .unwrap(),
            Value::Bytes(br#"["a",1,true]"#.to_vec())
        );
        assert!(kit_value(&json!([{"bad": true}]), &set, &[set_index]).is_err());
    }

    #[test]
    fn cursor_v2_rejects_tampering_expiry_and_other_server_keys() {
        let key = [7; 32];
        let stamp = mongreldb_core::AuthorizedReadStamp {
            table_id: 11,
            schema_id: 12,
            data_generation: 13,
            security_version: 14,
            snapshot: mongreldb_core::Snapshot::at(mongreldb_core::Epoch(15)),
        };
        let binding = new_cursor_binding(stamp, None, [16; 32], 17, 1);
        let token = format_kit_query_cursor(
            KitQueryCursor {
                epoch: 15,
                row_id: 18,
                binding,
            },
            &key,
        );
        assert_eq!(parse_kit_query_cursor(&token, &key).unwrap().row_id, 18);

        let mut tampered = token.into_bytes();
        let index = tampered.iter().position(|byte| *byte == b'8').unwrap();
        tampered[index] = b'9';
        let tampered = String::from_utf8(tampered).unwrap();
        assert!(matches!(
            parse_kit_query_cursor(&tampered, &key),
            Err(MongrelError::InvalidArgument(_))
        ));
        assert!(matches!(
            parse_kit_query_cursor(
                &format_kit_query_cursor(
                    KitQueryCursor {
                        epoch: 15,
                        row_id: 18,
                        binding,
                    },
                    &key,
                ),
                &[8; 32]
            ),
            Err(MongrelError::InvalidArgument(_))
        ));

        let now = cursor_now_nanos();
        let expired = KitCursorBinding {
            issued_at_nanos: now - CURSOR_TTL_NANOS,
            expires_at_nanos: now - 1,
            ..binding
        };
        let expired = format_kit_query_cursor(
            KitQueryCursor {
                epoch: 15,
                row_id: 18,
                binding: expired,
            },
            &key,
        );
        assert!(matches!(
            parse_kit_query_cursor(&expired, &key),
            Err(MongrelError::CursorExpired)
        ));
        assert!(matches!(
            parse_kit_query_cursor("q1:old", &key),
            Err(MongrelError::CursorStale(_))
        ));
    }
}
