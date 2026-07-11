//! mongreldb-client — a lightweight HTTP client for `mongreldb-server`.
//!
//! Hardened surface:
//! - Every call checks the HTTP status and returns a typed [`ClientError`] on
//!   non-2xx (the server's `KitErrorEnvelope` is decoded into
//!   [`KitError::unique_violation`]/`fk_violation`/etc.).
//! - Typed [`KitTxnRequest`]/[`KitTxnResponse`] models mirror the server's
//!   `/kit/txn` batch endpoint.
//! - [`MongrelClient::kit_schema`] fetches table metadata (columns +
//!   constraints).
//! - Legacy SQL read ([`MongrelClient::sql`]) returns Arrow IPC batches.

use std::io::Cursor;

use arrow::ipc::reader::FileReader;
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// A typed client error. Network/IO failures and non-2xx HTTP responses both
/// surface here; constraint violations from `/kit/txn` are decoded into the
/// matching variant so callers can branch on them.
#[derive(Debug, Error)]
pub enum ClientError {
    #[error("io/transport error: {0}")]
    Transport(String),
    #[error("http {status}: {body}")]
    Http { status: u16, body: String },
    #[error("decode error: {0}")]
    Decode(String),
    #[error("kit error: {code}: {message}")]
    Kit {
        code: KitErrorCode,
        message: String,
        op_index: Option<usize>,
        status: u16,
    },
}

/// Typed error codes mirrored from the server's `/kit/txn` envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KitErrorCode {
    UniqueViolation,
    FkViolation,
    CheckViolation,
    ProcedureNotFound,
    ProcedureValidation,
    ProcedureExecution,
    TriggerNotFound,
    TriggerValidation,
    Conflict,
    BadRequest,
    NotFound,
    Internal,
    Other,
}

impl std::fmt::Display for KitErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl KitErrorCode {
    fn as_str(&self) -> &'static str {
        match self {
            KitErrorCode::UniqueViolation => "UNIQUE_VIOLATION",
            KitErrorCode::FkViolation => "FK_VIOLATION",
            KitErrorCode::CheckViolation => "CHECK_VIOLATION",
            KitErrorCode::ProcedureNotFound => "PROCEDURE_NOT_FOUND",
            KitErrorCode::ProcedureValidation => "PROCEDURE_VALIDATION",
            KitErrorCode::ProcedureExecution => "PROCEDURE_EXECUTION",
            KitErrorCode::TriggerNotFound => "TRIGGER_NOT_FOUND",
            KitErrorCode::TriggerValidation => "TRIGGER_VALIDATION",
            KitErrorCode::Conflict => "CONFLICT",
            KitErrorCode::BadRequest => "BAD_REQUEST",
            KitErrorCode::NotFound => "NOT_FOUND",
            KitErrorCode::Internal => "INTERNAL",
            KitErrorCode::Other => "OTHER",
        }
    }

    fn from_str(s: &str) -> Self {
        match s {
            "UNIQUE_VIOLATION" => KitErrorCode::UniqueViolation,
            "FK_VIOLATION" => KitErrorCode::FkViolation,
            "CHECK_VIOLATION" => KitErrorCode::CheckViolation,
            "PROCEDURE_NOT_FOUND" => KitErrorCode::ProcedureNotFound,
            "PROCEDURE_VALIDATION" => KitErrorCode::ProcedureValidation,
            "PROCEDURE_EXECUTION" => KitErrorCode::ProcedureExecution,
            "TRIGGER_NOT_FOUND" => KitErrorCode::TriggerNotFound,
            "TRIGGER_VALIDATION" => KitErrorCode::TriggerValidation,
            "CONFLICT" => KitErrorCode::Conflict,
            "BAD_REQUEST" => KitErrorCode::BadRequest,
            "NOT_FOUND" => KitErrorCode::NotFound,
            "INTERNAL" => KitErrorCode::Internal,
            _ => KitErrorCode::Other,
        }
    }
}

impl From<reqwest::Error> for ClientError {
    fn from(e: reqwest::Error) -> Self {
        ClientError::Transport(e.to_string())
    }
}

impl From<std::io::Error> for ClientError {
    fn from(e: std::io::Error) -> Self {
        ClientError::Transport(e.to_string())
    }
}

pub type ClientResult<T> = std::result::Result<T, ClientError>;

pub struct MongrelClient {
    base_url: String,
    client: reqwest::blocking::Client,
}

#[derive(Serialize)]
struct SqlReq {
    sql: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    format: Option<&'static str>,
}

#[derive(Deserialize)]
struct CountResp {
    count: u64,
}

/// Server-side schema metadata for one table (subset of the server's descriptor).
#[derive(Debug, Clone, Deserialize)]
pub struct TableSchemaInfo {
    pub schema_id: u64,
    pub columns: Vec<ColumnMeta>,
    pub constraints: ConstraintMeta,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ColumnMeta {
    pub id: u16,
    pub name: String,
    pub ty: String,
    pub primary_key: bool,
    pub nullable: bool,
    pub auto_increment: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ConstraintMeta {
    #[serde(default)]
    pub uniques: Vec<serde_json::Value>,
    #[serde(default)]
    pub foreign_keys: Vec<serde_json::Value>,
    #[serde(default)]
    pub checks: Vec<serde_json::Value>,
}

// ── /kit/txn typed models ───────────────────────────────────────────────────

/// A typed atomic batch request for `/kit/txn`.
#[derive(Debug, Clone, Serialize)]
pub struct KitTxnRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    pub ops: Vec<KitOp>,
}

impl KitTxnRequest {
    pub fn new(ops: Vec<KitOp>) -> Self {
        Self {
            idempotency_key: None,
            ops,
        }
    }
    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }
}

/// One operation in a `/kit/txn` batch.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KitOp {
    Put {
        table: String,
        cells: Vec<serde_json::Value>,
        returning: bool,
    },
    Upsert {
        table: String,
        cells: Vec<serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        update_cells: Option<Vec<serde_json::Value>>,
        returning: bool,
    },
    Delete {
        table: String,
        row_id: u64,
    },
    DeleteByPk {
        table: String,
        pk: serde_json::Value,
    },
}

impl KitOp {
    pub fn put(table: impl Into<String>, cells: Vec<serde_json::Value>) -> Self {
        KitOp::Put {
            table: table.into(),
            cells,
            returning: false,
        }
    }
    pub fn put_returning(table: impl Into<String>, cells: Vec<serde_json::Value>) -> Self {
        KitOp::Put {
            table: table.into(),
            cells,
            returning: true,
        }
    }
    pub fn upsert(table: impl Into<String>, cells: Vec<serde_json::Value>) -> Self {
        KitOp::Upsert {
            table: table.into(),
            cells,
            update_cells: None,
            returning: false,
        }
    }
    pub fn delete_by_pk(table: impl Into<String>, pk: serde_json::Value) -> Self {
        KitOp::DeleteByPk {
            table: table.into(),
            pk,
        }
    }
}

/// A typed per-op result from `/kit/txn`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum KitOpResult {
    Put {
        row_id: Option<String>,
        auto_inc: Option<i64>,
        #[serde(default)]
        row: Option<Vec<serde_json::Value>>,
    },
    Upsert {
        action: String,
        auto_inc: Option<i64>,
        #[serde(default)]
        row: Option<Vec<serde_json::Value>>,
    },
    Deleted,
    NotFound,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KitTxnResponse {
    pub status: String,
    pub epoch: u64,
    pub results: Vec<KitOpResult>,
}

// ── /kit/query typed models ─────────────────────────────────────────────────

/// A native typed query (`POST /kit/query`). `conditions` are raw JSON objects
/// mirroring the daemon's condition variants, e.g. `{"pk": {"value": 2}}`,
/// `{"range": {"column_id": 2, "lo": 0, "hi": 100}}`,
/// `{"ann": {"column_id": 5, "query": [...], "k": 10}}`.
#[derive(Debug, Clone, Serialize)]
pub struct KitQueryRequest {
    pub table: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub projection: Option<Vec<u16>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KitQueryResponse {
    pub rows: Vec<KitQueryRow>,
    pub truncated: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KitQueryRow {
    pub row_id: String,
    /// Flat `[col_id, val, col_id, val, …]` cells.
    pub cells: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProcedureRequest {
    pub procedure: mongreldb_core::StoredProcedure,
}

#[derive(Debug, Clone, Serialize)]
pub struct TriggerRequest {
    pub trigger: mongreldb_core::StoredTrigger,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProcedureResponse {
    pub status: String,
    pub procedure: mongreldb_core::StoredProcedure,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TriggerResponse {
    #[serde(default)]
    pub status: Option<String>,
    pub trigger: mongreldb_core::StoredTrigger,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProceduresResponse {
    pub procedures: Vec<mongreldb_core::StoredProcedure>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TriggersResponse {
    pub triggers: Vec<mongreldb_core::StoredTrigger>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProcedureCallRequest {
    #[serde(default)]
    pub args: serde_json::Map<String, serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProcedureCallResponse {
    pub status: String,
    #[serde(default)]
    pub epoch: Option<u64>,
    pub result: serde_json::Value,
}

// Internal mirror of the server's error envelope.
#[derive(Debug, Deserialize)]
struct KitErrorEnvelope {
    #[allow(dead_code)]
    status: String,
    error: KitErrorBody,
}

#[derive(Debug, Deserialize)]
struct KitErrorBody {
    code: String,
    message: String,
    #[serde(default)]
    op_index: Option<usize>,
}

impl MongrelClient {
    pub fn new(url: &str) -> Self {
        Self {
            base_url: url.trim_end_matches('/').to_string(),
            client: reqwest::blocking::Client::new(),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    /// Send a request and, on non-2xx, decode a typed [`ClientError::Kit`] when
    /// the body is a `KitErrorEnvelope`, else a plain [`ClientError::Http`].
    fn check(
        &self,
        resp: reqwest::blocking::Response,
    ) -> ClientResult<reqwest::blocking::Response> {
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }
        let status_u16 = status.as_u16();
        let body = resp.text().unwrap_or_default();
        if let Ok(env) = serde_json::from_str::<KitErrorEnvelope>(&body) {
            return Err(ClientError::Kit {
                code: KitErrorCode::from_str(&env.error.code),
                message: env.error.message,
                op_index: env.error.op_index,
                status: status_u16,
            });
        }
        Err(ClientError::Http {
            status: status_u16,
            body,
        })
    }

    pub fn health(&self) -> ClientResult<String> {
        let resp = self.client.get(self.url("/health")).send()?;
        self.check(resp)?.text().map_err(Into::into)
    }

    // ── Table management ──

    pub fn list_tables(&self) -> ClientResult<Vec<String>> {
        let resp = self.client.get(self.url("/tables")).send()?;
        Ok(self.check(resp)?.json()?)
    }

    pub fn create_table(&self, name: &str, columns: Vec<ColumnDefJson>) -> ClientResult<u64> {
        let resp = self
            .client
            .post(self.url("/tables"))
            .json(&serde_json::json!({ "name": name, "columns": columns }))
            .send()?;
        let resp = self.check(resp)?;
        let v: serde_json::Value = resp.json()?;
        Ok(v["table_id"].as_u64().unwrap_or(0))
    }

    pub fn drop_table(&self, name: &str) -> ClientResult<()> {
        let resp = self
            .client
            .delete(self.url(&format!("/tables/{name}")))
            .send()?;
        self.check(resp)?;
        Ok(())
    }

    // ── Table-qualified operations ──

    pub fn count(&self, table: &str) -> ClientResult<u64> {
        let resp = self
            .client
            .get(self.url(&format!("/tables/{table}/count")))
            .send()?;
        let resp = self.check(resp)?;
        let cr: CountResp = resp.json()?;
        Ok(cr.count)
    }

    pub fn put(&self, table: &str, row: Vec<(u16, mongreldb_core::Value)>) -> ClientResult<u64> {
        let json_row: Vec<serde_json::Value> = row
            .iter()
            .flat_map(|(id, v)| vec![serde_json::json!(id), value_to_json(v)])
            .collect();
        let resp = self
            .client
            .post(self.url(&format!("/tables/{table}/put")))
            .json(&serde_json::json!({ "row": json_row }))
            .send()?;
        let resp = self.check(resp)?;
        let v: serde_json::Value = resp.json()?;
        Ok(v["row_id"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0))
    }

    pub fn commit(&self, table: &str) -> ClientResult<u64> {
        let resp = self
            .client
            .post(self.url(&format!("/tables/{table}/commit")))
            .send()?;
        let resp = self.check(resp)?;
        let v: serde_json::Value = resp.json()?;
        Ok(v["epoch"].as_u64().unwrap_or(0))
    }

    // ── SQL read ──

    pub fn sql(&self, sql: &str) -> ClientResult<Vec<RecordBatch>> {
        let resp = self
            .client
            .post(self.url("/sql"))
            .json(&SqlReq {
                sql: sql.to_string(),
                format: Some("arrow"), // Rust client decodes Arrow IPC directly
            })
            .send()?;
        let resp = self.check(resp)?;
        let bytes = resp.bytes()?;
        read_arrow_ipc(&bytes)
    }

    // ── Atomic txn (legacy, raw) ──

    pub fn txn(&self, ops: Vec<TxnOp>) -> ClientResult<()> {
        let resp = self
            .client
            .post(self.url("/txn"))
            .json(&serde_json::json!({ "ops": ops }))
            .send()?;
        self.check(resp)?;
        Ok(())
    }

    // ── Typed Kit surface ──

    /// Fetch one table's schema + constraint metadata (`GET /kit/schema/{t}`).
    pub fn kit_schema(&self, table: &str) -> ClientResult<TableSchemaInfo> {
        let resp = self
            .client
            .get(self.url(&format!("/kit/schema/{table}")))
            .send()?;
        let resp = self.check(resp)?;
        Ok(resp.json()?)
    }

    /// Run a typed atomic batch (`POST /kit/txn`). Constraint violations and
    /// conflicts return [`ClientError::Kit`] with the matching code.
    pub fn kit_txn(&self, req: &KitTxnRequest) -> ClientResult<KitTxnResponse> {
        let resp = self.client.post(self.url("/kit/txn")).json(req).send()?;
        let resp = self.check(resp)?;
        Ok(resp.json()?)
    }

    /// Run a native typed query (`POST /kit/query`) returning physical row ids
    /// and typed cells. Conditions intersect in the row-id space; this is the
    /// native counterpart to SQL reads (which hide row ids).
    pub fn kit_query(&self, req: &KitQueryRequest) -> ClientResult<KitQueryResponse> {
        let resp = self.client.post(self.url("/kit/query")).json(req).send()?;
        let resp = self.check(resp)?;
        Ok(resp.json()?)
    }

    /// Create a constraint-bearing table over HTTP (`POST /kit/create_table`).
    /// `body` is the full request JSON — `{name, columns:[{id,name,ty,
    /// primary_key,nullable,auto_increment,…}], constraints:{uniques,…,
    /// foreign_keys,…, checks:[{id,name,expr}]}}`. Returns the assigned table id.
    pub fn kit_create_table(&self, body: &serde_json::Value) -> ClientResult<u64> {
        let resp = self
            .client
            .post(self.url("/kit/create_table"))
            .json(body)
            .send()?;
        let resp = self.check(resp)?;
        let v: serde_json::Value = resp.json()?;
        Ok(v["table_id"].as_u64().unwrap_or(0))
    }

    pub fn procedures(&self) -> ClientResult<Vec<mongreldb_core::StoredProcedure>> {
        let resp = self.client.get(self.url("/procedures")).send()?;
        let resp = self.check(resp)?;
        Ok(resp.json::<ProceduresResponse>()?.procedures)
    }

    pub fn procedure(&self, name: &str) -> ClientResult<mongreldb_core::StoredProcedure> {
        let resp = self
            .client
            .get(self.url(&format!("/procedures/{name}")))
            .send()?;
        let resp = self.check(resp)?;
        Ok(resp.json::<ProcedureResponse>()?.procedure)
    }

    pub fn create_procedure(
        &self,
        procedure: mongreldb_core::StoredProcedure,
    ) -> ClientResult<mongreldb_core::StoredProcedure> {
        let resp = self
            .client
            .post(self.url("/procedures"))
            .json(&ProcedureRequest { procedure })
            .send()?;
        let resp = self.check(resp)?;
        Ok(resp.json::<ProcedureResponse>()?.procedure)
    }

    pub fn replace_procedure(
        &self,
        name: &str,
        procedure: mongreldb_core::StoredProcedure,
    ) -> ClientResult<mongreldb_core::StoredProcedure> {
        let resp = self
            .client
            .put(self.url(&format!("/procedures/{name}")))
            .json(&ProcedureRequest { procedure })
            .send()?;
        let resp = self.check(resp)?;
        Ok(resp.json::<ProcedureResponse>()?.procedure)
    }

    pub fn drop_procedure(&self, name: &str) -> ClientResult<()> {
        let resp = self
            .client
            .delete(self.url(&format!("/procedures/{name}")))
            .send()?;
        self.check(resp)?;
        Ok(())
    }

    pub fn call_procedure(
        &self,
        name: &str,
        req: &ProcedureCallRequest,
    ) -> ClientResult<ProcedureCallResponse> {
        let resp = self
            .client
            .post(self.url(&format!("/procedures/{name}/call")))
            .json(req)
            .send()?;
        let resp = self.check(resp)?;
        Ok(resp.json()?)
    }

    pub fn kit_call_procedure(
        &self,
        name: &str,
        req: &ProcedureCallRequest,
    ) -> ClientResult<ProcedureCallResponse> {
        let resp = self
            .client
            .post(self.url(&format!("/kit/procedures/{name}/call")))
            .json(req)
            .send()?;
        let resp = self.check(resp)?;
        Ok(resp.json()?)
    }

    pub fn triggers(&self) -> ClientResult<Vec<mongreldb_core::StoredTrigger>> {
        let resp = self.client.get(self.url("/triggers")).send()?;
        let resp = self.check(resp)?;
        Ok(resp.json::<TriggersResponse>()?.triggers)
    }

    pub fn trigger(&self, name: &str) -> ClientResult<mongreldb_core::StoredTrigger> {
        let resp = self
            .client
            .get(self.url(&format!("/triggers/{name}")))
            .send()?;
        let resp = self.check(resp)?;
        Ok(resp.json::<TriggerResponse>()?.trigger)
    }

    pub fn create_trigger(
        &self,
        trigger: mongreldb_core::StoredTrigger,
    ) -> ClientResult<mongreldb_core::StoredTrigger> {
        self.create_trigger_with_idempotency_key(trigger, None::<String>)
    }

    pub fn create_trigger_with_idempotency_key(
        &self,
        trigger: mongreldb_core::StoredTrigger,
        idempotency_key: Option<impl Into<String>>,
    ) -> ClientResult<mongreldb_core::StoredTrigger> {
        let resp = self
            .client
            .post(self.url("/triggers"))
            .json(&TriggerRequest {
                trigger,
                idempotency_key: idempotency_key.map(Into::into),
            })
            .send()?;
        let resp = self.check(resp)?;
        Ok(resp.json::<TriggerResponse>()?.trigger)
    }

    pub fn replace_trigger(
        &self,
        name: &str,
        trigger: mongreldb_core::StoredTrigger,
    ) -> ClientResult<mongreldb_core::StoredTrigger> {
        self.replace_trigger_with_idempotency_key(name, trigger, None::<String>)
    }

    pub fn replace_trigger_with_idempotency_key(
        &self,
        name: &str,
        trigger: mongreldb_core::StoredTrigger,
        idempotency_key: Option<impl Into<String>>,
    ) -> ClientResult<mongreldb_core::StoredTrigger> {
        let resp = self
            .client
            .put(self.url(&format!("/triggers/{name}")))
            .json(&TriggerRequest {
                trigger,
                idempotency_key: idempotency_key.map(Into::into),
            })
            .send()?;
        let resp = self.check(resp)?;
        Ok(resp.json::<TriggerResponse>()?.trigger)
    }

    pub fn drop_trigger(&self, name: &str) -> ClientResult<()> {
        self.drop_trigger_with_idempotency_key(name, None::<String>)
    }

    pub fn drop_trigger_with_idempotency_key(
        &self,
        name: &str,
        idempotency_key: Option<impl Into<String>>,
    ) -> ClientResult<()> {
        let mut request = self.client.delete(self.url(&format!("/triggers/{name}")));
        if let Some(idempotency_key) = idempotency_key {
            request = request.header("Idempotency-Key", idempotency_key.into());
        }
        self.check(request.send()?)?;
        Ok(())
    }
}

#[derive(Serialize, Clone)]
pub struct ColumnDefJson {
    pub id: u16,
    pub name: String,
    pub ty: String,
    pub primary_key: bool,
}

#[derive(Serialize, Clone)]
pub struct TxnOp {
    pub table: String,
    pub op: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cells: Option<Vec<serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_id: Option<u64>,
}

fn value_to_json(v: &mongreldb_core::Value) -> serde_json::Value {
    match v {
        mongreldb_core::Value::Int64(n) => serde_json::Value::Number((*n).into()),
        mongreldb_core::Value::Float64(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        mongreldb_core::Value::Bytes(b) => {
            serde_json::Value::String(String::from_utf8_lossy(b).into_owned())
        }
        mongreldb_core::Value::Bool(b) => serde_json::Value::Bool(*b),
        mongreldb_core::Value::Null => serde_json::Value::Null,
        other => serde_json::Value::String(format!("{other:?}")),
    }
}

fn read_arrow_ipc(bytes: &[u8]) -> ClientResult<Vec<RecordBatch>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let cursor = Cursor::new(bytes);
    let reader = FileReader::try_new(cursor, None)
        .map_err(|e| ClientError::Decode(format!("arrow ipc: {e}")))?;
    reader
        .into_iter()
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| ClientError::Decode(format!("arrow read: {e}")))
}

/// A replication follower that polls a leader's `/wal/stream` endpoint and
/// applies WAL records to a local database directory via `SharedWal::replay`.
///
/// Usage:
/// ```no_run
/// use mongreldb_client::ReplicationFollower;
///
/// let mut follower = ReplicationFollower::new("http://leader:8453", "/local/copy");
/// follower.sync(); // fetches and applies all new records since last sync
/// ```
pub struct ReplicationFollower {
    leader_url: String,
    local_path: std::path::PathBuf,
    client: reqwest::blocking::Client,
    last_epoch: u64,
    bearer_token: Option<String>,
    basic_auth: Option<(String, String)>,
    local_passphrase: Option<String>,
    local_credentials: Option<(String, String)>,
}

impl ReplicationFollower {
    /// Create a follower. `leader_url` is the daemon base URL; `local_path` is
    /// the local database directory to sync into.
    pub fn new(leader_url: &str, local_path: impl AsRef<std::path::Path>) -> Self {
        let local_path = local_path.as_ref().to_path_buf();
        let last_epoch = mongreldb_core::replica_epoch(&local_path).unwrap_or(0);
        Self {
            leader_url: leader_url.trim_end_matches('/').to_string(),
            local_path,
            client: reqwest::blocking::Client::new(),
            last_epoch,
            bearer_token: None,
            basic_auth: None,
            local_passphrase: None,
            local_credentials: None,
        }
    }

    pub fn with_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.bearer_token = Some(token.into());
        self
    }

    pub fn with_basic_auth(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.basic_auth = Some((username.into(), password.into()));
        self
    }

    pub fn with_local_encryption_passphrase(mut self, passphrase: impl Into<String>) -> Self {
        self.local_passphrase = Some(passphrase.into());
        self
    }

    pub fn with_local_credentials(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.local_credentials = Some((username.into(), password.into()));
        self
    }

    /// Bootstrap when needed, fetch complete committed transactions, append
    /// them durably, recover the local database, then advance the watermark.
    pub fn sync(&mut self) -> Result<usize, String> {
        if !self.local_path.join("CATALOG").exists() {
            self.bootstrap()?;
        } else if !mongreldb_core::is_replica(&self.local_path) {
            return Err(format!(
                "refusing to overwrite non-replica database at {}",
                self.local_path.display()
            ));
        }

        let mut resp = self.fetch_wal()?;
        if resp.status() == reqwest::StatusCode::CONFLICT {
            self.bootstrap()?;
            resp = self.fetch_wal()?;
        }
        if !resp.status().is_success() {
            return Err(format!("leader returned {}", resp.status()));
        }
        let leader_epoch = resp
            .headers()
            .get("x-mongreldb-current-epoch")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(self.last_epoch);
        let body = resp
            .text()
            .map_err(|e| format!("failed to read response: {e}"))?;
        let mut records = Vec::new();
        for line in body.lines() {
            if line.trim().is_empty() {
                continue;
            }
            records.push(
                serde_json::from_str::<mongreldb_core::wal::Record>(line)
                    .map_err(|error| format!("invalid WAL record from leader: {error}"))?,
            );
        }
        if records.is_empty() {
            if leader_epoch > self.last_epoch {
                return Err("leader returned no WAL records for a newer epoch".into());
            }
            return Ok(0);
        }

        let local = self.open_local()?;
        let applied_epoch = local
            .append_replication_batch(&records)
            .map_err(|error| error.to_string())?;
        drop(local);
        let recovered = self.open_local()?;
        if recovered.visible_epoch().0 < applied_epoch {
            return Err(format!(
                "replica recovery stopped at epoch {}, expected {applied_epoch}",
                recovered.visible_epoch().0
            ));
        }
        drop(recovered);
        mongreldb_core::write_replica_epoch(&self.local_path, applied_epoch)
            .map_err(|error| error.to_string())?;
        self.last_epoch = applied_epoch;
        Ok(records.len())
    }

    pub fn bootstrap(&mut self) -> Result<(), String> {
        let url = format!("{}/replication/snapshot", self.leader_url);
        let response = self
            .request(&url)
            .send()
            .map_err(|error| format!("failed to fetch replication snapshot: {error}"))?;
        if !response.status().is_success() {
            return Err(format!(
                "leader snapshot endpoint returned {}",
                response.status()
            ));
        }
        let bytes = response
            .bytes()
            .map_err(|error| format!("failed to read replication snapshot: {error}"))?;
        let snapshot = mongreldb_core::ReplicationSnapshot::decode(&bytes)
            .map_err(|error| error.to_string())?;
        snapshot
            .install(&self.local_path)
            .map_err(|error| error.to_string())?;
        self.last_epoch = snapshot.epoch();
        Ok(())
    }

    fn fetch_wal(&self) -> Result<reqwest::blocking::Response, String> {
        let url = format!("{}/wal/stream?since={}", self.leader_url, self.last_epoch);
        self.request(&url)
            .send()
            .map_err(|error| format!("failed to connect to leader: {error}"))
    }

    fn request(&self, url: &str) -> reqwest::blocking::RequestBuilder {
        let request = self.client.get(url);
        if let Some(token) = &self.bearer_token {
            request.bearer_auth(token)
        } else if let Some((username, password)) = &self.basic_auth {
            request.basic_auth(username, Some(password))
        } else {
            request
        }
    }

    fn open_local(&self) -> Result<mongreldb_core::Database, String> {
        let result = match (&self.local_passphrase, &self.local_credentials) {
            (Some(passphrase), Some((username, password))) => {
                mongreldb_core::Database::open_encrypted_with_credentials(
                    &self.local_path,
                    passphrase,
                    username,
                    password,
                )
            }
            (Some(passphrase), None) => {
                mongreldb_core::Database::open_encrypted(&self.local_path, passphrase)
            }
            (None, Some((username, password))) => mongreldb_core::Database::open_with_credentials(
                &self.local_path,
                username,
                password,
            ),
            (None, None) => mongreldb_core::Database::open(&self.local_path),
        };
        result.map_err(|error| format!("failed to open local replica: {error}"))
    }

    /// The highest leader commit epoch applied so far.
    pub fn last_epoch(&self) -> u64 {
        self.last_epoch
    }

    /// Backward-compatible alias for [`Self::last_epoch`].
    pub fn last_seq(&self) -> u64 {
        self.last_epoch
    }
}
