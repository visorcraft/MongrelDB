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

    pub fn create_table(
        &self,
        name: &str,
        columns: Vec<ColumnDefJson>,
    ) -> ClientResult<u64> {
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

    pub fn put(
        &self,
        table: &str,
        row: Vec<(u16, mongreldb_core::Value)>,
    ) -> ClientResult<u64> {
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
        let resp = self
            .client
            .post(self.url("/kit/txn"))
            .json(req)
            .send()?;
        let resp = self.check(resp)?;
        Ok(resp.json()?)
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
