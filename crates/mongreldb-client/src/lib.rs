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

pub mod native;

use std::io::{Cursor, Read};

use arrow::ipc::reader::FileReader;
use arrow::record_batch::RecordBatch;
use secrecy::ExposeSecret;
pub use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zeroize::Zeroizing;

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
        committed: Option<bool>,
        epoch: Option<u64>,
        epoch_text: Option<String>,
        retryable: Option<bool>,
    },
    #[error("remote query {code}: {message}")]
    Query {
        status: u16,
        code: RemoteQueryErrorCode,
        message: String,
        response: Box<RemoteQueryErrorResponse>,
    },
    #[error("query {query_id} outcome unknown after transport loss: {message}")]
    QueryOutcomeUnknown {
        query_id: String,
        message: String,
        status: Option<Box<RemoteQueryStatus>>,
        cancel_outcome: Option<RemoteCancelOutcome>,
    },
    #[error("native RPC {code}: {category}: {message}")]
    Native {
        code: String,
        category_code: Option<u32>,
        category: String,
        message: String,
        retryable: bool,
    },
}

/// Typed error codes mirrored from the server's Kit error envelope.
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
    AuthRequired,
    PermissionDenied,
    DeadlineExceeded,
    WorkBudgetExceeded,
    Cancelled,
    CommitOutcome,
    QueryOutcomeUnknown,
    IdempotencyKeyReuseMismatch,
    IdempotencyStoreFull,
    IdempotencyStoreUnavailable,
    InvalidIdempotencyKey,
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
            KitErrorCode::AuthRequired => "AUTH_REQUIRED",
            KitErrorCode::PermissionDenied => "PERMISSION_DENIED",
            KitErrorCode::DeadlineExceeded => "DEADLINE_EXCEEDED",
            KitErrorCode::WorkBudgetExceeded => "WORK_BUDGET_EXCEEDED",
            KitErrorCode::Cancelled => "CANCELLED",
            KitErrorCode::CommitOutcome => "COMMIT_OUTCOME",
            KitErrorCode::QueryOutcomeUnknown => "QUERY_OUTCOME_UNKNOWN",
            KitErrorCode::IdempotencyKeyReuseMismatch => "IDEMPOTENCY_KEY_REUSE_MISMATCH",
            KitErrorCode::IdempotencyStoreFull => "IDEMPOTENCY_STORE_FULL",
            KitErrorCode::IdempotencyStoreUnavailable => "IDEMPOTENCY_STORE_UNAVAILABLE",
            KitErrorCode::InvalidIdempotencyKey => "INVALID_IDEMPOTENCY_KEY",
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
            "AUTH_REQUIRED" => KitErrorCode::AuthRequired,
            "PERMISSION_DENIED" => KitErrorCode::PermissionDenied,
            "DEADLINE_EXCEEDED" => KitErrorCode::DeadlineExceeded,
            "WORK_BUDGET_EXCEEDED" => KitErrorCode::WorkBudgetExceeded,
            "CANCELLED" => KitErrorCode::Cancelled,
            "COMMIT_OUTCOME" => KitErrorCode::CommitOutcome,
            "QUERY_OUTCOME_UNKNOWN" => KitErrorCode::QueryOutcomeUnknown,
            "IDEMPOTENCY_KEY_REUSE_MISMATCH" => KitErrorCode::IdempotencyKeyReuseMismatch,
            "IDEMPOTENCY_STORE_FULL" => KitErrorCode::IdempotencyStoreFull,
            "IDEMPOTENCY_STORE_UNAVAILABLE" => KitErrorCode::IdempotencyStoreUnavailable,
            "INVALID_IDEMPOTENCY_KEY" => KitErrorCode::InvalidIdempotencyKey,
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

const MAX_CONTROL_RESPONSE_BYTES: u64 = 1024 * 1024;
// Server output-byte limits cover result data, not the enclosing JSON/Arrow
// protocol metadata. Keep bounded headroom for a valid 64 MiB result.
const MAX_SQL_RESPONSE_BYTES: u64 = 65 * 1024 * 1024;
const MAX_REPLICATION_WAL_RESPONSE_BYTES: u64 = 256 * 1024 * 1024;
const MAX_REPLICATION_SNAPSHOT_BYTES: u64 = 512 * 1024 * 1024;

struct StrictJsonValue(serde_json::Value);

impl<'de> Deserialize<'de> for StrictJsonValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = StrictJsonValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON value without duplicate object keys")
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
                Ok(StrictJsonValue(value.into()))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
                Ok(StrictJsonValue(value.into()))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
                Ok(StrictJsonValue(value.into()))
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                serde_json::Number::from_f64(value)
                    .map(serde_json::Value::Number)
                    .map(StrictJsonValue)
                    .ok_or_else(|| E::custom("non-finite JSON number"))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
                Ok(StrictJsonValue(value.into()))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
                Ok(StrictJsonValue(value.into()))
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(StrictJsonValue(serde_json::Value::Null))
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(StrictJsonValue(serde_json::Value::Null))
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                <StrictJsonValue as Deserialize>::deserialize(deserializer)
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or(0));
                while let Some(value) = sequence.next_element::<StrictJsonValue>()? {
                    values.push(value.0);
                }
                Ok(StrictJsonValue(serde_json::Value::Array(values)))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut values = serde_json::Map::with_capacity(map.size_hint().unwrap_or(0));
                while let Some(key) = map.next_key::<String>()? {
                    if values.contains_key(&key) {
                        return Err(serde::de::Error::custom(format!(
                            "duplicate JSON object key {key:?}"
                        )));
                    }
                    let value = map.next_value::<StrictJsonValue>()?;
                    values.insert(key, value.0);
                }
                Ok(StrictJsonValue(serde_json::Value::Object(values)))
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

fn strict_json_value(bytes: &[u8]) -> Result<serde_json::Value, String> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value =
        StrictJsonValue::deserialize(&mut deserializer).map_err(|error| error.to_string())?;
    deserializer.end().map_err(|error| error.to_string())?;
    Ok(value.0)
}

fn strict_json<T: serde::de::DeserializeOwned>(bytes: &[u8], context: &str) -> ClientResult<T> {
    let value = strict_json_value(bytes)
        .map_err(|error| ClientError::Decode(format!("invalid {context} response: {error}")))?;
    serde_json::from_value(value)
        .map_err(|error| ClientError::Decode(format!("invalid {context} response: {error}")))
}

fn strict_roundtrip_json<T>(bytes: &[u8], context: &str) -> ClientResult<T>
where
    T: serde::de::DeserializeOwned + Serialize,
{
    let value = strict_json_value(bytes)
        .map_err(|error| ClientError::Decode(format!("invalid {context}: {error}")))?;
    let parsed: T = serde_json::from_value(value.clone())
        .map_err(|error| ClientError::Decode(format!("invalid {context}: {error}")))?;
    if serde_json::to_value(&parsed)
        .map_err(|error| ClientError::Decode(format!("invalid {context}: {error}")))?
        != value
    {
        return Err(ClientError::Decode(format!(
            "invalid {context}: unknown or non-canonical fields"
        )));
    }
    Ok(parsed)
}

fn bounded_blocking_bytes(
    mut response: reqwest::blocking::Response,
    limit: u64,
) -> Result<Vec<u8>, String> {
    if response
        .content_length()
        .is_some_and(|length| length > limit)
    {
        return Err(format!("response exceeds {limit} bytes"));
    }
    let mut bytes = Vec::new();
    response
        .by_ref()
        .take(limit.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() as u64 > limit {
        return Err(format!("response exceeds {limit} bytes"));
    }
    Ok(bytes)
}

async fn bounded_async_bytes(
    mut response: reqwest::Response,
    limit: u64,
) -> Result<Vec<u8>, String> {
    if response
        .content_length()
        .is_some_and(|length| length > limit)
    {
        return Err(format!("response exceeds {limit} bytes"));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|error| error.to_string())? {
        if (bytes.len() as u64).saturating_add(chunk.len() as u64) > limit {
            return Err(format!("response exceeds {limit} bytes"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn decode_blocking_json<T: serde::de::DeserializeOwned>(
    response: reqwest::blocking::Response,
    limit: u64,
    context: &str,
) -> ClientResult<T> {
    let bytes = bounded_blocking_bytes(response, limit)
        .map_err(|error| ClientError::Decode(format!("invalid {context} response: {error}")))?;
    strict_json(&bytes, context)
}

async fn decode_async_json<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
    limit: u64,
    context: &str,
) -> ClientResult<T> {
    let bytes = bounded_async_bytes(response, limit)
        .await
        .map_err(|error| ClientError::Decode(format!("invalid {context} response: {error}")))?;
    strict_json(&bytes, context)
}

#[derive(Clone)]
pub enum RemoteAuth {
    Bearer(SecretString),
    Basic {
        username: String,
        password: SecretString,
    },
}

impl std::fmt::Debug for RemoteAuth {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bearer(_) => formatter.write_str("Bearer([REDACTED])"),
            Self::Basic { username, .. } => formatter
                .debug_struct("Basic")
                .field("username", username)
                .field("password", &"[REDACTED]")
                .finish(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RemoteOptions {
    pub auth: Option<RemoteAuth>,
    pub transport_timeout: Option<std::time::Duration>,
}

#[derive(Clone)]
pub struct MongrelClient {
    base_url: String,
    client: reqwest::blocking::Client,
    transport: ClientTransportOptions,
}

#[derive(Clone, Copy, Default)]
struct ClientTransportOptions {
    connect_timeout: Option<std::time::Duration>,
    request_timeout: Option<std::time::Duration>,
    pool_idle_timeout: Option<std::time::Duration>,
}

/// Builder for a blocking [`MongrelClient`]. Authorization is installed as a
/// sensitive default header, so every route uses it and debug output redacts it.
pub struct MongrelClientBuilder {
    base_url: String,
    invalid_base_url: bool,
    authorization: Option<reqwest::header::HeaderValue>,
    invalid_authorization: bool,
    connect_timeout: Option<std::time::Duration>,
    request_timeout: Option<std::time::Duration>,
    pool_idle_timeout: Option<std::time::Duration>,
}

/// Builder for an [`AsyncMongrelClient`].
pub struct AsyncMongrelClientBuilder {
    base_url: String,
    invalid_base_url: bool,
    authorization: Option<reqwest::header::HeaderValue>,
    invalid_authorization: bool,
    connect_timeout: Option<std::time::Duration>,
    request_timeout: Option<std::time::Duration>,
    pool_idle_timeout: Option<std::time::Duration>,
}

fn bearer_header(token: &str) -> ClientResult<reqwest::header::HeaderValue> {
    let value = Zeroizing::new(format!("Bearer {token}"));
    sensitive_header(value.as_str())
}

fn basic_header(username: &str, password: &str) -> ClientResult<reqwest::header::HeaderValue> {
    let credentials = Zeroizing::new(format!("{username}:{password}"));
    let encoded = Zeroizing::new(base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        credentials.as_bytes(),
    ));
    let value = Zeroizing::new(format!("Basic {}", encoded.as_str()));
    sensitive_header(value.as_str())
}

fn sensitive_header(value: &str) -> ClientResult<reqwest::header::HeaderValue> {
    let mut value = reqwest::header::HeaderValue::from_str(value)
        .map_err(|_| ClientError::Transport("invalid authorization credentials".into()))?;
    value.set_sensitive(true);
    Ok(value)
}

fn default_headers(
    authorization: Option<reqwest::header::HeaderValue>,
) -> reqwest::header::HeaderMap {
    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(value) = authorization {
        headers.insert(reqwest::header::AUTHORIZATION, value);
    }
    headers
}

fn url_with_segments(base_url: &str, segments: &[&str]) -> ClientResult<String> {
    let mut url = reqwest::Url::parse(base_url)
        .map_err(|_| ClientError::Transport("invalid client base URL".into()))?;
    {
        let mut path = url
            .path_segments_mut()
            .map_err(|_| ClientError::Transport("client base URL cannot contain a path".into()))?;
        path.pop_if_empty();
        path.extend(segments.iter().copied());
    }
    Ok(url.into())
}

fn blocking_http_client(
    authorization: Option<reqwest::header::HeaderValue>,
    transport: ClientTransportOptions,
) -> ClientResult<reqwest::blocking::Client> {
    let mut builder =
        reqwest::blocking::Client::builder().default_headers(default_headers(authorization));
    if let Some(timeout) = transport.connect_timeout {
        builder = builder.connect_timeout(timeout);
    }
    if let Some(timeout) = transport.request_timeout {
        builder = builder.timeout(timeout);
    }
    if let Some(timeout) = transport.pool_idle_timeout {
        builder = builder.pool_idle_timeout(timeout);
    }
    Ok(builder.build()?)
}

fn async_http_client(
    authorization: Option<reqwest::header::HeaderValue>,
    transport: ClientTransportOptions,
) -> ClientResult<reqwest::Client> {
    let mut builder = reqwest::Client::builder().default_headers(default_headers(authorization));
    if let Some(timeout) = transport.connect_timeout {
        builder = builder.connect_timeout(timeout);
    }
    if let Some(timeout) = transport.request_timeout {
        builder = builder.timeout(timeout);
    }
    if let Some(timeout) = transport.pool_idle_timeout {
        builder = builder.pool_idle_timeout(timeout);
    }
    Ok(builder.build()?)
}

fn sanitized_base_url(value: &str) -> Option<String> {
    let url = reqwest::Url::parse(value).ok()?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return None;
    }
    Some(url.as_str().trim_end_matches('/').to_owned())
}

impl MongrelClientBuilder {
    pub fn connect_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    pub fn request_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    pub fn pool_idle_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.pool_idle_timeout = Some(timeout);
        self
    }

    pub fn bearer_token(mut self, token: impl AsRef<str>) -> Self {
        match bearer_header(token.as_ref()) {
            Ok(value) => self.authorization = Some(value),
            Err(_) => self.invalid_authorization = true,
        }
        self
    }

    pub fn basic_auth(mut self, username: impl AsRef<str>, password: impl AsRef<str>) -> Self {
        match basic_header(username.as_ref(), password.as_ref()) {
            Ok(value) => self.authorization = Some(value),
            Err(_) => self.invalid_authorization = true,
        }
        self
    }

    pub fn build(self) -> ClientResult<MongrelClient> {
        if self.invalid_base_url {
            return Err(ClientError::Transport(
                "invalid MongrelDB server URL; use HTTP(S) without credentials, query, or fragment"
                    .into(),
            ));
        }
        if self.invalid_authorization {
            return Err(ClientError::Transport(
                "invalid authorization credentials".into(),
            ));
        }
        let transport = ClientTransportOptions {
            connect_timeout: self.connect_timeout,
            request_timeout: self.request_timeout,
            pool_idle_timeout: self.pool_idle_timeout,
        };
        let client = blocking_http_client(self.authorization, transport)?;
        Ok(MongrelClient {
            base_url: self.base_url,
            client,
            transport,
        })
    }
}

impl AsyncMongrelClientBuilder {
    pub fn connect_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    pub fn request_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    pub fn pool_idle_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.pool_idle_timeout = Some(timeout);
        self
    }

    pub fn bearer_token(mut self, token: impl AsRef<str>) -> Self {
        match bearer_header(token.as_ref()) {
            Ok(value) => self.authorization = Some(value),
            Err(_) => self.invalid_authorization = true,
        }
        self
    }

    pub fn basic_auth(mut self, username: impl AsRef<str>, password: impl AsRef<str>) -> Self {
        match basic_header(username.as_ref(), password.as_ref()) {
            Ok(value) => self.authorization = Some(value),
            Err(_) => self.invalid_authorization = true,
        }
        self
    }

    pub fn build(self) -> ClientResult<AsyncMongrelClient> {
        if self.invalid_base_url {
            return Err(ClientError::Transport(
                "invalid MongrelDB server URL; use HTTP(S) without credentials, query, or fragment"
                    .into(),
            ));
        }
        if self.invalid_authorization {
            return Err(ClientError::Transport(
                "invalid authorization credentials".into(),
            ));
        }
        let transport = ClientTransportOptions {
            connect_timeout: self.connect_timeout,
            request_timeout: self.request_timeout,
            pool_idle_timeout: self.pool_idle_timeout,
        };
        let client = async_http_client(self.authorization, transport)?;
        Ok(AsyncMongrelClient {
            base_url: self.base_url,
            client,
            transport,
        })
    }
}

impl MongrelClient {
    pub fn try_with_bearer_token(mut self, token: impl AsRef<str>) -> ClientResult<Self> {
        self.client = blocking_http_client(Some(bearer_header(token.as_ref())?), self.transport)?;
        Ok(self)
    }

    pub fn with_bearer_token(self, token: impl AsRef<str>) -> ClientResult<Self> {
        self.try_with_bearer_token(token)
    }

    pub fn try_with_basic_auth(
        mut self,
        username: impl AsRef<str>,
        password: impl AsRef<str>,
    ) -> ClientResult<Self> {
        self.client = blocking_http_client(
            Some(basic_header(username.as_ref(), password.as_ref())?),
            self.transport,
        )?;
        Ok(self)
    }

    pub fn with_basic_auth(
        self,
        username: impl AsRef<str>,
        password: impl AsRef<str>,
    ) -> ClientResult<Self> {
        self.try_with_basic_auth(username, password)
    }
}

/// Async counterpart for Kit and health calls.
#[derive(Clone)]
pub struct AsyncMongrelClient {
    base_url: String,
    client: reqwest::Client,
    transport: ClientTransportOptions,
}

const SQL_RECOVERY_WINDOW: std::time::Duration = std::time::Duration::from_secs(2);
const SQL_RECOVERY_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(250);
const SQL_RECOVERY_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(25);

#[derive(Debug, Clone, Default)]
pub struct SqlClientOptions {
    pub query_id: Option<mongreldb_query::QueryId>,
    pub timeout: Option<std::time::Duration>,
}

pub type RemoteSqlControlOptions = SqlClientOptions;

#[derive(Debug, Clone)]
pub struct SqlPageOptions {
    pub query_id: Option<mongreldb_query::QueryId>,
    pub timeout: Option<std::time::Duration>,
    pub max_output_rows: Option<u64>,
    pub max_output_bytes: Option<u64>,
    pub page_size_rows: u64,
    pub projection: Vec<String>,
    pub max_page_bytes: Option<u64>,
    pub max_page_tokens: Option<u64>,
}

impl SqlPageOptions {
    pub fn new(page_size_rows: u64, projection: Vec<String>) -> Self {
        Self {
            query_id: None,
            timeout: None,
            max_output_rows: None,
            max_output_bytes: None,
            page_size_rows,
            projection,
            max_page_bytes: None,
            max_page_tokens: None,
        }
    }
}

fn validate_sql_page_options(options: &SqlPageOptions) -> ClientResult<()> {
    if options.page_size_rows == 0
        || options.max_output_rows == Some(0)
        || options.max_output_bytes == Some(0)
        || options.max_page_bytes == Some(0)
        || options.max_page_tokens == Some(0)
    {
        return Err(ClientError::Decode(
            "SQL pagination row, byte, and token limits must be positive".into(),
        ));
    }
    if options.projection.is_empty() || options.projection.len() > 128 {
        return Err(ClientError::Decode(
            "SQL pagination projection must contain 1 to 128 columns".into(),
        ));
    }
    let mut seen = std::collections::HashSet::new();
    let projection_bytes = options
        .projection
        .iter()
        .map(String::len)
        .fold(0usize, usize::saturating_add);
    if projection_bytes > 16 * 1024
        || options.projection.iter().any(|column| {
            column.is_empty()
                || column == "*"
                || column.len() > 256
                || !seen.insert(column.as_str())
        })
    {
        return Err(ClientError::Decode(
            "SQL pagination projection requires unique explicit column names of at most 256 bytes"
                .into(),
        ));
    }
    Ok(())
}

fn validate_remote_sql_page(
    page: RemoteSqlPage,
    initial_options: Option<&SqlPageOptions>,
) -> Result<RemoteSqlPage, String> {
    let metadata = &page.page;
    let end = metadata
        .offset
        .checked_add(metadata.row_count)
        .ok_or_else(|| "SQL page offset overflowed".to_owned())?;
    if page.status != "completed" {
        return Err("SQL page status is not completed".into());
    }
    if metadata.row_count != page.rows.len() {
        return Err("SQL page row_count does not match rows".into());
    }
    if page.rows.iter().any(|row| !row.is_object()) {
        return Err("SQL page rows must be JSON objects".into());
    }
    if metadata.projection.is_empty()
        || metadata.projection.iter().any(|column| column.is_empty())
        || metadata
            .projection
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len()
            != metadata.projection.len()
        || page.rows.iter().any(|row| match row.as_object() {
            Some(object) => {
                object.len() != metadata.projection.len()
                    || metadata
                        .projection
                        .iter()
                        .any(|column| !object.contains_key(column))
            }
            None => true,
        })
    {
        return Err("SQL page rows do not exactly match the projection".into());
    }
    let byte_count = page.rows.iter().try_fold(2_usize, |bytes, row| {
        serde_json::to_vec(row)
            .map(|encoded| {
                bytes
                    .saturating_add(usize::from(bytes > 2))
                    .saturating_add(encoded.len())
            })
            .map_err(|error| error.to_string())
    })?;
    if metadata.byte_count != byte_count
        || metadata.estimated_tokens != byte_count.saturating_add(3) / 4
    {
        return Err("SQL page byte or token estimate is invalid".into());
    }
    if metadata.offset > metadata.total_rows || end > metadata.total_rows {
        return Err("SQL page offset or row_count exceeds total_rows".into());
    }
    if metadata.limits.rows == 0
        || metadata.limits.bytes == 0
        || metadata.limits.tokens == 0
        || metadata.row_count > metadata.limits.rows
        || metadata.byte_count > metadata.limits.bytes
        || metadata.estimated_tokens > metadata.limits.tokens
    {
        return Err("SQL page exceeds its declared limits".into());
    }
    if metadata.expires_at_ms == 0
        || metadata.snapshot != "retained_result"
        || metadata.token_estimate != "ceil(projected_json_bytes/4)"
    {
        return Err("SQL page metadata is invalid".into());
    }
    let has_more = end < metadata.total_rows;
    if (has_more && metadata.row_count == 0)
        || has_more != page.next_cursor.is_some()
        || page
            .next_cursor
            .as_ref()
            .is_some_and(|cursor| cursor.is_empty() || cursor.len() > 2_048)
    {
        return Err("SQL page continuation cursor is inconsistent".into());
    }
    if let Some(options) = initial_options {
        if metadata.offset != 0
            || metadata.projection != options.projection
            || metadata.limits.rows as u64 > options.page_size_rows
            || options
                .max_page_bytes
                .is_some_and(|limit| metadata.limits.bytes as u64 > limit)
            || options
                .max_page_tokens
                .is_some_and(|limit| metadata.limits.tokens as u64 > limit)
            || options
                .max_output_rows
                .is_some_and(|limit| metadata.total_rows as u64 > limit)
            || options
                .max_output_bytes
                .is_some_and(|limit| metadata.byte_count as u64 > limit)
        {
            return Err("SQL page does not match the requested pagination options".into());
        }
    }
    Ok(page)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteCancelOutcome {
    Accepted,
    AlreadyCancelling,
    TooLate,
    AlreadyFinished,
    NotFound,
    PreCancelled,
}

fn cancel_outcome_from_wire(value: Option<&str>) -> Option<RemoteCancelOutcome> {
    match value {
        Some("accepted" | "cancellation_requested") => Some(RemoteCancelOutcome::Accepted),
        Some("already_cancelling" | "cancelling") => Some(RemoteCancelOutcome::AlreadyCancelling),
        Some("too_late" | "commit_critical") => Some(RemoteCancelOutcome::TooLate),
        Some("already_finished" | "finished") => Some(RemoteCancelOutcome::AlreadyFinished),
        Some("pre_cancelled") => Some(RemoteCancelOutcome::PreCancelled),
        Some("not_found") => Some(RemoteCancelOutcome::NotFound),
        _ => None,
    }
}

fn decode_cancel_outcome(
    body: &serde_json::Value,
    expected_query_id: mongreldb_query::QueryId,
    http_status: u16,
) -> ClientResult<RemoteCancelOutcome> {
    const FIELDS: &[&str] = &[
        "query_id",
        "status",
        "terminal_state",
        "state",
        "server_state",
        "code",
        "operation",
        "committed",
        "committed_statements",
        "last_commit_epoch",
        "last_commit_epoch_text",
        "first_commit_statement_index",
        "last_commit_statement_index",
        "completed_statements",
        "statement_index",
        "cancel_outcome",
        "cancellation_reason",
        "retryable",
        "outcome",
        "terminal_error",
        "error",
        "trace",
        "started_ms_ago",
        "deadline_ms_remaining",
        "session_id",
    ];
    let object = body
        .as_object()
        .ok_or_else(|| ClientError::Decode("cancellation response is not an object".into()))?;
    if let Some(field) = object
        .keys()
        .find(|field| !FIELDS.contains(&field.as_str()))
    {
        return Err(ClientError::Decode(format!(
            "cancellation response contains unknown field {field:?}"
        )));
    }
    if let Some(outcome) = object.get("outcome") {
        serde_json::from_value::<RemoteQueryOutcome>(outcome.clone()).map_err(|error| {
            ClientError::Decode(format!("invalid cancellation outcome: {error}"))
        })?;
    }
    if let Some(error) = object.get("error") {
        serde_json::from_value::<RemoteQueryErrorBody>(error.clone())
            .map_err(|error| ClientError::Decode(format!("invalid cancellation error: {error}")))?;
    }
    if let Some(error) = object
        .get("terminal_error")
        .filter(|value| !value.is_null())
    {
        serde_json::from_value::<RemoteTerminalError>(error.clone()).map_err(|error| {
            ClientError::Decode(format!("invalid cancellation terminal error: {error}"))
        })?;
    }
    if body.get("query_id").and_then(serde_json::Value::as_str)
        != Some(expected_query_id.to_string().as_str())
    {
        return Err(ClientError::Decode(
            "cancellation response query_id does not match the request".into(),
        ));
    }
    let outcome = cancel_outcome_from_wire(
        body.get("cancel_outcome")
            .and_then(serde_json::Value::as_str),
    );
    let state = cancel_outcome_from_wire(body.get("state").and_then(serde_json::Value::as_str));
    if outcome.is_some() && state.is_some() && outcome != state {
        return Err(ClientError::Decode(
            "cancellation response state and cancel_outcome disagree".into(),
        ));
    }
    let outcome = outcome
        .or(state)
        .ok_or_else(|| ClientError::Decode("cancellation response has no valid outcome".into()))?;
    let compatible = matches!(
        (http_status, outcome),
        (
            202,
            RemoteCancelOutcome::Accepted | RemoteCancelOutcome::PreCancelled
        ) | (
            200,
            RemoteCancelOutcome::AlreadyCancelling | RemoteCancelOutcome::AlreadyFinished
        ) | (409, RemoteCancelOutcome::TooLate)
            | (404, RemoteCancelOutcome::NotFound)
    );
    if !compatible {
        return Err(ClientError::Decode(
            "cancellation HTTP status and outcome disagree".into(),
        ));
    }
    Ok(outcome)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteQueryErrorCode {
    QueryCancelled,
    DeadlineExceeded,
    QueryIdConflict,
    QueryRegistryFull,
    CancelTooLate,
    QueryAlreadyFinished,
    QueryNotFound,
    TransactionAborted,
    NoSqlTransaction,
    SavepointNotFound,
    CommitOutcome,
    QueryFailed,
    QueryCancelledAfterCommit,
    DeadlineAfterCommit,
    ResultLimitExceeded,
    SerializationFailed,
    SerializationFailedAfterCommit,
    SerializationWorkerFailed,
    CapabilityUnsupported,
    QueryOutcomeUnknown,
    InvalidQueryOptions,
    IncompatibleSqlControls,
    InvalidIdempotencyKey,
    IdempotencyKeyReuseMismatch,
    IdempotencyRequiresJson,
    IdempotencyRequiresSingleWrite,
    IdempotencyStoreFull,
    IdempotencyUnsupportedInTransaction,
    IdempotencyStoreUnavailable,
    PaginationRequiresJson,
    PaginationRequiresSingleReadQuery,
    InvalidPaginationOptions,
    InvalidSqlCursor,
    SqlCursorExpired,
    SqlCursorNotFound,
    InvalidSqlProjection,
    InvalidPageOffset,
    SqlPageStoreFull,
    SqlAdmissionClosed,
    EntropyUnavailable,
    Other(String),
}

impl RemoteQueryErrorCode {
    pub fn as_str(&self) -> &str {
        match self {
            Self::QueryCancelled => "QUERY_CANCELLED",
            Self::DeadlineExceeded => "DEADLINE_EXCEEDED",
            Self::QueryIdConflict => "QUERY_ID_CONFLICT",
            Self::QueryRegistryFull => "QUERY_REGISTRY_FULL",
            Self::CancelTooLate => "CANCEL_TOO_LATE",
            Self::QueryAlreadyFinished => "QUERY_ALREADY_FINISHED",
            Self::QueryNotFound => "QUERY_NOT_FOUND",
            Self::TransactionAborted => "TRANSACTION_ABORTED",
            Self::NoSqlTransaction => "NO_SQL_TRANSACTION",
            Self::SavepointNotFound => "SAVEPOINT_NOT_FOUND",
            Self::CommitOutcome => "COMMIT_OUTCOME",
            Self::QueryFailed => "QUERY_FAILED",
            Self::QueryCancelledAfterCommit => "QUERY_CANCELLED_AFTER_COMMIT",
            Self::DeadlineAfterCommit => "DEADLINE_AFTER_COMMIT",
            Self::ResultLimitExceeded => "RESULT_LIMIT_EXCEEDED",
            Self::SerializationFailed => "SERIALIZATION_FAILED",
            Self::SerializationFailedAfterCommit => "SERIALIZATION_FAILED_AFTER_COMMIT",
            Self::SerializationWorkerFailed => "SERIALIZATION_WORKER_FAILED",
            Self::CapabilityUnsupported => "CAPABILITY_UNSUPPORTED",
            Self::QueryOutcomeUnknown => "QUERY_OUTCOME_UNKNOWN",
            Self::InvalidQueryOptions => "INVALID_QUERY_OPTIONS",
            Self::IncompatibleSqlControls => "INCOMPATIBLE_SQL_CONTROLS",
            Self::InvalidIdempotencyKey => "INVALID_IDEMPOTENCY_KEY",
            Self::IdempotencyKeyReuseMismatch => "IDEMPOTENCY_KEY_REUSE_MISMATCH",
            Self::IdempotencyRequiresJson => "IDEMPOTENCY_REQUIRES_JSON",
            Self::IdempotencyRequiresSingleWrite => "IDEMPOTENCY_REQUIRES_SINGLE_WRITE",
            Self::IdempotencyStoreFull => "IDEMPOTENCY_STORE_FULL",
            Self::IdempotencyUnsupportedInTransaction => "IDEMPOTENCY_UNSUPPORTED_IN_TRANSACTION",
            Self::IdempotencyStoreUnavailable => "IDEMPOTENCY_STORE_UNAVAILABLE",
            Self::PaginationRequiresJson => "PAGINATION_REQUIRES_JSON",
            Self::PaginationRequiresSingleReadQuery => "PAGINATION_REQUIRES_SINGLE_READ_QUERY",
            Self::InvalidPaginationOptions => "INVALID_PAGINATION_OPTIONS",
            Self::InvalidSqlCursor => "INVALID_SQL_CURSOR",
            Self::SqlCursorExpired => "SQL_CURSOR_EXPIRED",
            Self::SqlCursorNotFound => "SQL_CURSOR_NOT_FOUND",
            Self::InvalidSqlProjection => "INVALID_SQL_PROJECTION",
            Self::InvalidPageOffset => "INVALID_PAGE_OFFSET",
            Self::SqlPageStoreFull => "SQL_PAGE_STORE_FULL",
            Self::SqlAdmissionClosed => "SQL_ADMISSION_CLOSED",
            Self::EntropyUnavailable => "ENTROPY_UNAVAILABLE",
            Self::Other(code) => code,
        }
    }

    fn from_code(code: &str) -> Self {
        match code {
            "QUERY_CANCELLED" => Self::QueryCancelled,
            "DEADLINE_EXCEEDED" => Self::DeadlineExceeded,
            "QUERY_ID_CONFLICT" => Self::QueryIdConflict,
            "QUERY_REGISTRY_FULL" => Self::QueryRegistryFull,
            "CANCEL_TOO_LATE" => Self::CancelTooLate,
            "QUERY_ALREADY_FINISHED" => Self::QueryAlreadyFinished,
            "QUERY_NOT_FOUND" => Self::QueryNotFound,
            "TRANSACTION_ABORTED" => Self::TransactionAborted,
            "NO_SQL_TRANSACTION" => Self::NoSqlTransaction,
            "SAVEPOINT_NOT_FOUND" => Self::SavepointNotFound,
            "COMMIT_OUTCOME" => Self::CommitOutcome,
            "QUERY_FAILED" => Self::QueryFailed,
            "QUERY_CANCELLED_AFTER_COMMIT" => Self::QueryCancelledAfterCommit,
            "DEADLINE_AFTER_COMMIT" => Self::DeadlineAfterCommit,
            "RESULT_LIMIT_EXCEEDED" => Self::ResultLimitExceeded,
            "SERIALIZATION_FAILED" => Self::SerializationFailed,
            "SERIALIZATION_FAILED_AFTER_COMMIT" => Self::SerializationFailedAfterCommit,
            "SERIALIZATION_WORKER_FAILED" => Self::SerializationWorkerFailed,
            "CAPABILITY_UNSUPPORTED" => Self::CapabilityUnsupported,
            "QUERY_OUTCOME_UNKNOWN" => Self::QueryOutcomeUnknown,
            "INVALID_QUERY_OPTIONS" => Self::InvalidQueryOptions,
            "INCOMPATIBLE_SQL_CONTROLS" => Self::IncompatibleSqlControls,
            "INVALID_IDEMPOTENCY_KEY" => Self::InvalidIdempotencyKey,
            "IDEMPOTENCY_KEY_REUSE_MISMATCH" => Self::IdempotencyKeyReuseMismatch,
            "IDEMPOTENCY_REQUIRES_JSON" => Self::IdempotencyRequiresJson,
            "IDEMPOTENCY_REQUIRES_SINGLE_WRITE" => Self::IdempotencyRequiresSingleWrite,
            "IDEMPOTENCY_STORE_FULL" => Self::IdempotencyStoreFull,
            "IDEMPOTENCY_UNSUPPORTED_IN_TRANSACTION" => Self::IdempotencyUnsupportedInTransaction,
            "IDEMPOTENCY_STORE_UNAVAILABLE" => Self::IdempotencyStoreUnavailable,
            "PAGINATION_REQUIRES_JSON" => Self::PaginationRequiresJson,
            "PAGINATION_REQUIRES_SINGLE_READ_QUERY" => Self::PaginationRequiresSingleReadQuery,
            "INVALID_PAGINATION_OPTIONS" => Self::InvalidPaginationOptions,
            "INVALID_SQL_CURSOR" => Self::InvalidSqlCursor,
            "SQL_CURSOR_EXPIRED" => Self::SqlCursorExpired,
            "SQL_CURSOR_NOT_FOUND" => Self::SqlCursorNotFound,
            "INVALID_SQL_PROJECTION" => Self::InvalidSqlProjection,
            "INVALID_PAGE_OFFSET" => Self::InvalidPageOffset,
            "SQL_PAGE_STORE_FULL" => Self::SqlPageStoreFull,
            "SQL_ADMISSION_CLOSED" => Self::SqlAdmissionClosed,
            "ENTROPY_UNAVAILABLE" => Self::EntropyUnavailable,
            other => Self::Other(other.into()),
        }
    }
}

impl std::fmt::Display for RemoteQueryErrorCode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for RemoteQueryErrorCode {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let code = String::deserialize(deserializer)?;
        Ok(Self::from_code(&code))
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteQueryOutcome {
    #[serde(default)]
    pub committed: Option<bool>,
    #[serde(default)]
    pub committed_statements: Option<usize>,
    #[serde(default)]
    pub last_commit_epoch: Option<u64>,
    #[serde(default)]
    pub last_commit_epoch_text: Option<String>,
    #[serde(default)]
    pub first_commit_statement_index: Option<usize>,
    #[serde(default)]
    pub last_commit_statement_index: Option<usize>,
    #[serde(default)]
    pub completed_statements: Option<usize>,
    #[serde(default)]
    pub statement_index: Option<usize>,
    #[serde(default)]
    pub serialization: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteTerminalError {
    pub code: RemoteQueryErrorCode,
    pub category: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteQueryErrorBody {
    pub code: RemoteQueryErrorCode,
    pub message: String,
    #[serde(default)]
    pub query_id: Option<String>,
    #[serde(default)]
    pub committed: Option<bool>,
    #[serde(default)]
    pub retryable: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteQueryErrorResponse {
    #[serde(default)]
    pub query_id: Option<String>,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub terminal_state: Option<String>,
    #[serde(default)]
    pub committed: Option<bool>,
    #[serde(default)]
    pub committed_statements: Option<usize>,
    #[serde(default)]
    pub last_commit_epoch: Option<u64>,
    #[serde(default)]
    pub last_commit_epoch_text: Option<String>,
    #[serde(default)]
    pub first_commit_statement_index: Option<usize>,
    #[serde(default)]
    pub last_commit_statement_index: Option<usize>,
    #[serde(default)]
    pub completed_statements: Option<usize>,
    #[serde(default)]
    pub statement_index: Option<usize>,
    #[serde(default)]
    pub cancel_outcome: Option<RemoteCancelOutcome>,
    #[serde(default)]
    pub cancellation_reason: Option<String>,
    #[serde(default)]
    pub retryable: bool,
    #[serde(default)]
    pub server_state: Option<String>,
    #[serde(default)]
    pub outcome: RemoteQueryOutcome,
    pub error: RemoteQueryErrorBody,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlCancellationCapabilities {
    pub version: u8,
    pub client_query_ids: bool,
    pub cancel_endpoint: bool,
    pub query_status: bool,
    #[serde(default)]
    pub pre_registration_cancel: bool,
    pub stream_disconnect_cancels: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlIdempotencyCapabilities {
    pub version: u8,
    pub durable_pre_execution_intent: bool,
    pub replay_committed_receipt: bool,
    pub indeterminate_never_reexecutes: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlPaginationCapabilities {
    pub version: u8,
    pub continuation_endpoint: String,
    pub retained_snapshot: bool,
    pub projection_required: bool,
    pub byte_and_token_hints: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerCapabilities {
    pub sql_cancellation: SqlCancellationCapabilities,
    #[serde(default)]
    pub sql_idempotency: Option<SqlIdempotencyCapabilities>,
    #[serde(default)]
    pub sql_pagination: Option<SqlPaginationCapabilities>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteQueryStatus {
    pub query_id: String,
    #[serde(default)]
    pub detail: Option<String>,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub server_state: String,
    #[serde(default)]
    pub terminal_state: Option<String>,
    #[serde(default)]
    pub operation: String,
    #[serde(default)]
    pub started_ms_ago: Option<u64>,
    #[serde(default)]
    pub deadline_ms_remaining: Option<u64>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub code: Option<RemoteQueryErrorCode>,
    #[serde(default)]
    pub committed: Option<bool>,
    #[serde(default)]
    pub cancellation_reason: String,
    #[serde(default)]
    pub committed_statements: Option<usize>,
    #[serde(default)]
    pub last_commit_epoch: Option<u64>,
    #[serde(default)]
    pub last_commit_epoch_text: Option<String>,
    #[serde(default)]
    pub first_commit_statement_index: Option<usize>,
    #[serde(default)]
    pub last_commit_statement_index: Option<usize>,
    #[serde(default)]
    pub completed_statements: Option<usize>,
    #[serde(default)]
    pub statement_index: Option<usize>,
    #[serde(default)]
    pub cancel_outcome: Option<RemoteCancelOutcome>,
    #[serde(default)]
    pub retryable: bool,
    #[serde(default)]
    pub outcome: RemoteQueryOutcome,
    #[serde(default)]
    pub terminal_error: Option<RemoteTerminalError>,
    #[serde(default)]
    pub trace: serde_json::Value,
}

impl RemoteQueryStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.server_state_or_state(),
            "completed" | "failed" | "cancelled" | "pre_cancelled" | "finished"
        )
    }

    pub fn durably_committed(&self) -> Option<bool> {
        match (self.committed, self.outcome.committed) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), _) | (_, Some(false)) => Some(false),
            (None, None) => None,
        }
    }

    pub fn server_state_or_state(&self) -> &str {
        if self.server_state.is_empty() {
            &self.state
        } else {
            &self.server_state
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteSqlReceipt {
    pub query_id: String,
    pub original_query_id: String,
    pub status: String,
    #[serde(default)]
    pub terminal_state: Option<String>,
    #[serde(default)]
    pub server_state: String,
    #[serde(default)]
    pub cancel_outcome: Option<RemoteCancelOutcome>,
    #[serde(default)]
    pub cancellation_reason: String,
    pub committed: bool,
    pub committed_statements: usize,
    pub last_commit_epoch: Option<u64>,
    #[serde(default)]
    pub last_commit_epoch_text: Option<String>,
    #[serde(default)]
    pub first_commit_statement_index: Option<usize>,
    #[serde(default)]
    pub last_commit_statement_index: Option<usize>,
    #[serde(default)]
    pub completed_statements: usize,
    #[serde(default)]
    pub statement_index: usize,
    pub retryable: bool,
    pub idempotency_replayed: bool,
    pub idempotency_persisted: bool,
    pub idempotency_expires_at_ms: u64,
    pub outcome: RemoteQueryOutcome,
    #[serde(default)]
    pub terminal_error: Option<RemoteTerminalError>,
    #[serde(default)]
    pub commit_receipt: Option<RemoteCommitReceipt>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteCommitReceipt {
    pub transaction_id: String,
    pub commit_ts_physical_micros: u64,
    pub commit_ts_logical: u32,
    pub commit_ts_node_tiebreaker: u32,
    pub log_term: u64,
    pub log_index: u64,
    pub durability: String,
}

fn exact_epoch(text: Option<&str>, numeric: Option<u64>) -> Result<Option<u64>, String> {
    match text {
        Some(text) => {
            let epoch = text
                .parse::<u64>()
                .map_err(|_| "last_commit_epoch_text is not an unsigned integer".to_owned())?;
            if epoch.to_string() != text {
                return Err("last_commit_epoch_text is not canonical".into());
            }
            if numeric.is_some_and(|numeric| numeric != epoch) {
                return Err("last_commit_epoch and last_commit_epoch_text disagree".into());
            }
            Ok(Some(epoch))
        }
        None => Ok(numeric),
    }
}

fn validate_remote_query_status(
    mut status: RemoteQueryStatus,
    expected_query_id: mongreldb_query::QueryId,
) -> Result<RemoteQueryStatus, String> {
    const STATES: &[&str] = &[
        "queued",
        "planning",
        "executing",
        "streaming",
        "serializing",
        "commit_critical",
        "cancelling",
        "completed",
        "failed",
        "cancelled",
        "pre_cancelled",
        "finished",
    ];
    const STATUSES: &[&str] = &[
        "running",
        "outcome_unknown",
        "completed",
        "failed_before_commit",
        "cancelled_before_commit",
        "deadline_before_commit",
        "cancelled_before_start",
        "committed",
        "committed_with_error",
        "partially_committed",
        "cancelled_after_commit",
        "deadline_after_commit",
        "finished",
    ];
    if status.query_id != expected_query_id.to_string() {
        return Err("query status query_id does not match the request".into());
    }
    if status
        .detail
        .as_deref()
        .is_some_and(|detail| detail != "compact")
    {
        return Err("query status detail is invalid".into());
    }
    if !STATUSES.contains(&status.status.as_str())
        || status.state.is_empty()
        || !STATES.contains(&status.state.as_str())
        || (!status.server_state.is_empty()
            && (!STATES.contains(&status.server_state.as_str())
                || status.server_state != status.state))
        || status
            .terminal_state
            .as_ref()
            .is_some_and(|terminal| terminal != &status.status)
    {
        return Err("query status state or status is invalid".into());
    }
    let state_matches_status = match status.status.as_str() {
        "running" => matches!(
            status.state.as_str(),
            "queued"
                | "planning"
                | "executing"
                | "streaming"
                | "serializing"
                | "commit_critical"
                | "cancelling"
        ),
        "committed" => !matches!(
            status.state.as_str(),
            "failed" | "cancelled" | "pre_cancelled" | "finished"
        ),
        "completed" => status.state == "completed",
        "failed_before_commit"
        | "committed_with_error"
        | "partially_committed"
        | "outcome_unknown" => status.state == "failed",
        "cancelled_before_commit"
        | "deadline_before_commit"
        | "cancelled_after_commit"
        | "deadline_after_commit" => status.state == "cancelled",
        "cancelled_before_start" => status.state == "pre_cancelled",
        "finished" => status.state == "finished",
        _ => false,
    };
    if !state_matches_status {
        return Err("query status state and status disagree".into());
    }
    let top_epoch = exact_epoch(
        status.last_commit_epoch_text.as_deref(),
        status.last_commit_epoch,
    )?;
    let outcome_epoch = exact_epoch(
        status.outcome.last_commit_epoch_text.as_deref(),
        status.outcome.last_commit_epoch,
    )?;
    if status
        .last_commit_epoch
        .is_some_and(|numeric| Some(numeric) != top_epoch)
        || status
            .outcome
            .last_commit_epoch
            .is_some_and(|numeric| Some(numeric) != outcome_epoch)
        || top_epoch != outcome_epoch
        || status.committed != status.outcome.committed
        || status.committed_statements != status.outcome.committed_statements
        || status.first_commit_statement_index != status.outcome.first_commit_statement_index
        || status.last_commit_statement_index != status.outcome.last_commit_statement_index
        || status.completed_statements != status.outcome.completed_statements
        || status.statement_index != status.outcome.statement_index
    {
        return Err("query status top-level and outcome fields disagree".into());
    }
    match status.committed {
        Some(true) => {
            if status.committed_statements == Some(0)
                || status.committed_statements.is_none()
                || top_epoch.is_none()
                || status.last_commit_epoch_text.is_none()
                || status.outcome.last_commit_epoch_text.is_none()
                || status.first_commit_statement_index.is_none()
                || status.last_commit_statement_index.is_none()
                || status.completed_statements.is_none()
                || status.statement_index.is_none()
                || !matches!(
                    status.status.as_str(),
                    "committed"
                        | "committed_with_error"
                        | "partially_committed"
                        | "cancelled_after_commit"
                        | "deadline_after_commit"
                )
            {
                return Err("committed query status has invalid durable metadata".into());
            }
        }
        Some(false) => {
            if status.committed_statements != Some(0)
                || top_epoch.is_some()
                || status.first_commit_statement_index.is_some()
                || status.last_commit_statement_index.is_some()
                || status.completed_statements.is_none()
                || status.statement_index.is_none()
                || matches!(
                    status.status.as_str(),
                    "committed"
                        | "committed_with_error"
                        | "partially_committed"
                        | "cancelled_after_commit"
                        | "deadline_after_commit"
                        | "outcome_unknown"
                        | "finished"
                )
            {
                return Err("non-committed query status has invalid durable metadata".into());
            }
        }
        None => {
            if status.committed_statements.is_some()
                || top_epoch.is_some()
                || status.first_commit_statement_index.is_some()
                || status.last_commit_statement_index.is_some()
                || status.completed_statements.is_some()
                || status.statement_index.is_some()
                || !matches!(status.status.as_str(), "outcome_unknown" | "finished")
            {
                return Err("unknown query status contains durable metadata".into());
            }
        }
    }
    if let (Some(first), Some(last), Some(committed)) = (
        status.first_commit_statement_index,
        status.last_commit_statement_index,
        status.committed_statements,
    ) {
        if first > last
            || committed > last.saturating_sub(first).saturating_add(1)
            || status
                .statement_index
                .is_some_and(|statement| last > statement)
        {
            return Err("query status commit statement indexes are invalid".into());
        }
    }
    if let (Some(completed), Some(statement)) =
        (status.completed_statements, status.statement_index)
    {
        if statement > completed || completed > statement.saturating_add(1) {
            return Err("query status statement index and completed count disagree".into());
        }
    }
    let terminal_error = status.terminal_error.as_ref();
    if terminal_error.is_some_and(|error| {
        error.code.as_str().trim().is_empty()
            || !matches!(
                error.category.as_str(),
                "cancellation" | "deadline" | "result_limit" | "serialization" | "execution"
            )
    }) {
        return Err("query status terminal error fields are invalid".into());
    }
    let terminal_error_matches = match status.status.as_str() {
        "running" | "completed" | "committed" | "finished" => terminal_error.is_none(),
        "outcome_unknown" => terminal_error.is_some_and(|error| {
            error.code.as_str() == "QUERY_OUTCOME_UNKNOWN" && error.category == "execution"
        }),
        "cancelled_before_start" | "cancelled_before_commit" => {
            terminal_error.is_some_and(|error| {
                error.code.as_str() == "QUERY_CANCELLED" && error.category == "cancellation"
            })
        }
        "cancelled_after_commit" => terminal_error.is_some_and(|error| {
            error.code.as_str() == "QUERY_CANCELLED_AFTER_COMMIT"
                && error.category == "cancellation"
        }),
        "deadline_before_commit" => terminal_error.is_some_and(|error| {
            error.code.as_str() == "DEADLINE_EXCEEDED" && error.category == "deadline"
        }),
        "deadline_after_commit" => terminal_error.is_some_and(|error| {
            error.code.as_str() == "DEADLINE_AFTER_COMMIT" && error.category == "deadline"
        }),
        "failed_before_commit" | "committed_with_error" | "partially_committed" => {
            terminal_error.is_some()
        }
        _ => false,
    };
    if !terminal_error_matches {
        return Err("query status terminal error disagrees with status".into());
    }
    if terminal_error.is_some_and(|error| {
        (error.category == "cancellation")
            != matches!(
                error.code.as_str(),
                "QUERY_CANCELLED" | "QUERY_CANCELLED_AFTER_COMMIT"
            )
            || (error.category == "deadline")
                != matches!(
                    error.code.as_str(),
                    "DEADLINE_EXCEEDED" | "DEADLINE_AFTER_COMMIT"
                )
    }) {
        return Err("query status terminal error code and category disagree".into());
    }
    let retryable = terminal_error.is_some_and(|error| {
        matches!(
            error.code.as_str(),
            "IDEMPOTENCY_STORE_FULL" | "IDEMPOTENCY_STORE_UNAVAILABLE"
        )
    });
    if status.retryable != retryable {
        return Err("query status retryable flag disagrees with terminal error".into());
    }
    let expected_cancel_outcome = match status.state.as_str() {
        "commit_critical" => Some(RemoteCancelOutcome::TooLate),
        "cancelling" => Some(RemoteCancelOutcome::Accepted),
        "completed" | "failed" | "cancelled" | "finished" => {
            Some(RemoteCancelOutcome::AlreadyFinished)
        }
        "pre_cancelled" => Some(RemoteCancelOutcome::PreCancelled),
        _ => None,
    };
    if status.cancel_outcome != expected_cancel_outcome {
        return Err("query status cancel_outcome disagrees with state".into());
    }
    const CANCELLATION_REASONS: &[&str] = &[
        "none",
        "client_request",
        "client_disconnected",
        "session_closed",
        "server_shutdown",
        "deadline",
    ];
    let valid_reason = match status.status.as_str() {
        "finished" => status.cancellation_reason == "none",
        "cancelled_before_start" | "cancelled_before_commit" | "cancelled_after_commit" => {
            CANCELLATION_REASONS.contains(&status.cancellation_reason.as_str())
                && !matches!(status.cancellation_reason.as_str(), "none" | "deadline")
        }
        "deadline_before_commit" | "deadline_after_commit" => {
            status.cancellation_reason == "deadline"
        }
        "running" | "committed" if status.state == "cancelling" => {
            CANCELLATION_REASONS.contains(&status.cancellation_reason.as_str())
                && status.cancellation_reason != "none"
        }
        _ => status.cancellation_reason == "none",
    };
    if !valid_reason {
        return Err("query status cancellation_reason disagrees with status".into());
    }
    let serialization = status.outcome.serialization.as_str();
    let valid_serialization = match status.status.as_str() {
        "finished" | "outcome_unknown" => serialization == "unknown",
        "completed" => serialization == "succeeded",
        "running" | "committed" => match status.state.as_str() {
            "serializing" => serialization == "in_progress",
            "cancelling" => matches!(serialization, "not_started" | "in_progress"),
            "completed" => serialization == "succeeded",
            _ => serialization == "not_started",
        },
        _ => matches!(serialization, "not_started" | "failed"),
    };
    if !valid_serialization {
        return Err("query status serialization state is invalid".into());
    }
    let terminal_state_valid = match status.status.as_str() {
        "running" | "finished" => status.terminal_state.is_none(),
        "committed" if status.state != "completed" => status.terminal_state.is_none(),
        _ => status.terminal_state.as_deref() == Some(status.status.as_str()),
    };
    if !terminal_state_valid {
        return Err("query status terminal_state is invalid".into());
    }
    if matches!(status.state.as_str(), "pre_cancelled" | "finished") {
        if !status.operation.is_empty() {
            return Err("synthetic query status unexpectedly names an operation".into());
        }
    } else if status.operation.is_empty() {
        return Err("live query status lacks operation".into());
    }
    status.last_commit_epoch = top_epoch;
    status.outcome.last_commit_epoch = outcome_epoch;
    Ok(status)
}

fn validate_remote_query_error(
    mut response: RemoteQueryErrorResponse,
    expected_query_id: mongreldb_query::QueryId,
) -> Result<RemoteQueryErrorResponse, String> {
    let expected = expected_query_id.to_string();
    if response.query_id.as_deref() != Some(&expected)
        || response.error.query_id.as_deref() != Some(&expected)
        || response.terminal_state.as_deref() != Some(response.status.as_str())
        || response.error.message.is_empty()
    {
        return Err("query error query_id does not match the request".into());
    }
    if response.committed != response.outcome.committed
        || response.committed != response.error.committed
        || response.committed_statements != response.outcome.committed_statements
        || response.first_commit_statement_index != response.outcome.first_commit_statement_index
        || response.last_commit_statement_index != response.outcome.last_commit_statement_index
        || response.completed_statements != response.outcome.completed_statements
        || response.statement_index != response.outcome.statement_index
        || response.retryable != response.error.retryable
    {
        return Err("query error top-level, outcome, and error fields disagree".into());
    }
    let top_epoch = exact_epoch(
        response.last_commit_epoch_text.as_deref(),
        response.last_commit_epoch,
    )?;
    let outcome_epoch = exact_epoch(
        response.outcome.last_commit_epoch_text.as_deref(),
        response.outcome.last_commit_epoch,
    )?;
    if response
        .last_commit_epoch
        .is_some_and(|numeric| Some(numeric) != top_epoch)
        || response
            .outcome
            .last_commit_epoch
            .is_some_and(|numeric| Some(numeric) != outcome_epoch)
        || top_epoch != outcome_epoch
    {
        return Err("query error top-level and outcome commit epochs disagree".into());
    }
    let outcome_unknown = response.error.code == RemoteQueryErrorCode::QueryOutcomeUnknown;
    match response.committed {
        Some(true) => {
            if outcome_unknown
                || response.committed_statements == Some(0)
                || response.committed_statements.is_none()
                || top_epoch.is_none()
                || response.last_commit_epoch_text.is_none()
                || response.outcome.last_commit_epoch_text.is_none()
                || response.first_commit_statement_index.is_none()
                || response.last_commit_statement_index.is_none()
                || response.completed_statements.is_none()
                || response.statement_index.is_none()
                || !matches!(
                    response.status.as_str(),
                    "committed"
                        | "committed_with_error"
                        | "partially_committed"
                        | "cancelled_after_commit"
                        | "deadline_after_commit"
                )
            {
                return Err("committed query error has invalid durable metadata".into());
            }
        }
        Some(false) => {
            if outcome_unknown
                || response.committed_statements != Some(0)
                || top_epoch.is_some()
                || response.first_commit_statement_index.is_some()
                || response.last_commit_statement_index.is_some()
                || response.completed_statements.is_none()
                || response.statement_index.is_none()
                || !matches!(
                    response.status.as_str(),
                    "failed_before_commit"
                        | "cancelled_before_commit"
                        | "deadline_before_commit"
                        | "cancelled_before_start"
                )
            {
                return Err("non-committed query error has invalid durable metadata".into());
            }
        }
        None => {
            if !outcome_unknown
                || response.status != "outcome_unknown"
                || response.committed_statements.is_some()
                || top_epoch.is_some()
                || response.first_commit_statement_index.is_some()
                || response.last_commit_statement_index.is_some()
                || response.completed_statements.is_some()
                || response.statement_index.is_some()
                || response.retryable
            {
                return Err("unknown query error contains contradictory metadata".into());
            }
        }
    }
    if outcome_unknown && response.status != "outcome_unknown" {
        return Err("outcome-unknown error has the wrong status".into());
    }
    if response.retryable
        && (response.committed != Some(false)
            || !matches!(
                response.error.code.as_str(),
                "QUERY_REGISTRY_FULL" | "IDEMPOTENCY_STORE_FULL" | "IDEMPOTENCY_STORE_UNAVAILABLE"
            ))
    {
        return Err("query error retryable flag is unsafe".into());
    }
    if let (Some(first), Some(last), Some(committed)) = (
        response.first_commit_statement_index,
        response.last_commit_statement_index,
        response.committed_statements,
    ) {
        if first > last
            || committed > last.saturating_sub(first).saturating_add(1)
            || response
                .statement_index
                .is_some_and(|statement| last > statement)
        {
            return Err("query error commit statement indexes are invalid".into());
        }
    }
    if let (Some(completed), Some(statement)) =
        (response.completed_statements, response.statement_index)
    {
        if statement > completed || completed > statement.saturating_add(1) {
            return Err("query error statement index and completed count disagree".into());
        }
    }
    let code_matches = match response.error.code {
        RemoteQueryErrorCode::QueryOutcomeUnknown => response.status == "outcome_unknown",
        RemoteQueryErrorCode::QueryCancelledAfterCommit => {
            response.status == "cancelled_after_commit" && response.committed == Some(true)
        }
        RemoteQueryErrorCode::DeadlineAfterCommit => {
            response.status == "deadline_after_commit" && response.committed == Some(true)
        }
        RemoteQueryErrorCode::QueryCancelled => matches!(
            response.status.as_str(),
            "cancelled_before_commit" | "cancelled_before_start"
        ),
        RemoteQueryErrorCode::DeadlineExceeded => response.status == "deadline_before_commit",
        RemoteQueryErrorCode::CommitOutcome
        | RemoteQueryErrorCode::SerializationFailedAfterCommit => response.committed == Some(true),
        RemoteQueryErrorCode::SerializationFailed => response.committed == Some(false),
        _ => true,
    };
    if !code_matches {
        return Err("query error code and status disagree".into());
    }
    let status_matches_code = match response.status.as_str() {
        "outcome_unknown" => response.error.code == RemoteQueryErrorCode::QueryOutcomeUnknown,
        "cancelled_after_commit" => {
            response.error.code == RemoteQueryErrorCode::QueryCancelledAfterCommit
        }
        "deadline_after_commit" => response.error.code == RemoteQueryErrorCode::DeadlineAfterCommit,
        "cancelled_before_commit" | "cancelled_before_start" => {
            response.error.code == RemoteQueryErrorCode::QueryCancelled
        }
        "deadline_before_commit" => response.error.code == RemoteQueryErrorCode::DeadlineExceeded,
        _ => true,
    };
    if !status_matches_code {
        return Err("query error status and code disagree".into());
    }
    const CANCELLATION_REASONS: &[&str] = &[
        "none",
        "client_request",
        "client_disconnected",
        "session_closed",
        "server_shutdown",
        "deadline",
    ];
    let cancellation_error = matches!(
        response.error.code,
        RemoteQueryErrorCode::QueryCancelled | RemoteQueryErrorCode::QueryCancelledAfterCommit
    );
    let deadline_error = matches!(
        response.error.code,
        RemoteQueryErrorCode::DeadlineExceeded | RemoteQueryErrorCode::DeadlineAfterCommit
    );
    let expected_cancel_outcome = if cancellation_error || deadline_error {
        Some(RemoteCancelOutcome::Accepted)
    } else {
        match response.server_state.as_deref() {
            Some("commit_critical") => Some(RemoteCancelOutcome::TooLate),
            Some("cancelling") => Some(RemoteCancelOutcome::Accepted),
            Some("completed" | "failed" | "cancelled") => {
                Some(RemoteCancelOutcome::AlreadyFinished)
            }
            Some(_) | None => None,
        }
    };
    if response.cancel_outcome != expected_cancel_outcome {
        return Err("query error cancel_outcome disagrees with its phase".into());
    }
    let valid_reason = match response.cancellation_reason.as_deref() {
        Some("deadline") => deadline_error,
        Some(reason) if CANCELLATION_REASONS.contains(&reason) => {
            if cancellation_error {
                reason != "none"
            } else {
                response.server_state.is_some() && reason == "none"
            }
        }
        None => response.server_state.is_none() && !cancellation_error && !deadline_error,
        Some(_) => false,
    };
    if !valid_reason {
        return Err("query error cancellation_reason is invalid".into());
    }
    let valid_server_state = match response.server_state.as_deref() {
        None => true,
        Some("cancelled") => cancellation_error || deadline_error,
        Some("failed") => !cancellation_error && !deadline_error,
        Some(_) => false,
    };
    if !valid_server_state {
        return Err("query error server_state disagrees with its status".into());
    }
    let valid_serialization = match response.server_state.as_deref() {
        None => response.outcome.serialization == "unknown",
        Some(_) if outcome_unknown => response.outcome.serialization == "unknown",
        Some("failed" | "cancelled") => {
            matches!(
                response.outcome.serialization.as_str(),
                "not_started" | "failed"
            )
        }
        Some(_) => false,
    };
    if !valid_serialization {
        return Err("query error serialization state is invalid".into());
    }
    response.last_commit_epoch = top_epoch;
    response.outcome.last_commit_epoch = outcome_epoch;
    Ok(response)
}

fn validate_queryless_sql_error(
    mut response: RemoteQueryErrorResponse,
) -> Result<RemoteQueryErrorResponse, String> {
    if response.query_id.is_some()
        || response.error.query_id.is_some()
        || response.status != "failed_before_commit"
        || response.terminal_state.as_deref() != Some("failed_before_commit")
        || response.server_state.as_deref() != Some("failed")
        || response.committed != Some(false)
        || response.error.committed != Some(false)
        || response.outcome.committed != Some(false)
        || response.committed_statements != Some(0)
        || response.outcome.committed_statements != Some(0)
        || response.last_commit_epoch.is_some()
        || response.last_commit_epoch_text.is_some()
        || response.outcome.last_commit_epoch.is_some()
        || response.outcome.last_commit_epoch_text.is_some()
        || response.first_commit_statement_index.is_some()
        || response.last_commit_statement_index.is_some()
        || response.outcome.first_commit_statement_index.is_some()
        || response.outcome.last_commit_statement_index.is_some()
        || response.completed_statements != Some(0)
        || response.outcome.completed_statements != Some(0)
        || response.statement_index != Some(0)
        || response.outcome.statement_index != Some(0)
        || response.cancel_outcome.is_some()
        || response.cancellation_reason.is_some()
        || response.retryable
        || response.error.retryable
        || response.outcome.serialization != "not_started"
        || response.error.message.is_empty()
        || !matches!(
            response.error.code,
            RemoteQueryErrorCode::InvalidSqlCursor
                | RemoteQueryErrorCode::SqlCursorExpired
                | RemoteQueryErrorCode::SqlCursorNotFound
                | RemoteQueryErrorCode::ResultLimitExceeded
                | RemoteQueryErrorCode::SerializationFailed
                | RemoteQueryErrorCode::SerializationWorkerFailed
                | RemoteQueryErrorCode::SqlAdmissionClosed
                | RemoteQueryErrorCode::EntropyUnavailable
        )
    {
        return Err("queryless SQL error metadata is invalid".into());
    }
    response.last_commit_epoch = None;
    response.outcome.last_commit_epoch = None;
    Ok(response)
}

fn validate_remote_sql_receipt(
    mut receipt: RemoteSqlReceipt,
    expected_query_id: mongreldb_query::QueryId,
    expected_original_query_id: Option<mongreldb_query::QueryId>,
) -> Result<RemoteSqlReceipt, String> {
    if receipt.query_id != expected_query_id.to_string()
        || receipt.terminal_state.as_deref() != Some(receipt.status.as_str())
    {
        return Err("receipt query_id does not match the request".into());
    }
    if receipt
        .original_query_id
        .parse::<mongreldb_query::QueryId>()
        .is_err()
    {
        return Err("receipt original_query_id is invalid".into());
    }
    let status_committed = match receipt.status.as_str() {
        "completed" => false,
        "committed"
        | "committed_with_error"
        | "partially_committed"
        | "cancelled_after_commit"
        | "deadline_after_commit" => true,
        _ => return Err("receipt status is invalid".into()),
    };
    let expected_server_state = match receipt.status.as_str() {
        "completed" | "committed" => "completed",
        "committed_with_error" | "partially_committed" => "failed",
        "cancelled_after_commit" | "deadline_after_commit" => "cancelled",
        _ => return Err("receipt status is invalid".into()),
    };
    if receipt.server_state != expected_server_state
        || receipt.cancel_outcome != Some(RemoteCancelOutcome::AlreadyFinished)
    {
        return Err("receipt terminal state metadata is invalid".into());
    }
    let terminal_error = receipt.terminal_error.as_ref();
    let terminal_error_matches = match receipt.status.as_str() {
        "completed" | "committed" => terminal_error.is_none(),
        "cancelled_after_commit" => terminal_error.is_some_and(|error| {
            error.code.as_str() == "QUERY_CANCELLED_AFTER_COMMIT"
                && error.category == "cancellation"
        }),
        "deadline_after_commit" => terminal_error.is_some_and(|error| {
            error.code.as_str() == "DEADLINE_AFTER_COMMIT" && error.category == "deadline"
        }),
        "committed_with_error" | "partially_committed" => terminal_error.is_some(),
        _ => false,
    };
    if !terminal_error_matches
        || terminal_error.is_some_and(|error| {
            !matches!(
                error.category.as_str(),
                "cancellation" | "deadline" | "result_limit" | "serialization" | "execution"
            ) || (error.category == "cancellation")
                != (error.code.as_str() == "QUERY_CANCELLED_AFTER_COMMIT")
                || (error.category == "deadline")
                    != (error.code.as_str() == "DEADLINE_AFTER_COMMIT")
                || (error.category == "serialization")
                    != (error.code.as_str() == "SERIALIZATION_FAILED_AFTER_COMMIT")
        })
    {
        return Err("receipt terminal error disagrees with status".into());
    }
    let valid_reason = match receipt.status.as_str() {
        "cancelled_after_commit" => matches!(
            receipt.cancellation_reason.as_str(),
            "client_request" | "client_disconnected" | "session_closed" | "server_shutdown"
        ),
        "deadline_after_commit" => receipt.cancellation_reason == "deadline",
        _ => receipt.cancellation_reason == "none",
    };
    let serialization = receipt.outcome.serialization.as_str();
    let valid_serialization = matches!(serialization, "not_started" | "succeeded" | "failed")
        && match receipt.status.as_str() {
            "completed" | "committed" => serialization == "succeeded",
            _ if terminal_error.is_some_and(|error| error.category == "serialization") => {
                serialization == "failed"
            }
            _ => serialization != "succeeded",
        };
    if !valid_reason || !valid_serialization {
        return Err("receipt cancellation or serialization metadata is invalid".into());
    }
    if receipt.committed != status_committed
        || receipt.outcome.committed != Some(receipt.committed)
        || receipt.outcome.committed_statements != Some(receipt.committed_statements)
        || receipt.outcome.first_commit_statement_index != receipt.first_commit_statement_index
        || receipt.outcome.last_commit_statement_index != receipt.last_commit_statement_index
        || receipt.outcome.completed_statements != Some(receipt.completed_statements)
        || receipt.outcome.statement_index != Some(receipt.statement_index)
    {
        return Err("receipt top-level and outcome fields disagree".into());
    }
    let top_epoch = exact_epoch(
        receipt.last_commit_epoch_text.as_deref(),
        receipt.last_commit_epoch,
    )?;
    let outcome_epoch = exact_epoch(
        receipt.outcome.last_commit_epoch_text.as_deref(),
        receipt.outcome.last_commit_epoch,
    )?;
    if receipt
        .last_commit_epoch
        .is_some_and(|numeric| Some(numeric) != top_epoch)
        || receipt
            .outcome
            .last_commit_epoch
            .is_some_and(|numeric| Some(numeric) != outcome_epoch)
        || top_epoch != outcome_epoch
    {
        return Err("receipt top-level and outcome commit epochs disagree".into());
    }
    if receipt.committed {
        if receipt.committed_statements == 0
            || top_epoch.is_none()
            || receipt.last_commit_epoch_text.is_none()
            || receipt.outcome.last_commit_epoch_text.is_none()
            || receipt.first_commit_statement_index.is_none()
            || receipt.last_commit_statement_index.is_none()
        {
            return Err("committed receipt has no durable commit metadata".into());
        }
    } else if receipt.committed_statements != 0
        || top_epoch.is_some()
        || receipt.first_commit_statement_index.is_some()
        || receipt.last_commit_statement_index.is_some()
    {
        return Err("non-committed receipt contains commit metadata".into());
    }
    if let (Some(first), Some(last)) = (
        receipt.first_commit_statement_index,
        receipt.last_commit_statement_index,
    ) {
        if first > last
            || receipt.committed_statements > last.saturating_sub(first).saturating_add(1)
            || last > receipt.statement_index
        {
            return Err("receipt commit statement indexes are invalid".into());
        }
    }
    if receipt.statement_index > receipt.completed_statements
        || receipt.completed_statements > receipt.statement_index.saturating_add(1)
    {
        return Err("receipt statement index and completed count disagree".into());
    }
    let idempotency_identity_valid = match expected_original_query_id {
        Some(original) if receipt.idempotency_replayed => {
            receipt.original_query_id == original.to_string()
        }
        Some(_) => receipt.original_query_id == expected_query_id.to_string(),
        None if receipt.idempotency_replayed => true,
        None => receipt.original_query_id == expected_query_id.to_string(),
    };
    if !receipt.idempotency_persisted
        || receipt.idempotency_expires_at_ms == 0
        || receipt.retryable
        || !idempotency_identity_valid
    {
        return Err("receipt idempotency metadata is invalid".into());
    }
    receipt.last_commit_epoch = top_epoch;
    receipt.outcome.last_commit_epoch = outcome_epoch;
    Ok(receipt)
}

#[derive(Clone)]
struct SqlReceiptCommitProof {
    epoch: u64,
    epoch_text: String,
    committed_statements: usize,
}

fn sql_receipt_commit_proof(
    value: &serde_json::Value,
    expected_query_id: mongreldb_query::QueryId,
    expected_original_query_id: Option<mongreldb_query::QueryId>,
) -> Option<SqlReceiptCommitProof> {
    let object = value.as_object()?;
    let status = object.get("status")?.as_str()?;
    if !matches!(
        status,
        "committed"
            | "committed_with_error"
            | "partially_committed"
            | "cancelled_after_commit"
            | "deadline_after_commit"
    ) || object.get("query_id")?.as_str()? != expected_query_id.to_string()
        || !object.get("committed")?.as_bool()?
        || object.get("retryable")?.as_bool()?
        || !object.get("idempotency_persisted")?.as_bool()?
    {
        return None;
    }
    let original = object.get("original_query_id")?.as_str()?;
    let replayed = object.get("idempotency_replayed")?.as_bool()?;
    let identity_valid = match expected_original_query_id {
        Some(expected) if replayed => original == expected.to_string(),
        Some(_) => original == expected_query_id.to_string(),
        None if replayed => original.parse::<mongreldb_query::QueryId>().is_ok(),
        None => original == expected_query_id.to_string(),
    };
    if !identity_valid {
        return None;
    }
    let committed_statements =
        usize::try_from(object.get("committed_statements")?.as_u64()?).ok()?;
    if committed_statements == 0 {
        return None;
    }
    let epoch_text = object.get("last_commit_epoch_text")?.as_str()?.to_owned();
    let numeric_epoch = object.get("last_commit_epoch")?.as_u64();
    let epoch = exact_epoch(Some(&epoch_text), numeric_epoch).ok()??;
    if numeric_epoch.is_some_and(|numeric| numeric != epoch) {
        return None;
    }
    let outcome = object.get("outcome")?.as_object()?;
    let outcome_numeric_epoch = outcome
        .get("last_commit_epoch")
        .and_then(serde_json::Value::as_u64);
    let outcome_epoch = exact_epoch(
        outcome
            .get("last_commit_epoch_text")
            .and_then(serde_json::Value::as_str),
        outcome_numeric_epoch,
    )
    .ok()??;
    if !outcome.get("committed")?.as_bool()?
        || usize::try_from(outcome.get("committed_statements")?.as_u64()?).ok()?
            != committed_statements
        || outcome.get("last_commit_epoch_text")?.as_str()? != epoch_text
        || outcome_epoch != epoch
        || outcome_numeric_epoch.is_some_and(|numeric| numeric != outcome_epoch)
    {
        return None;
    }
    Some(SqlReceiptCommitProof {
        epoch,
        epoch_text,
        committed_statements,
    })
}

fn committed_sql_receipt_decode_error(
    query_id: mongreldb_query::QueryId,
    proof: SqlReceiptCommitProof,
    message: impl Into<String>,
) -> ClientError {
    let message = message.into();
    let query_id = query_id.to_string();
    let code = RemoteQueryErrorCode::CommitOutcome;
    let response = RemoteQueryErrorResponse {
        query_id: Some(query_id.clone()),
        status: "committed_with_error".into(),
        terminal_state: Some("committed_with_error".into()),
        committed: Some(true),
        committed_statements: Some(proof.committed_statements),
        last_commit_epoch: Some(proof.epoch),
        last_commit_epoch_text: Some(proof.epoch_text.clone()),
        first_commit_statement_index: None,
        last_commit_statement_index: None,
        completed_statements: None,
        statement_index: None,
        cancel_outcome: Some(RemoteCancelOutcome::AlreadyFinished),
        cancellation_reason: Some("none".into()),
        retryable: false,
        server_state: Some("failed".into()),
        outcome: RemoteQueryOutcome {
            committed: Some(true),
            committed_statements: Some(proof.committed_statements),
            last_commit_epoch: Some(proof.epoch),
            last_commit_epoch_text: Some(proof.epoch_text),
            serialization: "unknown".into(),
            ..RemoteQueryOutcome::default()
        },
        error: RemoteQueryErrorBody {
            code: code.clone(),
            message: message.clone(),
            query_id: Some(query_id),
            committed: Some(true),
            retryable: false,
        },
    };
    ClientError::Query {
        status: 0,
        code,
        message,
        response: Box::new(response),
    }
}

fn sql_error_commit_proof(
    value: &serde_json::Value,
    expected_query_id: mongreldb_query::QueryId,
) -> Option<SqlReceiptCommitProof> {
    let object = value.as_object()?;
    let expected = expected_query_id.to_string();
    if object.get("query_id")?.as_str()? != expected
        || !object.get("committed")?.as_bool()?
        || object.get("retryable")?.as_bool()?
        || !matches!(
            object.get("status")?.as_str()?,
            "committed"
                | "committed_with_error"
                | "partially_committed"
                | "cancelled_after_commit"
                | "deadline_after_commit"
        )
    {
        return None;
    }
    let committed_statements =
        usize::try_from(object.get("committed_statements")?.as_u64()?).ok()?;
    if committed_statements == 0 {
        return None;
    }
    let epoch_text = object.get("last_commit_epoch_text")?.as_str()?.to_owned();
    let numeric_epoch = object.get("last_commit_epoch")?.as_u64()?;
    let epoch = exact_epoch(Some(&epoch_text), Some(numeric_epoch)).ok()??;
    if numeric_epoch != epoch {
        return None;
    }
    let outcome = object.get("outcome")?.as_object()?;
    let outcome_epoch_text = outcome.get("last_commit_epoch_text")?.as_str()?;
    let outcome_numeric_epoch = outcome.get("last_commit_epoch")?.as_u64()?;
    let outcome_epoch =
        exact_epoch(Some(outcome_epoch_text), Some(outcome_numeric_epoch)).ok()??;
    if !outcome.get("committed")?.as_bool()?
        || usize::try_from(outcome.get("committed_statements")?.as_u64()?).ok()?
            != committed_statements
        || outcome_epoch_text != epoch_text
        || outcome_epoch != epoch
        || outcome_numeric_epoch != outcome_epoch
    {
        return None;
    }
    let error = object.get("error")?.as_object()?;
    if error.get("query_id")?.as_str()? != expected
        || !error.get("committed")?.as_bool()?
        || error.get("retryable")?.as_bool()?
    {
        return None;
    }
    Some(SqlReceiptCommitProof {
        epoch,
        epoch_text,
        committed_statements,
    })
}

enum SqlReceiptDecodeError {
    KnownCommit(ClientError),
    Unknown(String),
}

fn decode_remote_sql_receipt(
    bytes: &[u8],
    query_id: mongreldb_query::QueryId,
    expected_original_query_id: Option<mongreldb_query::QueryId>,
) -> Result<RemoteSqlReceipt, SqlReceiptDecodeError> {
    let value = strict_json_value(bytes)
        .map_err(|error| SqlReceiptDecodeError::Unknown(format!("invalid SQL receipt: {error}")))?;
    let proof = sql_receipt_commit_proof(&value, query_id, expected_original_query_id);
    let receipt = serde_json::from_value::<RemoteSqlReceipt>(value).map_err(|error| {
        proof.clone().map_or_else(
            || SqlReceiptDecodeError::Unknown(format!("invalid SQL receipt: {error}")),
            |proof| {
                SqlReceiptDecodeError::KnownCommit(committed_sql_receipt_decode_error(
                    query_id,
                    proof,
                    format!("SQL committed but its receipt was invalid: {error}"),
                ))
            },
        )
    })?;
    validate_remote_sql_receipt(receipt, query_id, expected_original_query_id).map_err(|error| {
        proof.map_or_else(
            || SqlReceiptDecodeError::Unknown(error.clone()),
            |proof| {
                SqlReceiptDecodeError::KnownCommit(committed_sql_receipt_decode_error(
                    query_id,
                    proof,
                    format!("SQL committed but its receipt was invalid: {error}"),
                ))
            },
        )
    })
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RemoteSqlPageLimits {
    pub rows: usize,
    pub bytes: usize,
    pub tokens: usize,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RemoteSqlPageMetadata {
    pub offset: usize,
    pub row_count: usize,
    pub total_rows: usize,
    pub byte_count: usize,
    pub estimated_tokens: usize,
    pub limits: RemoteSqlPageLimits,
    pub projection: Vec<String>,
    pub expires_at_ms: u64,
    pub snapshot: String,
    pub token_estimate: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RemoteSqlPage {
    pub status: String,
    pub rows: Vec<serde_json::Value>,
    pub next_cursor: Option<String>,
    pub page: RemoteSqlPageMetadata,
}

pub struct RemoteSqlQueryHandle {
    query_id: mongreldb_query::QueryId,
    client: MongrelClient,
    result: std::thread::JoinHandle<ClientResult<Vec<RecordBatch>>>,
}

impl RemoteSqlQueryHandle {
    pub fn id(&self) -> mongreldb_query::QueryId {
        self.query_id
    }

    pub fn cancel(&self) -> ClientResult<RemoteCancelOutcome> {
        self.client.cancel_sql(self.query_id)
    }

    pub fn status(&self) -> ClientResult<RemoteQueryStatus> {
        self.client.query_status(self.query_id)
    }

    pub fn wait(self) -> ClientResult<Vec<RecordBatch>> {
        match self.result.join() {
            Ok(result) => result,
            Err(_) => Err(self
                .client
                .recover_after_transport_loss(self.query_id, "SQL worker panicked".into())),
        }
    }
}

#[derive(Serialize)]
struct SqlReq {
    sql: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    format: Option<&'static str>,
    query_id: mongreldb_query::QueryId,
    #[serde(skip_serializing_if = "Option::is_none")]
    timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_output_rows: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_output_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    idempotency_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pagination: Option<SqlPaginationReq>,
}

#[derive(Debug, Clone, Serialize)]
struct SqlPaginationReq {
    page_size_rows: u64,
    projection: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_page_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_page_tokens: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CountResp {
    count: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TableIdResponse {
    table_id: u64,
    table_id_text: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RowIdResponse {
    row_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EpochResponse {
    epoch: u64,
    epoch_text: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CommittedWriteResponse {
    status: String,
    epoch: u64,
    epoch_text: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TriggerDropResponse {
    status: String,
    epoch: u64,
    epoch_text: String,
    dropped_trigger: mongreldb_core::StoredTrigger,
    resource_tables: Vec<TriggerTableBindingResponse>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TriggerTableBindingResponse {
    name: String,
    table_id: u64,
    schema_id: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryRetention {
    pub history_retention_epochs: u64,
    pub earliest_retained_epoch: u64,
}

/// Server-side schema metadata for one table (subset of the server's descriptor).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableSchemaInfo {
    pub schema_id: u64,
    pub columns: Vec<ColumnMeta>,
    #[serde(default)]
    pub indexes: Vec<IndexMeta>,
    pub constraints: ConstraintMeta,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexMeta {
    pub name: String,
    pub column_id: u16,
    pub kind: String,
    pub predicate: Option<String>,
    #[serde(default)]
    pub options: mongreldb_core::schema::IndexOptions,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnMeta {
    pub id: u16,
    pub name: String,
    pub ty: String,
    pub primary_key: bool,
    pub nullable: bool,
    pub auto_increment: bool,
    #[serde(default)]
    pub embedding_source: Option<mongreldb_core::EmbeddingSource>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
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
#[serde(deny_unknown_fields)]
pub struct KitTxnResponse {
    pub status: String,
    pub epoch: u64,
    #[serde(default)]
    pub epoch_text: Option<String>,
    pub results: Vec<KitOpResult>,
}

fn validate_kit_txn_response(
    response: KitTxnResponse,
    request: &KitTxnRequest,
) -> ClientResult<KitTxnResponse> {
    if response.status != "committed" {
        return Err(ClientError::Decode(
            "Kit transaction success status is not committed".into(),
        ));
    }
    if response.epoch_text.is_none()
        || exact_kit_epoch(response.epoch_text.as_deref(), Some(response.epoch))
            .map_err(ClientError::Decode)?
            .is_none()
    {
        return Err(ClientError::Decode(
            "Kit transaction response has no exact epoch".into(),
        ));
    }
    validate_kit_results(&response.results, request)?;
    Ok(response)
}

fn kit_txn_outcome_unknown(message: impl Into<String>) -> ClientError {
    ClientError::Kit {
        code: KitErrorCode::QueryOutcomeUnknown,
        message: message.into(),
        op_index: None,
        status: 0,
        committed: None,
        epoch: None,
        epoch_text: None,
        retryable: Some(false),
    }
}

fn kit_txn_auth_error(status: u16) -> ClientError {
    let (code, message) = if status == 401 {
        (KitErrorCode::AuthRequired, "authentication required")
    } else {
        (KitErrorCode::PermissionDenied, "permission denied")
    };
    ClientError::Kit {
        code,
        message: message.into(),
        op_index: None,
        status,
        committed: Some(false),
        epoch: None,
        epoch_text: None,
        retryable: Some(false),
    }
}

fn committed_kit_txn_decode_error(
    status: u16,
    epoch: u64,
    epoch_text: String,
    message: impl Into<String>,
) -> ClientError {
    ClientError::Kit {
        code: KitErrorCode::CommitOutcome,
        message: message.into(),
        op_index: None,
        status,
        committed: Some(true),
        epoch: Some(epoch),
        epoch_text: Some(epoch_text),
        retryable: Some(false),
    }
}

fn decode_kit_txn_success(
    body: &[u8],
    request: &KitTxnRequest,
    status: u16,
) -> ClientResult<KitTxnResponse> {
    let value = strict_json_value(body).map_err(|error| {
        kit_txn_outcome_unknown(format!("invalid Kit transaction success response: {error}"))
    })?;
    let epoch = value.get("epoch").and_then(serde_json::Value::as_u64);
    let epoch_text = value
        .get("epoch_text")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let proven_commit = value.get("status").and_then(serde_json::Value::as_str)
        == Some("committed")
        && epoch.is_some()
        && epoch_text
            .as_deref()
            .is_some_and(|text| exact_kit_epoch(Some(text), epoch).ok().flatten() == epoch);
    let (Some(epoch), Some(epoch_text)) = (epoch, epoch_text) else {
        return Err(kit_txn_outcome_unknown(
            "Kit transaction success response does not prove a commit",
        ));
    };
    if !proven_commit {
        return Err(kit_txn_outcome_unknown(
            "Kit transaction success response does not prove a commit",
        ));
    }
    if let Err(error) = exact_json_object_fields(
        &value,
        &["status", "epoch", "epoch_text", "results"],
        &["status", "epoch", "epoch_text", "results"],
        "Kit transaction success response",
    ) {
        return Err(committed_kit_txn_decode_error(
            status,
            epoch,
            epoch_text,
            format!("transaction committed but its response was invalid: {error}"),
        ));
    }
    if !value["results"].is_array() {
        return Err(committed_kit_txn_decode_error(
            status,
            epoch,
            epoch_text,
            "transaction committed but its results were not an array",
        ));
    }
    let response = serde_json::from_value::<KitTxnResponse>(value).map_err(|error| {
        committed_kit_txn_decode_error(
            status,
            epoch,
            epoch_text.clone(),
            format!("transaction committed but its result could not be decoded: {error}"),
        )
    })?;
    validate_kit_txn_response(response, request).map_err(|error| {
        committed_kit_txn_decode_error(
            status,
            epoch,
            epoch_text,
            format!("transaction committed but its result was invalid: {error}"),
        )
    })
}

fn validate_kit_results(results: &[KitOpResult], request: &KitTxnRequest) -> ClientResult<()> {
    if results.len() != request.ops.len() {
        return Err(ClientError::Decode(
            "Kit transaction result count does not match request".into(),
        ));
    }
    for (index, (operation, result)) in request.ops.iter().zip(results).enumerate() {
        let matches = match (operation, result) {
            (KitOp::Put { returning, .. }, KitOpResult::Put { row_id, row, .. }) => {
                row_id.is_none()
                    && row.is_some() == *returning
                    && row.as_deref().is_none_or(valid_flat_kit_cells)
            }
            (KitOp::Upsert { returning, .. }, KitOpResult::Upsert { action, row, .. }) => {
                matches!(action.as_str(), "inserted" | "updated" | "unchanged")
                    && row.is_some() == *returning
                    && row.as_deref().is_none_or(valid_flat_kit_cells)
            }
            (KitOp::Delete { .. }, KitOpResult::Deleted) => true,
            (KitOp::DeleteByPk { .. }, KitOpResult::Deleted | KitOpResult::NotFound) => true,
            _ => false,
        };
        if !matches {
            return Err(ClientError::Decode(format!(
                "Kit transaction result {index} does not match request operation"
            )));
        }
    }
    Ok(())
}

fn valid_flat_kit_cells(cells: &[serde_json::Value]) -> bool {
    if !cells.len().is_multiple_of(2) {
        return false;
    }
    let mut column_ids = std::collections::HashSet::new();
    cells.chunks_exact(2).all(|pair| {
        pair[0]
            .as_u64()
            .and_then(|column_id| u16::try_from(column_id).ok())
            .is_some_and(|column_id| column_ids.insert(column_id))
    })
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KitQueryResponse {
    pub rows: Vec<KitQueryRow>,
    pub truncated: bool,
    #[serde(default)]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KitQueryRow {
    pub row_id: String,
    /// Flat `[col_id, val, col_id, val, …]` cells.
    pub cells: Vec<serde_json::Value>,
}

fn validate_kit_query_response(
    response: KitQueryResponse,
    request: &KitQueryRequest,
) -> ClientResult<KitQueryResponse> {
    if response.truncated != response.next_cursor.is_some()
        || response
            .next_cursor
            .as_ref()
            .is_some_and(|cursor| cursor.is_empty() || cursor.len() > 2_048)
    {
        return Err(ClientError::Decode(
            "Kit query continuation cursor is inconsistent".into(),
        ));
    }
    let limit = request
        .limit
        .unwrap_or(mongreldb_core::query::MAX_FINAL_LIMIT);
    if response.rows.len() > limit {
        return Err(ClientError::Decode(
            "Kit query returned more rows than requested".into(),
        ));
    }
    let projection = request.projection.as_ref().map(|columns| {
        columns
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>()
    });
    let mut row_ids = std::collections::HashSet::new();
    for row in &response.rows {
        if row
            .row_id
            .parse::<u64>()
            .ok()
            .is_none_or(|row_id| row_id.to_string() != row.row_id)
            || !row_ids.insert(row.row_id.as_str())
            || !valid_flat_kit_cells(&row.cells)
            || projection.as_ref().is_some_and(|projection| {
                row.cells.chunks_exact(2).any(|pair| {
                    pair[0]
                        .as_u64()
                        .and_then(|column_id| u16::try_from(column_id).ok())
                        .is_none_or(|column_id| !projection.contains(&column_id))
                })
            })
        {
            return Err(ClientError::Decode(
                "Kit query row identifier or cell layout is invalid".into(),
            ));
        }
    }
    Ok(response)
}

/// Cooperative execution limits accepted by every Kit AI endpoint.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct AiExecutionOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deadline_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_work: Option<usize>,
}

#[derive(Serialize)]
struct WithAiExecutionOptions<'a, T> {
    #[serde(flatten)]
    request: &'a T,
    #[serde(flatten)]
    options: &'a AiExecutionOptions,
}

#[derive(Debug, Clone, Serialize)]
pub struct KitRetrieveRequest {
    pub table: String,
    pub retriever: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KitRetrieveResponse {
    pub hits: Vec<KitRetrieverHit>,
}

#[derive(Debug, Clone, Serialize)]
pub struct KitAnnRerankRequest {
    pub table: String,
    pub column_id: u16,
    pub query: Vec<f32>,
    pub candidate_k: usize,
    pub limit: usize,
    pub metric: KitVectorMetric,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KitVectorMetric {
    Cosine,
    DotProduct,
    Euclidean,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KitAnnRerankResponse {
    pub hits: Vec<KitAnnRerankHit>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KitAnnRerankHit {
    pub row_id: String,
    pub candidate_distance: KitAnnCandidateDistance,
    pub exact_score: f32,
}

/// Tagged ANN candidate distance from `/kit/ann_rerank`.
///
/// `kind` is `"hamming"` (BinarySign) or `"cosine"` (Dense). `value` holds the
/// numeric distance; Hamming values are integral, cosine is `1 - similarity`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KitAnnCandidateDistance {
    pub kind: String,
    pub value: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KitRetrieverHit {
    pub row_id: String,
    pub rank: usize,
    pub score: KitScore,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KitScore {
    pub kind: String,
    pub value: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct KitSetSimilarityRequest {
    pub table: String,
    pub column_id: u16,
    pub members: Vec<serde_json::Value>,
    pub candidate_k: usize,
    pub min_jaccard: f32,
    pub limit: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KitSetSimilarityResponse {
    pub hits: Vec<KitSetSimilarityHit>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KitSetSimilarityHit {
    pub row_id: String,
    pub estimated_jaccard: f32,
    pub exact_jaccard: f32,
}

#[derive(Debug, Clone, Serialize)]
pub struct KitSearchRequest {
    pub table: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub must: Vec<serde_json::Value>,
    pub retrievers: Vec<serde_json::Value>,
    pub fusion: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rerank: Option<serde_json::Value>,
    pub limit: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub projection: Option<Vec<u16>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deadline_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_work: Option<usize>,
    #[serde(default)]
    pub explain: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KitSearchResponse {
    pub hits: Vec<KitSearchHit>,
    #[serde(default)]
    pub trace: Option<serde_json::Value>,
    #[serde(default)]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KitSearchHit {
    pub row_id: String,
    pub cells: Vec<serde_json::Value>,
    pub components: Vec<KitComponentScore>,
    pub fused_score: f64,
    #[serde(default)]
    pub exact_rerank_score: Option<f32>,
    #[serde(default)]
    pub final_score: Option<f64>,
    #[serde(default)]
    pub final_rank: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KitComponentScore {
    pub retriever_name: String,
    pub rank: usize,
    pub raw_score: KitScore,
    pub contribution: f64,
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
#[serde(deny_unknown_fields)]
pub struct ProcedureResponse {
    #[serde(default)]
    pub status: String,
    pub procedure: mongreldb_core::StoredProcedure,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TriggerResponse {
    #[serde(default)]
    pub status: Option<String>,
    pub trigger: mongreldb_core::StoredTrigger,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProceduresResponse {
    pub procedures: Vec<mongreldb_core::StoredProcedure>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct ProcedureCallResponse {
    pub status: String,
    pub committed: bool,
    #[serde(default)]
    pub epoch: Option<u64>,
    #[serde(default)]
    pub epoch_text: Option<String>,
    pub result: serde_json::Value,
}

fn validate_procedure_call_response(
    response: ProcedureCallResponse,
) -> ClientResult<ProcedureCallResponse> {
    if response.status != "ok"
        || response.committed != response.epoch.is_some()
        || response.committed != response.epoch_text.is_some()
        || exact_kit_epoch(response.epoch_text.as_deref(), response.epoch)
            .map_err(ClientError::Decode)?
            != response.epoch
    {
        return Err(ClientError::Decode(
            "procedure call response has invalid durable metadata".into(),
        ));
    }
    Ok(response)
}

fn procedure_definition_matches(
    actual: &mongreldb_core::StoredProcedure,
    requested: &mongreldb_core::StoredProcedure,
) -> bool {
    actual.mode == requested.mode
        && actual.params == requested.params
        && actual.body == requested.body
}

fn trigger_definition_matches(
    actual: &mongreldb_core::StoredTrigger,
    requested: &mongreldb_core::StoredTrigger,
) -> bool {
    actual.target == requested.target
        && actual.timing == requested.timing
        && actual.event == requested.event
        && actual.update_of == requested.update_of
        && actual.target_columns == requested.target_columns
        && actual.when == requested.when
        && actual.program == requested.program
        && actual.enabled
}

// Internal mirror of the server's error envelope.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct KitErrorEnvelope {
    #[allow(dead_code)]
    status: String,
    #[serde(default)]
    committed: Option<bool>,
    #[serde(default)]
    epoch: Option<u64>,
    #[serde(default)]
    epoch_text: Option<String>,
    #[serde(default)]
    retryable: Option<bool>,
    #[serde(default)]
    results: Option<Vec<KitOpResult>>,
    error: KitErrorBody,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct KitErrorBody {
    code: String,
    message: String,
    #[serde(default)]
    op_index: Option<usize>,
}

fn exact_json_object_fields(
    value: &serde_json::Value,
    allowed: &[&str],
    required: &[&str],
    context: &str,
) -> Result<(), String> {
    let object = value
        .as_object()
        .ok_or_else(|| format!("{context} is not an object"))?;
    if let Some(field) = object
        .keys()
        .find(|field| !allowed.contains(&field.as_str()))
    {
        return Err(format!("{context} contains unknown field {field:?}"));
    }
    if let Some(field) = required.iter().find(|field| !object.contains_key(**field)) {
        return Err(format!("{context} lacks required field {field:?}"));
    }
    Ok(())
}

fn is_exact_query_not_found_response(
    body: &[u8],
    expected_query_id: mongreldb_query::QueryId,
) -> bool {
    const FIELDS: &[&str] = &[
        "query_id",
        "status",
        "terminal_state",
        "committed",
        "committed_statements",
        "last_commit_epoch",
        "last_commit_epoch_text",
        "first_commit_statement_index",
        "last_commit_statement_index",
        "completed_statements",
        "statement_index",
        "cancel_outcome",
        "cancellation_reason",
        "retryable",
        "server_state",
        "outcome",
        "error",
    ];
    const OUTCOME_FIELDS: &[&str] = &[
        "committed",
        "committed_statements",
        "last_commit_epoch",
        "last_commit_epoch_text",
        "first_commit_statement_index",
        "last_commit_statement_index",
        "completed_statements",
        "statement_index",
        "serialization",
    ];
    const NULL_FIELDS: &[&str] = &[
        "terminal_state",
        "committed",
        "committed_statements",
        "last_commit_epoch",
        "last_commit_epoch_text",
        "first_commit_statement_index",
        "last_commit_statement_index",
        "completed_statements",
        "statement_index",
        "cancellation_reason",
    ];
    let Ok(value) = strict_json_value(body) else {
        return false;
    };
    if exact_json_object_fields(&value, FIELDS, FIELDS, "query-not-found response").is_err() {
        return false;
    }
    let expected_query_id = expected_query_id.to_string();
    if value["query_id"].as_str() != Some(expected_query_id.as_str())
        || value["status"].as_str() != Some("unknown")
        || value["cancel_outcome"].as_str() != Some("not_found")
        || value["server_state"].as_str() != Some("not_found")
        || value["retryable"].as_bool() != Some(false)
        || NULL_FIELDS.iter().any(|field| !value[*field].is_null())
    {
        return false;
    }
    let outcome = &value["outcome"];
    if exact_json_object_fields(
        outcome,
        OUTCOME_FIELDS,
        OUTCOME_FIELDS,
        "query-not-found outcome",
    )
    .is_err()
        || outcome["serialization"].as_str() != Some("unknown")
        || OUTCOME_FIELDS
            .iter()
            .filter(|field| **field != "serialization")
            .any(|field| !outcome[*field].is_null())
    {
        return false;
    }
    let error = &value["error"];
    exact_json_object_fields(
        error,
        &["code", "message", "query_id", "committed", "retryable"],
        &["code", "message", "query_id", "committed", "retryable"],
        "query-not-found error",
    )
    .is_ok()
        && error["code"].as_str() == Some("QUERY_NOT_FOUND")
        && error["message"].as_str().is_some()
        && error["query_id"].as_str() == Some(expected_query_id.as_str())
        && error["committed"].is_null()
        && error["retryable"].as_bool() == Some(false)
}

fn validate_kit_txn_error_json(value: &serde_json::Value) -> Result<(), String> {
    let error = value
        .get("error")
        .ok_or_else(|| "Kit transaction error response lacks error".to_owned())?;
    exact_json_object_fields(
        error,
        &["code", "message", "op_index"],
        &["code", "message"],
        "Kit transaction error",
    )?;
    let status = value
        .get("status")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "Kit transaction error response lacks status".to_owned())?;
    if matches!(status, "committed" | "outcome_unknown")
        && error
            .as_object()
            .is_some_and(|error| error.contains_key("op_index"))
    {
        return Err("durable Kit transaction error must omit op_index".into());
    }
    match status {
        "aborted" => {
            let enriched = value.get("committed").is_some() || value.get("retryable").is_some();
            if enriched {
                exact_json_object_fields(
                    value,
                    &["status", "committed", "retryable", "error"],
                    &["status", "committed", "retryable", "error"],
                    "Kit transaction error response",
                )?;
            } else {
                exact_json_object_fields(
                    value,
                    &["status", "error"],
                    &["status", "error"],
                    "Kit transaction error response",
                )?;
            }
        }
        "committed" => exact_json_object_fields(
            value,
            &[
                "status",
                "committed",
                "epoch",
                "epoch_text",
                "results",
                "retryable",
                "error",
            ],
            &[
                "status",
                "committed",
                "epoch",
                "epoch_text",
                "retryable",
                "error",
            ],
            "Kit transaction error response",
        )?,
        "outcome_unknown" => exact_json_object_fields(
            value,
            &[
                "status",
                "committed",
                "epoch",
                "epoch_text",
                "retryable",
                "error",
            ],
            &[
                "status",
                "committed",
                "epoch",
                "epoch_text",
                "retryable",
                "error",
            ],
            "Kit transaction error response",
        )?,
        _ => return Err("Kit transaction error response status is invalid".into()),
    }
    Ok(())
}

fn proven_kit_commit_metadata(value: &serde_json::Value) -> Option<(u64, String)> {
    if value.get("status")?.as_str()? != "committed"
        || !value.get("committed")?.as_bool()?
        || value.get("retryable")?.as_bool()?
    {
        return None;
    }
    let error = value.get("error")?.as_object()?;
    if error.get("code")?.as_str()? != "COMMIT_OUTCOME"
        || error.get("message")?.as_str()?.is_empty()
        || error.get("op_index").is_some_and(|index| {
            !index.is_null()
                && index
                    .as_u64()
                    .and_then(|index| usize::try_from(index).ok())
                    .is_none()
        })
    {
        return None;
    }
    let epoch = value.get("epoch")?.as_u64()?;
    let epoch_text = value.get("epoch_text")?.as_str()?.to_owned();
    (exact_kit_epoch(Some(&epoch_text), Some(epoch))
        .ok()
        .flatten()
        == Some(epoch))
    .then_some((epoch, epoch_text))
}

fn decode_http_error(status: u16, body: &[u8]) -> ClientError {
    let value = match strict_json_value(body) {
        Ok(value) => value,
        Err(error) => {
            if body
                .iter()
                .copied()
                .find(|byte| !byte.is_ascii_whitespace())
                .is_some_and(|byte| matches!(byte, b'{' | b'['))
            {
                return ClientError::Decode(format!("invalid HTTP error response: {error}"));
            }
            return ClientError::Http {
                status,
                body: format!("non-JSON error response ({} bytes)", body.len()),
            };
        }
    };
    if status == 404
        && value
            .get("query_id")
            .and_then(serde_json::Value::as_str)
            .and_then(|query_id| query_id.parse::<mongreldb_query::QueryId>().ok())
            .is_some_and(|query_id| is_exact_query_not_found_response(body, query_id))
    {
        return match serde_json::from_value::<RemoteQueryErrorResponse>(value) {
            Ok(response) => ClientError::Query {
                status,
                code: RemoteQueryErrorCode::QueryNotFound,
                message: response.error.message.clone(),
                response: Box::new(response),
            },
            Err(error) => ClientError::Decode(format!("invalid query-not-found response: {error}")),
        };
    }
    let queryless_sql_response = value.get("query_id").is_none()
        && value
            .get("error")
            .and_then(|error| error.get("query_id"))
            .is_none()
        && value.get("outcome").is_some();
    if queryless_sql_response {
        return match serde_json::from_value::<RemoteQueryErrorResponse>(value) {
            Ok(response) => match validate_queryless_sql_error(response) {
                Ok(response) => ClientError::Query {
                    status,
                    code: response.error.code.clone(),
                    message: response.error.message.clone(),
                    response: Box::new(response),
                },
                Err(error) => {
                    ClientError::Decode(format!("invalid queryless SQL error response: {error}"))
                }
            },
            Err(error) => {
                ClientError::Decode(format!("invalid queryless SQL error response: {error}"))
            }
        };
    }
    let query_response = value.get("query_id").is_some()
        || value.get("outcome").is_some()
        || value.get("server_state").is_some()
        || value
            .get("error")
            .and_then(|error| error.get("query_id"))
            .is_some();
    if query_response {
        return match serde_json::from_value::<RemoteQueryErrorResponse>(value) {
            Ok(response) => {
                let query_id = response
                    .query_id
                    .as_deref()
                    .or(response.error.query_id.as_deref())
                    .and_then(|query_id| query_id.parse::<mongreldb_query::QueryId>().ok());
                match query_id
                    .ok_or_else(|| "query error response lacks a valid query_id".to_owned())
                    .and_then(|query_id| validate_remote_query_error(response, query_id))
                {
                    Ok(response) => ClientError::Query {
                        status,
                        code: response.error.code.clone(),
                        message: response.error.message.clone(),
                        response: Box::new(response),
                    },
                    Err(error) => {
                        ClientError::Decode(format!("invalid query error response: {error}"))
                    }
                }
            }
            Err(error) => ClientError::Decode(format!("invalid query error response: {error}")),
        };
    }
    match serde_json::from_value::<KitErrorEnvelope>(value) {
        Ok(env) => match validate_kit_error_envelope(&env) {
            Ok(epoch) => ClientError::Kit {
                code: KitErrorCode::from_str(&env.error.code),
                message: env.error.message,
                op_index: env.error.op_index,
                status,
                committed: env.committed,
                epoch,
                epoch_text: env.epoch_text,
                retryable: env.retryable,
            },
            Err(message) => ClientError::Decode(message),
        },
        Err(error) => ClientError::Decode(format!("invalid Kit error response: {error}")),
    }
}

fn validate_kit_error_envelope(env: &KitErrorEnvelope) -> Result<Option<u64>, String> {
    if env.status.is_empty()
        || env.error.code.is_empty()
        || env.error.message.is_empty()
        || env.retryable == Some(true)
            && !matches!(
                env.error.code.as_str(),
                "IDEMPOTENCY_STORE_FULL" | "IDEMPOTENCY_STORE_UNAVAILABLE"
            )
    {
        return Err("invalid Kit error metadata".into());
    }
    let epoch = exact_kit_epoch(env.epoch_text.as_deref(), env.epoch)?;
    match env.error.code.as_str() {
        "COMMIT_OUTCOME" => {
            if env.status != "committed"
                || env.committed != Some(true)
                || epoch.is_none()
                || env.epoch_text.is_none()
                || env.retryable != Some(false)
                || env.error.op_index.is_some()
            {
                return Err("invalid committed Kit error metadata".into());
            }
        }
        "QUERY_OUTCOME_UNKNOWN" => {
            if env.status != "outcome_unknown"
                || env.committed.is_some()
                || env.results.is_some()
                || env.retryable != Some(false)
                || env.error.op_index.is_some()
            {
                return Err("invalid unknown Kit outcome metadata".into());
            }
        }
        _ => {
            let metadata_valid = match env.status.as_str() {
                "aborted" => matches!(
                    (env.committed, env.retryable),
                    (None, None) | (Some(false), Some(_))
                ),
                "error" => env.committed.is_none() && env.retryable.is_none(),
                _ => false,
            };
            if !metadata_valid || epoch.is_some() || env.results.is_some() {
                return Err("invalid aborted Kit error metadata".into());
            }
        }
    }
    Ok(epoch)
}

fn decode_kit_txn_http_error(status: u16, body: &[u8], request: &KitTxnRequest) -> ClientError {
    let value = match strict_json_value(body) {
        Ok(value) => value,
        Err(error) => {
            return kit_txn_outcome_unknown(format!(
                "invalid Kit transaction error response: {error}"
            ))
        }
    };
    let proven_commit = proven_kit_commit_metadata(&value);
    if let Err(error) = validate_kit_txn_error_json(&value) {
        if let Some((epoch, epoch_text)) = proven_commit.as_ref() {
            return committed_kit_txn_decode_error(status, *epoch, epoch_text.clone(), error);
        }
        return kit_txn_outcome_unknown(error);
    }
    let env = match serde_json::from_value::<KitErrorEnvelope>(value) {
        Ok(env) => env,
        Err(error) => {
            if let Some((epoch, epoch_text)) = proven_commit {
                return committed_kit_txn_decode_error(
                    status,
                    epoch,
                    epoch_text,
                    format!(
                        "transaction committed but its error result could not be decoded: {error}"
                    ),
                );
            }
            return kit_txn_outcome_unknown(format!(
                "invalid Kit transaction error response: {error}"
            ));
        }
    };
    let epoch = match validate_kit_error_envelope(&env) {
        Ok(epoch) => epoch,
        Err(error) => {
            if let Some((epoch, epoch_text)) = proven_commit {
                return committed_kit_txn_decode_error(status, epoch, epoch_text, error);
            }
            return kit_txn_outcome_unknown(error);
        }
    };
    if env
        .error
        .op_index
        .is_some_and(|op_index| op_index >= request.ops.len())
    {
        return kit_txn_outcome_unknown("Kit transaction error op_index is out of bounds");
    }
    if env.committed == Some(true) {
        if let Some(results) = env.results.as_deref() {
            if let Err(error) = validate_kit_results(results, request) {
                let (Some(epoch), Some(epoch_text)) = (epoch, env.epoch_text.clone()) else {
                    return kit_txn_outcome_unknown(
                        "committed Kit transaction response lost validated epoch metadata",
                    );
                };
                return committed_kit_txn_decode_error(
                    status,
                    epoch,
                    epoch_text,
                    format!("transaction committed but its result was invalid: {error}"),
                );
            }
        }
    }
    ClientError::Kit {
        code: KitErrorCode::from_str(&env.error.code),
        message: env.error.message,
        op_index: env.error.op_index,
        status,
        committed: env.committed,
        epoch,
        epoch_text: env.epoch_text,
        retryable: env.retryable,
    }
}

fn decode_sql_http_error(
    status: u16,
    body: &[u8],
    expected_query_id: mongreldb_query::QueryId,
) -> Result<ClientError, String> {
    let value =
        strict_json_value(body).map_err(|error| format!("invalid SQL error response: {error}"))?;
    if status == 404 && is_exact_query_not_found_response(body, expected_query_id) {
        let response = serde_json::from_value::<RemoteQueryErrorResponse>(value)
            .map_err(|error| format!("invalid query-not-found response: {error}"))?;
        return Ok(ClientError::Query {
            status,
            code: RemoteQueryErrorCode::QueryNotFound,
            message: response.error.message.clone(),
            response: Box::new(response),
        });
    }
    let proof = sql_error_commit_proof(&value, expected_query_id);
    let response = match serde_json::from_value::<RemoteQueryErrorResponse>(value) {
        Ok(response) => response,
        Err(error) => {
            if let Some(proof) = proof.clone() {
                return Ok(committed_sql_receipt_decode_error(
                    expected_query_id,
                    proof,
                    format!("SQL committed but its error response was invalid: {error}"),
                ));
            }
            return Err(format!("invalid SQL error response: {error}"));
        }
    };
    let response = match validate_remote_query_error(response, expected_query_id) {
        Ok(response) => response,
        Err(error) => {
            if let Some(proof) = proof {
                return Ok(committed_sql_receipt_decode_error(
                    expected_query_id,
                    proof,
                    format!("SQL committed but its error response was invalid: {error}"),
                ));
            }
            return Err(error);
        }
    };
    Ok(ClientError::Query {
        status,
        code: response.error.code.clone(),
        message: response.error.message.clone(),
        response: Box::new(response),
    })
}

fn validate_sql_query_id_header(
    headers: &reqwest::header::HeaderMap,
    expected_query_id: mongreldb_query::QueryId,
) -> Result<(), String> {
    let value = headers
        .get("x-mongreldb-query-id")
        .ok_or_else(|| "SQL response is missing x-mongreldb-query-id".to_owned())?
        .to_str()
        .map_err(|_| "SQL response has a non-UTF-8 x-mongreldb-query-id".to_owned())?;
    if value != expected_query_id.to_string() {
        return Err("SQL response x-mongreldb-query-id does not match the request".into());
    }
    Ok(())
}

fn client_error_proves_commit(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::Query { response, .. }
            if response.committed == Some(true)
                && response.committed_statements.is_some_and(|count| count > 0)
                && response.last_commit_epoch.is_some()
                && response.last_commit_epoch_text.is_some()
    )
}

fn exact_kit_epoch(text: Option<&str>, numeric: Option<u64>) -> Result<Option<u64>, String> {
    let Some(text) = text else {
        return Ok(numeric);
    };
    let exact = text
        .parse::<u64>()
        .map_err(|_| "epoch_text is not an unsigned integer".to_owned())?;
    if exact.to_string() != text {
        return Err("epoch_text is not canonical".into());
    }
    if numeric.is_some_and(|numeric| numeric != exact) {
        return Err("epoch and epoch_text disagree".into());
    }
    Ok(Some(exact))
}

fn exact_required_u64(field: &str, numeric: u64, text: &str) -> ClientResult<u64> {
    let exact = text
        .parse::<u64>()
        .map_err(|_| ClientError::Decode(format!("{field}_text is not an unsigned integer")))?;
    if exact.to_string() != text {
        return Err(ClientError::Decode(format!(
            "{field}_text is not canonical"
        )));
    }
    if exact != numeric {
        return Err(ClientError::Decode(format!(
            "{field} and {field}_text disagree"
        )));
    }
    Ok(exact)
}

fn parse_required_u64_text(field: &str, text: &str) -> ClientResult<u64> {
    let value = text
        .parse::<u64>()
        .map_err(|_| ClientError::Decode(format!("{field} is not an unsigned integer")))?;
    if value.to_string() != text {
        return Err(ClientError::Decode(format!("{field} is not canonical")));
    }
    Ok(value)
}

fn decode_required_json<T: serde::de::DeserializeOwned>(
    response: reqwest::blocking::Response,
    context: &str,
) -> ClientResult<T> {
    decode_blocking_json(response, MAX_CONTROL_RESPONSE_BYTES, context)
}

fn write_outcome_unknown(context: &str, error: impl std::fmt::Display) -> ClientError {
    kit_txn_outcome_unknown(format!(
        "{context} outcome is unknown because its response could not be confirmed: {error}"
    ))
}

fn decode_write_json<T: serde::de::DeserializeOwned>(
    response: reqwest::blocking::Response,
    context: &str,
) -> ClientResult<T> {
    decode_required_json(response, context).map_err(|error| write_outcome_unknown(context, error))
}

fn validate_committed_write(response: &CommittedWriteResponse, context: &str) -> ClientResult<u64> {
    if response.status != "committed" {
        return Err(write_outcome_unknown(
            context,
            "success response does not prove a commit",
        ));
    }
    exact_required_u64("epoch", response.epoch, &response.epoch_text)
        .map_err(|error| write_outcome_unknown(context, error))
}

fn validate_trigger_drop_response(
    response: &TriggerDropResponse,
    expected_name: &str,
) -> ClientResult<()> {
    validate_committed_write(
        &CommittedWriteResponse {
            status: response.status.clone(),
            epoch: response.epoch,
            epoch_text: response.epoch_text.clone(),
        },
        "drop trigger",
    )?;
    let mut names = std::collections::HashSet::new();
    let mut identities = std::collections::HashSet::new();
    if response.dropped_trigger.name != expected_name
        || response.resource_tables.iter().any(|binding| {
            binding.name.is_empty()
                || !names.insert(&binding.name)
                || !identities.insert((binding.table_id, binding.schema_id))
        })
    {
        return Err(write_outcome_unknown(
            "drop trigger",
            "success response has an invalid resource binding",
        ));
    }
    Ok(())
}

fn capability_unsupported(message: impl Into<String>) -> ClientError {
    let message = message.into();
    let response = RemoteQueryErrorResponse {
        query_id: None,
        status: "failed_before_commit".into(),
        terminal_state: Some("failed_before_commit".into()),
        committed: Some(false),
        committed_statements: Some(0),
        last_commit_epoch: None,
        last_commit_epoch_text: None,
        first_commit_statement_index: None,
        last_commit_statement_index: None,
        completed_statements: Some(0),
        statement_index: Some(0),
        cancel_outcome: None,
        cancellation_reason: None,
        retryable: false,
        server_state: None,
        outcome: RemoteQueryOutcome::default(),
        error: RemoteQueryErrorBody {
            code: RemoteQueryErrorCode::CapabilityUnsupported,
            message: message.clone(),
            query_id: None,
            committed: Some(false),
            retryable: false,
        },
    };
    ClientError::Query {
        status: 0,
        code: RemoteQueryErrorCode::CapabilityUnsupported,
        message,
        response: Box::new(response),
    }
}

fn client_serialization_error(
    query_id: Option<mongreldb_query::QueryId>,
    message: impl Into<String>,
) -> ClientError {
    let message = message.into();
    let query_id = query_id.map(|query_id| query_id.to_string());
    let response = RemoteQueryErrorResponse {
        query_id: query_id.clone(),
        status: "failed_before_commit".into(),
        terminal_state: Some("failed_before_commit".into()),
        committed: Some(false),
        committed_statements: Some(0),
        last_commit_epoch: None,
        last_commit_epoch_text: None,
        first_commit_statement_index: None,
        last_commit_statement_index: None,
        completed_statements: Some(0),
        statement_index: Some(0),
        cancel_outcome: None,
        cancellation_reason: None,
        retryable: false,
        server_state: Some("failed".into()),
        outcome: RemoteQueryOutcome {
            committed: Some(false),
            committed_statements: Some(0),
            completed_statements: Some(0),
            statement_index: Some(0),
            serialization: "failed".into(),
            ..RemoteQueryOutcome::default()
        },
        error: RemoteQueryErrorBody {
            code: RemoteQueryErrorCode::SerializationFailed,
            message: message.clone(),
            query_id,
            committed: Some(false),
            retryable: false,
        },
    };
    ClientError::Query {
        status: 0,
        code: RemoteQueryErrorCode::SerializationFailed,
        message,
        response: Box::new(response),
    }
}

struct IdempotentAttemptError {
    error: ClientError,
    replay: bool,
}

impl IdempotentAttemptError {
    fn final_error(error: ClientError) -> Self {
        Self {
            error,
            replay: false,
        }
    }
}

fn fresh_query_id(previous: mongreldb_query::QueryId) -> ClientResult<mongreldb_query::QueryId> {
    let query_id = mongreldb_query::QueryId::random()
        .map_err(|error| ClientError::Transport(error.to_string()))?;
    if query_id == previous {
        return Err(ClientError::Transport(
            "generated duplicate SQL query ID".into(),
        ));
    }
    Ok(query_id)
}

fn max_known(left: Option<usize>, right: Option<usize>) -> Option<usize> {
    left.into_iter().chain(right).max()
}

fn recovered_query_error(status: RemoteQueryStatus, message: String) -> ClientError {
    let committed = status.durably_committed();
    let code = status
        .terminal_error
        .as_ref()
        .map(|error| error.code.clone())
        .unwrap_or_else(|| {
            if committed == Some(true) {
                RemoteQueryErrorCode::CommitOutcome
            } else {
                RemoteQueryErrorCode::SerializationFailed
            }
        });
    let response = RemoteQueryErrorResponse {
        query_id: Some(status.query_id.clone()),
        status: status.status.clone(),
        terminal_state: status.terminal_state.clone(),
        committed,
        committed_statements: max_known(
            status.committed_statements,
            status.outcome.committed_statements,
        ),
        last_commit_epoch: status
            .last_commit_epoch
            .or(status.outcome.last_commit_epoch),
        last_commit_epoch_text: status
            .last_commit_epoch_text
            .clone()
            .or_else(|| status.outcome.last_commit_epoch_text.clone()),
        first_commit_statement_index: status
            .first_commit_statement_index
            .or(status.outcome.first_commit_statement_index),
        last_commit_statement_index: status
            .last_commit_statement_index
            .or(status.outcome.last_commit_statement_index),
        completed_statements: max_known(
            status.completed_statements,
            status.outcome.completed_statements,
        ),
        statement_index: max_known(status.statement_index, status.outcome.statement_index),
        cancel_outcome: status.cancel_outcome,
        cancellation_reason: (!status.cancellation_reason.is_empty())
            .then_some(status.cancellation_reason.clone()),
        retryable: status.retryable,
        server_state: Some(status.server_state_or_state().to_owned()),
        outcome: status.outcome,
        error: RemoteQueryErrorBody {
            code: code.clone(),
            message: message.clone(),
            query_id: Some(status.query_id),
            committed,
            retryable: status.retryable,
        },
    };
    ClientError::Query {
        status: 0,
        code,
        message,
        response: Box::new(response),
    }
}

fn recovery_status_is_decisive(status: &RemoteQueryStatus) -> bool {
    status.durably_committed() == Some(true)
        || status.is_terminal()
            && (status.durably_committed().is_some() || status.terminal_error.is_some())
}

impl MongrelClient {
    pub fn builder(url: impl AsRef<str>) -> MongrelClientBuilder {
        let base_url = sanitized_base_url(url.as_ref());
        MongrelClientBuilder {
            invalid_base_url: base_url.is_none(),
            base_url: base_url.unwrap_or_default(),
            authorization: None,
            invalid_authorization: false,
            connect_timeout: None,
            request_timeout: None,
            pool_idle_timeout: None,
        }
    }

    pub fn new(url: &str) -> ClientResult<Self> {
        Self::builder(url).build()
    }

    pub fn with_options(url: impl AsRef<str>, options: RemoteOptions) -> ClientResult<Self> {
        let mut builder = Self::builder(url);
        if let Some(timeout) = options.transport_timeout {
            builder = builder.request_timeout(timeout);
        }
        builder = match options.auth {
            Some(RemoteAuth::Bearer(token)) => builder.bearer_token(token.expose_secret()),
            Some(RemoteAuth::Basic { username, password }) => {
                builder.basic_auth(username, password.expose_secret())
            }
            None => builder,
        };
        builder.build()
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    fn url_segments(&self, segments: &[&str]) -> ClientResult<String> {
        url_with_segments(&self.base_url, segments)
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
        let body = bounded_blocking_bytes(resp, MAX_CONTROL_RESPONSE_BYTES).map_err(|error| {
            ClientError::Decode(format!("invalid HTTP error response: {error}"))
        })?;
        Err(decode_http_error(status_u16, &body))
    }

    fn write_response(
        &self,
        response: Result<reqwest::blocking::Response, reqwest::Error>,
        context: &str,
    ) -> ClientResult<reqwest::blocking::Response> {
        let response = response.map_err(|error| write_outcome_unknown(context, error))?;
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let body = bounded_blocking_bytes(response, MAX_CONTROL_RESPONSE_BYTES)
            .map_err(|error| write_outcome_unknown(context, error))?;
        let error = decode_http_error(status.as_u16(), &body);
        match error {
            ClientError::Decode(message) => Err(write_outcome_unknown(context, message)),
            error => Err(error),
        }
    }

    fn check_sql_response(
        &self,
        response: reqwest::blocking::Response,
        query_id: mongreldb_query::QueryId,
    ) -> ClientResult<reqwest::blocking::Response> {
        let status = response.status();
        let pre_handler_auth = matches!(
            status,
            reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN
        );
        if status.is_success() {
            return Ok(response);
        }
        let header_error = (!pre_handler_auth)
            .then(|| validate_sql_query_id_header(response.headers(), query_id).err())
            .flatten();
        let status = status.as_u16();
        let body =
            bounded_blocking_bytes(response, MAX_CONTROL_RESPONSE_BYTES).map_err(|error| {
                if pre_handler_auth {
                    ClientError::Http {
                        status,
                        body: format!("unreadable authentication error response: {error}"),
                    }
                } else {
                    self.recover_after_transport_loss(query_id, error.to_string())
                }
            })?;
        if pre_handler_auth {
            return Err(decode_http_error(status, &body));
        }
        match decode_sql_http_error(status, &body, query_id) {
            Ok(error) if client_error_proves_commit(&error) => Err(error),
            Ok(error) if header_error.is_none() => Err(error),
            Ok(_) => Err(self.recover_after_transport_loss(
                query_id,
                header_error.unwrap_or_else(|| "SQL response identity is invalid".into()),
            )),
            Err(error) => Err(self.recover_after_transport_loss(query_id, error)),
        }
    }

    fn check_query_status_response(
        &self,
        response: reqwest::blocking::Response,
        query_id: mongreldb_query::QueryId,
    ) -> ClientResult<reqwest::blocking::Response> {
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let status = status.as_u16();
        let body = bounded_blocking_bytes(response, MAX_CONTROL_RESPONSE_BYTES)
            .map_err(ClientError::Decode)?;
        if matches!(status, 401 | 403) {
            return Err(decode_http_error(status, &body));
        }
        match decode_sql_http_error(status, &body, query_id) {
            Ok(error) => Err(error),
            Err(error) => Err(ClientError::Decode(error)),
        }
    }

    pub fn health(&self) -> ClientResult<String> {
        let resp = self.client.get(self.url("/health")).send()?;
        let bytes = bounded_blocking_bytes(self.check(resp)?, MAX_CONTROL_RESPONSE_BYTES)
            .map_err(ClientError::Transport)?;
        String::from_utf8(bytes)
            .map_err(|_| ClientError::Decode("invalid health response: non-UTF-8 body".into()))
    }

    pub fn capabilities(&self) -> ClientResult<ServerCapabilities> {
        let response = self.client.get(self.url("/capabilities")).send()?;
        decode_blocking_json(
            self.check(response)?,
            MAX_CONTROL_RESPONSE_BYTES,
            "capabilities",
        )
    }

    pub fn sql_cancellation_capabilities(&self) -> ClientResult<SqlCancellationCapabilities> {
        let capabilities = self.capabilities()?.sql_cancellation;
        if capabilities.version < 2
            || !capabilities.client_query_ids
            || !capabilities.cancel_endpoint
            || !capabilities.query_status
            || !capabilities.pre_registration_cancel
        {
            return Err(capability_unsupported(
                "server does not support SQL cancellation capability version 2",
            ));
        }
        Ok(capabilities)
    }

    pub fn sql_idempotency_capabilities(&self) -> ClientResult<SqlIdempotencyCapabilities> {
        let capabilities = self
            .capabilities()?
            .sql_idempotency
            .ok_or_else(|| capability_unsupported("server does not advertise SQL idempotency"))?;
        if capabilities.version < 1
            || !capabilities.durable_pre_execution_intent
            || !capabilities.replay_committed_receipt
            || !capabilities.indeterminate_never_reexecutes
        {
            return Err(capability_unsupported(
                "server does not support safe SQL idempotency capability version 1",
            ));
        }
        Ok(capabilities)
    }

    pub fn sql_pagination_capabilities(&self) -> ClientResult<SqlPaginationCapabilities> {
        let capabilities = self
            .capabilities()?
            .sql_pagination
            .ok_or_else(|| capability_unsupported("server does not advertise SQL pagination"))?;
        if capabilities.version < 1
            || capabilities.continuation_endpoint != "/sql/continue"
            || !capabilities.retained_snapshot
            || !capabilities.projection_required
            || !capabilities.byte_and_token_hints
        {
            return Err(capability_unsupported(
                "server does not support SQL pagination capability version 1",
            ));
        }
        Ok(capabilities)
    }

    pub fn set_history_retention_epochs(&self, epochs: u64) -> ClientResult<HistoryRetention> {
        let resp = self
            .client
            .put(self.url("/history/retention"))
            .json(&serde_json::json!({"history_retention_epochs": epochs}))
            .send()?;
        decode_blocking_json(
            self.check(resp)?,
            MAX_CONTROL_RESPONSE_BYTES,
            "history retention",
        )
    }

    pub fn history_retention_epochs(&self) -> ClientResult<u64> {
        Ok(self.history_retention()?.history_retention_epochs)
    }

    pub fn earliest_retained_epoch(&self) -> ClientResult<u64> {
        Ok(self.history_retention()?.earliest_retained_epoch)
    }

    fn history_retention(&self) -> ClientResult<HistoryRetention> {
        let resp = self.client.get(self.url("/history/retention")).send()?;
        decode_blocking_json(
            self.check(resp)?,
            MAX_CONTROL_RESPONSE_BYTES,
            "history retention",
        )
    }

    // ── Table management ──

    pub fn list_tables(&self) -> ClientResult<Vec<String>> {
        let resp = self.client.get(self.url("/tables")).send()?;
        decode_blocking_json(self.check(resp)?, MAX_CONTROL_RESPONSE_BYTES, "table list")
    }

    pub fn create_table(&self, name: &str, columns: Vec<ColumnDefJson>) -> ClientResult<u64> {
        let response = self.write_response(
            self.client
                .post(self.url("/tables"))
                .json(&serde_json::json!({ "name": name, "columns": columns }))
                .send(),
            "create table",
        )?;
        let response: TableIdResponse = decode_write_json(response, "create table")?;
        exact_required_u64("table_id", response.table_id, &response.table_id_text)
            .map_err(|error| write_outcome_unknown("create table", error))
    }

    pub fn drop_table(&self, name: &str) -> ClientResult<()> {
        let response = self.write_response(
            self.client
                .delete(self.url_segments(&["tables", name])?)
                .send(),
            "drop table",
        )?;
        let response: CommittedWriteResponse = decode_write_json(response, "drop table")?;
        validate_committed_write(&response, "drop table")?;
        Ok(())
    }

    // ── Table-qualified operations ──

    pub fn count(&self, table: &str) -> ClientResult<u64> {
        let resp = self
            .client
            .get(self.url_segments(&["tables", table, "count"])?)
            .send()?;
        let resp = self.check(resp)?;
        let cr: CountResp = decode_blocking_json(resp, MAX_CONTROL_RESPONSE_BYTES, "count")?;
        Ok(cr.count)
    }

    pub fn put(&self, table: &str, row: Vec<(u16, mongreldb_core::Value)>) -> ClientResult<u64> {
        let mut json_row = Vec::with_capacity(row.len() * 2);
        for (id, value) in &row {
            json_row.push(serde_json::json!(id));
            json_row.push(value_to_json(value)?);
        }
        let response = self.write_response(
            self.client
                .post(self.url_segments(&["tables", table, "put"])?)
                .json(&serde_json::json!({ "row": json_row }))
                .send(),
            "put",
        )?;
        let response: RowIdResponse = decode_write_json(response, "put")?;
        parse_required_u64_text("row_id", &response.row_id)
            .map_err(|error| write_outcome_unknown("put", error))
    }

    pub fn commit(&self, table: &str) -> ClientResult<u64> {
        let response = self.write_response(
            self.client
                .post(self.url_segments(&["tables", table, "commit"])?)
                .send(),
            "commit",
        )?;
        let response: EpochResponse = decode_write_json(response, "commit")?;
        exact_required_u64("epoch", response.epoch, &response.epoch_text)
            .map_err(|error| write_outcome_unknown("commit", error))
    }

    // ── SQL read ──

    pub fn sql(&self, sql: &str) -> ClientResult<Vec<RecordBatch>> {
        self.sql_with_options(sql, SqlClientOptions::default())
    }

    pub fn sql_with_options(
        &self,
        sql: &str,
        mut options: SqlClientOptions,
    ) -> ClientResult<Vec<RecordBatch>> {
        if options.query_id.is_some() || options.timeout.is_some() {
            self.sql_cancellation_capabilities()?;
        }
        let query_id = match options.query_id.take() {
            Some(query_id) => query_id,
            None => mongreldb_query::QueryId::random()
                .map_err(|error| ClientError::Transport(error.to_string()))?,
        };
        let timeout_ms = options
            .timeout
            .map(|timeout| timeout.as_millis().min(u128::from(u64::MAX)) as u64);
        let response = self
            .client
            .post(self.url("/sql"))
            .json(&SqlReq {
                sql: sql.to_string(),
                format: Some("arrow"), // Rust client decodes Arrow IPC directly
                query_id,
                timeout_ms,
                max_output_rows: None,
                max_output_bytes: None,
                idempotency_key: None,
                pagination: None,
            })
            .send();
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                return Err(self.recover_after_transport_loss(query_id, error.to_string()));
            }
        };
        let resp = self.check_sql_response(response, query_id)?;
        if let Err(error) = validate_sql_query_id_header(resp.headers(), query_id) {
            return Err(self.recover_after_transport_loss(query_id, error));
        }
        let bytes = bounded_blocking_bytes(resp, MAX_SQL_RESPONSE_BYTES)
            .map_err(|error| self.recover_after_transport_loss(query_id, error.to_string()))?;
        read_arrow_ipc(&bytes)
            .map_err(|error| self.recover_after_transport_loss(query_id, error.to_string()))
    }

    pub fn sql_write_idempotent(
        &self,
        sql: &str,
        idempotency_key: impl Into<String>,
    ) -> ClientResult<RemoteSqlReceipt> {
        self.sql_write_idempotent_with_options(sql, idempotency_key, SqlClientOptions::default())
    }

    pub fn sql_write_idempotent_with_options(
        &self,
        sql: &str,
        idempotency_key: impl Into<String>,
        mut options: SqlClientOptions,
    ) -> ClientResult<RemoteSqlReceipt> {
        let idempotency_key = idempotency_key.into();
        if idempotency_key.is_empty() || idempotency_key.len() > 256 {
            return Err(ClientError::Decode(
                "SQL idempotency key must contain 1 to 256 bytes".into(),
            ));
        }
        self.sql_idempotency_capabilities()?;
        let query_id = match options.query_id.take() {
            Some(query_id) => query_id,
            None => mongreldb_query::QueryId::random()
                .map_err(|error| ClientError::Transport(error.to_string()))?,
        };
        let result =
            self.sql_write_idempotent_once(sql, &idempotency_key, &options, query_id, None);
        if result.as_ref().is_err_and(|error| error.replay) {
            self.sql_idempotency_capabilities()?;
            return self
                .sql_write_idempotent_once(
                    sql,
                    &idempotency_key,
                    &options,
                    fresh_query_id(query_id)?,
                    Some(query_id),
                )
                .map_err(|error| error.error);
        }
        result.map_err(|error| error.error)
    }

    fn sql_write_idempotent_once(
        &self,
        sql: &str,
        idempotency_key: &str,
        options: &SqlClientOptions,
        query_id: mongreldb_query::QueryId,
        expected_original_query_id: Option<mongreldb_query::QueryId>,
    ) -> Result<RemoteSqlReceipt, IdempotentAttemptError> {
        let timeout_ms = options
            .timeout
            .map(|timeout| timeout.as_millis().min(u128::from(u64::MAX)) as u64);
        let response = self
            .client
            .post(self.url("/sql"))
            .json(&SqlReq {
                sql: sql.to_owned(),
                format: None,
                query_id,
                timeout_ms,
                max_output_rows: None,
                max_output_bytes: None,
                idempotency_key: Some(idempotency_key.to_owned()),
                pagination: None,
            })
            .send();
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                return Err(self.idempotent_attempt_loss(query_id, error.to_string()));
            }
        };
        let response = self
            .check_sql_response(response, query_id)
            .map_err(IdempotentAttemptError::final_error)?;
        let header_error = validate_sql_query_id_header(response.headers(), query_id).err();
        let bytes = bounded_blocking_bytes(response, MAX_CONTROL_RESPONSE_BYTES)
            .map_err(|error| self.idempotent_attempt_loss(query_id, error))?;
        match decode_remote_sql_receipt(&bytes, query_id, expected_original_query_id) {
            Ok(receipt) if header_error.is_none() => Ok(receipt),
            Ok(_) => {
                let proof = strict_json_value(&bytes).ok().and_then(|value| {
                    sql_receipt_commit_proof(&value, query_id, expected_original_query_id)
                });
                match proof {
                    Some(proof) => Err(IdempotentAttemptError::final_error(
                        committed_sql_receipt_decode_error(
                            query_id,
                            proof,
                            header_error
                                .clone()
                                .unwrap_or_else(|| "SQL response identity is invalid".into()),
                        ),
                    )),
                    None => Err(self.idempotent_attempt_loss(
                        query_id,
                        header_error.unwrap_or_else(|| "SQL response identity is invalid".into()),
                    )),
                }
            }
            Err(SqlReceiptDecodeError::KnownCommit(error)) => {
                Err(IdempotentAttemptError::final_error(error))
            }
            Err(SqlReceiptDecodeError::Unknown(error)) => {
                Err(self.idempotent_attempt_loss(query_id, error))
            }
        }
    }

    fn idempotent_attempt_loss(
        &self,
        query_id: mongreldb_query::QueryId,
        message: String,
    ) -> IdempotentAttemptError {
        let missing = self
            .client
            .get(self.url(&format!("/queries/{query_id}")))
            .timeout(SQL_RECOVERY_REQUEST_TIMEOUT)
            .send()
            .ok()
            .filter(|response| response.status() == reqwest::StatusCode::NOT_FOUND)
            .and_then(|response| bounded_blocking_bytes(response, MAX_CONTROL_RESPONSE_BYTES).ok())
            .is_some_and(|body| is_exact_query_not_found_response(&body, query_id));
        if missing {
            return IdempotentAttemptError {
                error: ClientError::QueryOutcomeUnknown {
                    query_id: query_id.to_string(),
                    message,
                    status: None,
                    cancel_outcome: None,
                },
                replay: true,
            };
        }
        IdempotentAttemptError::final_error(self.recover_after_transport_loss(query_id, message))
    }

    pub fn sql_page(&self, sql: &str, mut options: SqlPageOptions) -> ClientResult<RemoteSqlPage> {
        validate_sql_page_options(&options)?;
        self.sql_pagination_capabilities()?;
        let query_id = match options.query_id.take() {
            Some(query_id) => query_id,
            None => mongreldb_query::QueryId::random()
                .map_err(|error| ClientError::Transport(error.to_string()))?,
        };
        let timeout_ms = options
            .timeout
            .map(|timeout| timeout.as_millis().min(u128::from(u64::MAX)) as u64);
        let response = self
            .client
            .post(self.url("/sql"))
            .json(&SqlReq {
                sql: sql.to_owned(),
                format: None,
                query_id,
                timeout_ms,
                max_output_rows: options.max_output_rows,
                max_output_bytes: options.max_output_bytes,
                idempotency_key: None,
                pagination: Some(SqlPaginationReq {
                    page_size_rows: options.page_size_rows,
                    projection: options.projection.clone(),
                    max_page_bytes: options.max_page_bytes,
                    max_page_tokens: options.max_page_tokens,
                }),
            })
            .send();
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                return Err(self.recover_after_transport_loss(query_id, error.to_string()));
            }
        };
        let page = bounded_blocking_bytes(
            {
                let response = self.check_sql_response(response, query_id)?;
                if let Err(error) = validate_sql_query_id_header(response.headers(), query_id) {
                    return Err(self.recover_after_transport_loss(query_id, error));
                }
                response
            },
            MAX_SQL_RESPONSE_BYTES,
        )
        .and_then(|bytes| {
            strict_json::<RemoteSqlPage>(&bytes, "SQL page").map_err(|error| error.to_string())
        })
        .and_then(|page| validate_remote_sql_page(page, Some(&options)));
        page.map_err(|error| self.recover_after_transport_loss(query_id, error))
    }

    pub fn continue_sql_page(
        &self,
        cursor: &str,
        mut options: RemoteSqlControlOptions,
    ) -> ClientResult<RemoteSqlPage> {
        if cursor.is_empty() || cursor.len() > 2_048 {
            return Err(ClientError::Decode(
                "SQL continuation cursor must contain 1 to 2048 bytes".into(),
            ));
        }
        self.sql_pagination_capabilities()?;
        let query_id = match options.query_id.take() {
            Some(query_id) => query_id,
            None => mongreldb_query::QueryId::random()
                .map_err(|error| ClientError::Transport(error.to_string()))?,
        };
        if options.timeout == Some(std::time::Duration::ZERO) {
            return Err(ClientError::Decode("timeout must be positive".into()));
        }
        let timeout_ms = options
            .timeout
            .map(|timeout| timeout.as_millis().min(u128::from(u64::MAX)) as u64);
        let response = match self
            .client
            .post(self.url("/sql/continue"))
            .json(&serde_json::json!({
                "cursor": cursor,
                "operation_id": query_id,
                "timeout_ms": timeout_ms,
            }))
            .send()
        {
            Ok(response) => response,
            Err(error) => {
                return Err(self.recover_after_transport_loss(query_id, error.to_string()))
            }
        };
        let response = self.check_sql_response(response, query_id)?;
        validate_sql_query_id_header(response.headers(), query_id)
            .map_err(|error| client_serialization_error(Some(query_id), error))?;
        bounded_blocking_bytes(response, MAX_SQL_RESPONSE_BYTES)
            .and_then(|bytes| {
                strict_json::<RemoteSqlPage>(&bytes, "SQL page").map_err(|error| error.to_string())
            })
            .and_then(|page| validate_remote_sql_page(page, None))
            .map_err(|error| client_serialization_error(Some(query_id), error))
    }

    pub fn start_sql(
        &self,
        sql: impl Into<String>,
        mut options: SqlClientOptions,
    ) -> ClientResult<RemoteSqlQueryHandle> {
        self.sql_cancellation_capabilities()?;
        let query_id = match options.query_id {
            Some(query_id) => query_id,
            None => mongreldb_query::QueryId::random()
                .map_err(|error| ClientError::Transport(error.to_string()))?,
        };
        options.query_id = Some(query_id);
        let client = self.clone();
        let cancel_client = self.clone();
        let sql = sql.into();
        let result = std::thread::Builder::new()
            .name(format!("mongreldb-sql-{query_id}"))
            .spawn(move || client.sql_with_options(&sql, options))
            .map_err(|error| ClientError::Transport(error.to_string()))?;
        Ok(RemoteSqlQueryHandle {
            query_id,
            client: cancel_client,
            result,
        })
    }

    pub fn cancel_sql(
        &self,
        query_id: mongreldb_query::QueryId,
    ) -> ClientResult<RemoteCancelOutcome> {
        self.sql_cancellation_capabilities()?;
        let response = self
            .client
            .post(self.url(&format!("/queries/{query_id}/cancel")))
            .send()?;
        let status = response.status();
        if !matches!(
            status,
            reqwest::StatusCode::OK
                | reqwest::StatusCode::ACCEPTED
                | reqwest::StatusCode::CONFLICT
                | reqwest::StatusCode::NOT_FOUND
        ) {
            return match self.check(response) {
                Err(error) => Err(error),
                Ok(_) => Err(ClientError::Decode(
                    "unexpected successful cancellation response".into(),
                )),
            };
        }
        let body: serde_json::Value =
            decode_blocking_json(response, MAX_CONTROL_RESPONSE_BYTES, "cancellation")?;
        decode_cancel_outcome(&body, query_id, status.as_u16())
    }

    pub fn query_status(
        &self,
        query_id: mongreldb_query::QueryId,
    ) -> ClientResult<RemoteQueryStatus> {
        self.sql_cancellation_capabilities()?;
        let response = self
            .client
            .get(self.url(&format!("/queries/{query_id}")))
            .send()?;
        let status = decode_blocking_json(
            self.check_query_status_response(response, query_id)?,
            MAX_CONTROL_RESPONSE_BYTES,
            "query status",
        )?;
        validate_remote_query_status(status, query_id).map_err(ClientError::Decode)
    }

    fn query_status_optional(
        &self,
        query_id: mongreldb_query::QueryId,
        timeout: std::time::Duration,
    ) -> Option<RemoteQueryStatus> {
        let response = self
            .client
            .get(self.url(&format!("/queries/{query_id}")))
            .timeout(timeout)
            .send()
            .ok()?;
        if !response.status().is_success() {
            return None;
        }
        let status =
            decode_blocking_json(response, MAX_CONTROL_RESPONSE_BYTES, "query status").ok()?;
        validate_remote_query_status(status, query_id).ok()
    }

    fn cancel_sql_for_recovery(
        &self,
        query_id: mongreldb_query::QueryId,
        timeout: std::time::Duration,
    ) -> Option<RemoteCancelOutcome> {
        let response = self
            .client
            .post(self.url(&format!("/queries/{query_id}/cancel")))
            .timeout(timeout)
            .send()
            .ok()?;
        let status = response.status();
        if !matches!(
            status,
            reqwest::StatusCode::OK
                | reqwest::StatusCode::ACCEPTED
                | reqwest::StatusCode::CONFLICT
                | reqwest::StatusCode::NOT_FOUND
        ) {
            return None;
        }
        let body =
            decode_blocking_json(response, MAX_CONTROL_RESPONSE_BYTES, "cancellation").ok()?;
        decode_cancel_outcome(&body, query_id, status.as_u16()).ok()
    }

    fn recover_after_transport_loss(
        &self,
        query_id: mongreldb_query::QueryId,
        message: String,
    ) -> ClientError {
        let deadline = std::time::Instant::now() + SQL_RECOVERY_WINDOW;
        let mut status = self.query_status_optional(query_id, SQL_RECOVERY_REQUEST_TIMEOUT);
        if let Some(decisive) = status
            .as_ref()
            .filter(|status| recovery_status_is_decisive(status))
        {
            return recovered_query_error(decisive.clone(), message);
        }
        let cancel_outcome = self.cancel_sql_for_recovery(
            query_id,
            deadline
                .saturating_duration_since(std::time::Instant::now())
                .min(SQL_RECOVERY_REQUEST_TIMEOUT),
        );
        if status.is_none() && cancel_outcome == Some(RemoteCancelOutcome::NotFound) {
            return ClientError::QueryOutcomeUnknown {
                query_id: query_id.to_string(),
                message,
                status: None,
                cancel_outcome,
            };
        }
        while !status.as_ref().is_some_and(recovery_status_is_decisive)
            && std::time::Instant::now() < deadline
        {
            std::thread::sleep(
                deadline
                    .saturating_duration_since(std::time::Instant::now())
                    .min(SQL_RECOVERY_POLL_INTERVAL),
            );
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            status = self
                .query_status_optional(query_id, remaining.min(SQL_RECOVERY_REQUEST_TIMEOUT))
                .or(status);
        }
        if let Some(decisive) = status
            .as_ref()
            .filter(|status| recovery_status_is_decisive(status))
        {
            return recovered_query_error(decisive.clone(), message);
        }
        ClientError::QueryOutcomeUnknown {
            query_id: query_id.to_string(),
            message,
            status: status.map(Box::new),
            cancel_outcome,
        }
    }

    // ── Atomic txn (legacy, raw) ──

    pub fn txn(&self, ops: Vec<TxnOp>) -> ClientResult<()> {
        let response = self.write_response(
            self.client
                .post(self.url("/txn"))
                .json(&serde_json::json!({ "ops": ops }))
                .send(),
            "transaction",
        )?;
        let response: CommittedWriteResponse = decode_write_json(response, "transaction")?;
        validate_committed_write(&response, "transaction")?;
        Ok(())
    }

    // ── Typed Kit surface ──

    /// Fetch one table's schema + constraint metadata (`GET /kit/schema/{t}`).
    pub fn kit_schema(&self, table: &str) -> ClientResult<TableSchemaInfo> {
        let resp = self
            .client
            .get(self.url_segments(&["kit", "schema", table])?)
            .send()?;
        decode_blocking_json(self.check(resp)?, MAX_CONTROL_RESPONSE_BYTES, "Kit schema")
    }

    /// Run a typed atomic batch (`POST /kit/txn`). Constraint violations and
    /// conflicts return [`ClientError::Kit`] with the matching code.
    pub fn kit_txn(&self, req: &KitTxnRequest) -> ClientResult<KitTxnResponse> {
        let resp = self
            .client
            .post(self.url("/kit/txn"))
            .json(req)
            .send()
            .map_err(|error| {
                kit_txn_outcome_unknown(format!(
                    "Kit transaction transport failed before outcome confirmation: {error}"
                ))
            })?;
        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            if matches!(status, 401 | 403) {
                return Err(kit_txn_auth_error(status));
            }
            let body = bounded_blocking_bytes(resp, MAX_SQL_RESPONSE_BYTES).map_err(|error| {
                kit_txn_outcome_unknown(format!(
                    "Kit transaction error response could not be read: {error}"
                ))
            })?;
            return Err(decode_kit_txn_http_error(status, &body, req));
        }
        let status = resp.status().as_u16();
        let body = bounded_blocking_bytes(resp, MAX_SQL_RESPONSE_BYTES).map_err(|error| {
            kit_txn_outcome_unknown(format!(
                "Kit transaction success response could not be read: {error}"
            ))
        })?;
        decode_kit_txn_success(&body, req, status)
    }

    /// Run a native typed query (`POST /kit/query`) returning physical row ids
    /// and typed cells. Conditions intersect in the row-id space; this is the
    /// native counterpart to SQL reads (which hide row ids).
    pub fn kit_query(&self, req: &KitQueryRequest) -> ClientResult<KitQueryResponse> {
        let resp = self.client.post(self.url("/kit/query")).json(req).send()?;
        let response =
            decode_blocking_json(self.check(resp)?, MAX_SQL_RESPONSE_BYTES, "Kit query")?;
        validate_kit_query_response(response, req)
    }

    pub fn kit_retrieve(&self, req: &KitRetrieveRequest) -> ClientResult<KitRetrieveResponse> {
        self.kit_retrieve_with_options(req, &AiExecutionOptions::default())
    }

    pub fn kit_retrieve_with_options(
        &self,
        req: &KitRetrieveRequest,
        options: &AiExecutionOptions,
    ) -> ClientResult<KitRetrieveResponse> {
        let resp = self
            .client
            .post(self.url("/kit/retrieve"))
            .json(&WithAiExecutionOptions {
                request: req,
                options,
            })
            .send()?;
        decode_blocking_json(self.check(resp)?, MAX_SQL_RESPONSE_BYTES, "Kit retrieve")
    }

    pub fn kit_ann_rerank(&self, req: &KitAnnRerankRequest) -> ClientResult<KitAnnRerankResponse> {
        self.kit_ann_rerank_with_options(req, &AiExecutionOptions::default())
    }

    pub fn kit_ann_rerank_with_options(
        &self,
        req: &KitAnnRerankRequest,
        options: &AiExecutionOptions,
    ) -> ClientResult<KitAnnRerankResponse> {
        let resp = self
            .client
            .post(self.url("/kit/ann_rerank"))
            .json(&WithAiExecutionOptions {
                request: req,
                options,
            })
            .send()?;
        decode_blocking_json(self.check(resp)?, MAX_SQL_RESPONSE_BYTES, "Kit ANN rerank")
    }

    pub fn kit_set_similarity(
        &self,
        req: &KitSetSimilarityRequest,
    ) -> ClientResult<KitSetSimilarityResponse> {
        self.kit_set_similarity_with_options(req, &AiExecutionOptions::default())
    }

    pub fn kit_set_similarity_with_options(
        &self,
        req: &KitSetSimilarityRequest,
        options: &AiExecutionOptions,
    ) -> ClientResult<KitSetSimilarityResponse> {
        let resp = self
            .client
            .post(self.url("/kit/set_similarity"))
            .json(&WithAiExecutionOptions {
                request: req,
                options,
            })
            .send()?;
        decode_blocking_json(
            self.check(resp)?,
            MAX_SQL_RESPONSE_BYTES,
            "Kit set similarity",
        )
    }

    pub fn kit_search(&self, req: &KitSearchRequest) -> ClientResult<KitSearchResponse> {
        let resp = self.client.post(self.url("/kit/search")).json(req).send()?;
        decode_blocking_json(self.check(resp)?, MAX_SQL_RESPONSE_BYTES, "Kit search")
    }

    pub fn kit_ai_metrics(&self) -> ClientResult<serde_json::Value> {
        let resp = self.client.get(self.url("/kit/ai/metrics")).send()?;
        decode_blocking_json(
            self.check(resp)?,
            MAX_CONTROL_RESPONSE_BYTES,
            "Kit AI metrics",
        )
    }

    /// Create a table over HTTP (`POST /kit/create_table`) from the complete
    /// Kit schema JSON. The body may include constraints, all six public index
    /// kinds and their options, partial predicates, and column embedding
    /// sources. Returns the assigned table id.
    pub fn kit_create_table(&self, body: &serde_json::Value) -> ClientResult<u64> {
        let response = self.write_response(
            self.client
                .post(self.url("/kit/create_table"))
                .json(body)
                .send(),
            "Kit create table",
        )?;
        let response: TableIdResponse = decode_write_json(response, "Kit create table")?;
        exact_required_u64("table_id", response.table_id, &response.table_id_text)
            .map_err(|error| write_outcome_unknown("Kit create table", error))
    }

    pub fn procedures(&self) -> ClientResult<Vec<mongreldb_core::StoredProcedure>> {
        let resp = self.client.get(self.url("/procedures")).send()?;
        let response: ProceduresResponse = decode_blocking_json(
            self.check(resp)?,
            MAX_CONTROL_RESPONSE_BYTES,
            "procedure list",
        )?;
        Ok(response.procedures)
    }

    pub fn procedure(&self, name: &str) -> ClientResult<mongreldb_core::StoredProcedure> {
        let resp = self
            .client
            .get(self.url_segments(&["procedures", name])?)
            .send()?;
        let response: ProcedureResponse =
            decode_blocking_json(self.check(resp)?, MAX_CONTROL_RESPONSE_BYTES, "procedure")?;
        Ok(response.procedure)
    }

    pub fn create_procedure(
        &self,
        procedure: mongreldb_core::StoredProcedure,
    ) -> ClientResult<mongreldb_core::StoredProcedure> {
        let expected_name = procedure.name.clone();
        let expected_definition = procedure.clone();
        let response = self.write_response(
            self.client
                .post(self.url("/procedures"))
                .json(&ProcedureRequest { procedure })
                .send(),
            "create procedure",
        )?;
        let response: ProcedureResponse = decode_write_json(response, "create procedure")?;
        if response.status != "ok"
            || response.procedure.name != expected_name
            || response.procedure.version == 0
            || response.procedure.created_epoch == 0
            || response.procedure.updated_epoch < response.procedure.created_epoch
            || response.procedure.checksum.is_empty()
            || response.procedure.validate().is_err()
            || !procedure_definition_matches(&response.procedure, &expected_definition)
        {
            return Err(write_outcome_unknown(
                "create procedure",
                "success response does not match the requested procedure",
            ));
        }
        Ok(response.procedure)
    }

    pub fn replace_procedure(
        &self,
        name: &str,
        procedure: mongreldb_core::StoredProcedure,
    ) -> ClientResult<mongreldb_core::StoredProcedure> {
        let expected_definition = procedure.clone();
        let response = self.write_response(
            self.client
                .put(self.url_segments(&["procedures", name])?)
                .json(&ProcedureRequest { procedure })
                .send(),
            "replace procedure",
        )?;
        let response: ProcedureResponse = decode_write_json(response, "replace procedure")?;
        if response.status != "ok"
            || response.procedure.name != name
            || response.procedure.version == 0
            || response.procedure.created_epoch == 0
            || response.procedure.updated_epoch < response.procedure.created_epoch
            || response.procedure.checksum.is_empty()
            || response.procedure.validate().is_err()
            || !procedure_definition_matches(&response.procedure, &expected_definition)
        {
            return Err(write_outcome_unknown(
                "replace procedure",
                "success response does not match the requested procedure",
            ));
        }
        Ok(response.procedure)
    }

    pub fn drop_procedure(&self, name: &str) -> ClientResult<()> {
        let response = self.write_response(
            self.client
                .delete(self.url_segments(&["procedures", name])?)
                .send(),
            "drop procedure",
        )?;
        let response: CommittedWriteResponse = decode_write_json(response, "drop procedure")?;
        validate_committed_write(&response, "drop procedure")?;
        Ok(())
    }

    pub fn call_procedure(
        &self,
        name: &str,
        req: &ProcedureCallRequest,
    ) -> ClientResult<ProcedureCallResponse> {
        let response = self.write_response(
            self.client
                .post(self.url_segments(&["procedures", name, "call"])?)
                .json(req)
                .send(),
            "procedure call",
        )?;
        let response = decode_write_json(response, "procedure call")?;
        validate_procedure_call_response(response)
            .map_err(|error| write_outcome_unknown("procedure call", error))
    }

    pub fn kit_call_procedure(
        &self,
        name: &str,
        req: &ProcedureCallRequest,
    ) -> ClientResult<ProcedureCallResponse> {
        let response = self.write_response(
            self.client
                .post(self.url_segments(&["kit", "procedures", name, "call"])?)
                .json(req)
                .send(),
            "Kit procedure call",
        )?;
        let response = decode_write_json(response, "Kit procedure call")?;
        validate_procedure_call_response(response)
            .map_err(|error| write_outcome_unknown("Kit procedure call", error))
    }

    pub fn triggers(&self) -> ClientResult<Vec<mongreldb_core::StoredTrigger>> {
        let resp = self.client.get(self.url("/triggers")).send()?;
        let response: TriggersResponse = decode_blocking_json(
            self.check(resp)?,
            MAX_CONTROL_RESPONSE_BYTES,
            "trigger list",
        )?;
        Ok(response.triggers)
    }

    pub fn trigger(&self, name: &str) -> ClientResult<mongreldb_core::StoredTrigger> {
        let resp = self
            .client
            .get(self.url_segments(&["triggers", name])?)
            .send()?;
        let response: TriggerResponse =
            decode_blocking_json(self.check(resp)?, MAX_CONTROL_RESPONSE_BYTES, "trigger")?;
        Ok(response.trigger)
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
        let expected_name = trigger.name.clone();
        let expected_definition = trigger.clone();
        let response = self.write_response(
            self.client
                .post(self.url("/triggers"))
                .json(&TriggerRequest {
                    trigger,
                    idempotency_key: idempotency_key.map(Into::into),
                })
                .send(),
            "create trigger",
        )?;
        let response: TriggerResponse = decode_write_json(response, "create trigger")?;
        if response.status.as_deref() != Some("ok")
            || response.trigger.name != expected_name
            || response.trigger.version == 0
            || response.trigger.created_epoch == 0
            || response.trigger.updated_epoch < response.trigger.created_epoch
            || response.trigger.checksum.is_empty()
            || response.trigger.validate().is_err()
            || !trigger_definition_matches(&response.trigger, &expected_definition)
        {
            return Err(write_outcome_unknown(
                "create trigger",
                "success response does not match the requested trigger",
            ));
        }
        Ok(response.trigger)
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
        let expected_definition = trigger.clone();
        let response = self.write_response(
            self.client
                .put(self.url_segments(&["triggers", name])?)
                .json(&TriggerRequest {
                    trigger,
                    idempotency_key: idempotency_key.map(Into::into),
                })
                .send(),
            "replace trigger",
        )?;
        let response: TriggerResponse = decode_write_json(response, "replace trigger")?;
        if response.status.as_deref() != Some("ok")
            || response.trigger.name != name
            || response.trigger.version == 0
            || response.trigger.created_epoch == 0
            || response.trigger.updated_epoch < response.trigger.created_epoch
            || response.trigger.checksum.is_empty()
            || response.trigger.validate().is_err()
            || !trigger_definition_matches(&response.trigger, &expected_definition)
        {
            return Err(write_outcome_unknown(
                "replace trigger",
                "success response does not match the requested trigger",
            ));
        }
        Ok(response.trigger)
    }

    pub fn drop_trigger(&self, name: &str) -> ClientResult<()> {
        self.drop_trigger_with_idempotency_key(name, None::<String>)
    }

    pub fn drop_trigger_with_idempotency_key(
        &self,
        name: &str,
        idempotency_key: Option<impl Into<String>>,
    ) -> ClientResult<()> {
        let mut request = self.client.delete(self.url_segments(&["triggers", name])?);
        if let Some(idempotency_key) = idempotency_key {
            request = request.header("Idempotency-Key", idempotency_key.into());
        }
        let response = self.write_response(request.send(), "drop trigger")?;
        let response: TriggerDropResponse = decode_write_json(response, "drop trigger")?;
        validate_trigger_drop_response(&response, name)?;
        Ok(())
    }
}

impl AsyncMongrelClient {
    pub fn builder(url: impl AsRef<str>) -> AsyncMongrelClientBuilder {
        let base_url = sanitized_base_url(url.as_ref());
        AsyncMongrelClientBuilder {
            invalid_base_url: base_url.is_none(),
            base_url: base_url.unwrap_or_default(),
            authorization: None,
            invalid_authorization: false,
            connect_timeout: None,
            request_timeout: None,
            pool_idle_timeout: None,
        }
    }

    pub fn new(url: &str) -> ClientResult<Self> {
        Self::builder(url).build()
    }

    pub fn with_options(url: impl AsRef<str>, options: RemoteOptions) -> ClientResult<Self> {
        let mut builder = Self::builder(url);
        if let Some(timeout) = options.transport_timeout {
            builder = builder.request_timeout(timeout);
        }
        builder = match options.auth {
            Some(RemoteAuth::Bearer(token)) => builder.bearer_token(token.expose_secret()),
            Some(RemoteAuth::Basic { username, password }) => {
                builder.basic_auth(username, password.expose_secret())
            }
            None => builder,
        };
        builder.build()
    }

    pub fn try_with_bearer_token(mut self, token: impl AsRef<str>) -> ClientResult<Self> {
        self.client = async_http_client(Some(bearer_header(token.as_ref())?), self.transport)?;
        Ok(self)
    }

    pub fn with_bearer_token(self, token: impl AsRef<str>) -> ClientResult<Self> {
        self.try_with_bearer_token(token)
    }

    pub fn try_with_basic_auth(
        mut self,
        username: impl AsRef<str>,
        password: impl AsRef<str>,
    ) -> ClientResult<Self> {
        self.client = async_http_client(
            Some(basic_header(username.as_ref(), password.as_ref())?),
            self.transport,
        )?;
        Ok(self)
    }

    pub fn with_basic_auth(
        self,
        username: impl AsRef<str>,
        password: impl AsRef<str>,
    ) -> ClientResult<Self> {
        self.try_with_basic_auth(username, password)
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    async fn check(&self, response: reqwest::Response) -> ClientResult<reqwest::Response> {
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let status_u16 = status.as_u16();
        let body = bounded_async_bytes(response, MAX_CONTROL_RESPONSE_BYTES)
            .await
            .map_err(|error| {
                ClientError::Decode(format!("invalid HTTP error response: {error}"))
            })?;
        Err(decode_http_error(status_u16, &body))
    }

    async fn check_sql_response(
        &self,
        response: reqwest::Response,
        query_id: mongreldb_query::QueryId,
    ) -> ClientResult<reqwest::Response> {
        let status = response.status();
        let pre_handler_auth = matches!(
            status,
            reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN
        );
        if status.is_success() {
            return Ok(response);
        }
        let header_error = (!pre_handler_auth)
            .then(|| validate_sql_query_id_header(response.headers(), query_id).err())
            .flatten();
        let status = status.as_u16();
        let body = match bounded_async_bytes(response, MAX_CONTROL_RESPONSE_BYTES).await {
            Ok(body) => body,
            Err(error) => {
                if pre_handler_auth {
                    return Err(ClientError::Http {
                        status,
                        body: format!("unreadable authentication error response: {error}"),
                    });
                }
                return Err(self
                    .recover_after_transport_loss(query_id, error.to_string())
                    .await);
            }
        };
        if pre_handler_auth {
            return Err(decode_http_error(status, &body));
        }
        match decode_sql_http_error(status, &body, query_id) {
            Ok(error) if client_error_proves_commit(&error) => Err(error),
            Ok(error) if header_error.is_none() => Err(error),
            Ok(_) => Err(self
                .recover_after_transport_loss(
                    query_id,
                    header_error.unwrap_or_else(|| "SQL response identity is invalid".into()),
                )
                .await),
            Err(error) => Err(self.recover_after_transport_loss(query_id, error).await),
        }
    }

    async fn check_query_status_response(
        &self,
        response: reqwest::Response,
        query_id: mongreldb_query::QueryId,
    ) -> ClientResult<reqwest::Response> {
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let status = status.as_u16();
        let body = bounded_async_bytes(response, MAX_CONTROL_RESPONSE_BYTES)
            .await
            .map_err(ClientError::Decode)?;
        if matches!(status, 401 | 403) {
            return Err(decode_http_error(status, &body));
        }
        match decode_sql_http_error(status, &body, query_id) {
            Ok(error) => Err(error),
            Err(error) => Err(ClientError::Decode(error)),
        }
    }

    pub async fn health(&self) -> ClientResult<String> {
        let response = self.client.get(self.url("/health")).send().await?;
        let bytes = bounded_async_bytes(self.check(response).await?, MAX_CONTROL_RESPONSE_BYTES)
            .await
            .map_err(ClientError::Transport)?;
        String::from_utf8(bytes)
            .map_err(|_| ClientError::Decode("invalid health response: non-UTF-8 body".into()))
    }

    pub async fn capabilities(&self) -> ClientResult<ServerCapabilities> {
        let response = self.client.get(self.url("/capabilities")).send().await?;
        decode_async_json(
            self.check(response).await?,
            MAX_CONTROL_RESPONSE_BYTES,
            "capabilities",
        )
        .await
    }

    pub async fn sql_cancellation_capabilities(&self) -> ClientResult<SqlCancellationCapabilities> {
        let capabilities = self.capabilities().await?.sql_cancellation;
        if capabilities.version < 2
            || !capabilities.client_query_ids
            || !capabilities.cancel_endpoint
            || !capabilities.query_status
            || !capabilities.pre_registration_cancel
        {
            return Err(capability_unsupported(
                "server does not support SQL cancellation capability version 2",
            ));
        }
        Ok(capabilities)
    }

    pub async fn sql_idempotency_capabilities(&self) -> ClientResult<SqlIdempotencyCapabilities> {
        let capabilities =
            self.capabilities().await?.sql_idempotency.ok_or_else(|| {
                capability_unsupported("server does not advertise SQL idempotency")
            })?;
        if capabilities.version < 1
            || !capabilities.durable_pre_execution_intent
            || !capabilities.replay_committed_receipt
            || !capabilities.indeterminate_never_reexecutes
        {
            return Err(capability_unsupported(
                "server does not support safe SQL idempotency capability version 1",
            ));
        }
        Ok(capabilities)
    }

    pub async fn sql_pagination_capabilities(&self) -> ClientResult<SqlPaginationCapabilities> {
        let capabilities =
            self.capabilities().await?.sql_pagination.ok_or_else(|| {
                capability_unsupported("server does not advertise SQL pagination")
            })?;
        if capabilities.version < 1
            || capabilities.continuation_endpoint != "/sql/continue"
            || !capabilities.retained_snapshot
            || !capabilities.projection_required
            || !capabilities.byte_and_token_hints
        {
            return Err(capability_unsupported(
                "server does not support SQL pagination capability version 1",
            ));
        }
        Ok(capabilities)
    }

    pub async fn sql(&self, sql: &str) -> ClientResult<Vec<RecordBatch>> {
        self.sql_with_options(sql, SqlClientOptions::default())
            .await
    }

    pub async fn sql_with_options(
        &self,
        sql: &str,
        mut options: SqlClientOptions,
    ) -> ClientResult<Vec<RecordBatch>> {
        if options.query_id.is_some() || options.timeout.is_some() {
            self.sql_cancellation_capabilities().await?;
        }
        let query_id = match options.query_id.take() {
            Some(query_id) => query_id,
            None => mongreldb_query::QueryId::random()
                .map_err(|error| ClientError::Transport(error.to_string()))?,
        };
        let timeout_ms = options
            .timeout
            .map(|timeout| timeout.as_millis().min(u128::from(u64::MAX)) as u64);
        let response = self
            .client
            .post(self.url("/sql"))
            .json(&SqlReq {
                sql: sql.to_string(),
                format: Some("arrow"),
                query_id,
                timeout_ms,
                max_output_rows: None,
                max_output_bytes: None,
                idempotency_key: None,
                pagination: None,
            })
            .send()
            .await;
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                return Err(self
                    .recover_after_transport_loss(query_id, error.to_string())
                    .await);
            }
        };
        let response = self.check_sql_response(response, query_id).await?;
        if let Err(error) = validate_sql_query_id_header(response.headers(), query_id) {
            return Err(self.recover_after_transport_loss(query_id, error).await);
        }
        let bytes = match bounded_async_bytes(response, MAX_SQL_RESPONSE_BYTES).await {
            Ok(bytes) => bytes,
            Err(error) => {
                return Err(self
                    .recover_after_transport_loss(query_id, error.to_string())
                    .await)
            }
        };
        match read_arrow_ipc(&bytes) {
            Ok(batches) => Ok(batches),
            Err(error) => Err(self
                .recover_after_transport_loss(query_id, error.to_string())
                .await),
        }
    }

    pub async fn sql_write_idempotent(
        &self,
        sql: &str,
        idempotency_key: impl Into<String>,
    ) -> ClientResult<RemoteSqlReceipt> {
        self.sql_write_idempotent_with_options(sql, idempotency_key, SqlClientOptions::default())
            .await
    }

    pub async fn sql_write_idempotent_with_options(
        &self,
        sql: &str,
        idempotency_key: impl Into<String>,
        mut options: SqlClientOptions,
    ) -> ClientResult<RemoteSqlReceipt> {
        let idempotency_key = idempotency_key.into();
        if idempotency_key.is_empty() || idempotency_key.len() > 256 {
            return Err(ClientError::Decode(
                "SQL idempotency key must contain 1 to 256 bytes".into(),
            ));
        }
        self.sql_idempotency_capabilities().await?;
        let query_id = match options.query_id.take() {
            Some(query_id) => query_id,
            None => mongreldb_query::QueryId::random()
                .map_err(|error| ClientError::Transport(error.to_string()))?,
        };
        let result = self
            .sql_write_idempotent_once(sql, &idempotency_key, &options, query_id, None)
            .await;
        if result.as_ref().is_err_and(|error| error.replay) {
            self.sql_idempotency_capabilities().await?;
            return self
                .sql_write_idempotent_once(
                    sql,
                    &idempotency_key,
                    &options,
                    fresh_query_id(query_id)?,
                    Some(query_id),
                )
                .await
                .map_err(|error| error.error);
        }
        result.map_err(|error| error.error)
    }

    async fn sql_write_idempotent_once(
        &self,
        sql: &str,
        idempotency_key: &str,
        options: &SqlClientOptions,
        query_id: mongreldb_query::QueryId,
        expected_original_query_id: Option<mongreldb_query::QueryId>,
    ) -> Result<RemoteSqlReceipt, IdempotentAttemptError> {
        let timeout_ms = options
            .timeout
            .map(|timeout| timeout.as_millis().min(u128::from(u64::MAX)) as u64);
        let response = self
            .client
            .post(self.url("/sql"))
            .json(&SqlReq {
                sql: sql.to_owned(),
                format: None,
                query_id,
                timeout_ms,
                max_output_rows: None,
                max_output_bytes: None,
                idempotency_key: Some(idempotency_key.to_owned()),
                pagination: None,
            })
            .send()
            .await;
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                return Err(self
                    .idempotent_attempt_loss(query_id, error.to_string())
                    .await);
            }
        };
        let response = self
            .check_sql_response(response, query_id)
            .await
            .map_err(IdempotentAttemptError::final_error)?;
        let header_error = validate_sql_query_id_header(response.headers(), query_id).err();
        let bytes = match bounded_async_bytes(response, MAX_CONTROL_RESPONSE_BYTES).await {
            Ok(bytes) => bytes,
            Err(error) => return Err(self.idempotent_attempt_loss(query_id, error).await),
        };
        match decode_remote_sql_receipt(&bytes, query_id, expected_original_query_id) {
            Ok(receipt) if header_error.is_none() => Ok(receipt),
            Ok(_) => {
                let proof = strict_json_value(&bytes).ok().and_then(|value| {
                    sql_receipt_commit_proof(&value, query_id, expected_original_query_id)
                });
                match proof {
                    Some(proof) => Err(IdempotentAttemptError::final_error(
                        committed_sql_receipt_decode_error(
                            query_id,
                            proof,
                            header_error
                                .clone()
                                .unwrap_or_else(|| "SQL response identity is invalid".into()),
                        ),
                    )),
                    None => Err(self
                        .idempotent_attempt_loss(
                            query_id,
                            header_error
                                .unwrap_or_else(|| "SQL response identity is invalid".into()),
                        )
                        .await),
                }
            }
            Err(SqlReceiptDecodeError::KnownCommit(error)) => {
                Err(IdempotentAttemptError::final_error(error))
            }
            Err(SqlReceiptDecodeError::Unknown(error)) => {
                Err(self.idempotent_attempt_loss(query_id, error).await)
            }
        }
    }

    async fn idempotent_attempt_loss(
        &self,
        query_id: mongreldb_query::QueryId,
        message: String,
    ) -> IdempotentAttemptError {
        let missing = match self
            .client
            .get(self.url(&format!("/queries/{query_id}")))
            .timeout(SQL_RECOVERY_REQUEST_TIMEOUT)
            .send()
            .await
        {
            Ok(response) if response.status() == reqwest::StatusCode::NOT_FOUND => {
                bounded_async_bytes(response, MAX_CONTROL_RESPONSE_BYTES)
                    .await
                    .ok()
                    .is_some_and(|body| is_exact_query_not_found_response(&body, query_id))
            }
            _ => false,
        };
        if missing {
            return IdempotentAttemptError {
                error: ClientError::QueryOutcomeUnknown {
                    query_id: query_id.to_string(),
                    message,
                    status: None,
                    cancel_outcome: None,
                },
                replay: true,
            };
        }
        IdempotentAttemptError::final_error(
            self.recover_after_transport_loss(query_id, message).await,
        )
    }

    pub async fn sql_page(
        &self,
        sql: &str,
        mut options: SqlPageOptions,
    ) -> ClientResult<RemoteSqlPage> {
        validate_sql_page_options(&options)?;
        self.sql_pagination_capabilities().await?;
        let query_id = match options.query_id.take() {
            Some(query_id) => query_id,
            None => mongreldb_query::QueryId::random()
                .map_err(|error| ClientError::Transport(error.to_string()))?,
        };
        let timeout_ms = options
            .timeout
            .map(|timeout| timeout.as_millis().min(u128::from(u64::MAX)) as u64);
        let response = self
            .client
            .post(self.url("/sql"))
            .json(&SqlReq {
                sql: sql.to_owned(),
                format: None,
                query_id,
                timeout_ms,
                max_output_rows: options.max_output_rows,
                max_output_bytes: options.max_output_bytes,
                idempotency_key: None,
                pagination: Some(SqlPaginationReq {
                    page_size_rows: options.page_size_rows,
                    projection: options.projection.clone(),
                    max_page_bytes: options.max_page_bytes,
                    max_page_tokens: options.max_page_tokens,
                }),
            })
            .send()
            .await;
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                return Err(self
                    .recover_after_transport_loss(query_id, error.to_string())
                    .await);
            }
        };
        let response = self.check_sql_response(response, query_id).await?;
        if let Err(error) = validate_sql_query_id_header(response.headers(), query_id) {
            return Err(self.recover_after_transport_loss(query_id, error).await);
        }
        let page = bounded_async_bytes(response, MAX_SQL_RESPONSE_BYTES)
            .await
            .and_then(|bytes| {
                strict_json::<RemoteSqlPage>(&bytes, "SQL page").map_err(|error| error.to_string())
            })
            .and_then(|page| validate_remote_sql_page(page, Some(&options)));
        match page {
            Ok(page) => Ok(page),
            Err(error) => Err(self.recover_after_transport_loss(query_id, error).await),
        }
    }

    pub async fn continue_sql_page(
        &self,
        cursor: &str,
        mut options: RemoteSqlControlOptions,
    ) -> ClientResult<RemoteSqlPage> {
        if cursor.is_empty() || cursor.len() > 2_048 {
            return Err(ClientError::Decode(
                "SQL continuation cursor must contain 1 to 2048 bytes".into(),
            ));
        }
        self.sql_pagination_capabilities().await?;
        let query_id = match options.query_id.take() {
            Some(query_id) => query_id,
            None => mongreldb_query::QueryId::random()
                .map_err(|error| ClientError::Transport(error.to_string()))?,
        };
        if options.timeout == Some(std::time::Duration::ZERO) {
            return Err(ClientError::Decode("timeout must be positive".into()));
        }
        let timeout_ms = options
            .timeout
            .map(|timeout| timeout.as_millis().min(u128::from(u64::MAX)) as u64);
        let response = self
            .client
            .post(self.url("/sql/continue"))
            .json(&serde_json::json!({
                "cursor": cursor,
                "operation_id": query_id,
                "timeout_ms": timeout_ms,
            }))
            .send()
            .await
            .map_err(|error| ClientError::Transport(error.to_string()))?;
        let response = self.check_sql_response(response, query_id).await?;
        validate_sql_query_id_header(response.headers(), query_id)
            .map_err(|error| client_serialization_error(Some(query_id), error))?;
        bounded_async_bytes(response, MAX_SQL_RESPONSE_BYTES)
            .await
            .and_then(|bytes| {
                strict_json::<RemoteSqlPage>(&bytes, "SQL page").map_err(|error| error.to_string())
            })
            .and_then(|page| validate_remote_sql_page(page, None))
            .map_err(|error| client_serialization_error(Some(query_id), error))
    }

    pub async fn cancel_sql(
        &self,
        query_id: mongreldb_query::QueryId,
    ) -> ClientResult<RemoteCancelOutcome> {
        self.sql_cancellation_capabilities().await?;
        let response = self
            .client
            .post(self.url(&format!("/queries/{query_id}/cancel")))
            .send()
            .await?;
        let status = response.status();
        if !matches!(
            status,
            reqwest::StatusCode::OK
                | reqwest::StatusCode::ACCEPTED
                | reqwest::StatusCode::CONFLICT
                | reqwest::StatusCode::NOT_FOUND
        ) {
            return match self.check(response).await {
                Err(error) => Err(error),
                Ok(_) => Err(ClientError::Decode(
                    "unexpected successful cancellation response".into(),
                )),
            };
        }
        let body: serde_json::Value =
            decode_async_json(response, MAX_CONTROL_RESPONSE_BYTES, "cancellation").await?;
        decode_cancel_outcome(&body, query_id, status.as_u16())
    }

    pub async fn query_status(
        &self,
        query_id: mongreldb_query::QueryId,
    ) -> ClientResult<RemoteQueryStatus> {
        self.sql_cancellation_capabilities().await?;
        let response = self
            .client
            .get(self.url(&format!("/queries/{query_id}")))
            .send()
            .await?;
        let status = decode_async_json(
            self.check_query_status_response(response, query_id).await?,
            MAX_CONTROL_RESPONSE_BYTES,
            "query status",
        )
        .await?;
        validate_remote_query_status(status, query_id).map_err(ClientError::Decode)
    }

    async fn query_status_optional(
        &self,
        query_id: mongreldb_query::QueryId,
        timeout: std::time::Duration,
    ) -> Option<RemoteQueryStatus> {
        let response = self
            .client
            .get(self.url(&format!("/queries/{query_id}")))
            .timeout(timeout)
            .send()
            .await
            .ok()?;
        if !response.status().is_success() {
            return None;
        }
        let status = decode_async_json(response, MAX_CONTROL_RESPONSE_BYTES, "query status")
            .await
            .ok()?;
        validate_remote_query_status(status, query_id).ok()
    }

    async fn cancel_sql_for_recovery(
        &self,
        query_id: mongreldb_query::QueryId,
        timeout: std::time::Duration,
    ) -> Option<RemoteCancelOutcome> {
        let response = self
            .client
            .post(self.url(&format!("/queries/{query_id}/cancel")))
            .timeout(timeout)
            .send()
            .await
            .ok()?;
        let status = response.status();
        if !matches!(
            status,
            reqwest::StatusCode::OK
                | reqwest::StatusCode::ACCEPTED
                | reqwest::StatusCode::CONFLICT
                | reqwest::StatusCode::NOT_FOUND
        ) {
            return None;
        }
        let body = decode_async_json(response, MAX_CONTROL_RESPONSE_BYTES, "cancellation")
            .await
            .ok()?;
        decode_cancel_outcome(&body, query_id, status.as_u16()).ok()
    }

    async fn recover_after_transport_loss(
        &self,
        query_id: mongreldb_query::QueryId,
        message: String,
    ) -> ClientError {
        let deadline = tokio::time::Instant::now() + SQL_RECOVERY_WINDOW;
        let mut status = self
            .query_status_optional(query_id, SQL_RECOVERY_REQUEST_TIMEOUT)
            .await;
        if let Some(decisive) = status
            .as_ref()
            .filter(|status| recovery_status_is_decisive(status))
        {
            return recovered_query_error(decisive.clone(), message);
        }
        let cancel_outcome = self
            .cancel_sql_for_recovery(
                query_id,
                deadline
                    .saturating_duration_since(tokio::time::Instant::now())
                    .min(SQL_RECOVERY_REQUEST_TIMEOUT),
            )
            .await;
        if status.is_none() && cancel_outcome == Some(RemoteCancelOutcome::NotFound) {
            return ClientError::QueryOutcomeUnknown {
                query_id: query_id.to_string(),
                message,
                status: None,
                cancel_outcome,
            };
        }
        while !status.as_ref().is_some_and(recovery_status_is_decisive)
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(
                deadline
                    .saturating_duration_since(tokio::time::Instant::now())
                    .min(SQL_RECOVERY_POLL_INTERVAL),
            )
            .await;
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            status = self
                .query_status_optional(query_id, remaining.min(SQL_RECOVERY_REQUEST_TIMEOUT))
                .await
                .or(status);
        }
        if let Some(decisive) = status
            .as_ref()
            .filter(|status| recovery_status_is_decisive(status))
        {
            return recovered_query_error(decisive.clone(), message);
        }
        ClientError::QueryOutcomeUnknown {
            query_id: query_id.to_string(),
            message,
            status: status.map(Box::new),
            cancel_outcome,
        }
    }

    /// Fetch one table's schema + constraint metadata (`GET /kit/schema/{t}`).
    pub async fn kit_schema(&self, table: &str) -> ClientResult<TableSchemaInfo> {
        let response = self
            .client
            .get(url_with_segments(
                &self.base_url,
                &["kit", "schema", table],
            )?)
            .send()
            .await?;
        decode_async_json(
            self.check(response).await?,
            MAX_CONTROL_RESPONSE_BYTES,
            "Kit schema",
        )
        .await
    }

    /// Run a typed atomic batch (`POST /kit/txn`).
    pub async fn kit_txn(&self, req: &KitTxnRequest) -> ClientResult<KitTxnResponse> {
        let response = self
            .client
            .post(self.url("/kit/txn"))
            .json(req)
            .send()
            .await
            .map_err(|error| {
                kit_txn_outcome_unknown(format!(
                    "Kit transaction transport failed before outcome confirmation: {error}"
                ))
            })?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            if matches!(status, 401 | 403) {
                return Err(kit_txn_auth_error(status));
            }
            let body = bounded_async_bytes(response, MAX_SQL_RESPONSE_BYTES)
                .await
                .map_err(|error| {
                    kit_txn_outcome_unknown(format!(
                        "Kit transaction error response could not be read: {error}"
                    ))
                })?;
            return Err(decode_kit_txn_http_error(status, &body, req));
        }
        let status = response.status().as_u16();
        let body = bounded_async_bytes(response, MAX_SQL_RESPONSE_BYTES)
            .await
            .map_err(|error| {
                kit_txn_outcome_unknown(format!(
                    "Kit transaction success response could not be read: {error}"
                ))
            })?;
        decode_kit_txn_success(&body, req, status)
    }

    /// Run a native typed query (`POST /kit/query`).
    pub async fn kit_query(&self, req: &KitQueryRequest) -> ClientResult<KitQueryResponse> {
        let response = self
            .client
            .post(self.url("/kit/query"))
            .json(req)
            .send()
            .await?;
        let response = decode_async_json(
            self.check(response).await?,
            MAX_SQL_RESPONSE_BYTES,
            "Kit query",
        )
        .await?;
        validate_kit_query_response(response, req)
    }

    pub async fn kit_retrieve(
        &self,
        req: &KitRetrieveRequest,
    ) -> ClientResult<KitRetrieveResponse> {
        self.kit_retrieve_with_options(req, &AiExecutionOptions::default())
            .await
    }

    pub async fn kit_retrieve_with_options(
        &self,
        req: &KitRetrieveRequest,
        options: &AiExecutionOptions,
    ) -> ClientResult<KitRetrieveResponse> {
        let response = self
            .client
            .post(self.url("/kit/retrieve"))
            .json(&WithAiExecutionOptions {
                request: req,
                options,
            })
            .send()
            .await?;
        decode_async_json(
            self.check(response).await?,
            MAX_SQL_RESPONSE_BYTES,
            "Kit retrieve",
        )
        .await
    }

    pub async fn kit_ann_rerank(
        &self,
        req: &KitAnnRerankRequest,
    ) -> ClientResult<KitAnnRerankResponse> {
        self.kit_ann_rerank_with_options(req, &AiExecutionOptions::default())
            .await
    }

    pub async fn kit_ann_rerank_with_options(
        &self,
        req: &KitAnnRerankRequest,
        options: &AiExecutionOptions,
    ) -> ClientResult<KitAnnRerankResponse> {
        let response = self
            .client
            .post(self.url("/kit/ann_rerank"))
            .json(&WithAiExecutionOptions {
                request: req,
                options,
            })
            .send()
            .await?;
        decode_async_json(
            self.check(response).await?,
            MAX_SQL_RESPONSE_BYTES,
            "Kit ANN rerank",
        )
        .await
    }

    pub async fn kit_set_similarity(
        &self,
        req: &KitSetSimilarityRequest,
    ) -> ClientResult<KitSetSimilarityResponse> {
        self.kit_set_similarity_with_options(req, &AiExecutionOptions::default())
            .await
    }

    pub async fn kit_set_similarity_with_options(
        &self,
        req: &KitSetSimilarityRequest,
        options: &AiExecutionOptions,
    ) -> ClientResult<KitSetSimilarityResponse> {
        let response = self
            .client
            .post(self.url("/kit/set_similarity"))
            .json(&WithAiExecutionOptions {
                request: req,
                options,
            })
            .send()
            .await?;
        decode_async_json(
            self.check(response).await?,
            MAX_SQL_RESPONSE_BYTES,
            "Kit set similarity",
        )
        .await
    }

    pub async fn kit_search(&self, req: &KitSearchRequest) -> ClientResult<KitSearchResponse> {
        let response = self
            .client
            .post(self.url("/kit/search"))
            .json(req)
            .send()
            .await?;
        decode_async_json(
            self.check(response).await?,
            MAX_SQL_RESPONSE_BYTES,
            "Kit search",
        )
        .await
    }

    pub async fn kit_ai_metrics(&self) -> ClientResult<serde_json::Value> {
        let response = self.client.get(self.url("/kit/ai/metrics")).send().await?;
        decode_async_json(
            self.check(response).await?,
            MAX_CONTROL_RESPONSE_BYTES,
            "Kit AI metrics",
        )
        .await
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

fn value_to_json(value: &mongreldb_core::Value) -> ClientResult<serde_json::Value> {
    use mongreldb_core::Value;

    Ok(match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(value) => serde_json::Value::Bool(*value),
        Value::Int64(value) => serde_json::Value::Number((*value).into()),
        Value::Float64(value) => serde_json::Number::from_f64(*value)
            .map(serde_json::Value::Number)
            .ok_or_else(|| ClientError::Decode("legacy put rejects non-finite floats".into()))?,
        Value::Bytes(value) => tagged_hex_value("bytes", value),
        Value::Embedding(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|value| {
                    serde_json::Number::from_f64(f64::from(*value))
                        .map(serde_json::Value::Number)
                        .ok_or_else(|| {
                            ClientError::Decode(
                                "legacy put rejects non-finite embedding values".into(),
                            )
                        })
                })
                .collect::<ClientResult<Vec<_>>>()?,
        ),
        Value::GeneratedEmbedding(value) => serde_json::Value::Array(
            value
                .vector
                .iter()
                .map(|value| {
                    serde_json::Number::from_f64(f64::from(*value))
                        .map(serde_json::Value::Number)
                        .ok_or_else(|| {
                            ClientError::Decode(
                                "legacy put rejects non-finite embedding values".into(),
                            )
                        })
                })
                .collect::<ClientResult<Vec<_>>>()?,
        ),
        Value::Decimal(value) => serde_json::json!({
            "$mongreldb_type": "decimal",
            "unscaled": value.to_string(),
        }),
        Value::Interval {
            months,
            days,
            nanos,
        } => serde_json::json!({
            "$mongreldb_type": "interval",
            "months": months.to_string(),
            "days": days.to_string(),
            "nanos": nanos.to_string(),
        }),
        Value::Uuid(value) => tagged_hex_value("uuid", value),
        Value::Json(value) => {
            std::str::from_utf8(value)
                .map_err(|_| ClientError::Decode("legacy put JSON is not UTF-8".into()))?;
            serde_json::from_slice::<serde_json::Value>(value).map_err(|error| {
                ClientError::Decode(format!("legacy put JSON is invalid: {error}"))
            })?;
            tagged_hex_value("json", value)
        }
    })
}

fn tagged_hex_value(kind: &str, bytes: &[u8]) -> serde_json::Value {
    serde_json::json!({
        "$mongreldb_type": kind,
        "hex": hex_bytes(bytes),
    })
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
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
/// let mut follower = ReplicationFollower::new("http://leader:8453", "/local/copy")?;
/// follower.sync(); // fetches and applies all new records since last sync
/// # Ok::<(), mongreldb_client::ClientError>(())
/// ```
pub struct ReplicationFollower {
    leader_url: String,
    local_path: std::path::PathBuf,
    client: reqwest::blocking::Client,
    last_epoch: u64,
    bearer_token: Option<SecretString>,
    basic_auth: Option<(String, SecretString)>,
    local_passphrase: Option<SecretString>,
    local_credentials: Option<(String, SecretString)>,
}

impl std::fmt::Debug for ReplicationFollower {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReplicationFollower")
            .field("leader_url", &self.leader_url)
            .field("local_path", &self.local_path)
            .field("last_epoch", &self.last_epoch)
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field(
                "basic_auth",
                &self
                    .basic_auth
                    .as_ref()
                    .map(|(username, _)| (username, "[REDACTED]")),
            )
            .field(
                "local_passphrase",
                &self.local_passphrase.as_ref().map(|_| "[REDACTED]"),
            )
            .field(
                "local_credentials",
                &self
                    .local_credentials
                    .as_ref()
                    .map(|(username, _)| (username, "[REDACTED]")),
            )
            .finish()
    }
}

impl ReplicationFollower {
    /// Create a follower. `leader_url` is the daemon base URL; `local_path` is
    /// the local database directory to sync into.
    pub fn new(leader_url: &str, local_path: impl AsRef<std::path::Path>) -> ClientResult<Self> {
        let leader_url = sanitized_base_url(leader_url).ok_or_else(|| {
            ClientError::Transport(
                "invalid leader URL: expected http(s) URL without credentials, query, or fragment"
                    .into(),
            )
        })?;
        let local_path = local_path.as_ref().to_path_buf();
        let last_epoch = mongreldb_core::replica_epoch(&local_path).unwrap_or(0);
        Ok(Self {
            leader_url,
            local_path,
            client: reqwest::blocking::Client::new(),
            last_epoch,
            bearer_token: None,
            basic_auth: None,
            local_passphrase: None,
            local_credentials: None,
        })
    }

    pub fn with_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.bearer_token = Some(token.into().into());
        self
    }

    pub fn with_basic_auth(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.basic_auth = Some((username.into(), password.into().into()));
        self
    }

    pub fn with_local_encryption_passphrase(mut self, passphrase: impl Into<String>) -> Self {
        self.local_passphrase = Some(passphrase.into().into());
        self
    }

    pub fn with_local_credentials(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.local_credentials = Some((username.into(), password.into().into()));
        self
    }

    /// Bootstrap when needed, fetch complete committed transactions, append
    /// them durably, recover the local database, then advance the watermark.
    pub fn sync(&mut self) -> Result<usize, String> {
        let legacy_replica_needs_snapshot = mongreldb_core::is_replica(&self.local_path)
            && !self.local_path.join("_meta/replication_source_id").exists();
        if !self.local_path.join("CATALOG").exists() || legacy_replica_needs_snapshot {
            self.ensure_bootstrap_destination_is_owned_or_empty()?;
            self.bootstrap()?;
        } else if !mongreldb_core::is_replica(&self.local_path) {
            return Err(format!(
                "refusing to overwrite non-replica database at {}",
                self.local_path.display()
            ));
        }

        let mut resp = self.fetch_wal()?;
        if resp.status() == reqwest::StatusCode::CONFLICT {
            self.validate_snapshot_required_response(resp)?;
            self.bootstrap()?;
            resp = self.fetch_wal()?;
        }
        if !resp.status().is_success() {
            return Err(format!("leader returned {}", resp.status()));
        }
        let from_epoch = replication_u64_header(&resp, "x-mongreldb-from-epoch")?;
        let leader_epoch = replication_u64_header(&resp, "x-mongreldb-current-epoch")?;
        let source_id = replication_digest_header(&resp, "x-mongreldb-source-id")?;
        let commit_count = replication_u64_header(&resp, "x-mongreldb-commit-count")?;
        let records_sha256 = replication_digest_header(&resp, "x-mongreldb-records-sha256")?;
        let earliest_epoch = resp
            .headers()
            .get("x-mongreldb-earliest-epoch")
            .map(|value| {
                value
                    .to_str()
                    .map_err(|error| format!("invalid x-mongreldb-earliest-epoch: {error}"))?
                    .parse::<u64>()
                    .map_err(|error| format!("invalid x-mongreldb-earliest-epoch: {error}"))
            })
            .transpose()?;
        let body = bounded_blocking_bytes(resp, MAX_REPLICATION_WAL_RESPONSE_BYTES)
            .map_err(|error| format!("failed to read WAL response: {error}"))?;
        let body = std::str::from_utf8(&body)
            .map_err(|_| "invalid WAL response: body is not UTF-8".to_owned())?;
        let mut records = Vec::new();
        for line in body.lines() {
            if line.trim().is_empty() {
                continue;
            }
            records.push(
                strict_roundtrip_json::<mongreldb_core::wal::Record>(
                    line.as_bytes(),
                    "WAL record from leader",
                )
                .map_err(|error| error.to_string())?,
            );
        }
        let record_count = records.len();
        let batch = mongreldb_core::ReplicationBatch::from_wire(
            source_id,
            from_epoch,
            leader_epoch,
            earliest_epoch,
            commit_count,
            records_sha256,
            records,
        );

        let local = self.open_local()?;
        let applied_epoch = local
            .append_replication_batch(&batch)
            .map_err(|error| error.to_string())?;
        if record_count == 0 {
            return Ok(0);
        }
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
        Ok(record_count)
    }

    pub fn bootstrap(&mut self) -> Result<(), String> {
        self.ensure_bootstrap_destination_is_owned_or_empty()?;
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
        let response_source_id = replication_digest_header(&response, "x-mongreldb-source-id")?;
        let response_epoch = replication_u64_header(&response, "x-mongreldb-current-epoch")?;
        let bytes = bounded_blocking_bytes(response, MAX_REPLICATION_SNAPSHOT_BYTES)
            .map_err(|error| format!("failed to read replication snapshot: {error}"))?;
        let snapshot = mongreldb_core::ReplicationSnapshot::decode(&bytes)
            .map_err(|error| error.to_string())?;
        if snapshot.source_id() != response_source_id {
            return Err("replication snapshot source header does not match snapshot".into());
        }
        if snapshot.epoch() != response_epoch {
            return Err("replication snapshot epoch header does not match snapshot".into());
        }
        let mut minimum_epoch = self
            .last_epoch
            .max(mongreldb_core::replica_epoch(&self.local_path).unwrap_or(0));
        if self.local_path.join("CATALOG").exists() {
            let local = self.open_local()?;
            minimum_epoch = minimum_epoch.max(local.visible_epoch().0);
        }
        if snapshot.epoch() < minimum_epoch {
            return Err(format!(
                "refusing replication snapshot epoch {} older than local epoch {minimum_epoch}",
                snapshot.epoch()
            ));
        }
        snapshot
            .install_validated(&self.local_path, |stage| {
                let database = self
                    .open_path(stage)
                    .map_err(mongreldb_core::MongrelError::Other)?;
                drop(database);
                Ok(())
            })
            .map_err(|error| error.to_string())?;
        self.last_epoch = snapshot.epoch();
        Ok(())
    }

    fn ensure_bootstrap_destination_is_owned_or_empty(&self) -> Result<(), String> {
        let metadata = match std::fs::symlink_metadata(&self.local_path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(format!(
                    "failed to inspect replication destination {}: {error}",
                    self.local_path.display()
                ))
            }
        };
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(format!(
                "replication destination is not a directory: {}",
                self.local_path.display()
            ));
        }
        if mongreldb_core::is_replica(&self.local_path) {
            return Ok(());
        }
        let mut entries = std::fs::read_dir(&self.local_path).map_err(|error| {
            format!(
                "failed to inspect replication destination {}: {error}",
                self.local_path.display()
            )
        })?;
        if entries
            .next()
            .transpose()
            .map_err(|error| error.to_string())?
            .is_some()
        {
            return Err(format!(
                "refusing to overwrite non-replica directory at {}",
                self.local_path.display()
            ));
        }
        drop(entries);
        std::fs::remove_dir(&self.local_path).map_err(|error| {
            format!(
                "failed to claim empty replication destination {}: {error}",
                self.local_path.display()
            )
        })?;
        Ok(())
    }

    fn validate_snapshot_required_response(
        &self,
        response: reqwest::blocking::Response,
    ) -> Result<(), String> {
        let from_epoch = replication_u64_header(&response, "x-mongreldb-from-epoch")?;
        let current_epoch = replication_u64_header(&response, "x-mongreldb-current-epoch")?;
        let source_id = replication_digest_header(&response, "x-mongreldb-source-id")?;
        let status = response
            .headers()
            .get("x-mongreldb-replication-status")
            .and_then(|value| value.to_str().ok());
        if status != Some("snapshot-required") {
            return Err("leader returned an unrecognized replication conflict".into());
        }
        if self.local_path.join("CATALOG").exists() {
            let local_source = mongreldb_core::replica_source_id(&self.local_path)
                .map_err(|error| error.to_string())?;
            if source_id != local_source {
                return Err("replication conflict came from a different source".into());
            }
        }
        replication_u64_header(&response, "x-mongreldb-commit-count")?;
        replication_digest_header(&response, "x-mongreldb-records-sha256")?;
        if from_epoch != self.last_epoch || current_epoch < self.last_epoch {
            return Err(format!(
                "invalid replication snapshot requirement: from epoch {from_epoch}, current epoch {current_epoch}, local epoch {}",
                self.last_epoch
            ));
        }
        let body = bounded_blocking_bytes(response, MAX_CONTROL_RESPONSE_BYTES)
            .map_err(|error| format!("failed to read replication conflict response: {error}"))?;
        if body != b"replication snapshot required: WAL retention gap or spilled run" {
            return Err("leader returned an unrecognized replication conflict".into());
        }
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
            request.bearer_auth(token.expose_secret())
        } else if let Some((username, password)) = &self.basic_auth {
            request.basic_auth(username, Some(password.expose_secret()))
        } else {
            request
        }
    }

    fn open_local(&self) -> Result<mongreldb_core::Database, String> {
        self.open_path(&self.local_path)
    }

    fn open_path(&self, path: &std::path::Path) -> Result<mongreldb_core::Database, String> {
        let result = match (&self.local_passphrase, &self.local_credentials) {
            (Some(passphrase), Some((username, password))) => {
                mongreldb_core::Database::open_encrypted_with_credentials(
                    path,
                    passphrase.expose_secret(),
                    username,
                    password.expose_secret(),
                )
            }
            (Some(passphrase), None) => {
                mongreldb_core::Database::open_encrypted(path, passphrase.expose_secret())
            }
            (None, Some((username, password))) => mongreldb_core::Database::open_with_credentials(
                path,
                username,
                password.expose_secret(),
            ),
            (None, None) => mongreldb_core::Database::open(path),
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

fn replication_u64_header(
    response: &reqwest::blocking::Response,
    name: &str,
) -> Result<u64, String> {
    response
        .headers()
        .get(name)
        .ok_or_else(|| format!("leader response missing {name}"))?
        .to_str()
        .map_err(|error| format!("invalid {name}: {error}"))?
        .parse()
        .map_err(|error| format!("invalid {name}: {error}"))
}

fn replication_digest_header(
    response: &reqwest::blocking::Response,
    name: &str,
) -> Result<[u8; 32], String> {
    let value = response
        .headers()
        .get(name)
        .ok_or_else(|| format!("leader response missing {name}"))?
        .to_str()
        .map_err(|error| format!("invalid {name}: {error}"))?;
    if value.len() != 64 {
        return Err(format!(
            "invalid {name}: expected 64 hexadecimal characters"
        ));
    }
    let mut digest = [0_u8; 32];
    for (index, byte) in digest.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|error| format!("invalid {name}: {error}"))?;
    }
    Ok(digest)
}
