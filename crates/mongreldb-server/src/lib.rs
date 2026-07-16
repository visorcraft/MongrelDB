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
//!   POST   /tables/{name}/commit       → { "epoch": N, "epoch_text": "N" }
//!   POST   /sql                       → Arrow IPC bytes
//!   POST   /txn                       → exact atomic commit receipt
//!
//! Usage: `mongreldb-server <db_dir> [port]`

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::{Path, State};
use axum::http::header;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Json;
use mongreldb_core::schema::{Schema, TypeId};
use mongreldb_core::{CancellationReason, Database, Value};
use mongreldb_query::{
    CancelOutcome, ExternalTableModule, ManagedQueryBatches, MongrelSession, QueryId,
    RegisteredQueryGuard, RegisteredSqlQuery, SqlQueryOptions, SqlQueryPhase, SqlQueryRegistry,
    SqlStreamCompletion,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::Digest;
use zeroize::Zeroizing;

mod audit;
mod kit;
mod metrics;
mod pre_cancel;
mod procedure;
mod sessions;
mod sql_idempotency;
mod sql_pages;
mod trigger;

pub use sessions::{spawn_session_reaper, SessionStore};

fn client_closed_request_status() -> StatusCode {
    StatusCode::from_u16(499).unwrap_or(StatusCode::BAD_REQUEST)
}

fn cancellation_checkpoint_error(query: &RegisteredSqlQuery) -> mongreldb_query::MongrelQueryError {
    query.checkpoint().err().unwrap_or_else(|| {
        mongreldb_query::MongrelQueryError::InvalidQueryState(
            "cancellation notification observed without a terminal checkpoint".into(),
        )
    })
}

/// Map an engine error to the appropriate HTTP status code for defense-in-depth.
/// Auth errors get 401/403; everything else stays 500. This ensures that even
/// after the HTTP auth middleware lets a request through, the storage layer's
/// permission checks surface as the right status (not a generic 500).
fn status_for_error(e: &mongreldb_core::MongrelError) -> StatusCode {
    use mongreldb_core::MongrelError;
    match e {
        MongrelError::AuthRequired | MongrelError::InvalidCredentials { .. } => {
            StatusCode::UNAUTHORIZED
        }
        MongrelError::AuthNotRequired => StatusCode::BAD_REQUEST,
        MongrelError::PermissionDenied { .. } => StatusCode::FORBIDDEN,
        MongrelError::InvalidArgument(_) => StatusCode::CONFLICT,
        MongrelError::Conflict(_) => StatusCode::CONFLICT,
        MongrelError::ReadOnlyReplica => StatusCode::CONFLICT,
        MongrelError::NotFound(_) => StatusCode::NOT_FOUND,
        MongrelError::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
        MongrelError::WorkBudgetExceeded => StatusCode::TOO_MANY_REQUESTS,
        MongrelError::Cancelled => client_closed_request_status(),
        MongrelError::CursorStale(_) => StatusCode::CONFLICT,
        MongrelError::CursorExpired => StatusCode::GONE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Map a query-layer error (which wraps engine errors via `Core(...)`) to the
/// appropriate HTTP status code.
fn status_for_query_error(e: &mongreldb_query::MongrelQueryError) -> StatusCode {
    use mongreldb_query::MongrelQueryError;
    match e {
        MongrelQueryError::Core(core) => status_for_error(core),
        MongrelQueryError::DeadlineExceeded { .. } => StatusCode::GATEWAY_TIMEOUT,
        MongrelQueryError::QueryCancelled { .. } => client_closed_request_status(),
        MongrelQueryError::QueryIdConflict { .. } => StatusCode::CONFLICT,
        MongrelQueryError::QueryRegistryFull => StatusCode::SERVICE_UNAVAILABLE,
        MongrelQueryError::ResultLimitExceeded { .. } => StatusCode::PAYLOAD_TOO_LARGE,
        MongrelQueryError::TransactionAborted => StatusCode::CONFLICT,
        MongrelQueryError::NoSqlTransaction | MongrelQueryError::SavepointNotFound { .. } => {
            StatusCode::CONFLICT
        }
        MongrelQueryError::CommitOutcome { .. } => StatusCode::CONFLICT,
        MongrelQueryError::OutcomeUnknown { .. } => StatusCode::CONFLICT,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Extractor that pulls the authenticated [`mongreldb_core::Principal`] (if the
/// auth middleware injected one) from request extensions without erroring when
/// absent (e.g. token-authenticated requests carry no `Principal`).
struct OptionalPrincipal(Option<mongreldb_core::Principal>);

impl<S> axum::extract::FromRequestParts<S> for OptionalPrincipal
where
    S: Send + Sync,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        Ok(OptionalPrincipal(
            parts.extensions.get::<mongreldb_core::Principal>().cloned(),
        ))
    }
}

struct AppState {
    db: Arc<Database>,
    idem: kit::IdempotencyStore,
    external_modules: Vec<Arc<dyn ExternalTableModule>>,
    auth_token: Option<String>,
    /// When true, authenticate via catalog users (HTTP Basic auth).
    user_auth: bool,
    /// Daemon-wide Prometheus-style counters, shared by all handlers.
    metrics: Arc<metrics::Metrics>,
    /// `/sql` requests slower than this are logged as slow queries.
    slow_query_threshold: std::time::Duration,
    /// Bounded security audit log (auth + DDL/privilege events).
    audit: Arc<audit::AuditLog>,
    /// Token-keyed pool of live sessions for cross-request interactive
    /// transactions (`X-Session-ID` on `/sql`).
    sessions: Arc<sessions::SessionStore>,
    /// Bounds CPU-heavy AI work submitted to Tokio's blocking pool.
    ai_semaphore: Arc<tokio::sync::Semaphore>,
    /// Process-wide SQL registry. Cancellation never takes a session lock.
    query_registry: Arc<SqlQueryRegistry>,
    /// Serializes registration with cancel-before-registration bookkeeping.
    query_lifecycle: Mutex<()>,
    /// Short-lived, owner/session-bound cancellations received before SQL.
    pre_cancellations: pre_cancel::PreCancelStore,
    /// Owner- and request-bound durable SQL write receipts.
    sql_idempotency: Arc<sql_idempotency::SqlIdempotencyStore>,
    /// Bounded projected results retained for stable SQL continuation cursors.
    sql_pages: sql_pages::SqlPageStore,
    /// Admission control for ordinary SQL, separate from AI workers.
    sql_semaphore: Arc<tokio::sync::Semaphore>,
    sql_default_timeout: std::time::Duration,
    sql_max_timeout: std::time::Duration,
    sql_cancel_grace: std::time::Duration,
    sql_max_output_bytes: usize,
    sql_max_output_rows: usize,
    accepting_sql: Arc<AtomicBool>,
    /// Lazily generated process-local HMAC key. Restart invalidates cursors.
    cursor_mac_key: CursorMacKey,
}

#[derive(Default)]
struct CursorMacKey {
    key: Mutex<Option<[u8; 32]>>,
}

impl CursorMacKey {
    fn get(&self) -> mongreldb_core::Result<[u8; 32]> {
        let mut key = match self.key.lock() {
            Ok(key) => key,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(key) = *key {
            return Ok(key);
        }
        let mut generated = [0u8; 32];
        mongreldb_core::encryption::fill_random(&mut generated)?;
        *key = Some(generated);
        Ok(generated)
    }
}

/// Handle retained by the daemon to coordinate graceful SQL shutdown without
/// coupling signal handling to Axum's router internals.
#[derive(Clone)]
pub struct ServerControl {
    query_registry: Arc<SqlQueryRegistry>,
    sessions: Arc<sessions::SessionStore>,
    accepting_sql: Arc<AtomicBool>,
    cancel_grace: std::time::Duration,
    metrics: Arc<metrics::Metrics>,
}

impl ServerControl {
    pub async fn shutdown(&self) -> usize {
        self.accepting_sql.store(false, Ordering::Release);
        self.query_registry
            .cancel_all(CancellationReason::ServerShutdown);
        let deadline = tokio::time::Instant::now() + self.cancel_grace;
        while self.query_registry.active_count() > 0 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        self.sessions.close_all();
        let stuck_queries = self.query_registry.active_statuses();
        for status in &stuck_queries {
            eprintln!(
                "[sql-cancel-stuck] query_id={} phase={}",
                status.query_id,
                query_phase_name(status.phase)
            );
        }
        let stuck = stuck_queries.len();
        self.metrics.add_sql_stuck_after_cancel(stuck);
        stuck
    }
}

pub fn build_app(db: Arc<Database>) -> axum::Router {
    build_app_with_config(
        db,
        std::iter::empty::<Arc<dyn ExternalTableModule>>(),
        None,
        None,
    )
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
/// Sessions are enabled with a default capacity (256) and idle timeout (300 s).
pub fn build_app_full(
    db: Arc<Database>,
    external_modules: impl IntoIterator<Item = Arc<dyn ExternalTableModule>>,
    auth_token: Option<String>,
    max_connections: Option<usize>,
    user_auth: bool,
) -> axum::Router {
    let sessions = Arc::new(sessions::SessionStore::new(
        default_max_sessions(),
        default_session_idle_timeout(),
    ));
    build_app_with_sessions(
        db,
        external_modules,
        auth_token,
        max_connections,
        user_auth,
        sessions,
    )
}

/// Build the daemon router with an explicit, externally-owned session store.
/// The caller (typically `main`) keeps the `Arc<SessionStore>` so it can spawn
/// the idle reaper against the same map the handlers use.
pub fn build_app_with_sessions(
    db: Arc<Database>,
    external_modules: impl IntoIterator<Item = Arc<dyn ExternalTableModule>>,
    auth_token: Option<String>,
    max_connections: Option<usize>,
    user_auth: bool,
    sessions: Arc<sessions::SessionStore>,
) -> axum::Router {
    build_app_with_sessions_and_control(
        db,
        external_modules,
        auth_token,
        max_connections,
        user_auth,
        sessions,
    )
    .0
}

/// Build the daemon router and return a handle for graceful query shutdown.
pub fn build_app_with_sessions_and_control(
    db: Arc<Database>,
    external_modules: impl IntoIterator<Item = Arc<dyn ExternalTableModule>>,
    auth_token: Option<String>,
    max_connections: Option<usize>,
    user_auth: bool,
    sessions: Arc<sessions::SessionStore>,
) -> (axum::Router, ServerControl) {
    db.set_replication_wal_retention_segments(default_replication_wal_segments());
    if let Err(error) = db.set_history_retention_epochs(default_history_retention_epochs()) {
        eprintln!("[history] failed to configure retention: {error}");
    }
    let max_active_queries = default_sql_max_active_queries();
    let query_registry = Arc::new(SqlQueryRegistry::new(
        max_active_queries,
        max_active_queries.saturating_mul(2),
        2 * 1024 * 1024,
        default_sql_finished_query_ttl(),
    ));
    let accepting_sql = Arc::new(AtomicBool::new(true));
    let sql_cancel_grace = default_sql_cancel_grace();
    let sql_max_timeout = default_sql_max_timeout();
    let sql_default_timeout = default_sql_default_timeout().min(sql_max_timeout);
    let metrics = Arc::new(metrics::Metrics::default());
    let (idempotency_root, idempotency_integrity) =
        match sql_idempotency::IdempotencyIntegrity::for_database(&db) {
            Ok((root, integrity)) => (root, Some(integrity)),
            Err(error) => {
                eprintln!("[idempotency] durable integrity key unavailable: {error}");
                (db.durable_root(), None)
            }
        };
    let sql_idempotency = Arc::new(sql_idempotency::SqlIdempotencyStore::new_with_integrity(
        Arc::clone(&idempotency_root),
        idempotency_integrity.clone(),
        default_sql_idempotency_ttl(),
        default_sql_idempotency_max_entries(),
    ));
    let server_control = ServerControl {
        query_registry: Arc::clone(&query_registry),
        sessions: Arc::clone(&sessions),
        accepting_sql: Arc::clone(&accepting_sql),
        cancel_grace: sql_cancel_grace,
        metrics: Arc::clone(&metrics),
    };
    let state = Arc::new(AppState {
        idem: kit::IdempotencyStore::new_with_integrity(
            idempotency_root,
            idempotency_integrity,
            default_sql_idempotency_ttl(),
            default_sql_idempotency_max_entries(),
        ),
        db,
        external_modules: external_modules.into_iter().collect(),
        auth_token,
        user_auth,
        metrics,
        slow_query_threshold: metrics::slow_query_threshold(),
        audit: Arc::new(audit::AuditLog::new(8192)),
        sessions,
        ai_semaphore: Arc::new(tokio::sync::Semaphore::new(default_ai_max_concurrent())),
        query_registry,
        query_lifecycle: Mutex::new(()),
        pre_cancellations: pre_cancel::PreCancelStore::new(
            default_sql_pre_cancel_ttl(),
            default_sql_pre_cancel_max_entries(),
            default_sql_pre_cancel_max_bytes(),
            default_sql_pre_cancel_max_entries_per_owner(),
            default_sql_pre_cancel_rate_window(),
            default_sql_pre_cancel_rate_per_owner(),
        ),
        sql_idempotency,
        sql_pages: sql_pages::SqlPageStore::new(
            default_sql_page_ttl(),
            default_sql_page_max_entries(),
            default_sql_page_max_bytes(),
            default_sql_page_max_entries_per_owner(),
        ),
        sql_semaphore: Arc::new(tokio::sync::Semaphore::new(default_sql_max_concurrent())),
        sql_default_timeout,
        sql_max_timeout,
        sql_cancel_grace,
        sql_max_output_bytes: default_sql_max_output_bytes(),
        sql_max_output_rows: default_sql_max_output_rows(),
        accepting_sql,
        cursor_mac_key: CursorMacKey::default(),
    });
    let router = axum::Router::new()
        .route("/health", get(health))
        .route("/capabilities", get(capabilities))
        .route(
            "/history/retention",
            get(history_retention).put(set_history_retention),
        )
        .route("/metrics", get(metrics_handler))
        .route("/audit", get(audit_handler))
        .route("/tables", get(list_tables).post(create_table))
        .route("/tables/{name}", axum::routing::delete(drop_table))
        .route("/tables/{name}/put", post(put_row))
        .route("/tables/{name}/count", get(count))
        .route("/tables/{name}/commit", post(commit))
        .route("/sql", post(sql))
        .route("/sql/continue", post(continue_sql_page))
        .route("/queries/{query_id}", get(query_status))
        .route("/queries/{query_id}/cancel", post(cancel_query))
        .route("/txn", post(txn))
        .route("/sessions", post(create_session))
        .route("/sessions/{id}", axum::routing::delete(close_session))
        .route("/sessions/{id}/prepare", post(prepare_statement))
        .route("/sessions/{id}/execute", post(execute_statement))
        .route(
            "/sessions/{id}/statements/{name}",
            axum::routing::delete(deallocate_statement),
        )
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
        .route("/kit/retrieve", post(kit::kit_retrieve))
        .route("/kit/ann_rerank", post(kit::kit_ann_rerank))
        .route("/kit/ai/metrics", get(kit::kit_ai_metrics))
        .route("/kit/set_similarity", post(kit::kit_set_similarity))
        .route("/kit/search", post(kit::kit_search))
        .route("/kit/create_table", post(kit::kit_create_table))
        .route("/kit/procedures/{name}/call", post(procedure::kit_call))
        .route("/compact", post(compact_all))
        .route("/tables/{name}/compact", post(compact_table))
        .route("/wal/stream", get(wal_stream))
        .route("/replication/snapshot", get(replication_snapshot))
        .route("/events", get(events_stream))
        .with_state(state.clone());

    // A credential-enforced database must never expose the authenticated
    // daemon handle when the caller forgot to configure an HTTP auth mode.
    // With neither mode enabled the middleware rejects every route.
    let router = if state.auth_token.is_some() || state.user_auth || state.db.require_auth_enabled()
    {
        router.layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ))
    } else {
        router
    };

    // Apply connection limit if configured.
    let router = if let Some(max) = max_connections {
        router.layer(tower::limit::ConcurrencyLimitLayer::new(max))
    } else {
        router
    };
    (router, server_control)
}

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

    // Track the attempted identity + failure reason so EVERY 401 path emits
    // exactly one `login.fail` audit event (missing, malformed, wrong token,
    // wrong password are all logged — no unauthenticated probe goes unrecorded).
    let mut attempted = String::new();
    let mut fail_reason = "no credentials provided".to_string();

    // Mode 1: Token auth (Bearer).
    if let Some(token) = &state.auth_token {
        if let Some(provided) = header.strip_prefix("Bearer ") {
            attempted = "token".to_string();
            if provided == token {
                state
                    .audit
                    .record("token", "login.ok", "bearer token accepted");
                return Ok(next.run(req).await);
            }
            fail_reason = "invalid bearer token".to_string();
        }
    }

    // Mode 2: User auth (Basic).
    if state.user_auth {
        if let Some(encoded) = header.strip_prefix("Basic ") {
            if let Ok(decoded) = base64_decode(encoded) {
                let decoded = Zeroizing::new(decoded);
                if let Ok(creds) = std::str::from_utf8(&decoded) {
                    if let Some((username, password)) = creds.split_once(':') {
                        attempted = username.to_string();
                        if let Ok(Some(principal)) =
                            state.db.authenticate_principal(username, password)
                        {
                            let username = principal.username.clone();
                            drop(decoded);
                            state
                                .audit
                                .record(&username, "login.ok", "basic credentials accepted");
                            req.extensions_mut().insert(principal);
                            return Ok(next.run(req).await);
                        }
                        fail_reason = "invalid basic credentials".to_string();
                    } else {
                        fail_reason = "malformed basic credentials (no ':')".to_string();
                    }
                } else {
                    fail_reason = "malformed basic credentials (non-utf8)".to_string();
                }
            } else {
                fail_reason = "malformed basic credentials (bad base64)".to_string();
            }
        }
    }

    let who = if attempted.is_empty() {
        "anonymous"
    } else {
        attempted.as_str()
    };
    state.audit.record(who, "login.fail", fail_reason);
    Err(axum::http::StatusCode::UNAUTHORIZED)
}

/// Minimal Base64 decoder (no extra dep).
fn base64_decode(input: &str) -> Result<Vec<u8>, ()> {
    const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let input: Vec<u8> = input
        .bytes()
        .filter(|&b| b != b'\n' && b != b'\r' && b != b' ')
        .collect();
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

/// `GET /wal/stream?since=<epoch>` — return complete committed WAL
/// transactions after the follower epoch. A 409 response means retained WAL
/// cannot close the gap and the follower must fetch `/replication/snapshot`.
/// Records are newline-delimited JSON.
/// newline-delimited JSON for replication followers. Each line is a JSON
/// object `{ "seq": N, "txn_id": N, "op": {...} }`.
async fn wal_stream(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    axum::extract::Query(params): axum::extract::Query<WalStreamParams>,
) -> Result<Response, StatusCode> {
    state
        .db
        .require_for(
            request_principal(&state, &principal).as_ref(),
            &mongreldb_core::Permission::Admin,
        )
        .map_err(|error| status_for_error(&error))?;
    let since = params.since.unwrap_or(0);
    let db = Arc::clone(&state.db);
    let batch = tokio::task::spawn_blocking(move || db.replication_batch_since(since))
        .await
        .map_err(|_e| StatusCode::INTERNAL_SERVER_ERROR)?;
    let batch = batch.map_err(|e| {
        eprintln!("wal_stream error: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    if batch.requires_snapshot {
        let mut response = (
            StatusCode::CONFLICT,
            "replication snapshot required: WAL retention gap or spilled run",
        )
            .into_response();
        response.headers_mut().insert(
            "x-mongreldb-replication-status",
            "snapshot-required"
                .parse()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
        );
        set_replication_headers(
            &mut response,
            &batch.source_id,
            batch.from_epoch,
            batch.current_epoch,
            batch.earliest_epoch,
            batch.commit_count,
            &batch.records_sha256,
        )?;
        return Ok(response);
    }
    let mut body = String::new();
    for record in &batch.records {
        let json = serde_json::to_string(record).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        body.push_str(&json);
        body.push('\n');
    }
    let mut response = (
        [
            (header::CONTENT_TYPE, "application/x-ndjson".to_string()),
            (header::CACHE_CONTROL, "no-cache".to_string()),
        ],
        body,
    )
        .into_response();
    set_replication_headers(
        &mut response,
        &batch.source_id,
        batch.from_epoch,
        batch.current_epoch,
        batch.earliest_epoch,
        batch.commit_count,
        &batch.records_sha256,
    )?;
    Ok(response)
}

async fn replication_snapshot(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
) -> Response {
    if let Err(error) = state.db.require_for(
        request_principal(&state, &principal).as_ref(),
        &mongreldb_core::Permission::Admin,
    ) {
        return (status_for_error(&error), error.to_string()).into_response();
    }
    let db = Arc::clone(&state.db);
    let snapshot = match tokio::task::spawn_blocking(move || db.replication_snapshot()).await {
        Ok(Ok(snapshot)) => snapshot,
        Ok(Err(error)) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
        }
        Err(error) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
        }
    };
    let epoch = snapshot.epoch();
    // ponytail: bootstrap buffers one image; add framed file streaming when
    // real snapshot sizes make this memory ceiling measurable.
    match snapshot.encode() {
        Ok(bytes) => {
            let mut response =
                ([(header::CONTENT_TYPE, "application/octet-stream")], bytes).into_response();
            let Ok(value) = epoch.to_string().parse() else {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "invalid replication epoch response header",
                )
                    .into_response();
            };
            response
                .headers_mut()
                .insert("x-mongreldb-current-epoch", value);
            let source_id = snapshot
                .source_id()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            let Ok(source_id) = source_id.parse() else {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "invalid replication source response header",
                )
                    .into_response();
            };
            response
                .headers_mut()
                .insert("x-mongreldb-source-id", source_id);
            response
        }
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

fn set_replication_headers(
    response: &mut Response,
    source_id: &[u8; 32],
    from: u64,
    current: u64,
    earliest: Option<u64>,
    commit_count: u64,
    records_sha256: &[u8; 32],
) -> Result<(), StatusCode> {
    let source_id = source_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    response.headers_mut().insert(
        "x-mongreldb-source-id",
        source_id
            .parse()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
    );
    response.headers_mut().insert(
        "x-mongreldb-from-epoch",
        from.to_string()
            .parse()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
    );
    response.headers_mut().insert(
        "x-mongreldb-current-epoch",
        current
            .to_string()
            .parse()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
    );
    response.headers_mut().insert(
        "x-mongreldb-commit-count",
        commit_count
            .to_string()
            .parse()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
    );
    let digest = records_sha256
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    response.headers_mut().insert(
        "x-mongreldb-records-sha256",
        digest
            .parse()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
    );
    if let Some(earliest) = earliest {
        response.headers_mut().insert(
            "x-mongreldb-earliest-epoch",
            earliest
                .to_string()
                .parse()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
        );
    }
    Ok(())
}

#[derive(serde::Deserialize)]
struct WalStreamParams {
    since: Option<u64>,
}

/// `GET /events` — long-lived SSE for durable WAL-backed row changes plus
/// ephemeral SQL NOTIFY messages. `Last-Event-ID` resumes from a stable
/// `<commit_epoch>:<operation_index>` id. A retention gap returns 409 before
/// the stream starts, or a terminal `gap` SSE if the client falls behind.
async fn events_stream(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    headers: axum::http::HeaderMap,
) -> Result<Response, StatusCode> {
    use axum::response::sse::{Event, KeepAlive, Sse};
    use futures::Stream;
    use std::collections::VecDeque;
    use std::convert::Infallible;

    state
        .db
        .require_for(
            request_principal(&state, &principal).as_ref(),
            &mongreldb_core::Permission::Admin,
        )
        .map_err(|error| status_for_error(&error))?;

    struct State {
        db: Arc<Database>,
        receiver: tokio::sync::broadcast::Receiver<mongreldb_core::ChangeEvent>,
        change_wake: tokio::sync::broadcast::Receiver<()>,
        interval: tokio::time::Interval,
        pending: VecDeque<mongreldb_core::ChangeEvent>,
        last_id: Option<String>,
        poll_now: bool,
        done: bool,
    }

    fn event(change: mongreldb_core::ChangeEvent) -> Event {
        let id = change.id.clone();
        let kind = if change.op == "notify" {
            "notify"
        } else {
            "change"
        };
        let mut event = Event::default().event(kind).data(
            serde_json::to_string(&change)
                .unwrap_or_else(|error| format!(r#"{{"error":"{error}"}}"#)),
        );
        if let Some(id) = id {
            event = event.id(id);
        }
        event
    }

    let last_id = match headers.get("last-event-id") {
        Some(value) => Some(
            value
                .to_str()
                .map_err(|_| StatusCode::BAD_REQUEST)?
                .to_owned(),
        ),
        None => None,
    };
    let receiver = state.db.subscribe_changes();
    let change_wake = state.db.subscribe_change_commits();
    let db = Arc::clone(&state.db);
    let resume = last_id.clone();
    let initial = tokio::task::spawn_blocking(move || db.change_events_since(resume.as_deref()))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map_err(|error| match error {
            mongreldb_core::MongrelError::InvalidArgument(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        })?;
    if initial.gap {
        return Ok((
            StatusCode::CONFLICT,
            Json(json!({
                "error": "cdc retention gap",
                "earliest_epoch": initial.earliest_epoch,
                "current_epoch": initial.current_epoch,
            })),
        )
            .into_response());
    }

    let stream: std::pin::Pin<Box<dyn Stream<Item = Result<Event, Infallible>> + Send>> = Box::pin(
        futures::stream::unfold(
            State {
                db: Arc::clone(&state.db),
                receiver,
                change_wake,
                interval: tokio::time::interval(std::time::Duration::from_millis(250)),
                pending: initial.events.into(),
                last_id,
                poll_now: false,
                done: false,
            },
            |mut stream| async move {
                if stream.done {
                    return None;
                }
                loop {
                    if stream.poll_now {
                        stream.poll_now = false;
                        let db = Arc::clone(&stream.db);
                        let last_id = stream.last_id.clone();
                        match tokio::task::spawn_blocking(move || {
                            db.change_events_since(last_id.as_deref())
                        })
                        .await
                        {
                            Ok(Ok(batch)) if batch.gap => {
                                stream.done = true;
                                let gap = Event::default().event("gap").data(
                                    json!({
                                        "error": "cdc retention gap",
                                        "earliest_epoch": batch.earliest_epoch,
                                        "current_epoch": batch.current_epoch,
                                    })
                                    .to_string(),
                                );
                                return Some((Ok(gap), stream));
                            }
                            Ok(Ok(batch)) => stream.pending.extend(batch.events),
                            Ok(Err(error)) => {
                                stream.done = true;
                                return Some((
                                    Ok(Event::default().event("error").data(error.to_string())),
                                    stream,
                                ));
                            }
                            Err(error) => {
                                stream.done = true;
                                return Some((
                                    Ok(Event::default().event("error").data(error.to_string())),
                                    stream,
                                ));
                            }
                        }
                    }
                    if let Some(change) = stream.pending.pop_front() {
                        if let Some(id) = &change.id {
                            stream.last_id = Some(id.clone());
                        }
                        return Some((Ok(event(change)), stream));
                    }
                    tokio::select! {
                        received = stream.receiver.recv() => {
                            match received {
                                Ok(change) => return Some((Ok(event(change)), stream)),
                                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {},
                                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                    return None;
                                }
                            }
                        }
                        received = stream.change_wake.recv() => {
                            match received {
                                Ok(()) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                    stream.poll_now = true;
                                }
                                Err(tokio::sync::broadcast::error::RecvError::Closed) => {}
                            }
                        }
                        _ = stream.interval.tick() => {
                            stream.poll_now = true;
                        }
                    }
                }
            },
        ),
    );

    Ok(Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(std::time::Duration::from_secs(15))
                .text("keep-alive"),
        )
        .into_response())
}

/// Launch the §5.9 background auto-compaction sweep (run-count cost trigger).
/// One OS thread, sleeping `interval` between sweeps; each tick locks each
/// table individually and calls `Table::maybe_compact`. Best-effort: a
/// compaction error is logged and never aborts the sweep.
pub fn spawn_auto_compactor(db: Arc<Database>) {
    if let Err(error) = std::thread::Builder::new()
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
    {
        eprintln!("[auto-compact] failed to start background thread: {error}");
    }
}

async fn health() -> StatusCode {
    StatusCode::OK
}

#[derive(Debug, Serialize)]
struct SqlCancellationCapabilities {
    version: u8,
    client_query_ids: bool,
    cancel_endpoint: bool,
    query_status: bool,
    pre_registration_cancel: bool,
    stream_disconnect_cancels: bool,
}

#[derive(Debug, Serialize)]
struct SqlIdempotencyCapabilities {
    version: u8,
    durable_pre_execution_intent: bool,
    replay_committed_receipt: bool,
    indeterminate_never_reexecutes: bool,
}

#[derive(Debug, Serialize)]
struct SqlPaginationCapabilities {
    version: u8,
    continuation_endpoint: &'static str,
    retained_snapshot: bool,
    projection_required: bool,
    byte_and_token_hints: bool,
}

#[derive(Debug, Serialize)]
struct CapabilitiesResponse {
    sql_cancellation: SqlCancellationCapabilities,
    sql_idempotency: SqlIdempotencyCapabilities,
    sql_pagination: SqlPaginationCapabilities,
}

async fn capabilities() -> Json<CapabilitiesResponse> {
    Json(CapabilitiesResponse {
        sql_cancellation: SqlCancellationCapabilities {
            version: 2,
            client_query_ids: true,
            cancel_endpoint: true,
            query_status: true,
            pre_registration_cancel: true,
            stream_disconnect_cancels: true,
        },
        sql_idempotency: SqlIdempotencyCapabilities {
            version: 1,
            durable_pre_execution_intent: true,
            replay_committed_receipt: true,
            indeterminate_never_reexecutes: true,
        },
        sql_pagination: SqlPaginationCapabilities {
            version: 1,
            continuation_endpoint: "/sql/continue",
            retained_snapshot: true,
            projection_required: true,
            byte_and_token_hints: true,
        },
    })
}

#[derive(Debug, Deserialize)]
struct HistoryRetentionRequest {
    #[serde(default)]
    history_retention_epochs: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct HistoryRetentionResponse {
    history_retention_epochs: u64,
    earliest_retained_epoch: u64,
}

fn history_retention_response(db: &Database) -> HistoryRetentionResponse {
    HistoryRetentionResponse {
        history_retention_epochs: db.history_retention_epochs(),
        earliest_retained_epoch: db.earliest_retained_epoch().0,
    }
}

/// `GET /history/retention` — inspect the durable MVCC history window.
async fn history_retention(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
) -> Response {
    if let Err(error) = state.db.require_for(
        request_principal(&state, &principal).as_ref(),
        &mongreldb_core::Permission::Admin,
    ) {
        return (status_for_error(&error), error.to_string()).into_response();
    }
    Json(history_retention_response(&state.db)).into_response()
}

/// `PUT /history/retention` — set the durable MVCC history window.
async fn set_history_retention(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Json(request): Json<HistoryRetentionRequest>,
) -> Response {
    if let Err(error) = state.db.require_for(
        request_principal(&state, &principal).as_ref(),
        &mongreldb_core::Permission::Admin,
    ) {
        return (status_for_error(&error), error.to_string()).into_response();
    }
    let Some(epochs) = request.history_retention_epochs.as_u64() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "history_retention_epochs must be a u64"})),
        )
            .into_response();
    };
    match state.db.set_history_retention_epochs(epochs) {
        Ok(()) => Json(history_retention_response(&state.db)).into_response(),
        Err(error) => (status_for_error(&error), error.to_string()).into_response(),
    }
}

/// `GET /audit` — recent security-audit events (auth + DDL/privilege) as a JSON
/// array, oldest-first. Subject to the same auth middleware as every other
/// route. This is a best-effort in-memory ring buffer, not a tamper-evident
/// log (see `audit` module docs).
async fn audit_handler(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
) -> Response {
    if let Err(error) = state.db.require_for(
        request_principal(&state, &principal).as_ref(),
        &mongreldb_core::Permission::Admin,
    ) {
        return (status_for_error(&error), error.to_string()).into_response();
    }
    let recent = state.audit.recent();
    Json(recent).into_response()
}

/// Default max live sessions when `--max-sessions` is not given.
fn default_max_sessions() -> usize {
    std::env::var("MONGRELBL_MAX_SESSIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(256)
}

/// Default idle-session timeout when `--session-idle-timeout` is not given.
fn default_session_idle_timeout() -> std::time::Duration {
    std::time::Duration::from_secs(
        std::env::var("MONGRELBL_SESSION_IDLE_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300),
    )
}

fn default_replication_wal_segments() -> usize {
    let replication = std::env::var("MONGRELDB_REPLICATION_WAL_SEGMENTS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(16);
    let cdc = std::env::var("MONGRELDB_CDC_WAL_SEGMENTS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(16);
    replication.max(cdc)
}

fn default_history_retention_epochs() -> u64 {
    std::env::var("MONGRELDB_HISTORY_RETENTION_EPOCHS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1024)
}

fn default_ai_max_concurrent() -> usize {
    std::env::var("MONGRELDB_AI_MAX_CONCURRENT")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(4)
}

fn positive_env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn positive_env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn default_sql_default_timeout() -> std::time::Duration {
    std::time::Duration::from_millis(positive_env_u64("MONGRELDB_SQL_DEFAULT_TIMEOUT_MS", 30_000))
}

fn default_sql_max_timeout() -> std::time::Duration {
    std::time::Duration::from_millis(positive_env_u64("MONGRELDB_SQL_MAX_TIMEOUT_MS", 300_000))
}

fn default_sql_max_concurrent() -> usize {
    positive_env_usize(
        "MONGRELDB_SQL_MAX_CONCURRENT",
        std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(4),
    )
}

fn default_sql_max_active_queries() -> usize {
    positive_env_usize("MONGRELDB_SQL_MAX_ACTIVE_QUERIES", 1_024)
}

fn default_sql_finished_query_ttl() -> std::time::Duration {
    std::time::Duration::from_secs(positive_env_u64(
        "MONGRELDB_SQL_FINISHED_QUERY_TTL_SECS",
        60,
    ))
}

fn default_sql_pre_cancel_ttl() -> std::time::Duration {
    std::time::Duration::from_millis(positive_env_u64("MONGRELDB_SQL_PRE_CANCEL_TTL_MS", 15_000))
}

fn default_sql_pre_cancel_max_entries() -> usize {
    positive_env_usize("MONGRELDB_SQL_PRE_CANCEL_MAX_ENTRIES", 2_048)
}

fn default_sql_pre_cancel_max_bytes() -> usize {
    positive_env_usize("MONGRELDB_SQL_PRE_CANCEL_MAX_BYTES", 1024 * 1024)
}

fn default_sql_pre_cancel_max_entries_per_owner() -> usize {
    positive_env_usize("MONGRELDB_SQL_PRE_CANCEL_MAX_PER_OWNER", 256)
}

fn default_sql_pre_cancel_rate_window() -> std::time::Duration {
    std::time::Duration::from_millis(positive_env_u64(
        "MONGRELDB_SQL_PRE_CANCEL_RATE_WINDOW_MS",
        1_000,
    ))
}

fn default_sql_pre_cancel_rate_per_owner() -> usize {
    positive_env_usize("MONGRELDB_SQL_PRE_CANCEL_RATE_PER_OWNER", 256)
}

pub(crate) fn default_sql_idempotency_ttl() -> std::time::Duration {
    std::time::Duration::from_secs(positive_env_u64(
        "MONGRELDB_SQL_IDEMPOTENCY_TTL_SECS",
        86_400,
    ))
}

pub(crate) fn default_sql_idempotency_max_entries() -> usize {
    positive_env_usize("MONGRELDB_SQL_IDEMPOTENCY_MAX_ENTRIES", 4_096)
}

fn default_sql_page_ttl() -> std::time::Duration {
    std::time::Duration::from_secs(positive_env_u64("MONGRELDB_SQL_PAGE_TTL_SECS", 60))
}

fn default_sql_page_max_entries() -> usize {
    positive_env_usize("MONGRELDB_SQL_PAGE_MAX_ENTRIES", 128)
}

fn default_sql_page_max_bytes() -> usize {
    positive_env_usize("MONGRELDB_SQL_PAGE_MAX_RETAINED_BYTES", 128 * 1024 * 1024)
}

fn default_sql_page_max_entries_per_owner() -> usize {
    positive_env_usize("MONGRELDB_SQL_PAGE_MAX_PER_OWNER", 16)
}

fn default_sql_cancel_grace() -> std::time::Duration {
    std::time::Duration::from_millis(positive_env_u64("MONGRELDB_SQL_CANCEL_GRACE_MS", 1_000))
}

fn default_sql_max_output_bytes() -> usize {
    positive_env_usize("MONGRELDB_SQL_MAX_OUTPUT_BYTES", 64 * 1024 * 1024)
}

fn default_sql_max_output_rows() -> usize {
    positive_env_usize("MONGRELDB_SQL_MAX_OUTPUT_ROWS", 1_000_000)
}

/// Stable request ownership. Usernames and the literal bearer token are never
/// ownership keys, so replacement credentials cannot inherit live resources.
fn request_owner(state: &AppState, principal: &Option<mongreldb_core::Principal>) -> String {
    if let Some(p) = principal {
        return format!("user:{}:{}", p.user_id, p.created_epoch);
    }
    if let Some(token) = state.auth_token.as_deref() {
        let mut digest = sha2::Sha256::new();
        digest.update(b"mongreldb-server-bearer-owner-v1\0");
        digest.update((token.len() as u64).to_le_bytes());
        digest.update(token.as_bytes());
        return format!("bearer:{}", sql_idempotency::hex(&digest.finalize()));
    }
    if state.user_auth || state.db.require_auth_enabled() {
        return "unauthenticated".into();
    }
    "anonymous".into()
}

fn request_principal(
    state: &AppState,
    principal: &Option<mongreldb_core::Principal>,
) -> Option<mongreldb_core::Principal> {
    if let Some(principal) = principal {
        return state
            .db
            .resolve_current_principal(principal)
            .or_else(|| Some(principal.clone()));
    }
    state.auth_token.as_ref().and_then(|_| {
        if state.db.require_auth_enabled() {
            return state
                .db
                .principal_snapshot()
                .and_then(|principal| state.db.resolve_current_principal(&principal))
                .filter(|principal| principal.is_admin);
        }
        Some(mongreldb_core::Principal {
            user_id: 0,
            created_epoch: 0,
            username: "token".into(),
            is_admin: true,
            roles: Vec::new(),
            permissions: Vec::new(),
        })
    })
}

fn current_request_principal(
    state: &AppState,
    principal: &Option<mongreldb_core::Principal>,
) -> Option<mongreldb_core::Principal> {
    if let Some(principal) = principal {
        return state.db.resolve_current_principal(principal);
    }
    request_principal(state, principal)
}

fn request_identity_is_current(
    state: &AppState,
    principal: &Option<mongreldb_core::Principal>,
) -> bool {
    if principal.is_some() || state.auth_token.is_some() {
        return current_request_principal(state, principal).is_some();
    }
    !state.user_auth && !state.db.require_auth_enabled()
}

/// `POST /sessions` — open a long-lived session for cross-request interactive
/// transactions. Returns `{"session_id": "..."}`; send `X-Session-ID: <token>`
/// on subsequent `/sql` requests to route to it. The session is owned by the
/// authenticated principal and auto-expires after the idle timeout.
async fn create_session(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
) -> Response {
    if !state.accepting_sql.load(Ordering::Acquire) {
        return (StatusCode::SERVICE_UNAVAILABLE, "server is shutting down").into_response();
    }
    if !request_identity_is_current(&state, &principal) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let owner = request_owner(&state, &principal);
    let session = match MongrelSession::open_with_external_modules_as(
        Arc::clone(&state.db),
        state.external_modules.iter().cloned(),
        request_principal(&state, &principal),
    ) {
        Ok(session) => session.with_query_registry(Arc::clone(&state.query_registry)),
        Err(e) => return (status_for_query_error(&e), e.to_string()).into_response(),
    };
    match state.sessions.create(session, owner.clone()) {
        Some(token) => {
            state.audit.record(owner, "session.open", "session created");
            Json(json!({ "session_id": token })).into_response()
        }
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            "session limit reached; close an idle session or raise --max-sessions",
        )
            .into_response(),
    }
}

/// `DELETE /sessions/{id}` — close a session, discarding any open (staged but
/// uncommitted) transaction. Only the owning principal may close it.
async fn close_session(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Path(id): Path<String>,
) -> Response {
    if !request_identity_is_current(&state, &principal) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let owner = request_owner(&state, &principal);
    if let Some(entry) = state.sessions.take_for_close(&id, &owner) {
        entry
            .session
            .query_registry()
            .cancel_session(&id, CancellationReason::SessionClosed);
        let deadline = tokio::time::Instant::now() + state.sql_cancel_grace;
        while entry.session.query_registry().active_for_session(&id) > 0
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        state.audit.record(owner, "session.close", "session closed");
        StatusCode::OK.into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            "session not found or not owned by caller",
        )
            .into_response()
    }
}

/// Choose a buffered response serialization: `"arrow"` (IPC file) or JSON.
/// Streaming IPC is dispatched before query collection.
async fn dispatch_buffered_sql_format(
    state: &AppState,
    format: Option<&str>,
    output: ManagedQueryBatches,
    query_id: QueryId,
    test_hook: Option<mongreldb_query::SqlTestHook>,
    output_limits: (usize, usize),
) -> Response {
    let format = format.unwrap_or("json").to_owned();
    let (max_rows, max_bytes) = output_limits;
    // Keep the lifecycle guard outside the worker. If the worker panics, the
    // join-error path must record a serialization failure, not let dropping a
    // moved guard misclassify it as a client disconnect.
    let serialization_batches = output.batches().to_vec();
    let serialization_query = output.query().clone();
    let serialized = tokio::task::spawn_blocking(move || {
        serialize_buffered_output(
            &format,
            &serialization_batches,
            &serialization_query,
            max_rows,
            max_bytes,
            test_hook.as_ref(),
        )
    })
    .await;
    let result = match serialized {
        Ok(result) => result,
        Err(error) => {
            output
                .query()
                .record_serialization_failure("SERIALIZATION_WORKER_FAILED");
            output.fail();
            state.metrics.inc_sql_errors();
            return terminal_server_error_response(
                state,
                query_id,
                StatusCode::INTERNAL_SERVER_ERROR,
                "SERIALIZATION_WORKER_FAILED",
                error.to_string(),
            );
        }
    };
    match result {
        Ok(serialized) => {
            if let Err(error) = output.try_complete() {
                state.metrics.inc_sql_errors();
                return tracked_query_error_response(state, &error, Some(query_id));
            }
            state.metrics.add_sql_output_bytes(serialized.bytes.len());
            let content_type = if serialized.arrow {
                "application/vnd.apache.arrow.file"
            } else {
                "application/json"
            };
            with_query_id(
                ([(header::CONTENT_TYPE, content_type)], serialized.bytes).into_response(),
                query_id,
            )
        }
        Err(BufferedSerializationError::Query(error)) => {
            output.fail();
            state.metrics.inc_sql_errors();
            tracked_query_error_response(state, &error, Some(query_id))
        }
        Err(BufferedSerializationError::Limit(message)) => {
            output.fail_result_limit();
            state.metrics.inc_sql_errors();
            terminal_server_error_response(
                state,
                query_id,
                StatusCode::PAYLOAD_TOO_LARGE,
                "RESULT_LIMIT_EXCEEDED",
                message,
            )
        }
        Err(BufferedSerializationError::Encoding(message)) => {
            output.fail_serialization();
            state.metrics.inc_sql_errors();
            terminal_server_error_response(
                state,
                query_id,
                StatusCode::INTERNAL_SERVER_ERROR,
                "SERIALIZATION_FAILED",
                message,
            )
        }
    }
}

#[derive(Debug)]
enum PaginatedSerializationError {
    Query(mongreldb_query::MongrelQueryError),
    Limit(String),
    Projection(String),
    Encoding(String),
}

struct SerializedPageRows {
    rows: Vec<serde_json::Value>,
    retained_bytes: usize,
}

struct PaginatedJsonReader<'a> {
    cursor: std::io::Cursor<&'a [u8]>,
    query: &'a RegisteredSqlQuery,
    test_hook: Option<&'a mongreldb_query::SqlTestHook>,
}

impl std::io::Read for PaginatedJsonReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if let Some(hook) = self.test_hook {
            hook(mongreldb_query::SqlTestHookPoint::DuringPaginationDeserialization);
        }
        self.query
            .checkpoint()
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        // Bound work between cancellation checks even for one very large row.
        let length = buffer.len().min(64 * 1024);
        std::io::Read::read(&mut self.cursor, &mut buffer[..length])
    }
}

const PAGINATED_VALUE_NODE_BYTES: usize = 64;
const PAGINATED_OBJECT_ENTRY_BYTES: usize = 128;
const PAGINATED_MEMORY_LIMIT_ERROR: &str = "SQL retained output memory limit exceeded";

struct PaginatedDecodeBudget<'a> {
    used: usize,
    limit: usize,
    nodes: usize,
    exceeded: bool,
    query: &'a RegisteredSqlQuery,
    test_hook: Option<&'a mongreldb_query::SqlTestHook>,
}

impl PaginatedDecodeBudget<'_> {
    fn begin_value<E: serde::de::Error>(&mut self) -> Result<(), E> {
        self.nodes = self.nodes.saturating_add(1);
        if self.nodes & 255 == 0 {
            if let Some(hook) = self.test_hook {
                hook(mongreldb_query::SqlTestHookPoint::DuringPaginationDeserialization);
            }
            self.query.checkpoint().map_err(E::custom)?;
        }
        self.charge::<E>(PAGINATED_VALUE_NODE_BYTES)
    }

    fn charge<E: serde::de::Error>(&mut self, bytes: usize) -> Result<(), E> {
        let next = self.used.saturating_add(bytes);
        if next > self.limit {
            self.exceeded = true;
            return Err(E::custom(PAGINATED_MEMORY_LIMIT_ERROR));
        }
        self.used = next;
        Ok(())
    }
}

struct BudgetedJsonValueSeed<'a, 'query> {
    budget: &'a mut PaginatedDecodeBudget<'query>,
}

impl<'de> serde::de::DeserializeSeed<'de> for BudgetedJsonValueSeed<'_, '_> {
    type Value = serde_json::Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        self.budget.begin_value::<D::Error>()?;
        deserializer.deserialize_any(BudgetedJsonValueVisitor {
            budget: self.budget,
        })
    }
}

struct BudgetedJsonValueVisitor<'a, 'query> {
    budget: &'a mut PaginatedDecodeBudget<'query>,
}

impl<'de> serde::de::Visitor<'de> for BudgetedJsonValueVisitor<'_, '_> {
    type Value = serde_json::Value;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Number(value.into()))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Number(value.into()))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .ok_or_else(|| E::custom("JSON number is not finite"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.budget.charge::<E>(value.len())?;
        Ok(serde_json::Value::String(value.to_owned()))
    }

    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_str(value)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.budget.charge::<E>(value.capacity())?;
        Ok(serde_json::Value::String(value))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Null)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Null)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }

    fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element_seed(BudgetedJsonValueSeed {
            budget: self.budget,
        })? {
            values.push(value);
        }
        Ok(serde_json::Value::Array(values))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut values = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            self.budget
                .charge::<A::Error>(PAGINATED_OBJECT_ENTRY_BYTES.saturating_add(key.capacity()))?;
            let value = map.next_value_seed(BudgetedJsonValueSeed {
                budget: self.budget,
            })?;
            values.insert(key, value);
        }
        Ok(serde_json::Value::Object(values))
    }
}

struct BudgetedJsonRowsSeed<'a, 'query> {
    budget: &'a mut PaginatedDecodeBudget<'query>,
}

impl<'de> serde::de::DeserializeSeed<'de> for BudgetedJsonRowsSeed<'_, '_> {
    type Value = Vec<serde_json::Value>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(BudgetedJsonRowsVisitor {
            budget: self.budget,
        })
    }
}

struct BudgetedJsonRowsVisitor<'a, 'query> {
    budget: &'a mut PaginatedDecodeBudget<'query>,
}

impl<'de> serde::de::Visitor<'de> for BudgetedJsonRowsVisitor<'_, '_> {
    type Value = Vec<serde_json::Value>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON array of SQL rows")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut rows = Vec::new();
        while let Some(row) = sequence.next_element_seed(BudgetedJsonValueSeed {
            budget: self.budget,
        })? {
            rows.push(row);
        }
        Ok(rows)
    }
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_paginated_sql(
    state: &AppState,
    output: ManagedQueryBatches,
    query_id: QueryId,
    owner: &str,
    pagination: ResolvedSqlPagination,
    output_limits: (usize, usize),
    test_hook: Option<mongreldb_query::SqlTestHook>,
    binding: sql_pages::SqlPageBinding,
) -> Response {
    let projection = pagination.projection;
    let serialization_projection = projection.clone();
    let serialization_batches = output.batches().to_vec();
    let serialization_query = output.query().clone();
    let response_test_hook = test_hook.clone();
    let retained_memory_limit = output_limits.1.min(state.sql_pages.max_retained_bytes());
    let serialized = tokio::task::spawn_blocking(move || {
        serialize_paginated_rows(
            &serialization_batches,
            &serialization_query,
            &serialization_projection,
            output_limits.0,
            output_limits.1,
            retained_memory_limit,
            test_hook.as_ref(),
        )
    })
    .await;
    let serialized = match serialized {
        Ok(result) => result,
        Err(error) => {
            output
                .query()
                .record_serialization_failure("SERIALIZATION_WORKER_FAILED");
            output.fail();
            state.metrics.inc_sql_errors();
            return terminal_server_error_response(
                state,
                query_id,
                StatusCode::INTERNAL_SERVER_ERROR,
                "SERIALIZATION_WORKER_FAILED",
                error.to_string(),
            );
        }
    };
    let serialized = match serialized {
        Ok(serialized) => serialized,
        Err(PaginatedSerializationError::Query(error)) => {
            output.fail();
            state.metrics.inc_sql_errors();
            return tracked_query_error_response(state, &error, Some(query_id));
        }
        Err(PaginatedSerializationError::Limit(message)) => {
            output.fail_result_limit();
            state.metrics.inc_sql_errors();
            return terminal_server_error_response(
                state,
                query_id,
                StatusCode::PAYLOAD_TOO_LARGE,
                "RESULT_LIMIT_EXCEEDED",
                message,
            );
        }
        Err(PaginatedSerializationError::Projection(message)) => {
            output.fail_with_error(
                "INVALID_SQL_PROJECTION",
                mongreldb_query::QueryTerminalErrorCategory::Execution,
            );
            state.metrics.inc_sql_errors();
            return terminal_server_error_response(
                state,
                query_id,
                StatusCode::BAD_REQUEST,
                "INVALID_SQL_PROJECTION",
                message,
            );
        }
        Err(PaginatedSerializationError::Encoding(message)) => {
            output.fail_serialization();
            state.metrics.inc_sql_errors();
            return terminal_server_error_response(
                state,
                query_id,
                StatusCode::INTERNAL_SERVER_ERROR,
                "SERIALIZATION_FAILED",
                message,
            );
        }
    };
    let retained_bytes = serialized.retained_bytes;
    let current_binding = sql_pages::SqlPageBinding {
        security_version: state.db.security_version(),
        catalog_epoch: state.db.catalog_snapshot().db_epoch,
    };
    if current_binding != binding {
        output.fail_with_error(
            "SQL_CURSOR_EXPIRED",
            mongreldb_query::QueryTerminalErrorCategory::Execution,
        );
        return sql_cursor_error_response(
            StatusCode::CONFLICT,
            "SQL_CURSOR_EXPIRED",
            "authorization or schema changed while retaining the SQL result",
        );
    }
    let retained = match state.sql_pages.insert(
        owner,
        serialized.rows,
        projection,
        pagination.limits,
        retained_bytes,
        binding,
    ) {
        Ok(retained) => retained,
        Err(sql_pages::InsertError::Full | sql_pages::InsertError::OwnerLimit) => {
            output.fail_with_error(
                "SQL_PAGE_STORE_FULL",
                mongreldb_query::QueryTerminalErrorCategory::Execution,
            );
            state.metrics.inc_sql_errors();
            return terminal_server_error_response(
                state,
                query_id,
                StatusCode::SERVICE_UNAVAILABLE,
                "SQL_PAGE_STORE_FULL",
                "retained SQL result capacity reached",
            );
        }
        Err(sql_pages::InsertError::EntropyUnavailable) => {
            output.fail_with_error(
                "ENTROPY_UNAVAILABLE",
                mongreldb_query::QueryTerminalErrorCategory::Execution,
            );
            state.metrics.inc_sql_errors();
            return terminal_server_error_response(
                state,
                query_id,
                StatusCode::INTERNAL_SERVER_ERROR,
                "ENTROPY_UNAVAILABLE",
                "OS CSPRNG unavailable",
            );
        }
    };
    let cursor_mac_key = match state.cursor_mac_key.get() {
        Ok(key) => key,
        Err(_) => {
            state.sql_pages.discard(&retained);
            output.fail_with_error(
                "ENTROPY_UNAVAILABLE",
                mongreldb_query::QueryTerminalErrorCategory::Execution,
            );
            state.metrics.inc_sql_errors();
            return terminal_server_error_response(
                state,
                query_id,
                StatusCode::INTERNAL_SERVER_ERROR,
                "ENTROPY_UNAVAILABLE",
                "OS CSPRNG unavailable",
            );
        }
    };
    let page = match sql_pages::SqlPageStore::first_page(&retained, &cursor_mac_key) {
        Ok(page) => page,
        Err(sql_pages::PageError::RowExceedsLimits) => {
            state.sql_pages.discard(&retained);
            output.fail_result_limit();
            state.metrics.inc_sql_errors();
            return terminal_server_error_response(
                state,
                query_id,
                StatusCode::PAYLOAD_TOO_LARGE,
                "RESULT_LIMIT_EXCEEDED",
                "one projected row exceeds the page byte or token limit",
            );
        }
        Err(sql_pages::PageError::OffsetInvalid) => {
            state.sql_pages.discard(&retained);
            output.fail_with_error(
                "INVALID_PAGE_OFFSET",
                mongreldb_query::QueryTerminalErrorCategory::Serialization,
            );
            state.metrics.inc_sql_errors();
            return terminal_server_error_response(
                state,
                query_id,
                StatusCode::INTERNAL_SERVER_ERROR,
                "INVALID_PAGE_OFFSET",
                "failed to create the first SQL result page",
            );
        }
    };
    let discard_after_response = page.next_cursor.is_none();
    let page_byte_count = page.byte_count;
    let encoded_page = tokio::task::spawn_blocking(move || serialize_sql_page(page)).await;
    let encoded_page = match encoded_page {
        Ok(Ok(encoded_page)) => encoded_page,
        Ok(Err(error)) => {
            state.sql_pages.discard(&retained);
            output.fail_serialization();
            state.metrics.inc_sql_errors();
            return terminal_server_error_response(
                state,
                query_id,
                StatusCode::INTERNAL_SERVER_ERROR,
                "SERIALIZATION_FAILED",
                error.to_string(),
            );
        }
        Err(_) => {
            state.sql_pages.discard(&retained);
            output.fail_serialization();
            state.metrics.inc_sql_errors();
            return terminal_server_error_response(
                state,
                query_id,
                StatusCode::INTERNAL_SERVER_ERROR,
                "SERIALIZATION_WORKER_FAILED",
                "SQL page response serialization worker failed",
            );
        }
    };
    if let Some(hook) = response_test_hook {
        hook(mongreldb_query::SqlTestHookPoint::AfterPageResponseSerialization);
    }
    if let Err(error) = output.query().checkpoint() {
        state.sql_pages.discard(&retained);
        state.metrics.inc_sql_errors();
        return tracked_query_error_response(state, &error, Some(query_id));
    }
    if let Err(error) = output.try_complete() {
        state.sql_pages.discard(&retained);
        state.metrics.inc_sql_errors();
        return tracked_query_error_response(state, &error, Some(query_id));
    }
    if discard_after_response {
        state.sql_pages.discard(&retained);
    }
    state.metrics.add_sql_output_bytes(page_byte_count);
    with_query_id(sql_page_response(encoded_page), query_id)
}

fn serialize_paginated_rows(
    batches: &[arrow::record_batch::RecordBatch],
    query: &RegisteredSqlQuery,
    projection: &[String],
    max_rows: usize,
    max_bytes: usize,
    retained_memory_limit: usize,
    test_hook: Option<&mongreldb_query::SqlTestHook>,
) -> Result<SerializedPageRows, PaginatedSerializationError> {
    const ROW_CHECKPOINT_INTERVAL: usize = 256;
    if batches.is_empty() {
        let retained_bytes = sql_pages::accounted_bytes(2, &[], projection, || query.checkpoint())
            .map_err(PaginatedSerializationError::Query)?;
        if retained_bytes > retained_memory_limit {
            return Err(PaginatedSerializationError::Limit(
                PAGINATED_MEMORY_LIMIT_ERROR.into(),
            ));
        }
        return Ok(SerializedPageRows {
            rows: Vec::new(),
            retained_bytes,
        });
    }
    let fields = batches[0].schema();
    let mut indices = Vec::with_capacity(projection.len());
    for name in projection {
        let matches: Vec<_> = fields
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(index, field)| (field.name() == name).then_some(index))
            .collect();
        if matches.len() != 1 {
            return Err(PaginatedSerializationError::Projection(format!(
                "projected output column {name:?} is missing or ambiguous"
            )));
        }
        indices.push(matches[0]);
    }

    let mut writer_output = LimitedOutput::new(max_bytes);
    let mut rows = 0usize;
    let encoding = (|| {
        let mut writer = arrow::json::writer::ArrayWriter::new(&mut writer_output);
        for batch in batches {
            let batch = batch.project(&indices).map_err(|error| error.to_string())?;
            for offset in (0..batch.num_rows()).step_by(ROW_CHECKPOINT_INTERVAL) {
                if let Some(hook) = test_hook {
                    hook(mongreldb_query::SqlTestHookPoint::BeforeSerializationBatch);
                }
                query.checkpoint().map_err(|error| error.to_string())?;
                let length = ROW_CHECKPOINT_INTERVAL.min(batch.num_rows() - offset);
                rows = rows.saturating_add(length);
                if rows > max_rows {
                    return Err("SQL retained output row limit exceeded".into());
                }
                let slice = batch.slice(offset, length);
                writer
                    .write_batches(&[&slice])
                    .map_err(|error| error.to_string())?;
            }
        }
        writer.finish().map_err(|error| error.to_string())
    })();
    if let Err(error) = encoding {
        if let Err(query_error) = query.checkpoint() {
            return Err(PaginatedSerializationError::Query(query_error));
        }
        if writer_output.exceeded || rows > max_rows {
            return Err(PaginatedSerializationError::Limit(error));
        }
        return Err(PaginatedSerializationError::Encoding(error));
    }
    let bytes = writer_output.bytes.len();
    let retained_base = sql_pages::accounted_bytes(bytes, &[], projection, || query.checkpoint())
        .map_err(PaginatedSerializationError::Query)?;
    if retained_base > retained_memory_limit {
        return Err(PaginatedSerializationError::Limit(
            PAGINATED_MEMORY_LIMIT_ERROR.into(),
        ));
    }
    let reader = PaginatedJsonReader {
        cursor: std::io::Cursor::new(writer_output.bytes.as_slice()),
        query,
        test_hook,
    };
    let mut deserializer =
        serde_json::Deserializer::from_reader(std::io::BufReader::with_capacity(64 * 1024, reader));
    let mut budget = PaginatedDecodeBudget {
        used: retained_base,
        limit: retained_memory_limit,
        nodes: 0,
        exceeded: false,
        query,
        test_hook,
    };
    let rows = match serde::de::DeserializeSeed::deserialize(
        BudgetedJsonRowsSeed {
            budget: &mut budget,
        },
        &mut deserializer,
    ) {
        Ok(rows) => {
            if let Err(error) = deserializer.end() {
                return Err(PaginatedSerializationError::Encoding(error.to_string()));
            }
            rows
        }
        Err(error) => {
            if let Err(query_error) = query.checkpoint() {
                return Err(PaginatedSerializationError::Query(query_error));
            }
            if budget.exceeded {
                return Err(PaginatedSerializationError::Limit(
                    PAGINATED_MEMORY_LIMIT_ERROR.into(),
                ));
            }
            return Err(PaginatedSerializationError::Encoding(error.to_string()));
        }
    };
    if let Some(hook) = test_hook {
        hook(mongreldb_query::SqlTestHookPoint::AfterSerialization);
    }
    let retained_bytes =
        sql_pages::accounted_bytes(bytes, &rows, projection, || query.checkpoint())
            .map_err(PaginatedSerializationError::Query)?;
    if retained_bytes > retained_memory_limit {
        return Err(PaginatedSerializationError::Limit(
            PAGINATED_MEMORY_LIMIT_ERROR.into(),
        ));
    }
    Ok(SerializedPageRows {
        rows,
        retained_bytes,
    })
}

fn serialize_sql_page(page: sql_pages::SqlPage) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&json!({
        "status": "completed",
        "rows": page.rows,
        "next_cursor": page.next_cursor,
        "page": {
            "offset": page.offset,
            "row_count": page.row_count,
            "total_rows": page.total_rows,
            "byte_count": page.byte_count,
            "estimated_tokens": page.estimated_tokens,
            "limits": page.limits,
            "projection": page.projection,
            "expires_at_ms": page.expires_at_ms,
            "snapshot": "retained_result",
            "token_estimate": "ceil(projected_json_bytes/4)",
        }
    }))
}

fn sql_page_response(body: Vec<u8>) -> Response {
    ([(header::CONTENT_TYPE, "application/json")], body).into_response()
}

/// Validate a prepared-statement name: bare identifier only
/// (`[A-Za-z_][A-Za-z0-9_]*`). Prevents SQL injection via the name, which is
/// interpolated into `PREPARE <name> AS ...` / `EXECUTE <name>(...)`.
fn validate_stmt_name(name: &str) -> Result<(), String> {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return Err("statement name must start with a letter or underscore".into()),
    }
    if !chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err("statement name may contain only letters, digits, or underscore".into());
    }
    Ok(())
}

/// Render a JSON value as a safe SQL literal for `EXECUTE` parameter binding.
/// Values are escaped so a client cannot inject SQL through a parameter.
/// Returns `Err` for non-scalar JSON (arrays/objects) so the caller rejects the
/// request with 400 rather than silently binding NULL.
fn render_sql_literal(v: &serde_json::Value) -> Result<String, String> {
    match v {
        serde_json::Value::Null => Ok("NULL".into()),
        serde_json::Value::Bool(b) => {
            if *b {
                Ok("TRUE".into())
            } else {
                Ok("FALSE".into())
            }
        }
        serde_json::Value::Number(n) => Ok(n.to_string()),
        // Single-quote, doubling embedded single quotes (SQL-standard escape).
        serde_json::Value::String(s) => {
            let mut out = String::with_capacity(s.len() + 2);
            out.push('\'');
            for c in s.chars() {
                if c == '\'' {
                    out.push_str("''");
                } else {
                    out.push(c);
                }
            }
            out.push('\'');
            Ok(out)
        }
        // Arrays/objects are not valid scalar params; reject explicitly.
        _ => Err("prepared-statement parameters must be scalar (null/bool/number/string)".into()),
    }
}

#[derive(Deserialize)]
struct PrepareRequest {
    name: String,
    sql: String,
    #[serde(default)]
    query_id: Option<QueryId>,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

/// `POST /sessions/{id}/prepare` — parse+plan `sql` once and store it under
/// `name` on the session. Subsequent `EXECUTE name(...)` calls (via this
/// endpoint or `EXECUTE` SQL) reuse the cached plan, skipping re-planning.
async fn prepare_statement(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Path(id): Path<String>,
    headers: axum::http::HeaderMap,
    Json(req): Json<PrepareRequest>,
) -> Response {
    if !state.accepting_sql.load(Ordering::Acquire) {
        return (StatusCode::SERVICE_UNAVAILABLE, "server is shutting down").into_response();
    }
    if !request_identity_is_current(&state, &principal) {
        return StatusCode::NOT_FOUND.into_response();
    }
    if let Err(msg) = validate_stmt_name(&req.name) {
        return (StatusCode::BAD_REQUEST, msg).into_response();
    }
    let owner = request_owner(&state, &principal);
    let Some(entry) = state.sessions.get(&id, &owner) else {
        return (
            StatusCode::NOT_FOUND,
            "session not found or not owned by caller",
        )
            .into_response();
    };
    let (options, query_id) = match resolve_query_options(
        &state,
        &headers,
        req.query_id,
        req.timeout_ms,
        owner,
        Some(id),
    ) {
        Ok(options) => options,
        Err(response) => return *response,
    };
    let query = match register_controlled_query(&state, &entry.session, options) {
        Ok(query) => query,
        Err(error) => return tracked_query_error_response(&state, &error, Some(query_id)),
    };
    let registration = RegisteredQueryGuard::new(query);
    if mongreldb_query::contains_boolean_ai_predicate(&req.sql) {
        registration.fail();
        return with_query_id(remote_boolean_ai_error(), query_id);
    }
    let _sql_permit = match acquire_sql_permit(&state, &entry.session, registration.query()).await {
        Ok(permit) => permit,
        Err(error) => return tracked_query_error_response(&state, &error, Some(query_id)),
    };
    let _guard = tokio::select! {
        guard = entry.lock.lock() => guard,
        _ = registration.query().control().cancelled() => {
            return tracked_query_error_response(
                &state,
                &cancellation_checkpoint_error(registration.query()),
                Some(query_id),
            );
        }
    };
    if entry.is_closed() {
        return with_query_id(
            (StatusCode::NOT_FOUND, "session no longer available").into_response(),
            query_id,
        );
    }
    entry.touch();
    let sql = format!("PREPARE {} AS {}", req.name, req.sql);
    let query = registration.into_query();
    match entry.session.run_with_query(&sql, query).await {
        Ok(_) => with_query_id(
            Json(json!({ "prepared": req.name })).into_response(),
            query_id,
        ),
        Err(error) => tracked_query_error_response(&state, &error, Some(query_id)),
    }
}

#[derive(Deserialize)]
struct ExecuteRequest {
    name: String,
    params: Vec<serde_json::Value>,
    #[serde(default)]
    format: Option<String>,
    #[serde(default)]
    query_id: Option<QueryId>,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

/// `POST /sessions/{id}/execute` — run a previously-prepared statement with
/// typed parameters, reusing its cached plan. Returns the same formats as
/// `/sql` (`json` default, `arrow`, `arrow-stream`).
async fn execute_statement(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Path(id): Path<String>,
    headers: axum::http::HeaderMap,
    Json(req): Json<ExecuteRequest>,
) -> Response {
    if !state.accepting_sql.load(Ordering::Acquire) {
        return (StatusCode::SERVICE_UNAVAILABLE, "server is shutting down").into_response();
    }
    if !request_identity_is_current(&state, &principal) {
        return StatusCode::NOT_FOUND.into_response();
    }
    if let Err(msg) = validate_stmt_name(&req.name) {
        return (StatusCode::BAD_REQUEST, msg).into_response();
    }
    let owner = request_owner(&state, &principal);
    let Some(entry) = state.sessions.get(&id, &owner) else {
        return (
            StatusCode::NOT_FOUND,
            "session not found or not owned by caller",
        )
            .into_response();
    };
    let (options, query_id) = match resolve_query_options(
        &state,
        &headers,
        req.query_id,
        req.timeout_ms,
        owner,
        Some(id),
    ) {
        Ok(options) => options,
        Err(response) => return *response,
    };
    let query = match register_controlled_query(&state, &entry.session, options) {
        Ok(query) => query,
        Err(error) => return tracked_query_error_response(&state, &error, Some(query_id)),
    };
    let registration = RegisteredQueryGuard::new(query);
    let sql_permit = match acquire_sql_permit(&state, &entry.session, registration.query()).await {
        Ok(permit) => permit,
        Err(error) => return tracked_query_error_response(&state, &error, Some(query_id)),
    };
    let _guard = tokio::select! {
        guard = entry.lock.lock() => guard,
        _ = registration.query().control().cancelled() => {
            return tracked_query_error_response(
                &state,
                &cancellation_checkpoint_error(registration.query()),
                Some(query_id),
            );
        }
    };
    if entry.is_closed() {
        return with_query_id(
            (StatusCode::NOT_FOUND, "session no longer available").into_response(),
            query_id,
        );
    }
    entry.touch();
    state.metrics.inc_sql_queries();
    let literals: Vec<String> = match req
        .params
        .iter()
        .map(render_sql_literal)
        .collect::<Result<_, _>>()
    {
        Ok(v) => v,
        Err(msg) => {
            state.metrics.inc_sql_errors();
            return (StatusCode::BAD_REQUEST, msg).into_response();
        }
    };
    let sql = format!("EXECUTE {}({})", req.name, literals.join(", "));
    let start = std::time::Instant::now();
    let result = if req.format.as_deref() == Some("arrow-stream") {
        let query = registration.into_query();
        match entry
            .session
            .run_stream_with_query_for_serialization(&sql, query)
            .await
        {
            Ok((stream, completion)) => Ok(sql_arrow_stream_response_controlled(
                stream,
                completion,
                sql_permit,
                (state.sql_max_output_rows, state.sql_max_output_bytes),
                &state,
                query_id,
                entry.session.sql_test_hook(),
            )),
            Err(error) => Err(error),
        }
    } else {
        let query = registration.into_query();
        match entry
            .session
            .run_with_query_for_serialization_with_limits(
                &sql,
                query,
                mongreldb_query::SqlCollectionLimits::new(
                    state.sql_max_output_rows,
                    state.sql_max_output_bytes,
                ),
            )
            .await
        {
            Ok(output) => Ok(dispatch_buffered_sql_format(
                &state,
                req.format.as_deref(),
                output,
                query_id,
                entry.session.sql_test_hook(),
                (state.sql_max_output_rows, state.sql_max_output_bytes),
            )
            .await),
            Err(error) => Err(error),
        }
    };
    let elapsed = start.elapsed();
    if elapsed >= state.slow_query_threshold {
        state.metrics.inc_slow_queries();
        eprintln!(
            "[slow-query] {}\u{00b5}s \u{2014} EXECUTE {}",
            elapsed.as_micros(),
            req.name
        );
    }
    match result {
        Ok(response) => with_query_id(response, query_id),
        Err(e) => {
            state.metrics.inc_sql_errors();
            // A reference to an unprepared/unknown statement is a client error.
            let msg = format!("{e}");
            let status = if msg.contains("does not exist") {
                StatusCode::NOT_FOUND
            } else {
                status_for_query_error(&e)
            };
            if status == status_for_query_error(&e) {
                tracked_query_error_response(&state, &e, Some(query_id))
            } else {
                with_query_id(
                    (status, format!("{msg} ({}µs)", elapsed.as_micros())).into_response(),
                    query_id,
                )
            }
        }
    }
}

/// `DELETE /sessions/{id}/statements/{name}` — drop a prepared statement from
/// the session (SQL `DEALLOCATE`).
async fn deallocate_statement(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Path((id, name)): Path<(String, String)>,
    headers: axum::http::HeaderMap,
) -> Response {
    if !state.accepting_sql.load(Ordering::Acquire) {
        return (StatusCode::SERVICE_UNAVAILABLE, "server is shutting down").into_response();
    }
    if !request_identity_is_current(&state, &principal) {
        return StatusCode::NOT_FOUND.into_response();
    }
    if let Err(msg) = validate_stmt_name(&name) {
        return (StatusCode::BAD_REQUEST, msg).into_response();
    }
    let owner = request_owner(&state, &principal);
    let Some(entry) = state.sessions.get(&id, &owner) else {
        return (
            StatusCode::NOT_FOUND,
            "session not found or not owned by caller",
        )
            .into_response();
    };
    let (options, query_id) =
        match resolve_query_options(&state, &headers, None, None, owner, Some(id)) {
            Ok(options) => options,
            Err(response) => return *response,
        };
    let query = match register_controlled_query(&state, &entry.session, options) {
        Ok(query) => query,
        Err(error) => return tracked_query_error_response(&state, &error, Some(query_id)),
    };
    let registration = RegisteredQueryGuard::new(query);
    let _sql_permit = match acquire_sql_permit(&state, &entry.session, registration.query()).await {
        Ok(permit) => permit,
        Err(error) => return tracked_query_error_response(&state, &error, Some(query_id)),
    };
    let _guard = tokio::select! {
        guard = entry.lock.lock() => guard,
        _ = registration.query().control().cancelled() => {
            return tracked_query_error_response(
                &state,
                &cancellation_checkpoint_error(registration.query()),
                Some(query_id),
            );
        }
    };
    if entry.is_closed() {
        return with_query_id(
            (StatusCode::NOT_FOUND, "session no longer available").into_response(),
            query_id,
        );
    }
    entry.touch();
    state.metrics.inc_sql_queries();
    let sql = format!("DEALLOCATE {name}");
    let start = std::time::Instant::now();
    let result = entry
        .session
        .run_with_query(&sql, registration.into_query())
        .await;
    let elapsed = start.elapsed();
    if elapsed >= state.slow_query_threshold {
        state.metrics.inc_slow_queries();
        eprintln!(
            "[slow-query] {}\u{00b5}s query_id={} operation=DEALLOCATE",
            elapsed.as_micros(),
            query_id,
        );
    }
    match result {
        Ok(_) => with_query_id(
            Json(json!({ "deallocated": name })).into_response(),
            query_id,
        ),
        Err(error) => {
            state.metrics.inc_sql_errors();
            tracked_query_error_response(&state, &error, Some(query_id))
        }
    }
}

/// `mongreldb_tables` gauge. Subject to the same auth middleware as every other
/// route (scrape with the configured Bearer token / Basic credentials).
async fn metrics_handler(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
) -> Response {
    if let Err(error) = state.db.require_for(
        request_principal(&state, &principal).as_ref(),
        &mongreldb_core::Permission::Admin,
    ) {
        return (status_for_error(&error), error.to_string()).into_response();
    }
    let body = state.metrics.prometheus_text(
        state.db.table_names().len(),
        state.query_registry.active_count(),
        state.query_registry.queued_count(),
        state.query_registry.entry_count(),
        state.query_registry.approximate_bytes(),
        (
            state.pre_cancellations.len(),
            state.pre_cancellations.approximate_bytes(),
        ),
    );
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8".to_string(),
        )],
        body,
    )
        .into_response()
}

/// `POST /compact` — compact all mounted tables.
async fn compact_all(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
) -> (StatusCode, Json<serde_json::Value>) {
    if let Err(error) = state.db.require_for(
        request_principal(&state, &principal).as_ref(),
        &mongreldb_core::Permission::Ddl,
    ) {
        return (
            status_for_error(&error),
            Json(json!({ "status": "error", "message": error.to_string() })),
        );
    }
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
    OptionalPrincipal(principal): OptionalPrincipal,
    Path(name): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    if let Err(error) = state.db.require_for(
        request_principal(&state, &principal).as_ref(),
        &mongreldb_core::Permission::Ddl,
    ) {
        return (
            status_for_error(&error),
            Json(json!({ "status": "error", "table": name, "message": error.to_string() })),
        );
    }
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
    OptionalPrincipal(principal): OptionalPrincipal,
    Json(req): Json<CreateTableRequest>,
) -> Response {
    if let Err(error) = state.db.require_for(
        request_principal(&state, &principal).as_ref(),
        &mongreldb_core::Permission::Ddl,
    ) {
        return (status_for_error(&error), error.to_string()).into_response();
    }
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
            default_value: None,
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
        Ok(id) => Json(json!({
            "table_id": id,
            "table_id_text": id.to_string()
        }))
        .into_response(),
        Err(error) => crate::kit::durable_core_error_response(&error)
            .unwrap_or_else(|| (status_for_error(&error), error.to_string()).into_response()),
    }
}

async fn list_tables(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
) -> Json<Vec<String>> {
    let principal = request_principal(&state, &principal);
    Json(
        state
            .db
            .table_names()
            .into_iter()
            .filter(|table| {
                state
                    .db
                    .select_column_ids_for(table, principal.as_ref())
                    .is_ok()
            })
            .collect(),
    )
}

async fn drop_table(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Path(name): Path<String>,
) -> Response {
    if let Err(error) = state.db.require_for(
        request_principal(&state, &principal).as_ref(),
        &mongreldb_core::Permission::Ddl,
    ) {
        return (status_for_error(&error), error.to_string()).into_response();
    }
    match state.db.drop_table_with_epoch(&name) {
        Ok(epoch) => Json(json!({
            "status": "committed",
            "epoch": epoch.0,
            "epoch_text": epoch.0.to_string()
        }))
        .into_response(),
        Err(error) => crate::kit::durable_core_error_response(&error)
            .unwrap_or_else(|| (status_for_error(&error), error.to_string()).into_response()),
    }
}

#[derive(Deserialize)]
struct PutRequest {
    row: Vec<serde_json::Value>,
}

pub(crate) fn json_to_value(v: &serde_json::Value, expected: &TypeId) -> Value {
    match (v, expected) {
        (serde_json::Value::Number(n), TypeId::Float64) => {
            n.as_f64().map(Value::Float64).unwrap_or(Value::Null)
        }
        (serde_json::Value::Number(n), TypeId::Int64) => {
            n.as_i64().map(Value::Int64).unwrap_or(Value::Null)
        }
        (serde_json::Value::String(s), TypeId::Bytes) => Value::Bytes(s.as_bytes().to_vec()),
        (serde_json::Value::String(s), TypeId::Enum { variants }) => {
            if variants.iter().any(|v| v == s) {
                Value::Bytes(s.as_bytes().to_vec())
            } else {
                Value::Null
            }
        }
        (serde_json::Value::Bool(b), TypeId::Bool) => Value::Bool(*b),
        // Embedding input: a JSON array of numbers, validated against the
        // declared dimension. Mismatched length or non-numeric elements → Null.
        (serde_json::Value::Array(arr), TypeId::Embedding { dim }) => {
            if arr.len() as u32 != *dim {
                return Value::Null;
            }
            let vec: Option<Vec<f32>> =
                arr.iter().map(|el| el.as_f64().map(|f| f as f32)).collect();
            vec.map(Value::Embedding).unwrap_or(Value::Null)
        }
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

fn legacy_json_to_value(value: &serde_json::Value, expected: &TypeId) -> Result<Value, String> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    match expected {
        TypeId::Bool => value
            .as_bool()
            .map(Value::Bool)
            .ok_or_else(|| "expected a boolean".into()),
        TypeId::Int8
        | TypeId::Int16
        | TypeId::Int32
        | TypeId::Int64
        | TypeId::UInt8
        | TypeId::UInt16
        | TypeId::UInt32
        | TypeId::UInt64
        | TypeId::TimestampNanos
        | TypeId::Date32
        | TypeId::Date64
        | TypeId::Time64 => value
            .as_i64()
            .map(Value::Int64)
            .ok_or_else(|| "expected a signed 64-bit integer".into()),
        TypeId::Float32 | TypeId::Float64 => value
            .as_f64()
            .filter(|value| value.is_finite())
            .map(Value::Float64)
            .ok_or_else(|| "expected a finite number".into()),
        TypeId::Bytes => match value.as_str() {
            Some(value) => Ok(Value::Bytes(value.as_bytes().to_vec())),
            None => decode_tagged_hex(value, "bytes").map(Value::Bytes),
        },
        TypeId::Enum { variants } => {
            let bytes = match value.as_str() {
                Some(value) => value.as_bytes().to_vec(),
                None => decode_tagged_hex(value, "bytes")?,
            };
            let value =
                std::str::from_utf8(&bytes).map_err(|_| "enum variant is not UTF-8".to_string())?;
            if !variants.iter().any(|variant| variant == value) {
                return Err("expected a declared enum variant".into());
            }
            Ok(Value::Bytes(bytes))
        }
        TypeId::Embedding { dim } => {
            let values = value
                .as_array()
                .ok_or_else(|| "expected an embedding array".to_string())?;
            if values.len() != *dim as usize {
                return Err(format!("expected an embedding with {dim} values"));
            }
            values
                .iter()
                .map(|value| {
                    value
                        .as_f64()
                        .map(|value| value as f32)
                        .filter(|value| value.is_finite())
                        .ok_or_else(|| "embedding values must be finite numbers".to_string())
                })
                .collect::<Result<Vec<_>, _>>()
                .map(Value::Embedding)
        }
        TypeId::Decimal128 { .. } => {
            let object = exact_tagged_object(value, "decimal", &["unscaled"])?;
            let text = object["unscaled"]
                .as_str()
                .ok_or_else(|| "decimal unscaled value must be a string".to_string())?;
            let value = text
                .parse::<i128>()
                .map_err(|_| "decimal unscaled value is invalid".to_string())?;
            if value.to_string() != text {
                return Err("decimal unscaled value is not canonical".into());
            }
            Ok(Value::Decimal(value))
        }
        TypeId::Interval => {
            let object = exact_tagged_object(value, "interval", &["months", "days", "nanos"])?;
            let months = canonical_i64(&object["months"], "interval months")?;
            let days = canonical_i64(&object["days"], "interval days")?
                .try_into()
                .map_err(|_| "interval days is outside i32 range".to_string())?;
            let nanos = canonical_i64(&object["nanos"], "interval nanos")?;
            Ok(Value::Interval {
                months,
                days,
                nanos,
            })
        }
        TypeId::Uuid => {
            let bytes = decode_tagged_hex(value, "uuid")?;
            let bytes: [u8; 16] = bytes
                .try_into()
                .map_err(|_| "UUID must contain exactly 16 bytes".to_string())?;
            Ok(Value::Uuid(bytes))
        }
        TypeId::Json => {
            let bytes = decode_tagged_hex(value, "json")?;
            std::str::from_utf8(&bytes).map_err(|_| "JSON value is not UTF-8".to_string())?;
            serde_json::from_slice::<serde_json::Value>(&bytes)
                .map_err(|error| format!("JSON value is invalid: {error}"))?;
            Ok(Value::Json(bytes))
        }
        TypeId::Array { .. } => Err("legacy put does not support array columns".into()),
    }
}

fn exact_tagged_object<'a>(
    value: &'a serde_json::Value,
    expected_kind: &str,
    fields: &[&str],
) -> Result<&'a serde_json::Map<String, serde_json::Value>, String> {
    let object = value
        .as_object()
        .ok_or_else(|| format!("expected tagged {expected_kind} value"))?;
    if object.len() != fields.len() + 1
        || object
            .get("$mongreldb_type")
            .and_then(|value| value.as_str())
            != Some(expected_kind)
        || fields.iter().any(|field| !object.contains_key(*field))
    {
        return Err(format!("invalid tagged {expected_kind} value"));
    }
    Ok(object)
}

fn decode_tagged_hex(value: &serde_json::Value, expected_kind: &str) -> Result<Vec<u8>, String> {
    let object = exact_tagged_object(value, expected_kind, &["hex"])?;
    let encoded = object["hex"]
        .as_str()
        .ok_or_else(|| format!("tagged {expected_kind} hex must be a string"))?;
    if encoded.len() % 2 != 0 {
        return Err(format!("tagged {expected_kind} hex has odd length"));
    }
    encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_nibble(pair[0])?;
            let low = hex_nibble(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn hex_nibble(value: u8) -> Result<u8, String> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err("hex value must use lowercase ASCII digits".into()),
    }
}

fn canonical_i64(value: &serde_json::Value, field: &str) -> Result<i64, String> {
    let text = value
        .as_str()
        .ok_or_else(|| format!("{field} must be a string"))?;
    let value = text
        .parse::<i64>()
        .map_err(|_| format!("{field} is invalid"))?;
    if value.to_string() != text {
        return Err(format!("{field} is not canonical"));
    }
    Ok(value)
}

#[cfg(test)]
mod legacy_wire_tests {
    use super::*;

    #[test]
    fn typed_values_are_exact_and_malformed_values_fail_closed() {
        assert_eq!(
            legacy_json_to_value(
                &json!({"$mongreldb_type": "bytes", "hex": "00ff61"}),
                &TypeId::Bytes,
            )
            .unwrap(),
            Value::Bytes(vec![0, 0xff, b'a'])
        );
        for (value, ty) in [
            (
                json!({"$mongreldb_type": "bytes", "hex": "00FF"}),
                TypeId::Bytes,
            ),
            (
                json!({"$mongreldb_type": "decimal", "unscaled": "01"}),
                TypeId::Decimal128 {
                    precision: 38,
                    scale: 0,
                },
            ),
            (
                json!({"$mongreldb_type": "uuid", "hex": "00"}),
                TypeId::Uuid,
            ),
            (
                json!({"$mongreldb_type": "json", "hex": "7b"}),
                TypeId::Json,
            ),
            (json!([1.0]), TypeId::Embedding { dim: 2 }),
            (json!([1e100, 1.0]), TypeId::Embedding { dim: 2 }),
        ] {
            assert!(legacy_json_to_value(&value, &ty).is_err(), "{value}");
        }
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
    let mut seen = std::collections::HashSet::new();
    for chunk in row.chunks(2) {
        let col_id = chunk[0]
            .as_u64()
            .and_then(|value| u16::try_from(value).ok())
            .ok_or("column id must be an unsigned 16-bit integer")?;
        if !seen.insert(col_id) {
            return Err(format!("duplicate column id {col_id}"));
        }
        let expected = schema
            .columns
            .iter()
            .find(|c| c.id == col_id)
            .map(|c| c.ty.clone())
            .ok_or_else(|| format!("unknown column id {col_id}"))?;
        let val = legacy_json_to_value(&chunk[1], &expected)?;
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
    OptionalPrincipal(principal): OptionalPrincipal,
    Path(name): Path<String>,
    Json(req): Json<PutRequest>,
) -> Response {
    let handle = match state.db.table(&name) {
        Ok(h) => h,
        Err(e) => return (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    };
    let schema = handle.lock().schema().clone();
    let row = match parse_cells(&req.row, &schema) {
        Ok(r) => r,
        Err(msg) => return (StatusCode::BAD_REQUEST, msg).into_response(),
    };
    state.metrics.inc_puts();
    let principal = request_principal(&state, &principal);
    match state.db.put_for(&name, row, principal.as_ref()) {
        Ok(rid) => Json(json!({ "row_id": rid.0.to_string() })).into_response(),
        Err(e) => (status_for_error(&e), e.to_string()).into_response(),
    }
}

async fn count(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Path(name): Path<String>,
) -> Response {
    let principal = request_principal(&state, &principal);
    match state.db.count_for(&name, principal.as_ref()) {
        Ok(count) => Json(json!({ "count": count })).into_response(),
        Err(error) => (status_for_error(&error), error.to_string()).into_response(),
    }
}

async fn commit(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Path(name): Path<String>,
) -> Response {
    if let Err(error) = state.db.require_for(
        request_principal(&state, &principal).as_ref(),
        &mongreldb_core::Permission::Update {
            table: name.clone(),
        },
    ) {
        return (status_for_error(&error), error.to_string()).into_response();
    }
    let handle = match state.db.table(&name) {
        Ok(h) => h,
        Err(e) => return (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    };
    let mut g = handle.lock();
    state.metrics.inc_commits();
    match g.commit() {
        Ok(epoch) => Json(json!({
            "epoch": epoch.0,
            "epoch_text": epoch.0.to_string()
        }))
        .into_response(),
        Err(error) => crate::kit::durable_core_error_response(&error)
            .unwrap_or_else(|| (status_for_error(&error), error.to_string()).into_response()),
    }
}

#[derive(Deserialize)]
struct SqlRequest {
    sql: String,
    /// Output format: `"json"` (the default) for a JSON array of row objects,
    /// `"arrow"` for Arrow IPC file bytes.
    #[serde(default)]
    format: Option<String>,
    /// Body values take precedence over the equivalent convenience headers.
    #[serde(default)]
    query_id: Option<QueryId>,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    max_output_rows: Option<u64>,
    #[serde(default)]
    max_output_bytes: Option<u64>,
    #[serde(default)]
    idempotency_key: Option<String>,
    #[serde(default)]
    pagination: Option<SqlPaginationRequest>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SqlPaginationRequest {
    page_size_rows: u64,
    projection: Vec<String>,
    #[serde(default)]
    max_page_bytes: Option<u64>,
    #[serde(default)]
    max_page_tokens: Option<u64>,
}

#[derive(Clone)]
struct ResolvedSqlPagination {
    projection: Vec<String>,
    limits: sql_pages::SqlPageLimits,
}

struct ResolvedSqlRequest {
    request: SqlRequest,
    output_limits: (usize, usize),
    idempotency: Option<sql_idempotency::SqlIdempotencyExecution>,
    pagination: Option<ResolvedSqlPagination>,
}

fn query_error_response(
    error: &mongreldb_query::MongrelQueryError,
    query_id: Option<QueryId>,
) -> Response {
    query_error_response_with_status(error, query_id, None)
}

fn query_error_response_with_status(
    error: &mongreldb_query::MongrelQueryError,
    query_id: Option<QueryId>,
    status: Option<&mongreldb_query::QueryStatus>,
) -> Response {
    use mongreldb_query::MongrelQueryError;
    let (base_code, id) = match error {
        MongrelQueryError::QueryCancelled { query_id, .. } => ("QUERY_CANCELLED", Some(*query_id)),
        MongrelQueryError::DeadlineExceeded { query_id, .. } => {
            ("DEADLINE_EXCEEDED", Some(*query_id))
        }
        MongrelQueryError::QueryIdConflict { query_id } => ("QUERY_ID_CONFLICT", Some(*query_id)),
        MongrelQueryError::QueryRegistryFull => ("QUERY_REGISTRY_FULL", query_id),
        MongrelQueryError::ResultLimitExceeded { query_id, .. } => {
            ("RESULT_LIMIT_EXCEEDED", Some(*query_id))
        }
        MongrelQueryError::TransactionAborted => ("TRANSACTION_ABORTED", query_id),
        MongrelQueryError::NoSqlTransaction => ("NO_SQL_TRANSACTION", query_id),
        MongrelQueryError::SavepointNotFound { .. } => ("SAVEPOINT_NOT_FOUND", query_id),
        MongrelQueryError::CommitOutcome { query_id, .. } => ("COMMIT_OUTCOME", Some(*query_id)),
        MongrelQueryError::OutcomeUnknown { query_id, .. } => {
            ("QUERY_OUTCOME_UNKNOWN", Some(*query_id))
        }
        _ => ("QUERY_FAILED", query_id),
    };
    let (
        error_committed,
        error_committed_statements,
        error_last_commit_epoch,
        error_first_commit_statement_index,
        error_last_commit_statement_index,
    ) = match error {
        MongrelQueryError::QueryCancelled {
            committed,
            committed_statements,
            last_commit_epoch,
            first_commit_statement_index,
            last_commit_statement_index,
            ..
        }
        | MongrelQueryError::DeadlineExceeded {
            committed,
            committed_statements,
            last_commit_epoch,
            first_commit_statement_index,
            last_commit_statement_index,
            ..
        }
        | MongrelQueryError::ResultLimitExceeded {
            committed,
            committed_statements,
            last_commit_epoch,
            first_commit_statement_index,
            last_commit_statement_index,
            ..
        } => (
            *committed,
            *committed_statements,
            *last_commit_epoch,
            *first_commit_statement_index,
            *last_commit_statement_index,
        ),
        MongrelQueryError::CommitOutcome {
            committed,
            committed_statements,
            last_commit_epoch,
            first_commit_statement_index,
            last_commit_statement_index,
            ..
        } => (
            *committed,
            *committed_statements,
            *last_commit_epoch,
            *first_commit_statement_index,
            *last_commit_statement_index,
        ),
        _ => (false, 0, None, None, None),
    };
    let committed = status.map_or_else(
        || error_committed,
        |status| status.durable_outcome.committed,
    );
    let outcome_unknown = matches!(error, MongrelQueryError::OutcomeUnknown { .. })
        || status.is_some_and(|status| status.outcome_unknown);
    let code = status
        .and_then(|status| {
            status
                .terminal_error
                .as_ref()
                .map(|error| error.code.as_str())
        })
        .unwrap_or(match (base_code, committed) {
            ("QUERY_CANCELLED", true) => "QUERY_CANCELLED_AFTER_COMMIT",
            ("DEADLINE_EXCEEDED", true) => "DEADLINE_AFTER_COMMIT",
            _ => base_code,
        });
    let response_status = if outcome_unknown {
        "outcome_unknown"
    } else {
        status
            .and_then(mongreldb_query::QueryStatus::terminal_state)
            .map(terminal_state_name)
            .unwrap_or_else(|| match (error, committed) {
                (MongrelQueryError::QueryCancelled { .. }, true) => "cancelled_after_commit",
                (MongrelQueryError::QueryCancelled { .. }, false) => "cancelled_before_commit",
                (MongrelQueryError::DeadlineExceeded { .. }, true) => "deadline_after_commit",
                (MongrelQueryError::DeadlineExceeded { .. }, false) => "deadline_before_commit",
                (_, true) => "committed_with_error",
                _ => "failed_before_commit",
            })
    };
    let (completed_statements, statement_index) = status.map_or_else(
        || match error {
            MongrelQueryError::QueryCancelled {
                completed_statements,
                cancelled_statement_index,
                ..
            }
            | MongrelQueryError::DeadlineExceeded {
                completed_statements,
                cancelled_statement_index,
                ..
            } => (*completed_statements, *cancelled_statement_index),
            MongrelQueryError::ResultLimitExceeded {
                completed_statements,
                statement_index,
                ..
            } => (*completed_statements, *statement_index),
            MongrelQueryError::CommitOutcome {
                completed_statements,
                statement_index,
                ..
            } => (*completed_statements, *statement_index),
            _ => (0, 0),
        },
        |status| (status.completed_statements, status.statement_index),
    );
    let committed_statements = status.map_or(error_committed_statements, |status| {
        status.durable_outcome.committed_statements
    });
    let last_commit_epoch = status.map_or(error_last_commit_epoch, |status| {
        status.durable_outcome.last_commit_epoch
    });
    let first_commit_statement_index = status
        .map_or(error_first_commit_statement_index, |status| {
            status.durable_outcome.first_commit_statement_index
        });
    let last_commit_statement_index = status.map_or(error_last_commit_statement_index, |status| {
        status.durable_outcome.last_commit_statement_index
    });
    let cancellation_reason = status
        .map(|status| status.cancellation_reason)
        .or(match error {
            MongrelQueryError::QueryCancelled { reason, .. } => Some(*reason),
            MongrelQueryError::DeadlineExceeded { .. } => Some(CancellationReason::Deadline),
            _ => None,
        })
        .map(cancellation_reason_name);
    let cancel_outcome = match error {
        MongrelQueryError::QueryCancelled { .. } | MongrelQueryError::DeadlineExceeded { .. } => {
            Some("accepted")
        }
        _ => status.and_then(query_cancel_outcome),
    };
    let outcome = if outcome_unknown {
        json!({
            "committed": null,
            "committed_statements": null,
            "last_commit_epoch": null,
            "last_commit_epoch_text": null,
            "first_commit_statement_index": null,
            "last_commit_statement_index": null,
            "completed_statements": null,
            "statement_index": null,
            "serialization": "unknown",
        })
    } else {
        status.map_or_else(
            || {
                json!({
                    "committed": committed,
                    "committed_statements": committed_statements,
                    "last_commit_epoch": last_commit_epoch,
                    "last_commit_epoch_text": epoch_text(last_commit_epoch),
                    "first_commit_statement_index": first_commit_statement_index,
                    "last_commit_statement_index": last_commit_statement_index,
                    "completed_statements": completed_statements,
                    "statement_index": statement_index,
                    "serialization": "unknown",
                })
            },
            |status| query_outcome_json(Some(status)),
        )
    };
    let http_status = match code {
        "QUERY_CANCELLED_AFTER_COMMIT" | "DEADLINE_AFTER_COMMIT" => StatusCode::CONFLICT,
        "QUERY_CANCELLED" => client_closed_request_status(),
        "DEADLINE_EXCEEDED" => StatusCode::GATEWAY_TIMEOUT,
        _ => status_for_query_error(error),
    };
    let mut response = (
        http_status,
        Json(json!({
            "query_id": id.map(|value| value.to_string()),
            "status": response_status,
            "terminal_state": response_status,
            "committed": (!outcome_unknown).then_some(committed),
            "committed_statements": (!outcome_unknown).then_some(committed_statements),
            "last_commit_epoch": (!outcome_unknown).then_some(last_commit_epoch).flatten(),
            "last_commit_epoch_text": (!outcome_unknown).then_some(epoch_text(last_commit_epoch)).flatten(),
            "first_commit_statement_index": (!outcome_unknown).then_some(first_commit_statement_index).flatten(),
            "last_commit_statement_index": (!outcome_unknown).then_some(last_commit_statement_index).flatten(),
            "completed_statements": (!outcome_unknown).then_some(completed_statements),
            "statement_index": (!outcome_unknown).then_some(statement_index),
            "cancel_outcome": cancel_outcome,
            "cancellation_reason": cancellation_reason,
            "retryable": matches!(error, MongrelQueryError::QueryRegistryFull),
            "server_state": status.map(|status| query_phase_name(status.phase)),
            "outcome": outcome,
            "error": {
                "code": code,
                "message": error.to_string(),
                "query_id": id.map(|value| value.to_string()),
                "committed": (!outcome_unknown).then_some(committed),
                "retryable": matches!(error, MongrelQueryError::QueryRegistryFull),
            }
        })),
    )
        .into_response();
    if let Some(id) = id {
        add_query_id_header(&mut response, id);
    }
    response
}

fn record_query_error(metrics: &metrics::Metrics, error: &mongreldb_query::MongrelQueryError) {
    match error {
        mongreldb_query::MongrelQueryError::QueryCancelled { reason, .. } => {
            metrics.inc_sql_cancelled(*reason)
        }
        mongreldb_query::MongrelQueryError::DeadlineExceeded { .. } => {
            metrics.inc_sql_deadline_exceeded();
            metrics.inc_sql_cancelled(CancellationReason::Deadline);
        }
        _ => {}
    }
}

fn tracked_query_error_response(
    state: &AppState,
    error: &mongreldb_query::MongrelQueryError,
    query_id: Option<QueryId>,
) -> Response {
    record_query_error(&state.metrics, error);
    if let mongreldb_query::MongrelQueryError::QueryCancelled { query_id, .. } = error {
        if let Some(requested_at) = state
            .query_registry
            .status(*query_id)
            .and_then(|status| status.cancel_requested_at)
        {
            state
                .metrics
                .observe_sql_cancel_latency(requested_at.elapsed());
        }
    }
    let status = if matches!(
        error,
        mongreldb_query::MongrelQueryError::QueryIdConflict { .. }
            | mongreldb_query::MongrelQueryError::QueryRegistryFull
    ) {
        None
    } else {
        query_id
            .or(match error {
                mongreldb_query::MongrelQueryError::QueryCancelled { query_id, .. }
                | mongreldb_query::MongrelQueryError::DeadlineExceeded { query_id, .. }
                | mongreldb_query::MongrelQueryError::CommitOutcome { query_id, .. }
                | mongreldb_query::MongrelQueryError::OutcomeUnknown { query_id, .. } => {
                    Some(*query_id)
                }
                _ => None,
            })
            .and_then(|query_id| state.query_registry.status(query_id))
    };
    query_error_response_with_status(error, query_id, status.as_ref())
}

fn add_query_id_header(response: &mut Response, query_id: QueryId) {
    if let Ok(value) = axum::http::HeaderValue::from_str(&query_id.to_string()) {
        response.headers_mut().insert("x-mongreldb-query-id", value);
    }
}

fn with_query_id(mut response: Response, query_id: QueryId) -> Response {
    add_query_id_header(&mut response, query_id);
    response
}

fn bad_query_control_request(message: impl Into<String>, query_id: Option<QueryId>) -> Response {
    let mut response = (
        StatusCode::BAD_REQUEST,
        Json(json!({
            "query_id": query_id.map(|value| value.to_string()),
            "status": "failed_before_commit",
            "terminal_state": "failed_before_commit",
            "committed": false,
            "committed_statements": 0,
            "last_commit_epoch": null,
            "last_commit_epoch_text": null,
            "first_commit_statement_index": null,
            "last_commit_statement_index": null,
            "completed_statements": 0,
            "statement_index": 0,
            "cancel_outcome": null,
            "cancellation_reason": null,
            "retryable": false,
            "server_state": "failed",
            "outcome": {
                "committed": false,
                "committed_statements": 0,
                "last_commit_epoch": null,
                "last_commit_epoch_text": null,
                "first_commit_statement_index": null,
                "last_commit_statement_index": null,
                "completed_statements": 0,
                "statement_index": 0,
                "serialization": "not_started",
            },
            "error": {
                "code": "INVALID_QUERY_OPTIONS",
                "message": message.into(),
                "query_id": query_id.map(|value| value.to_string()),
                "committed": false,
                "retryable": false,
            }
        })),
    )
        .into_response();
    if let Some(query_id) = query_id {
        add_query_id_header(&mut response, query_id);
    }
    response
}

fn resolve_query_options(
    state: &AppState,
    headers: &axum::http::HeaderMap,
    body_query_id: Option<QueryId>,
    body_timeout_ms: Option<u64>,
    owner: String,
    session_id: Option<String>,
) -> std::result::Result<(SqlQueryOptions, QueryId), Box<Response>> {
    let query_id = match body_query_id {
        Some(query_id) => query_id,
        None => match headers.get("x-mongreldb-query-id") {
            Some(value) => {
                let value = value.to_str().map_err(|_| {
                    Box::new(bad_query_control_request(
                        "X-MongrelDB-Query-ID is not valid text",
                        None,
                    ))
                })?;
                value
                    .parse()
                    .map_err(|error: mongreldb_query::MongrelQueryError| {
                        Box::new(bad_query_control_request(error.to_string(), None))
                    })?
            }
            None => {
                QueryId::random().map_err(|error| Box::new(query_error_response(&error, None)))?
            }
        },
    };
    let timeout_ms = match body_timeout_ms {
        Some(timeout_ms) => timeout_ms,
        None => match headers.get("x-mongreldb-timeout-ms") {
            Some(value) => value
                .to_str()
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or_else(|| {
                    Box::new(bad_query_control_request(
                        "X-MongrelDB-Timeout-Ms must be a positive integer",
                        Some(query_id),
                    ))
                })?,
            None => state
                .sql_default_timeout
                .as_millis()
                .min(u128::from(u64::MAX)) as u64,
        },
    };
    if timeout_ms == 0 {
        return Err(Box::new(bad_query_control_request(
            "timeout_ms must be positive",
            Some(query_id),
        )));
    }
    let timeout = std::time::Duration::from_millis(timeout_ms);
    if timeout > state.sql_max_timeout {
        return Err(Box::new(bad_query_control_request(
            format!(
                "timeout_ms exceeds server maximum of {}",
                state.sql_max_timeout.as_millis()
            ),
            Some(query_id),
        )));
    }
    Ok((
        SqlQueryOptions {
            query_id: Some(query_id),
            timeout: Some(timeout),
            owner: Some(owner),
            session_id,
            parent_control: None,
        },
        query_id,
    ))
}

fn resolve_sql_output_limits(
    state: &AppState,
    request: &SqlRequest,
    query_id: QueryId,
) -> std::result::Result<(usize, usize), Box<Response>> {
    fn resolve(
        requested: Option<u64>,
        configured: usize,
        name: &str,
        query_id: QueryId,
    ) -> std::result::Result<usize, Box<Response>> {
        if requested == Some(0) {
            return Err(Box::new(bad_query_control_request(
                format!("{name} must be positive"),
                Some(query_id),
            )));
        }
        let requested = requested
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or(usize::MAX);
        Ok(requested.min(configured))
    }

    Ok((
        resolve(
            request.max_output_rows,
            state.sql_max_output_rows,
            "max_output_rows",
            query_id,
        )?,
        resolve(
            request.max_output_bytes,
            state.sql_max_output_bytes,
            "max_output_bytes",
            query_id,
        )?,
    ))
}

fn resolve_sql_pagination(
    headers: &axum::http::HeaderMap,
    request: &SqlRequest,
    output_limits: (usize, usize),
    registration: RegisteredQueryGuard,
    query_id: QueryId,
) -> Result<(RegisteredQueryGuard, Option<ResolvedSqlPagination>), Box<Response>> {
    let Some(pagination) = request.pagination.as_ref() else {
        return Ok((registration, None));
    };
    if requested_sql_idempotency_key(headers, request)
        .ok()
        .flatten()
        .is_some()
    {
        return Err(Box::new(registered_sql_error_response(
            registration,
            query_id,
            StatusCode::BAD_REQUEST,
            "INCOMPATIBLE_SQL_CONTROLS",
            "idempotency_key cannot be combined with SQL pagination",
            false,
        )));
    }
    if request
        .format
        .as_deref()
        .is_some_and(|format| format != "json")
    {
        return Err(Box::new(registered_sql_error_response(
            registration,
            query_id,
            StatusCode::BAD_REQUEST,
            "PAGINATION_REQUIRES_JSON",
            "SQL pagination supports JSON responses only",
            false,
        )));
    }
    registration.query().set_sql_metadata(&request.sql);
    if !mongreldb_query::is_single_read_only_query(&request.sql) {
        return Err(Box::new(registered_sql_error_response(
            registration,
            query_id,
            StatusCode::BAD_REQUEST,
            "PAGINATION_REQUIRES_SINGLE_READ_QUERY",
            "SQL pagination accepts exactly one read-only query statement",
            false,
        )));
    }
    let page_size = match usize::try_from(pagination.page_size_rows) {
        Ok(0) | Err(_) => {
            return Err(Box::new(registered_sql_error_response(
                registration,
                query_id,
                StatusCode::BAD_REQUEST,
                "INVALID_PAGINATION_OPTIONS",
                "pagination.page_size_rows must be positive",
                false,
            )))
        }
        Ok(value) => value.min(output_limits.0),
    };
    if pagination.projection.is_empty() || pagination.projection.len() > 128 {
        return Err(Box::new(registered_sql_error_response(
            registration,
            query_id,
            StatusCode::BAD_REQUEST,
            "INVALID_SQL_PROJECTION",
            "pagination.projection must contain between 1 and 128 output column names",
            false,
        )));
    }
    let mut seen = std::collections::HashSet::new();
    let metadata_bytes = pagination
        .projection
        .iter()
        .map(String::len)
        .fold(0usize, usize::saturating_add);
    if metadata_bytes > 16 * 1024
        || pagination.projection.iter().any(|column| {
            column.is_empty()
                || column == "*"
                || column.len() > 256
                || !seen.insert(column.as_str())
        })
    {
        return Err(Box::new(registered_sql_error_response(
            registration,
            query_id,
            StatusCode::BAD_REQUEST,
            "INVALID_SQL_PROJECTION",
            "pagination.projection requires unique explicit output names of at most 256 bytes",
            false,
        )));
    }
    let max_page_bytes = match pagination.max_page_bytes {
        Some(0) => {
            return Err(Box::new(registered_sql_error_response(
                registration,
                query_id,
                StatusCode::BAD_REQUEST,
                "INVALID_PAGINATION_OPTIONS",
                "pagination.max_page_bytes must be positive",
                false,
            )))
        }
        Some(value) => usize::try_from(value)
            .unwrap_or(usize::MAX)
            .min(output_limits.1),
        None => output_limits.1.min(1024 * 1024),
    };
    let token_cap = (output_limits.1.saturating_add(3) / 4).max(1);
    let max_page_tokens = match pagination.max_page_tokens {
        Some(0) => {
            return Err(Box::new(registered_sql_error_response(
                registration,
                query_id,
                StatusCode::BAD_REQUEST,
                "INVALID_PAGINATION_OPTIONS",
                "pagination.max_page_tokens must be positive",
                false,
            )))
        }
        Some(value) => usize::try_from(value).unwrap_or(usize::MAX).min(token_cap),
        None => (max_page_bytes.saturating_add(3) / 4).max(1),
    };
    Ok((
        registration,
        Some(ResolvedSqlPagination {
            projection: pagination.projection.clone(),
            limits: sql_pages::SqlPageLimits {
                rows: page_size,
                bytes: max_page_bytes,
                tokens: max_page_tokens,
            },
        }),
    ))
}

fn requested_sql_idempotency_key(
    headers: &axum::http::HeaderMap,
    request: &SqlRequest,
) -> Result<Option<String>, &'static str> {
    let header = match headers.get("idempotency-key") {
        Some(value) => Some(
            value
                .to_str()
                .map_err(|_| "Idempotency-Key must be valid UTF-8")?,
        ),
        None => None,
    };
    match (request.idempotency_key.as_deref(), header) {
        (Some(body), Some(header)) if body != header => {
            Err("body idempotency_key and Idempotency-Key header must match")
        }
        (Some(body), _) => Ok(Some(body.to_owned())),
        (None, Some(header)) => Ok(Some(header.to_owned())),
        (None, None) => Ok(None),
    }
}

fn sql_idempotency_binding(
    request: &SqlRequest,
    output_limits: (usize, usize),
    session_id: Option<&str>,
    expires_after_ms: u64,
) -> Result<sql_idempotency::SqlIdempotencyBinding, serde_json::Error> {
    let request_semantics = serde_json::to_vec(&json!({
        "format": request.format.as_deref().unwrap_or("json"),
        "max_output_rows": output_limits.0,
        "max_output_bytes": output_limits.1,
        "pagination": request.pagination.as_ref(),
    }))?;
    let session_semantics = session_id.map_or_else(
        || b"ephemeral".to_vec(),
        |session_id| {
            let mut semantics = b"session\0".to_vec();
            semantics.extend_from_slice(session_id.as_bytes());
            semantics
        },
    );
    Ok(sql_idempotency::SqlIdempotencyBinding {
        sql_fingerprint: mongreldb_query::normalized_sql_fingerprint(&request.sql),
        // `/sql` currently has no separate bind-parameter array. SQL literals
        // are covered by the normalized SQL fingerprint above.
        parameter_hash: sql_idempotency::hash(b"[]"),
        request_semantics_hash: sql_idempotency::hash(&request_semantics),
        session_semantics_hash: sql_idempotency::hash(&session_semantics),
        expires_after_ms,
    })
}

struct SqlIdempotencyContext<'a> {
    headers: &'a axum::http::HeaderMap,
    request: &'a SqlRequest,
    output_limits: (usize, usize),
    owner: &'a str,
    session_id: Option<&'a str>,
    session_in_transaction: bool,
    query_id: QueryId,
}

async fn begin_sql_idempotency(
    state: &AppState,
    context: SqlIdempotencyContext<'_>,
    registration: RegisteredQueryGuard,
) -> Result<
    (
        RegisteredQueryGuard,
        Option<sql_idempotency::SqlIdempotencyExecution>,
    ),
    Response,
> {
    let SqlIdempotencyContext {
        headers,
        request,
        output_limits,
        owner,
        session_id,
        session_in_transaction,
        query_id,
    } = context;
    let key = match requested_sql_idempotency_key(headers, request) {
        Ok(key) => key,
        Err(message) => {
            return Err(registered_sql_error_response(
                registration,
                query_id,
                StatusCode::BAD_REQUEST,
                "INVALID_IDEMPOTENCY_KEY",
                message,
                false,
            ))
        }
    };
    let Some(key) = key else {
        return Ok((registration, None));
    };
    match mongreldb_query::classify_sql_idempotency(&request.sql) {
        mongreldb_query::SqlIdempotencyClass::ReadOnly
        | mongreldb_query::SqlIdempotencyClass::Unsupported => {
            return Err(registered_sql_error_response(
                registration,
                query_id,
                StatusCode::BAD_REQUEST,
                "IDEMPOTENCY_REQUIRES_SINGLE_WRITE",
                "idempotency_key accepts one non-transaction SQL write statement",
                false,
            ));
        }
        mongreldb_query::SqlIdempotencyClass::SingleWrite => {}
    }
    if let Err(message) = sql_idempotency::SqlIdempotencyStore::validate_key(&key) {
        return Err(registered_sql_error_response(
            registration,
            query_id,
            StatusCode::BAD_REQUEST,
            "INVALID_IDEMPOTENCY_KEY",
            message,
            false,
        ));
    }
    if request
        .format
        .as_deref()
        .is_some_and(|format| format != "json")
    {
        return Err(registered_sql_error_response(
            registration,
            query_id,
            StatusCode::BAD_REQUEST,
            "IDEMPOTENCY_REQUIRES_JSON",
            "SQL idempotency supports buffered JSON responses only",
            false,
        ));
    }
    if session_in_transaction {
        return Err(registered_sql_error_response(
            registration,
            query_id,
            StatusCode::CONFLICT,
            "IDEMPOTENCY_UNSUPPORTED_IN_TRANSACTION",
            "SQL idempotency cannot be used inside an open session transaction",
            false,
        ));
    }
    registration.query().set_sql_metadata(&request.sql);
    let binding = match sql_idempotency_binding(
        request,
        output_limits,
        session_id,
        state.sql_idempotency.expires_after_ms(),
    ) {
        Ok(binding) => binding,
        Err(_) => {
            return Err(registered_sql_error_response(
                registration,
                query_id,
                StatusCode::INTERNAL_SERVER_ERROR,
                "SERIALIZATION_FAILED",
                "failed to serialize SQL idempotency request semantics",
                false,
            ))
        }
    };
    let begin = tokio::select! {
        begin = state.sql_idempotency.begin(owner, &key, binding) => begin,
        _ = registration.query().control().cancelled() => {
            return Err(tracked_query_error_response(
                state,
                &cancellation_checkpoint_error(registration.query()),
                Some(query_id),
            ));
        }
    };
    match begin {
        sql_idempotency::BeginResult::Execute(execution) => Ok((registration, Some(execution))),
        sql_idempotency::BeginResult::Replay {
            receipt,
            expires_at_ms,
        } => match restore_idempotency_replay(registration, &receipt) {
            Ok(()) => Err(sql_idempotency_receipt_response(
                query_id,
                &receipt,
                true,
                expires_at_ms,
                true,
            )),
            Err(error) => Err(tracked_query_error_response(state, &error, Some(query_id))),
        },
        sql_idempotency::BeginResult::Mismatch => Err(registered_sql_error_response(
            registration,
            query_id,
            StatusCode::CONFLICT,
            "IDEMPOTENCY_KEY_REUSE_MISMATCH",
            "idempotency key was already used with different SQL or request semantics",
            false,
        )),
        sql_idempotency::BeginResult::Indeterminate { created_at_ms } => Err(
            sql_idempotency_indeterminate_response(registration, query_id, created_at_ms),
        ),
        sql_idempotency::BeginResult::Full => Err(registered_sql_error_response(
            registration,
            query_id,
            StatusCode::SERVICE_UNAVAILABLE,
            "IDEMPOTENCY_STORE_FULL",
            "SQL idempotency receipt store is full",
            true,
        )),
        sql_idempotency::BeginResult::Unavailable => Err(registered_sql_error_response(
            registration,
            query_id,
            StatusCode::SERVICE_UNAVAILABLE,
            "IDEMPOTENCY_STORE_UNAVAILABLE",
            "could not durably reserve the SQL idempotency key",
            true,
        )),
    }
}

fn restore_idempotency_replay(
    registration: RegisteredQueryGuard,
    receipt: &sql_idempotency::SqlDurableReceipt,
) -> mongreldb_query::Result<()> {
    use mongreldb_query::{
        DurableOutcome, QueryTerminalError, QueryTerminalErrorCategory, QueryTerminalState,
        SerializationOutcome,
    };

    let invalid_receipt = |field: &str| {
        mongreldb_query::MongrelQueryError::InvalidQueryState(format!(
            "durable SQL idempotency receipt has invalid {field}"
        ))
    };
    let terminal_state = match receipt.status.as_str() {
        "completed" => QueryTerminalState::Completed,
        "failed_before_commit" => QueryTerminalState::FailedBeforeCommit,
        "cancelled_before_commit" => QueryTerminalState::CancelledBeforeCommit,
        "deadline_before_commit" => QueryTerminalState::DeadlineBeforeCommit,
        "committed" => QueryTerminalState::Committed,
        "committed_with_error" => QueryTerminalState::CommittedWithError,
        "partially_committed" => QueryTerminalState::PartiallyCommitted,
        "cancelled_after_commit" => QueryTerminalState::CancelledAfterCommit,
        "deadline_after_commit" => QueryTerminalState::DeadlineAfterCommit,
        _ => return Err(invalid_receipt("terminal state")),
    };
    let serialization = match receipt.outcome.serialization.as_str() {
        "not_started" => SerializationOutcome::NotStarted,
        "in_progress" => SerializationOutcome::InProgress,
        "succeeded" => SerializationOutcome::Succeeded,
        "failed" => SerializationOutcome::Failed,
        _ => return Err(invalid_receipt("serialization state")),
    };
    let terminal_error = match receipt.terminal_error.as_ref() {
        Some(error) => Some(QueryTerminalError {
            code: error.code.clone(),
            category: match error.category.as_str() {
                "cancellation" => QueryTerminalErrorCategory::Cancellation,
                "deadline" => QueryTerminalErrorCategory::Deadline,
                "result_limit" => QueryTerminalErrorCategory::ResultLimit,
                "serialization" => QueryTerminalErrorCategory::Serialization,
                "execution" => QueryTerminalErrorCategory::Execution,
                _ => return Err(invalid_receipt("terminal error category")),
            },
        }),
        None => None,
    };
    let cancellation_reason = CancellationReason::from_protocol_str(&receipt.cancellation_reason)
        .ok_or_else(|| invalid_receipt("cancellation reason"))?;
    let phase = match receipt.server_state.as_str() {
        "completed" => SqlQueryPhase::Completed,
        "cancelled" => SqlQueryPhase::Cancelled,
        "failed" => SqlQueryPhase::Failed,
        _ => return Err(invalid_receipt("server state")),
    };
    let query = registration.into_query();
    query.restore_replayed_outcome(
        DurableOutcome {
            committed: receipt.outcome.committed,
            committed_statements: receipt.outcome.committed_statements,
            last_commit_epoch: receipt.outcome.last_commit_epoch,
            first_commit_statement_index: receipt.outcome.first_commit_statement_index,
            last_commit_statement_index: receipt.outcome.last_commit_statement_index,
        },
        receipt.outcome.completed_statements,
        receipt.outcome.statement_index,
        serialization,
        terminal_error,
        terminal_state,
        cancellation_reason,
        phase,
    );
    query.try_complete()
}

fn sql_idempotency_indeterminate_response(
    registration: RegisteredQueryGuard,
    query_id: QueryId,
    created_at_ms: Option<u64>,
) -> Response {
    registration.query().mark_outcome_unknown();
    registration.fail();
    with_query_id(
        (
            StatusCode::CONFLICT,
            Json(json!({
                "query_id": query_id.to_string(),
                "status": "outcome_unknown",
                "terminal_state": "outcome_unknown",
                "committed": null,
                "committed_statements": null,
                "last_commit_epoch": null,
                "last_commit_epoch_text": null,
                "first_commit_statement_index": null,
                "last_commit_statement_index": null,
                "completed_statements": null,
                "statement_index": null,
                "cancel_outcome": null,
                "cancellation_reason": null,
                "retryable": false,
                "server_state": "failed",
                "idempotency_replayed": true,
                "idempotency_intent_created_at_ms": created_at_ms,
                "outcome": {
                    "committed": null,
                    "committed_statements": null,
                    "last_commit_epoch": null,
                    "last_commit_epoch_text": null,
                    "first_commit_statement_index": null,
                    "last_commit_statement_index": null,
                    "completed_statements": null,
                    "statement_index": null,
                    "serialization": "unknown",
                },
                "error": {
                    "code": "QUERY_OUTCOME_UNKNOWN",
                    "message": "a durable write intent exists without a durable receipt; the SQL was not re-executed",
                    "query_id": query_id.to_string(),
                    "committed": null,
                    "retryable": false,
                }
            })),
        )
            .into_response(),
        query_id,
    )
}

fn registered_sql_error_response(
    registration: RegisteredQueryGuard,
    query_id: QueryId,
    status: StatusCode,
    code: &'static str,
    message: impl Into<String>,
    retryable: bool,
) -> Response {
    let message = message.into();
    registration
        .query()
        .record_terminal_error(code, mongreldb_query::QueryTerminalErrorCategory::Execution);
    registration.fail();
    with_query_id(
        (
            status,
            Json(json!({
                "query_id": query_id.to_string(),
                "status": "failed_before_commit",
                "terminal_state": "failed_before_commit",
                "committed": false,
                "committed_statements": 0,
                "last_commit_epoch": null,
                "last_commit_epoch_text": null,
                "first_commit_statement_index": null,
                "last_commit_statement_index": null,
                "completed_statements": 0,
                "statement_index": 0,
                "cancel_outcome": null,
                "cancellation_reason": null,
                "retryable": retryable,
                "server_state": "failed",
                "outcome": {
                    "committed": false,
                    "committed_statements": 0,
                    "last_commit_epoch": null,
                    "last_commit_epoch_text": null,
                    "first_commit_statement_index": null,
                    "last_commit_statement_index": null,
                    "completed_statements": 0,
                    "statement_index": 0,
                    "serialization": "not_started",
                },
                "error": {
                    "code": code,
                    "message": message,
                    "query_id": query_id.to_string(),
                    "committed": false,
                    "retryable": retryable,
                }
            })),
        )
            .into_response(),
        query_id,
    )
}

fn register_controlled_query(
    state: &AppState,
    session: &MongrelSession,
    options: SqlQueryOptions,
) -> std::result::Result<RegisteredSqlQuery, mongreldb_query::MongrelQueryError> {
    let query_id = options.query_id.ok_or_else(|| {
        mongreldb_query::MongrelQueryError::InvalidQueryState(
            "server query registration requires a query id".into(),
        )
    })?;
    let owner = options.owner.clone().unwrap_or_default();
    let session_id = options.session_id.clone();
    let _lifecycle = state
        .query_lifecycle
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let pre_cancel_reason = state
        .pre_cancellations
        .reason(query_id, &owner, session_id.as_deref());
    let query = session.register_query(options)?;
    let Some(reason) = pre_cancel_reason else {
        return Ok(query);
    };
    state
        .pre_cancellations
        .take(query_id, &owner, session_id.as_deref());
    query.request_cancel(reason);
    let error = query.checkpoint().err().unwrap_or_else(|| {
        mongreldb_query::MongrelQueryError::InvalidQueryState(format!(
            "pre-cancelled query {query_id} remained runnable"
        ))
    });
    query.fail();
    Err(error)
}

async fn acquire_sql_permit(
    state: &AppState,
    session: &MongrelSession,
    query: &RegisteredSqlQuery,
) -> std::result::Result<tokio::sync::OwnedSemaphorePermit, mongreldb_query::MongrelQueryError> {
    session.fire_test_hook(mongreldb_query::SqlTestHookPoint::WaitingForSqlPermit);
    tokio::select! {
        permit = Arc::clone(&state.sql_semaphore).acquire_owned() => permit.map_err(|_| {
            mongreldb_query::MongrelQueryError::InvalidQueryState(
                "SQL admission semaphore closed".into(),
            )
        }),
        _ = query.control().cancelled() => Err(cancellation_checkpoint_error(query)),
    }
}

fn caller_may_manage_query(
    state: &AppState,
    principal: &Option<mongreldb_core::Principal>,
    owner: Option<&str>,
) -> bool {
    let current = current_request_principal(state, principal);
    if (principal.is_some()
        || state.auth_token.is_some()
        || state.user_auth
        || state.db.require_auth_enabled())
        && current.is_none()
    {
        return false;
    }
    current.is_some_and(|principal| principal.is_admin)
        || owner == Some(request_owner(state, principal).as_str())
}

fn query_phase_name(phase: SqlQueryPhase) -> &'static str {
    match phase {
        SqlQueryPhase::Queued => "queued",
        SqlQueryPhase::Planning => "planning",
        SqlQueryPhase::Executing => "executing",
        SqlQueryPhase::Streaming => "streaming",
        SqlQueryPhase::Serializing => "serializing",
        SqlQueryPhase::CommitCritical => "commit_critical",
        SqlQueryPhase::Cancelling => "cancelling",
        SqlQueryPhase::Completed => "completed",
        SqlQueryPhase::Failed => "failed",
        SqlQueryPhase::Cancelled => "cancelled",
    }
}

fn commit_fence_outcome_name(outcome: mongreldb_query::CommitFenceOutcome) -> &'static str {
    match outcome {
        mongreldb_query::CommitFenceOutcome::NotReached => "not_reached",
        mongreldb_query::CommitFenceOutcome::CancelWon => "cancel_won",
        mongreldb_query::CommitFenceOutcome::CommitWon => "commit_won",
    }
}

fn terminal_state_name(state: mongreldb_query::QueryTerminalState) -> &'static str {
    use mongreldb_query::QueryTerminalState;
    match state {
        QueryTerminalState::OutcomeUnknown => "outcome_unknown",
        QueryTerminalState::Completed => "completed",
        QueryTerminalState::FailedBeforeCommit => "failed_before_commit",
        QueryTerminalState::CancelledBeforeCommit => "cancelled_before_commit",
        QueryTerminalState::DeadlineBeforeCommit => "deadline_before_commit",
        QueryTerminalState::Committed => "committed",
        QueryTerminalState::CommittedWithError => "committed_with_error",
        QueryTerminalState::PartiallyCommitted => "partially_committed",
        QueryTerminalState::CancelledAfterCommit => "cancelled_after_commit",
        QueryTerminalState::DeadlineAfterCommit => "deadline_after_commit",
    }
}

fn serialization_outcome_name(outcome: mongreldb_query::SerializationOutcome) -> &'static str {
    use mongreldb_query::SerializationOutcome;
    match outcome {
        SerializationOutcome::NotStarted => "not_started",
        SerializationOutcome::InProgress => "in_progress",
        SerializationOutcome::Succeeded => "succeeded",
        SerializationOutcome::Failed => "failed",
    }
}

fn terminal_error_category_name(
    category: mongreldb_query::QueryTerminalErrorCategory,
) -> &'static str {
    use mongreldb_query::QueryTerminalErrorCategory;
    match category {
        QueryTerminalErrorCategory::Cancellation => "cancellation",
        QueryTerminalErrorCategory::Deadline => "deadline",
        QueryTerminalErrorCategory::ResultLimit => "result_limit",
        QueryTerminalErrorCategory::Serialization => "serialization",
        QueryTerminalErrorCategory::Execution => "execution",
    }
}

fn terminal_error_retryable(error: Option<&mongreldb_query::QueryTerminalError>) -> bool {
    error.is_some_and(|error| {
        matches!(
            error.code.as_str(),
            "IDEMPOTENCY_STORE_FULL" | "IDEMPOTENCY_STORE_UNAVAILABLE"
        )
    })
}

fn epoch_text(epoch: Option<u64>) -> Option<String> {
    epoch.map(|epoch| epoch.to_string())
}

fn cancellation_reason_name(reason: CancellationReason) -> &'static str {
    reason.as_str()
}

fn query_cancel_outcome(status: &mongreldb_query::QueryStatus) -> Option<&'static str> {
    match status.phase {
        SqlQueryPhase::CommitCritical => Some("too_late"),
        SqlQueryPhase::Completed | SqlQueryPhase::Failed | SqlQueryPhase::Cancelled => {
            Some("already_finished")
        }
        SqlQueryPhase::Cancelling => Some("accepted"),
        _ => None,
    }
}

fn query_outcome_json(status: Option<&mongreldb_query::QueryStatus>) -> serde_json::Value {
    let Some(status) = status else {
        return json!({
            "committed": false,
            "committed_statements": 0,
            "last_commit_epoch": null,
            "last_commit_epoch_text": null,
            "first_commit_statement_index": null,
            "last_commit_statement_index": null,
            "completed_statements": 0,
            "statement_index": 0,
            "serialization": "not_started",
        });
    };
    if status.outcome_unknown {
        return json!({
            "committed": null,
            "committed_statements": null,
            "last_commit_epoch": null,
            "last_commit_epoch_text": null,
            "first_commit_statement_index": null,
            "last_commit_statement_index": null,
            "completed_statements": null,
            "statement_index": null,
            "serialization": "unknown",
        });
    }
    json!({
        "committed": status.durable_outcome.committed,
        "committed_statements": (!status.outcome_unknown).then_some(status.durable_outcome.committed_statements),
        "last_commit_epoch": (!status.outcome_unknown).then_some(status.durable_outcome.last_commit_epoch).flatten(),
        "last_commit_epoch_text": (!status.outcome_unknown).then_some(epoch_text(status.durable_outcome.last_commit_epoch)).flatten(),
        "first_commit_statement_index": (!status.outcome_unknown).then_some(status.durable_outcome.first_commit_statement_index).flatten(),
        "last_commit_statement_index": (!status.outcome_unknown).then_some(status.durable_outcome.last_commit_statement_index).flatten(),
        "completed_statements": (!status.outcome_unknown).then_some(status.completed_statements),
        "statement_index": (!status.outcome_unknown).then_some(status.statement_index),
        "serialization": serialization_outcome_name(status.serialization_outcome),
    })
}

fn sql_terminal_idempotency_receipt(
    status: &mongreldb_query::QueryStatus,
) -> Option<sql_idempotency::SqlDurableReceipt> {
    if status.outcome_unknown {
        return None;
    }
    let terminal_state = status.terminal_state()?;
    if !status.durable_outcome.committed
        && terminal_state != mongreldb_query::QueryTerminalState::Completed
    {
        return None;
    }
    Some(sql_idempotency::SqlDurableReceipt {
        original_query_id: status.query_id.to_string(),
        status: status
            .terminal_state()
            .map(terminal_state_name)
            .unwrap_or("committed")
            .to_owned(),
        server_state: query_phase_name(status.phase).to_owned(),
        cancellation_reason: cancellation_reason_name(status.cancellation_reason).to_owned(),
        outcome: sql_idempotency::SqlReceiptOutcome {
            committed: status.durable_outcome.committed,
            committed_statements: status.durable_outcome.committed_statements,
            last_commit_epoch: status.durable_outcome.last_commit_epoch,
            last_commit_epoch_text: epoch_text(status.durable_outcome.last_commit_epoch),
            first_commit_statement_index: status.durable_outcome.first_commit_statement_index,
            last_commit_statement_index: status.durable_outcome.last_commit_statement_index,
            completed_statements: status.completed_statements,
            statement_index: status.statement_index,
            serialization: serialization_outcome_name(status.serialization_outcome).to_owned(),
        },
        terminal_error: status.terminal_error.as_ref().map(|error| {
            sql_idempotency::SqlReceiptTerminalError {
                code: error.code.clone(),
                category: terminal_error_category_name(error.category).to_owned(),
            }
        }),
    })
}

fn sql_idempotency_receipt_response(
    query_id: QueryId,
    receipt: &sql_idempotency::SqlDurableReceipt,
    replayed: bool,
    expires_at_ms: u64,
    persisted: bool,
) -> Response {
    let mut response = Json(json!({
        "query_id": query_id.to_string(),
        "original_query_id": receipt.original_query_id,
        "status": receipt.status,
        "terminal_state": receipt.status,
        "server_state": receipt.server_state,
        "cancel_outcome": "already_finished",
        "cancellation_reason": receipt.cancellation_reason,
        "committed": receipt.outcome.committed,
        "committed_statements": receipt.outcome.committed_statements,
        "last_commit_epoch": receipt.outcome.last_commit_epoch,
        "last_commit_epoch_text": receipt.outcome.last_commit_epoch_text.as_deref(),
        "first_commit_statement_index": receipt.outcome.first_commit_statement_index,
        "last_commit_statement_index": receipt.outcome.last_commit_statement_index,
        "completed_statements": receipt.outcome.completed_statements,
        "statement_index": receipt.outcome.statement_index,
        "retryable": false,
        "idempotency_replayed": replayed,
        "idempotency_persisted": persisted,
        "idempotency_expires_at_ms": expires_at_ms,
        "outcome": receipt.outcome,
        "terminal_error": receipt.terminal_error,
    }))
    .into_response();
    response.headers_mut().insert(
        "idempotency-replayed",
        axum::http::HeaderValue::from_static(if replayed { "true" } else { "false" }),
    );
    response.headers_mut().insert(
        "idempotency-persisted",
        axum::http::HeaderValue::from_static(if persisted { "true" } else { "false" }),
    );
    if let Ok(value) = axum::http::HeaderValue::from_str(&receipt.original_query_id) {
        response
            .headers_mut()
            .insert("x-mongreldb-original-query-id", value);
    }
    with_query_id(response, query_id)
}

fn terminal_server_error_response(
    state: &AppState,
    query_id: QueryId,
    http_status: StatusCode,
    base_code: &'static str,
    message: impl Into<String>,
) -> Response {
    let status = state.query_registry.status(query_id);
    let committed = status
        .as_ref()
        .is_some_and(|status| status.durable_outcome.committed);
    let code = if committed && base_code.starts_with("SERIALIZATION_") {
        "SERIALIZATION_FAILED_AFTER_COMMIT"
    } else {
        base_code
    };
    let response_status = status
        .as_ref()
        .and_then(mongreldb_query::QueryStatus::terminal_state)
        .map(terminal_state_name)
        .unwrap_or(if committed {
            "committed_with_error"
        } else {
            "failed_before_commit"
        });
    let outcome = query_outcome_json(status.as_ref());
    with_query_id(
        (
            http_status,
            Json(json!({
                "query_id": query_id.to_string(),
                "status": response_status,
                "terminal_state": response_status,
                "committed": committed,
                "committed_statements": status.as_ref().map_or(0, |status| status.durable_outcome.committed_statements),
                "last_commit_epoch": status.as_ref().and_then(|status| status.durable_outcome.last_commit_epoch),
                "last_commit_epoch_text": epoch_text(status.as_ref().and_then(|status| status.durable_outcome.last_commit_epoch)),
                "first_commit_statement_index": status.as_ref().and_then(|status| status.durable_outcome.first_commit_statement_index),
                "last_commit_statement_index": status.as_ref().and_then(|status| status.durable_outcome.last_commit_statement_index),
                "completed_statements": status.as_ref().map_or(0, |status| status.completed_statements),
                "statement_index": status.as_ref().map_or(0, |status| status.statement_index),
                "cancel_outcome": null,
                "cancellation_reason": status.as_ref().map(|status| cancellation_reason_name(status.cancellation_reason)),
                "retryable": false,
                "server_state": status.as_ref().map(|status| query_phase_name(status.phase)),
                "outcome": outcome,
                "error": {
                    "code": code,
                    "message": message.into(),
                    "query_id": query_id.to_string(),
                    "committed": committed,
                    "retryable": false,
                }
            })),
        )
            .into_response(),
        query_id,
    )
}

fn query_not_found_response(query_id: Option<QueryId>) -> Response {
    let mut response = (
        StatusCode::NOT_FOUND,
        Json(json!({
            "query_id": query_id.map(|value| value.to_string()),
            "status": "unknown",
            "terminal_state": null,
            "committed": null,
            "committed_statements": null,
            "last_commit_epoch": null,
            "last_commit_epoch_text": null,
            "first_commit_statement_index": null,
            "last_commit_statement_index": null,
            "completed_statements": null,
            "statement_index": null,
            "cancel_outcome": "not_found",
            "cancellation_reason": null,
            "retryable": false,
            "server_state": "not_found",
            "outcome": {
                "committed": null,
                "committed_statements": null,
                "last_commit_epoch": null,
                "last_commit_epoch_text": null,
                "first_commit_statement_index": null,
                "last_commit_statement_index": null,
                "completed_statements": null,
                "statement_index": null,
                "serialization": "unknown",
            },
            "error": {
                "code": "QUERY_NOT_FOUND",
                "message": "query not found",
                "query_id": query_id.map(|value| value.to_string()),
                "committed": null,
                "retryable": false,
            }
        })),
    )
        .into_response();
    if let Some(query_id) = query_id {
        add_query_id_header(&mut response, query_id);
    }
    response
}

fn query_session_header(
    headers: &axum::http::HeaderMap,
    query_id: Option<QueryId>,
) -> std::result::Result<Option<String>, Box<Response>> {
    match headers.get("x-session-id") {
        Some(value) => match value.to_str() {
            Ok(value) if value.len() <= 256 => Ok(Some(value.to_owned())),
            _ => Err(Box::new(bad_query_control_request(
                "X-Session-ID must be valid text no longer than 256 bytes",
                query_id,
            ))),
        },
        None => Ok(None),
    }
}

fn pre_cancelled_query_response(
    query_id: QueryId,
    reason: CancellationReason,
    status: StatusCode,
) -> Response {
    with_query_id(
        (
            status,
            Json(json!({
                "query_id": query_id.to_string(),
                "status": "cancelled_before_start",
                "terminal_state": "cancelled_before_start",
                "state": "pre_cancelled",
                "server_state": "pre_cancelled",
                "cancel_outcome": "pre_cancelled",
                "committed": false,
                "committed_statements": 0,
                "last_commit_epoch": null,
                "last_commit_epoch_text": null,
                "first_commit_statement_index": null,
                "last_commit_statement_index": null,
                "completed_statements": 0,
                "statement_index": 0,
                "cancellation_reason": cancellation_reason_name(reason),
                "outcome": {
                    "committed": false,
                    "committed_statements": 0,
                    "last_commit_epoch": null,
                    "last_commit_epoch_text": null,
                    "first_commit_statement_index": null,
                    "last_commit_statement_index": null,
                    "completed_statements": 0,
                    "statement_index": 0,
                    "serialization": "not_started",
                },
                "terminal_error": {
                    "code": "QUERY_CANCELLED",
                    "category": "cancellation",
                },
                "retryable": false,
            })),
        )
            .into_response(),
        query_id,
    )
}

fn compact_finished_query_response(query_id: QueryId) -> Response {
    with_query_id(
        Json(json!({
            "query_id": query_id.to_string(),
            "status": "finished",
            "terminal_state": null,
            "state": "finished",
            "server_state": "finished",
            "cancel_outcome": "already_finished",
            "code": "QUERY_ALREADY_FINISHED",
            "committed": null,
            "committed_statements": null,
            "last_commit_epoch": null,
            "last_commit_epoch_text": null,
            "first_commit_statement_index": null,
            "last_commit_statement_index": null,
            "completed_statements": null,
            "statement_index": null,
            "cancellation_reason": "none",
            "outcome": {
                "committed": null,
                "committed_statements": null,
                "last_commit_epoch": null,
                "last_commit_epoch_text": null,
                "first_commit_statement_index": null,
                "last_commit_statement_index": null,
                "completed_statements": null,
                "statement_index": null,
                "serialization": "unknown",
            },
            "terminal_error": null,
            "retryable": false,
        }))
        .into_response(),
        query_id,
    )
}

async fn query_status(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Path(query_id): Path<String>,
    headers: axum::http::HeaderMap,
) -> Response {
    let Ok(query_id) = query_id.parse::<QueryId>() else {
        return query_not_found_response(None);
    };
    if !request_identity_is_current(&state, &principal) {
        return query_not_found_response(Some(query_id));
    }
    let requested_session = match query_session_header(&headers, Some(query_id)) {
        Ok(session_id) => session_id,
        Err(response) => return *response,
    };
    let owner = request_owner(&state, &principal);
    let is_admin =
        current_request_principal(&state, &principal).is_some_and(|principal| principal.is_admin);
    let _lifecycle = state
        .query_lifecycle
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let Some(status) = state.query_registry.status(query_id) else {
        if let Some((finished_owner, finished_session)) =
            state.query_registry.compact_finished_identity(query_id)
        {
            if !caller_may_manage_query(&state, &principal, finished_owner.as_deref())
                || requested_session
                    .as_deref()
                    .is_some_and(|session| finished_session.as_deref() != Some(session))
            {
                return query_not_found_response(Some(query_id));
            }
            return compact_finished_query_response(query_id);
        }
        let reason = state
            .pre_cancellations
            .reason(query_id, &owner, requested_session.as_deref())
            .or_else(|| {
                is_admin.then(|| match requested_session.as_deref() {
                    Some(session_id) => state
                        .pre_cancellations
                        .reason_for_query_in_session(query_id, session_id),
                    None => state.pre_cancellations.reason_for_query(query_id),
                })?
            });
        if let Some(reason) = reason {
            return pre_cancelled_query_response(query_id, reason, StatusCode::OK);
        }
        return query_not_found_response(Some(query_id));
    };
    if !caller_may_manage_query(&state, &principal, status.owner.as_deref())
        || requested_session
            .as_deref()
            .is_some_and(|session| status.session_id.as_deref() != Some(session))
    {
        return query_not_found_response(Some(query_id));
    }
    let terminal_status = status.terminal_state().map(terminal_state_name);
    let terminal_error = status.terminal_error.as_ref().map(|error| {
        json!({
            "code": error.code,
            "category": terminal_error_category_name(error.category),
        })
    });
    let retryable = terminal_error_retryable(status.terminal_error.as_ref());
    let cancel_outcome = query_cancel_outcome(&status);
    let outcome = query_outcome_json(Some(&status));
    let response = Json(json!({
        "query_id": query_id.to_string(),
        "status": terminal_status.unwrap_or(if status.durable_outcome.committed {
            "committed"
        } else {
            "running"
        }),
        "terminal_state": terminal_status,
        "state": query_phase_name(status.phase),
        "server_state": query_phase_name(status.phase),
        "started_ms_ago": status.started_at.elapsed().as_millis(),
        "deadline_ms_remaining": status.deadline.map(|deadline| {
            deadline.saturating_duration_since(std::time::Instant::now()).as_millis()
        }),
        "session_id": status.session_id,
        "operation": status.operation,
        "committed": (!status.outcome_unknown).then_some(status.committed),
        "committed_statements": (!status.outcome_unknown).then_some(status.durable_outcome.committed_statements),
        "last_commit_epoch": (!status.outcome_unknown).then_some(status.durable_outcome.last_commit_epoch).flatten(),
        "last_commit_epoch_text": (!status.outcome_unknown).then_some(epoch_text(status.durable_outcome.last_commit_epoch)).flatten(),
        "first_commit_statement_index": (!status.outcome_unknown).then_some(status.durable_outcome.first_commit_statement_index).flatten(),
        "last_commit_statement_index": (!status.outcome_unknown).then_some(status.durable_outcome.last_commit_statement_index).flatten(),
        "cancellation_reason": cancellation_reason_name(status.cancellation_reason),
        "completed_statements": (!status.outcome_unknown).then_some(status.completed_statements),
        "statement_index": (!status.outcome_unknown).then_some(status.statement_index),
        "cancel_outcome": cancel_outcome,
        "retryable": retryable,
        "outcome": outcome,
        "terminal_error": terminal_error,
        "trace": {
            "queue_duration_us": status.queue_duration.as_micros(),
            "planning_duration_us": status.planning_duration.as_micros(),
            "execution_duration_us": status.execution_duration.as_micros(),
            "serialization_duration_us": status.serialization_duration.as_micros(),
            "cancel_requested_phase": status.cancel_requested_phase.map(query_phase_name),
            "cancel_observed_phase": status.cancel_observed_phase.map(query_phase_name),
            "commit_fence_outcome": commit_fence_outcome_name(status.commit_fence_outcome),
        },
    }))
    .into_response();
    with_query_id(response, query_id)
}

async fn cancel_query(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Path(query_id): Path<String>,
    headers: axum::http::HeaderMap,
) -> Response {
    let Ok(query_id) = query_id.parse::<QueryId>() else {
        return query_not_found_response(None);
    };
    if !request_identity_is_current(&state, &principal) {
        return query_not_found_response(Some(query_id));
    }
    let requested_session = match query_session_header(&headers, Some(query_id)) {
        Ok(session_id) => session_id,
        Err(response) => return *response,
    };
    let owner = request_owner(&state, &principal);
    let _lifecycle = state
        .query_lifecycle
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    state.metrics.inc_sql_cancel_requests();
    let Some(status) = state.query_registry.status(query_id) else {
        if let Some((finished_owner, finished_session)) =
            state.query_registry.compact_finished_identity(query_id)
        {
            if !caller_may_manage_query(&state, &principal, finished_owner.as_deref())
                || requested_session
                    .as_deref()
                    .is_some_and(|session| finished_session.as_deref() != Some(session))
            {
                return query_not_found_response(Some(query_id));
            }
            return compact_finished_query_response(query_id);
        }
        return match state.pre_cancellations.insert(
            query_id,
            &owner,
            requested_session.as_deref(),
            CancellationReason::ClientRequest,
        ) {
            Ok(()) => {
                state.metrics.inc_sql_commit_cancel_winner_cancel();
                pre_cancelled_query_response(
                    query_id,
                    CancellationReason::ClientRequest,
                    StatusCode::ACCEPTED,
                )
            }
            Err(pre_cancel::InsertError::MetadataTooLarge) => bad_query_control_request(
                "query owner or session metadata exceeds 256 bytes",
                Some(query_id),
            ),
            Err(
                pre_cancel::InsertError::Full
                | pre_cancel::InsertError::OwnerLimit
                | pre_cancel::InsertError::RateLimited,
            ) => with_query_id(
                (
                    StatusCode::TOO_MANY_REQUESTS,
                    Json(json!({
                        "query_id": query_id.to_string(),
                        "status": "failed_before_commit",
                        "terminal_state": "failed_before_commit",
                        "server_state": "failed",
                        "cancel_outcome": null,
                        "cancellation_reason": null,
                        "committed": false,
                        "committed_statements": 0,
                        "last_commit_epoch": null,
                        "last_commit_epoch_text": null,
                        "first_commit_statement_index": null,
                        "last_commit_statement_index": null,
                        "completed_statements": 0,
                        "statement_index": 0,
                        "retryable": true,
                        "outcome": {
                            "committed": false,
                            "committed_statements": 0,
                            "last_commit_epoch": null,
                            "last_commit_epoch_text": null,
                            "first_commit_statement_index": null,
                            "last_commit_statement_index": null,
                            "completed_statements": 0,
                            "statement_index": 0,
                            "serialization": "not_started",
                        },
                        "error": {
                            "code": "QUERY_REGISTRY_FULL",
                            "message": "pre-registration cancellation limit reached",
                            "query_id": query_id.to_string(),
                            "committed": false,
                            "retryable": true,
                        }
                    })),
                )
                    .into_response(),
                query_id,
            ),
        };
    };
    if !caller_may_manage_query(&state, &principal, status.owner.as_deref())
        || requested_session
            .as_deref()
            .is_some_and(|session| status.session_id.as_deref() != Some(session))
    {
        return query_not_found_response(Some(query_id));
    }
    let (http_status, mut body) = match state.query_registry.cancel(query_id) {
        CancelOutcome::Accepted => (
            {
                state.metrics.inc_sql_commit_cancel_winner_cancel();
                StatusCode::ACCEPTED
            },
            json!({
                "query_id": query_id.to_string(),
                "state": "cancellation_requested",
                "cancel_outcome": "accepted",
            }),
        ),
        CancelOutcome::AlreadyCancelling => (
            StatusCode::OK,
            json!({
                "query_id": query_id.to_string(),
                "state": "cancelling",
                "cancel_outcome": "already_cancelling",
            }),
        ),
        CancelOutcome::TooLate => (
            {
                state.metrics.inc_sql_commit_cancel_winner_commit();
                StatusCode::CONFLICT
            },
            json!({
                "query_id": query_id.to_string(),
                "state": "commit_critical",
                "cancel_outcome": "too_late",
                "committed": status.durable_outcome.committed,
                "outcome": query_outcome_json(Some(&status)),
                "retryable": false,
                "error": {
                    "code": "CANCEL_TOO_LATE",
                    "message": "the query has entered its durable commit phase",
                    "committed": status.durable_outcome.committed,
                    "retryable": false,
                }
            }),
        ),
        CancelOutcome::AlreadyFinished => (
            StatusCode::OK,
            json!({
                "query_id": query_id.to_string(),
                "state": "finished",
                "status": status.terminal_state().map(terminal_state_name),
                "cancel_outcome": "already_finished",
                "code": "QUERY_ALREADY_FINISHED",
                "committed": status.durable_outcome.committed,
                "outcome": query_outcome_json(Some(&status)),
                "retryable": false,
            }),
        ),
        CancelOutcome::NotFound => return query_not_found_response(Some(query_id)),
    };
    let status = state.query_registry.status(query_id).unwrap_or(status);
    let response_status = status.terminal_state().map(terminal_state_name).unwrap_or(
        if status.durable_outcome.committed {
            "committed"
        } else {
            "running"
        },
    );
    if let Some(body) = body.as_object_mut() {
        body.insert("status".into(), json!(response_status));
        body.insert(
            "terminal_state".into(),
            json!(status.terminal_state().map(terminal_state_name)),
        );
        body.insert(
            "committed".into(),
            json!((!status.outcome_unknown).then_some(status.durable_outcome.committed)),
        );
        body.insert(
            "committed_statements".into(),
            json!((!status.outcome_unknown).then_some(status.durable_outcome.committed_statements)),
        );
        body.insert(
            "last_commit_epoch".into(),
            json!((!status.outcome_unknown)
                .then_some(status.durable_outcome.last_commit_epoch)
                .flatten()),
        );
        body.insert(
            "last_commit_epoch_text".into(),
            json!((!status.outcome_unknown)
                .then_some(epoch_text(status.durable_outcome.last_commit_epoch))
                .flatten()),
        );
        body.insert(
            "first_commit_statement_index".into(),
            json!((!status.outcome_unknown)
                .then_some(status.durable_outcome.first_commit_statement_index)
                .flatten()),
        );
        body.insert(
            "last_commit_statement_index".into(),
            json!((!status.outcome_unknown)
                .then_some(status.durable_outcome.last_commit_statement_index)
                .flatten()),
        );
        body.insert(
            "completed_statements".into(),
            json!((!status.outcome_unknown).then_some(status.completed_statements)),
        );
        body.insert(
            "statement_index".into(),
            json!((!status.outcome_unknown).then_some(status.statement_index)),
        );
        body.insert(
            "cancellation_reason".into(),
            json!(cancellation_reason_name(status.cancellation_reason)),
        );
        body.insert("retryable".into(), json!(false));
        body.insert("server_state".into(), json!(query_phase_name(status.phase)));
        body.insert("outcome".into(), query_outcome_json(Some(&status)));
    }
    with_query_id((http_status, Json(body)).into_response(), query_id)
}

#[derive(Deserialize)]
struct SqlContinuationRequest {
    cursor: String,
}

async fn continue_sql_page(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Json(request): Json<SqlContinuationRequest>,
) -> Response {
    if request.cursor.len() > 2_048 {
        return sql_cursor_error_response(
            StatusCode::BAD_REQUEST,
            "INVALID_SQL_CURSOR",
            "invalid SQL continuation cursor",
        );
    }
    if !request_identity_is_current(&state, &principal) {
        return sql_cursor_error_response(
            StatusCode::NOT_FOUND,
            "SQL_CURSOR_NOT_FOUND",
            "SQL continuation result is unavailable",
        );
    }
    let owner = request_owner(&state, &principal);
    let _permit = match state.sql_semaphore.acquire().await {
        Ok(permit) => permit,
        Err(_) => {
            return sql_cursor_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "SQL_ADMISSION_CLOSED",
                "SQL continuation admission is closed",
            );
        }
    };
    let cursor_mac_key = match state.cursor_mac_key.get() {
        Ok(key) => key,
        Err(_) => {
            return sql_cursor_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ENTROPY_UNAVAILABLE",
                "OS CSPRNG unavailable",
            );
        }
    };
    match state.sql_pages.continue_page(
        &request.cursor,
        &owner,
        &cursor_mac_key,
        sql_pages::SqlPageBinding {
            security_version: state.db.security_version(),
            catalog_epoch: state.db.catalog_snapshot().db_epoch,
        },
    ) {
        Ok(page) => {
            let page_byte_count = page.byte_count;
            match tokio::task::spawn_blocking(move || serialize_sql_page(page)).await {
                Ok(Ok(body)) => {
                    state.metrics.add_sql_output_bytes(page_byte_count);
                    sql_page_response(body)
                }
                Ok(Err(_)) => sql_cursor_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "SERIALIZATION_FAILED",
                    "failed to serialize SQL continuation page",
                ),
                Err(_) => sql_cursor_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "SERIALIZATION_WORKER_FAILED",
                    "SQL continuation serialization worker failed",
                ),
            }
        }
        Err(sql_pages::CursorError::Invalid) => sql_cursor_error_response(
            StatusCode::BAD_REQUEST,
            "INVALID_SQL_CURSOR",
            "invalid SQL continuation cursor",
        ),
        Err(sql_pages::CursorError::Expired) => sql_cursor_error_response(
            StatusCode::GONE,
            "SQL_CURSOR_EXPIRED",
            "SQL continuation cursor expired",
        ),
        Err(sql_pages::CursorError::NotFound) => sql_cursor_error_response(
            StatusCode::NOT_FOUND,
            "SQL_CURSOR_NOT_FOUND",
            "SQL continuation result is unavailable",
        ),
        Err(sql_pages::CursorError::PageLimit) => sql_cursor_error_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "RESULT_LIMIT_EXCEEDED",
            "one projected row exceeds the page byte or token limit",
        ),
    }
}

fn sql_cursor_error_response(
    status: StatusCode,
    code: &'static str,
    message: &'static str,
) -> Response {
    (
        status,
        Json(json!({
            "status": "failed_before_commit",
            "terminal_state": "failed_before_commit",
            "server_state": "failed",
            "committed": false,
            "committed_statements": 0,
            "last_commit_epoch": null,
            "last_commit_epoch_text": null,
            "first_commit_statement_index": null,
            "last_commit_statement_index": null,
            "completed_statements": 0,
            "statement_index": 0,
            "cancel_outcome": null,
            "cancellation_reason": null,
            "retryable": false,
            "outcome": {
                "committed": false,
                "committed_statements": 0,
                "last_commit_epoch": null,
                "last_commit_epoch_text": null,
                "first_commit_statement_index": null,
                "last_commit_statement_index": null,
                "completed_statements": 0,
                "statement_index": 0,
                "serialization": "not_started",
            },
            "error": {
                "code": code,
                "message": message,
                "committed": false,
                "retryable": false,
            }
        })),
    )
        .into_response()
}

async fn sql(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    headers: axum::http::HeaderMap,
    Json(req): Json<SqlRequest>,
) -> Response {
    if !state.accepting_sql.load(Ordering::Acquire) {
        return (StatusCode::SERVICE_UNAVAILABLE, "server is shutting down").into_response();
    }
    if !request_identity_is_current(&state, &principal) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    // Session routing: an `X-Session-ID` header routes the request to a pooled
    // long-lived session, enabling cross-request `BEGIN`/`INSERT`/`COMMIT`
    // transactions. Without the header, a fresh ephemeral session is used
    // (the historical behavior).
    let session_id = match query_session_header(&headers, None) {
        Ok(session_id) => session_id,
        Err(response) => return *response,
    };

    let owner = request_owner(&state, &principal);
    if let Some(sid) = session_id {
        let Some(entry) = state.sessions.get(&sid, &owner) else {
            return (
                StatusCode::NOT_FOUND,
                "session not found or not owned by caller",
            )
                .into_response();
        };
        let (options, query_id) = match resolve_query_options(
            &state,
            &headers,
            req.query_id,
            req.timeout_ms,
            owner.clone(),
            Some(sid.clone()),
        ) {
            Ok(options) => options,
            Err(response) => return *response,
        };
        let query = match register_controlled_query(&state, &entry.session, options) {
            Ok(query) => query,
            Err(error) => return tracked_query_error_response(&state, &error, Some(query_id)),
        };
        let registration = RegisteredQueryGuard::new(query);
        if mongreldb_query::contains_boolean_ai_predicate(&req.sql) {
            registration.fail();
            return with_query_id(remote_boolean_ai_error(), query_id);
        }
        let output_limits = match resolve_sql_output_limits(&state, &req, query_id) {
            Ok(limits) => limits,
            Err(response) => {
                registration.fail();
                return *response;
            }
        };
        let (registration, pagination) =
            match resolve_sql_pagination(&headers, &req, output_limits, registration, query_id) {
                Ok(resolved) => resolved,
                Err(response) => return *response,
            };
        let sql_permit =
            match acquire_sql_permit(&state, &entry.session, registration.query()).await {
                Ok(permit) => permit,
                Err(error) => {
                    return tracked_query_error_response(&state, &error, Some(query_id));
                }
            };
        // Registration and global admission happen before this session lock.
        let _guard = tokio::select! {
            guard = entry.lock.lock() => guard,
            _ = registration.query().control().cancelled() => {
                return tracked_query_error_response(
                    &state,
                    &cancellation_checkpoint_error(registration.query()),
                    Some(query_id),
                );
            }
        };
        // Re-check closed: the session may have been closed/evicted between
        // get() and acquiring the lock.
        if entry.is_closed() {
            return (StatusCode::NOT_FOUND, "session no longer available").into_response();
        }
        if req.idempotency_key.is_some() || headers.contains_key("idempotency-key") {
            entry
                .session
                .fire_test_hook(mongreldb_query::SqlTestHookPoint::BeforeServerIdempotencyCheck);
        }
        let (registration, idempotency) = match begin_sql_idempotency(
            &state,
            SqlIdempotencyContext {
                headers: &headers,
                request: &req,
                output_limits,
                owner: &owner,
                session_id: Some(&sid),
                session_in_transaction: entry.session.staged_sql_operation_count().is_some(),
                query_id,
            },
            registration,
        )
        .await
        {
            Ok(resolved) => resolved,
            Err(response) => return response,
        };
        entry.touch();
        let query = registration.into_query();
        execute_sql(
            &state,
            &principal,
            &entry.session,
            ResolvedSqlRequest {
                request: req,
                output_limits,
                idempotency,
                pagination,
            },
            query,
            query_id,
            sql_permit,
        )
        .await
    } else {
        let session = match MongrelSession::open_with_external_modules_as(
            Arc::clone(&state.db),
            state.external_modules.iter().cloned(),
            request_principal(&state, &principal),
        ) {
            Ok(session) => session.with_query_registry(Arc::clone(&state.query_registry)),
            Err(e) => return (status_for_query_error(&e), e.to_string()).into_response(),
        };
        let (options, query_id) = match resolve_query_options(
            &state,
            &headers,
            req.query_id,
            req.timeout_ms,
            owner.clone(),
            None,
        ) {
            Ok(options) => options,
            Err(response) => return *response,
        };
        let query = match register_controlled_query(&state, &session, options) {
            Ok(query) => query,
            Err(error) => return tracked_query_error_response(&state, &error, Some(query_id)),
        };
        let registration = RegisteredQueryGuard::new(query);
        if mongreldb_query::contains_boolean_ai_predicate(&req.sql) {
            registration.fail();
            return with_query_id(remote_boolean_ai_error(), query_id);
        }
        let output_limits = match resolve_sql_output_limits(&state, &req, query_id) {
            Ok(limits) => limits,
            Err(response) => {
                registration.fail();
                return *response;
            }
        };
        let (registration, pagination) =
            match resolve_sql_pagination(&headers, &req, output_limits, registration, query_id) {
                Ok(resolved) => resolved,
                Err(response) => return *response,
            };
        let (registration, idempotency) = match begin_sql_idempotency(
            &state,
            SqlIdempotencyContext {
                headers: &headers,
                request: &req,
                output_limits,
                owner: &owner,
                session_id: None,
                session_in_transaction: false,
                query_id,
            },
            registration,
        )
        .await
        {
            Ok(resolved) => resolved,
            Err(response) => return response,
        };
        let sql_permit = match acquire_sql_permit(&state, &session, registration.query()).await {
            Ok(permit) => permit,
            Err(error) => {
                if let Some(idempotency) = idempotency {
                    idempotency.abort();
                }
                return tracked_query_error_response(&state, &error, Some(query_id));
            }
        };
        let query = registration.into_query();
        execute_sql(
            &state,
            &principal,
            &session,
            ResolvedSqlRequest {
                request: req,
                output_limits,
                idempotency,
                pagination,
            },
            query,
            query_id,
            sql_permit,
        )
        .await
    }
}

/// Run one SQL request against a given session: bump counters, audit DDL
/// (after execution, with redaction), log slow queries on both success and
/// failure, and dispatch the response format. Shared by the pooled-session and
/// fresh-session paths.
async fn execute_sql(
    state: &AppState,
    principal: &Option<mongreldb_core::Principal>,
    session: &MongrelSession,
    request: ResolvedSqlRequest,
    query: RegisteredSqlQuery,
    query_id: QueryId,
    sql_permit: tokio::sync::OwnedSemaphorePermit,
) -> Response {
    let ResolvedSqlRequest {
        request: req,
        output_limits,
        idempotency,
        pagination,
    } = request;
    // Keep a direct handle until the durable receipt is persisted. Finished
    // tombstone eviction must not make a committed idempotent write ambiguous.
    let idempotency_query = idempotency.as_ref().map(|_| query.clone());
    state.metrics.inc_sql_queries();
    let audited = audit::is_audited_sql(&req.sql);
    let actor = request_owner(state, principal);
    let page_binding = sql_pages::SqlPageBinding {
        security_version: state.db.security_version(),
        catalog_epoch: state.db.catalog_snapshot().db_epoch,
    };
    let start = std::time::Instant::now();
    // NOTE: deliberately NOT using `run_sql_traced` here. Its thread-local
    // push/pop spans an `.await`, and on a multi-threaded tokio runtime the
    // task can resume on a different thread, corrupting the trace stack and
    // leaking scopes. Wall-clock timing is sufficient for slow-query detection
    // and works across awaits.
    let result = if let Some(pagination) = pagination {
        match session
            .run_with_query_for_serialization_with_limits(
                &req.sql,
                query,
                mongreldb_query::SqlCollectionLimits::new(output_limits.0, output_limits.1),
            )
            .await
        {
            Ok(output) => Ok(dispatch_paginated_sql(
                state,
                output,
                query_id,
                &actor,
                pagination,
                output_limits,
                session.sql_test_hook(),
                page_binding,
            )
            .await),
            Err(error) => Err(error),
        }
    } else if req.format.as_deref() == Some("arrow-stream") {
        match session
            .run_stream_with_query_for_serialization(&req.sql, query)
            .await
        {
            Ok((stream, completion)) => Ok(sql_arrow_stream_response_controlled(
                stream,
                completion,
                sql_permit,
                output_limits,
                state,
                query_id,
                session.sql_test_hook(),
            )),
            Err(error) => Err(error),
        }
    } else {
        match session
            .run_with_query_for_serialization_with_limits(
                &req.sql,
                query,
                mongreldb_query::SqlCollectionLimits::new(output_limits.0, output_limits.1),
            )
            .await
        {
            Ok(output) => Ok(dispatch_buffered_sql_format(
                state,
                req.format.as_deref(),
                output,
                query_id,
                session.sql_test_hook(),
                output_limits,
            )
            .await),
            Err(error) => Err(error),
        }
    };
    let elapsed = start.elapsed();
    // Slow-query logging covers BOTH success and failure (the slowest errors
    // matter most for diagnosis), checked before branching on the outcome.
    if elapsed >= state.slow_query_threshold {
        state.metrics.inc_slow_queries();
        eprintln!(
            "[slow-query] {}\u{00b5}s query_id={} operation={}",
            elapsed.as_micros(),
            query_id,
            safe_sql_operation(&req.sql)
        );
    }
    // Audit DDL/privilege AFTER execution so the outcome (ok/fail) is captured.
    // `redacted_ddl_detail` never logs credential literals.
    if audited {
        let (action, detail) = audit::redacted_ddl_detail(&req.sql, result.is_ok());
        state.audit.record(actor, action, detail);
    }
    let response = match result {
        Ok(response) => with_query_id(response, query_id),
        Err(e) => {
            state.metrics.inc_sql_errors();
            tracked_query_error_response(state, &e, Some(query_id))
        }
    };
    let Some(idempotency) = idempotency else {
        return response;
    };
    let status = idempotency_query.map(|query| query.status());
    if let Some(receipt) = status.as_ref().and_then(sql_terminal_idempotency_receipt) {
        let (expires_at_ms, persisted) = idempotency.commit(receipt.clone());
        return sql_idempotency_receipt_response(
            query_id,
            &receipt,
            false,
            expires_at_ms,
            persisted,
        );
    }
    if status.as_ref().is_some_and(can_abort_idempotency_intent) {
        idempotency.abort();
    }
    response
}

fn can_abort_idempotency_intent(status: &mongreldb_query::QueryStatus) -> bool {
    !status.outcome_unknown
        && !status.durable_outcome.committed
        && matches!(
            status.terminal_state(),
            Some(
                mongreldb_query::QueryTerminalState::FailedBeforeCommit
                    | mongreldb_query::QueryTerminalState::CancelledBeforeCommit
                    | mongreldb_query::QueryTerminalState::DeadlineBeforeCommit
            )
        )
}

fn safe_sql_operation(sql: &str) -> String {
    sql.split_whitespace()
        .next()
        .unwrap_or("UNKNOWN")
        .chars()
        .filter(|character| character.is_ascii_alphabetic())
        .take(16)
        .collect::<String>()
        .to_ascii_uppercase()
}

fn remote_boolean_ai_error() -> Response {
    (
        StatusCode::BAD_REQUEST,
        "Boolean ANN/Sparse SQL is disabled remotely; use scored SQL functions",
    )
        .into_response()
}

#[derive(Debug)]
struct SerializedOutput {
    bytes: Vec<u8>,
    arrow: bool,
}

#[derive(Debug)]
enum BufferedSerializationError {
    Query(mongreldb_query::MongrelQueryError),
    Limit(String),
    Encoding(String),
}

struct LimitedOutput {
    bytes: Vec<u8>,
    max_bytes: usize,
    exceeded: bool,
}

impl LimitedOutput {
    fn new(max_bytes: usize) -> Self {
        Self {
            bytes: Vec::new(),
            max_bytes,
            exceeded: false,
        }
    }
}

impl std::io::Write for LimitedOutput {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.bytes.len().saturating_add(bytes.len()) > self.max_bytes {
            self.exceeded = true;
            return Err(std::io::Error::other("SQL output byte limit exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn serialize_buffered_output(
    format: &str,
    batches: &[arrow::record_batch::RecordBatch],
    query: &RegisteredSqlQuery,
    max_rows: usize,
    max_bytes: usize,
    test_hook: Option<&mongreldb_query::SqlTestHook>,
) -> std::result::Result<SerializedOutput, BufferedSerializationError> {
    const ROW_CHECKPOINT_INTERVAL: usize = 256;
    let mut rows = 0usize;
    let mut writer_output = LimitedOutput::new(max_bytes);

    if format == "arrow" {
        if batches.is_empty() {
            if let Some(hook) = test_hook {
                hook(mongreldb_query::SqlTestHookPoint::AfterSerialization);
            }
            return Ok(SerializedOutput {
                bytes: Vec::new(),
                arrow: true,
            });
        }
        let schema = batches[0].schema();
        let encoding_result = (|| {
            let mut writer =
                arrow::ipc::writer::FileWriter::try_new(&mut writer_output, schema.as_ref())
                    .map_err(|error| error.to_string())?;
            for batch in batches {
                for offset in (0..batch.num_rows()).step_by(ROW_CHECKPOINT_INTERVAL) {
                    if let Some(hook) = test_hook {
                        hook(mongreldb_query::SqlTestHookPoint::BeforeSerializationBatch);
                    }
                    query.checkpoint().map_err(|error| error.to_string())?;
                    let length = ROW_CHECKPOINT_INTERVAL.min(batch.num_rows() - offset);
                    rows = rows.saturating_add(length);
                    if rows > max_rows {
                        return Err("SQL output row limit exceeded".into());
                    }
                    writer
                        .write(&batch.slice(offset, length))
                        .map_err(|error| error.to_string())?;
                }
            }
            writer.finish().map_err(|error| error.to_string())
        })();
        if let Err(error) = encoding_result {
            if let Err(query_error) = query.checkpoint() {
                return Err(BufferedSerializationError::Query(query_error));
            }
            if writer_output.exceeded || rows > max_rows {
                return Err(BufferedSerializationError::Limit(error));
            }
            return Err(BufferedSerializationError::Encoding(error));
        }
        if let Some(hook) = test_hook {
            hook(mongreldb_query::SqlTestHookPoint::AfterSerialization);
        }
        return Ok(SerializedOutput {
            bytes: writer_output.bytes,
            arrow: true,
        });
    }

    let encoding_result = (|| {
        let mut writer = arrow::json::writer::ArrayWriter::new(&mut writer_output);
        for batch in batches {
            for offset in (0..batch.num_rows()).step_by(ROW_CHECKPOINT_INTERVAL) {
                if let Some(hook) = test_hook {
                    hook(mongreldb_query::SqlTestHookPoint::BeforeSerializationBatch);
                }
                query.checkpoint().map_err(|error| error.to_string())?;
                let length = ROW_CHECKPOINT_INTERVAL.min(batch.num_rows() - offset);
                rows = rows.saturating_add(length);
                if rows > max_rows {
                    return Err("SQL output row limit exceeded".into());
                }
                let slice = batch.slice(offset, length);
                writer
                    .write_batches(&[&slice])
                    .map_err(|error| error.to_string())?;
            }
        }
        writer.finish().map_err(|error| error.to_string())
    })();
    if let Err(error) = encoding_result {
        if let Err(query_error) = query.checkpoint() {
            return Err(BufferedSerializationError::Query(query_error));
        }
        if writer_output.exceeded || rows > max_rows {
            return Err(BufferedSerializationError::Limit(error));
        }
        return Err(BufferedSerializationError::Encoding(error));
    }
    if let Some(hook) = test_hook {
        hook(mongreldb_query::SqlTestHookPoint::AfterSerialization);
    }
    Ok(SerializedOutput {
        bytes: writer_output.bytes,
        arrow: false,
    })
}

/// Serialize a DataFusion record-batch stream as Arrow streaming IPC. The body
/// holds only the active query batch and one serialized IPC message.
#[cfg(test)]
fn sql_arrow_stream_response(batches: mongreldb_query::MongrelRecordBatchStream) -> Response {
    use futures::{stream, StreamExt};

    const STREAM_CT: &str = "application/vnd.apache.arrow.stream";

    let schema = batches.schema();
    let mut writer = match arrow::ipc::writer::StreamWriter::try_new(Vec::new(), schema.as_ref()) {
        Ok(w) => w,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("arrow stream init error: {e}"),
            )
                .into_response()
        }
    };
    // `try_new` synchronously writes the schema message; drain it so it becomes
    // the first chunk yielded to the client.
    let schema_chunk: Vec<u8> = std::mem::take(writer.get_mut());
    let batch_stream = stream::unfold(
        (batches, Some(writer)),
        |(mut batches, writer)| async move {
            let mut writer = writer?;
            match batches.next().await {
                Some(Ok(batch)) => match writer.write(&batch) {
                    Ok(()) => {
                        let chunk = std::mem::take(writer.get_mut());
                        Some((Ok(chunk), (batches, Some(writer))))
                    }
                    Err(error) => Some((Err(std::io::Error::other(error)), (batches, None))),
                },
                Some(Err(error)) => Some((Err(std::io::Error::other(error)), (batches, None))),
                None => match writer.finish() {
                    Ok(()) => {
                        let chunk = std::mem::take(writer.get_mut());
                        Some((Ok(chunk), (batches, None)))
                    }
                    Err(error) => Some((Err(std::io::Error::other(error)), (batches, None))),
                },
            }
        },
    );

    // Schema first, then batches + EOS. Each item is already `Result<Vec<u8>,
    // io::Error>` so a mid-stream encode failure surfaces as a body error.
    let schema_item: Result<Vec<u8>, std::io::Error> = Ok(schema_chunk);
    let full = stream::iter([schema_item]).chain(batch_stream);
    let body = axum::body::Body::from_stream(full);
    ([(header::CONTENT_TYPE, STREAM_CT)], body).into_response()
}

fn sql_arrow_stream_response_controlled(
    batches: mongreldb_query::MongrelRecordBatchStream,
    completion: SqlStreamCompletion,
    sql_permit: tokio::sync::OwnedSemaphorePermit,
    limits: (usize, usize),
    state: &AppState,
    query_id: QueryId,
    test_hook: Option<mongreldb_query::SqlTestHook>,
) -> Response {
    use futures::{stream, StreamExt};

    const STREAM_CT: &str = "application/vnd.apache.arrow.stream";
    let (max_rows, max_bytes) = limits;
    let schema = batches.schema();
    let mut writer = match arrow::ipc::writer::StreamWriter::try_new(Vec::new(), schema.as_ref()) {
        Ok(writer) => writer,
        Err(error) => {
            completion.fail_serialization();
            drop(batches);
            return terminal_server_error_response(
                state,
                query_id,
                StatusCode::INTERNAL_SERVER_ERROR,
                "SERIALIZATION_FAILED",
                format!("arrow stream init error: {error}"),
            );
        }
    };
    let schema_chunk = std::mem::take(writer.get_mut());
    if schema_chunk.len() > max_bytes {
        completion.fail_result_limit();
        drop(batches);
        return terminal_server_error_response(
            state,
            query_id,
            StatusCode::PAYLOAD_TOO_LARGE,
            "RESULT_LIMIT_EXCEEDED",
            "SQL output byte limit exceeded",
        );
    }
    let metrics = Arc::clone(&state.metrics);
    metrics.add_sql_output_bytes(schema_chunk.len());
    let batch_stream = stream::unfold(
        (
            batches,
            Some(writer),
            completion,
            Some(sql_permit),
            0usize,
            schema_chunk.len(),
            metrics,
        ),
        move |(mut batches, writer, completion, permit, rows, bytes, metrics)| {
            let test_hook = test_hook.clone();
            async move {
                let mut writer = writer?;
                match batches.next().await {
                    Some(Ok(batch)) => {
                        let next_rows = rows.saturating_add(batch.num_rows());
                        if next_rows > max_rows {
                            completion.fail_result_limit();
                            return Some((
                                Err(std::io::Error::other("SQL output row limit exceeded")),
                                (batches, None, completion, permit, next_rows, bytes, metrics),
                            ));
                        }
                        match writer.write(&batch) {
                            Ok(()) => {
                                let chunk = std::mem::take(writer.get_mut());
                                let next_bytes = bytes.saturating_add(chunk.len());
                                if next_bytes > max_bytes {
                                    completion.fail_result_limit();
                                    return Some((
                                        Err(std::io::Error::other(
                                            "SQL output byte limit exceeded",
                                        )),
                                        (
                                            batches, None, completion, permit, next_rows,
                                            next_bytes, metrics,
                                        ),
                                    ));
                                }
                                metrics.add_sql_output_bytes(chunk.len());
                                Some((
                                    Ok(chunk),
                                    (
                                        batches,
                                        Some(writer),
                                        completion,
                                        permit,
                                        next_rows,
                                        next_bytes,
                                        metrics,
                                    ),
                                ))
                            }
                            Err(error) => {
                                completion.fail_serialization();
                                Some((
                                    Err(std::io::Error::other(error)),
                                    (batches, None, completion, permit, rows, bytes, metrics),
                                ))
                            }
                        }
                    }
                    Some(Err(error)) => Some((
                        Err(std::io::Error::other(error)),
                        (batches, None, completion, permit, rows, bytes, metrics),
                    )),
                    None => match writer.finish() {
                        Ok(()) => {
                            let chunk = std::mem::take(writer.get_mut());
                            let next_bytes = bytes.saturating_add(chunk.len());
                            if next_bytes > max_bytes {
                                completion.fail_result_limit();
                                return Some((
                                    Err(std::io::Error::other("SQL output byte limit exceeded")),
                                    (batches, None, completion, permit, rows, next_bytes, metrics),
                                ));
                            }
                            if let Some(hook) = test_hook {
                                hook(mongreldb_query::SqlTestHookPoint::AfterSerialization);
                            }
                            match completion.try_complete() {
                                Ok(()) => {
                                    metrics.add_sql_output_bytes(chunk.len());
                                    Some((
                                        Ok(chunk),
                                        (
                                            batches, None, completion, permit, rows, next_bytes,
                                            metrics,
                                        ),
                                    ))
                                }
                                Err(error) => {
                                    metrics.inc_sql_errors();
                                    Some((
                                        Err(std::io::Error::other(error.to_string())),
                                        (batches, None, completion, permit, rows, bytes, metrics),
                                    ))
                                }
                            }
                        }
                        Err(error) => {
                            completion.fail_serialization();
                            Some((
                                Err(std::io::Error::other(error)),
                                (batches, None, completion, permit, rows, bytes, metrics),
                            ))
                        }
                    },
                }
            }
        },
    );
    let schema_item: Result<Vec<u8>, std::io::Error> = Ok(schema_chunk);
    let body = axum::body::Body::from_stream(stream::iter([schema_item]).chain(batch_stream));
    ([(header::CONTENT_TYPE, STREAM_CT)], body).into_response()
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
    OptionalPrincipal(principal): OptionalPrincipal,
    Json(req): Json<TxnRequest>,
) -> Response {
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

    state.metrics.inc_txns();
    let mut transaction = state.db.begin_as(request_principal(&state, &principal));
    let result = (|| {
        for (table, action) in &parsed {
            match action {
                TxnAction::Put(cells) => {
                    transaction.put(table, cells.clone())?;
                }
                TxnAction::Delete(rid) => {
                    transaction.delete(table, mongreldb_core::RowId(*rid))?;
                }
            }
        }
        transaction.commit()
    })();
    match result {
        Ok(epoch) => Json(json!({
            "status": "committed",
            "epoch": epoch.0,
            "epoch_text": epoch.0.to_string()
        }))
        .into_response(),
        Err(error) => crate::kit::durable_core_error_response(&error)
            .unwrap_or_else(|| (status_for_error(&error), error.to_string()).into_response()),
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

    #[test]
    fn slow_query_operation_does_not_include_literals() {
        let sql = "CREATE USER alice PASSWORD 'never-log-this'";
        let operation = safe_sql_operation(sql);
        assert_eq!(operation, "CREATE");
        assert!(!operation.contains("never-log-this"));
    }

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

    #[tokio::test]
    async fn capabilities_advertise_sql_cancellation_v2() {
        let dir = tempdir().unwrap();
        let db = Arc::new(Database::create(dir.path()).unwrap());
        let app = build_app_with_config(db, std::iter::empty(), None, None);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let body: serde_json::Value = reqwest::Client::new()
            .get(format!("http://{addr}/capabilities"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(body["sql_cancellation"]["version"], 2);
        assert_eq!(body["sql_cancellation"]["client_query_ids"], true);
        assert_eq!(body["sql_cancellation"]["cancel_endpoint"], true);
        assert_eq!(body["sql_cancellation"]["query_status"], true);
        assert_eq!(body["sql_cancellation"]["pre_registration_cancel"], true);
        assert_eq!(body["sql_cancellation"]["stream_disconnect_cancels"], true);
        assert_eq!(body["sql_idempotency"]["version"], 1);
        assert_eq!(
            body["sql_idempotency"]["indeterminate_never_reexecutes"],
            true
        );
        assert_eq!(body["sql_pagination"]["version"], 1);
        assert_eq!(
            body["sql_pagination"]["continuation_endpoint"],
            "/sql/continue"
        );
    }
}

#[cfg(test)]
mod query_response_tests {
    use super::*;

    #[test]
    fn cancellation_reason_names_are_stable_snake_case() {
        assert_eq!(cancellation_reason_name(CancellationReason::None), "none");
        assert_eq!(
            cancellation_reason_name(CancellationReason::ClientRequest),
            "client_request"
        );
        assert_eq!(
            cancellation_reason_name(CancellationReason::ClientDisconnected),
            "client_disconnected"
        );
        assert_eq!(
            cancellation_reason_name(CancellationReason::SessionClosed),
            "session_closed"
        );
        assert_eq!(
            cancellation_reason_name(CancellationReason::ServerShutdown),
            "server_shutdown"
        );
        assert_eq!(
            cancellation_reason_name(CancellationReason::Deadline),
            "deadline"
        );
    }

    #[test]
    fn unknown_outcome_never_proves_idempotency_intent_safe_to_abort() {
        let registry = Arc::new(SqlQueryRegistry::default());
        let unknown_id: QueryId = "102132435465768798a9bacbdcedfe0f".parse().unwrap();
        let unknown = registry
            .register(SqlQueryOptions {
                query_id: Some(unknown_id),
                ..SqlQueryOptions::default()
            })
            .unwrap();
        unknown.mark_outcome_unknown();
        unknown.fail();
        assert!(!can_abort_idempotency_intent(
            &registry.status(unknown_id).unwrap()
        ));

        let failed_id: QueryId = "2031425364758697a8b9cadbecfd0e1f".parse().unwrap();
        let failed = registry
            .register(SqlQueryOptions {
                query_id: Some(failed_id),
                ..SqlQueryOptions::default()
            })
            .unwrap();
        failed.fail();
        assert!(can_abort_idempotency_intent(
            &registry.status(failed_id).unwrap()
        ));
    }

    #[test]
    fn unknown_outcome_never_becomes_durable_receipt() {
        let registry = Arc::new(SqlQueryRegistry::default());
        let query = registry.register(SqlQueryOptions::default()).unwrap();
        query.record_commit(0, 42);
        query.mark_outcome_unknown();
        query.fail();
        let status = registry.status(query.id()).unwrap();
        assert!(status.durable_outcome.committed);
        assert!(status.outcome_unknown);
        assert!(sql_terminal_idempotency_receipt(&status).is_none());
    }

    #[test]
    fn successful_noop_write_becomes_noncommitting_receipt() {
        let registry = Arc::new(SqlQueryRegistry::default());
        let query = registry.register(SqlQueryOptions::default()).unwrap();
        query
            .transition(SqlQueryPhase::Queued, SqlQueryPhase::Executing)
            .unwrap();
        query.try_complete().unwrap();
        let status = registry.status(query.id()).unwrap();
        assert!(!status.durable_outcome.committed);
        assert!(!can_abort_idempotency_intent(&status));
        let receipt = sql_terminal_idempotency_receipt(&status).unwrap();
        assert_eq!(receipt.status, "completed");
        assert!(!receipt.outcome.committed);
        assert_eq!(receipt.outcome.committed_statements, 0);
        assert_eq!(receipt.outcome.last_commit_epoch, None);
    }

    #[test]
    fn conflicting_idempotency_key_sources_are_rejected() {
        let request = SqlRequest {
            sql: "INSERT INTO items VALUES (1)".into(),
            format: None,
            query_id: None,
            timeout_ms: None,
            max_output_rows: None,
            max_output_bytes: None,
            idempotency_key: Some("body-key".into()),
            pagination: None,
        };
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("idempotency-key", "header-key".parse().unwrap());
        assert_eq!(
            requested_sql_idempotency_key(&headers, &request),
            Err("body idempotency_key and Idempotency-Key header must match")
        );

        headers.insert("idempotency-key", "body-key".parse().unwrap());
        assert_eq!(
            requested_sql_idempotency_key(&headers, &request),
            Ok(Some("body-key".into()))
        );
    }

    #[test]
    fn idempotency_binding_includes_pagination_semantics() {
        let mut request = SqlRequest {
            sql: "INSERT INTO items VALUES (1)".into(),
            format: None,
            query_id: None,
            timeout_ms: None,
            max_output_rows: None,
            max_output_bytes: None,
            idempotency_key: Some("key".into()),
            pagination: None,
        };
        let unpaged = sql_idempotency_binding(&request, (100, 1_024), None, 60_000).unwrap();
        request.pagination = Some(SqlPaginationRequest {
            page_size_rows: 10,
            projection: vec!["id".into()],
            max_page_bytes: Some(512),
            max_page_tokens: Some(128),
        });
        let paged = sql_idempotency_binding(&request, (100, 1_024), None, 60_000).unwrap();
        assert_ne!(unpaged.request_semantics_hash, paged.request_semantics_hash);
    }

    #[test]
    fn paginated_decode_stops_nested_heap_amplification_at_budget() {
        let registry = Arc::new(SqlQueryRegistry::default());
        let query = registry.register(SqlQueryOptions::default()).unwrap();
        let json = format!("[[{}]]", vec!["null"; 10_000].join(","));
        let mut deserializer = serde_json::Deserializer::from_slice(json.as_bytes());
        let mut budget = PaginatedDecodeBudget {
            used: 0,
            limit: 4 * 1024,
            nodes: 0,
            exceeded: false,
            query: &query,
            test_hook: None,
        };
        let error = serde::de::DeserializeSeed::deserialize(
            BudgetedJsonRowsSeed {
                budget: &mut budget,
            },
            &mut deserializer,
        )
        .unwrap_err();
        assert!(error.to_string().contains(PAGINATED_MEMORY_LIMIT_ERROR));
        assert!(budget.exceeded);
        assert!(budget.nodes < 100, "decoded {} nodes", budget.nodes);
        query.fail();
    }

    #[tokio::test]
    async fn unknown_outcome_response_never_claims_no_commit() {
        let registry = Arc::new(SqlQueryRegistry::default());
        let query = registry.register(SqlQueryOptions::default()).unwrap();
        let query_id = query.id();
        query
            .transition(SqlQueryPhase::Queued, SqlQueryPhase::Executing)
            .unwrap();
        let error = query.outcome_unknown_error("fenced maintenance failed");
        query.fail();
        let status = registry.status(query_id).unwrap();
        assert_eq!(
            status.terminal_state(),
            Some(mongreldb_query::QueryTerminalState::OutcomeUnknown)
        );

        let response = query_error_response_with_status(&error, Some(query_id), Some(&status));
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["status"], "outcome_unknown");
        assert_eq!(body["error"]["code"], "QUERY_OUTCOME_UNKNOWN");
        assert!(body["committed"].is_null());
        assert!(body["committed_statements"].is_null());
        assert!(body["last_commit_epoch"].is_null());
        assert!(body["completed_statements"].is_null());
        assert!(body["statement_index"].is_null());
        assert!(body["outcome"]["committed"].is_null());
        assert!(body["error"]["committed"].is_null());
    }

    #[tokio::test]
    async fn retryable_idempotency_error_survives_terminal_status() {
        let registry = Arc::new(SqlQueryRegistry::default());
        let query = registry.register(SqlQueryOptions::default()).unwrap();
        let query_id = query.id();
        let response = registered_sql_error_response(
            RegisteredQueryGuard::new(query),
            query_id,
            StatusCode::SERVICE_UNAVAILABLE,
            "IDEMPOTENCY_STORE_UNAVAILABLE",
            "could not durably reserve the SQL idempotency key",
            true,
        );
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["retryable"], true);
        assert_eq!(body["error"]["retryable"], true);

        let status = registry.status(query_id).unwrap();
        assert_eq!(
            status.terminal_error.as_ref().unwrap().code,
            "IDEMPOTENCY_STORE_UNAVAILABLE"
        );
        assert!(terminal_error_retryable(status.terminal_error.as_ref()));
    }

    #[tokio::test]
    async fn committed_idempotency_terminal_error_is_a_receipt() {
        let query_id: QueryId = "00112233445566778899aabbccddeeff".parse().unwrap();
        let receipt = sql_idempotency::SqlDurableReceipt {
            original_query_id: query_id.to_string(),
            status: "committed_with_error".into(),
            server_state: "failed".into(),
            cancellation_reason: "client_disconnected".into(),
            outcome: sql_idempotency::SqlReceiptOutcome {
                committed: true,
                committed_statements: 1,
                last_commit_epoch: Some(42),
                last_commit_epoch_text: Some("42".into()),
                first_commit_statement_index: Some(0),
                last_commit_statement_index: Some(0),
                completed_statements: 1,
                statement_index: 0,
                serialization: "failed".into(),
            },
            terminal_error: Some(sql_idempotency::SqlReceiptTerminalError {
                code: "SERIALIZATION_FAILED_AFTER_COMMIT".into(),
                category: "serialization".into(),
            }),
        };
        let response = sql_idempotency_receipt_response(query_id, &receipt, false, 99, true);
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["status"], "committed_with_error");
        assert_eq!(body["server_state"], "failed");
        assert_eq!(body["cancellation_reason"], "client_disconnected");
        assert_eq!(body["first_commit_statement_index"], 0);
        assert_eq!(body["last_commit_statement_index"], 0);
        assert_eq!(
            body["terminal_error"]["code"],
            "SERIALIZATION_FAILED_AFTER_COMMIT"
        );
    }

    #[test]
    fn durable_replay_restores_terminal_status_parity() {
        let registry = Arc::new(SqlQueryRegistry::default());
        let query_id: QueryId = "11223344556677889900aabbccddeeff".parse().unwrap();
        let query = registry
            .register(SqlQueryOptions {
                query_id: Some(query_id),
                ..SqlQueryOptions::default()
            })
            .unwrap();
        let receipt = sql_idempotency::SqlDurableReceipt {
            original_query_id: "00112233445566778899aabbccddeeff".into(),
            status: "cancelled_after_commit".into(),
            server_state: "cancelled".into(),
            cancellation_reason: "client_disconnected".into(),
            outcome: sql_idempotency::SqlReceiptOutcome {
                committed: true,
                committed_statements: 1,
                last_commit_epoch: Some(42),
                last_commit_epoch_text: Some("42".into()),
                first_commit_statement_index: Some(0),
                last_commit_statement_index: Some(0),
                completed_statements: 1,
                statement_index: 1,
                serialization: "failed".into(),
            },
            terminal_error: Some(sql_idempotency::SqlReceiptTerminalError {
                code: "QUERY_CANCELLED_AFTER_COMMIT".into(),
                category: "cancellation".into(),
            }),
        };
        restore_idempotency_replay(RegisteredQueryGuard::new(query), &receipt).unwrap();
        let status = registry.status(query_id).unwrap();
        assert_eq!(status.phase, SqlQueryPhase::Cancelled);
        assert_eq!(
            status.terminal_state(),
            Some(mongreldb_query::QueryTerminalState::CancelledAfterCommit)
        );
        assert_eq!(
            status.cancellation_reason,
            CancellationReason::ClientDisconnected
        );
        assert_eq!(status.durable_outcome.committed_statements, 1);
        assert_eq!(status.durable_outcome.last_commit_epoch, Some(42));
        assert_eq!(
            status.terminal_error.unwrap().code,
            "QUERY_CANCELLED_AFTER_COMMIT"
        );
    }

    #[test]
    fn durable_replay_rejects_invalid_authenticated_state() {
        let registry = Arc::new(SqlQueryRegistry::default());
        let query = registry.register(SqlQueryOptions::default()).unwrap();
        let receipt = sql_idempotency::SqlDurableReceipt {
            original_query_id: query.id().to_string(),
            status: "invented_terminal_state".into(),
            server_state: "completed".into(),
            cancellation_reason: "none".into(),
            outcome: sql_idempotency::SqlReceiptOutcome {
                committed: true,
                committed_statements: 1,
                last_commit_epoch: Some(42),
                last_commit_epoch_text: Some("42".into()),
                first_commit_statement_index: Some(0),
                last_commit_statement_index: Some(0),
                completed_statements: 1,
                statement_index: 0,
                serialization: "succeeded".into(),
            },
            terminal_error: None,
        };
        let error =
            restore_idempotency_replay(RegisteredQueryGuard::new(query), &receipt).unwrap_err();
        assert!(error.to_string().contains("invalid terminal state"));
    }

    #[test]
    fn durable_replay_cancel_wins_before_receipt_response() {
        let registry = Arc::new(SqlQueryRegistry::default());
        let query_id: QueryId = "22334455667788990011aabbccddeeff".parse().unwrap();
        let query = registry
            .register(SqlQueryOptions {
                query_id: Some(query_id),
                ..SqlQueryOptions::default()
            })
            .unwrap();
        assert_eq!(
            query.request_cancel(CancellationReason::ClientRequest),
            CancelOutcome::Accepted
        );
        let receipt = sql_idempotency::SqlDurableReceipt {
            original_query_id: "00112233445566778899aabbccddeeff".into(),
            status: "completed".into(),
            server_state: "completed".into(),
            cancellation_reason: "none".into(),
            outcome: sql_idempotency::SqlReceiptOutcome {
                committed: true,
                committed_statements: 1,
                last_commit_epoch: Some(42),
                last_commit_epoch_text: Some("42".into()),
                first_commit_statement_index: Some(0),
                last_commit_statement_index: Some(0),
                completed_statements: 1,
                statement_index: 0,
                serialization: "succeeded".into(),
            },
            terminal_error: None,
        };
        let error = restore_idempotency_replay(RegisteredQueryGuard::new(query), &receipt)
            .expect_err("accepted cancellation must suppress replay success");
        assert!(matches!(
            error,
            mongreldb_query::MongrelQueryError::QueryCancelled { .. }
        ));
        let status = registry.status(query_id).unwrap();
        assert_eq!(status.phase, SqlQueryPhase::Cancelled);
        assert!(status.durable_outcome.committed);
    }

    #[test]
    fn direct_query_handle_preserves_receipt_after_tombstone_eviction() {
        let registry = Arc::new(SqlQueryRegistry::new(
            1,
            1,
            usize::MAX,
            std::time::Duration::from_secs(60),
        ));
        let first = registry.register(SqlQueryOptions::default()).unwrap();
        first
            .transition(SqlQueryPhase::Queued, SqlQueryPhase::Executing)
            .unwrap();
        first.record_commit(0, 42);
        first.complete_current_statement();
        first.try_complete().unwrap();

        let second = registry.register(SqlQueryOptions::default()).unwrap();
        second
            .transition(SqlQueryPhase::Queued, SqlQueryPhase::Executing)
            .unwrap();
        second.try_complete().unwrap();

        assert!(registry.status(first.id()).is_none());
        let receipt = sql_terminal_idempotency_receipt(&first.status()).unwrap();
        assert_eq!(receipt.outcome.committed_statements, 1);
        assert_eq!(receipt.outcome.last_commit_epoch, Some(42));
    }

    #[test]
    fn cancellation_checkpoint_mismatch_returns_typed_error() {
        let registry = Arc::new(SqlQueryRegistry::default());
        let query = registry.register(SqlQueryOptions::default()).unwrap();
        assert!(matches!(
            cancellation_checkpoint_error(&query),
            mongreldb_query::MongrelQueryError::InvalidQueryState(_)
        ));
        assert_eq!(
            query.request_cancel(CancellationReason::ClientRequest),
            CancelOutcome::Accepted
        );
        assert!(matches!(
            cancellation_checkpoint_error(&query),
            mongreldb_query::MongrelQueryError::QueryCancelled { .. }
        ));
    }

    #[tokio::test]
    async fn cancellation_after_commit_reports_durable_outcome() {
        let registry = Arc::new(SqlQueryRegistry::default());
        let query = registry.register(SqlQueryOptions::default()).unwrap();
        let query_id = query.id();
        query
            .transition(SqlQueryPhase::Queued, SqlQueryPhase::Executing)
            .unwrap();
        query.record_commit(0, 42);
        assert_eq!(
            query.request_cancel(CancellationReason::ClientRequest),
            CancelOutcome::Accepted
        );
        let error = query.checkpoint().unwrap_err();
        query.fail();
        let status = registry.status(query_id).unwrap();

        let response = query_error_response_with_status(&error, Some(query_id), Some(&status));
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["status"], "cancelled_after_commit");
        assert_eq!(body["error"]["code"], "QUERY_CANCELLED_AFTER_COMMIT");
        assert_eq!(body["committed"], true);
        assert_eq!(body["outcome"]["committed_statements"], 1);
        assert_eq!(body["outcome"]["last_commit_epoch"], 42);
        assert_eq!(body["outcome"]["last_commit_epoch_text"], "42");
        assert_eq!(body["first_commit_statement_index"], 0);
        assert_eq!(body["last_commit_statement_index"], 0);
        assert_eq!(body["outcome"]["first_commit_statement_index"], 0);
        assert_eq!(body["outcome"]["last_commit_statement_index"], 0);

        let response = query_error_response_with_status(&error, Some(query_id), None);
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["status"], "cancelled_after_commit");
        assert_eq!(body["error"]["code"], "QUERY_CANCELLED_AFTER_COMMIT");
        assert_eq!(body["committed"], true);
        assert_eq!(body["committed_statements"], 1);
        assert_eq!(body["last_commit_epoch"], 42);
        assert_eq!(body["last_commit_epoch_text"], "42");
        assert_eq!(body["first_commit_statement_index"], 0);
        assert_eq!(body["last_commit_statement_index"], 0);
        assert_eq!(body["outcome"]["committed"], true);
        assert_eq!(body["outcome"]["committed_statements"], 1);
        assert_eq!(body["outcome"]["last_commit_epoch"], 42);
        assert_eq!(body["outcome"]["last_commit_epoch_text"], "42");
        assert_eq!(body["outcome"]["first_commit_statement_index"], 0);
        assert_eq!(body["outcome"]["last_commit_statement_index"], 0);
    }

    #[tokio::test]
    async fn commit_outcome_fallback_preserves_exact_progress() {
        let query_id: QueryId = "33445566778899001122aabbccddeeff".parse().unwrap();
        let error = mongreldb_query::MongrelQueryError::CommitOutcome {
            query_id,
            committed: true,
            committed_statements: 3,
            last_commit_epoch: Some(77),
            first_commit_statement_index: Some(1),
            last_commit_statement_index: Some(4),
            completed_statements: 4,
            statement_index: 5,
            message: "durable outcome retained".into(),
        };
        let response = query_error_response_with_status(&error, Some(query_id), None);
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["status"], "committed_with_error");
        assert_eq!(body["committed"], true);
        assert_eq!(body["committed_statements"], 3);
        assert_eq!(body["last_commit_epoch"], 77);
        assert_eq!(body["last_commit_epoch_text"], "77");
        assert_eq!(body["first_commit_statement_index"], 1);
        assert_eq!(body["last_commit_statement_index"], 4);
        assert_eq!(body["completed_statements"], 4);
        assert_eq!(body["statement_index"], 5);
        assert_eq!(body["outcome"]["committed_statements"], 3);
        assert_eq!(body["outcome"]["last_commit_epoch"], 77);
        assert_eq!(body["outcome"]["first_commit_statement_index"], 1);
        assert_eq!(body["outcome"]["last_commit_statement_index"], 4);
        assert_eq!(body["outcome"]["completed_statements"], 4);
        assert_eq!(body["outcome"]["statement_index"], 5);
    }

    #[test]
    fn status_cancel_outcome_matches_cancel_endpoint_state() {
        let registry = Arc::new(SqlQueryRegistry::default());
        let commit = registry.register(SqlQueryOptions::default()).unwrap();
        commit
            .transition(SqlQueryPhase::Queued, SqlQueryPhase::Executing)
            .unwrap();
        commit.enter_commit_critical().unwrap();
        assert_eq!(
            query_cancel_outcome(&registry.status(commit.id()).unwrap()),
            Some("too_late")
        );

        let completed = registry.register(SqlQueryOptions::default()).unwrap();
        completed
            .transition(SqlQueryPhase::Queued, SqlQueryPhase::Executing)
            .unwrap();
        completed.try_complete().unwrap();
        assert_eq!(
            query_cancel_outcome(&registry.status(completed.id()).unwrap()),
            Some("already_finished")
        );
    }
}

#[cfg(test)]
mod wal_stream_tests {
    use super::*;
    use mongreldb_client::ReplicationFollower;
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
                default_value: None,
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

        let resp = reqwest::get(format!("http://{addr}/wal/stream"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body = resp.text().await.unwrap();
        // Should contain at least one record (the flush commit).
        assert!(!body.is_empty(), "wal_stream should return records");
        assert!(body.contains("seq"), "response should contain seq field");
    }

    #[tokio::test]
    async fn follower_bootstraps_and_applies_incremental_commit() {
        let leader_dir = tempdir().unwrap();
        let follower_dir = tempdir().unwrap();
        let follower_path = follower_dir.path().join("copy");
        let db = Arc::new(Database::create(leader_dir.path()).unwrap());
        db.create_table(
            "items",
            mongreldb_core::schema::Schema {
                schema_id: 1,
                columns: vec![mongreldb_core::schema::ColumnDef {
                    id: 1,
                    name: "id".into(),
                    ty: TypeId::Int64,
                    flags: mongreldb_core::schema::ColumnFlags::empty()
                        .with(mongreldb_core::schema::ColumnFlags::PRIMARY_KEY),
                    default_value: None,
                }],
                indexes: vec![],
                colocation: vec![],
                constraints: Default::default(),
                clustered: false,
            },
        )
        .unwrap();
        let handle = db.table("items").unwrap();
        handle.lock().put(vec![(1, Value::Int64(1))]).unwrap();
        handle.lock().commit().unwrap();

        let app = build_app_with_config(
            Arc::clone(&db),
            std::iter::empty::<Arc<dyn ExternalTableModule>>(),
            Some("replication-secret".into()),
            None,
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let leader_url = format!("http://{addr}");
        let first_path = follower_path.clone();
        let (mut follower, initial) = tokio::task::spawn_blocking(move || {
            let mut follower = ReplicationFollower::new(&leader_url, first_path)
                .unwrap()
                .with_bearer_token("replication-secret");
            let applied = follower.sync().unwrap();
            (follower, applied)
        })
        .await
        .unwrap();
        assert_eq!(initial, 0);

        handle.lock().put(vec![(1, Value::Int64(2))]).unwrap();
        handle.lock().commit().unwrap();
        let applied = tokio::task::spawn_blocking(move || {
            let count = follower.sync().unwrap();
            (follower, count)
        })
        .await
        .unwrap();
        follower = applied.0;
        assert!(applied.1 > 0);
        assert!(follower.last_epoch() > 0);

        let replica = Database::open(&follower_path).unwrap();
        assert_eq!(replica.table("items").unwrap().lock().count(), 2);
        drop(replica);

        db.set_spill_threshold(1);
        db.transaction(|txn| {
            txn.put("items", vec![(1, Value::Int64(3))])?;
            Ok(())
        })
        .unwrap();
        let (follower_after_bootstrap, applied) = tokio::task::spawn_blocking(move || {
            let count = follower.sync().unwrap();
            (follower, count)
        })
        .await
        .unwrap();
        assert_eq!(applied, 0, "spilled run should trigger safe rebootstrap");
        assert!(follower_after_bootstrap.last_epoch() > 0);
        let replica = Database::open(&follower_path).unwrap();
        assert_eq!(replica.table("items").unwrap().lock().count(), 3);
    }
}

#[cfg(test)]
mod metrics_tests {
    use super::*;
    use mongreldb_core::Database;
    use tempfile::tempdir;

    /// Helper: spin up a daemon over a fresh DB with one `items(id int64 pk)`
    /// table pre-created, returning the bound address.
    async fn setup() -> (tempfile::TempDir, std::net::SocketAddr) {
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
                default_value: None,
            }],
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        };
        db.create_table("items", table_schema).unwrap();
        let app = build_app(db);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        (dir, addr)
    }

    #[tokio::test]
    async fn metrics_endpoint_returns_prometheus_text() {
        let (_dir, addr) = setup().await;
        let client = reqwest::Client::new();

        // Exercise a few handlers to bump counters.
        let _ = client
            .post(format!("http://{addr}/tables/items/put"))
            .json(&json!({ "row": [1, 1] }))
            .send()
            .await
            .unwrap();
        let _ = client
            .post(format!("http://{addr}/sql"))
            .json(&json!({ "sql": "SELECT count(*) FROM items" }))
            .send()
            .await
            .unwrap();

        let resp = client
            .get(format!("http://{addr}/metrics"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let ct = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        assert!(
            ct.contains("text/plain"),
            "content-type is prometheus text: {ct}"
        );
        let body = resp.text().await.unwrap();
        // Prometheus series + type lines are present.
        assert!(body.contains("# TYPE mongreldb_sql_queries_total counter"));
        assert!(body.contains("# TYPE mongreldb_puts_total counter"));
        assert!(body.contains("# TYPE mongreldb_tables gauge"));
        // Counters were bumped: at least one query and one put were served.
        assert!(
            body.contains("mongreldb_sql_queries_total 1"),
            "sql_queries counter should reflect the /sql call: {body}"
        );
        assert!(
            body.contains("mongreldb_puts_total 1"),
            "puts counter should reflect the put call: {body}"
        );
        // The tables gauge reflects the single `items` table.
        assert!(body.contains("mongreldb_tables 1"));
    }

    #[tokio::test]
    async fn metrics_error_counter_increments_on_bad_sql() {
        let (_dir, addr) = setup().await;
        let client = reqwest::Client::new();
        // A query against a non-existent table errors at the engine layer.
        let _ = client
            .post(format!("http://{addr}/sql"))
            .json(&json!({ "sql": "SELECT * FROM does_not_exist" }))
            .send()
            .await
            .unwrap();
        let body = client
            .get(format!("http://{addr}/metrics"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(
            body.contains("mongreldb_sql_errors_total 1"),
            "sql_errors should increment on a failed query: {body}"
        );
    }

    #[tokio::test]
    async fn arrow_stream_returns_ipc_stream_bytes() {
        let (_dir, addr) = setup().await;
        let client = reqwest::Client::new();
        // Insert a couple of rows so there are real batches to stream, then
        // flush so the rows are durable/visible to a fresh SQL session.
        for i in 1..=3 {
            let resp = client
                .post(format!("http://{addr}/tables/items/put"))
                .json(&json!({ "row": [1, i] }))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200, "put should succeed");
        }
        let _ = client
            .post(format!("http://{addr}/tables/items/commit"))
            .send()
            .await
            .unwrap();
        // Sanity: the rows are durable and visible to the table handle.
        let count_body = client
            .get(format!("http://{addr}/tables/items/count"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(
            count_body.contains("\"count\":3"),
            "expected 3 visible rows, got: {count_body}"
        );
        let resp = client
            .post(format!("http://{addr}/sql"))
            .json(&json!({ "sql": "SELECT count(*) FROM items", "format": "arrow-stream" }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "streaming query should succeed");
        let ct = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        assert!(
            ct.contains("application/vnd.apache.arrow.stream"),
            "content-type should be the arrow stream format: {ct}"
        );
        let bytes = resp.bytes().await.unwrap();
        // Arrow IPC streams begin with the magic continuation marker
        // 0xFFFFFFFF followed by the schema message length. The stream must be
        // non-empty (3 rows were written) and end with the EOS marker.
        assert!(
            !bytes.is_empty(),
            "arrow stream body should contain schema + batch + EOS"
        );
        assert!(
            bytes.starts_with(&0xFFFFFFFFu32.to_le_bytes()),
            "arrow stream must begin with the IPC continuation marker"
        );
        assert!(
            bytes.ends_with(&[0u8, 0, 0, 0]),
            "arrow stream should end with the EOS marker (trailing zero length)"
        );
    }
}

#[cfg(test)]
mod streaming_tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::common::DataFusionError;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::StreamExt;
    use std::sync::Arc as StdArc;

    fn batch_stream(batches: Vec<RecordBatch>) -> mongreldb_query::MongrelRecordBatchStream {
        let schema = batches
            .first()
            .map(RecordBatch::schema)
            .unwrap_or_else(|| StdArc::new(Schema::empty()));
        let batches =
            futures::stream::iter(batches.into_iter().map(Ok::<RecordBatch, DataFusionError>));
        Box::pin(RecordBatchStreamAdapter::new(schema, batches))
    }

    /// Unit-level check: feed two synthetic batches through the streaming
    /// serializer and re-parse the resulting IPC stream end-to-end. This
    /// validates the per-message chunking (schema + N batches + EOS) without
    /// depending on the engine's scan visibility.
    #[tokio::test]
    async fn arrow_stream_serializes_multiple_batches_roundtrip() {
        let schema = StdArc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let b1 = RecordBatch::try_new(
            schema.clone(),
            vec![StdArc::new(Int64Array::from(vec![1, 2]))],
        )
        .unwrap();
        let b2 = RecordBatch::try_new(
            schema.clone(),
            vec![StdArc::new(Int64Array::from(vec![3, 4, 5]))],
        )
        .unwrap();

        let resp = sql_arrow_stream_response(batch_stream(vec![b1, b2]));
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();

        // Begins with the IPC continuation marker.
        assert!(bytes.starts_with(&0xFFFFFFFFu32.to_le_bytes()));

        // Re-parse the full stream and confirm all rows survived the chunked
        // serialization.
        let slice: &[u8] = bytes.as_ref();
        let mut reader = arrow::ipc::reader::StreamReader::try_new(slice, None).unwrap();
        let mut total_rows = 0;
        for batch in reader.by_ref() {
            let batch = batch.expect("each IPC message should decode");
            total_rows += batch.num_rows();
        }
        assert_eq!(
            total_rows, 5,
            "all rows should round-trip through the stream"
        );
    }

    #[tokio::test]
    async fn arrow_stream_emits_schema_before_first_batch() {
        let schema = StdArc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let pending = futures::stream::pending::<Result<RecordBatch, DataFusionError>>();
        let batches = Box::pin(RecordBatchStreamAdapter::new(schema, pending));
        let mut body = sql_arrow_stream_response(batches)
            .into_body()
            .into_data_stream();

        let chunk = tokio::time::timeout(std::time::Duration::from_millis(100), body.next())
            .await
            .expect("schema chunk should not wait for a query batch")
            .unwrap()
            .unwrap();
        assert!(chunk.starts_with(&0xFFFFFFFFu32.to_le_bytes()));
    }

    #[tokio::test]
    async fn arrow_stream_empty_query_is_valid_ipc() {
        let resp = sql_arrow_stream_response(batch_stream(Vec::new()));
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        let slice: &[u8] = bytes.as_ref();
        let reader = arrow::ipc::reader::StreamReader::try_new(slice, None).unwrap();
        assert_eq!(reader.count(), 0);
    }

    #[tokio::test]
    async fn buffered_output_limits_are_typed() {
        let dir = tempfile::tempdir().unwrap();
        let db = StdArc::new(mongreldb_core::Database::create(dir.path()).unwrap());
        let session = MongrelSession::open(db).unwrap();
        let query = session.register_query(SqlQueryOptions::default()).unwrap();
        let output = session
            .run_with_query_for_serialization("SELECT 1", query)
            .await
            .unwrap();

        let row_error =
            serialize_buffered_output("json", output.batches(), output.query(), 0, 1024, None)
                .unwrap_err();
        assert!(matches!(row_error, BufferedSerializationError::Limit(_)));
        let byte_error =
            serialize_buffered_output("json", output.batches(), output.query(), 10, 1, None)
                .unwrap_err();
        assert!(matches!(byte_error, BufferedSerializationError::Limit(_)));
        output.fail();
    }
}

#[cfg(test)]
mod audit_tests {
    use super::*;
    use mongreldb_core::Database;
    use tempfile::tempdir;

    /// Build a daemon with user-auth enabled plus one catalog user `alice`.
    async fn auth_setup(password: &str) -> std::net::SocketAddr {
        let dir = tempdir().unwrap();
        let db = Arc::new(Database::create(dir.path()).unwrap());
        db.create_user("alice", password).unwrap();
        db.set_user_admin("alice", true).unwrap();
        let app = build_app_full(
            db,
            std::iter::empty::<Arc<dyn ExternalTableModule>>(),
            None,
            None,
            true,
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        addr
    }

    #[tokio::test]
    async fn audit_records_login_success_and_failure() {
        let addr = auth_setup("s3cret").await;
        let client = reqwest::Client::new();

        // Successful basic auth.
        let resp = client
            .get(format!("http://{addr}/health"))
            .header("Authorization", basic("alice", "s3cret"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        // Failed basic auth (wrong password).
        let resp = client
            .get(format!("http://{addr}/health"))
            .header("Authorization", basic("alice", "wrong"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 401);

        let body = client
            .get(format!("http://{addr}/audit"))
            .header("Authorization", basic("alice", "s3cret"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(
            body.contains("\"action\":\"login.ok\""),
            "audit should record the successful login: {body}"
        );
        assert!(
            body.contains("\"action\":\"login.fail\""),
            "audit should record the failed login: {body}"
        );
        assert!(
            body.contains("\"principal\":\"alice\""),
            "audit should attribute events to alice: {body}"
        );
    }

    #[tokio::test]
    async fn audit_records_ddl_sql() {
        let dir = tempdir().unwrap();
        let db = Arc::new(Database::create(dir.path()).unwrap());
        let app = build_app(db);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let client = reqwest::Client::new();
        let _ = client
            .post(format!("http://{addr}/sql"))
            .json(&json!({ "sql": "CREATE TABLE t (id BIGINT PRIMARY KEY)" }))
            .send()
            .await
            .unwrap();
        // A plain SELECT is NOT audited.
        let _ = client
            .post(format!("http://{addr}/sql"))
            .json(&json!({ "sql": "SELECT 1" }))
            .send()
            .await
            .unwrap();

        let body = client
            .get(format!("http://{addr}/audit"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(
            body.contains("\"action\":\"ddl.ok\""),
            "audit should record the successful DDL statement: {body}"
        );
        assert!(
            body.contains("CREATE TABLE"),
            "audit detail should carry the DDL snippet: {body}"
        );
        // SELECT must not appear.
        assert!(
            !body.contains("SELECT 1"),
            "non-DDL reads should not be audited: {body}"
        );
    }

    #[tokio::test]
    async fn audit_redacts_credential_passwords() {
        let dir = tempdir().unwrap();
        let db = Arc::new(Database::create(dir.path()).unwrap());
        let app = build_app(db);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let client = reqwest::Client::new();
        // A CREATE USER carrying a plaintext password must be audited but the
        // password must NEVER reach /audit or stderr.
        let _ = client
            .post(format!("http://{addr}/sql"))
            .json(&json!({ "sql": "CREATE USER alice WITH PASSWORD 'topsecret'" }))
            .send()
            .await
            .unwrap();

        let body = client
            .get(format!("http://{addr}/audit"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(
            !body.contains("topsecret"),
            "password must never appear in the audit log: {body}"
        );
        assert!(
            body.contains("redacted credential statement"),
            "credential DDL should be recorded as redacted: {body}"
        );
    }

    fn basic(user: &str, pass: &str) -> String {
        let raw = format!("{user}:{pass}");
        format!("Basic {}", base64_encode(raw.as_bytes()))
    }

    fn base64_encode(input: &[u8]) -> String {
        const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = String::new();
        let mut buf = 0u32;
        let mut bits = 0u32;
        for &b in input {
            buf = (buf << 8) | b as u32;
            bits += 8;
            while bits >= 6 {
                bits -= 6;
                out.push(TABLE[((buf >> bits) & 0x3F) as usize] as char);
            }
        }
        if bits > 0 {
            out.push(TABLE[((buf << (6 - bits)) & 0x3F) as usize] as char);
        }
        while !out.len().is_multiple_of(4) {
            out.push('=');
        }
        out
    }
}

#[cfg(test)]
mod session_tests {
    use super::*;
    use mongreldb_core::Database;
    use tempfile::tempdir;

    /// Spin up a daemon over a fresh DB with one `items(id int64 pk)` table.
    /// Returns the TempDir (must be held alive for the test's duration — dropping
    /// it deletes the database directory mid-test) and the bound address.
    async fn setup() -> (tempfile::TempDir, std::net::SocketAddr) {
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
                default_value: None,
            }],
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        };
        db.create_table("items", table_schema).unwrap();
        let app = build_app(db);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        (dir, addr)
    }

    /// Open a session and return its token.
    async fn open_session(client: &reqwest::Client, addr: &std::net::SocketAddr) -> String {
        let resp = client
            .post(format!("http://{addr}/sessions"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        resp.json::<serde_json::Value>()
            .await
            .unwrap()
            .get("session_id")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string()
    }

    async fn sql_on(
        client: &reqwest::Client,
        addr: &std::net::SocketAddr,
        session: &str,
        sql: &str,
    ) -> reqwest::Response {
        client
            .post(format!("http://{addr}/sql"))
            .header("X-Session-ID", session)
            .json(&json!({ "sql": sql }))
            .send()
            .await
            .unwrap()
    }

    async fn count_items(client: &reqwest::Client, addr: &std::net::SocketAddr) -> u64 {
        client
            .get(format!("http://{addr}/tables/items/count"))
            .send()
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap()
            .get("count")
            .unwrap()
            .as_u64()
            .unwrap()
    }

    #[tokio::test]
    async fn cross_request_transaction_commits() {
        let (_dir, addr) = setup().await;
        let client = reqwest::Client::new();
        let session = open_session(&client, &addr).await;

        // BEGIN, INSERT, COMMIT — each its own HTTP request on the same session.
        let r = sql_on(&client, &addr, &session, "BEGIN").await;
        assert_eq!(r.status(), 200);
        let r = sql_on(
            &client,
            &addr,
            &session,
            "INSERT INTO items (id) VALUES (1)",
        )
        .await;
        assert_eq!(r.status(), 200, "INSERT should stage successfully");
        // Not yet committed → not visible to other connections.
        assert_eq!(count_items(&client, &addr).await, 0);
        let r = sql_on(&client, &addr, &session, "COMMIT").await;
        assert_eq!(r.status(), 200);

        // After COMMIT the row is durable and visible.
        assert_eq!(count_items(&client, &addr).await, 1);
    }

    #[tokio::test]
    async fn cross_request_transaction_rolls_back() {
        let (_dir, addr) = setup().await;
        let client = reqwest::Client::new();
        let session = open_session(&client, &addr).await;

        sql_on(&client, &addr, &session, "BEGIN").await;
        sql_on(
            &client,
            &addr,
            &session,
            "INSERT INTO items (id) VALUES (5)",
        )
        .await;
        // ROLLBACK discards the staged insert.
        let r = sql_on(&client, &addr, &session, "ROLLBACK").await;
        assert_eq!(r.status(), 200);
        assert_eq!(
            count_items(&client, &addr).await,
            0,
            "rollback discards staged writes"
        );
    }

    #[tokio::test]
    async fn unknown_session_id_is_404() {
        let (_dir, addr) = setup().await;
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{addr}/sql"))
            .header("X-Session-ID", "does-not-exist")
            .json(&json!({ "sql": "SELECT 1" }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);
    }

    #[tokio::test]
    async fn invalid_session_headers_do_not_autocommit() {
        let (_dir, addr) = setup().await;
        let client = reqwest::Client::new();

        let resp = client
            .post(format!("http://{addr}/sql"))
            .header(
                "X-Session-ID",
                reqwest::header::HeaderValue::from_bytes(&[0xff]).unwrap(),
            )
            .json(&json!({ "sql": "INSERT INTO items (id) VALUES (1)" }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let resp = client
            .post(format!("http://{addr}/sql"))
            .header("X-Session-ID", "x".repeat(257))
            .json(&json!({ "sql": "INSERT INTO items (id) VALUES (2)" }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(count_items(&client, &addr).await, 0);
    }

    #[tokio::test]
    async fn close_session_ends_cross_request_state() {
        let (_dir, addr) = setup().await;
        let client = reqwest::Client::new();
        let session = open_session(&client, &addr).await;

        // BEGIN on the session, then close it → staged txn is discarded.
        sql_on(&client, &addr, &session, "BEGIN").await;
        let r = client
            .delete(format!("http://{addr}/sessions/{session}"))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);

        // The session token is now invalid.
        let resp = sql_on(&client, &addr, &session, "COMMIT").await;
        assert_eq!(resp.status(), 404, "closed session is no longer usable");
    }

    #[tokio::test]
    async fn no_session_header_uses_fresh_ephemeral_session() {
        // Without X-Session-ID, BEGIN..COMMIT must still work within a single
        // multi-statement /sql body (the historical behavior is preserved).
        let (_dir, addr) = setup().await;
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{addr}/sql"))
            .json(&json!({ "sql": "INSERT INTO items (id) VALUES (42)" }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(count_items(&client, &addr).await, 1);
    }

    #[tokio::test]
    async fn prepared_statement_prepare_execute_and_reuse() {
        let (_dir, addr) = setup().await;
        let client = reqwest::Client::new();
        let session = open_session(&client, &addr).await;
        sql_on(
            &client,
            &addr,
            &session,
            "INSERT INTO items (id) VALUES (1), (2), (3), (4)",
        )
        .await;

        // Prepare a parameterized query once.
        let resp = client
            .post(format!("http://{addr}/sessions/{session}/prepare"))
            .json(&json!({"name":"gt","sql":"SELECT id FROM items WHERE id > $1"}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        // Execute with param 2 → ids 3,4.
        let resp = client
            .post(format!("http://{addr}/sessions/{session}/execute"))
            .json(&json!({"name":"gt","params":[2]}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body = resp.json::<serde_json::Value>().await.unwrap();
        let arr = body
            .as_array()
            .expect("execute returns a JSON array of rows");
        assert_eq!(arr.len(), 2, "ids > 2 are {{3,4}}: {body}");

        // Re-execute with a different param → reuses the cached plan, fewer rows.
        let resp = client
            .post(format!("http://{addr}/sessions/{session}/execute"))
            .json(&json!({"name":"gt","params":[3]}))
            .send()
            .await
            .unwrap();
        let body = resp.json::<serde_json::Value>().await.unwrap();
        assert_eq!(body.as_array().unwrap().len(), 1, "ids > 3 is {{4}}");
    }

    #[tokio::test]
    async fn prepared_statement_deallocate_then_execute_fails() {
        let (_dir, addr) = setup().await;
        let client = reqwest::Client::new();
        let session = open_session(&client, &addr).await;
        let _ = client
            .post(format!("http://{addr}/sessions/{session}/prepare"))
            .json(&json!({"name":"p","sql":"SELECT $1"}))
            .send()
            .await
            .unwrap();
        let deallocate_query_id = "dadadadadadadadadadadadadadadada";
        let resp = client
            .delete(format!("http://{addr}/sessions/{session}/statements/p"))
            .header("X-MongrelDB-Query-ID", deallocate_query_id)
            .header("X-MongrelDB-Timeout-Ms", "10000")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(
            resp.headers()
                .get("X-MongrelDB-Query-ID")
                .unwrap()
                .to_str()
                .unwrap(),
            deallocate_query_id
        );
        let status = client
            .get(format!("http://{addr}/queries/{deallocate_query_id}"))
            .send()
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap();
        assert_eq!(status["state"], "completed");
        assert_eq!(status["operation"], "DEALLOCATE");
        // Execute after deallocate must error.
        let resp = client
            .post(format!("http://{addr}/sessions/{session}/execute"))
            .json(&json!({"name":"p","params":[1]}))
            .send()
            .await
            .unwrap();
        assert_ne!(resp.status(), 200, "execute after DEALLOCATE must fail");
    }

    #[tokio::test]
    async fn prepared_statement_deallocate_honors_pre_registration_cancel() {
        let (_dir, addr) = setup().await;
        let client = reqwest::Client::new();
        let session = open_session(&client, &addr).await;
        let prepared = client
            .post(format!("http://{addr}/sessions/{session}/prepare"))
            .json(&json!({"name":"p","sql":"SELECT $1"}))
            .send()
            .await
            .unwrap();
        assert_eq!(prepared.status(), StatusCode::OK);

        let query_id = "dbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdb";
        let cancel = client
            .post(format!("http://{addr}/queries/{query_id}/cancel"))
            .header("X-Session-ID", &session)
            .send()
            .await
            .unwrap();
        assert_eq!(cancel.status(), StatusCode::ACCEPTED);
        let deallocate = client
            .delete(format!("http://{addr}/sessions/{session}/statements/p"))
            .header("X-MongrelDB-Query-ID", query_id)
            .send()
            .await
            .unwrap();
        assert_eq!(deallocate.status().as_u16(), 499);
        assert_eq!(
            deallocate.json::<serde_json::Value>().await.unwrap()["error"]["code"],
            "QUERY_CANCELLED"
        );

        let execute = client
            .post(format!("http://{addr}/sessions/{session}/execute"))
            .json(&json!({"name":"p","params":[1]}))
            .send()
            .await
            .unwrap();
        assert_eq!(execute.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn prepared_statement_rejects_bad_name() {
        let (_dir, addr) = setup().await;
        let client = reqwest::Client::new();
        let session = open_session(&client, &addr).await;
        let resp = client
            .post(format!("http://{addr}/sessions/{session}/prepare"))
            .json(&json!({"name":"1bad","sql":"SELECT 1"}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            400,
            "statement name starting with a digit must be rejected"
        );
    }
}
