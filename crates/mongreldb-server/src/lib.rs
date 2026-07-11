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
use serde::{Deserialize, Serialize};
use serde_json::json;

mod audit;
mod kit;
mod metrics;
mod procedure;
mod sessions;
mod trigger;

pub use sessions::{spawn_session_reaper, SessionStore};

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
        MongrelError::ReadOnlyReplica => StatusCode::CONFLICT,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Map a query-layer error (which wraps engine errors via `Core(...)`) to the
/// appropriate HTTP status code.
fn status_for_query_error(e: &mongreldb_query::MongrelQueryError) -> StatusCode {
    use mongreldb_query::MongrelQueryError;
    match e {
        MongrelQueryError::Core(core) => status_for_error(core),
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
    db.set_replication_wal_retention_segments(default_replication_wal_segments());
    if let Err(error) = db.set_history_retention_epochs(default_history_retention_epochs()) {
        eprintln!("[history] failed to configure retention: {error}");
    }
    let state = Arc::new(AppState {
        idem: kit::IdempotencyStore::new(db.root()),
        db,
        external_modules: external_modules.into_iter().collect(),
        auth_token,
        user_auth,
        metrics: Arc::new(metrics::Metrics::default()),
        slow_query_threshold: metrics::slow_query_threshold(),
        audit: Arc::new(audit::AuditLog::new(8192)),
        sessions,
    });
    let router = axum::Router::new()
        .route("/health", get(health))
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
        .route("/kit/create_table", post(kit::kit_create_table))
        .route("/kit/procedures/{name}/call", post(procedure::kit_call))
        .route("/compact", post(compact_all))
        .route("/tables/{name}/compact", post(compact_table))
        .route("/wal/stream", get(wal_stream))
        .route("/replication/snapshot", get(replication_snapshot))
        .route("/events", get(events_stream))
        .with_state(state.clone());

    // Apply auth middleware if token auth or user auth is enabled.
    let router = if state.auth_token.is_some() || state.user_auth {
        router.layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ))
    } else {
        router
    };

    // Apply connection limit if configured.
    if let Some(max) = max_connections {
        router.layer(tower::limit::ConcurrencyLimitLayer::new(max))
    } else {
        router
    }
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
                if let Ok(creds) = std::str::from_utf8(&decoded) {
                    if let Some((username, password)) = creds.split_once(':') {
                        attempted = username.to_string();
                        if let Ok(Some(_user)) = state.db.verify_user(username, password) {
                            state
                                .audit
                                .record(username, "login.ok", "basic credentials accepted");
                            // Inject the principal for permission checks.
                            if let Some(principal) = state.db.resolve_principal(username) {
                                req.extensions_mut().insert(principal);
                            }
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
        set_replication_headers(&mut response, batch.current_epoch, batch.earliest_epoch);
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
    set_replication_headers(&mut response, batch.current_epoch, batch.earliest_epoch);
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
            set_replication_headers(&mut response, epoch, None);
            response
        }
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

fn set_replication_headers(response: &mut Response, current: u64, earliest: Option<u64>) {
    response.headers_mut().insert(
        "x-mongreldb-current-epoch",
        current.to_string().parse().unwrap(),
    );
    if let Some(earliest) = earliest {
        response.headers_mut().insert(
            "x-mongreldb-earliest-epoch",
            earliest.to_string().parse().unwrap(),
        );
    }
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

    let last_id = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
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

/// The principal a request is attributed to, for session ownership: a resolved
/// user principal wins; otherwise `token` when token auth is active; else
/// `anonymous` (no auth configured).
fn request_owner(state: &AppState, principal: &Option<mongreldb_core::Principal>) -> String {
    if let Some(p) = principal {
        return p.username.clone();
    }
    if state.auth_token.is_some() {
        return "token".into();
    }
    "anonymous".into()
}

fn request_principal(
    state: &AppState,
    principal: &Option<mongreldb_core::Principal>,
) -> Option<mongreldb_core::Principal> {
    principal.clone().or_else(|| {
        state
            .auth_token
            .as_ref()
            .map(|_| mongreldb_core::Principal {
                username: "token".into(),
                is_admin: true,
                roles: Vec::new(),
                permissions: Vec::new(),
            })
    })
}

/// `POST /sessions` — open a long-lived session for cross-request interactive
/// transactions. Returns `{"session_id": "..."}`; send `X-Session-ID: <token>`
/// on subsequent `/sql` requests to route to it. The session is owned by the
/// authenticated principal and auto-expires after the idle timeout.
async fn create_session(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
) -> Response {
    let owner = request_owner(&state, &principal);
    let session = match MongrelSession::open_with_external_modules_as(
        Arc::clone(&state.db),
        state.external_modules.iter().cloned(),
        request_principal(&state, &principal),
    ) {
        Ok(s) => s,
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
    let owner = request_owner(&state, &principal);
    if state.sessions.close(&id, &owner) {
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
fn dispatch_buffered_sql_format(
    format: Option<&str>,
    batches: &[arrow::record_batch::RecordBatch],
) -> Response {
    match format {
        Some("arrow") => sql_arrow_response(batches),
        _ => sql_json_response(batches),
    }
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
}

/// `POST /sessions/{id}/prepare` — parse+plan `sql` once and store it under
/// `name` on the session. Subsequent `EXECUTE name(...)` calls (via this
/// endpoint or `EXECUTE` SQL) reuse the cached plan, skipping re-planning.
async fn prepare_statement(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Path(id): Path<String>,
    Json(req): Json<PrepareRequest>,
) -> Response {
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
    let _guard = entry.lock.lock().await;
    if entry.is_closed() {
        return (StatusCode::NOT_FOUND, "session no longer available").into_response();
    }
    entry.touch();
    let sql = format!("PREPARE {} AS {}", req.name, req.sql);
    match entry.session.run(&sql).await {
        Ok(_) => Json(json!({ "prepared": req.name })).into_response(),
        Err(e) => (status_for_query_error(&e), e.to_string()).into_response(),
    }
}

#[derive(Deserialize)]
struct ExecuteRequest {
    name: String,
    params: Vec<serde_json::Value>,
    #[serde(default)]
    format: Option<String>,
}

/// `POST /sessions/{id}/execute` — run a previously-prepared statement with
/// typed parameters, reusing its cached plan. Returns the same formats as
/// `/sql` (`json` default, `arrow`, `arrow-stream`).
async fn execute_statement(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Path(id): Path<String>,
    Json(req): Json<ExecuteRequest>,
) -> Response {
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
    let _guard = entry.lock.lock().await;
    if entry.is_closed() {
        return (StatusCode::NOT_FOUND, "session no longer available").into_response();
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
        entry
            .session
            .run_stream(&sql)
            .await
            .map(sql_arrow_stream_response)
    } else {
        entry
            .session
            .run(&sql)
            .await
            .map(|batches| dispatch_buffered_sql_format(req.format.as_deref(), &batches))
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
        Ok(response) => response,
        Err(e) => {
            state.metrics.inc_sql_errors();
            // A reference to an unprepared/unknown statement is a client error.
            let msg = format!("{e}");
            let status = if msg.contains("does not exist") {
                StatusCode::NOT_FOUND
            } else {
                status_for_query_error(&e)
            };
            (status, format!("{msg} ({}µs)", elapsed.as_micros())).into_response()
        }
    }
}

/// `DELETE /sessions/{id}/statements/{name}` — drop a prepared statement from
/// the session (SQL `DEALLOCATE`).
async fn deallocate_statement(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Path((id, name)): Path<(String, String)>,
) -> Response {
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
    let _guard = entry.lock.lock().await;
    if entry.is_closed() {
        return (StatusCode::NOT_FOUND, "session no longer available").into_response();
    }
    entry.touch();
    let sql = format!("DEALLOCATE {name}");
    match entry.session.run(&sql).await {
        Ok(_) => Json(json!({ "deallocated": name })).into_response(),
        Err(e) => (status_for_query_error(&e), e.to_string()).into_response(),
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
    let body = state.metrics.prometheus_text(state.db.table_names().len());
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
        Ok(id) => Json(json!({ "table_id": id })).into_response(),
        Err(e) => (status_for_error(&e), e.to_string()).into_response(),
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
    match state.db.drop_table(&name) {
        Ok(_) => {
            // Invalidate cached idempotency entries. A cached transaction
            // may reference the dropped table; replaying it would silently
            // report success without writing to the recreated table.
            state.idem.clear();
            StatusCode::OK.into_response()
        }
        Err(e) => (status_for_error(&e), e.to_string()).into_response(),
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
            .map(|c| c.ty.clone())
            .ok_or_else(|| format!("unknown column id {col_id}"))?;
        let val = json_to_value(&chunk[1], &expected);
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
        Ok(epoch) => Json(json!({ "epoch": epoch.0 })).into_response(),
        Err(e) => (status_for_error(&e), e.to_string()).into_response(),
    }
}

#[derive(Deserialize)]
struct SqlRequest {
    sql: String,
    /// Output format: `"json"` (the default) for a JSON array of row objects,
    /// `"arrow"` for Arrow IPC file bytes.
    #[serde(default)]
    format: Option<String>,
}

async fn sql(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    headers: axum::http::HeaderMap,
    Json(req): Json<SqlRequest>,
) -> Response {
    // Session routing: an `X-Session-ID` header routes the request to a pooled
    // long-lived session, enabling cross-request `BEGIN`/`INSERT`/`COMMIT`
    // transactions. Without the header, a fresh ephemeral session is used
    // (the historical behavior).
    let session_id = headers
        .get("x-session-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    if let Some(sid) = session_id {
        let owner = request_owner(&state, &principal);
        let Some(entry) = state.sessions.get(&sid, &owner) else {
            return (
                StatusCode::NOT_FOUND,
                "session not found or not owned by caller",
            )
                .into_response();
        };
        // Serialize per-session access so two concurrent requests on the same
        // token cannot interleave a transaction's staged writes.
        let _guard = entry.lock.lock().await;
        // Re-check closed: the session may have been closed/evicted between
        // get() and acquiring the lock.
        if entry.is_closed() {
            return (StatusCode::NOT_FOUND, "session no longer available").into_response();
        }
        entry.touch();
        execute_sql(&state, &principal, &entry.session, req).await
    } else {
        let session = match MongrelSession::open_with_external_modules_as(
            Arc::clone(&state.db),
            state.external_modules.iter().cloned(),
            request_principal(&state, &principal),
        ) {
            Ok(s) => s,
            Err(e) => return (status_for_query_error(&e), e.to_string()).into_response(),
        };
        execute_sql(&state, &principal, &session, req).await
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
    req: SqlRequest,
) -> Response {
    state.metrics.inc_sql_queries();
    let audited = audit::is_audited_sql(&req.sql);
    let actor = request_owner(state, principal);
    let start = std::time::Instant::now();
    // NOTE: deliberately NOT using `run_sql_traced` here. Its thread-local
    // push/pop spans an `.await`, and on a multi-threaded tokio runtime the
    // task can resume on a different thread, corrupting the trace stack and
    // leaking scopes. Wall-clock timing is sufficient for slow-query detection
    // and works across awaits.
    let result = if req.format.as_deref() == Some("arrow-stream") {
        session
            .run_stream(&req.sql)
            .await
            .map(sql_arrow_stream_response)
    } else {
        session
            .run(&req.sql)
            .await
            .map(|batches| dispatch_buffered_sql_format(req.format.as_deref(), &batches))
    };
    let elapsed = start.elapsed();
    // Slow-query logging covers BOTH success and failure (the slowest errors
    // matter most for diagnosis), checked before branching on the outcome.
    if elapsed >= state.slow_query_threshold {
        state.metrics.inc_slow_queries();
        let preview: String = req.sql.chars().take(80).collect();
        eprintln!(
            "[slow-query] {}\u{00b5}s \u{2014} {preview}",
            elapsed.as_micros()
        );
    }
    // Audit DDL/privilege AFTER execution so the outcome (ok/fail) is captured.
    // `redacted_ddl_detail` never logs credential literals.
    if audited {
        let (action, detail) = audit::redacted_ddl_detail(&req.sql, result.is_ok());
        state.audit.record(actor, action, detail);
    }
    match result {
        Ok(response) => response,
        Err(e) => {
            state.metrics.inc_sql_errors();
            (
                status_for_query_error(&e),
                format!("{e} ({}µs)", elapsed.as_micros()),
            )
                .into_response()
        }
    }
}

/// Serialize Arrow record batches as Arrow IPC file bytes. This is the
/// high-performance binary format for clients with Arrow library support.
fn sql_arrow_response(batches: &[arrow::record_batch::RecordBatch]) -> Response {
    if batches.is_empty() {
        return StatusCode::OK.into_response();
    }
    let schema = batches[0].schema();
    let mut buf = Vec::new();
    let mut writer = arrow::ipc::writer::FileWriter::try_new(&mut buf, schema.as_ref()).unwrap();
    for b in batches {
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

/// Serialize a DataFusion record-batch stream as Arrow streaming IPC. The body
/// holds only the active query batch and one serialized IPC message.
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

/// Serialize Arrow record batches into a JSON array of row objects using the
/// Arrow JSON writer. Each row becomes a JSON object keyed by column name.
/// This handles all Arrow types correctly (Decimal128, Timestamp, Date32, List,
/// etc.) and streams directly into a byte buffer without intermediate
/// serde_json::Value allocations.
fn sql_json_response(batches: &[arrow::record_batch::RecordBatch]) -> Response {
    if batches.is_empty() {
        return ([(header::CONTENT_TYPE, "application/json")], b"[]" as &[u8]).into_response();
    }

    let mut buf = Vec::new();
    {
        let mut writer = arrow::json::writer::ArrayWriter::new(&mut buf);
        let refs: Vec<&_> = batches.iter().collect();
        if let Err(e) = writer.write_batches(&refs) {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("JSON serialization error: {e}"),
            )
                .into_response();
        }
        let _ = writer.finish();
    }

    ([(header::CONTENT_TYPE, "application/json")], buf).into_response()
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
        Ok(_) => Json(json!({ "status": "committed" })).into_response(),
        Err(e) => (status_for_error(&e), e.to_string()).into_response(),
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
    async fn setup() -> std::net::SocketAddr {
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
        addr
    }

    #[tokio::test]
    async fn metrics_endpoint_returns_prometheus_text() {
        let addr = setup().await;
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
        let addr = setup().await;
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
        let addr = setup().await;
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
        let resp = client
            .delete(format!("http://{addr}/sessions/{session}/statements/p"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
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
