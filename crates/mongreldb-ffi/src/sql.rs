//! SQL execution FFI: `mongreldb_database_sql` runs DataFusion SQL through the
//! engine's `MongrelSession` and returns the result serialized as Arrow IPC
//! *file* bytes — the same wire format the NAPI addon, Kit, and the daemon
//! emit for `format: "arrow"`.
//!
//! The session is lazily opened on the first SQL call and cached on the
//! `FFIDatabase` handle so repeated calls reuse catalog/view state. The async
//! `MongrelSession::run` is driven on a process-global single-threaded tokio
//! runtime (mirroring `mongreldb-kit`'s `sql_runtime`).
//!
//! Result format: Arrow IPC file bytes (empty `Vec` for DDL/DML that produces
//! no rows). Callers decode with any Arrow IPC reader (e.g. C++ `arrow::ipc`,
//! C `nanoarrow`, or the bundled decoder in the HTTP client). This matches
//! the NAPI addon's `native_cols_to_ipc_from_batches` and Kit's
//! `arrow_util::batches_to_ipc`.

use crate::database::{as_db, mongreldb_database_t};
use crate::error::{
    clear, copy_c_text, mongreldb_error_details_v1, set_error, set_error_msg,
    set_error_with_details, ErrorCode,
};
use arrow::ipc::writer::FileWriter;
use mongreldb_query::MongrelSession;
use std::os::raw::{c_char, c_void};
use std::sync::{Arc, Mutex, OnceLock};

/// Process-global tokio runtime for SQL execution. A multi-thread runtime lets
/// independent FFI query workers make progress without nesting runtimes.
fn sql_runtime() -> Result<&'static tokio::runtime::Runtime, &'static str> {
    static RT: OnceLock<Result<tokio::runtime::Runtime, String>> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|error| format!("failed to build FFI SQL runtime: {error}"))
    })
    .as_ref()
    .map_err(String::as_str)
}

fn database_session(
    database: &crate::database::FFIDatabase,
) -> Result<Arc<MongrelSession>, mongreldb_query::MongrelQueryError> {
    if let Some(session) = database.sql_session.read().as_ref() {
        return Ok(Arc::clone(session));
    }
    let session = Arc::new(MongrelSession::open(Arc::clone(&database.db))?);
    let mut cached = database.sql_session.write();
    Ok(Arc::clone(cached.get_or_insert(session)))
}

/// Serialize Arrow record batches to IPC *file* bytes. Empty input yields an
/// empty `Vec<u8>` (matching the NAPI addon and Kit behavior for DDL/DML).
enum FfiOutputFailure {
    Query(mongreldb_query::MongrelQueryError),
    ResultLimit(String),
    Serialization(String),
}

enum FfiSqlFailure {
    Query(mongreldb_query::MongrelQueryError),
    ResultLimit(String),
    Serialization(String),
}

fn ffi_query_error_code(error: &mongreldb_query::MongrelQueryError) -> ErrorCode {
    use mongreldb_query::MongrelQueryError;
    match error {
        MongrelQueryError::Core(error) => crate::error::categorize(error),
        MongrelQueryError::QueryCancelled {
            committed: true, ..
        } => ErrorCode::QueryCancelledAfterCommit,
        MongrelQueryError::QueryCancelled { .. } => ErrorCode::QueryCancelled,
        MongrelQueryError::DeadlineExceeded {
            committed: true, ..
        } => ErrorCode::DeadlineAfterCommit,
        MongrelQueryError::DeadlineExceeded { .. } => ErrorCode::DeadlineExceeded,
        MongrelQueryError::QueryIdConflict { .. } => ErrorCode::QueryIdConflict,
        MongrelQueryError::QueryRegistryFull => ErrorCode::QueryRegistryFull,
        MongrelQueryError::ResultLimitExceeded { .. } => ErrorCode::ResultLimit,
        MongrelQueryError::TransactionAborted => ErrorCode::TransactionAborted,
        MongrelQueryError::NoSqlTransaction | MongrelQueryError::SavepointNotFound { .. } => {
            ErrorCode::TransactionState
        }
        MongrelQueryError::CommitOutcome { .. } => ErrorCode::CommitOutcome,
        MongrelQueryError::OutcomeUnknown { .. } => ErrorCode::OutcomeUnknown,
        MongrelQueryError::InvalidQueryState(_) => ErrorCode::InvalidQueryState,
        MongrelQueryError::Arrow(_)
        | MongrelQueryError::DataFusion(_)
        | MongrelQueryError::Schema(_) => ErrorCode::SqlExecution,
        _ => ErrorCode::SqlExecution,
    }
}

#[allow(clippy::too_many_arguments)]
fn fill_error_details(
    details: &mut mongreldb_error_details_v1,
    query_id: mongreldb_query::QueryId,
    committed: bool,
    committed_statements: usize,
    last_commit_epoch: Option<u64>,
    first_commit_statement_index: Option<usize>,
    last_commit_statement_index: Option<usize>,
    completed_statements: usize,
    statement_index: usize,
    cancellation_reason: mongreldb_core::CancellationReason,
    server_state: &str,
) {
    copy_c_text(&mut details.query_id, &query_id.to_string());
    details.committed = u8::from(committed);
    details.committed_statements = committed_statements;
    details.has_last_commit_epoch = u8::from(last_commit_epoch.is_some());
    details.last_commit_epoch = last_commit_epoch.unwrap_or_default();
    details.has_first_commit_statement_index = u8::from(first_commit_statement_index.is_some());
    details.first_commit_statement_index = first_commit_statement_index.unwrap_or_default();
    details.has_last_commit_statement_index = u8::from(last_commit_statement_index.is_some());
    details.last_commit_statement_index = last_commit_statement_index.unwrap_or_default();
    details.completed_statements = completed_statements;
    details.has_statement_index = 1;
    details.statement_index = statement_index;
    details.cancellation_reason = cancellation_reason as i32;
    copy_c_text(&mut details.server_state, server_state);
}

fn set_query_error(error: &mongreldb_query::MongrelQueryError) -> ErrorCode {
    use mongreldb_query::MongrelQueryError;
    if let MongrelQueryError::Core(error) = error {
        return set_error(error);
    }
    let code = ffi_query_error_code(error);
    let mut details = mongreldb_error_details_v1 {
        code: code.as_return(),
        outcome_known: u8::from(!matches!(error, MongrelQueryError::OutcomeUnknown { .. })),
        retryable: u8::from(matches!(error, MongrelQueryError::QueryRegistryFull)),
        ..Default::default()
    };
    match error {
        MongrelQueryError::QueryCancelled {
            query_id,
            reason,
            committed,
            committed_statements,
            last_commit_epoch,
            first_commit_statement_index,
            last_commit_statement_index,
            completed_statements,
            cancelled_statement_index,
        } => fill_error_details(
            &mut details,
            *query_id,
            *committed,
            *committed_statements,
            *last_commit_epoch,
            *first_commit_statement_index,
            *last_commit_statement_index,
            *completed_statements,
            *cancelled_statement_index,
            *reason,
            "cancelled",
        ),
        MongrelQueryError::DeadlineExceeded {
            query_id,
            committed,
            committed_statements,
            last_commit_epoch,
            first_commit_statement_index,
            last_commit_statement_index,
            completed_statements,
            cancelled_statement_index,
            ..
        } => fill_error_details(
            &mut details,
            *query_id,
            *committed,
            *committed_statements,
            *last_commit_epoch,
            *first_commit_statement_index,
            *last_commit_statement_index,
            *completed_statements,
            *cancelled_statement_index,
            mongreldb_core::CancellationReason::Deadline,
            "cancelled",
        ),
        MongrelQueryError::ResultLimitExceeded {
            query_id,
            committed,
            committed_statements,
            last_commit_epoch,
            first_commit_statement_index,
            last_commit_statement_index,
            completed_statements,
            statement_index,
            ..
        }
        | MongrelQueryError::CommitOutcome {
            query_id,
            committed,
            committed_statements,
            last_commit_epoch,
            first_commit_statement_index,
            last_commit_statement_index,
            completed_statements,
            statement_index,
            ..
        } => fill_error_details(
            &mut details,
            *query_id,
            *committed,
            *committed_statements,
            *last_commit_epoch,
            *first_commit_statement_index,
            *last_commit_statement_index,
            *completed_statements,
            *statement_index,
            mongreldb_core::CancellationReason::None,
            "failed",
        ),
        MongrelQueryError::OutcomeUnknown { query_id, .. } => {
            copy_c_text(&mut details.query_id, &query_id.to_string());
            copy_c_text(&mut details.server_state, "failed");
        }
        MongrelQueryError::QueryIdConflict { query_id } => {
            copy_c_text(&mut details.query_id, &query_id.to_string());
        }
        _ => {}
    }
    set_error_with_details(code, error.to_string(), details)
}

fn ffi_serialization_error_code(committed: bool) -> ErrorCode {
    if committed {
        ErrorCode::SerializationAfterCommit
    } else {
        ErrorCode::Serialization
    }
}

fn query_phase_text(phase: mongreldb_query::SqlQueryPhase) -> &'static str {
    match phase {
        mongreldb_query::SqlQueryPhase::Queued => "queued",
        mongreldb_query::SqlQueryPhase::Planning => "planning",
        mongreldb_query::SqlQueryPhase::Executing => "executing",
        mongreldb_query::SqlQueryPhase::Streaming => "streaming",
        mongreldb_query::SqlQueryPhase::Serializing => "serializing",
        mongreldb_query::SqlQueryPhase::CommitCritical => "commit_critical",
        mongreldb_query::SqlQueryPhase::Cancelling => "cancelling",
        mongreldb_query::SqlQueryPhase::Completed => "completed",
        mongreldb_query::SqlQueryPhase::Failed => "failed",
        mongreldb_query::SqlQueryPhase::Cancelled => "cancelled",
    }
}

fn set_status_error(
    query: &mongreldb_query::RegisteredSqlQuery,
    code: ErrorCode,
    message: impl Into<String>,
) -> ErrorCode {
    let status = query.status();
    let mut details = mongreldb_error_details_v1 {
        code: code.as_return(),
        outcome_known: u8::from(!status.outcome_unknown),
        retryable: u8::from(status.terminal_error.as_ref().is_some_and(|error| {
            matches!(
                error.code.as_str(),
                "QUERY_REGISTRY_FULL" | "IDEMPOTENCY_STORE_FULL" | "IDEMPOTENCY_STORE_UNAVAILABLE"
            )
        })),
        cancellation_reason: status.cancellation_reason as i32,
        cancel_outcome: match status.phase {
            mongreldb_query::SqlQueryPhase::CommitCritical => 3,
            mongreldb_query::SqlQueryPhase::Completed
            | mongreldb_query::SqlQueryPhase::Failed
            | mongreldb_query::SqlQueryPhase::Cancelled => 4,
            mongreldb_query::SqlQueryPhase::Cancelling => 1,
            _ => 0,
        },
        ..Default::default()
    };
    copy_c_text(&mut details.query_id, &status.query_id.to_string());
    copy_c_text(&mut details.server_state, query_phase_text(status.phase));
    if !status.outcome_unknown {
        details.committed = u8::from(status.durable_outcome.committed);
        details.committed_statements = status.durable_outcome.committed_statements;
        details.has_last_commit_epoch =
            u8::from(status.durable_outcome.last_commit_epoch.is_some());
        details.last_commit_epoch = status.durable_outcome.last_commit_epoch.unwrap_or_default();
        details.has_first_commit_statement_index = u8::from(
            status
                .durable_outcome
                .first_commit_statement_index
                .is_some(),
        );
        details.first_commit_statement_index = status
            .durable_outcome
            .first_commit_statement_index
            .unwrap_or_default();
        details.has_last_commit_statement_index =
            u8::from(status.durable_outcome.last_commit_statement_index.is_some());
        details.last_commit_statement_index = status
            .durable_outcome
            .last_commit_statement_index
            .unwrap_or_default();
        details.completed_statements = status.completed_statements;
        details.has_statement_index = 1;
        details.statement_index = status.statement_index;
    }
    set_error_with_details(code, message, details)
}

fn batches_to_ipc_controlled(
    batches: &[arrow::record_batch::RecordBatch],
    query: &mongreldb_query::RegisteredSqlQuery,
    max_rows: usize,
    max_bytes: usize,
) -> Result<Vec<u8>, FfiOutputFailure> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let schema = batches[0].schema();
    let exceeded = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut output = LimitedIpcOutput {
        bytes: Vec::new(),
        max_bytes,
        exceeded: Arc::clone(&exceeded),
    };
    let mut rows = 0usize;
    {
        let mut writer = match FileWriter::try_new(&mut output, schema.as_ref()) {
            Ok(writer) => writer,
            Err(error) => {
                if exceeded.load(std::sync::atomic::Ordering::Acquire) {
                    return Err(FfiOutputFailure::ResultLimit(format!(
                        "RESULT_LIMIT_EXCEEDED: SQL result exceeds {max_bytes} bytes"
                    )));
                }
                return Err(FfiOutputFailure::Serialization(error.to_string()));
            }
        };
        for batch in batches {
            for offset in (0..batch.num_rows()).step_by(256) {
                query.checkpoint().map_err(FfiOutputFailure::Query)?;
                let length = 256.min(batch.num_rows() - offset);
                rows = rows.saturating_add(length);
                if rows > max_rows {
                    return Err(FfiOutputFailure::ResultLimit(format!(
                        "RESULT_LIMIT_EXCEEDED: SQL result exceeds {max_rows} rows"
                    )));
                }
                if let Err(error) = writer.write(&batch.slice(offset, length)) {
                    if exceeded.load(std::sync::atomic::Ordering::Acquire) {
                        return Err(FfiOutputFailure::ResultLimit(format!(
                            "RESULT_LIMIT_EXCEEDED: SQL result exceeds {max_bytes} bytes"
                        )));
                    }
                    return Err(FfiOutputFailure::Serialization(error.to_string()));
                }
            }
        }
        if let Err(error) = writer.finish() {
            if exceeded.load(std::sync::atomic::Ordering::Acquire) {
                return Err(FfiOutputFailure::ResultLimit(format!(
                    "RESULT_LIMIT_EXCEEDED: SQL result exceeds {max_bytes} bytes"
                )));
            }
            return Err(FfiOutputFailure::Serialization(error.to_string()));
        }
    }
    query.checkpoint().map_err(FfiOutputFailure::Query)?;
    Ok(output.bytes)
}

struct LimitedIpcOutput {
    bytes: Vec<u8>,
    max_bytes: usize,
    exceeded: Arc<std::sync::atomic::AtomicBool>,
}

impl std::io::Write for LimitedIpcOutput {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.bytes.len().saturating_add(bytes.len()) > self.max_bytes {
            self.exceeded
                .store(true, std::sync::atomic::Ordering::Release);
            return Err(std::io::Error::other("SQL result byte limit exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub type mongreldb_sql_query_t = *mut c_void;

#[repr(C)]
pub struct mongreldb_sql_options {
    pub query_id: *const c_char,
    pub timeout_ms: u64,
}

#[repr(C)]
pub struct mongreldb_sql_options_v2 {
    pub query_id: *const c_char,
    pub timeout_ms: u64,
    pub max_output_rows: usize,
    pub max_output_bytes: usize,
}

#[repr(C)]
pub struct mongreldb_sql_result_t {
    pub data: *mut u8,
    pub len: usize,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct mongreldb_sql_query_status_v1 {
    pub query_id: [c_char; 33],
    pub phase: i32,
    pub terminal_state: i32,
    pub committed: u8,
    pub committed_statements: usize,
    pub has_last_commit_epoch: u8,
    pub last_commit_epoch: u64,
    pub has_first_commit_statement_index: u8,
    pub first_commit_statement_index: usize,
    pub has_last_commit_statement_index: u8,
    pub last_commit_statement_index: usize,
    pub completed_statements: usize,
    pub statement_index: usize,
    pub cancel_outcome: i32,
    pub cancellation_reason: i32,
    pub retryable: u8,
    pub terminal_error_category: i32,
    pub terminal_error_code: [c_char; 64],
}

/// V2 status preserves whether the durable outcome fields are known. When
/// `outcome_known` is zero, every commit/progress field in `v1` is an
/// unspecified compatibility placeholder and must not be interpreted as zero.
#[repr(C)]
pub struct mongreldb_sql_query_status_v2 {
    pub v1: mongreldb_sql_query_status_v1,
    pub outcome_known: u8,
}

struct FFISqlQuery {
    query: mongreldb_query::RegisteredSqlQuery,
    worker: Mutex<Option<SqlWorker>>,
}

type SqlWorker = std::thread::JoinHandle<Result<Vec<u8>, FfiSqlFailure>>;

unsafe fn as_sql_query(handle: mongreldb_sql_query_t) -> Option<&'static FFISqlQuery> {
    if handle.is_null() {
        set_error_msg(ErrorCode::InvalidArgument, "SQL query handle is null");
        return None;
    }
    Some(&*(handle as *const FFISqlQuery))
}

/// Start SQL on a background worker and return an addressable query handle.
/// A null `options` pointer selects a random query ID and configured timeout.
unsafe fn sql_query_start_with_limits(
    db: mongreldb_database_t,
    sql: *const c_char,
    options: *const mongreldb_sql_options,
    max_output_rows: usize,
    max_output_bytes: usize,
) -> mongreldb_sql_query_t {
    clear();
    let Some(database) = as_db(db) else {
        return std::ptr::null_mut();
    };
    if sql.is_null() {
        set_error_msg(ErrorCode::InvalidArgument, "sql must not be null");
        return std::ptr::null_mut();
    }
    let sql = match std::ffi::CStr::from_ptr(sql).to_str() {
        Ok(sql) => sql.to_owned(),
        Err(_) => {
            set_error_msg(ErrorCode::InvalidArgument, "sql is not valid UTF-8");
            return std::ptr::null_mut();
        }
    };
    let (query_id, timeout) = if options.is_null() {
        (None, None)
    } else {
        let options = &*options;
        let query_id = if options.query_id.is_null() {
            None
        } else {
            let query_id = match std::ffi::CStr::from_ptr(options.query_id).to_str() {
                Ok(query_id) => query_id,
                Err(_) => {
                    set_error_msg(ErrorCode::InvalidArgument, "query_id is not valid UTF-8");
                    return std::ptr::null_mut();
                }
            };
            match query_id.parse::<mongreldb_query::QueryId>() {
                Ok(query_id) => Some(query_id),
                Err(error) => {
                    set_error_msg(ErrorCode::InvalidArgument, error.to_string());
                    return std::ptr::null_mut();
                }
            }
        };
        let timeout =
            (options.timeout_ms > 0).then(|| std::time::Duration::from_millis(options.timeout_ms));
        (query_id, timeout)
    };
    let session = match database_session(database) {
        Ok(session) => session,
        Err(error) => {
            set_query_error(&error);
            return std::ptr::null_mut();
        }
    };
    let query = match session.register_query(mongreldb_query::SqlQueryOptions {
        query_id,
        timeout,
        ..mongreldb_query::SqlQueryOptions::default()
    }) {
        Ok(query) => query,
        Err(error) => {
            set_query_error(&error);
            return std::ptr::null_mut();
        }
    };
    let id = query.id();
    let retained_query = query.clone();
    let registration = mongreldb_query::RegisteredQueryGuard::new(query);
    let worker_session = Arc::clone(&session);
    let runtime = match sql_runtime() {
        Ok(runtime) => runtime,
        Err(error) => {
            set_error_msg(ErrorCode::Unknown, error);
            return std::ptr::null_mut();
        }
    };
    let worker = match std::thread::Builder::new()
        .name(format!("mongreldb-ffi-sql-{id}"))
        .spawn(move || {
            let output = runtime
                .block_on(worker_session.run_with_query_for_serialization_with_limits(
                    &sql,
                    registration.into_query(),
                    mongreldb_query::SqlCollectionLimits::new(max_output_rows, max_output_bytes),
                ))
                .map_err(FfiSqlFailure::Query)?;
            worker_session
                .fire_test_hook(mongreldb_query::SqlTestHookPoint::BeforeSerializationBatch);
            match batches_to_ipc_controlled(
                output.batches(),
                output.query(),
                max_output_rows,
                max_output_bytes,
            ) {
                Ok(bytes) => {
                    worker_session
                        .fire_test_hook(mongreldb_query::SqlTestHookPoint::AfterSerialization);
                    output.try_complete().map_err(FfiSqlFailure::Query)?;
                    Ok(bytes)
                }
                Err(FfiOutputFailure::Query(error)) => {
                    output.fail();
                    Err(FfiSqlFailure::Query(error))
                }
                Err(FfiOutputFailure::ResultLimit(error)) => {
                    output.fail_result_limit();
                    Err(FfiSqlFailure::ResultLimit(error))
                }
                Err(FfiOutputFailure::Serialization(error)) => {
                    output.fail_serialization();
                    Err(FfiSqlFailure::Serialization(error))
                }
            }
        }) {
        Ok(worker) => worker,
        Err(error) => {
            set_error_msg(ErrorCode::Unknown, error.to_string());
            return std::ptr::null_mut();
        }
    };
    Box::into_raw(Box::new(FFISqlQuery {
        query: retained_query,
        worker: Mutex::new(Some(worker)),
    })) as mongreldb_sql_query_t
}

#[no_mangle]
pub unsafe extern "C" fn mongreldb_sql_query_start(
    db: mongreldb_database_t,
    sql: *const c_char,
    options: *const mongreldb_sql_options,
) -> mongreldb_sql_query_t {
    sql_query_start_with_limits(db, sql, options, 1_000_000, 64 * 1024 * 1024)
}

#[no_mangle]
pub unsafe extern "C" fn mongreldb_sql_query_start_v2(
    db: mongreldb_database_t,
    sql: *const c_char,
    options: *const mongreldb_sql_options_v2,
) -> mongreldb_sql_query_t {
    if options.is_null() {
        return sql_query_start_with_limits(db, sql, std::ptr::null(), 1_000_000, 64 * 1024 * 1024);
    }
    let options = &*options;
    let legacy = mongreldb_sql_options {
        query_id: options.query_id,
        timeout_ms: options.timeout_ms,
    };
    sql_query_start_with_limits(
        db,
        sql,
        &legacy,
        if options.max_output_rows == 0 {
            1_000_000
        } else {
            options.max_output_rows
        },
        if options.max_output_bytes == 0 {
            64 * 1024 * 1024
        } else {
            options.max_output_bytes
        },
    )
}

#[no_mangle]
pub unsafe extern "C" fn mongreldb_sql_query_cancel(query: mongreldb_sql_query_t) -> i32 {
    clear();
    let Some(query) = as_sql_query(query) else {
        return ErrorCode::InvalidArgument.as_return();
    };
    match query
        .query
        .request_cancel(mongreldb_core::CancellationReason::ClientRequest)
    {
        mongreldb_query::CancelOutcome::Accepted => 1,
        mongreldb_query::CancelOutcome::AlreadyCancelling => 2,
        mongreldb_query::CancelOutcome::TooLate => 3,
        mongreldb_query::CancelOutcome::AlreadyFinished => 4,
        mongreldb_query::CancelOutcome::NotFound => 5,
    }
}

fn copy_status_text<const N: usize>(target: &mut [c_char; N], value: &str) {
    let bytes = value.as_bytes();
    let length = bytes.len().min(N.saturating_sub(1));
    for (target, source) in target.iter_mut().zip(bytes.iter()).take(length) {
        *target = *source as c_char;
    }
    target[length] = 0;
}

#[no_mangle]
pub unsafe extern "C" fn mongreldb_sql_query_get_status(
    query: mongreldb_sql_query_t,
    out_status: *mut mongreldb_sql_query_status_v1,
) -> i32 {
    clear();
    if out_status.is_null() {
        return set_error_msg(ErrorCode::InvalidArgument, "out_status must not be null")
            .as_return();
    }
    let Some(query) = as_sql_query(query) else {
        return ErrorCode::InvalidArgument.as_return();
    };
    let status = query.query.status();
    let terminal_state = status.terminal_state().map_or(0, |state| match state {
        mongreldb_query::QueryTerminalState::Completed => 1,
        mongreldb_query::QueryTerminalState::FailedBeforeCommit => 2,
        mongreldb_query::QueryTerminalState::CancelledBeforeCommit => 3,
        mongreldb_query::QueryTerminalState::DeadlineBeforeCommit => 4,
        mongreldb_query::QueryTerminalState::Committed => 5,
        mongreldb_query::QueryTerminalState::CommittedWithError => 6,
        mongreldb_query::QueryTerminalState::PartiallyCommitted => 7,
        mongreldb_query::QueryTerminalState::CancelledAfterCommit => 8,
        mongreldb_query::QueryTerminalState::DeadlineAfterCommit => 9,
        mongreldb_query::QueryTerminalState::OutcomeUnknown => 10,
    });
    let phase = match status.phase {
        mongreldb_query::SqlQueryPhase::Queued => 1,
        mongreldb_query::SqlQueryPhase::Planning => 2,
        mongreldb_query::SqlQueryPhase::Executing => 3,
        mongreldb_query::SqlQueryPhase::Streaming => 4,
        mongreldb_query::SqlQueryPhase::Serializing => 5,
        mongreldb_query::SqlQueryPhase::CommitCritical => 6,
        mongreldb_query::SqlQueryPhase::Cancelling => 7,
        mongreldb_query::SqlQueryPhase::Completed => 8,
        mongreldb_query::SqlQueryPhase::Failed => 9,
        mongreldb_query::SqlQueryPhase::Cancelled => 10,
    };
    let cancel_outcome = match status.phase {
        mongreldb_query::SqlQueryPhase::CommitCritical => 3,
        mongreldb_query::SqlQueryPhase::Completed
        | mongreldb_query::SqlQueryPhase::Failed
        | mongreldb_query::SqlQueryPhase::Cancelled => 4,
        mongreldb_query::SqlQueryPhase::Cancelling => 1,
        _ => 0,
    };
    let terminal_error_category =
        status
            .terminal_error
            .as_ref()
            .map_or(0, |error| match error.category {
                mongreldb_query::QueryTerminalErrorCategory::Cancellation => 1,
                mongreldb_query::QueryTerminalErrorCategory::Deadline => 2,
                mongreldb_query::QueryTerminalErrorCategory::ResultLimit => 3,
                mongreldb_query::QueryTerminalErrorCategory::Serialization => 4,
                mongreldb_query::QueryTerminalErrorCategory::Execution => 5,
            });
    let retryable = status.terminal_error.as_ref().is_some_and(|error| {
        matches!(
            error.code.as_str(),
            "QUERY_REGISTRY_FULL" | "IDEMPOTENCY_STORE_FULL" | "IDEMPOTENCY_STORE_UNAVAILABLE"
        )
    });
    let mut result = mongreldb_sql_query_status_v1 {
        query_id: [0; 33],
        phase,
        terminal_state,
        committed: u8::from(status.durable_outcome.committed),
        committed_statements: status.durable_outcome.committed_statements,
        has_last_commit_epoch: u8::from(status.durable_outcome.last_commit_epoch.is_some()),
        last_commit_epoch: status.durable_outcome.last_commit_epoch.unwrap_or_default(),
        has_first_commit_statement_index: u8::from(
            status
                .durable_outcome
                .first_commit_statement_index
                .is_some(),
        ),
        first_commit_statement_index: status
            .durable_outcome
            .first_commit_statement_index
            .unwrap_or_default(),
        has_last_commit_statement_index: u8::from(
            status.durable_outcome.last_commit_statement_index.is_some(),
        ),
        last_commit_statement_index: status
            .durable_outcome
            .last_commit_statement_index
            .unwrap_or_default(),
        completed_statements: status.completed_statements,
        statement_index: status.statement_index,
        cancel_outcome,
        cancellation_reason: status.cancellation_reason as i32,
        retryable: u8::from(retryable),
        terminal_error_category,
        terminal_error_code: [0; 64],
    };
    copy_status_text(&mut result.query_id, &status.query_id.to_string());
    if let Some(error) = status.terminal_error {
        copy_status_text(&mut result.terminal_error_code, &error.code);
    }
    *out_status = result;
    0
}

fn ffi_outcome_known(terminal_state: i32) -> u8 {
    u8::from(terminal_state != 10)
}

/// Return structured query state without collapsing an indeterminate durable
/// outcome into `committed = 0`.
///
/// # Safety
/// `query` must be a valid query handle and `out_status` must be a valid,
/// non-null pointer. When `outcome_known` is zero, callers must ignore the
/// commit/progress fields in `out_status->v1`.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_sql_query_get_status_v2(
    query: mongreldb_sql_query_t,
    out_status: *mut mongreldb_sql_query_status_v2,
) -> i32 {
    clear();
    if out_status.is_null() {
        return set_error_msg(ErrorCode::InvalidArgument, "out_status must not be null")
            .as_return();
    }
    let mut v1: mongreldb_sql_query_status_v1 = std::mem::zeroed();
    let result = mongreldb_sql_query_get_status(query, &mut v1);
    if result != 0 {
        return result;
    }
    *out_status = mongreldb_sql_query_status_v2 {
        outcome_known: ffi_outcome_known(v1.terminal_state),
        v1,
    };
    0
}

#[no_mangle]
pub unsafe extern "C" fn mongreldb_sql_query_wait(
    query: mongreldb_sql_query_t,
    out_result: *mut mongreldb_sql_result_t,
) -> i32 {
    clear();
    if out_result.is_null() {
        return set_error_msg(ErrorCode::InvalidArgument, "out_result must not be null")
            .as_return();
    }
    (*out_result).data = std::ptr::null_mut();
    (*out_result).len = 0;
    let Some(query) = as_sql_query(query) else {
        return ErrorCode::InvalidArgument.as_return();
    };
    let worker = match query.worker.lock() {
        Ok(mut worker) => match worker.take() {
            Some(worker) => worker,
            None => {
                return set_error_msg(ErrorCode::InvalidArgument, "SQL query already waited")
                    .as_return()
            }
        },
        Err(_) => return set_error_msg(ErrorCode::Unknown, "SQL query lock poisoned").as_return(),
    };
    let bytes = match worker.join() {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(FfiSqlFailure::Query(error))) => return set_query_error(&error).as_return(),
        Ok(Err(FfiSqlFailure::ResultLimit(error))) => {
            return set_status_error(&query.query, ErrorCode::ResultLimit, error).as_return()
        }
        Ok(Err(FfiSqlFailure::Serialization(error))) => {
            let committed = query.query.status().durable_outcome.committed;
            return set_status_error(&query.query, ffi_serialization_error_code(committed), error)
                .as_return();
        }
        Err(_) => {
            return set_status_error(&query.query, ErrorCode::Unknown, "SQL worker panicked")
                .as_return()
        }
    };
    let len = bytes.len();
    let mut bytes = bytes.into_boxed_slice();
    let data = bytes.as_mut_ptr();
    std::mem::forget(bytes);
    (*out_result).data = data;
    (*out_result).len = len;
    0
}

#[no_mangle]
pub unsafe extern "C" fn mongreldb_sql_query_free(query: mongreldb_sql_query_t) {
    if query.is_null() {
        return;
    }
    let query = Box::from_raw(query as *mut FFISqlQuery);
    let _ = query
        .query
        .request_cancel(mongreldb_core::CancellationReason::ClientDisconnected);
    drop(query);
}

/// Run a SQL statement and write the result (Arrow IPC file bytes) into
/// `*out_buf` / `*out_len`. For DDL/DML the buffer is empty (`*out_len = 0`
/// and `*out_buf` points to a zero-length allocation). Returns 0 on success.
///
/// The session is cached on the database handle; repeated calls reuse it.
/// After a `mongreldb_create_table` / `mongreldb_drop_table` / schema change,
/// the cached session may not see the new table set — call
/// [`mongreldb_database_sql_refresh`] to rebuild it.
///
/// The caller owns `*out_buf` and must free it with
/// [`mongreldb_free_sql_result`].
///
/// # Safety
/// `db` must be a valid handle; `sql` must be a NUL-terminated UTF-8 C string;
/// `out_buf` and `out_len` must be valid non-null pointers.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_database_sql(
    db: mongreldb_database_t,
    sql: *const c_char,
    out_buf: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    clear();
    // Zero the out-params on every error path so callers can always safely call
    // mongreldb_free_sql_result(*out_buf, *out_len) without double-freeing a
    // stale pointer from a previous successful call.
    if !out_buf.is_null() {
        *out_buf = std::ptr::null_mut();
    }
    if !out_len.is_null() {
        *out_len = 0;
    }
    if out_buf.is_null() || out_len.is_null() {
        return set_error_msg(
            ErrorCode::InvalidArgument,
            "out_buf and out_len must not be null",
        )
        .as_return();
    }

    let query = mongreldb_sql_query_start(db, sql, std::ptr::null());
    if query.is_null() {
        return crate::error::mongreldb_last_error_code();
    }
    let mut result = mongreldb_sql_result_t {
        data: std::ptr::null_mut(),
        len: 0,
    };
    let status = mongreldb_sql_query_wait(query, &mut result);
    mongreldb_sql_query_free(query);
    if status == 0 {
        *out_buf = result.data;
        *out_len = result.len;
    }
    status
}

/// Rebuild the cached SQL session so it sees the current table set. The
/// `MongrelSession` snapshots the catalog at construction; after a table
/// create/drop/rename via the FFI (not via SQL), the session must be rebuilt.
/// Returns 0 on success. No-op if no session was cached yet.
///
/// # Safety
/// `db` must be a valid handle.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_database_sql_refresh(db: mongreldb_database_t) -> i32 {
    clear();
    let Some(h) = as_db(db) else {
        return ErrorCode::InvalidArgument.as_return();
    };
    match MongrelSession::open(h.db.clone()) {
        Ok(session) => {
            *h.sql_session.write() = Some(Arc::new(session));
            0
        }
        Err(e) => set_error_msg(ErrorCode::Unknown, format!("{e:?}")).as_return(),
    }
}

/// Free a byte buffer previously returned by [`mongreldb_database_sql`]. Safe
/// to call with null or a zero-length pointer.
///
/// # Safety
/// `ptr` must be null or a pointer returned by `mongreldb_database_sql`, and
/// must not be freed twice. `len` must match the length returned at allocation
/// time.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_free_sql_result(ptr: *mut u8, len: usize) {
    if ptr.is_null() || len == 0 {
        return;
    }
    // SAFETY: caller guarantees the pointer was produced by
    // `mongreldb_database_sql` with exactly this length. Reconstruct the
    // boxed slice and drop it.
    let slice = std::slice::from_raw_parts_mut(ptr, len);
    drop(Box::from_raw(slice as *mut [u8]));
}

#[cfg(test)]
mod tests {
    use super::{
        batches_to_ipc_controlled, ffi_outcome_known, ffi_query_error_code,
        ffi_serialization_error_code,
    };
    use crate::error::ErrorCode;
    use arrow::array::Int64Array;
    use arrow::record_batch::RecordBatch;
    use mongreldb_query::{QueryTerminalErrorCategory, SqlQueryOptions, SqlQueryRegistry};
    use std::sync::Arc;

    #[test]
    fn status_v2_marks_unknown_outcome_as_not_known() {
        assert_eq!(ffi_outcome_known(10), 0);
        assert_eq!(ffi_outcome_known(2), 1);
        assert_eq!(ffi_outcome_known(5), 1);
    }

    #[test]
    fn ffi_handle_status_survives_registry_tombstone_eviction() {
        let registry = Arc::new(SqlQueryRegistry::new(
            4,
            1,
            16 * 1024,
            std::time::Duration::from_secs(60),
        ));
        let query = registry.register(SqlQueryOptions::default()).unwrap();
        let id = query.id();
        query.try_complete().unwrap();
        let handle = Box::into_raw(Box::new(super::FFISqlQuery {
            query: query.clone(),
            worker: std::sync::Mutex::new(None),
        })) as super::mongreldb_sql_query_t;
        let evictor = registry.register(SqlQueryOptions::default()).unwrap();
        evictor.try_complete().unwrap();
        assert!(registry.status(id).is_none());

        let mut status: super::mongreldb_sql_query_status_v1 = unsafe { std::mem::zeroed() };
        assert_eq!(
            unsafe { super::mongreldb_sql_query_get_status(handle, &mut status) },
            0
        );
        assert_eq!(status.phase, 8);
        unsafe { super::mongreldb_sql_query_free(handle) };
    }

    #[test]
    fn controlled_ipc_records_row_and_byte_limits() {
        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int64Array::from(vec![1, 2])) as arrow::array::ArrayRef,
        )])
        .unwrap();

        for (max_rows, max_bytes) in [(1, 1_024), (10, 1)] {
            let registry = Arc::new(SqlQueryRegistry::default());
            let query = registry.register(SqlQueryOptions::default()).unwrap();
            let id = query.id();
            let error = batches_to_ipc_controlled(
                std::slice::from_ref(&batch),
                &query,
                max_rows,
                max_bytes,
            )
            .unwrap_err();
            let message = match error {
                super::FfiOutputFailure::Query(error) => error.to_string(),
                super::FfiOutputFailure::ResultLimit(message)
                | super::FfiOutputFailure::Serialization(message) => message,
            };
            assert!(message.contains("RESULT_LIMIT_EXCEEDED"));
            query.fail_result_limit();
            let status = registry.status(id).unwrap();
            assert_eq!(
                status.terminal_error.unwrap().category,
                QueryTerminalErrorCategory::ResultLimit
            );
        }
    }

    #[test]
    fn controlled_ipc_observes_cancel_and_deadline() {
        let batch = batch();
        for deadline in [false, true] {
            let registry = Arc::new(SqlQueryRegistry::default());
            let query = registry
                .register(SqlQueryOptions {
                    timeout: deadline.then(|| std::time::Duration::from_millis(1)),
                    ..SqlQueryOptions::default()
                })
                .unwrap();
            if deadline {
                std::thread::sleep(std::time::Duration::from_millis(2));
            } else {
                assert_eq!(
                    query.request_cancel(mongreldb_core::CancellationReason::ClientRequest),
                    mongreldb_query::CancelOutcome::Accepted
                );
            }
            let error = batches_to_ipc_controlled(std::slice::from_ref(&batch), &query, 10, 1_024)
                .unwrap_err();
            let message = match error {
                super::FfiOutputFailure::Query(error) => error.to_string(),
                super::FfiOutputFailure::ResultLimit(message)
                | super::FfiOutputFailure::Serialization(message) => message,
            };
            assert!(
                message.contains("cancelled") || message.contains("deadline exceeded"),
                "{message}"
            );
            query.fail();
        }
    }

    #[test]
    fn query_start_failures_have_stable_error_codes() {
        let query_id = "00112233445566778899aabbccddeeff".parse().unwrap();
        assert_eq!(
            ffi_query_error_code(&mongreldb_query::MongrelQueryError::QueryIdConflict { query_id }),
            ErrorCode::QueryIdConflict
        );
        assert_eq!(
            ffi_query_error_code(&mongreldb_query::MongrelQueryError::QueryRegistryFull),
            ErrorCode::QueryRegistryFull
        );
        let cancelled_after_commit = mongreldb_query::MongrelQueryError::QueryCancelled {
            query_id,
            reason: mongreldb_core::CancellationReason::ClientRequest,
            committed: true,
            committed_statements: 1,
            last_commit_epoch: Some(7),
            first_commit_statement_index: Some(0),
            last_commit_statement_index: Some(0),
            completed_statements: 0,
            cancelled_statement_index: 0,
        };
        assert_eq!(
            ffi_query_error_code(&cancelled_after_commit),
            ErrorCode::QueryCancelledAfterCommit
        );
        assert_eq!(
            ffi_serialization_error_code(true),
            ErrorCode::SerializationAfterCommit
        );
        assert_eq!(
            ffi_query_error_code(&mongreldb_query::MongrelQueryError::TransactionAborted),
            ErrorCode::TransactionAborted
        );
        assert_eq!(
            ffi_serialization_error_code(false),
            ErrorCode::Serialization
        );
    }

    fn batch() -> RecordBatch {
        RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int64Array::from(vec![1, 2])) as arrow::array::ArrayRef,
        )])
        .unwrap()
    }
}
