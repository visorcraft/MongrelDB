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
use crate::error::{clear, set_error_msg, ErrorCode};
use arrow::ipc::writer::FileWriter;
use mongreldb_query::MongrelSession;
use std::os::raw::{c_char, c_void};
use std::sync::{Arc, Mutex, OnceLock};

/// Process-global tokio runtime for SQL execution. Single-threaded
/// (current-thread) since the FFI is synchronous and we only need to drive
/// one `MongrelSession::run` future at a time. Mirrors Kit's `sql_runtime()`.
fn sql_runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to build FFI SQL tokio runtime")
    })
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
fn batches_to_ipc(
    batches: &[arrow::record_batch::RecordBatch],
) -> Result<Vec<u8>, arrow::error::ArrowError> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let schema = batches[0].schema();
    let mut buf = Vec::new();
    {
        let mut writer = FileWriter::try_new(&mut buf, schema.as_ref())?;
        for b in batches {
            writer.write(b)?;
        }
        writer.finish()?;
    }
    Ok(buf)
}

pub type mongreldb_sql_query_t = *mut c_void;

#[repr(C)]
pub struct mongreldb_sql_options {
    pub query_id: *const c_char,
    pub timeout_ms: u64,
}

#[repr(C)]
pub struct mongreldb_sql_result_t {
    pub data: *mut u8,
    pub len: usize,
}

struct FFISqlQuery {
    id: mongreldb_query::QueryId,
    session: Arc<MongrelSession>,
    worker: Mutex<Option<SqlWorker>>,
}

type SqlWorker = std::thread::JoinHandle<Result<Vec<u8>, String>>;

unsafe fn as_sql_query(handle: mongreldb_sql_query_t) -> Option<&'static FFISqlQuery> {
    if handle.is_null() {
        set_error_msg(ErrorCode::InvalidArgument, "SQL query handle is null");
        return None;
    }
    Some(&*(handle as *const FFISqlQuery))
}

/// Start SQL on a background worker and return an addressable query handle.
/// A null `options` pointer selects a random query ID and configured timeout.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_sql_query_start(
    db: mongreldb_database_t,
    sql: *const c_char,
    options: *const mongreldb_sql_options,
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
            set_error_msg(ErrorCode::Unknown, error.to_string());
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
            set_error_msg(ErrorCode::Unknown, error.to_string());
            return std::ptr::null_mut();
        }
    };
    let id = query.id();
    let registration = mongreldb_query::RegisteredQueryGuard::new(query);
    let worker_session = Arc::clone(&session);
    let worker = match std::thread::Builder::new()
        .name(format!("mongreldb-ffi-sql-{id}"))
        .spawn(move || {
            let batches = sql_runtime()
                .block_on(worker_session.run_with_query(&sql, registration.into_query()))
                .map_err(|error| error.to_string())?;
            batches_to_ipc(&batches).map_err(|error| error.to_string())
        }) {
        Ok(worker) => worker,
        Err(error) => {
            set_error_msg(ErrorCode::Unknown, error.to_string());
            return std::ptr::null_mut();
        }
    };
    Box::into_raw(Box::new(FFISqlQuery {
        id,
        session,
        worker: Mutex::new(Some(worker)),
    })) as mongreldb_sql_query_t
}

#[no_mangle]
pub unsafe extern "C" fn mongreldb_sql_query_cancel(query: mongreldb_sql_query_t) -> i32 {
    clear();
    let Some(query) = as_sql_query(query) else {
        return ErrorCode::InvalidArgument.as_return();
    };
    match query.session.cancel_query(query.id) {
        mongreldb_query::CancelOutcome::Accepted
        | mongreldb_query::CancelOutcome::AlreadyCancelling => 1,
        mongreldb_query::CancelOutcome::TooLate
        | mongreldb_query::CancelOutcome::AlreadyFinished => 0,
        mongreldb_query::CancelOutcome::NotFound => {
            set_error_msg(ErrorCode::NotFound, "SQL query is not active").as_return()
        }
    }
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
        Ok(Err(error)) => return set_error_msg(ErrorCode::Unknown, error).as_return(),
        Err(_) => return set_error_msg(ErrorCode::Unknown, "SQL worker panicked").as_return(),
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
    let _ = query.session.cancel_query(query.id);
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
