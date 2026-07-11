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
use crate::error::{clear, set_error, set_error_msg, ErrorCode};
use arrow::ipc::writer::FileWriter;
use mongreldb_query::MongrelSession;
use std::os::raw::c_char;
use std::sync::OnceLock;

/// Process-global tokio runtime for SQL execution. Single-threaded
/// (current-thread) since the FFI is synchronous and we only need to drive
/// one `MongrelSession::run` future at a time. Mirrors Kit's `sql_runtime()`.
fn sql_runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build FFI SQL tokio runtime")
    })
}

/// Serialize Arrow record batches to IPC *file* bytes. Empty input yields an
/// empty `Vec<u8>` (matching the NAPI addon and Kit behavior for DDL/DML).
fn batches_to_ipc(batches: &[arrow::record_batch::RecordBatch]) -> Result<Vec<u8>, arrow::error::ArrowError> {
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
    let Some(h) = as_db(db) else {
        return ErrorCode::InvalidArgument.as_return();
    };
    if sql.is_null() {
        return set_error_msg(ErrorCode::InvalidArgument, "sql must not be null").as_return();
    }
    if out_buf.is_null() || out_len.is_null() {
        return set_error_msg(
            ErrorCode::InvalidArgument,
            "out_buf and out_len must not be null",
        )
        .as_return();
    }

    // SAFETY: caller guarantees a valid NUL-terminated C string.
    let sql_str = match std::ffi::CStr::from_ptr(sql).to_str() {
        Ok(s) => s.to_owned(),
        Err(_) => {
            return set_error_msg(ErrorCode::InvalidArgument, "sql is not valid UTF-8").as_return();
        }
    };

    // Lazily open (or reuse) the cached MongrelSession.
    let session = match h.sql_session.lock().take() {
        Some(s) => s,
        None => match MongrelSession::open(h.db.clone()) {
            Ok(s) => s,
            Err(e) => return set_error_msg(ErrorCode::Unknown, format!("{e:?}")).as_return(),
        },
    };

    let runtime = sql_runtime();
    let result = runtime.block_on(session.run(&sql_str));

    // Preserve the session for subsequent calls (even on error - it may hold
    // session state worth keeping).
    *h.sql_session.lock() = Some(session);

    let batches = match result {
        Ok(b) => b,
        Err(e) => return set_error_msg(ErrorCode::Unknown, format!("{e:?}")).as_return(),
    };

    let ipc = match batches_to_ipc(&batches) {
        Ok(bytes) => bytes,
        Err(e) => return set_error_msg(ErrorCode::Unknown, format!("{e:?}")).as_return(),
    };

    // Hand ownership of the byte buffer to C. We leak it into a raw pointer;
    // the caller reclaims it via mongreldb_free_sql_result.
    let len = ipc.len();
    let mut boxed = ipc.into_boxed_slice();
    let ptr = boxed.as_mut_ptr();
    std::mem::forget(boxed);
    *out_buf = ptr;
    *out_len = len;
    0
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
            *h.sql_session.lock() = Some(session);
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
