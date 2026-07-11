//! Kit SQL FFI: `mongreldb_kit_sql_rows` returns JSON results,
//! `mongreldb_kit_sql_arrow` returns Arrow IPC file bytes.
//! Both delegate to the Kit Database's `sql_rows` / `sql_arrow` methods.

use crate::database::{as_kit_db, mongreldb_kit_database_t};
use crate::error::{clear, parse_cstr, set_error, set_error_msg, write_json_out, KitErrorCode};
use std::os::raw::c_char;

/// Run a SQL statement and return results as a JSON array of row objects
/// (column name -> value). DDL/DML returns an empty array `[]`. The result is
/// written into `*out_json` as a NUL-terminated UTF-8 C string (caller frees
/// with [`crate::mongreldb_kit_free_json`]).
///
/// # Safety
/// `db` must be a valid handle; `sql` must be a NUL-terminated C string;
/// `out_json` must be a valid non-null pointer.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_kit_sql_rows(
    db: mongreldb_kit_database_t,
    sql: *const c_char,
    out_json: *mut *const c_char,
) -> i32 {
    clear();
    let Some(h) = as_kit_db(db) else {
        return KitErrorCode::InvalidArgument.as_return();
    };
    let sql_str = match parse_cstr(sql, "sql") {
        Ok(s) => s,
        Err(code) => return code.as_return(),
    };
    if out_json.is_null() {
        return set_error_msg(KitErrorCode::InvalidArgument, "out_json must not be null").as_return();
    }

    let rows = match h.db.borrow().sql_rows(sql_str) {
        Ok(r) => r,
        Err(e) => return set_error(&e).as_return(),
    };

    // Serialize the Vec<Map<String, Value>> as a JSON array.
    let json_result = serde_json::to_string(&rows);
    write_json_out(json_result, out_json)
}

/// Run a SQL statement and return results as Arrow IPC *file* bytes (starts
/// with "ARROW1" magic). DDL/DML returns a zero-length buffer. The result is
/// written into `*out_buf` / `*out_len` (caller frees with
/// [`mongreldb_kit_free_arrow`]).
///
/// # Safety
/// `db` must be a valid handle; `sql` must be a NUL-terminated C string;
/// `out_buf` and `out_len` must be valid non-null pointers.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_kit_sql_arrow(
    db: mongreldb_kit_database_t,
    sql: *const c_char,
    out_buf: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    clear();
    // Zero out-params on every error path.
    if !out_buf.is_null() {
        *out_buf = std::ptr::null_mut();
    }
    if !out_len.is_null() {
        *out_len = 0;
    }
    let Some(h) = as_kit_db(db) else {
        return KitErrorCode::InvalidArgument.as_return();
    };
    let sql_str = match parse_cstr(sql, "sql") {
        Ok(s) => s,
        Err(code) => return code.as_return(),
    };
    if out_buf.is_null() || out_len.is_null() {
        return set_error_msg(
            KitErrorCode::InvalidArgument,
            "out_buf and out_len must not be null",
        )
        .as_return();
    }

    let ipc_bytes = match h.db.borrow().sql_arrow(sql_str) {
        Ok(bytes) => bytes,
        Err(e) => return set_error(&e).as_return(),
    };

    let len = ipc_bytes.len();
    let mut boxed = ipc_bytes.into_boxed_slice();
    let ptr = boxed.as_mut_ptr();
    std::mem::forget(boxed);
    *out_buf = ptr;
    *out_len = len;
    0
}

/// Free an Arrow IPC buffer returned by [`mongreldb_kit_sql_arrow`]. Safe to
/// call with null or zero-length.
///
/// # Safety
/// `ptr` must be null or a pointer returned by `mongreldb_kit_sql_arrow`,
/// and must not be freed twice. `len` must match.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_kit_free_arrow(ptr: *mut u8, len: usize) {
    if ptr.is_null() || len == 0 {
        return;
    }
    // SAFETY: caller guarantees the pointer/length pair came from
    // mongreldb_kit_sql_arrow.
    let slice = std::slice::from_raw_parts_mut(ptr, len);
    drop(Box::from_raw(slice as *mut [u8]));
}
