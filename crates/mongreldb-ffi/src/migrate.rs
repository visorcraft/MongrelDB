//! Migration planning FFI: exposes `plan_migrations` and `migration_checksum`
//! from `mongreldb-kit-core` through the C ABI, using JSON for the complex
//! `Migration`/`MigrationOp` types.
//!
//! The migration *planning* and *checksum* logic is centralized here so every
//! language binding produces identical results. The *execution* (applying each
//! `MigrationOp` to a live database) is orchestrated by the host language
//! using the existing FFI functions:
//!
//! - `CreateTable` → `mongreldb_schema_*` + `mongreldb_create_table`
//! - `DropTable`   → `mongreldb_drop_table`
//! - `RawSql`      → `mongreldb_database_sql`
//! - etc.
//!
//! See `docs/migrations.md` for the full op → FFI mapping.

use crate::error::{clear, set_error_msg, ErrorCode};
use mongreldb_kit_core::{migration_checksum, plan_migrations, Migration};
use std::os::raw::c_char;

/// Free a string returned by [`mongreldb_plan_migrations_json`] or
/// [`mongreldb_migration_checksum_json`]. This is an alias for
/// [`crate::mongreldb_free_string`] (same allocation path).
///
/// # Safety
/// `ptr` must be null or a pointer returned by a migration FFI function.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_free_migrate_string(ptr: *mut c_char) {
    if ptr.is_null() {
        return;
    }
    // SAFETY: same allocation path as mongreldb_free_string (CString::into_raw).
    drop(std::ffi::CString::from_raw(ptr));
}

/// Plan pending migrations. Takes two JSON arrays of `Migration` objects:
/// `applied_json` (migrations already recorded in the db) and `desired_json`
/// (the full ordered set defined by the application). Returns a JSON array of
/// the pending `Migration` objects (those with version > max applied), sorted
/// by version.
///
/// The result is written into `*out_json` as a NUL-terminated UTF-8 C string
/// owned by the FFI layer. The caller must free it with
/// [`mongreldb_free_migrate_string`].
///
/// JSON format (matches `serde_json` with `preserve_order`):
/// ```json
/// [{"version":1,"name":"initial","ops":[{"create_table":{"name":"users"}}]}]
/// ```
///
/// Returns 0 on success, negative error code on failure (call
/// `mongreldb_last_error()` for details).
///
/// # Safety
/// `applied_json` and `desired_json` must be NUL-terminated UTF-8 C strings
/// (valid JSON arrays). `out_json` must be a valid non-null pointer.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_plan_migrations_json(
    applied_json: *const c_char,
    desired_json: *const c_char,
    out_json: *mut *const c_char,
) -> i32 {
    clear();
    if applied_json.is_null() || desired_json.is_null() {
        return set_error_msg(
            ErrorCode::InvalidArgument,
            "applied_json and desired_json must not be null",
        )
        .as_return();
    }
    if out_json.is_null() {
        return set_error_msg(ErrorCode::InvalidArgument, "out_json must not be null").as_return();
    }

    // SAFETY: caller guarantees valid NUL-terminated C strings.
    let applied_str = match std::ffi::CStr::from_ptr(applied_json).to_str() {
        Ok(s) => s,
        Err(_) => {
            return set_error_msg(
                ErrorCode::InvalidArgument,
                "applied_json is not valid UTF-8",
            )
            .as_return();
        }
    };
    let desired_str = match std::ffi::CStr::from_ptr(desired_json).to_str() {
        Ok(s) => s,
        Err(_) => {
            return set_error_msg(
                ErrorCode::InvalidArgument,
                "desired_json is not valid UTF-8",
            )
            .as_return();
        }
    };

    let applied: Vec<Migration> = match serde_json::from_str(applied_str) {
        Ok(v) => v,
        Err(e) => {
            return set_error_msg(
                ErrorCode::InvalidArgument,
                format!("failed to parse applied_json: {e}"),
            )
            .as_return();
        }
    };
    let desired: Vec<Migration> = match serde_json::from_str(desired_str) {
        Ok(v) => v,
        Err(e) => {
            return set_error_msg(
                ErrorCode::InvalidArgument,
                format!("failed to parse desired_json: {e}"),
            )
            .as_return();
        }
    };

    let pending = plan_migrations(&applied, &desired);
    let json = match serde_json::to_string(&pending) {
        Ok(s) => s,
        Err(e) => {
            return set_error_msg(
                ErrorCode::Unknown,
                format!("failed to serialize result: {e}"),
            )
            .as_return();
        }
    };

    // Hand ownership to C via CString. The caller frees with
    // mongreldb_free_migrate_string.
    *out_json = std::ffi::CString::new(json)
        .expect("migration JSON should not contain interior NUL")
        .into_raw() as *const c_char;
    0
}

/// Compute the SHA-256 checksum of a single migration. The checksum is
/// canonical (byte-for-byte identical across all language bindings). Takes
/// the version (i64), name, and a JSON array of `MigrationOp` objects.
///
/// The result is written into `*out_checksum` as a NUL-terminated hex string.
/// The caller must free it with [`mongreldb_free_migrate_string`].
///
/// Returns 0 on success, negative error code on failure.
///
/// # Safety
/// `name` and `ops_json` must be NUL-terminated UTF-8 C strings. `ops_json`
/// must be valid JSON. `out_checksum` must be a valid non-null pointer.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_migration_checksum_json(
    version: i64,
    name: *const c_char,
    ops_json: *const c_char,
    out_checksum: *mut *const c_char,
) -> i32 {
    clear();
    if name.is_null() || ops_json.is_null() {
        return set_error_msg(
            ErrorCode::InvalidArgument,
            "name and ops_json must not be null",
        )
        .as_return();
    }
    if out_checksum.is_null() {
        return set_error_msg(ErrorCode::InvalidArgument, "out_checksum must not be null")
            .as_return();
    }

    let name_str = match std::ffi::CStr::from_ptr(name).to_str() {
        Ok(s) => s.to_owned(),
        Err(_) => {
            return set_error_msg(ErrorCode::InvalidArgument, "name is not valid UTF-8")
                .as_return();
        }
    };
    let ops_str = match std::ffi::CStr::from_ptr(ops_json).to_str() {
        Ok(s) => s,
        Err(_) => {
            return set_error_msg(ErrorCode::InvalidArgument, "ops_json is not valid UTF-8")
                .as_return();
        }
    };

    // Parse ops_json as a Vec<MigrationOp>.
    let ops: Vec<mongreldb_kit_core::MigrationOp> = match serde_json::from_str(ops_str) {
        Ok(v) => v,
        Err(e) => {
            return set_error_msg(
                ErrorCode::InvalidArgument,
                format!("failed to parse ops_json: {e}"),
            )
            .as_return();
        }
    };

    let checksum = migration_checksum(version, &name_str, &ops);
    *out_checksum = std::ffi::CString::new(checksum)
        .expect("checksum hex string should not contain interior NUL")
        .into_raw() as *const c_char;
    0
}
