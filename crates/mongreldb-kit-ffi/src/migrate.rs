//! Kit migration FFI: `mongreldb_kit_migrate_json` runs the full Kit migration
//! runner (which applies each MigrationOp to the live database), and
//! `mongreldb_kit_applied_migrations_json` reads back the recorded migrations.

use crate::database::{as_kit_db, mongreldb_kit_database_t};
use crate::error::{clear, parse_cstr, set_error, set_error_msg, write_json_out, KitErrorCode};
use mongreldb_kit_core::Migration;
use std::os::raw::c_char;

/// Run the Kit migration runner. Takes a JSON array of `Migration` objects and
/// applies them to the database. The runner executes each `MigrationOp` via
/// the Kit Database's internal engine calls (create_table, sql for RawSql, etc.).
///
/// Migrations are applied in order. Already-applied migrations (version <= the
/// highest recorded) are skipped. Returns 0 on success.
///
/// # Safety
/// `db` must be a valid handle; `migrations_json` must be a NUL-terminated
/// UTF-8 C string containing a valid JSON array of Migration objects.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_kit_migrate_json(
    db: mongreldb_kit_database_t,
    migrations_json: *const c_char,
) -> i32 {
    clear();
    let Some(h) = as_kit_db(db) else {
        return KitErrorCode::InvalidArgument.as_return();
    };
    let json_str = match parse_cstr(migrations_json, "migrations_json") {
        Ok(s) => s,
        Err(code) => return code.as_return(),
    };

    let migrations: Vec<Migration> = match serde_json::from_str(json_str) {
        Ok(v) => v,
        Err(e) => {
            return set_error_msg(
                KitErrorCode::InvalidArgument,
                format!("failed to parse migrations_json: {e}"),
            )
            .as_return();
        }
    };

    // The Kit migrate function requires &mut Database. We borrow mutably.
    // SAFETY: the RefCell ensures no other borrow is active (would panic if
    // it is, which indicates a bug in the caller's threading).
    let result = h.db.try_borrow_mut();
    let mut db_guard = match result {
        Ok(g) => g,
        Err(_) => {
            return set_error_msg(
                KitErrorCode::Unknown,
                "database is in use (borrow conflict) - close any open transaction first",
            )
            .as_return();
        }
    };
    match kit::migrate(&mut db_guard, &migrations) {
        Ok(()) => 0,
        Err(e) => set_error(&e).as_return(),
    }
}

/// Read the list of migrations already applied to the database. Returns a
/// JSON array of `Migration` objects, written into `*out_json` (caller frees
/// with [`crate::mongreldb_kit_free_json`]).
///
/// # Safety
/// `db` must be a valid handle; `out_json` must be a valid non-null pointer.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_kit_applied_migrations_json(
    db: mongreldb_kit_database_t,
    out_json: *mut *const c_char,
) -> i32 {
    clear();
    let Some(h) = as_kit_db(db) else {
        return KitErrorCode::InvalidArgument.as_return();
    };
    if out_json.is_null() {
        return set_error_msg(KitErrorCode::InvalidArgument, "out_json must not be null").as_return();
    }

    let migrations = match h.db.borrow().applied_migrations() {
        Ok(m) => m,
        Err(e) => return set_error(&e).as_return(),
    };

    write_json_out(serde_json::to_string(&migrations), out_json)
}
