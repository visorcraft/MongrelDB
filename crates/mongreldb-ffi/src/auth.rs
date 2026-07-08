//! FFI wrappers for user/role/credential management. Each function delegates
//! to the matching `Database` method and returns `int32_t` (0 = OK, negative =
//! error code). Permission strings follow the NAPI `parse_permission` grammar:
//! `"all"`, `"ddl"`, `"admin"`, or `"<verb>:<table>"` where verb is one of
//! `select`, `insert`, `update`, `delete`.

use crate::cstr::cstr_to_string;
use crate::database::{as_db, mongreldb_database_t};
use crate::error::{clear, set_error, set_error_msg, ErrorCode};
use mongreldb_core::auth::Permission;
use std::os::raw::c_char;

/// Parse a permission string into a core [`Permission`]. Mirrors the NAPI
/// `parse_permission` grammar (case-insensitive verb).
fn parse_permission(s: &str) -> Result<Permission, ErrorCode> {
    let lower = s.to_ascii_lowercase();
    Ok(match lower.as_str() {
        "all" => Permission::All,
        "ddl" => Permission::Ddl,
        "admin" => Permission::Admin,
        _ if lower.starts_with("select:") => Permission::Select {
            table: lower[7..].to_string(),
        },
        _ if lower.starts_with("insert:") => Permission::Insert {
            table: lower[7..].to_string(),
        },
        _ if lower.starts_with("update:") => Permission::Update {
            table: lower[7..].to_string(),
        },
        _ if lower.starts_with("delete:") => Permission::Delete {
            table: lower[7..].to_string(),
        },
        other => {
            return Err(set_error_msg(
                ErrorCode::InvalidArgument,
                format!(
                    "unknown permission '{other}'. Use: all, ddl, admin, select:table, insert:table, update:table, delete:table"
                ),
            ))
        }
    })
}

/// SAFETY boilerplate: borrow the db and parse a single string arg, returning
/// the owned values or recording an error.
unsafe fn require_db_and_str(
    db: mongreldb_database_t,
    name_ptr: *const c_char,
    what: &str,
) -> Result<(&'static crate::database::FFIDatabase, String), ErrorCode> {
    let h = as_db(db).ok_or(ErrorCode::InvalidArgument)?;
    if name_ptr.is_null() {
        return Err(set_error_msg(
            ErrorCode::InvalidArgument,
            format!("{what} must not be null"),
        ));
    }
    Ok((h, cstr_to_string(name_ptr, what)))
}

/// Create a catalog user with an Argon2id-hashed password.
///
/// # Safety
/// `db` must be a valid handle; `username` and `password` must be NUL-terminated
/// UTF-8 C strings.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_create_user(
    db: mongreldb_database_t,
    username: *const c_char,
    password: *const c_char,
) -> i32 {
    clear();
    let (h, username) = match require_db_and_str(db, username, "username") {
        Ok(v) => v,
        Err(code) => return code.as_return(),
    };
    let password = match require_str(password, "password") {
        Ok(p) => p,
        Err(code) => return code.as_return(),
    };
    match h.db.create_user(&username, &password) {
        Ok(_) => 0,
        Err(e) => set_error(&e).as_return(),
    }
}

/// Drop a user by username.
///
/// # Safety
/// `db` must be valid; `username` must be a NUL-terminated UTF-8 C string.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_drop_user(
    db: mongreldb_database_t,
    username: *const c_char,
) -> i32 {
    clear();
    let (h, username) = match require_db_and_str(db, username, "username") {
        Ok(v) => v,
        Err(code) => return code.as_return(),
    };
    match h.db.drop_user(&username) {
        Ok(()) => 0,
        Err(e) => set_error(&e).as_return(),
    }
}

/// Change a user's password.
///
/// # Safety
/// `db` must be valid; both strings must be NUL-terminated UTF-8.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_alter_user_password(
    db: mongreldb_database_t,
    username: *const c_char,
    new_password: *const c_char,
) -> i32 {
    clear();
    let (h, username) = match require_db_and_str(db, username, "username") {
        Ok(v) => v,
        Err(code) => return code.as_return(),
    };
    let new_password = match require_str(new_password, "new_password") {
        Ok(p) => p,
        Err(code) => return code.as_return(),
    };
    match h.db.alter_user_password(&username, &new_password) {
        Ok(()) => 0,
        Err(e) => set_error(&e).as_return(),
    }
}

/// Verify credentials. Writes `1` into `out_ok` on success, `0` on a bad
/// password / missing user. Returns 0 unless an internal error occurred.
///
/// # Safety
/// `db` must be valid; both strings must be NUL-terminated UTF-8; `out_ok` if
/// non-null must be a writable `u8` slot.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_verify_user(
    db: mongreldb_database_t,
    username: *const c_char,
    password: *const c_char,
    out_ok: *mut u8,
) -> i32 {
    clear();
    let (h, username) = match require_db_and_str(db, username, "username") {
        Ok(v) => v,
        Err(code) => return code.as_return(),
    };
    let password = match require_str(password, "password") {
        Ok(p) => p,
        Err(code) => return code.as_return(),
    };
    match h.db.verify_user(&username, &password) {
        Ok(Some(_)) => {
            if !out_ok.is_null() {
                *out_ok = 1;
            }
            0
        }
        Ok(None) => {
            if !out_ok.is_null() {
                *out_ok = 0;
            }
            0
        }
        Err(e) => set_error(&e).as_return(),
    }
}

/// Grant or revoke admin privileges on a user.
///
/// # Safety
/// `db` must be valid; `username` must be a NUL-terminated UTF-8 C string.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_set_user_admin(
    db: mongreldb_database_t,
    username: *const c_char,
    is_admin: u8,
) -> i32 {
    clear();
    let (h, username) = match require_db_and_str(db, username, "username") {
        Ok(v) => v,
        Err(code) => return code.as_return(),
    };
    match h.db.set_user_admin(&username, is_admin != 0) {
        Ok(()) => 0,
        Err(e) => set_error(&e).as_return(),
    }
}

/// Create a role.
///
/// # Safety
/// `db` must be valid; `name` must be a NUL-terminated UTF-8 C string.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_create_role(
    db: mongreldb_database_t,
    name: *const c_char,
) -> i32 {
    clear();
    let (h, name) = match require_db_and_str(db, name, "role name") {
        Ok(v) => v,
        Err(code) => return code.as_return(),
    };
    match h.db.create_role(&name) {
        Ok(_) => 0,
        Err(e) => set_error(&e).as_return(),
    }
}

/// Drop a role.
///
/// # Safety
/// `db` must be valid; `name` must be a NUL-terminated UTF-8 C string.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_drop_role(db: mongreldb_database_t, name: *const c_char) -> i32 {
    clear();
    let (h, name) = match require_db_and_str(db, name, "role name") {
        Ok(v) => v,
        Err(code) => return code.as_return(),
    };
    match h.db.drop_role(&name) {
        Ok(()) => 0,
        Err(e) => set_error(&e).as_return(),
    }
}

/// Grant a role to a user.
///
/// # Safety
/// `db` must be valid; both strings must be NUL-terminated UTF-8.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_grant_role(
    db: mongreldb_database_t,
    username: *const c_char,
    role_name: *const c_char,
) -> i32 {
    clear();
    let (h, username) = match require_db_and_str(db, username, "username") {
        Ok(v) => v,
        Err(code) => return code.as_return(),
    };
    let role_name = match require_str(role_name, "role_name") {
        Ok(r) => r,
        Err(code) => return code.as_return(),
    };
    match h.db.grant_role(&username, &role_name) {
        Ok(()) => 0,
        Err(e) => set_error(&e).as_return(),
    }
}

/// Revoke a role from a user.
///
/// # Safety
/// `db` must be valid; both strings must be NUL-terminated UTF-8.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_revoke_role(
    db: mongreldb_database_t,
    username: *const c_char,
    role_name: *const c_char,
) -> i32 {
    clear();
    let (h, username) = match require_db_and_str(db, username, "username") {
        Ok(v) => v,
        Err(code) => return code.as_return(),
    };
    let role_name = match require_str(role_name, "role_name") {
        Ok(r) => r,
        Err(code) => return code.as_return(),
    };
    match h.db.revoke_role(&username, &role_name) {
        Ok(()) => 0,
        Err(e) => set_error(&e).as_return(),
    }
}

/// Grant a permission to a role. `permission` is one of: `"all"`, `"ddl"`,
/// `"admin"`, or `"<verb>:<table>"`.
///
/// # Safety
/// `db` must be valid; both strings must be NUL-terminated UTF-8.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_grant_permission(
    db: mongreldb_database_t,
    role_name: *const c_char,
    permission: *const c_char,
) -> i32 {
    clear();
    let (h, role_name) = match require_db_and_str(db, role_name, "role_name") {
        Ok(v) => v,
        Err(code) => return code.as_return(),
    };
    let perm_str = match require_str(permission, "permission") {
        Ok(p) => p,
        Err(code) => return code.as_return(),
    };
    let perm = match parse_permission(&perm_str) {
        Ok(p) => p,
        Err(code) => return code.as_return(),
    };
    match h.db.grant_permission(&role_name, perm) {
        Ok(()) => 0,
        Err(e) => set_error(&e).as_return(),
    }
}

/// Revoke a permission from a role.
///
/// # Safety
/// `db` must be valid; both strings must be NUL-terminated UTF-8.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_revoke_permission(
    db: mongreldb_database_t,
    role_name: *const c_char,
    permission: *const c_char,
) -> i32 {
    clear();
    let (h, role_name) = match require_db_and_str(db, role_name, "role_name") {
        Ok(v) => v,
        Err(code) => return code.as_return(),
    };
    let perm_str = match require_str(permission, "permission") {
        Ok(p) => p,
        Err(code) => return code.as_return(),
    };
    let perm = match parse_permission(&perm_str) {
        Ok(p) => p,
        Err(code) => return code.as_return(),
    };
    match h.db.revoke_permission(&role_name, perm) {
        Ok(()) => 0,
        Err(e) => set_error(&e).as_return(),
    }
}

/// Enable `require_auth` on a credentialless database: creates the first admin
/// user and caches the admin principal on the handle.
///
/// # Safety
/// `db` must be valid; both strings must be NUL-terminated UTF-8.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_enable_auth(
    db: mongreldb_database_t,
    admin_username: *const c_char,
    admin_password: *const c_char,
) -> i32 {
    clear();
    let (h, admin_username) = match require_db_and_str(db, admin_username, "admin_username") {
        Ok(v) => v,
        Err(code) => return code.as_return(),
    };
    let admin_password = match require_str(admin_password, "admin_password") {
        Ok(p) => p,
        Err(code) => return code.as_return(),
    };
    match h.db.enable_auth(&admin_username, &admin_password) {
        Ok(()) => 0,
        Err(e) => set_error(&e).as_return(),
    }
}

/// Disable `require_auth`, reverting to credentialless mode (recovery).
///
/// # Safety
/// `db` must be a valid handle.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_disable_auth(db: mongreldb_database_t) -> i32 {
    clear();
    let Some(h) = as_db(db) else {
        return ErrorCode::InvalidArgument.as_return();
    };
    match h.db.disable_auth() {
        Ok(()) => 0,
        Err(e) => set_error(&e).as_return(),
    }
}

/// Helper: require a non-null C string.
unsafe fn require_str(ptr: *const c_char, what: &str) -> Result<String, ErrorCode> {
    if ptr.is_null() {
        return Err(set_error_msg(
            ErrorCode::InvalidArgument,
            format!("{what} must not be null"),
        ));
    }
    Ok(cstr_to_string(ptr, what))
}
