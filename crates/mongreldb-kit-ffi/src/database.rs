//! Kit Database handle and lifecycle FFI functions.
//!
//! The handle wraps `Rc<RefCell<kit::Database>>` so transaction
//! sub-handles can pin the database alive (matching the PyO3 binding pattern).

use crate::error::{clear, parse_cstr, set_error, set_error_msg, KitErrorCode};
use kit::Database as KitDatabase;
use std::cell::RefCell;
use std::os::raw::{c_char, c_void};
use std::path::Path;
use std::rc::Rc;

/// Opaque Kit database handle.
#[allow(non_camel_case_types)]
pub type mongreldb_kit_database_t = *mut c_void;

/// The Rust-side wrapper behind [`mongreldb_kit_database_t`]. Uses `Rc<RefCell>`
/// so a transaction can borrow the database mutably while keeping it alive.
pub struct FFIKitDatabase {
    pub(crate) db: Rc<RefCell<KitDatabase>>,
}

impl FFIKitDatabase {
    pub fn into_handle(self) -> mongreldb_kit_database_t {
        Box::into_raw(Box::new(self)) as mongreldb_kit_database_t
    }
}

/// SAFETY helper: borrow a handle or record an error.
pub(crate) unsafe fn as_kit_db(
    handle: mongreldb_kit_database_t,
) -> Option<&'static FFIKitDatabase> {
    if handle.is_null() {
        set_error_msg(KitErrorCode::InvalidArgument, "database handle is null");
        return None;
    }
    // SAFETY: caller guarantees the pointer is live.
    Some(&*(handle as *const FFIKitDatabase))
}

// ── lifecycle ─────────────────────────────────────────────────────────────

/// Open an existing Kit database. Returns a handle or null on error.
///
/// # Safety
/// `path` must be a NUL-terminated UTF-8 C string.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_kit_open(path: *const c_char) -> mongreldb_kit_database_t {
    clear();
    let path_str = match parse_cstr(path, "path") {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    match KitDatabase::open(Path::new(path_str)) {
        Ok(db) => FFIKitDatabase {
            db: Rc::new(RefCell::new(db)),
        }
        .into_handle(),
        Err(e) => {
            set_error(&e);
            std::ptr::null_mut()
        }
    }
}

/// Create a fresh Kit database with the given JSON schema. Returns a handle
/// or null on error.
///
/// # Safety
/// `path` and `schema_json` must be NUL-terminated UTF-8 C strings.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_kit_create(
    path: *const c_char,
    schema_json: *const c_char,
) -> mongreldb_kit_database_t {
    clear();
    let path_str = match parse_cstr(path, "path") {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    let schema_str = match parse_cstr(schema_json, "schema_json") {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    let schema: kit::Schema = match serde_json::from_str(schema_str) {
        Ok(s) => s,
        Err(e) => {
            set_error_msg(
                KitErrorCode::Schema,
                format!("failed to parse schema_json: {e}"),
            );
            return std::ptr::null_mut();
        }
    };
    match KitDatabase::create(Path::new(path_str), schema) {
        Ok(db) => FFIKitDatabase {
            db: Rc::new(RefCell::new(db)),
        }
        .into_handle(),
        Err(e) => {
            set_error(&e);
            std::ptr::null_mut()
        }
    }
}

/// Open an encrypted Kit database with a passphrase. Returns a handle or null.
///
/// # Safety
/// `path` and `passphrase` must be NUL-terminated UTF-8 C strings.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_kit_open_encrypted(
    path: *const c_char,
    passphrase: *const c_char,
) -> mongreldb_kit_database_t {
    clear();
    let path_str = match parse_cstr(path, "path") {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    let pass = match parse_cstr(passphrase, "passphrase") {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    match KitDatabase::open_encrypted(Path::new(path_str), pass) {
        Ok(db) => FFIKitDatabase {
            db: Rc::new(RefCell::new(db)),
        }
        .into_handle(),
        Err(e) => {
            set_error(&e);
            std::ptr::null_mut()
        }
    }
}

/// Create an encrypted Kit database. Returns a handle or null.
///
/// # Safety
/// All arguments must be NUL-terminated UTF-8 C strings.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_kit_create_encrypted(
    path: *const c_char,
    schema_json: *const c_char,
    passphrase: *const c_char,
) -> mongreldb_kit_database_t {
    clear();
    let path_str = match parse_cstr(path, "path") {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    let schema_str = match parse_cstr(schema_json, "schema_json") {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    let pass = match parse_cstr(passphrase, "passphrase") {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    let schema: kit::Schema = match serde_json::from_str(schema_str) {
        Ok(s) => s,
        Err(e) => {
            set_error_msg(
                KitErrorCode::Schema,
                format!("failed to parse schema_json: {e}"),
            );
            return std::ptr::null_mut();
        }
    };
    match KitDatabase::create_encrypted(Path::new(path_str), schema, pass) {
        Ok(db) => FFIKitDatabase {
            db: Rc::new(RefCell::new(db)),
        }
        .into_handle(),
        Err(e) => {
            set_error(&e);
            std::ptr::null_mut()
        }
    }
}

/// Open a Kit database with authentication. Returns a handle or null.
///
/// # Safety
/// All arguments must be NUL-terminated UTF-8 C strings.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_kit_open_with_credentials(
    path: *const c_char,
    user: *const c_char,
    password: *const c_char,
) -> mongreldb_kit_database_t {
    clear();
    let path_str = match parse_cstr(path, "path") {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    let user = match parse_cstr(user, "user") {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    let password = match parse_cstr(password, "password") {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    match KitDatabase::open_with_credentials(Path::new(path_str), user, password) {
        Ok(db) => FFIKitDatabase {
            db: Rc::new(RefCell::new(db)),
        }
        .into_handle(),
        Err(e) => {
            set_error(&e);
            std::ptr::null_mut()
        }
    }
}

/// Create a Kit database with authentication and an admin user. Returns a
/// handle or null.
///
/// # Safety
/// All arguments must be NUL-terminated UTF-8 C strings.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_kit_create_with_credentials(
    path: *const c_char,
    schema_json: *const c_char,
    admin_user: *const c_char,
    admin_password: *const c_char,
) -> mongreldb_kit_database_t {
    clear();
    let path_str = match parse_cstr(path, "path") {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    let schema_str = match parse_cstr(schema_json, "schema_json") {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    let admin_user = match parse_cstr(admin_user, "admin_user") {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    let admin_password = match parse_cstr(admin_password, "admin_password") {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    let schema: kit::Schema = match serde_json::from_str(schema_str) {
        Ok(s) => s,
        Err(e) => {
            set_error_msg(
                KitErrorCode::Schema,
                format!("failed to parse schema_json: {e}"),
            );
            return std::ptr::null_mut();
        }
    };
    match KitDatabase::create_with_credentials(Path::new(path_str), schema, admin_user, admin_password)
    {
        Ok(db) => FFIKitDatabase {
            db: Rc::new(RefCell::new(db)),
        }
        .into_handle(),
        Err(e) => {
            set_error(&e);
            std::ptr::null_mut()
        }
    }
}

/// Rebuild the cached SQL session so it sees the current table set. Call after
/// a migration that creates or drops tables. Returns 0 on success.
///
/// # Safety
/// `db` must be a valid handle.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_kit_refresh_sql_session(
    db: mongreldb_kit_database_t,
) -> i32 {
    clear();
    let Some(h) = as_kit_db(db) else {
        return KitErrorCode::InvalidArgument.as_return();
    };
    match h.db.borrow().refresh_sql_session() {
        Ok(()) => 0,
        Err(e) => set_error(&e).as_return(),
    }
}

/// Free a Kit database handle. No-op on null.
///
/// # Safety
/// `db` must be null or a valid handle, not reused after this call.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_kit_database_free(db: mongreldb_kit_database_t) {
    if db.is_null() {
        return;
    }
    // SAFETY: upheld by caller.
    drop(Box::from_raw(db as *mut FFIKitDatabase));
}
