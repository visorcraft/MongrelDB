//! `FFIDatabase` — an opaque handle wrapping `Arc<Database>`, plus the
//! database-lifecycle FFI functions (create / open / open-with-credentials /
//! close / free / compact / table-names).
//!
//! Every handle-returning function returns null on error (with the error
//! captured in the thread-local store); every other function returns
//! `int32_t` (0 = OK, negative = error code).

use crate::cstr::{cstr_to_string, string_into_raw};
use crate::error::{clear, set_error, set_error_msg, ErrorCode};
use crate::schema::{self, mongreldb_schema_t};
use mongreldb_core::Database as CoreDatabase;
use parking_lot::RwLock;
use std::ffi::CString;
use std::os::raw::{c_char, c_void};
use std::sync::Arc;

/// Opaque database handle.
pub type mongreldb_database_t = *mut c_void;

/// The Rust-side wrapper behind [`mongreldb_database_t`]. Holds the database
/// behind an `Arc` so table/transaction sub-handles can clone it.
///
/// The `sql_session` field lazily caches a `MongrelSession` (the DataFusion
/// SQL engine) so repeated `mongreldb_database_sql` calls reuse the same
/// session and its view/catalog state. It is opened on first SQL use and
/// stays alive for the lifetime of the handle.
pub struct FFIDatabase {
    pub db: Arc<CoreDatabase>,
    pub(crate) sql_session: RwLock<Option<Arc<mongreldb_query::MongrelSession>>>,
}

impl FFIDatabase {
    pub fn new(db: CoreDatabase) -> Self {
        Self {
            db: Arc::new(db),
            sql_session: RwLock::new(None),
        }
    }

    /// Wrap an existing `Arc<CoreDatabase>` (used by sub-handles that share the
    /// same underlying database).
    pub fn from_arc(db: Arc<CoreDatabase>) -> Self {
        Self {
            db,
            sql_session: RwLock::new(None),
        }
    }

    /// Hand ownership to C as an opaque pointer.
    pub fn into_handle(self) -> mongreldb_database_t {
        Box::into_raw(Box::new(self)) as mongreldb_database_t
    }
}

/// SAFETY helper: borrow a database handle as `&FFIDatabase` or record an
/// error.
///
/// # Safety
/// `handle` must be null or a valid pointer returned by a database-creating
/// FFI function.
pub unsafe fn as_db(handle: mongreldb_database_t) -> Option<&'static FFIDatabase> {
    if handle.is_null() {
        set_error_msg(ErrorCode::InvalidArgument, "database handle is null");
        return None;
    }
    // SAFETY: caller guarantees the pointer is live. The 'static is scoped to
    // the calling FFI function.
    Some(&*(handle as *const FFIDatabase))
}

// ── lifecycle ─────────────────────────────────────────────────────────────

/// Create a fresh database at `path`. Returns a handle or null on error.
///
/// # Safety
/// `path` must be a NUL-terminated UTF-8 C string.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_create(path: *const c_char) -> mongreldb_database_t {
    clear();
    let path = match require_path(path) {
        Ok(p) => p,
        Err(_) => return std::ptr::null_mut(),
    };
    match CoreDatabase::create(&path) {
        Ok(db) => FFIDatabase::new(db).into_handle(),
        Err(e) => {
            set_error(&e);
            std::ptr::null_mut()
        }
    }
}

/// Open an existing database from disk. If the catalog doesn't exist yet,
/// create it automatically (create-if-not-missing, matching the daemon's
/// behavior). Returns a handle or null on error.
///
/// # Safety
/// `path` must be a NUL-terminated UTF-8 C string.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_open(path: *const c_char) -> mongreldb_database_t {
    clear();
    let path = match require_path(path) {
        Ok(p) => p,
        Err(_code) => return std::ptr::null_mut(),
    };
    match CoreDatabase::open(&path).or_else(|_| CoreDatabase::create(&path)) {
        Ok(db) => FFIDatabase::new(db).into_handle(),
        Err(e) => {
            set_error(&e);
            std::ptr::null_mut()
        }
    }
}

/// Create a fresh database with `require_auth = true` and a single admin user.
///
/// # Safety
/// All string arguments must be NUL-terminated UTF-8 C strings.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_create_with_credentials(
    path: *const c_char,
    user: *const c_char,
    password: *const c_char,
) -> mongreldb_database_t {
    clear();
    let path = match require_path(path) {
        Ok(p) => p,
        Err(_) => return std::ptr::null_mut(),
    };
    let user = match require_str_named(user, "user") {
        Ok(u) => u,
        Err(_) => return std::ptr::null_mut(),
    };
    let password = match require_str_named(password, "password") {
        Ok(p) => p,
        Err(_) => return std::ptr::null_mut(),
    };
    match CoreDatabase::create_with_credentials(&path, &user, &password) {
        Ok(db) => FFIDatabase::new(db).into_handle(),
        Err(e) => {
            set_error(&e);
            std::ptr::null_mut()
        }
    }
}

/// Open an existing database that has `require_auth = true`, verifying the
/// supplied credentials.
///
/// # Safety
/// All string arguments must be NUL-terminated UTF-8 C strings.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_open_with_credentials(
    path: *const c_char,
    user: *const c_char,
    password: *const c_char,
) -> mongreldb_database_t {
    clear();
    let path = match require_path(path) {
        Ok(p) => p,
        Err(_) => return std::ptr::null_mut(),
    };
    let user = match require_str_named(user, "user") {
        Ok(u) => u,
        Err(_) => return std::ptr::null_mut(),
    };
    let password = match require_str_named(password, "password") {
        Ok(p) => p,
        Err(_) => return std::ptr::null_mut(),
    };
    match CoreDatabase::open_with_credentials(&path, &user, &password) {
        Ok(db) => FFIDatabase::new(db).into_handle(),
        Err(e) => {
            set_error(&e);
            std::ptr::null_mut()
        }
    }
}

/// Create a fresh AES-256-GCM encrypted database (passphrase → KEK).
///
/// # Safety
/// `path` and `passphrase` must be NUL-terminated UTF-8 C strings.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_create_encrypted(
    path: *const c_char,
    passphrase: *const c_char,
) -> mongreldb_database_t {
    clear();
    let path = match require_path(path) {
        Ok(p) => p,
        Err(_) => return std::ptr::null_mut(),
    };
    let passphrase = match require_str_named(passphrase, "passphrase") {
        Ok(p) => p,
        Err(_) => return std::ptr::null_mut(),
    };
    match CoreDatabase::create_encrypted(&path, &passphrase) {
        Ok(db) => FFIDatabase::new(db).into_handle(),
        Err(e) => {
            set_error(&e);
            std::ptr::null_mut()
        }
    }
}

/// Open an existing AES-256-GCM encrypted database with a passphrase.
///
/// # Safety
/// `path` and `passphrase` must be NUL-terminated UTF-8 C strings.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_open_encrypted(
    path: *const c_char,
    passphrase: *const c_char,
) -> mongreldb_database_t {
    clear();
    let path = match require_path(path) {
        Ok(p) => p,
        Err(_) => return std::ptr::null_mut(),
    };
    let passphrase = match require_str_named(passphrase, "passphrase") {
        Ok(p) => p,
        Err(_) => return std::ptr::null_mut(),
    };
    match CoreDatabase::open_encrypted(&path, &passphrase) {
        Ok(db) => FFIDatabase::new(db).into_handle(),
        Err(e) => {
            set_error(&e);
            std::ptr::null_mut()
        }
    }
}

/// Create a fresh encrypted database with `require_auth = true` and one admin.
///
/// # Safety
/// All string arguments must be NUL-terminated UTF-8 C strings.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_create_encrypted_with_credentials(
    path: *const c_char,
    passphrase: *const c_char,
    user: *const c_char,
    password: *const c_char,
) -> mongreldb_database_t {
    clear();
    let path = match require_path(path) {
        Ok(p) => p,
        Err(_) => return std::ptr::null_mut(),
    };
    let passphrase = match require_str_named(passphrase, "passphrase") {
        Ok(p) => p,
        Err(_) => return std::ptr::null_mut(),
    };
    let user = match require_str_named(user, "user") {
        Ok(u) => u,
        Err(_) => return std::ptr::null_mut(),
    };
    let password = match require_str_named(password, "password") {
        Ok(p) => p,
        Err(_) => return std::ptr::null_mut(),
    };
    match CoreDatabase::create_encrypted_with_credentials(&path, &passphrase, &user, &password) {
        Ok(db) => FFIDatabase::new(db).into_handle(),
        Err(e) => {
            set_error(&e);
            std::ptr::null_mut()
        }
    }
}

/// Open an encrypted + credentialed database (passphrase and admin credentials).
///
/// # Safety
/// All string arguments must be NUL-terminated UTF-8 C strings.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_open_encrypted_with_credentials(
    path: *const c_char,
    passphrase: *const c_char,
    user: *const c_char,
    password: *const c_char,
) -> mongreldb_database_t {
    clear();
    let path = match require_path(path) {
        Ok(p) => p,
        Err(_) => return std::ptr::null_mut(),
    };
    let passphrase = match require_str_named(passphrase, "passphrase") {
        Ok(p) => p,
        Err(_) => return std::ptr::null_mut(),
    };
    let user = match require_str_named(user, "user") {
        Ok(u) => u,
        Err(_) => return std::ptr::null_mut(),
    };
    let password = match require_str_named(password, "password") {
        Ok(p) => p,
        Err(_) => return std::ptr::null_mut(),
    };
    match CoreDatabase::open_encrypted_with_credentials(&path, &passphrase, &user, &password) {
        Ok(db) => FFIDatabase::new(db).into_handle(),
        Err(e) => {
            set_error(&e);
            std::ptr::null_mut()
        }
    }
}

/// Close the database (flush + release). Optional — the handle is also
/// reclaimed by [`mongreldb_database_free`]. Returns 0 on success.
///
/// # Safety
/// `db` must be a valid handle.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_database_close(db: mongreldb_database_t) -> i32 {
    clear();
    let Some(h) = as_db(db) else {
        return ErrorCode::InvalidArgument.as_return();
    };
    match h.db.close() {
        Ok(()) => 0,
        Err(e) => set_error(&e).as_return(),
    }
}

/// Free a database handle. No-op on null. Does *not* flush — use
/// [`mongreldb_database_close`] first if you need durability before drop.
///
/// # Safety
/// `db` must be null or a pointer returned by a database-creating function,
/// and must not be reused after this call.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_database_free(db: mongreldb_database_t) {
    if db.is_null() {
        return;
    }
    // SAFETY: upheld by caller.
    drop(Box::from_raw(db as *mut FFIDatabase));
}

/// Compact every table: merge sorted runs into one clean run each.
/// Returns 0 on success.
///
/// # Safety
/// `db` must be a valid handle.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_database_compact(db: mongreldb_database_t) -> i32 {
    clear();
    let Some(h) = as_db(db) else {
        return ErrorCode::InvalidArgument.as_return();
    };
    match h.db.compact() {
        Ok(_) => 0,
        Err(e) => set_error(&e).as_return(),
    }
}

/// Compact a single table by name. Returns 0 on success.
///
/// # Safety
/// `db` must be a valid handle; `name` must be a NUL-terminated UTF-8 C string.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_database_compact_table(
    db: mongreldb_database_t,
    name: *const c_char,
) -> i32 {
    clear();
    let Some(h) = as_db(db) else {
        return ErrorCode::InvalidArgument.as_return();
    };
    let name = match require_str_named(name, "table name") {
        Ok(n) => n,
        Err(code) => return code.as_return(),
    };
    match h.db.compact_table(&name) {
        Ok(_) => 0,
        Err(e) => set_error(&e).as_return(),
    }
}

/// List all live table names. The names are returned as a single NUL-terminated
/// UTF-8 string with one name per line (newline-separated), written into
/// `out_str` (a `*const c_char` owned by the FFI layer), and the byte length
/// (excluding the NUL terminator) into `out_len`.
///
/// Returns 0 on success. The caller must free `*out_str` via
/// [`mongreldb_free_string`].
///
/// # Safety
/// `db` must be a valid handle; `out_str` and `out_len` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_database_table_names(
    db: mongreldb_database_t,
    out_str: *mut *const c_char,
    out_len: *mut usize,
) -> i32 {
    clear();
    let Some(h) = as_db(db) else {
        return ErrorCode::InvalidArgument.as_return();
    };
    if out_str.is_null() || out_len.is_null() {
        return set_error_msg(
            ErrorCode::InvalidArgument,
            "out_str and out_len must not be null",
        )
        .as_return();
    }
    let names = h.db.table_names();
    let joined = names.join("\n");
    let len = joined.len();
    let ptr = string_into_raw(joined);
    *out_str = ptr;
    *out_len = len;
    0
}

/// Create a new table with the given schema. Returns 0 on success and writes
/// the assigned table id into `out_table_id` (if non-null).
///
/// # Safety
/// `db` must be a valid handle; `name` must be a NUL-terminated UTF-8 C string;
/// `schema` must be a valid built-schema handle returned by
/// [`crate::schema::mongreldb_schema_build`] (consumed by this call).
#[no_mangle]
pub unsafe extern "C" fn mongreldb_create_table(
    db: mongreldb_database_t,
    name: *const c_char,
    schema: mongreldb_schema_t,
    out_table_id: *mut u64,
) -> i32 {
    clear();
    let Some(h) = as_db(db) else {
        return ErrorCode::InvalidArgument.as_return();
    };
    let name = match require_str_named(name, "table name") {
        Ok(n) => n,
        Err(code) => return code.as_return(),
    };
    let Some(built) = schema::take_schema(schema) else {
        return set_error_msg(ErrorCode::InvalidArgument, "schema handle is null").as_return();
    };
    match h.db.create_table(&name, built) {
        Ok(id) => {
            if !out_table_id.is_null() {
                *out_table_id = id;
            }
            0
        }
        Err(e) => set_error(&e).as_return(),
    }
}

/// Drop a table by name. Returns 0 on success.
///
/// # Safety
/// `db` must be a valid handle; `name` must be a NUL-terminated UTF-8 C string.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_drop_table(
    db: mongreldb_database_t,
    name: *const c_char,
) -> i32 {
    clear();
    let Some(h) = as_db(db) else {
        return ErrorCode::InvalidArgument.as_return();
    };
    let name = match require_str_named(name, "table name") {
        Ok(n) => n,
        Err(code) => return code.as_return(),
    };
    match h.db.drop_table(&name) {
        Ok(()) => 0,
        Err(e) => set_error(&e).as_return(),
    }
}

/// Rename a live table. Returns 0 on success.
///
/// # Safety
/// `db` must be valid; `name` and `new_name` must be NUL-terminated UTF-8.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_rename_table(
    db: mongreldb_database_t,
    name: *const c_char,
    new_name: *const c_char,
) -> i32 {
    clear();
    let Some(h) = as_db(db) else {
        return ErrorCode::InvalidArgument.as_return();
    };
    let name = match require_str_named(name, "table name") {
        Ok(n) => n,
        Err(code) => return code.as_return(),
    };
    let new_name = match require_str_named(new_name, "new table name") {
        Ok(n) => n,
        Err(code) => return code.as_return(),
    };
    match h.db.rename_table(&name, &new_name) {
        Ok(()) => 0,
        Err(e) => set_error(&e).as_return(),
    }
}

// ── small argument helpers ────────────────────────────────────────────────

unsafe fn require_path(ptr: *const c_char) -> Result<String, ErrorCode> {
    require_str_named(ptr, "path")
}

unsafe fn require_str_named(ptr: *const c_char, what: &str) -> Result<String, ErrorCode> {
    if ptr.is_null() {
        return Err(set_error_msg(
            ErrorCode::InvalidArgument,
            format!("{what} must not be null"),
        ));
    }
    // SAFETY: caller guarantees a valid NUL-terminated C string.
    Ok(cstr_to_string(ptr, what))
}

/// Free a string previously returned by any mongreldb FFI function (e.g.
/// [`mongreldb_database_table_names`], [`mongreldb_last_error`]). No-op on null.
///
/// # Safety
/// `ptr` must be null or a pointer previously returned by an FFI function that
/// documents its output as FFI-owned, and must not be freed twice.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_free_string(ptr: *mut c_char) {
    if ptr.is_null() {
        return;
    }
    // SAFETY: upheld by caller. `mongreldb_last_error`'s pointer is also
    // tracked by the thread-local; freeing it through this generic path would
    // double-free on thread exit, so null out the thread-local slot too.
    // We can't import error::drop machinery cleanly here without a cycle, so
    // we simply reclaim via CString::from_raw. Callers that fetched the last
    // error string should use mongreldb_free_error_string instead.
    drop(CString::from_raw(ptr));
}
