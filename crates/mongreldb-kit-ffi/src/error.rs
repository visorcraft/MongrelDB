//! Error handling: thread-local last-error store, matching the core FFI's
//! pattern. Every FFI function returns `int32_t` (0 = OK, negative = error);
//! `mongreldb_kit_last_error()` retrieves the human-readable message.

use std::cell::RefCell;
use std::ffi::{c_char, CStr, CString};
use std::os::raw::c_int;

/// Stable error codes (matching the core FFI's MDB_ERR_* values).
#[derive(Debug, Clone, Copy)]
#[repr(i32)]
pub enum KitErrorCode {
    Ok = 0,
    InvalidArgument = -6,
    Schema = -5,
    Io = -7,
    Unknown = -9,
}

impl KitErrorCode {
    pub fn as_return(self) -> i32 {
        self as i32
    }
}

struct LastError {
    code: i32,
    message: Option<CString>,
}

impl LastError {
    fn new() -> Self {
        Self {
            code: 0,
            message: None,
        }
    }
}

thread_local! {
    static LAST_ERROR: RefCell<LastError> = RefCell::new(LastError::new());
}

/// Clear the thread-local error state. Called at the start of every FFI fn.
pub fn clear() {
    LAST_ERROR.with(|e| {
        let mut e = e.borrow_mut();
        e.code = 0;
        e.message = None;
    });
}

/// Set the error from a `KitError`, formatting the message via Display.
pub fn set_error(e: &kit::KitError) -> KitErrorCode {
    let msg = format!("{e}");
    let code = match e {
        kit::KitError::Validation(_) => KitErrorCode::Schema,
        kit::KitError::Storage(_) | kit::KitError::Integrity(_) => {
            KitErrorCode::Io
        }
        _ => KitErrorCode::Unknown,
    };
    set_error_msg(code, &msg);
    code
}

/// Set a specific error code + message (for FFI-only errors like null args).
pub fn set_error_msg(code: KitErrorCode, msg: impl AsRef<str>) -> KitErrorCode {
    let msg = msg.as_ref();
    LAST_ERROR.with(|e| {
        let mut e = e.borrow_mut();
        e.code = code.as_return();
        e.message = CString::new(msg).ok();
    });
    code
}

/// Retrieve the last error message as a borrowed C string (valid until the
/// next FFI call on this thread). Returns null if no error is set.
///
/// # Safety
/// The returned pointer is valid until the next FFI call on this thread.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_kit_last_error() -> *const c_char {
    LAST_ERROR.with(|e| {
        e.borrow()
            .message
            .as_ref()
            .map(|s| s.as_ptr())
            .unwrap_or(std::ptr::null())
    })
}

/// Retrieve the last error code. Returns 0 if no error.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_kit_last_error_code() -> c_int {
    LAST_ERROR.with(|e| e.borrow().code)
}

/// Free an error string returned by [`mongreldb_kit_last_error`]. No-op on null.
///
/// # Safety
/// `ptr` must be null or a pointer returned by `mongreldb_kit_last_error`.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_kit_free_error_string(ptr: *mut c_char) {
    // The last-error pointer is borrowed (thread-local); freeing it would
    // double-free at thread exit. This function is a no-op to match the API
    // surface, but callers should not need it.
    let _ = ptr;
}

/// Free a JSON string returned by any Kit FFI function (query results,
/// applied migrations, etc.). No-op on null.
///
/// # Safety
/// `ptr` must be null or a pointer returned by a Kit FFI JSON-output function,
/// and must not be freed twice.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_kit_free_json(ptr: *mut c_char) {
    if ptr.is_null() {
        return;
    }
    // SAFETY: caller guarantees the pointer was produced by CString::into_raw.
    drop(CString::from_raw(ptr));
}

/// Helper: write a Rust string into an out-parameter as a C string (owned by
/// caller, freed via [`mongreldb_kit_free_json`]). Returns the error code.
pub(crate) unsafe fn write_json_out(
    json: Result<String, serde_json::Error>,
    out: *mut *const c_char,
) -> i32 {
    match json {
        Ok(s) => {
            let cstr = match CString::new(s) {
                Ok(c) => c,
                Err(_) => {
                    return set_error_msg(
                        KitErrorCode::Unknown,
                        "JSON result contained interior NUL byte",
                    )
                    .as_return();
                }
            };
            *out = cstr.into_raw() as *const c_char;
            0
        }
        Err(e) => set_error_msg(KitErrorCode::Unknown, format!("JSON serialization failed: {e}"))
            .as_return(),
    }
}

/// Helper: parse a C string argument, setting an error on failure.
pub(crate) unsafe fn parse_cstr<'a>(
    ptr: *const c_char,
    name: &str,
) -> Result<&'a str, KitErrorCode> {
    if ptr.is_null() {
        return Err(set_error_msg(
            KitErrorCode::InvalidArgument,
            format!("{name} must not be null"),
        ));
    }
    // SAFETY: caller guarantees valid NUL-terminated C string.
    match CStr::from_ptr(ptr).to_str() {
        Ok(s) => Ok(s),
        Err(_) => Err(set_error_msg(
            KitErrorCode::InvalidArgument,
            format!("{name} is not valid UTF-8"),
        )),
    }
}
