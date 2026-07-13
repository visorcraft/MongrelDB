//! Error codes, thread-local last-error capture, and the C-facing error
//! accessor functions.
//!
//! Every FFI function returns `int32_t`: `0` for OK, a negative [`ErrorCode`]
//! on failure. The most recent error for the calling thread is captured in a
//! thread-local [`LastError`] so a caller can fetch a human-readable message
//! via [`mongreldb_last_error`] / [`mongreldb_last_error_code`].

use crate::cstr::{cstr_to_string, drop_cstring_ptr};
use mongreldb_core::MongrelError;
use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;

/// Stable error codes returned by every FFI function (negated).
///
/// Values are deliberately small negative integers so a C caller can switch on
/// them. They must never be renumbered — the integer ABI is part of the
/// public contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ErrorCode {
    /// Success (returned literally as `0` by FFI functions; never stored).
    Ok = 0,
    /// `MongrelError::InvalidArgument` — bad input from the caller.
    InvalidArgument = -1,
    /// `MongrelError::NotFound` — table / row / user missing.
    NotFound = -2,
    /// `MongrelError::Conflict` — write/write conflict (retryable).
    Conflict = -3,
    /// `MongrelError::Schema` — schema validation failure.
    Schema = -4,
    /// `MongrelError::ColumnNotFound`.
    ColumnNotFound = -5,
    /// Auth/permission failures (`AuthRequired`, `AuthNotRequired`,
    /// `InvalidCredentials`, `PermissionDenied`).
    Unauthorized = -6,
    /// `MongrelError::Full` — table is full.
    Full = -7,
    /// I/O, serialization, corruption, encryption, or torn-write failures.
    Io = -8,
    /// Anything else (catch-all).
    Unknown = -99,
}

impl ErrorCode {
    /// The FFI return value for this code (already negative for errors).
    pub fn as_return(self) -> i32 {
        self as i32
    }
}

/// Categorize a core error into a stable [`ErrorCode`]. The error message is
/// also captured in the thread-local store so a caller can retrieve it.
pub fn categorize(e: &MongrelError) -> ErrorCode {
    use MongrelError::*;
    match e {
        InvalidArgument(_) => ErrorCode::InvalidArgument,
        NotFound(_) => ErrorCode::NotFound,
        Conflict(_) => ErrorCode::Conflict,
        Schema(_) => ErrorCode::Schema,
        ColumnNotFound(_) => ErrorCode::ColumnNotFound,
        AuthRequired | AuthNotRequired | InvalidCredentials { .. } | PermissionDenied { .. } => {
            ErrorCode::Unauthorized
        }
        Full(_) => ErrorCode::Full,
        // I/O, serialization, corruption, encryption, torn writes, checksum,
        // magic mismatch are all storage/infrastructure failures.
        Io(_)
        | Serialization(_)
        | CorruptWal { .. }
        | TornWrite { .. }
        | ChecksumMismatch { .. }
        | MagicMismatch { .. }
        | EncryptionDisabled
        | Encryption(_)
        | Decryption(_) => ErrorCode::Io,
        Other(_) => ErrorCode::Unknown,
        // `MongrelError` is `#[non_exhaustive]`; future variants map to
        // `Unknown` so the FFI stays total across core upgrades.
        _ => ErrorCode::Unknown,
    }
}

/// One captured error for the calling thread: the stable code plus an owned
/// NUL-terminated UTF-8 message.
#[derive(Default)]
pub struct LastError {
    pub code: i32,
    /// `CString::into_raw` pointer owned by this struct (null when no error).
    /// Valid until the next `set_error`/clear on this thread or thread exit.
    pub message: *mut c_char,
}

impl Drop for LastError {
    fn drop(&mut self) {
        // SAFETY: `message` was produced by `CString::into_raw` and is owned
        // exclusively by this struct.
        unsafe { drop_cstring_ptr(self.message) };
        self.message = std::ptr::null_mut();
    }
}

thread_local! {
    static LAST_ERROR: RefCell<LastError> = RefCell::new(LastError::default());
}

/// Record a core error as the thread's last error and return its stable code.
/// The message is formatted via `Display` and copied into an owned `CString`.
pub fn set_error(e: &MongrelError) -> ErrorCode {
    let code = categorize(e);
    let msg = format!("{e}");
    let cstring =
        CString::new(msg).unwrap_or_else(|_| CString::new("error message contained NUL").unwrap());
    LAST_ERROR.with(|cell| {
        let mut last = cell.borrow_mut();
        unsafe { drop_cstring_ptr(last.message) };
        last.code = code.as_return();
        last.message = cstring.into_raw();
    });
    code
}

/// Convenience: record an ad-hoc error (used for FFI-only failures like null
/// pointer arguments that never reach the core).
pub fn set_error_msg(code: ErrorCode, msg: impl Into<String>) -> ErrorCode {
    let cstring = CString::new(msg.into())
        .unwrap_or_else(|_| CString::new("error message contained NUL").unwrap());
    LAST_ERROR.with(|cell| {
        let mut last = cell.borrow_mut();
        unsafe { drop_cstring_ptr(last.message) };
        last.code = code.as_return();
        last.message = cstring.into_raw();
    });
    code
}

/// Clear the thread's last error (called at the start of each FFI function so
/// stale errors don't leak across calls).
pub fn clear() {
    LAST_ERROR.with(|cell| {
        let mut last = cell.borrow_mut();
        unsafe { drop_cstring_ptr(last.message) };
        last.code = ErrorCode::Ok.as_return();
        last.message = std::ptr::null_mut();
    });
}

/// Return the most recent error message as a borrowed `*const c_char`, or null
/// if no error is set. The pointer is owned by the FFI layer and remains valid
/// until the next FFI call on this thread.
///
/// SAFETY: the caller must not free the returned pointer and must not access it
/// after another mongreldb FFI call on the same thread.
#[no_mangle]
pub extern "C" fn mongreldb_last_error() -> *const c_char {
    LAST_ERROR.with(|cell| {
        let last = cell.borrow();
        if last.message.is_null() {
            std::ptr::null()
        } else {
            last.message as *const c_char
        }
    })
}

/// Return the most recent error code (0 if no error is set).
#[no_mangle]
pub extern "C" fn mongreldb_last_error_code() -> i32 {
    LAST_ERROR.with(|cell| cell.borrow().code)
}

/// Free a string previously returned by [`mongreldb_last_error`]. Passing null
/// is a no-op. This is only necessary when the caller wants to reclaim memory
/// before the next FFI call overwrites it.
///
/// SAFETY: `ptr` must be null or a pointer previously returned by
/// [`mongreldb_last_error`] on the same thread, and must not have been passed
/// to this function before.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_free_error_string(ptr: *mut c_char) {
    // Note: the thread-local still owns the pointer; if the caller frees the
    // active one, clear the slot so Drop doesn't double-free.
    if ptr.is_null() {
        return;
    }
    LAST_ERROR.with(|cell| {
        let mut last = cell.borrow_mut();
        if last.message == ptr {
            // Reclaim via CString::from_raw then drop, and null the slot.
            // SAFETY: produced by CString::into_raw in set_error.
            let _ = unsafe { CString::from_raw(ptr) };
            last.message = std::ptr::null_mut();
        }
    });
}

/// Read a `*const c_char` into a Rust `String`, categorizing a null pointer as
/// an [`ErrorCode::InvalidArgument`] error. Used pervasively by the FFI layer.
pub unsafe fn require_string(ptr: *const c_char, what: &str) -> Result<String, ErrorCode> {
    if ptr.is_null() {
        return Err(set_error_msg(
            ErrorCode::InvalidArgument,
            format!("{what} must not be null"),
        ));
    }
    // SAFETY: caller guarantees NUL-terminated UTF-8.
    Ok(cstr_to_string(ptr, what))
}

/// Re-export so other modules can borrow the `CStr` slice form too.
pub unsafe fn cstr_bytes<'a>(ptr: *const c_char) -> &'a [u8] {
    // SAFETY: caller guarantees a valid NUL-terminated C string that outlives
    // the borrow.
    unsafe { CStr::from_ptr(ptr).to_bytes() }
}
