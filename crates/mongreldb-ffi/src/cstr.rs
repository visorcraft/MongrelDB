//! Small shared helpers for crossing the C ABI: NUL-terminated string
//! conversions and `Box::into_raw` / `Box::from_raw` handle plumbing.

use std::ffi::{CStr, CString};
use std::os::raw::c_char;

/// Copy a `*const c_char` into an owned `String`. `ptr` must be a valid,
/// NUL-terminated UTF-8 C string. `what` is used in the panic message if the
/// bytes are not valid UTF-8.
///
/// # Panics
/// Panics if the bytes are not valid UTF-8 (FFI strings are documented as
/// UTF-8). A caller passing invalid UTF-8 has violated the contract.
pub unsafe fn cstr_to_string(ptr: *const c_char, what: &str) -> String {
    // SAFETY: caller guarantees `ptr` is a valid NUL-terminated C string.
    let bytes = unsafe { CStr::from_ptr(ptr).to_bytes() };
    std::str::from_utf8(bytes)
        .map(|s| s.to_string())
        .unwrap_or_else(|_| panic!("{what} was not valid UTF-8"))
}

/// Reclaim a `CString::into_raw` pointer. Null is a no-op.
///
/// # Safety
/// `ptr` must be null or a pointer previously returned by `CString::into_raw`,
/// and must not have been reclaimed already.
pub unsafe fn drop_cstring_ptr_unchecked(ptr: *mut c_char) {
    if ptr.is_null() {
        return;
    }
    // SAFETY: upheld by caller.
    drop(CString::from_raw(ptr));
}

/// Reclaim a `CString::into_raw` pointer. Null is a no-op. Safe wrapper used
/// by [`crate::error`] so its `Drop` impl can't accidentally cause UB.
pub unsafe fn drop_cstring_ptr(ptr: *mut c_char) {
    // SAFETY: the only producers of these pointers are `CString::into_raw`
    // calls inside this crate, and each pointer is reclaimed exactly once.
    drop_cstring_ptr_unchecked(ptr)
}

/// Hand ownership of a `String` to C as a NUL-terminated `*mut c_char`. The
/// caller (C side) must eventually return the pointer to
/// [`mongreldb_free_string`].
///
/// [`mongreldb_free_string`]: crate::mongreldb_free_string
pub fn string_into_raw(s: impl Into<String>) -> *mut c_char {
    CString::new(s.into())
        .unwrap_or_else(|_| CString::new("string contained NUL").unwrap())
        .into_raw()
}
