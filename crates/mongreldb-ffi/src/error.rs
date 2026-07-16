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

/// Structured, additive error metadata for callers that must reason about
/// durable outcomes without parsing the human-readable message.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct mongreldb_error_details_v1 {
    pub struct_size: usize,
    pub version: u32,
    pub code: i32,
    pub outcome_known: u8,
    pub committed: u8,
    pub retryable: u8,
    pub has_last_commit_epoch: u8,
    pub last_commit_epoch: u64,
    pub committed_statements: usize,
    pub has_first_commit_statement_index: u8,
    pub first_commit_statement_index: usize,
    pub has_last_commit_statement_index: u8,
    pub last_commit_statement_index: usize,
    pub completed_statements: usize,
    pub has_statement_index: u8,
    pub statement_index: usize,
    pub cancel_outcome: i32,
    pub cancellation_reason: i32,
    pub query_id: [c_char; 33],
    pub server_state: [c_char; 32],
}

impl Default for mongreldb_error_details_v1 {
    fn default() -> Self {
        Self {
            struct_size: std::mem::size_of::<Self>(),
            version: 1,
            code: 0,
            outcome_known: 1,
            committed: 0,
            retryable: 0,
            has_last_commit_epoch: 0,
            last_commit_epoch: 0,
            committed_statements: 0,
            has_first_commit_statement_index: 0,
            first_commit_statement_index: 0,
            has_last_commit_statement_index: 0,
            last_commit_statement_index: 0,
            completed_statements: 0,
            has_statement_index: 0,
            statement_index: 0,
            cancel_outcome: 0,
            cancellation_reason: 0,
            query_id: [0; 33],
            server_state: [0; 32],
        }
    }
}

pub(crate) fn copy_c_text<const N: usize>(target: &mut [c_char; N], value: &str) {
    target.fill(0);
    for (slot, byte) in target
        .iter_mut()
        .take(N.saturating_sub(1))
        .zip(value.bytes())
    {
        *slot = byte as c_char;
    }
}

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
    /// SQL execution was cancelled before any durable commit.
    QueryCancelled = -9,
    /// SQL execution exceeded its configured deadline.
    DeadlineExceeded = -10,
    /// The requested SQL query ID is active or retained.
    QueryIdConflict = -11,
    /// The bounded SQL query registry cannot accept another query.
    QueryRegistryFull = -12,
    /// SQL transaction state rejects the requested operation.
    TransactionState = -13,
    /// The SQL query state machine rejected the operation.
    InvalidQueryState = -14,
    /// A durable commit occurred and a later operation failed.
    CommitOutcome = -15,
    /// The durable SQL outcome cannot be determined safely.
    OutcomeUnknown = -16,
    /// SQL output exceeded a configured row or byte limit.
    ResultLimit = -17,
    /// SQL output serialization failed.
    Serialization = -18,
    /// SQL planning or execution failed.
    SqlExecution = -19,
    /// SQL execution was cancelled after at least one durable commit.
    QueryCancelledAfterCommit = -20,
    /// SQL execution exceeded its deadline after at least one durable commit.
    DeadlineAfterCommit = -21,
    /// SQL output serialization failed after at least one durable commit.
    SerializationAfterCommit = -22,
    /// The current SQL transaction was aborted and must be rolled back.
    TransactionAborted = -23,
    /// The requested protocol capability is unsupported.
    ///
    /// Reserved for clients built on this ABI. The embedded SQL API currently
    /// performs no remote capability negotiation.
    CapabilityUnsupported = -24,
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
        TriggerValidation(_) => ErrorCode::InvalidArgument,
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
        | Decryption(_)
        | EntropyUnavailable(_) => ErrorCode::Io,
        DeadlineExceeded => ErrorCode::DeadlineExceeded,
        Cancelled => ErrorCode::QueryCancelled,
        DurableCommit { .. } => ErrorCode::CommitOutcome,
        CommitOutcomeUnknown { .. } => ErrorCode::OutcomeUnknown,
        ResourceLimitExceeded { .. } | WorkBudgetExceeded => ErrorCode::Full,
        CursorStale(_) => ErrorCode::Conflict,
        CursorExpired => ErrorCode::NotFound,
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
    pub details: mongreldb_error_details_v1,
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
    let mut details = default_details(code);
    match e {
        MongrelError::DurableCommit { epoch, .. } => {
            details.committed = 1;
            details.has_last_commit_epoch = 1;
            details.last_commit_epoch = *epoch;
        }
        MongrelError::CommitOutcomeUnknown { epoch, .. } => {
            details.outcome_known = 0;
            details.has_last_commit_epoch = 1;
            details.last_commit_epoch = *epoch;
        }
        _ => {}
    }
    set_error_with_details(code, e.to_string(), details)
}

fn default_details(code: ErrorCode) -> mongreldb_error_details_v1 {
    mongreldb_error_details_v1 {
        code: code.as_return(),
        outcome_known: u8::from(code != ErrorCode::OutcomeUnknown),
        committed: u8::from(matches!(
            code,
            ErrorCode::CommitOutcome
                | ErrorCode::QueryCancelledAfterCommit
                | ErrorCode::DeadlineAfterCommit
                | ErrorCode::SerializationAfterCommit
        )),
        retryable: u8::from(matches!(
            code,
            ErrorCode::Conflict | ErrorCode::QueryRegistryFull
        )),
        ..Default::default()
    }
}

pub(crate) fn set_error_with_details(
    code: ErrorCode,
    msg: impl Into<String>,
    mut details: mongreldb_error_details_v1,
) -> ErrorCode {
    let cstring = CString::new(msg.into())
        .unwrap_or_else(|_| CString::new("error message contained NUL").unwrap());
    details.struct_size = std::mem::size_of::<mongreldb_error_details_v1>();
    details.version = 1;
    details.code = code.as_return();
    LAST_ERROR.with(|cell| {
        let mut last = cell.borrow_mut();
        unsafe { drop_cstring_ptr(last.message) };
        last.code = code.as_return();
        last.message = cstring.into_raw();
        last.details = details;
    });
    code
}

/// Convenience: record an ad-hoc error (used for FFI-only failures like null
/// pointer arguments that never reach the core).
pub fn set_error_msg(code: ErrorCode, msg: impl Into<String>) -> ErrorCode {
    set_error_with_details(code, msg, default_details(code))
}

/// Clear the thread's last error (called at the start of each FFI function so
/// stale errors don't leak across calls).
pub fn clear() {
    LAST_ERROR.with(|cell| {
        let mut last = cell.borrow_mut();
        unsafe { drop_cstring_ptr(last.message) };
        last.code = ErrorCode::Ok.as_return();
        last.message = std::ptr::null_mut();
        last.details = mongreldb_error_details_v1::default();
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

/// Copy the current thread's structured error metadata into `out_details`.
/// This accessor does not clear or replace the current error.
///
/// # Safety
/// `out_details` must be a valid writable pointer.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_last_error_details_v1(
    out_details: *mut mongreldb_error_details_v1,
) -> i32 {
    if out_details.is_null() {
        return ErrorCode::InvalidArgument.as_return();
    }
    LAST_ERROR.with(|cell| {
        let last = cell.borrow();
        *out_details = last.details;
        0
    })
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
