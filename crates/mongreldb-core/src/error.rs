use thiserror::Error;

pub type Result<T> = std::result::Result<T, MongrelError>;

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum MongrelError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] bincode::Error),
    #[error("corrupt wal record at offset {offset}: {reason}")]
    CorruptWal { offset: u64, reason: String },
    #[error("torn wal write detected at offset {offset}")]
    TornWrite { offset: u64 },
    #[error("checksum mismatch for {context}: expected {expected}, got {actual}")]
    ChecksumMismatch {
        expected: u64,
        actual: u64,
        context: String,
    },
    #[error("magic mismatch in {what}: expected {expected:?}, got {got:?}")]
    MagicMismatch {
        what: &'static str,
        expected: [u8; 8],
        got: [u8; 8],
    },
    #[error("schema error: {0}")]
    Schema(String),
    #[error("column not found: {0}")]
    ColumnNotFound(String),
    #[error("encryption is required for this table but the `encryption` feature is disabled")]
    EncryptionDisabled,
    #[error("encryption error: {0}")]
    Encryption(String),
    #[error("decryption error: {0}")]
    Decryption(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("table is full: {0}")]
    Full(String),
    #[error("transaction conflict: {0}")]
    Conflict(String),
    #[error("read-only replica: writes must be applied by ReplicationFollower")]
    ReadOnlyReplica,
    #[error("authentication required: this database has require_auth enabled; reopen with open_with_credentials / open_encrypted_with_credentials")]
    AuthRequired,
    #[error("authentication not required: this database does not have require_auth enabled; use the plain open/create constructors")]
    AuthNotRequired,
    #[error("invalid credentials for user {username:?}")]
    InvalidCredentials { username: String },
    #[error("permission denied: principal {principal:?} lacks {required}")]
    PermissionDenied {
        required: crate::auth::Permission,
        principal: String,
    },
    #[error("AI query deadline exceeded")]
    DeadlineExceeded,
    #[error("AI query work budget exceeded")]
    WorkBudgetExceeded,
    #[error("AI query cancelled")]
    Cancelled,
    #[error("cursor stale: {0}")]
    CursorStale(String),
    #[error("cursor expired")]
    CursorExpired,
    #[error("{0}")]
    Other(String),
}
