use thiserror::Error;

pub type Result<T> = std::result::Result<T, MongrelError>;

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum MongrelError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("database at {path} is locked: {message}")]
    DatabaseLocked {
        path: std::path::PathBuf,
        message: String,
    },
    #[error("database still has {strong_handles} shared handles")]
    DatabaseBusy { strong_handles: usize },
    #[error("database handle belongs to process {owner_pid}, not post-fork process {current_pid}; open after exec")]
    ForkedProcess { owner_pid: u32, current_pid: u32 },
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
    #[error("unsupported {component} storage version {found}: this build supports version {supported} only; recreate the database or export data using the engine version that created it")]
    UnsupportedStorageVersion {
        component: &'static str,
        found: u16,
        supported: u16,
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
    #[error("OS CSPRNG unavailable: {0}")]
    EntropyUnavailable(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("table is full: {0}")]
    Full(String),
    #[error("transaction conflict: {0}")]
    Conflict(String),
    #[error("trigger validation failed: {0}")]
    TriggerValidation(String),
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
    #[error("execution deadline exceeded")]
    DeadlineExceeded,
    #[error("AI query work budget exceeded")]
    WorkBudgetExceeded,
    #[error(
        "execution resource limit exceeded for {resource}: requested {requested}, limit {limit}"
    )]
    ResourceLimitExceeded {
        resource: &'static str,
        requested: usize,
        limit: usize,
    },
    #[error("execution cancelled")]
    Cancelled,
    #[error("commit {epoch} is durable: {message}")]
    DurableCommit { epoch: u64, message: String },
    #[error("commit outcome at epoch {epoch} is unknown: {message}")]
    CommitOutcomeUnknown { epoch: u64, message: String },
    #[error("cursor stale: {0}")]
    CursorStale(String),
    #[error("cursor expired")]
    CursorExpired,
    #[error("{0}")]
    Other(String),
}

impl MongrelError {
    /// Maps this error onto the stable cross-language error taxonomy
    /// (spec section 9.7, FND-007).
    ///
    /// The taxonomy is deliberately coarser than [`MongrelError`]: bindings
    /// and gateways only see the [`ErrorCategory`], while the full variant
    /// and message remain available in-process. The mapping is total; several
    /// single-node variants have no exact cluster counterpart, so non-obvious
    /// choices are documented at the arm.
    pub fn category(&self) -> mongreldb_types::errors::ErrorCategory {
        use mongreldb_types::errors::ErrorCategory;
        match self {
            // A storage I/O failure means this replica cannot serve the
            // request; a retry (possibly against another replica) may succeed.
            MongrelError::Io(_) => ErrorCategory::ReplicaUnavailable,
            // The exclusive database-file lock is a contended resource.
            MongrelError::DatabaseLocked { .. } => ErrorCategory::ResourceExhausted,
            // Outstanding shared handles are a transient busy resource.
            MongrelError::DatabaseBusy { .. } => ErrorCategory::ResourceExhausted,
            // The operating system refuses to hand the parent's handles to
            // the forked child: an ownership denial, closest to an
            // authorization failure (no credential can fix it).
            MongrelError::ForkedProcess { .. } => ErrorCategory::PermissionDenied,
            // A codec failure means the payload does not match the durable /
            // log format this binary reads and writes — a format-version
            // disagreement (§11.8 advertises log/snapshot format min/max).
            MongrelError::Serialization(_) => ErrorCategory::ClusterVersionMismatch,
            // Corruption or integrity-check failure of local durable state
            // renders this replica unable to serve until repaired or rebuilt.
            MongrelError::CorruptWal { .. }
            | MongrelError::TornWrite { .. }
            | MongrelError::ChecksumMismatch { .. }
            | MongrelError::MagicMismatch { .. } => ErrorCategory::ReplicaUnavailable,
            // Explicit skew between the binary and the on-disk format version.
            MongrelError::UnsupportedStorageVersion { .. } => ErrorCategory::ClusterVersionMismatch,
            // Schema errors and dangling column references describe requests
            // that disagree with the current schema; the taxonomy has no
            // plain invalid-DDL category, so they share the schema-mismatch
            // category. Retrying the identical request re-fails.
            MongrelError::Schema(_) | MongrelError::ColumnNotFound(_) => {
                ErrorCategory::SchemaVersionMismatch
            }
            // Feature-set disagreement (§11.8): this build lacks the
            // encryption feature the database requires.
            MongrelError::EncryptionDisabled => ErrorCategory::ClusterVersionMismatch,
            // AES-256-GCM tag verification failing is an authentication
            // failure: wrong key material or tampered ciphertext.
            MongrelError::Encryption(_) | MongrelError::Decryption(_) => {
                ErrorCategory::Unauthenticated
            }
            // The OS entropy source is an environmental resource that is
            // temporarily unavailable.
            MongrelError::EntropyUnavailable(_) => ErrorCategory::ResourceExhausted,
            // Absence is judged against the caller's current view; the safe
            // interpretation is that the caller may hold stale metadata and
            // should refresh before concluding the object does not exist.
            MongrelError::NotFound(_) => ErrorCategory::StaleMetadata,
            // The taxonomy has no invalid-request category: a request this
            // binary rejects as malformed reflects a client/server contract
            // disagreement. Not retryable — only changing one side helps.
            MongrelError::InvalidArgument(_) => ErrorCategory::ClusterVersionMismatch,
            MongrelError::Full(_) => ErrorCategory::ResourceExhausted,
            MongrelError::Conflict(_) => ErrorCategory::TransactionConflict,
            // Triggers are schema objects; the write violates constraints
            // declared in the current schema. Re-issuing the identical write
            // re-fails unless the schema changed.
            MongrelError::TriggerValidation(_) => ErrorCategory::SchemaVersionMismatch,
            // A follower rejects writes exactly like a non-leader: reroute to
            // the writer with a leader hint.
            MongrelError::ReadOnlyReplica => ErrorCategory::NotLeader,
            MongrelError::AuthRequired => ErrorCategory::Unauthenticated,
            // The client negotiated the wrong open contract (credentials
            // offered to a database that does not require them): a contract
            // disagreement, not a credential failure.
            MongrelError::AuthNotRequired => ErrorCategory::ClusterVersionMismatch,
            MongrelError::InvalidCredentials { .. } => ErrorCategory::Unauthenticated,
            MongrelError::PermissionDenied { .. } => ErrorCategory::PermissionDenied,
            MongrelError::DeadlineExceeded => ErrorCategory::DeadlineExceeded,
            MongrelError::WorkBudgetExceeded | MongrelError::ResourceLimitExceeded { .. } => {
                ErrorCategory::ResourceExhausted
            }
            MongrelError::Cancelled => ErrorCategory::Cancelled,
            // The outcome of a DurableCommit is in fact known — the commit is
            // durable — but the taxonomy has no committed-with-warning
            // category. The critical shared property with CommitOutcomeUnknown
            // is that blindly replaying the request may duplicate an
            // already-durable commit, so it inherits the §11.7 "never replay
            // without a durable idempotency key" rule.
            MongrelError::DurableCommit { .. } | MongrelError::CommitOutcomeUnknown { .. } => {
                ErrorCategory::CommitOutcomeUnknown
            }
            // The cursor's server-side state no longer matches or no longer
            // exists; refresh by re-issuing the query.
            MongrelError::CursorStale(_) | MongrelError::CursorExpired => {
                ErrorCategory::StaleMetadata
            }
            // Catch-all for uncategorized subsystem failures (backup, PITR,
            // replication glue): treated as the serving replica failing to
            // complete the request. Callers must inspect the message.
            MongrelError::Other(_) => ErrorCategory::ReplicaUnavailable,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongreldb_types::errors::ErrorCategory;

    fn io_error() -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied")
    }

    fn bincode_error() -> bincode::Error {
        bincode::ErrorKind::Custom("codec".to_string()).into()
    }

    #[test]
    fn category_mapping_is_total_and_matches_expectations() {
        let cases: Vec<(MongrelError, ErrorCategory)> = vec![
            (
                MongrelError::Io(io_error()),
                ErrorCategory::ReplicaUnavailable,
            ),
            (
                MongrelError::DatabaseLocked {
                    path: "db.mdb".into(),
                    message: "held".into(),
                },
                ErrorCategory::ResourceExhausted,
            ),
            (
                MongrelError::DatabaseBusy { strong_handles: 2 },
                ErrorCategory::ResourceExhausted,
            ),
            (
                MongrelError::ForkedProcess {
                    owner_pid: 1,
                    current_pid: 2,
                },
                ErrorCategory::PermissionDenied,
            ),
            (
                MongrelError::Serialization(bincode_error()),
                ErrorCategory::ClusterVersionMismatch,
            ),
            (
                MongrelError::CorruptWal {
                    offset: 8,
                    reason: "bad".into(),
                },
                ErrorCategory::ReplicaUnavailable,
            ),
            (
                MongrelError::TornWrite { offset: 16 },
                ErrorCategory::ReplicaUnavailable,
            ),
            (
                MongrelError::ChecksumMismatch {
                    expected: 1,
                    actual: 2,
                    context: "page".into(),
                },
                ErrorCategory::ReplicaUnavailable,
            ),
            (
                MongrelError::MagicMismatch {
                    what: "wal",
                    expected: [1; 8],
                    got: [2; 8],
                },
                ErrorCategory::ReplicaUnavailable,
            ),
            (
                MongrelError::UnsupportedStorageVersion {
                    component: "wal",
                    found: 2,
                    supported: 1,
                },
                ErrorCategory::ClusterVersionMismatch,
            ),
            (
                MongrelError::Schema("bad ddl".into()),
                ErrorCategory::SchemaVersionMismatch,
            ),
            (
                MongrelError::ColumnNotFound("c".into()),
                ErrorCategory::SchemaVersionMismatch,
            ),
            (
                MongrelError::EncryptionDisabled,
                ErrorCategory::ClusterVersionMismatch,
            ),
            (
                MongrelError::Encryption("seal".into()),
                ErrorCategory::Unauthenticated,
            ),
            (
                MongrelError::Decryption("open".into()),
                ErrorCategory::Unauthenticated,
            ),
            (
                MongrelError::EntropyUnavailable("rng".into()),
                ErrorCategory::ResourceExhausted,
            ),
            (
                MongrelError::NotFound("row".into()),
                ErrorCategory::StaleMetadata,
            ),
            (
                MongrelError::InvalidArgument("arg".into()),
                ErrorCategory::ClusterVersionMismatch,
            ),
            (
                MongrelError::Full("table".into()),
                ErrorCategory::ResourceExhausted,
            ),
            (
                MongrelError::Conflict("rw".into()),
                ErrorCategory::TransactionConflict,
            ),
            (
                MongrelError::TriggerValidation("ck".into()),
                ErrorCategory::SchemaVersionMismatch,
            ),
            (MongrelError::ReadOnlyReplica, ErrorCategory::NotLeader),
            (MongrelError::AuthRequired, ErrorCategory::Unauthenticated),
            (
                MongrelError::AuthNotRequired,
                ErrorCategory::ClusterVersionMismatch,
            ),
            (
                MongrelError::InvalidCredentials {
                    username: "alice".into(),
                },
                ErrorCategory::Unauthenticated,
            ),
            (
                MongrelError::PermissionDenied {
                    required: crate::auth::Permission::Admin,
                    principal: "bob".into(),
                },
                ErrorCategory::PermissionDenied,
            ),
            (
                MongrelError::DeadlineExceeded,
                ErrorCategory::DeadlineExceeded,
            ),
            (
                MongrelError::WorkBudgetExceeded,
                ErrorCategory::ResourceExhausted,
            ),
            (
                MongrelError::ResourceLimitExceeded {
                    resource: "rows",
                    requested: 10,
                    limit: 5,
                },
                ErrorCategory::ResourceExhausted,
            ),
            (MongrelError::Cancelled, ErrorCategory::Cancelled),
            (
                MongrelError::DurableCommit {
                    epoch: 9,
                    message: "callback".into(),
                },
                ErrorCategory::CommitOutcomeUnknown,
            ),
            (
                MongrelError::CommitOutcomeUnknown {
                    epoch: 10,
                    message: "lost".into(),
                },
                ErrorCategory::CommitOutcomeUnknown,
            ),
            (
                MongrelError::CursorStale("gen".into()),
                ErrorCategory::StaleMetadata,
            ),
            (MongrelError::CursorExpired, ErrorCategory::StaleMetadata),
            (
                MongrelError::Other("misc".into()),
                ErrorCategory::ReplicaUnavailable,
            ),
        ];
        assert_eq!(
            cases.len(),
            35,
            "every MongrelError variant must appear in the mapping table"
        );
        for (error, expected) in cases {
            assert_eq!(error.category(), expected, "wrong category for {error}");
        }
    }

    #[test]
    fn prescribed_fnd_007_mappings_hold() {
        assert_eq!(
            MongrelError::Conflict("x".into()).category(),
            ErrorCategory::TransactionConflict
        );
        assert_eq!(MongrelError::Cancelled.category(), ErrorCategory::Cancelled);
        assert_eq!(
            MongrelError::CommitOutcomeUnknown {
                epoch: 1,
                message: "m".into(),
            }
            .category(),
            ErrorCategory::CommitOutcomeUnknown
        );
        assert_eq!(
            MongrelError::DatabaseLocked {
                path: "p".into(),
                message: "m".into(),
            }
            .category(),
            ErrorCategory::ResourceExhausted
        );
        assert_eq!(
            MongrelError::ReadOnlyReplica.category(),
            ErrorCategory::NotLeader
        );
    }
}
