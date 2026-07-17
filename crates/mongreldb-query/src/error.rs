use thiserror::Error;

pub type Result<T> = std::result::Result<T, MongrelQueryError>;

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum MongrelQueryError {
    #[error("mongreldb error: {0}")]
    Core(mongreldb_core::MongrelError),
    #[error("arrow error: {0}")]
    Arrow(String),
    #[error("datafusion error: {0}")]
    DataFusion(String),
    #[error("schema error: {0}")]
    Schema(String),
    #[error("query {query_id} cancelled: {reason:?}")]
    QueryCancelled {
        query_id: crate::QueryId,
        reason: mongreldb_core::CancellationReason,
        committed: bool,
        committed_statements: usize,
        last_commit_epoch: Option<u64>,
        first_commit_statement_index: Option<usize>,
        last_commit_statement_index: Option<usize>,
        completed_statements: usize,
        cancelled_statement_index: usize,
    },
    #[error("query {query_id} deadline exceeded")]
    DeadlineExceeded {
        query_id: crate::QueryId,
        timeout_ms: Option<u64>,
        committed: bool,
        committed_statements: usize,
        last_commit_epoch: Option<u64>,
        first_commit_statement_index: Option<usize>,
        last_commit_statement_index: Option<usize>,
        completed_statements: usize,
        cancelled_statement_index: usize,
    },
    #[error("query id {query_id} is already active or retained")]
    QueryIdConflict { query_id: crate::QueryId },
    #[error("SQL query registry is full")]
    QueryRegistryFull,
    #[error("query {query_id} result limit exceeded: {message}")]
    ResultLimitExceeded {
        query_id: crate::QueryId,
        committed: bool,
        committed_statements: usize,
        last_commit_epoch: Option<u64>,
        first_commit_statement_index: Option<usize>,
        last_commit_statement_index: Option<usize>,
        completed_statements: usize,
        statement_index: usize,
        message: String,
    },
    #[error("transaction is aborted; ROLLBACK or ROLLBACK TO SAVEPOINT is required")]
    TransactionAborted,
    #[error("no SQL transaction is open")]
    NoSqlTransaction,
    #[error("no savepoint named '{name}'")]
    SavepointNotFound { name: String },
    #[error(
        "query {query_id} commit outcome: committed={committed}, committed_statements={committed_statements}, last_commit_epoch={last_commit_epoch:?}: {message}"
    )]
    CommitOutcome {
        query_id: crate::QueryId,
        committed: bool,
        committed_statements: usize,
        last_commit_epoch: Option<u64>,
        first_commit_statement_index: Option<usize>,
        last_commit_statement_index: Option<usize>,
        completed_statements: usize,
        statement_index: usize,
        message: String,
    },
    #[error("query {query_id} durable outcome is unknown: {message}")]
    OutcomeUnknown {
        query_id: crate::QueryId,
        message: String,
    },
    #[error("invalid query state: {0}")]
    InvalidQueryState(String),
}

impl From<mongreldb_core::MongrelError> for MongrelQueryError {
    /// Core errors arrive on their precise taxonomy variants — the commit
    /// path raises SSI certification aborts as
    /// `MongrelError::SerializationFailure` (category 8) natively and
    /// `From<LockError>` bridges deadlock victims onto `MongrelError::Deadlock`
    /// (category 9) — so the SQL boundary is a straight passthrough.
    fn from(error: mongreldb_core::MongrelError) -> Self {
        Self::Core(error)
    }
}

impl MongrelQueryError {
    /// Stable machine-readable category for language bindings and protocols.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Core(_) | Self::Arrow(_) | Self::DataFusion(_) | Self::Schema(_) => {
                "SQL_EXECUTION_FAILED"
            }
            Self::QueryCancelled {
                committed: true, ..
            } => "QUERY_CANCELLED_AFTER_COMMIT",
            Self::QueryCancelled { .. } => "QUERY_CANCELLED",
            Self::DeadlineExceeded {
                committed: true, ..
            } => "DEADLINE_AFTER_COMMIT",
            Self::DeadlineExceeded { .. } => "DEADLINE_EXCEEDED",
            Self::QueryIdConflict { .. } => "QUERY_ID_CONFLICT",
            Self::QueryRegistryFull => "QUERY_REGISTRY_FULL",
            Self::ResultLimitExceeded { .. } => "RESULT_LIMIT_EXCEEDED",
            Self::TransactionAborted => "TRANSACTION_ABORTED",
            Self::NoSqlTransaction => "NO_SQL_TRANSACTION",
            Self::SavepointNotFound { .. } => "SAVEPOINT_NOT_FOUND",
            Self::CommitOutcome { .. } => "COMMIT_OUTCOME",
            Self::OutcomeUnknown { .. } => "QUERY_OUTCOME_UNKNOWN",
            Self::InvalidQueryState(_) => "INVALID_QUERY_STATE",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongreldb_core::MongrelError;

    #[test]
    fn serialization_failure_passes_through_precise_with_category_8() {
        // The exact message core's commit path constructs (database.rs); the
        // variant now arrives native from core, so the boundary must preserve
        // it — and with it the precise taxonomy category 8.
        let core = MongrelError::SerializationFailure {
            message: "a concurrent commit invalidated this transaction's reads (pre-validate)"
                .into(),
        };
        let error = MongrelQueryError::from(core);
        match error {
            MongrelQueryError::Core(MongrelError::SerializationFailure { message }) => {
                assert_eq!(
                    message,
                    "a concurrent commit invalidated this transaction's reads (pre-validate)"
                );
                let precise = MongrelError::SerializationFailure { message };
                assert_eq!(precise.category().code(), 8);
                assert_eq!(
                    precise.to_string(),
                    "serialization failure: a concurrent commit invalidated this transaction's reads (pre-validate)"
                );
            }
            other => panic!("expected SerializationFailure, got {other:?}"),
        }
    }

    #[test]
    fn marker_prefixed_conflict_stays_a_plain_conflict() {
        // No string-marker reinterpretation survives at the boundary: even a
        // `Conflict` whose message begins with the legacy prefix keeps its
        // variant (and the generic transaction-conflict category).
        let core = MongrelError::Conflict("serialization failure: hand-built".into());
        let error = MongrelQueryError::from(core);
        assert!(
            matches!(
                &error,
                MongrelQueryError::Core(MongrelError::Conflict(message))
                    if message == "serialization failure: hand-built"
            ),
            "a marker-prefixed Conflict is never re-homed: {error:?}"
        );
    }

    #[test]
    fn non_marker_conflict_passes_through_untouched() {
        let core = MongrelError::Conflict(
            "write-write conflict (pre-validate, first-committer-wins)".into(),
        );
        let error = MongrelQueryError::from(core);
        assert!(
            matches!(
                &error,
                MongrelQueryError::Core(MongrelError::Conflict(message))
                    if message == "write-write conflict (pre-validate, first-committer-wins)"
            ),
            "plain conflicts keep their variant: {error:?}"
        );
    }

    #[test]
    fn deadlock_passes_through_precise() {
        // LockError::Deadlock bridges onto MongrelError::Deadlock in core;
        // the SQL boundary must preserve the dedicated variant.
        let core = MongrelError::from(mongreldb_core::locks::LockError::Deadlock {
            victim: 5,
            cycle: "5 → 2 → 5".into(),
        });
        let error = MongrelQueryError::from(core);
        assert!(
            matches!(
                &error,
                MongrelQueryError::Core(MongrelError::Deadlock { victim: 5, cycle })
                    if cycle == "5 → 2 → 5"
            ),
            "deadlock keeps victim and cycle: {error:?}"
        );
    }

    #[test]
    fn already_precise_variants_pass_through() {
        let error = MongrelQueryError::from(MongrelError::SerializationFailure {
            message: "native".into(),
        });
        assert!(matches!(
            error,
            MongrelQueryError::Core(MongrelError::SerializationFailure { .. })
        ));
        let error = MongrelQueryError::from(MongrelError::Cancelled);
        assert!(matches!(
            error,
            MongrelQueryError::Core(MongrelError::Cancelled)
        ));
    }
}
