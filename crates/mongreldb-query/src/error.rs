use thiserror::Error;

pub type Result<T> = std::result::Result<T, MongrelQueryError>;

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum MongrelQueryError {
    #[error("mongreldb error: {0}")]
    Core(#[from] mongreldb_core::MongrelError),
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
