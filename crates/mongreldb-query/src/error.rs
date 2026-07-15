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
    },
    #[error("query {query_id} deadline exceeded")]
    DeadlineExceeded {
        query_id: crate::QueryId,
        timeout_ms: Option<u64>,
    },
    #[error("query id {query_id} is already active")]
    QueryIdConflict { query_id: crate::QueryId },
    #[error("SQL query registry is full")]
    QueryRegistryFull,
    #[error("transaction is aborted; only ROLLBACK is allowed")]
    TransactionAborted,
    #[error("query {query_id} commit outcome: committed={committed}: {message}")]
    CommitOutcome {
        query_id: crate::QueryId,
        committed: bool,
        message: String,
    },
    #[error("invalid query state: {0}")]
    InvalidQueryState(String),
}
