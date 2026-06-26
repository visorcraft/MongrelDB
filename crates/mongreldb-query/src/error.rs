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
}
