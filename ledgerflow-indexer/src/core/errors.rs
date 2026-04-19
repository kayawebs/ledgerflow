use thiserror::Error;

pub type LedgerResult<T> = Result<T, LedgerError>;

#[derive(Debug, Error)]
pub enum LedgerError {
    #[error("transient ingestion error: {0}")]
    TransientIngestion(String),
    #[error("malformed evidence: {0}")]
    MalformedEvidence(String),
    #[error("persistence error: {0}")]
    Persistence(String),
    #[error("conflict: {0}")]
    Conflict(String),
}
