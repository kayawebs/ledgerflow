use thiserror::Error;

#[derive(Debug, Error)]
pub enum LedgerError {
    #[error("transient ingestion error: {0}")]
    TransientIngestion(String),
    #[error("unsupported trace: {0}")]
    UnsupportedTrace(String),
    #[error("malformed evidence: {0}")]
    MalformedEvidence(String),
    #[error("analysis invariant violation: {0}")]
    AnalysisInvariant(String),
    #[error("recognizer execution error: {0}")]
    RecognizerExecution(String),
    #[error("persistence error: {0}")]
    Persistence(String),
    #[error("not found: {0}")]
    NotFound(String),
}

pub type LedgerResult<T> = Result<T, LedgerError>;
