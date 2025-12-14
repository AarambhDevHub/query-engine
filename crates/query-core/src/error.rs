use thiserror::Error;

#[derive(Error, Debug)]
pub enum QueryError {
    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Planning error: {0}")]
    PlanningError(String),

    #[error("Execution error: {0}")]
    ExecutionError(String),

    #[error("Schema error: {0}")]
    SchemaError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),

    #[error("Parquet error: {0}")]
    ParquetError(String),

    #[error("Type mismatch: expected {expected}, found {found}")]
    TypeMismatch { expected: String, found: String },

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Index not found: {0}")]
    IndexNotFound(String),

    #[error("Index already exists: {0}")]
    IndexAlreadyExists(String),

    #[error("Unique constraint violation: {0}")]
    UniqueConstraintViolation(String),

    #[error("Cache error: {0}")]
    CacheError(String),

    #[error("Stream error: {0}")]
    StreamError(String),
}

impl From<parquet::errors::ParquetError> for QueryError {
    fn from(err: parquet::errors::ParquetError) -> Self {
        QueryError::ParquetError(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, QueryError>;
