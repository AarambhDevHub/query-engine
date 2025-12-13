//! Error types for distributed execution

use thiserror::Error;

/// Errors that can occur during distributed execution
#[derive(Error, Debug)]
pub enum DistributedError {
    /// Worker is not available
    #[error("Worker not available: {0}")]
    WorkerUnavailable(String),

    /// Worker not found
    #[error("Worker not found: {0}")]
    WorkerNotFound(String),

    /// Worker already registered
    #[error("Worker already registered: {0}")]
    WorkerAlreadyRegistered(String),

    /// Network communication error
    #[error("Network error: {0}")]
    NetworkError(String),

    /// Partition error
    #[error("Partition error: {0}")]
    PartitionError(String),

    /// Cluster coordination error
    #[error("Cluster error: {0}")]
    ClusterError(String),

    /// Task execution failed
    #[error("Task execution failed: {0}")]
    TaskExecutionFailed(String),

    /// Task timeout
    #[error("Task timeout after {0}ms")]
    TaskTimeout(u64),

    /// No workers available
    #[error("No workers available to execute query")]
    NoWorkersAvailable,

    /// Query planning error
    #[error("Distributed planning error: {0}")]
    PlanningError(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// Query engine error
    #[error("Query engine error: {0}")]
    QueryError(#[from] query_core::QueryError),
}

/// Result type for distributed operations
pub type Result<T> = std::result::Result<T, DistributedError>;
