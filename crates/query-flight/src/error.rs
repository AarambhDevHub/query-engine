//! Error types for Arrow Flight operations

use thiserror::Error;

/// Errors that can occur during Flight operations
#[derive(Debug, Error)]
pub enum FlightError {
    /// SQL parsing error
    #[error("Parse error: {0}")]
    ParseError(String),

    /// Query planning error
    #[error("Planning error: {0}")]
    PlanningError(String),

    /// Query execution error
    #[error("Execution error: {0}")]
    ExecutionError(String),

    /// Table not found
    #[error("Table not found: {0}")]
    TableNotFound(String),

    /// Arrow error
    #[error("Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),

    /// Arrow Flight error
    #[error("Flight error: {0}")]
    FlightDecodeError(#[from] arrow_flight::error::FlightError),

    /// gRPC transport error
    #[error("Transport error: {0}")]
    TransportError(#[from] tonic::transport::Error),

    /// gRPC status error
    #[error("gRPC error: {0}")]
    GrpcError(#[from] tonic::Status),

    /// IO error
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// Invalid request
    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<FlightError> for tonic::Status {
    fn from(err: FlightError) -> Self {
        match err {
            FlightError::ParseError(msg) => tonic::Status::invalid_argument(msg),
            FlightError::PlanningError(msg) => tonic::Status::invalid_argument(msg),
            FlightError::ExecutionError(msg) => tonic::Status::internal(msg),
            FlightError::TableNotFound(msg) => tonic::Status::not_found(msg),
            FlightError::ArrowError(e) => tonic::Status::internal(e.to_string()),
            FlightError::FlightDecodeError(e) => tonic::Status::internal(e.to_string()),
            FlightError::TransportError(e) => tonic::Status::unavailable(e.to_string()),
            FlightError::GrpcError(status) => status,
            FlightError::IoError(e) => tonic::Status::internal(e.to_string()),
            FlightError::InvalidRequest(msg) => tonic::Status::invalid_argument(msg),
            FlightError::Internal(msg) => tonic::Status::internal(msg),
        }
    }
}

impl From<query_core::QueryError> for FlightError {
    fn from(err: query_core::QueryError) -> Self {
        FlightError::ExecutionError(err.to_string())
    }
}
