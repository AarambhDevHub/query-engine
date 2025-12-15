//! Flight Stream Source
//!
//! StreamSource implementation for streaming from Arrow Flight servers.

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use query_core::{QueryError, Result};
use query_streaming::StreamSource;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::FlightClient;

/// Stream source that reads from a remote Arrow Flight server
pub struct FlightStreamSource {
    endpoint: String,
    query: String,
    buffer: Arc<Mutex<Vec<RecordBatch>>>,
    position: Arc<Mutex<usize>>,
    exhausted: Arc<Mutex<bool>>,
    name: String,
}

impl FlightStreamSource {
    /// Create a new Flight stream source
    pub fn new(endpoint: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            query: query.into(),
            buffer: Arc::new(Mutex::new(Vec::new())),
            position: Arc::new(Mutex::new(0)),
            exhausted: Arc::new(Mutex::new(false)),
            name: "flight".to_string(),
        }
    }

    /// Create with a custom name
    pub fn with_name(
        endpoint: impl Into<String>,
        query: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        let mut source = Self::new(endpoint, query);
        source.name = name.into();
        source
    }

    /// Get the endpoint
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Get the query
    pub fn query(&self) -> &str {
        &self.query
    }

    async fn fetch_data(&self) -> Result<Vec<RecordBatch>> {
        let mut client = FlightClient::connect(&self.endpoint)
            .await
            .map_err(|e| QueryError::ExecutionError(e.to_string()))?;

        client
            .execute_sql(&self.query)
            .await
            .map_err(|e| QueryError::ExecutionError(e.to_string()))
    }
}

#[async_trait]
impl StreamSource for FlightStreamSource {
    async fn next_batch(&mut self) -> Option<Result<RecordBatch>> {
        // Initialize buffer if empty
        {
            let buffer = self.buffer.lock().await;
            let position = self.position.lock().await;
            if buffer.is_empty() && *position == 0 {
                drop(buffer);
                drop(position);

                match self.fetch_data().await {
                    Ok(batches) => {
                        *self.buffer.lock().await = batches;
                    }
                    Err(e) => {
                        *self.exhausted.lock().await = true;
                        return Some(Err(e));
                    }
                }
            }
        }

        let buffer = self.buffer.lock().await;
        let mut position = self.position.lock().await;

        if *position < buffer.len() {
            let batch = buffer[*position].clone();
            *position += 1;
            Some(Ok(batch))
        } else {
            *self.exhausted.lock().await = true;
            None
        }
    }

    fn is_exhausted(&self) -> bool {
        self.exhausted.try_lock().map(|g| *g).unwrap_or(false)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flight_stream_source_creation() {
        let source = FlightStreamSource::new("http://localhost:50051", "users");
        assert_eq!(source.endpoint(), "http://localhost:50051");
        assert_eq!(source.query(), "users");
        assert_eq!(source.name(), "flight");
    }
}
