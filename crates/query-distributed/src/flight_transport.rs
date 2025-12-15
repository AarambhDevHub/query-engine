//! Flight Transport for Distributed Execution
//!
//! Uses Arrow Flight as the transport layer for distributed query execution.

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use query_core::{FlightEndpoint, QueryError, Result};

/// Transport layer using Arrow Flight for distributed communication
pub struct FlightTransport {
    /// Worker endpoints
    endpoints: Vec<FlightEndpoint>,
}

impl FlightTransport {
    /// Create a new Flight transport
    pub fn new() -> Self {
        Self {
            endpoints: Vec::new(),
        }
    }

    /// Add a worker endpoint
    pub fn add_endpoint(&mut self, endpoint: FlightEndpoint) {
        self.endpoints.push(endpoint);
    }

    /// Add a worker by URL
    pub fn add_worker(&mut self, url: impl Into<String>) {
        self.endpoints.push(FlightEndpoint::new(url));
    }

    /// Get all registered endpoints
    pub fn endpoints(&self) -> &[FlightEndpoint] {
        &self.endpoints
    }

    /// Send a query to a specific worker and get results
    pub async fn execute_on_worker(
        &self,
        worker_idx: usize,
        query: &str,
    ) -> Result<Vec<RecordBatch>> {
        if worker_idx >= self.endpoints.len() {
            return Err(QueryError::ExecutionError(format!(
                "Worker index {} out of range (have {} workers)",
                worker_idx,
                self.endpoints.len()
            )));
        }

        let endpoint = &self.endpoints[worker_idx];
        let mut client = query_flight::FlightClient::connect(&endpoint.url)
            .await
            .map_err(|e| QueryError::ExecutionError(e.to_string()))?;

        let batches = client
            .execute_sql(query)
            .await
            .map_err(|e| QueryError::ExecutionError(e.to_string()))?;

        Ok(batches)
    }

    /// Execute a query on all workers and collect results
    pub async fn execute_on_all(&self, query: &str) -> Result<Vec<Vec<RecordBatch>>> {
        let mut results = Vec::with_capacity(self.endpoints.len());

        for (idx, _endpoint) in self.endpoints.iter().enumerate() {
            let batches = self.execute_on_worker(idx, query).await?;
            results.push(batches);
        }

        Ok(results)
    }

    /// Get worker count
    pub fn worker_count(&self) -> usize {
        self.endpoints.len()
    }
}

impl Default for FlightTransport {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for distributed transport layer
#[async_trait]
pub trait DistributedTransport: Send + Sync {
    /// Execute query on a worker
    async fn execute(&self, worker: usize, query: &str) -> Result<Vec<RecordBatch>>;

    /// Get number of workers
    fn num_workers(&self) -> usize;
}

#[async_trait]
impl DistributedTransport for FlightTransport {
    async fn execute(&self, worker: usize, query: &str) -> Result<Vec<RecordBatch>> {
        self.execute_on_worker(worker, query).await
    }

    fn num_workers(&self) -> usize {
        self.worker_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flight_transport_creation() {
        let mut transport = FlightTransport::new();
        assert_eq!(transport.worker_count(), 0);

        transport.add_worker("http://localhost:50051");
        transport.add_worker("http://localhost:50052");
        assert_eq!(transport.worker_count(), 2);
    }

    #[test]
    fn test_flight_transport_endpoints() {
        let mut transport = FlightTransport::new();
        transport.add_endpoint(FlightEndpoint::with_auth(
            "http://localhost:50051",
            "secret_token",
        ));

        let endpoints = transport.endpoints();
        assert_eq!(endpoints.len(), 1);
        assert!(endpoints[0].auth_token.is_some());
    }
}
