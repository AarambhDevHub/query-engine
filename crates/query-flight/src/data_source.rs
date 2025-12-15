//! Flight Data Source
//!
//! DataSource implementation that reads from a remote Arrow Flight server.

use arrow::record_batch::RecordBatch;
use query_core::{QueryError, Result, Schema};
use query_executor::physical_plan::DataSource;
use std::sync::RwLock;
use tokio::runtime::Runtime;

use crate::FlightClient;

/// Data source that reads from a remote Arrow Flight server
#[derive(Debug)]
pub struct FlightDataSource {
    /// Remote Flight server URL
    endpoint: String,
    /// Query to execute (table name or SQL)
    query: String,
    /// Cached schema
    schema: Schema,
    /// Cached data (loaded on first scan)
    cached_batches: RwLock<Option<Vec<RecordBatch>>>,
}

impl FlightDataSource {
    /// Create a new Flight data source
    pub fn new(endpoint: impl Into<String>, query: impl Into<String>, schema: Schema) -> Self {
        Self {
            endpoint: endpoint.into(),
            query: query.into(),
            schema,
            cached_batches: RwLock::new(None),
        }
    }

    /// Create by connecting and fetching schema
    pub async fn connect(endpoint: impl Into<String>, table: impl Into<String>) -> Result<Self> {
        let endpoint = endpoint.into();
        let table = table.into();

        let mut client = FlightClient::connect(&endpoint)
            .await
            .map_err(|e| QueryError::ExecutionError(e.to_string()))?;

        let arrow_schema = client
            .get_table_schema(&table)
            .await
            .map_err(|e| QueryError::ExecutionError(e.to_string()))?;

        Ok(Self {
            endpoint,
            query: table,
            schema: Schema::from_arrow(&arrow_schema),
            cached_batches: RwLock::new(None),
        })
    }

    /// Get the endpoint URL
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

impl DataSource for FlightDataSource {
    fn scan(&self) -> Result<Vec<RecordBatch>> {
        // Check cache
        if let Some(ref batches) = *self.cached_batches.read().unwrap() {
            return Ok(batches.clone());
        }

        // Fetch from server
        let rt = Runtime::new().map_err(|e| QueryError::ExecutionError(e.to_string()))?;
        let batches = rt.block_on(self.fetch_data())?;

        // Cache result
        *self.cached_batches.write().unwrap() = Some(batches.clone());
        Ok(batches)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use query_core::{DataType, Field};

    #[test]
    fn test_flight_data_source_creation() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        let source = FlightDataSource::new("http://localhost:50051", "users", schema);
        assert_eq!(source.endpoint(), "http://localhost:50051");
        assert_eq!(source.query(), "users");
    }
}
