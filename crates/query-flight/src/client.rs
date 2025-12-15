//! Arrow Flight client for connecting to Flight servers
//!
//! Provides a client for executing SQL queries on remote Flight servers
//! and streaming results back as RecordBatches.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, FlightDescriptor, Ticket};
use futures::TryStreamExt;
use tonic::transport::Channel;
use tracing::info;

use crate::error::FlightError;

/// Flight client for remote query execution
pub struct FlightClient {
    client: FlightServiceClient<Channel>,
    endpoint: String,
}

impl FlightClient {
    /// Connect to a Flight server
    pub async fn connect(endpoint: &str) -> Result<Self, FlightError> {
        info!("Connecting to Flight server at {}", endpoint);

        let channel = Channel::from_shared(endpoint.to_string())
            .map_err(|e| FlightError::InvalidRequest(e.to_string()))?
            .connect()
            .await?;

        let client = FlightServiceClient::new(channel);

        Ok(Self {
            client,
            endpoint: endpoint.to_string(),
        })
    }

    /// Get the endpoint URL
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Execute a SQL query and return all results
    pub async fn execute_sql(
        &mut self,
        sql: impl Into<String>,
    ) -> Result<Vec<RecordBatch>, FlightError> {
        let sql_string = sql.into();
        info!("Executing SQL: {}", sql_string);

        // Create ticket with SQL query
        let ticket = Ticket::new(sql_string);

        // Get flight data stream
        let response = self.client.do_get(ticket).await?;
        let stream = response.into_inner();

        // Wrap the stream to convert Status errors to FlightError
        let mapped_stream = stream.map_err(arrow_flight::error::FlightError::Tonic);

        // Decode into record batches
        let batch_stream = FlightRecordBatchStream::new_from_flight_data(mapped_stream);
        let batches: Vec<RecordBatch> = batch_stream.try_collect().await?;

        info!("Received {} batches", batches.len());
        Ok(batches)
    }

    /// Get list of available tables
    pub async fn list_tables(&mut self) -> Result<Vec<String>, FlightError> {
        let action = Action::new("list_tables", "");

        let response = self.client.do_action(action).await?;
        let mut stream = response.into_inner();

        let mut tables = Vec::new();
        while let Some(result) = stream.message().await? {
            if let Ok(names) = serde_json::from_slice::<Vec<String>>(&result.body) {
                tables.extend(names);
            }
        }

        Ok(tables)
    }

    /// Get schema for a table
    pub async fn get_table_schema(
        &mut self,
        table_name: &str,
    ) -> Result<Arc<arrow::datatypes::Schema>, FlightError> {
        let descriptor = FlightDescriptor::new_path(vec![table_name.to_string()]);

        let response = self.client.get_flight_info(descriptor).await?;
        let flight_info = response.into_inner();

        let schema = flight_info
            .try_decode_schema()
            .map_err(FlightError::ArrowError)?;

        Ok(Arc::new(schema))
    }

    /// Get flight info for a SQL query (to check schema before execution)
    pub async fn get_query_info(
        &mut self,
        sql: impl Into<String>,
    ) -> Result<Arc<arrow::datatypes::Schema>, FlightError> {
        let sql_string = sql.into();
        let descriptor = FlightDescriptor::new_cmd(sql_string);

        let response = self.client.get_flight_info(descriptor).await?;
        let flight_info = response.into_inner();

        let schema = flight_info
            .try_decode_schema()
            .map_err(FlightError::ArrowError)?;

        Ok(Arc::new(schema))
    }

    /// Clear all tables on the server
    pub async fn clear_tables(&mut self) -> Result<(), FlightError> {
        let action = Action::new("clear_tables", "");

        let response = self.client.do_action(action).await?;
        let mut stream = response.into_inner();

        // Consume the response
        while stream.message().await?.is_some() {}

        info!("Cleared tables on server");
        Ok(())
    }

    /// Perform handshake (for future authentication)
    pub async fn handshake(&mut self) -> Result<(), FlightError> {
        use arrow_flight::HandshakeRequest;
        use futures::stream;

        let request = HandshakeRequest {
            protocol_version: 0,
            payload: Default::default(),
        };

        let response = self
            .client
            .handshake(stream::once(async { request }))
            .await?;

        let mut stream = response.into_inner();
        while stream.message().await?.is_some() {}

        info!("Handshake completed");
        Ok(())
    }

    /// List all available flights (tables)
    pub async fn list_flights(&mut self) -> Result<Vec<String>, FlightError> {
        use arrow_flight::Criteria;

        let criteria = Criteria::default();
        let response = self.client.list_flights(criteria).await?;
        let mut stream = response.into_inner();

        let mut flights = Vec::new();
        while let Some(info) = stream.message().await? {
            if let Some(descriptor) = info.flight_descriptor {
                if let Some(name) = descriptor.path.first() {
                    flights.push(name.clone());
                }
            }
        }

        Ok(flights)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_invalid_connection() {
        let result = FlightClient::connect("http://localhost:99999").await;
        // Should fail to connect to non-existent server
        assert!(result.is_err());
    }
}
