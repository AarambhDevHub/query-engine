//! Arrow Flight server implementation
//!
//! Provides a gRPC server that implements the Flight protocol for
//! executing SQL queries and streaming results.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use arrow_ipc::writer::IpcWriteOptions;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use parking_lot::RwLock;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, warn};

use query_core::Schema;
use query_executor::physical_plan::DataSource;
use query_storage::MemoryDataSource;

use crate::error::FlightError;

/// Table storage for the Flight server
#[derive(Default)]
struct TableStore {
    /// Tables stored as MemoryDataSource
    tables: HashMap<String, Arc<MemoryDataSource>>,
}

impl TableStore {
    fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    fn register(&mut self, name: &str, schema: Schema, batches: Vec<RecordBatch>) {
        let source = MemoryDataSource::with_table_name(schema, batches, name);
        self.tables.insert(name.to_string(), Arc::new(source));
    }

    fn get(&self, name: &str) -> Option<Arc<MemoryDataSource>> {
        self.tables.get(name).cloned()
    }

    fn schemas(&self) -> impl Iterator<Item = (&String, Schema)> {
        self.tables
            .iter()
            .map(|(name, source)| (name, source.schema().clone()))
    }

    fn names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    fn clear(&mut self) {
        self.tables.clear();
    }
}

/// Arrow Flight server for Query Engine
pub struct FlightServer {
    service: FlightServiceImpl,
}

impl FlightServer {
    /// Create a new Flight server
    pub fn new() -> Self {
        Self {
            service: FlightServiceImpl::new(),
        }
    }

    /// Start serving on the given address
    pub async fn serve(self, addr: SocketAddr) -> Result<(), FlightError> {
        info!("Starting Flight server on {}", addr);

        Server::builder()
            .add_service(FlightServiceServer::new(self.service))
            .serve(addr)
            .await?;

        Ok(())
    }

    /// Get a reference to the service for registering tables
    pub fn service(&self) -> &FlightServiceImpl {
        &self.service
    }

    /// Get a mutable reference to register tables
    pub fn service_mut(&mut self) -> &mut FlightServiceImpl {
        &mut self.service
    }
}

impl Default for FlightServer {
    fn default() -> Self {
        Self::new()
    }
}

/// Flight service implementation
pub struct FlightServiceImpl {
    tables: Arc<RwLock<TableStore>>,
}

impl FlightServiceImpl {
    /// Create a new service
    pub fn new() -> Self {
        Self {
            tables: Arc::new(RwLock::new(TableStore::new())),
        }
    }

    /// Register a table with the service
    pub fn register_table(&self, name: &str, schema: Schema, batches: Vec<RecordBatch>) {
        let mut tables = self.tables.write();
        tables.register(name, schema, batches);
        info!("Registered table: {}", name);
    }

    /// Register a table from a single RecordBatch
    pub fn register_batch(&self, name: &str, batch: RecordBatch) {
        let schema = Schema::from_arrow(batch.schema().as_ref());
        self.register_table(name, schema, vec![batch]);
    }

    /// Get list of registered tables
    pub fn list_tables(&self) -> Vec<String> {
        self.tables.read().names()
    }

    /// Execute a table scan query
    ///
    /// For now, this supports simple table scans. The input should be a table name.
    /// Format: "SELECT * FROM table_name" or just "table_name"
    async fn execute_query(&self, query: &str) -> Result<Vec<RecordBatch>, FlightError> {
        info!("Executing query: {}", query);

        // Extract table name from query
        let table_name = Self::extract_table_name(query);

        // Get table and scan it
        let batches = {
            let tables = self.tables.read();
            let source = tables
                .get(&table_name)
                .ok_or_else(|| FlightError::TableNotFound(table_name.clone()))?;

            // Perform the scan synchronously
            source
                .scan()
                .map_err(|e| FlightError::ExecutionError(e.to_string()))?
        }; // Lock is dropped here

        info!("Query returned {} batches", batches.len());
        Ok(batches)
    }

    /// Extract table name from a query string
    fn extract_table_name(query: &str) -> String {
        let query = query.trim();

        // Handle "SELECT * FROM table_name" format
        if query.to_uppercase().starts_with("SELECT") {
            if let Some(from_pos) = query.to_uppercase().find("FROM") {
                let after_from = &query[from_pos + 4..].trim();
                // Get the next word (table name)
                return after_from
                    .split(|c: char| c.is_whitespace() || c == ';')
                    .next()
                    .unwrap_or(query)
                    .to_string();
            }
        }

        // Otherwise, treat the whole query as a table name
        query.to_string()
    }
}

impl Default for FlightServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl FlightService for FlightServiceImpl {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    /// Handshake for authentication (no-op for now)
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let response = HandshakeResponse {
            protocol_version: 0,
            payload: Default::default(),
        };
        let stream = stream::once(async { Ok(response) }).boxed();
        Ok(Response::new(stream))
    }

    /// List available tables as flights
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        let tables = self.tables.read();
        let mut flights = Vec::new();

        for (name, schema) in tables.schemas() {
            let descriptor = FlightDescriptor::new_path(vec![name.clone()]);
            let info = FlightInfo::new()
                .with_descriptor(descriptor)
                .try_with_schema(&schema.to_arrow())
                .map_err(|e| Status::internal(e.to_string()))?;
            flights.push(Ok(info));
        }

        let stream = stream::iter(flights).boxed();
        Ok(Response::new(stream))
    }

    /// Get flight info for a specific query
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();

        // Get query from descriptor command or path
        let query = if !descriptor.cmd.is_empty() {
            String::from_utf8_lossy(&descriptor.cmd).to_string()
        } else if let Some(table_name) = descriptor.path.first() {
            table_name.clone()
        } else {
            return Err(Status::invalid_argument("No query or table provided"));
        };

        // Get the table
        let table_name = Self::extract_table_name(&query);
        let tables = self.tables.read();

        let source = tables
            .get(&table_name)
            .ok_or_else(|| Status::not_found(format!("Table not found: {}", table_name)))?;

        let schema = source.schema().to_arrow();

        // Create ticket with the query
        let ticket = Ticket::new(query);

        let info = FlightInfo::new()
            .with_descriptor(descriptor)
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(e.to_string()))?
            .with_endpoint(arrow_flight::FlightEndpoint::new().with_ticket(ticket));

        Ok(Response::new(info))
    }

    /// Poll for flight info (not supported)
    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not supported"))
    }

    /// Get schema for a flight
    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let descriptor = request.into_inner();

        // Try path-based lookup first
        let table_name = if let Some(name) = descriptor.path.first() {
            name.clone()
        } else if !descriptor.cmd.is_empty() {
            Self::extract_table_name(&String::from_utf8_lossy(&descriptor.cmd))
        } else {
            return Err(Status::invalid_argument("No table specified"));
        };

        let tables = self.tables.read();
        let source = tables
            .get(&table_name)
            .ok_or_else(|| Status::not_found(format!("Table not found: {}", table_name)))?;

        let arrow_schema = source.schema().to_arrow();
        let options = IpcWriteOptions::default();
        let schema_as_ipc = SchemaAsIpc::new(&arrow_schema, &options);
        let result: SchemaResult = schema_as_ipc
            .try_into()
            .map_err(|e: arrow::error::ArrowError| Status::internal(e.to_string()))?;

        Ok(Response::new(result))
    }

    /// Execute query and stream results
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let query = String::from_utf8_lossy(&ticket.ticket).to_string();

        info!("Executing Flight query: {}", query);

        // Execute the query
        let batches = self.execute_query(&query).await.map_err(Status::from)?;

        if batches.is_empty() {
            return Err(Status::not_found("Query returned no results"));
        }

        // Encode batches as FlightData stream
        let schema = batches[0].schema();
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream::iter(batches.into_iter().map(Ok)))
            .map_err(|e| Status::internal(e.to_string()));

        Ok(Response::new(flight_data_stream.boxed()))
    }

    /// Accept data upload (store as table)
    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut stream = request.into_inner();

        // Collect all flight data
        let mut table_name = String::new();

        while let Some(data) = stream.next().await {
            let data = data?;

            // Try to get table name from descriptor
            if let Some(descriptor) = &data.flight_descriptor {
                if let Some(name) = descriptor.path.first() {
                    table_name = name.clone();
                }
            }

            // For now, just acknowledge - full implementation would decode IPC
            if !data.data_body.is_empty() {
                warn!("do_put data decoding not fully implemented");
            }
        }

        if table_name.is_empty() {
            return Err(Status::invalid_argument("No table name provided"));
        }

        let result = PutResult {
            app_metadata: Default::default(),
        };
        let stream = stream::once(async { Ok(result) }).boxed();
        Ok(Response::new(stream))
    }

    /// Execute actions (e.g., clear cache)
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();

        match action.r#type.as_str() {
            "clear_tables" => {
                let mut tables = self.tables.write();
                tables.clear();
                info!("Cleared all tables");

                let result = arrow_flight::Result {
                    body: b"Tables cleared".to_vec().into(),
                };
                let stream = stream::once(async { Ok(result) }).boxed();
                Ok(Response::new(stream))
            }
            "list_tables" => {
                let tables = self.tables.read();
                let names = tables.names();
                let body = serde_json::to_vec(&names).unwrap_or_default();

                let result = arrow_flight::Result { body: body.into() };
                let stream = stream::once(async { Ok(result) }).boxed();
                Ok(Response::new(stream))
            }
            _ => Err(Status::unimplemented(format!(
                "Unknown action: {}",
                action.r#type
            ))),
        }
    }

    /// List available actions
    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![
            Ok(ActionType {
                r#type: "clear_tables".to_string(),
                description: "Clear all registered tables".to_string(),
            }),
            Ok(ActionType {
                r#type: "list_tables".to_string(),
                description: "List all registered tables".to_string(),
            }),
        ];

        let stream = stream::iter(actions).boxed();
        Ok(Response::new(stream))
    }

    /// Bidirectional exchange (not supported)
    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not supported"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

    #[test]
    fn test_flight_server_creation() {
        let server = FlightServer::new();
        assert!(server.service.list_tables().is_empty());
    }

    #[test]
    fn test_register_table() {
        let server = FlightServer::new();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        server.service.register_batch("test", batch);

        let tables = server.service.list_tables();
        assert!(tables.contains(&"test".to_string()));
    }

    #[test]
    fn test_extract_table_name() {
        assert_eq!(
            FlightServiceImpl::extract_table_name("SELECT * FROM users"),
            "users"
        );
        assert_eq!(
            FlightServiceImpl::extract_table_name("SELECT id, name FROM employees WHERE id > 5"),
            "employees"
        );
        assert_eq!(
            FlightServiceImpl::extract_table_name("my_table"),
            "my_table"
        );
    }
}
