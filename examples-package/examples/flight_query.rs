//! Arrow Flight Example
//!
//! Demonstrates using Arrow Flight for remote SQL query execution.

use anyhow::Result;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use query_core::Schema as CoreSchema;
use query_flight::{FlightClient, FlightServer};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing for logs
    tracing_subscriber::fmt::init();

    println!("=== Arrow Flight Example ===\n");

    // Create sample data
    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Alice", "Bob", "Charlie", "Diana", "Eve",
            ])),
            Arc::new(Int64Array::from(vec![25, 30, 35, 28, 32])),
        ],
    )?;

    println!("Sample Data:");
    print_batches(&[batch.clone()])?;
    println!();

    // Create and configure the Flight server
    let server = FlightServer::new();
    let core_schema = CoreSchema::from_arrow(&arrow_schema);
    server
        .service()
        .register_table("users", core_schema, vec![batch]);

    println!("✓ Registered table 'users' with Flight server\n");

    // Start server in background
    let addr: SocketAddr = "127.0.0.1:50051".parse()?;
    let server_handle = tokio::spawn(async move {
        if let Err(e) = server.serve(addr).await {
            eprintln!("Server error: {}", e);
        }
    });

    // Wait for server to start
    sleep(Duration::from_millis(500)).await;
    println!("✓ Flight server started on {}\n", addr);

    // Connect with a client
    let mut client = FlightClient::connect("http://127.0.0.1:50051").await?;
    println!("✓ Connected to Flight server\n");

    // Perform handshake
    client.handshake().await?;
    println!("✓ Handshake completed\n");

    // List available tables
    let tables = client.list_flights().await?;
    println!("Available tables: {:?}\n", tables);

    // Get table schema
    let schema = client.get_table_schema("users").await?;
    println!("Schema for 'users' table:");
    for field in schema.fields() {
        println!("  {} : {:?}", field.name(), field.data_type());
    }
    println!();

    // Execute a query
    println!("Executing query: SELECT * FROM users\n");
    let results = client.execute_sql("SELECT * FROM users").await?;

    println!("Query Results:");
    print_batches(&results)?;
    println!();

    // Also can use simple table name
    println!("Executing query: users\n");
    let results2 = client.execute_sql("users").await?;
    println!("Results (same as above):");
    print_batches(&results2)?;

    println!("\n=== Arrow Flight Example Complete ===");

    // Clean up - abort the server
    server_handle.abort();

    Ok(())
}
