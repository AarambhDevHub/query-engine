//! PostgreSQL Wire Protocol Server Example
//!
//! This example demonstrates how to use the query-pgwire crate to create
//! a PostgreSQL-compatible server that can be accessed by standard PostgreSQL clients.
//!
//! # Running the Example
//!
//! ```bash
//! cargo run --example pgwire_server
//! ```
//!
//! Then connect with:
//! ```bash
//! psql -h 127.0.0.1 -p 5433
//! ```

use anyhow::Result;
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use query_pgwire::PgServer;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("=== PostgreSQL Wire Protocol Server Example ===\n");

    // Create the PostgreSQL server
    let server = PgServer::new("127.0.0.1", 5433);

    // Create sample data
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("salary", DataType::Float64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Alice", "Bob", "Charlie", "Diana", "Eve",
            ])),
            Arc::new(Float64Array::from(vec![
                75000.0, 82000.0, 68000.0, 95000.0, 71000.0,
            ])),
        ],
    )?;

    // Register the table with the server
    server.register_table("employees", vec![batch]).await?;

    // You can also load CSV files:
    // server.load_csv("path/to/data.csv", "tablename").await?;

    println!("Server starting on 127.0.0.1:5433");
    println!("Table 'employees' registered with 5 rows");
    println!("\nConnect with:");
    println!("  psql -h 127.0.0.1 -p 5433");
    println!("\nExample queries:");
    println!("  SELECT * FROM employees;");
    println!("  SELECT name, salary FROM employees WHERE salary > 70000;");
    println!("  SELECT AVG(salary) FROM employees;");
    println!("\nPress Ctrl+C to stop the server.\n");

    // Start the server (this blocks)
    server.start().await?;

    Ok(())
}
