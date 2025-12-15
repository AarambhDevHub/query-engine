# API Guide

Guide to using Query Engine programmatically in Rust applications.

## Quick Start

Add to `Cargo.toml`:

```toml
[dependencies]
query-parser = { path = "crates/query-parser" }
query-planner = { path = "crates/query-planner" }
query-executor = { path = "crates/query-executor" }
query-storage = { path = "crates/query-storage" }
query-index = { path = "crates/query-index" }
query-flight = { path = "crates/query-flight" }
arrow = "53"
```

## Basic Query Execution

```rust
use query_parser::Parser;
use query_planner::Planner;
use query_executor::QueryExecutor;
use query_storage::CsvDataSource;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Load data
    let csv = CsvDataSource::from_path("data/employees.csv")?;
    let schema = csv.schema();
    let batches = csv.scan(None)?;
    
    // 2. Parse SQL
    let sql = "SELECT name, salary FROM employees WHERE salary > 50000";
    let mut parser = Parser::new(sql)?;
    let statement = parser.parse()?;
    
    // 3. Create plan
    let mut planner = Planner::new();
    planner.register_table("employees", schema);
    let plan = planner.create_logical_plan(&statement)?;
    
    // 4. Execute
    let executor = QueryExecutor::new();
    let mut tables = std::collections::HashMap::new();
    tables.insert("employees".to_string(), batches);
    let results = executor.execute(&plan, &tables)?;
    
    // 5. Process results
    for batch in results {
        println!("Got {} rows", batch.num_rows());
    }
    
    Ok(())
}
```

## Loading Data

### From CSV

```rust
use query_storage::CsvDataSource;

// From file
let csv = CsvDataSource::from_path("data.csv")?;

// With options
let csv = CsvDataSource::builder()
    .path("data.csv")
    .has_header(true)
    .delimiter(b',')
    .infer_schema_rows(100)
    .build()?;

// Get schema and data
let schema = csv.schema();
let batches = csv.scan(None)?;
```

### From Parquet

```rust
use query_storage::ParquetDataSource;

let parquet = ParquetDataSource::from_path("data.parquet")?;
let schema = parquet.schema();
let batches = parquet.scan(None)?;
```

### From Memory

```rust
use query_storage::MemoryTable;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{Schema, Field, DataType};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

let schema = Arc::new(Schema::new(vec![
    Field::new("id", DataType::Int64, false),
    Field::new("name", DataType::Utf8, false),
]));

let batch = RecordBatch::try_new(
    schema.clone(),
    vec![
        Arc::new(Int64Array::from(vec![1, 2, 3])),
        Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
    ],
)?;

let table = MemoryTable::new(schema, vec![batch]);
```

## Using Indexes

```rust
use query_index::{IndexManager, IndexKey};

let manager = IndexManager::new();

// Create B-Tree index (supports range queries)
manager.create_btree_index(
    "idx_salary",      // name
    "employees",       // table
    vec!["salary".to_string()],  // columns
    false              // unique
)?;

// Create Hash index (fast equality lookup)
manager.create_hash_index(
    "idx_email",
    "users",
    vec!["email".to_string()],
    true  // unique constraint
)?;

// Populate index
let index = manager.get_index("idx_salary").unwrap();
for (row_id, salary) in salaries.iter().enumerate() {
    index.insert(IndexKey::from_i64(*salary), row_id)?;
}

// Query index
let key = IndexKey::from_i64(75000);
let result = index.lookup(&key);
println!("Found rows: {:?}", result.row_ids);

// Range query (B-Tree only)
let start = IndexKey::from_i64(50000);
let end = IndexKey::from_i64(80000);
let result = index.range_scan(Some(&start), Some(&end));
```

## Building Logical Plans

### Programmatic Plan Building

```rust
use query_planner::LogicalPlan;
use query_core::{Schema, Expression};

// Table scan
let scan = LogicalPlan::TableScan {
    table_name: "employees".to_string(),
    schema: schema.clone(),
    projection: Some(vec![0, 1, 3]),  // Only id, name, salary
};

// Filter
let filter = LogicalPlan::Filter {
    input: Box::new(scan),
    predicate: Expression::BinaryOp {
        left: Box::new(Expression::column("salary")),
        op: BinaryOperator::Gt,
        right: Box::new(Expression::literal(50000)),
    },
};

// Projection
let projection = LogicalPlan::Projection {
    input: Box::new(filter),
    expressions: vec![
        Expression::column("name"),
        Expression::column("salary"),
    ],
};
```

## Working with Results

### Accessing Arrow Data

```rust
use arrow::array::{Int64Array, StringArray};

for batch in results {
    // Get column by index
    let names = batch.column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    
    let salaries = batch.column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    
    // Iterate rows
    for i in 0..batch.num_rows() {
        println!("{}: {}", names.value(i), salaries.value(i));
    }
}
```

### Converting to JSON

```rust
use arrow::json::writer::LineDelimitedWriter;

let mut buf = Vec::new();
{
    let mut writer = LineDelimitedWriter::new(&mut buf);
    for batch in &results {
        writer.write(batch)?;
    }
    writer.finish()?;
}
let json = String::from_utf8(buf)?;
```

### Converting to CSV

```rust
use arrow::csv::writer::WriterBuilder;

let mut buf = Vec::new();
{
    let mut writer = WriterBuilder::new()
        .with_header(true)
        .build(&mut buf);
    for batch in &results {
        writer.write(batch)?;
    }
}
let csv = String::from_utf8(buf)?;
```

## Arrow Flight (Network)

Use Arrow Flight for high-performance network data transfer between Query Engine instances:

### Start Server

```rust
use query_flight::FlightServer;
use query_core::Schema;
use std::net::SocketAddr;

let server = FlightServer::new();

// Register tables
server.service().register_table("users", schema, batches);

// Or register from a single batch
server.service().register_batch("orders", record_batch);

// Start serving
let addr: SocketAddr = "0.0.0.0:50051".parse()?;
server.serve(addr).await?;
```

### Client Operations

```rust
use query_flight::FlightClient;

let mut client = FlightClient::connect("http://localhost:50051").await?;

// Execute SQL query
let results = client.execute_sql("SELECT * FROM users").await?;

// List available tables
let tables = client.list_tables().await?;

// Get table schema
let schema = client.get_table_schema("users").await?;

// Upload data to server
let rows = client.upload_table("new_table", batches).await?;

// Bidirectional exchange
let response = client.exchange(Some("stored_name"), batches).await?;
```

### Flight as DataSource

```rust
use query_flight::FlightDataSource;
use query_executor::physical_plan::DataSource;

// Create with known schema
let source = FlightDataSource::new("http://localhost:50051", "users", schema);
let batches = source.scan()?;

// Or connect and auto-fetch schema
let source = FlightDataSource::connect("http://localhost:50051", "users").await?;
```

### Flight Streaming

```rust
use query_flight::FlightStreamSource;
use query_streaming::StreamSource;

let mut stream = FlightStreamSource::new("http://localhost:50051", "users");
while let Some(batch) = stream.next_batch().await {
    process(batch?);
}
```

## Distributed Execution

```rust
use query_distributed::{Coordinator, DistributedExecutor, ExecutorConfig};
use std::sync::Arc;

// Setup coordinator
let coordinator = Arc::new(Coordinator::default());
coordinator.register_worker("localhost:50051")?;
coordinator.register_worker("localhost:50052")?;

// Configure executor
let config = ExecutorConfig {
    max_concurrent_queries: 10,
    task_timeout_ms: 60000,
    batch_size: 4096,
    adaptive_execution: false,
};

let executor = DistributedExecutor::with_config(
    Arc::clone(&coordinator),
    config
);

// Execute distributed query
let results = executor.execute(&plan).await?;

// Check status
let status = coordinator.cluster_status();
println!("Active workers: {}", status.active_workers);
```

## Error Handling

```rust
use query_core::{QueryError, Result};

fn execute_query(sql: &str) -> Result<Vec<RecordBatch>> {
    let mut parser = Parser::new(sql).map_err(|e| {
        QueryError::ParseError(format!("Invalid SQL: {}", e))
    })?;
    
    let statement = parser.parse()?;
    
    // ... rest of execution
}

// Handle errors
match execute_query("SELECT * FROM users") {
    Ok(results) => println!("Got {} batches", results.len()),
    Err(QueryError::ParseError(msg)) => eprintln!("Parse error: {}", msg),
    Err(QueryError::PlanError(msg)) => eprintln!("Plan error: {}", msg),
    Err(QueryError::ExecutionError(msg)) => eprintln!("Execution error: {}", msg),
    Err(e) => eprintln!("Other error: {}", e),
}
```

## Complete Example

```rust
use query_parser::Parser;
use query_planner::Planner;
use query_executor::QueryExecutor;
use query_storage::CsvDataSource;
use query_index::{IndexManager, IndexKey};
use std::collections::HashMap;

fn run_query_with_index() -> anyhow::Result<()> {
    // Load data
    let employees = CsvDataSource::from_path("employees.csv")?;
    let departments = CsvDataSource::from_path("departments.csv")?;
    
    // Setup indexes
    let index_manager = IndexManager::new();
    index_manager.create_btree_index(
        "idx_dept", "employees", 
        vec!["dept_id".into()], false
    )?;
    
    // Build index
    let emp_batches = employees.scan(None)?;
    let idx = index_manager.get_index("idx_dept").unwrap();
    for batch in &emp_batches {
        let dept_col = batch.column_by_name("dept_id").unwrap();
        // ... populate index
    }
    
    // Parse and plan
    let sql = r#"
        SELECT e.name, d.dept_name, e.salary
        FROM employees e
        JOIN departments d ON e.dept_id = d.id
        WHERE e.salary > 70000
        ORDER BY e.salary DESC
        LIMIT 10
    "#;
    
    let mut parser = Parser::new(sql)?;
    let statement = parser.parse()?;
    
    let mut planner = Planner::new();
    planner.register_table("employees", employees.schema());
    planner.register_table("departments", departments.schema());
    let plan = planner.create_logical_plan(&statement)?;
    
    // Execute
    let executor = QueryExecutor::new();
    let mut tables = HashMap::new();
    tables.insert("employees".to_string(), emp_batches);
    tables.insert("departments".to_string(), departments.scan(None)?);
    
    let results = executor.execute(&plan, &tables)?;
    
    // Output
    for batch in results {
        println!("Result: {} rows", batch.num_rows());
    }
    
    Ok(())
}
```
