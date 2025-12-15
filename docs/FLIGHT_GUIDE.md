# Arrow Flight Guide

Apache Arrow Flight implementation for Query Engine, enabling high-performance network data transfer.

## Overview

The `query-flight` crate provides:
- **FlightServer**: Host tables via gRPC Flight protocol
- **FlightClient**: Connect to Flight servers and execute queries
- **FlightDataSource**: Use remote Flight servers as a DataSource
- **FlightStreamSource**: Stream data from Flight servers

## Quick Start

```rust
use query_flight::{FlightClient, FlightServer};
use std::net::SocketAddr;

// Start server
let server = FlightServer::new();
server.service().register_batch("users", batch);
server.serve("0.0.0.0:50051".parse()?).await?;

// Connect client
let mut client = FlightClient::connect("http://localhost:50051").await?;
let results = client.execute_sql("SELECT * FROM users").await?;
```

## Server API

| Method | Description |
|--------|-------------|
| `FlightServer::new()` | Create new server |
| `register_table(name, schema, batches)` | Register a table |
| `register_batch(name, batch)` | Register from RecordBatch |
| `serve(addr)` | Start gRPC server |

## Client API

| Method | Description |
|--------|-------------|
| `FlightClient::connect(url)` | Connect to server |
| `execute_sql(query)` | Execute query, return batches |
| `list_tables()` | List available tables |
| `get_table_schema(name)` | Get table schema |
| `list_flights()` | List all flights |

## Data Sources

### FlightDataSource

Use a remote Flight server as a DataSource for query execution:

```rust
use query_flight::FlightDataSource;
use query_executor::physical_plan::DataSource;

// Create with known schema
let source = FlightDataSource::new("http://localhost:50051", "users", schema);
let batches = source.scan()?;

// Or connect and auto-fetch schema
let source = FlightDataSource::connect("http://localhost:50051", "users").await?;
```

### FlightStreamSource

Stream data from a remote Flight server:

```rust
use query_flight::FlightStreamSource;
use query_streaming::StreamSource;

let mut stream = FlightStreamSource::new("http://localhost:50051", "users");

while let Some(batch) = stream.next_batch().await {
    let batch = batch?;
    // Process batch
}
```

## Flight Protocol Methods

| Method | Status | Description |
|--------|--------|-------------|
| `do_get` | ✅ | Execute query and stream results |
| `list_flights` | ✅ | List tables |
| `get_flight_info` | ✅ | Get query/table schema |
| `get_schema` | ✅ | Get table schema |
| `do_action` | ✅ | clear_tables, list_tables |
| `handshake` | ✅ | No-op (future auth) |
| `do_put` | 🚧 | Partial (acknowledges only) |
| `do_exchange` | ❌ | Not supported |

## Running the Example

```bash
cargo run --example flight_query
```

## CLI Commands

The Query Engine CLI now supports Flight:

### Start Flight Server

```bash
# Start server on default port
qe flight-server

# Load CSV files as tables
qe flight-server --load users=data/users.csv --load orders=data/orders.csv

# Custom host/port
qe flight-server --host 127.0.0.1 --port 8080
```

### Query Remote Server

```bash
# Query a table
qe flight-query --connect http://localhost:50051 --sql "SELECT * FROM users"

# Output as JSON
qe flight-query -c http://localhost:50051 -s "users" -o json

# Output as CSV
qe flight-query -c http://localhost:50051 -s "users" -o csv
```

## Current Limitations

- Query execution limited to table scans (`SELECT * FROM table`)
- Complex SQL (JOINs, aggregations) requires full query planning (future work)
- `do_put` doesn't decode incoming data yet

## Dependencies

```toml
[dependencies]
query-flight = { path = "../crates/query-flight" }
```
