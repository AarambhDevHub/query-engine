# Crate Reference

Detailed reference for each crate in the Query Engine workspace.

## Overview

```
query-engine workspace
├── query-core        # Foundation types and errors
├── query-parser      # SQL lexer and parser
├── query-planner     # Logical plan and optimization
├── query-executor    # Physical execution
├── query-storage     # Data sources
├── query-index       # Index implementations
├── query-cache       # Query result caching
├── query-streaming   # Real-time stream processing
├── query-distributed # Distributed execution
├── query-flight      # Arrow Flight server/client
├── query-pgwire      # PostgreSQL wire protocol
└── query-cli         # CLI interface
```

---

## query-core

**Purpose:** Foundation types, errors, and utilities used across all crates.

### Key Types

```rust
use query_core::{Schema, Field, DataType, QueryError, Result, ScalarValue};
```

#### Schema

Represents table structure:

```rust
let schema = Schema::new(vec![
    Field::new("id", DataType::Int64, false),
    Field::new("name", DataType::Utf8, false),
    Field::new("salary", DataType::Float64, true),
]);

// Access fields
let field = schema.field_by_name("id");
let fields = schema.fields();
```

#### DataType

Supported data types:

```rust
pub enum DataType {
    Int8, Int16, Int32, Int64,
    UInt8, UInt16, UInt32, UInt64,
    Float32, Float64,
    Utf8, LargeUtf8,
    Boolean,
    Date32, Timestamp,
    Binary,
    List(Box<DataType>),
}
```

#### QueryError

Unified error type:

```rust
pub enum QueryError {
    ParseError(String),
    PlanError(String),
    ExecutionError(String),
    IoError(std::io::Error),
    ArrowError(arrow::error::ArrowError),
}
```

---

## query-parser

**Purpose:** Parse SQL text into Abstract Syntax Trees (AST).

### Key Types

```rust
use query_parser::{Parser, Lexer, Token, Statement, Expression};
```

#### Parser

```rust
// Parse SQL
let sql = "SELECT name, salary FROM employees WHERE salary > 50000";
let mut parser = Parser::new(sql)?;
let statement = parser.parse()?;

// Match statement type
match statement {
    Statement::Select(select) => { /* handle SELECT */ }
    Statement::CreateTable(ct) => { /* handle CREATE TABLE */ }
    Statement::CreateIndex(ci) => { /* handle CREATE INDEX */ }
    Statement::Insert(ins) => { /* handle INSERT */ }
    _ => {}
}
```

#### Statement Types

```rust
pub enum Statement {
    Select(SelectStatement),
    CreateTable(CreateTableStatement),
    CreateIndex(CreateIndexStatement),
    DropTable(DropTableStatement),
    DropIndex(DropIndexStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
}
```

#### Expression Types

```rust
pub enum Expression {
    Literal(Literal),
    Identifier(String),
    QualifiedIdentifier { table: String, column: String },
    BinaryOp { left: Box<Expression>, op: BinaryOperator, right: Box<Expression> },
    UnaryOp { op: UnaryOperator, expr: Box<Expression> },
    Function { name: String, args: Vec<Expression> },
    Aggregate { function: AggregateFunction, expr: Box<Expression>, distinct: bool },
    Subquery(Box<SelectStatement>),
    Case { ... },
    Cast { expr: Box<Expression>, data_type: DataType },
}
```

---

## query-planner

**Purpose:** Transform AST into logical plans and optimize.

### Key Types

```rust
use query_planner::{Planner, LogicalPlan, Optimizer};
```

#### Planner

```rust
let mut planner = Planner::new();

// Register table schemas
planner.register_table("employees", schema);

// Create logical plan from statement
let plan = planner.create_logical_plan(&statement)?;
```

#### LogicalPlan

```rust
pub enum LogicalPlan {
    TableScan { table_name: String, schema: Schema, projection: Option<Vec<usize>> },
    Projection { input: Box<LogicalPlan>, expressions: Vec<Expression> },
    Filter { input: Box<LogicalPlan>, predicate: Expression },
    Aggregate { input: Box<LogicalPlan>, group_exprs: Vec<Expression>, aggr_exprs: Vec<Expression> },
    Sort { input: Box<LogicalPlan>, sort_exprs: Vec<SortExpr> },
    Limit { input: Box<LogicalPlan>, limit: usize, offset: Option<usize> },
    Join { left: Box<LogicalPlan>, right: Box<LogicalPlan>, on: Expression, join_type: JoinType },
    Union { inputs: Vec<LogicalPlan> },
    Distinct { input: Box<LogicalPlan> },
    Window { input: Box<LogicalPlan>, window_exprs: Vec<WindowExpr> },
}
```

#### Optimizer

```rust
let optimizer = Optimizer::new();
let optimized = optimizer.optimize(&plan)?;
```

---

## query-executor

**Purpose:** Execute logical plans and produce results.

### Key Types

```rust
use query_executor::{QueryExecutor, ExecutionContext};
use arrow::record_batch::RecordBatch;
```

#### QueryExecutor

```rust
let executor = QueryExecutor::new();

// Execute plan with table data
let results: Vec<RecordBatch> = executor.execute(&plan, &tables)?;

// Process results
for batch in results {
    println!("Rows: {}", batch.num_rows());
    for i in 0..batch.num_columns() {
        println!("Column {}: {:?}", i, batch.column(i));
    }
}
```

#### With Configuration

```rust
let context = ExecutionContext::new()
    .with_batch_size(4096)
    .with_memory_limit(1024 * 1024 * 100);  // 100MB

let results = context.execute(&plan, &tables)?;
```

---

## query-storage

**Purpose:** Data source implementations for CSV, Parquet, memory.

### Key Types

```rust
use query_storage::{CsvDataSource, ParquetDataSource, MemoryTable, DataSource};
```

#### CSV Data Source

```rust
// Load CSV
let csv = CsvDataSource::from_path("data/employees.csv")?;
let schema = csv.schema();
let batches = csv.scan(None)?;  // All columns

// With projection
let batches = csv.scan(Some(vec![0, 2]))?;  // Only columns 0 and 2
```

#### Parquet Data Source

```rust
let parquet = ParquetDataSource::from_path("data/sales.parquet")?;
let batches = parquet.scan(None)?;
```

#### Memory Table

```rust
let table = MemoryTable::new(schema, batches);
let data = table.scan(Some(vec![0, 1]))?;
```

---

## query-index

**Purpose:** B-Tree and Hash index implementations.

### Key Types

```rust
use query_index::{
    IndexManager, BTreeIndex, HashIndex, 
    Index, IndexKey, IndexMetadata, IndexType
};
```

#### IndexManager

```rust
let manager = IndexManager::new();

// Create indexes
manager.create_btree_index("idx_id", "users", vec!["id".into()], true)?;
manager.create_hash_index("idx_email", "users", vec!["email".into()], false)?;

// Use indexes
let index = manager.get_index("idx_id").unwrap();
let result = index.lookup(&IndexKey::from_i64(42));

// Find index for column
let index = manager.find_index_for_column("users", "email");
```

#### Direct Index Usage

```rust
// B-Tree (supports range)
let btree = BTreeIndex::with_metadata("idx", "tbl", vec!["col".into()], false);
btree.insert(IndexKey::from_i64(10), 0)?;
let result = btree.range_scan(Some(&start), Some(&end));

// Hash (equality only)
let hash = HashIndex::with_metadata("idx", "tbl", vec!["col".into()], false);
hash.insert(IndexKey::from_string("key"), 0)?;
let result = hash.lookup(&IndexKey::from_string("key"));
```

---

## query-distributed

**Purpose:** Distributed query execution framework.

### Key Types

```rust
use query_distributed::{
    Coordinator, Worker, DistributedExecutor,
    Partitioner, PartitionStrategy, FaultManager
};
```

#### Coordinator

```rust
let coordinator = Coordinator::default();

// Register workers
coordinator.register_worker("worker1:50051")?;
coordinator.register_worker("worker2:50051")?;

// Check cluster
let status = coordinator.cluster_status();
println!("Workers: {}", status.active_workers);
```

#### DistributedExecutor

```rust
let executor = DistributedExecutor::new(Arc::new(coordinator));
let results = executor.execute(&plan).await?;
```

#### Partitioner

```rust
// Hash partitioning
let partitioner = Partitioner::hash(vec!["customer_id".into()], 4);
let partitions = partitioner.partition(&batches)?;

// Round-robin
let partitioner = Partitioner::round_robin(3);
```

---

## query-cache

**Purpose:** LRU-based query result caching with TTL.

### Key Types

```rust
use query_cache::{QueryCache, CacheKey, CacheConfig, CacheStats};
```

#### QueryCache

```rust
// Create cache
let config = CacheConfig::default();
let cache = QueryCache::new(config);

// Cache query result
let key = CacheKey::from_sql("SELECT * FROM users");
cache.put(key.clone(), vec![batch]);

// Retrieve from cache
if let Some(batches) = cache.get(&key) {
    println!("Cache HIT: {} batches", batches.len());
}

// Get statistics
let stats = cache.stats();
println!("Hit rate: {:.1}%", stats.hit_rate() * 100.0);
```

---

## query-streaming

**Purpose:** Real-time stream processing for continuous data.

### Key Types

```rust
use query_streaming::{
    StreamingQuery, StreamConfig, StreamSource,
    ChannelStreamSource, MemoryStreamSource,
    WindowType, Watermark
};
```

#### StreamingQuery

```rust
// Create channel stream
let (tx, source) = ChannelStreamSource::new(100);
let config = StreamConfig::default()
    .with_batch_size(1000)
    .with_window(WindowType::tumbling(Duration::from_secs(60)));

let mut query = StreamingQuery::new(source, config);

// Process stream
while let Some(result) = query.next().await {
    let batch = result?;
    process(batch);
}

// Control
query.pause();
query.resume();
query.stop();
```

#### Window Types

```rust
WindowType::tumbling(Duration::from_secs(60))   // Non-overlapping
WindowType::sliding(Duration::from_secs(60), Duration::from_secs(10))  // Overlapping
WindowType::session(Duration::from_secs(30))    // Gap-based
```

---

## query-flight

**Purpose:** Arrow Flight server and client for network data transfer.

### Key Types

```rust
use query_flight::{
    FlightServer, FlightClient, FlightDataSource, FlightStreamSource
};
```

#### FlightServer

```rust
// Create server
let server = FlightServer::new();

// Register tables
server.service().register_batch("users", batch);

// Start serving
server.serve("0.0.0.0:50051".parse()?).await?;
```

#### FlightClient

```rust
// Connect to server
let mut client = FlightClient::connect("http://localhost:50051").await?;

// Execute query
let batches = client.execute_sql("SELECT * FROM users").await?;

// List tables
let tables = client.list_tables().await?;

// Get schema
let schema = client.get_table_schema("users").await?;
```

#### FlightDataSource

```rust
use query_executor::physical_plan::DataSource;

let source = FlightDataSource::new("http://localhost:50051", "users", schema);
let batches = source.scan()?;
```

#### FlightStreamSource

```rust
use query_streaming::StreamSource;

let mut stream = FlightStreamSource::new("http://localhost:50051", "users");
while let Some(batch) = stream.next_batch().await {
    process(batch?);
}
```

---

## query-pgwire

**Purpose:** PostgreSQL wire protocol for client connectivity.

### Key Types

```rust
use query_pgwire::{PgServer, QueryBackend, QueryServerHandlers};
```

#### PgServer

```rust
// Create and start server
let server = PgServer::new("0.0.0.0", 5432);

// Load CSV data
server.load_csv("data/users.csv", "users").await?;
server.load_csv("data/orders.csv", "orders").await?;

// Start listening
server.start().await?;
```

#### Connect with Clients

```bash
# psql
psql -h localhost -p 5432

# Python
import psycopg2
conn = psycopg2.connect(host="localhost", port=5432)
```

---

## query-cli

**Purpose:** Command-line interface and REPL.

### Usage

```bash
# Start REPL
qe

# Execute query
qe query --sql "SELECT * FROM t" --table t --file data.csv

# Register and query
qe register -n mytable -f data.csv
```

### REPL Commands

```
.load csv <path> <name>   Load CSV file
.tables                    List tables
.describe <table>         Show schema
.timing / .plan           Toggle display options
.quit                     Exit
```

---

## Dependency Graph

```
query-cli
    ├── query-executor
    │   └── query-planner
    │       └── query-parser
    │           └── query-core
    ├── query-storage
    │   └── query-core
    ├── query-distributed
    │   ├── query-executor
    │   └── query-planner
    └── query-index
        └── query-core
```
