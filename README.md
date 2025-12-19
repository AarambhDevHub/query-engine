# ⚡ Query Engine

[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)]()

A high-performance, production-ready SQL query engine built in Rust with Apache Arrow for vectorized execution.

## 🚀 Features

- **⚡ High Performance**: Vectorized execution using Apache Arrow for maximum throughput
- **🔍 SQL Support**: Comprehensive SQL syntax including SELECT, WHERE, GROUP BY, ORDER BY, LIMIT, and JOINs
- **✏️ Write Operations**: CREATE TABLE, INSERT, UPDATE, DELETE with in-memory storage
- **🔒 TLS/SSL**: Encrypted PostgreSQL connections with certificate-based security
- **🔐 Authentication**: MD5 password authentication for secure access
- **🔗 JOIN Operations**: Full support for INNER, LEFT, RIGHT, FULL OUTER, and CROSS JOINs with table aliases
- **📦 Subqueries & CTEs**: Common Table Expressions (WITH), scalar subqueries, IN/EXISTS subqueries, derived tables
- **📈 Window Functions**: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD with PARTITION BY and ORDER BY
- **🔧 Scalar Functions**: Built-in UDFs: UPPER, LOWER, LENGTH, CONCAT, ABS, ROUND, SQRT, etc.
- **📊 Aggregate Functions**: COUNT, SUM, AVG, MIN, MAX with GROUP BY support
- **🗂️ Index Support**: B-Tree and Hash indexes for fast data retrieval with CREATE/DROP INDEX syntax
- **🌐 Distributed Execution**: Coordinator/Worker architecture with partitioning and fault tolerance
- **🐘 PostgreSQL Protocol**: Connect with psql, pgAdmin, DBeaver, and other PostgreSQL clients
- **📁 Multiple Data Sources**: CSV, Parquet, and in-memory tables
- **🎯 Query Optimization**: Predicate pushdown and logical plan optimization
- **🚀 Query Caching**: LRU cache with TTL for repeated queries
- **💻 Interactive CLI**: Full-featured REPL with syntax highlighting and history
- **🏗️ Modular Architecture**: Clean workspace structure with separated concerns
- **🔧 Production Ready**: Optimized compilation, comprehensive error handling


## 📋 Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [CLI Usage](#cli-usage)
- [SQL Features](#sql-features)
- [Examples](#examples)
- [Performance](#performance)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

## 🔧 Installation

### Prerequisites

- Rust 1.70 or higher
- Cargo (comes with Rust)

### From Source

```
# Clone the repository
git clone https://github.com/AarambhDevHub/query-engine.git
cd query-engine

# Build release version
cargo build --release

# Install CLI globally
cargo install --path crates/query-cli

# Run the CLI
qe
```

### Quick Build

```
# Development build
cargo build

# Optimized release build
cargo build --release

# Run tests
cargo test

# Run benchmarks
cargo bench
```

## 🎯 Quick Start

### Interactive REPL

```
# Start the REPL
qe

# Or with database path
qe repl --db-path ./mydata
```

### Execute Single Query

```
# Query a CSV file
qe query \
  --sql "SELECT name, age FROM users WHERE age > 25" \
  --table users \
  --file data/users.csv

# Query with JOIN
qe query \
  --sql "SELECT e.name, d.dept_name FROM employees e JOIN departments d ON e.dept_id = d.id" \
  --table employees \
  --file data/employees.csv
```

### Load and Query Data

```
# Inside REPL
qe> .load csv data/employees.csv employees
qe> .load csv data/departments.csv departments
qe> SELECT e.name, d.dept_name FROM employees e JOIN departments d ON e.dept_id = d.dept_id;
qe> .describe employees
```

## 🏛️ Architecture

Query Engine follows a modular, layered architecture:

```
┌─────────────────────────────────────────────────────────────┐
│                        CLI / REPL                           │
│                     (User Interface)                        │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                      Query Executor                         │
│              (Physical Plan Execution)                      │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                     Query Planner                           │
│          (Logical Plan + Optimization)                      │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                      Query Parser                           │
│                  (SQL → AST)                               │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                     Data Sources                            │
│          (CSV, Parquet, Memory)                            │
└─────────────────────────────────────────────────────────────┘
```

### Workspace Structure

```
query-engine/
├── crates/
│   ├── query-core/         # Core types and errors
│   ├── query-parser/       # SQL lexer and parser
│   ├── query-planner/      # Logical planning and optimization
│   ├── query-executor/     # Physical execution engine
│   ├── query-storage/      # Data source implementations
│   ├── query-index/        # B-Tree and Hash index support
│   ├── query-distributed/  # Distributed execution framework
│   ├── query-pgwire/       # PostgreSQL wire protocol
│   └── query-cli/          # Command-line interface
├── examples-package/       # Usage examples
└── Cargo.toml             # Workspace configuration
```

## 💻 CLI Usage


### REPL Commands

```
qe>                         # Interactive prompt

# Data Management
.load csv <path> [name]     # Load CSV file
.load parquet <path> [name] # Load Parquet file
.tables                     # List all tables
.describe <table>           # Show table schema
.schema <table>             # Show CREATE TABLE statement
.drop <table>               # Remove table

# Configuration
.timing                     # Toggle query timing
.plan                       # Toggle query plan display
.format <type>              # Set output format (table|json|csv)

# Utilities
.help                       # Show help
.clear                      # Clear screen
.quit                       # Exit REPL
```

### Command-Line Options

```
# Query execution
qe query -s "SELECT * FROM users" -t users -f data.csv

# JOIN query
qe query -s "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id" -t orders -f orders.csv

# Table registration
qe register -n users -f data/users.csv -t csv

# Benchmarking
qe bench -q queries/complex.sql -i 1000

# Export results
qe export \
  -s "SELECT * FROM sales WHERE amount > 1000" \
  -t sales \
  -i data/sales.csv \
  -o results.parquet \
  -f parquet

# Start PostgreSQL server
qe pg-server --port 5432 --load users=data/users.csv

# Show help
qe --help
```

### PostgreSQL Server

Connect with standard PostgreSQL clients:

```bash
# Start the server
qe pg-server --port 5432 --load users=data/users.csv --load orders=data/orders.csv

# Connect with psql
psql -h localhost -p 5432

# Connect with other clients (pgAdmin, DBeaver, etc.)
# Host: localhost, Port: 5432
```

## 📚 SQL Features

### Supported Syntax

```
-- Basic SELECT
SELECT column1, column2 FROM table_name;

-- WHERE clause
SELECT * FROM users WHERE age > 25 AND status = 'active';

-- JOIN operations (NEW!)
SELECT e.name, e.salary, d.dept_name
FROM employees e
INNER JOIN departments d ON e.dept_id = d.dept_id;

-- LEFT JOIN
SELECT u.name, o.order_id
FROM users u
LEFT JOIN orders o ON u.id = o.user_id;

-- CROSS JOIN
SELECT p.name, c.category
FROM products p
CROSS JOIN categories c;

-- Multiple JOINs
SELECT e.name, d.dept_name, l.location
FROM employees e
JOIN departments d ON e.dept_id = d.id
JOIN locations l ON d.location_id = l.id;

-- Aggregate functions
SELECT department, COUNT(*), AVG(salary)
FROM employees
GROUP BY department
HAVING AVG(salary) > 50000;

-- ORDER BY and LIMIT
SELECT name, salary
FROM employees
ORDER BY salary DESC
LIMIT 10;

-- Complex query with JOINs and aggregates
SELECT
  d.dept_name,
  COUNT(e.id) as employee_count,
  AVG(e.salary) as avg_salary
FROM departments d
LEFT JOIN employees e ON d.dept_id = e.dept_id
GROUP BY d.dept_name
ORDER BY avg_salary DESC;
```

### JOIN Types Supported

- **INNER JOIN**: Returns matching rows from both tables
- **LEFT JOIN** (LEFT OUTER JOIN): Returns all rows from left table with matching rows from right
- **RIGHT JOIN** (RIGHT OUTER JOIN): Returns all rows from right table with matching rows from left
- **FULL JOIN** (FULL OUTER JOIN): Returns all rows when there's a match in either table
- **CROSS JOIN**: Returns Cartesian product of both tables

### Common Table Expressions (CTEs)

```sql
-- Simple CTE
WITH high_earners AS (
  SELECT name, salary, dept_id FROM employees WHERE salary > 80000
)
SELECT h.name, d.dept_name
FROM high_earners h
JOIN departments d ON h.dept_id = d.dept_id;

-- Multiple CTEs
WITH
  dept_stats AS (
    SELECT dept_id, AVG(salary) as avg_sal FROM employees GROUP BY dept_id
  ),
  big_depts AS (
    SELECT dept_id FROM dept_stats WHERE avg_sal > 70000
  )
SELECT d.dept_name FROM departments d JOIN big_depts b ON d.dept_id = b.dept_id;
```

### Subqueries

```sql
-- Subquery in FROM clause (derived table)
SELECT sub.name, sub.total
FROM (SELECT name, SUM(amount) as total FROM sales GROUP BY name) AS sub
WHERE sub.total > 1000;

-- Scalar subquery in SELECT
SELECT name, salary, (SELECT AVG(salary) FROM employees) as company_avg
FROM employees;

-- IN subquery
SELECT name FROM employees
WHERE dept_id IN (SELECT dept_id FROM departments WHERE location = 'NYC');

-- EXISTS subquery
SELECT d.dept_name FROM departments d
WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.dept_id);
```


### Supported Operators

- **Arithmetic**: `+`, `-`, `*`, `/`, `%`
- **Comparison**: `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`
- **Logical**: `AND`, `OR`, `NOT`
- **Functions**: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`

### Data Types

- **Integer**: `INT8`, `INT16`, `INT32`, `INT64`
- **Unsigned**: `UINT8`, `UINT16`, `UINT32`, `UINT64`
- **Float**: `FLOAT32`, `FLOAT64`
- **String**: `UTF8`, `BINARY`
- **Date/Time**: `DATE32`, `DATE64`, `TIMESTAMP`
- **Boolean**: `BOOLEAN`
- **Null**: `NULL`

### Index Support

Query Engine supports indexes for fast data retrieval:

```sql
-- Create a B-Tree index (good for range queries)
CREATE INDEX idx_salary ON employees(salary);

-- Create a Hash index (fast O(1) lookups)
CREATE INDEX idx_email ON users(email) USING HASH;

-- Create a unique index
CREATE UNIQUE INDEX idx_emp_id ON employees(employee_id);

-- Create a multi-column index
CREATE INDEX idx_name_dept ON employees(name, dept_id);

-- Drop an index
DROP INDEX idx_salary ON employees;
```

**Index Types:**

| Type | Use Case | Performance |
|------|----------|-------------|
| **B-Tree** | Range queries, ORDER BY, equality | O(log n) |
| **Hash** | Equality lookups only | O(1) average |

**Programmatic Usage:**

```rust
use query_index::{BTreeIndex, HashIndex, IndexManager};

// Create index manager
let mut manager = IndexManager::new();

// Create and build a B-Tree index
let btree = BTreeIndex::new("idx_salary");
btree.build(&salary_column)?;

// Create and build a Hash index
let hash = HashIndex::new("idx_email");
hash.build(&email_column)?;

// Lookup values
let row_ids = btree.range_scan(&start_value, &end_value)?;
let row_ids = hash.lookup(&value)?;
```

### Distributed Execution

Query Engine supports distributed query execution across multiple workers:

```rust
use query_distributed::{
    Coordinator, Worker, DistributedExecutor,
    Partitioner, PartitionStrategy, FaultManager
};
use std::sync::Arc;

// Create coordinator node
let coordinator = Arc::new(Coordinator::default());

// Register worker nodes
coordinator.register_worker("worker1:50051")?;
coordinator.register_worker("worker2:50051")?;
coordinator.register_worker("worker3:50051")?;

// Check cluster status
let status = coordinator.cluster_status();
println!("Active workers: {}", status.active_workers);

// Create distributed executor
let executor = DistributedExecutor::new(Arc::clone(&coordinator));

// Execute distributed query
let results = executor.execute(&logical_plan).await?;
```

**Partitioning Strategies:**

```rust
// Hash partitioning (for joins and aggregations)
let partitioner = Partitioner::hash(vec!["customer_id".into()], 4);

// Range partitioning (for sorted data)
let partitioner = Partitioner::range("date", vec![
    RangeBoundary::Int64(1000),
    RangeBoundary::Int64(2000),
]);

// Round-robin (even distribution)
let partitioner = Partitioner::round_robin(3);

// Partition data
let partitions = partitioner.partition(&record_batches)?;
```

**Architecture:**

```
┌─────────────────────────────────────────────────────────────┐
│                      Coordinator                            │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐       │
│  │DistPlanner   │ │TaskScheduler │ │FaultManager  │       │
│  └──────────────┘ └──────────────┘ └──────────────┘       │
└─────────────────────────────────────────────────────────────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │ Worker 1 │ │ Worker 2 │ │ Worker 3 │
        │Partition0│ │Partition1│ │Partition2│
        └──────────┘ └──────────┘ └──────────┘
```

**Fault Tolerance:**

```rust
// Configure fault tolerance
let fault_config = FaultConfig {
    max_task_retries: 3,
    worker_failure_threshold: 3,
    enable_checkpoints: true,
    ..Default::default()
};

let fault_manager = FaultManager::new(fault_config);

// Handle task failures with automatic retry
let action = fault_manager.handle_task_failure(task, worker_id, error);
match action {
    TaskRecoveryAction::Retry { delay_ms } => { /* retry after delay */ }
    TaskRecoveryAction::Fail { reason } => { /* abort query */ }
}
```

## 📖 Examples


### Example 1: Basic Queries

```
use query_engine::*;

#[tokio::main]
async fn main() -> Result<()> {
    // Create schema
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, false),
    ]);

    // Create sample data
    let batch = RecordBatch::try_new(
        Arc::new(schema.to_arrow()),
        vec![
            Arc::new(Int64Array::from(vec!)),[1][2][3][4][5]
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "Diana", "Eve"])),
            Arc::new(Int64Array::from(vec!)),
        ],
    )?;

    // Parse and execute query
    let sql = "SELECT name, age FROM users WHERE age > 28";
    let mut parser = Parser::new(sql)?;
    let statement = parser.parse()?;

    // Create logical plan
    let mut planner = Planner::new();
    planner.register_table("users", schema);
    let plan = planner.create_logical_plan(&statement)?;

    println!("Query Plan: {:#?}", plan);
    Ok(())
}
```

### Example 2: JOIN Queries

```
-- Load employee and department data
.load csv data/employees.csv employees
.load csv data/departments.csv departments

-- Simple INNER JOIN
SELECT e.name, e.salary, d.dept_name
FROM employees e
INNER JOIN departments d ON e.dept_id = d.dept_id;

-- LEFT JOIN to include all employees
SELECT e.name, e.salary, d.dept_name
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.dept_id;

-- JOIN with WHERE clause
SELECT e.name, d.dept_name, d.location
FROM employees e
JOIN departments d ON e.dept_id = d.dept_id
WHERE d.location = 'Building A' AND e.salary > 70000;

-- JOIN with aggregates
SELECT d.dept_name,
       COUNT(e.id) as employee_count,
       AVG(e.salary) as avg_salary
FROM departments d
LEFT JOIN employees e ON d.dept_id = e.dept_id
GROUP BY d.dept_name
ORDER BY avg_salary DESC;
```

### Example 3: Aggregate Queries

```
-- Load data
.load csv data/sales.csv sales

-- Total sales by region
SELECT region, SUM(amount) as total
FROM sales
GROUP BY region
ORDER BY total DESC;

-- Monthly statistics
SELECT
  month,
  COUNT(*) as orders,
  SUM(amount) as revenue,
  AVG(amount) as avg_order
FROM sales
GROUP BY month;
```

### Example 4: Complex Analytics

```
-- Top performing products
SELECT
  product_name,
  COUNT(DISTINCT customer_id) as unique_customers,
  SUM(quantity) as total_units,
  SUM(amount) as total_revenue,
  AVG(amount) as avg_price
FROM sales
GROUP BY product_name
HAVING total_revenue > 10000
ORDER BY total_revenue DESC
LIMIT 10;
```

### Example 5: Using the CLI

```
# Start REPL and load data
$ qe

qe> .load csv data/employees.csv employees
✓ Loaded table 'employees' from data/employees.csv (1000 rows, 5 columns)

qe> .load csv data/departments.csv departments
✓ Loaded table 'departments' from data/departments.csv (10 rows, 4 columns)

qe> .describe employees
Table: employees
Source: CSV file: data/employees.csv
Rows: 1000

┌───────────────┬──────────┬──────────┐
│ Column        │ Type     │ Nullable │
├───────────────┼──────────┼──────────┤
│ employee_id   │ Int64    │ NO       │
│ name          │ Utf8     │ NO       │
│ department_id │ Int64    │ NO       │
│ salary        │ Float64  │ NO       │
│ hire_date     │ Date32   │ YES      │
└───────────────┴──────────┴──────────┘

qe> SELECT e.name, e.salary, d.dept_name
    FROM employees e
    JOIN departments d ON e.department_id = d.dept_id
    WHERE e.salary > 80000;
✓ Query parsed and planned successfully!
Planning time: 0.05ms
```

## ⚡ Performance

### Optimization Features

- **Vectorized Execution**: SIMD operations via Apache Arrow
- **Predicate Pushdown**: Filter data early in the pipeline
- **Projection Pushdown**: Read only required columns
- **JOIN Optimization**: Efficient hash-based JOIN implementation
- **LTO Compilation**: Link-time optimization for release builds
- **Zero-Copy Operations**: Minimize memory allocations

### Benchmark Results

```
# Run benchmarks
cargo bench

# Custom benchmark
qe bench -q queries/complex.sql -i 1000

# JOIN benchmark
cargo run --example join_query
```

**Sample Results** (your results may vary):

```
Benchmark Results:
──────────────────────────────────────────────────
  Query Type:      INNER JOIN
  Iterations:      1000
  Total time:      1.23s
  Average:         1.23ms
  Median:          1.18ms
  Min:             0.98ms
  Max:             3.45ms
  95th percentile: 1.67ms
  99th percentile: 2.34ms
  QPS:             813.01
──────────────────────────────────────────────────
```

### Release Profile Optimizations

```
[profile.release]
opt-level = 3           # Maximum optimization
lto = "fat"             # Full link-time optimization
codegen-units = 1       # Better optimization
panic = "abort"         # Smaller binary
strip = true            # Remove debug symbols
```

## 🛠️ Development

### Building

```
# Debug build
cargo build

# Release build with optimizations
cargo build --release

# Build specific crate
cargo build -p query-parser

# Build CLI
cargo build --release -p query-cli
```

### Testing

```
# Run all tests
cargo test

# Run tests with output
cargo test -- --nocapture

# Run specific test
cargo test test_join_queries

# Run tests in specific crate
cargo test -p query-parser
```

### Running Examples

```bash
# Simple query example
cargo run --example simple_query

# Aggregate query example
cargo run --example aggregate_query

# JOIN query example
cargo run --example join_query

# Window function example
cargo run --example window_function_query

# User-defined functions example
cargo run --example udf_query

# Index operations example
cargo run --example index_query

# Distributed execution example
cargo run --example distributed_query

# Full demo
cargo run --example full_query_demo
```

### Code Quality

```bash
# Format code
cargo fmt

# Run clippy lints
cargo clippy

# Check without building
cargo check
```

## 🔍 Troubleshooting

### Common Issues

**Issue**: `File not found` error
```
# Solution: Use absolute path or relative to current directory
qe> .load csv ./data/users.csv users
```

**Issue**: `Column not found` error
```
-- Solution: Check table schema
qe> .describe users
```

**Issue**: `Failed to create logical plan` for CROSS JOIN
```
-- Solution: Make sure table aliases are used correctly
-- Good:
SELECT e.name, d.dept_name FROM employees e CROSS JOIN departments d;

-- Bad:
SELECT name, dept_name FROM employees CROSS JOIN departments;
```

**Issue**: Slow query performance
```
# Solution: Enable query plan to see optimization
qe> .plan
qe> SELECT * FROM large_table WHERE condition;
```

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines

- Write tests for new features
- Follow Rust naming conventions
- Update documentation
- Run `cargo fmt` and `cargo clippy`
- Ensure all tests pass

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [Apache Arrow](https://arrow.apache.org/) - Columnar in-memory data format
- [Apache Parquet](https://parquet.apache.org/) - Columnar storage format
- [DataFusion](https://github.com/apache/arrow-datafusion) - Inspiration for query engine design
- Rust Community - For excellent tooling and libraries

## 📞 Contact

- **Author**: Darshan Vichhi (Aarambh)
- **GitHub**: [@AarambhDevHub](https://github.com/AarambhDevHub)
- **Issues**: [GitHub Issues](https://github.com/AarambhDevHub/query-engine/issues)

## 🗺️ Roadmap

- [x] ~~JOIN operations (INNER, LEFT, RIGHT, FULL, CROSS)~~ ✅ **Completed!**
- [x] ~~Subqueries and CTEs~~ ✅ **Completed!**
- [x] ~~Window functions~~ ✅ **Completed!**
- [x] ~~User-defined functions (UDFs)~~ ✅ **Completed!**
- [x] ~~Index support~~ ✅ **Completed!**
- [x] ~~Distributed execution~~ ✅ **Completed!**
- [x] ~~Query caching~~ ✅ **Completed!**
- [x] ~~Real-time streaming queries~~ ✅ **Completed!**
- [x] ~~Arrow Flight (network data transfer)~~ ✅ **Completed!**
- [ ] PostgreSQL protocol compatibility
- [ ] Web UI dashboard
- [ ] Cost-based query optimizer

## 📊 Project Status

**Version**: 0.1.0
**Status**: Active Development
**Stability**: Alpha

### Recently Completed
- ✅ **Arrow Flight** (NEW!)
  - FlightServer for hosting tables via gRPC
  - FlightClient for remote SQL execution
  - All Flight protocol methods (do_get, do_put, do_exchange, etc.)
  - FlightDataSource and FlightStreamSource implementations
  - CLI commands: `flight-server` and `flight-query`
- ✅ **Real-time Streaming**
  - StreamSource trait for async data streams
  - Tumbling, Sliding, and Session windows
  - Event-time processing with watermarks
  - Late event handling policies
- ✅ **Query Caching**
  - LRU-based result caching
  - TTL expiration support
  - Memory limits configuration
- ✅ **Distributed Execution**
  - Coordinator/Worker architecture
  - Hash, Range, and Round-Robin partitioning strategies
  - Task scheduling and load balancing
  - Multi-stage query execution plans
  - Exchange and Merge operators for shuffles
  - Fault tolerance with retry and checkpointing
- ✅ **Index Support**
  - B-Tree indexes for range queries and equality
  - Hash indexes for fast O(1) equality lookups
  - `CREATE INDEX` and `DROP INDEX` SQL syntax
- ✅ **Scalar Functions (UDFs)**
  - String: UPPER, LOWER, LENGTH, CONCAT, SUBSTRING, TRIM, REPLACE
  - Math: ABS, CEIL, FLOOR, ROUND, SQRT, POWER
- ✅ **Window Functions**
  - ROW_NUMBER, RANK, DENSE_RANK, NTILE
  - LAG, LEAD, FIRST_VALUE, LAST_VALUE
- ✅ **Subqueries and CTEs**
- ✅ Full JOIN support (INNER, LEFT, RIGHT, FULL OUTER, CROSS)



---

**Built with ❤️ in Rust**

[![Star on GitHub](https://img.shields.io/github/stars/AarambhDevHub/query-engine?style=social)](https://github.com/AarambhDevHub/query-engine)
