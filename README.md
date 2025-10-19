# ⚡ Query Engine

[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)]()

A high-performance, production-ready SQL query engine built in Rust with Apache Arrow for vectorized execution.

## 🚀 Features

- **⚡ High Performance**: Vectorized execution using Apache Arrow for maximum throughput
- **🔍 SQL Support**: Comprehensive SQL syntax including SELECT, WHERE, GROUP BY, ORDER BY, LIMIT
- **📊 Aggregate Functions**: COUNT, SUM, AVG, MIN, MAX with GROUP BY support
- **📁 Multiple Data Sources**: CSV, Parquet, and in-memory tables
- **🎯 Query Optimization**: Predicate pushdown and logical plan optimization
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

# Query a Parquet file
qe query \
  --sql "SELECT department, SUM(salary) FROM employees GROUP BY department" \
  --table employees \
  --file data/employees.parquet \
  --output table
```

### Load and Query Data

```
# Inside REPL
qe> .load csv data/users.csv users
qe> SELECT name, age FROM users WHERE age > 25 ORDER BY age DESC;
qe> .describe users
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

# Show help
qe --help
```

## 📚 SQL Features

### Supported Syntax

```
-- Basic SELECT
SELECT column1, column2 FROM table_name;

-- WHERE clause
SELECT * FROM users WHERE age > 25 AND status = 'active';

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

-- Multiple aggregates
SELECT
  region,
  COUNT(*) as total_orders,
  SUM(amount) as total_sales,
  AVG(amount) as avg_sale
FROM sales
GROUP BY region
ORDER BY total_sales DESC;
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

### Example 2: Aggregate Queries

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

### Example 3: Complex Analytics

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

### Example 4: Using the CLI

```
# Start REPL and load data
$ qe

qe> .load csv data/employees.csv employees
✓ Loaded table 'employees' from data/employees.csv (1000 rows, 5 columns)

qe> .describe employees
Table: employees
Source: CSV file: data/employees.csv
Rows: 1000

┌───────────────┬──────────┬──────────┐
│ Column        │ Type     │ Nullable │
├───────────────┼──────────┼──────────┤
│ employee_id   │ Int64    │ NO       │
│ name          │ Utf8     │ NO       │
│ department    │ Utf8     │ NO       │
│ salary        │ Float64  │ NO       │
│ hire_date     │ Date32   │ YES      │
└───────────────┴──────────┴──────────┘

qe> SELECT department, AVG(salary) FROM employees GROUP BY department;
✓ Query parsed and planned successfully!
Planning time: 2.45ms
```

## ⚡ Performance

### Optimization Features

- **Vectorized Execution**: SIMD operations via Apache Arrow
- **Predicate Pushdown**: Filter data early in the pipeline
- **Projection Pushdown**: Read only required columns
- **LTO Compilation**: Link-time optimization for release builds
- **Zero-Copy Operations**: Minimize memory allocations

### Benchmark Results

```
# Run benchmarks
cargo bench

# Custom benchmark
qe bench -q queries/complex.sql -i 1000
```

**Sample Results** (your results may vary):

```
Benchmark Results:
──────────────────────────────────────────────────
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
cargo test test_name

# Run tests in specific crate
cargo test -p query-parser
```

### Running Examples

```
# Simple query example
cargo run --example simple_query

# Aggregate query example
cargo run --example aggregate_query

# Full demo
cargo run --example full_query_demo
```

### Code Quality

```
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

- **Author**: Darshan vichhi (Aarambh)
- **GitHub**: [@AarambhDevHub](https://github.com/AarambhDevHub)
- **Issues**: [GitHub Issues](https://github.com/AarambhDevHub/query-engine/issues)

## 🗺️ Roadmap

- [ ] JOIN operations (INNER, LEFT, RIGHT, FULL)
- [ ] Subqueries and CTEs
- [ ] Window functions
- [ ] User-defined functions (UDFs)
- [ ] Index support
- [ ] Distributed execution
- [ ] Query caching
- [ ] Real-time streaming queries
- [ ] PostgreSQL protocol compatibility
- [ ] Web UI dashboard

## 📊 Project Status

**Version**: 0.1.0
**Status**: Active Development
**Stability**: Alpha

---

**Built with ❤️ in Rust**

[![Star on GitHub](https://img.shields.io/github/stars/AarambhDevHub/query-engine?style=social)](https://github.com/AarambhDevHub/query-engine)
