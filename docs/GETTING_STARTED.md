# Getting Started Guide

This guide will help you get started with Query Engine quickly.

## Installation

### Prerequisites

- Rust 1.70 or higher
- Cargo (comes with Rust)

### Building from Source

```bash
# Clone the repository
git clone https://github.com/AarambhDevHub/query-engine.git
cd query-engine

# Build release version
cargo build --release

# Install CLI globally
cargo install --path crates/query-cli

# Verify installation
qe --version
```

## Quick Start

### 1. Start the REPL

```bash
qe
```

You'll see the interactive prompt:
```
Query Engine v0.1.0
Type '.help' for help, '.quit' to exit

qe>
```

### 2. Load Data

```bash
qe> .load csv data/employees.csv employees
✓ Loaded table 'employees' from data/employees.csv (100 rows, 5 columns)

qe> .load csv data/departments.csv departments  
✓ Loaded table 'departments' from data/departments.csv (10 rows, 3 columns)
```

### 3. Explore Tables

```bash
qe> .tables
Tables:
  - employees (100 rows)
  - departments (10 rows)

qe> .describe employees
Table: employees
┌───────────┬──────────┬──────────┐
│ Column    │ Type     │ Nullable │
├───────────┼──────────┼──────────┤
│ id        │ Int64    │ NO       │
│ name      │ Utf8     │ NO       │
│ dept_id   │ Int64    │ NO       │
│ salary    │ Float64  │ NO       │
│ hire_date │ Date32   │ YES      │
└───────────┴──────────┴──────────┘
```

### 4. Run Queries

```sql
-- Simple query
qe> SELECT name, salary FROM employees WHERE salary > 75000;

-- Join tables
qe> SELECT e.name, d.dept_name 
    FROM employees e 
    JOIN departments d ON e.dept_id = d.id;

-- Aggregations
qe> SELECT dept_id, COUNT(*), AVG(salary) as avg_sal
    FROM employees
    GROUP BY dept_id
    ORDER BY avg_sal DESC;
```

## Example Queries

### Basic SELECT

```sql
-- All columns
SELECT * FROM employees;

-- Specific columns
SELECT name, salary FROM employees;

-- With alias
SELECT name AS employee_name, salary * 12 AS annual_salary FROM employees;
```

### Filtering

```sql
-- Simple condition
SELECT * FROM employees WHERE salary > 50000;

-- Multiple conditions
SELECT * FROM employees 
WHERE salary > 50000 AND dept_id = 1;

-- BETWEEN
SELECT * FROM employees 
WHERE salary BETWEEN 50000 AND 80000;

-- IN list
SELECT * FROM employees 
WHERE dept_id IN (1, 2, 3);

-- LIKE pattern
SELECT * FROM employees 
WHERE name LIKE 'J%';
```

### Aggregations

```sql
-- Count
SELECT COUNT(*) FROM employees;

-- Group by
SELECT dept_id, AVG(salary) as avg_salary
FROM employees
GROUP BY dept_id;

-- Having
SELECT dept_id, AVG(salary) as avg_salary
FROM employees
GROUP BY dept_id
HAVING AVG(salary) > 60000;
```

### Joins

```sql
-- Inner join
SELECT e.name, d.dept_name
FROM employees e
INNER JOIN departments d ON e.dept_id = d.id;

-- Left join
SELECT e.name, d.dept_name
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.id;

-- Multiple joins
SELECT e.name, d.dept_name, l.city
FROM employees e
JOIN departments d ON e.dept_id = d.id
JOIN locations l ON d.location_id = l.id;
```

### Window Functions

```sql
-- Ranking
SELECT 
    name, 
    salary,
    ROW_NUMBER() OVER (ORDER BY salary DESC) as rank
FROM employees;

-- Partitioned
SELECT 
    name,
    dept_id,
    salary,
    AVG(salary) OVER (PARTITION BY dept_id) as dept_avg
FROM employees;
```

### CTEs

```sql
WITH high_earners AS (
    SELECT * FROM employees WHERE salary > 80000
)
SELECT COUNT(*) FROM high_earners;
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `.load csv <path> <name>` | Load CSV file |
| `.load parquet <path> <name>` | Load Parquet file |
| `.tables` | List all tables |
| `.describe <table>` | Show table schema |
| `.schema <table>` | Show CREATE TABLE |
| `.drop <table>` | Remove table |
| `.timing` | Toggle query timing |
| `.plan` | Toggle plan display |
| `.format <type>` | Set output (table/json/csv) |
| `.help` | Show help |
| `.quit` | Exit REPL |

## Programmatic Usage

### Basic Example

```rust
use query_parser::Parser;
use query_planner::Planner;
use query_executor::QueryExecutor;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse SQL
    let sql = "SELECT name, salary FROM employees WHERE salary > 50000";
    let mut parser = Parser::new(sql)?;
    let statement = parser.parse()?;
    
    // Create plan
    let mut planner = Planner::new();
    planner.register_table("employees", schema);
    let plan = planner.create_logical_plan(&statement)?;
    
    // Execute
    let executor = QueryExecutor::new();
    let results = executor.execute(&plan, &tables)?;
    
    println!("Results: {} rows", results[0].num_rows());
    Ok(())
}
```

### Using Indexes

```rust
use query_index::{IndexManager, IndexKey};

let manager = IndexManager::new();

// Create index
manager.create_btree_index("idx_salary", "employees", vec!["salary".into()], false)?;

// Use index for lookup
let index = manager.get_index("idx_salary").unwrap();
let result = index.lookup(&IndexKey::from_i64(75000));
println!("Found {} rows", result.row_ids.len());
```

## Running Examples

```bash
# Simple query
cargo run --example simple_query

# Join operations
cargo run --example join_query

# Window functions
cargo run --example window_function_query

# Index usage
cargo run --example index_query

# Distributed execution
cargo run --example distributed_query
```

## Next Steps

- Read the [SQL Reference](SQL_REFERENCE.md) for complete syntax
- Explore the [Architecture](ARCHITECTURE.md) to understand internals
- Check out [Distributed Execution](DISTRIBUTED_EXECUTION.md) for scaling
- See [Index Guide](INDEX_GUIDE.md) for optimization
