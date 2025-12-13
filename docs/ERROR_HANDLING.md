# Error Handling Guide

Guide to understanding and handling errors in Query Engine.

## Error Types

Query Engine uses a unified `QueryError` type:

```rust
pub enum QueryError {
    ParseError(String),
    PlanError(String),
    ExecutionError(String),
    IoError(std::io::Error),
    ArrowError(arrow::error::ArrowError),
}
```

## Common Errors

### Parse Errors

**Syntax Error:**
```
ParseError: Unexpected token 'FORM' at line 1, column 20
Expected: 'FROM'
```

**Fix:** Check SQL syntax
```sql
-- Wrong
SELECT * FORM users;

-- Correct
SELECT * FROM users;
```

**Unclosed String:**
```
ParseError: Unterminated string literal at line 1
```

**Fix:** Close string quotes
```sql
-- Wrong
SELECT * FROM users WHERE name = 'John;

-- Correct
SELECT * FROM users WHERE name = 'John';
```

### Plan Errors

**Table Not Found:**
```
PlanError: Table 'nonexistent' not found
```

**Fix:** Register the table first
```rust
planner.register_table("users", schema);
```

**Column Not Found:**
```
PlanError: Column 'unknown_col' not found in table 'users'
```

**Fix:** Check column name spelling
```sql
-- Check available columns
.describe users
```

**Ambiguous Column:**
```
PlanError: Ambiguous column 'id' - exists in multiple tables
```

**Fix:** Use table alias
```sql
-- Wrong
SELECT id FROM users JOIN orders ON ...

-- Correct
SELECT u.id FROM users u JOIN orders o ON ...
```

### Execution Errors

**Type Mismatch:**
```
ExecutionError: Cannot compare Int64 with Utf8
```

**Fix:** Ensure compatible types
```sql
-- Wrong
SELECT * FROM users WHERE age = 'thirty';

-- Correct
SELECT * FROM users WHERE age = 30;
```

**Division by Zero:**
```
ExecutionError: Division by zero
```

**Fix:** Guard against zero
```sql
SELECT amount / NULLIF(count, 0) FROM data;
```

**Index Constraint Violation:**
```
ExecutionError: Unique constraint violation in index 'idx_email'
```

**Fix:** Ensure unique values or use non-unique index

### I/O Errors

**File Not Found:**
```
IoError: No such file or directory: 'data/missing.csv'
```

**Fix:** Check file path

**Permission Denied:**
```
IoError: Permission denied: '/protected/data.csv'
```

**Fix:** Check file permissions

### Arrow Errors

**Schema Mismatch:**
```
ArrowError: Schema mismatch: expected 5 columns, got 4
```

**Fix:** Ensure data matches expected schema

## Error Handling in Code

### Basic Pattern

```rust
use query_core::{QueryError, Result};

fn execute_query(sql: &str) -> Result<Vec<RecordBatch>> {
    let mut parser = Parser::new(sql)?;
    let statement = parser.parse()?;
    let plan = planner.create_logical_plan(&statement)?;
    let results = executor.execute(&plan, &tables)?;
    Ok(results)
}

// Handle errors
match execute_query("SELECT * FROM users") {
    Ok(results) => process_results(results),
    Err(e) => {
        eprintln!("Query failed: {}", e);
        // Handle error appropriately
    }
}
```

### Matching Error Types

```rust
match execute_query(sql) {
    Ok(results) => { /* success */ }
    
    Err(QueryError::ParseError(msg)) => {
        eprintln!("Invalid SQL syntax: {}", msg);
        // Show syntax help
    }
    
    Err(QueryError::PlanError(msg)) => {
        if msg.contains("not found") {
            eprintln!("Table or column not found: {}", msg);
            // List available tables
        } else {
            eprintln!("Planning error: {}", msg);
        }
    }
    
    Err(QueryError::ExecutionError(msg)) => {
        eprintln!("Execution failed: {}", msg);
        // May be transient, could retry
    }
    
    Err(QueryError::IoError(e)) => {
        eprintln!("File error: {}", e);
        // Check file paths
    }
    
    Err(QueryError::ArrowError(e)) => {
        eprintln!("Data error: {}", e);
        // Check data format
    }
}
```

### Using anyhow

```rust
use anyhow::{Context, Result};

fn load_and_query() -> Result<()> {
    let csv = CsvDataSource::from_path("data.csv")
        .context("Failed to load data file")?;
    
    let results = execute_query("SELECT * FROM t")
        .context("Query execution failed")?;
    
    Ok(())
}
```

## Distributed Errors

### DistributedError

```rust
pub enum DistributedError {
    WorkerUnavailable(String),
    WorkerNotFound(String),
    WorkerAlreadyRegistered(String),
    NetworkError(String),
    PartitionError(String),
    TaskExecutionFailed(String),
    TaskTimeout(u64),
    NoWorkersAvailable,
    PlanningError(String),
    SerializationError(String),
}
```

### Handling

```rust
match coordinator.register_worker("host:port") {
    Ok(id) => println!("Registered: {}", id),
    Err(DistributedError::WorkerAlreadyRegistered(addr)) => {
        println!("Worker {} already registered", addr);
    }
    Err(e) => eprintln!("Failed: {}", e),
}
```

## Troubleshooting

### Query Not Returning Results

1. Check WHERE clause conditions
2. Verify data exists: `SELECT COUNT(*) FROM table`
3. Check for NULL values: `WHERE col IS NOT NULL`

### Slow Queries

1. Enable timing: `.timing`
2. View plan: `.plan`
3. Add indexes on filtered columns
4. Reduce selected columns

### Memory Issues

1. Add LIMIT clause
2. Process in batches
3. Project only needed columns

### Connection Issues (Distributed)

1. Check worker addresses
2. Verify network connectivity
3. Check firewall rules
4. Monitor heartbeats

## Best Practices

1. **Validate early**: Parse and plan before execution
2. **Use Result**: Propagate errors with `?`
3. **Context**: Add context with anyhow
4. **Log errors**: Use tracing for debugging
5. **Graceful degradation**: Handle partial failures

```rust
// Good error handling pattern
fn safe_query(sql: &str) -> Result<Vec<RecordBatch>> {
    // Validate
    let statement = Parser::new(sql)?.parse()
        .map_err(|e| QueryError::ParseError(format!("Invalid SQL: {}", e)))?;
    
    // Plan with context
    let plan = planner.create_logical_plan(&statement)
        .map_err(|e| QueryError::PlanError(format!("Planning failed: {}", e)))?;
    
    // Execute with logging
    tracing::debug!("Executing plan: {:?}", plan);
    let results = executor.execute(&plan, &tables)?;
    
    Ok(results)
}
```
