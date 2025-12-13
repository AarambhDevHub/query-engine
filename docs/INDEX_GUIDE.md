# Index Guide

Guide to using B-Tree and Hash indexes in Query Engine for faster query execution.

## Overview

Indexes accelerate query execution by enabling fast data lookups without scanning entire tables. Query Engine supports two index types:

| Index Type | Equality | Range | Space | Best For |
|------------|----------|-------|-------|----------|
| **B-Tree** | O(log n) | O(log n + k) | Higher | General purpose, ORDER BY |
| **Hash** | O(1) | ❌ | Lower | Primary keys, exact lookups |

## Creating Indexes

### SQL Syntax

```sql
-- B-Tree index (default)
CREATE INDEX idx_name ON table_name(column_name);

-- Hash index
CREATE INDEX idx_name ON table_name(column_name) USING HASH;

-- Unique constraint
CREATE UNIQUE INDEX idx_pk ON table_name(id);

-- Multi-column index
CREATE INDEX idx_composite ON table_name(col1, col2);
```

### Programmatic API

```rust
use query_index::{IndexManager, BTreeIndex, HashIndex, IndexKey};

let manager = IndexManager::new();

// B-Tree index
manager.create_btree_index(
    "idx_salary",      // index name
    "employees",       // table name
    vec!["salary".to_string()],  // columns
    false              // unique constraint
)?;

// Hash index
manager.create_hash_index(
    "idx_email",
    "users",
    vec!["email".to_string()],
    true  // unique
)?;
```

## B-Tree Index

### When to Use

✅ Equality queries: `WHERE id = 100`  
✅ Range queries: `WHERE salary BETWEEN 50000 AND 80000`  
✅ Greater/Less than: `WHERE age > 25`  
✅ ORDER BY optimization  
✅ Prefix searches: `WHERE name LIKE 'John%'`

### Example

```rust
use query_index::{BTreeIndex, IndexKey, Index};

// Create index
let index = BTreeIndex::with_metadata(
    "idx_salary", "employees", 
    vec!["salary".to_string()], false
);

// Insert entries
index.insert(IndexKey::from_i64(50000), 0)?;
index.insert(IndexKey::from_i64(75000), 1)?;
index.insert(IndexKey::from_i64(60000), 2)?;

// Equality lookup
let result = index.lookup(&IndexKey::from_i64(75000));
// result.row_ids = [1]

// Range scan
let result = index.range_scan(
    Some(&IndexKey::from_i64(50000)),
    Some(&IndexKey::from_i64(70000))
);
// result.row_ids = [0, 2]  (50000 and 60000)
```

### Sorted Order

B-Tree maintains keys in sorted order, making it efficient for:

```sql
-- Uses index for both filter and sort
SELECT * FROM employees 
WHERE salary > 50000 
ORDER BY salary;
```

## Hash Index

### When to Use

✅ Equality queries: `WHERE email = 'user@example.com'`  
✅ Primary key lookups  
✅ Foreign key joins  
❌ NOT for: Range queries, ORDER BY, LIKE patterns

### Example

```rust
use query_index::{HashIndex, IndexKey, Index};

// Create index
let index = HashIndex::with_metadata(
    "idx_email", "users",
    vec!["email".to_string()], true // unique
);

// Insert entries
index.insert(IndexKey::from_string("alice@test.com"), 0)?;
index.insert(IndexKey::from_string("bob@test.com"), 1)?;

// O(1) lookup
let result = index.lookup(&IndexKey::from_string("bob@test.com"));
// result.row_ids = [1]

// Range scan NOT supported - returns empty
let result = index.range_scan(Some(&key1), Some(&key2));
// result.row_ids = []
```

## Multi-Column Indexes

Multi-column indexes accelerate queries on column prefixes:

```sql
CREATE INDEX idx_name_dept ON employees(name, department);
```

**Index can accelerate:**
- `WHERE name = 'John'`
- `WHERE name = 'John' AND department = 'Sales'`

**Index CANNOT accelerate:**
- `WHERE department = 'Sales'` (not a prefix)

### Column Order Matters

Choose column order based on query patterns:

```sql
-- Query pattern 1: Filter by name, then department
SELECT * FROM employees WHERE name = 'John' AND department = 'Sales';
-- Good: CREATE INDEX idx_name_dept ON employees(name, department);

-- Query pattern 2: Filter by department only
SELECT * FROM employees WHERE department = 'Sales';
-- Good: CREATE INDEX idx_dept ON employees(department);
```

## Unique Indexes

Enforce uniqueness constraints:

```rust
// Unique index
let index = BTreeIndex::with_metadata(
    "idx_pk", "employees",
    vec!["id".to_string()], true // unique = true
);

index.insert(IndexKey::from_i64(1), 0)?;
index.insert(IndexKey::from_i64(1), 1)?; // ERROR: duplicate
```

## Index Manager

The `IndexManager` provides centralized index management:

```rust
use query_index::IndexManager;

let manager = IndexManager::new();

// Create indexes
manager.create_btree_index("idx_id", "employees", vec!["id".into()], true)?;
manager.create_hash_index("idx_name", "employees", vec!["name".into()], false)?;

// Get index by name
let index = manager.get_index("idx_id");

// Get all indexes for a table
let indexes = manager.get_indexes_for_table("employees");

// Find index for a column
let index = manager.find_index_for_column("employees", "name");

// Drop index
manager.drop_index("idx_name")?;

// List all indexes
let names = manager.list_indexes();
```

## Performance Tips

### 1. Choose the Right Index Type

| Query Pattern | Recommended Index |
|---------------|-------------------|
| Primary key lookup | Hash (if unique) or B-Tree |
| Range queries | B-Tree only |
| ORDER BY | B-Tree only |
| High cardinality equality | Hash |
| Low cardinality | Consider no index |

### 2. Index Selectivity

High selectivity indexes (many distinct values) are more effective:

```
Good: email, user_id, timestamp  (high selectivity)
Bad: gender, boolean flags       (low selectivity)
```

### 3. Avoid Over-Indexing

Each index:
- Slows INSERT/UPDATE/DELETE operations
- Consumes memory
- Needs maintenance

### 4. Bulk Loading

For large datasets, use bulk loading:

```rust
let entries: Vec<_> = data.iter()
    .enumerate()
    .map(|(row_id, value)| (IndexKey::from_i64(*value), row_id))
    .collect();

index.bulk_load(entries)?;
```

## Dropping Indexes

```sql
-- Drop by name
DROP INDEX idx_name;

-- Drop if exists (no error if missing)
DROP INDEX IF EXISTS idx_name;
```

```rust
manager.drop_index("idx_name")?;
manager.drop_index_if_exists("idx_maybe")?; // returns bool
```

## Index Statistics

```rust
let index = manager.get_index("idx_salary").unwrap();

// Number of entries
let count = index.len();

// Check if empty
let empty = index.is_empty();

// Supports range?
let supports_range = index.supports_range();
```
