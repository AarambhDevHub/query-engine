# Examples Reference

Collection of code examples for Query Engine.

## Available Examples

Run any example with:
```bash
cargo run --example <example_name>
```

| Example | Description |
|---------|-------------|
| `simple_query` | Basic SELECT queries |
| `join_query` | JOIN operations |
| `aggregate_query` | GROUP BY and aggregations |
| `subquery_demo` | Subqueries and CTEs |
| `window_function_query` | Window functions |
| `comprehensive_query` | Complex queries |
| `cli_demo` | CLI usage demonstration |
| `full_query_demo` | End-to-end workflow |
| `index_query` | Index creation and usage |
| `distributed_query` | Distributed execution |

---

## Basic Queries

### simple_query

```rust
// Basic SELECT with filtering
let sql = "SELECT name, salary FROM employees WHERE salary > 50000";

// Column aliasing
let sql = "SELECT name AS employee_name, salary * 12 AS annual";

// DISTINCT
let sql = "SELECT DISTINCT department FROM employees";

// LIMIT and OFFSET
let sql = "SELECT * FROM employees ORDER BY salary DESC LIMIT 10";
```

Run: `cargo run --example simple_query`

---

## JOIN Operations

### join_query

```rust
// INNER JOIN
let sql = r#"
    SELECT e.name, d.dept_name
    FROM employees e
    INNER JOIN departments d ON e.dept_id = d.id
"#;

// LEFT JOIN
let sql = r#"
    SELECT e.name, COALESCE(d.dept_name, 'No Dept')
    FROM employees e
    LEFT JOIN departments d ON e.dept_id = d.id
"#;

// Multiple JOINs
let sql = r#"
    SELECT e.name, d.dept_name, l.city
    FROM employees e
    JOIN departments d ON e.dept_id = d.id
    JOIN locations l ON d.location_id = l.id
"#;

// Self JOIN
let sql = r#"
    SELECT e.name, m.name AS manager
    FROM employees e
    LEFT JOIN employees m ON e.manager_id = m.id
"#;
```

Run: `cargo run --example join_query`

---

## Aggregations

### aggregate_query

```rust
// Basic aggregates
let sql = "SELECT COUNT(*), SUM(salary), AVG(salary) FROM employees";

// GROUP BY
let sql = r#"
    SELECT department, COUNT(*) as count, AVG(salary) as avg_sal
    FROM employees
    GROUP BY department
"#;

// HAVING
let sql = r#"
    SELECT department, COUNT(*) as count
    FROM employees
    GROUP BY department
    HAVING COUNT(*) > 5
"#;

// Multiple grouping columns
let sql = r#"
    SELECT department, job_title, AVG(salary)
    FROM employees
    GROUP BY department, job_title
"#;
```

Run: `cargo run --example aggregate_query`

---

## Subqueries

### subquery_demo

```rust
// Scalar subquery
let sql = r#"
    SELECT name, salary,
           (SELECT AVG(salary) FROM employees) as company_avg
    FROM employees
"#;

// IN subquery
let sql = r#"
    SELECT * FROM employees
    WHERE dept_id IN (
        SELECT id FROM departments WHERE location = 'NYC'
    )
"#;

// EXISTS
let sql = r#"
    SELECT * FROM departments d
    WHERE EXISTS (
        SELECT 1 FROM employees e WHERE e.dept_id = d.id
    )
"#;

// Derived table
let sql = r#"
    SELECT dept_name, avg_salary
    FROM (
        SELECT d.dept_name, AVG(e.salary) as avg_salary
        FROM departments d
        JOIN employees e ON d.id = e.dept_id
        GROUP BY d.dept_name
    ) AS dept_stats
    WHERE avg_salary > 60000
"#;

// CTE
let sql = r#"
    WITH high_earners AS (
        SELECT * FROM employees WHERE salary > 80000
    )
    SELECT department, COUNT(*)
    FROM high_earners
    GROUP BY department
"#;
```

Run: `cargo run --example subquery_demo`

---

## Window Functions

### window_function_query

```rust
// ROW_NUMBER
let sql = r#"
    SELECT name, salary,
           ROW_NUMBER() OVER (ORDER BY salary DESC) as rank
    FROM employees
"#;

// Partitioned ranking
let sql = r#"
    SELECT name, department, salary,
           RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank
    FROM employees
"#;

// Running totals
let sql = r#"
    SELECT date, amount,
           SUM(amount) OVER (ORDER BY date) as running_total
    FROM transactions
"#;

// Moving average
let sql = r#"
    SELECT date, value,
           AVG(value) OVER (
               ORDER BY date
               ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
           ) as moving_avg_3
    FROM metrics
"#;

// LAG/LEAD
let sql = r#"
    SELECT date, sales,
           LAG(sales, 1) OVER (ORDER BY date) as prev_day,
           LEAD(sales, 1) OVER (ORDER BY date) as next_day
    FROM daily_sales
"#;
```

Run: `cargo run --example window_function_query`

---

## Index Usage

### index_query

```rust
use query_index::{IndexManager, BTreeIndex, HashIndex, IndexKey};

// Create indexes
let manager = IndexManager::new();
manager.create_btree_index("idx_id", "users", vec!["id".into()], true)?;
manager.create_hash_index("idx_email", "users", vec!["email".into()], false)?;

// Build B-Tree index
let btree = BTreeIndex::with_metadata("idx", "t", vec!["col".into()], false);
for (row, value) in data.iter().enumerate() {
    btree.insert(IndexKey::from_i64(*value), row)?;
}

// Equality lookup
let result = btree.lookup(&IndexKey::from_i64(100));

// Range scan
let result = btree.range_scan(
    Some(&IndexKey::from_i64(50)),
    Some(&IndexKey::from_i64(150))
);

// Hash index for fast lookups
let hash = HashIndex::with_metadata("idx", "t", vec!["email".into()], true);
hash.insert(IndexKey::from_string("user@test.com"), 0)?;
let result = hash.lookup(&IndexKey::from_string("user@test.com"));
```

Run: `cargo run --example index_query`

---

## Distributed Execution

### distributed_query

```rust
use query_distributed::{Coordinator, DistributedExecutor, Worker, Partitioner};

// Create coordinator
let coordinator = Arc::new(Coordinator::default());

// Register workers
coordinator.register_worker("worker1:50051")?;
coordinator.register_worker("worker2:50051")?;

// Check cluster
let status = coordinator.cluster_status();
println!("Workers: {}", status.active_workers);

// Create executor
let executor = DistributedExecutor::new(Arc::clone(&coordinator));

// Partitioning strategies
let hash_part = Partitioner::hash(vec!["customer_id".into()], 4);
let rr_part = Partitioner::round_robin(3);

// Execute distributed query
let results = executor.execute(&plan).await?;

// Cleanup
coordinator.unregister_worker(worker_id)?;
```

Run: `cargo run --example distributed_query`

---

## Full Workflow

### full_query_demo

Complete end-to-end example:

```rust
// 1. Load multiple data sources
let employees = CsvDataSource::from_path("employees.csv")?;
let departments = CsvDataSource::from_path("departments.csv")?;

// 2. Create indexes for fast lookups
let manager = IndexManager::new();
manager.create_btree_index("idx_salary", "employees", vec!["salary".into()], false)?;

// 3. Parse complex query
let sql = r#"
    WITH dept_stats AS (
        SELECT dept_id, AVG(salary) as avg_salary
        FROM employees
        GROUP BY dept_id
    )
    SELECT e.name, e.salary, d.dept_name, ds.avg_salary
    FROM employees e
    JOIN departments d ON e.dept_id = d.id
    JOIN dept_stats ds ON e.dept_id = ds.dept_id
    WHERE e.salary > ds.avg_salary
    ORDER BY e.salary DESC
    LIMIT 20
"#;

// 4. Plan and execute
let mut parser = Parser::new(sql)?;
let statement = parser.parse()?;
let plan = planner.create_logical_plan(&statement)?;
let results = executor.execute(&plan, &tables)?;

// 5. Output results
for batch in results {
    // Format as table, JSON, or CSV
}
```

Run: `cargo run --example full_query_demo`

---

## Running All Examples

```bash
# Run specific example
cargo run --example simple_query

# Run with release optimizations
cargo run --release --example comprehensive_query

# List all examples
ls examples-package/examples/
```
