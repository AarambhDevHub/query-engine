# CLI Reference

Complete reference for the Query Engine command-line interface.

## Installation

```bash
# Install globally
cargo install --path crates/query-cli

# Verify
qe --version
```

## Commands

### REPL Mode

Start interactive shell:

```bash
qe
qe repl
qe repl --db-path ./mydata
```

### Query Mode

Execute single query:

```bash
qe query \
    --sql "SELECT * FROM users WHERE age > 25" \
    --table users \
    --file data/users.csv
```

**Options:**
- `-s, --sql <SQL>` - SQL query to execute
- `-t, --table <NAME>` - Table name
- `-f, --file <PATH>` - Data file path

### Multiple Tables

```bash
qe query \
    --sql "SELECT e.name, d.dept_name FROM employees e JOIN departments d ON e.dept_id = d.id" \
    --table employees \
    --file data/employees.csv \
    --extra-table departments:data/departments.csv
```

### Register Command

Pre-register tables:

```bash
qe register -n users -f data/users.csv -t csv
```

### Benchmark Command

Run performance benchmarks:

```bash
qe bench -q queries/complex.sql -i 1000
```

### Export Command

Export query results:

```bash
qe export \
    --sql "SELECT * FROM sales WHERE amount > 1000" \
    --table sales \
    --input data/sales.csv \
    --output results.parquet \
    --format parquet
```

### Flight Server

Start Arrow Flight server:

```bash
# Start on default port 50051
qe flight-server

# Load CSV files as tables
qe flight-server --load users=data/users.csv --load orders=data/orders.csv

# Custom host/port
qe flight-server --host 127.0.0.1 --port 8080
```

**Options:**
- `-p, --port <PORT>` - Port to listen on (default: 50051)
- `-H, --host <HOST>` - Host to bind (default: 0.0.0.0)
- `-l, --load <NAME=PATH>` - Load CSV files as tables

### Flight Query

Query remote Flight server:

```bash
# Execute query
qe flight-query --connect http://localhost:50051 --sql "SELECT * FROM users"

# Output as JSON
qe flight-query -c http://localhost:50051 -s "users" -o json

# Output as CSV
qe flight-query -c http://localhost:50051 -s "users" -o csv
```

**Options:**
- `-c, --connect <URL>` - Flight server URL
- `-s, --sql <QUERY>` - SQL query to execute
- `-o, --output <FORMAT>` - Output format: table, json, csv

### PostgreSQL Server

Start PostgreSQL-compatible server:

```bash
# Start on default port 5432 (no authentication)
qe pg-server

# Load CSV files as tables
qe pg-server --load users=data/users.csv --load orders=data/orders.csv

# Custom host/port
qe pg-server --host 127.0.0.1 --port 5433

# With MD5 password authentication
qe pg-server --user admin --password secret123 --load users=data/users.csv

# With TLS encryption
qe pg-server --tls-cert server.crt --tls-key server.key
```

**Options:**
- `-p, --port <PORT>` - Port to listen on (default: 5432)
- `-H, --host <HOST>` - Host to bind (default: 0.0.0.0)
- `-l, --load <NAME=PATH>` - Load CSV files as tables
- `-u, --user <USERNAME>` - Username for MD5 authentication (enables auth)
- `--password <PASSWORD>` - Password for authentication (requires --user)
- `--tls-cert <PATH>` - Path to TLS certificate (PEM format)
- `--tls-key <PATH>` - Path to TLS private key (PEM format)

**Connect with psql:**
```bash
# Without authentication
psql -h localhost -p 5432

# With authentication
psql -h localhost -p 5432 -U admin
# Enter password when prompted

# With TLS
PGSSLMODE=require psql -h localhost -p 5432
```

---

## REPL Commands

### Data Management

| Command | Description | Example |
|---------|-------------|---------|
| `.load csv <path> [name]` | Load CSV file | `.load csv data/users.csv users` |
| `.load parquet <path> [name]` | Load Parquet file | `.load parquet data/sales.parquet` |
| `.tables` | List all tables | `.tables` |
| `.describe <table>` | Show table schema | `.describe users` |
| `.schema <table>` | Show CREATE TABLE | `.schema users` |
| `.drop <table>` | Remove table | `.drop temp_table` |

### Display Settings

| Command | Description |
|---------|-------------|
| `.timing` | Toggle query timing display |
| `.plan` | Toggle query plan display |
| `.format <type>` | Set output format: `table`, `json`, `csv` |

### Utilities

| Command | Description |
|---------|-------------|
| `.help` | Show help message |
| `.clear` | Clear screen |
| `.quit` / `.exit` | Exit REPL |

### Cache Management

| Command | Description |
|---------|-------------|
| `.cache` | Show cache statistics |
| `.cache stats` | Show cache statistics |
| `.cache clear` | Clear all cached entries |

### Streaming

| Command | Description |
|---------|-------------|
| `.stream` | Show streaming commands |
| `.stream info` | Show streaming capabilities |
| `.stream help` | Show streaming usage examples |

---

## Examples

### Loading Data

```bash
qe> .load csv data/employees.csv employees
✓ Loaded table 'employees' from data/employees.csv (1000 rows, 5 columns)

qe> .load parquet data/orders.parquet orders
✓ Loaded table 'orders' from data/orders.parquet (50000 rows, 8 columns)
```

### Exploring Tables

```bash
qe> .tables
Tables:
  - employees (1000 rows)
  - orders (50000 rows)

qe> .describe employees
Table: employees
Source: CSV file: data/employees.csv
Rows: 1000

┌───────────────┬──────────┬──────────┐
│ Column        │ Type     │ Nullable │
├───────────────┼──────────┼──────────┤
│ id            │ Int64    │ NO       │
│ name          │ Utf8     │ NO       │
│ department_id │ Int64    │ NO       │
│ salary        │ Float64  │ NO       │
│ hire_date     │ Date32   │ YES      │
└───────────────┴──────────┴──────────┘
```

### Running Queries

```bash
qe> SELECT name, salary FROM employees WHERE salary > 80000 ORDER BY salary DESC LIMIT 5;
┌──────────────┬──────────┐
│ name         │ salary   │
├──────────────┼──────────┤
│ John Smith   │ 125000.0 │
│ Jane Doe     │ 115000.0 │
│ Bob Johnson  │ 105000.0 │
│ Alice Brown  │ 98000.0  │
│ Charlie Lee  │ 95000.0  │
└──────────────┴──────────┘
5 rows (0.023 ms)
```

### Timing and Plans

```bash
qe> .timing
Query timing enabled

qe> .plan
Query plan display enabled

qe> SELECT department_id, COUNT(*), AVG(salary) FROM employees GROUP BY department_id;

Query Plan:
  Aggregate { group_by: [department_id], aggregates: [COUNT(*), AVG(salary)] }
    └── TableScan { table: employees, columns: [department_id, salary] }

┌───────────────┬──────────┬──────────────┐
│ department_id │ count    │ avg_salary   │
├───────────────┼──────────┼──────────────┤
│ 1             │ 150      │ 72500.0      │
│ 2             │ 200      │ 68000.0      │
│ 3             │ 175      │ 75000.0      │
└───────────────┴──────────┴──────────────┘
3 rows

Planning: 0.05ms
Execution: 1.23ms
Total: 1.28ms
```

### Output Formats

```bash
qe> .format json
Output format set to JSON

qe> SELECT name, salary FROM employees LIMIT 2;
[
  {"name": "John Smith", "salary": 125000.0},
  {"name": "Jane Doe", "salary": 115000.0}
]

qe> .format csv
Output format set to CSV

qe> SELECT name, salary FROM employees LIMIT 2;
name,salary
John Smith,125000.0
Jane Doe,115000.0
```

### Index Management

```bash
qe> CREATE INDEX idx_salary ON employees(salary);
✓ Created index 'idx_salary' on employees(salary)

qe> CREATE UNIQUE INDEX idx_emp_id ON employees(id);
✓ Created unique index 'idx_emp_id' on employees(id)

qe> DROP INDEX idx_salary;
✓ Dropped index 'idx_salary'
```

### Cache Management

```bash
qe> .cache stats

Query Cache Statistics

+----------------+---------+
| Metric         | Value   |
+================+=========+
| Enabled        | Yes     |
| Entries        | 5       |
| Memory Used    | 2048 bytes |
| Total Requests | 12      |
| Hits           | 7       |
| Misses         | 5       |
| Hit Rate       | 58.3%   |
| Evictions      | 0       |
| Expirations    | 0       |
+----------------+---------+

qe> .cache clear
✓ Cache cleared
```

### Multi-line Queries

Queries can span multiple lines (end with semicolon):

```bash
qe> SELECT 
    e.name,
    d.dept_name,
    e.salary
FROM employees e
JOIN departments d ON e.department_id = d.id
WHERE e.salary > 70000
ORDER BY e.salary DESC
LIMIT 10;
```

---

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `QE_DB_PATH` | Default database path | `.` |
| `QE_FORMAT` | Default output format | `table` |
| `QE_HISTORY` | History file path | `~/.qe_history` |

### History

Command history is stored in `~/.qe_history` and persists across sessions.

Navigation:
- ↑/↓ arrows to browse history
- Ctrl+R to search history

---

## Error Messages

### Common Errors

**File not found:**
```
Error: File not found: data/missing.csv
Hint: Check the file path and ensure the file exists
```

**Table not found:**
```
Error: Table 'nonexistent' not found
Hint: Use .tables to list available tables
```

**Parse error:**
```
Error: Parse error at line 1, column 15: expected 'FROM'
Hint: Check SQL syntax near 'FORM'
```

**Column not found:**
```
Error: Column 'nonexistent_col' not found in table 'users'
Hint: Use .describe users to see available columns
```

---

## Tips

1. **Tab completion**: Coming soon
2. **Use aliases**: `SELECT e.name FROM employees e` 
3. **Check plan**: `.plan` shows optimization opportunities
4. **Time queries**: `.timing` for performance insights
5. **Multi-line**: Complex queries work across lines
6. **Monitor cache**: `.cache stats` to check cache hit rates
