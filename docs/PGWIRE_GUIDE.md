# PostgreSQL Wire Protocol Guide

This guide explains how to use the PostgreSQL wire protocol support in Query Engine, enabling connections from standard PostgreSQL clients.

## Overview

Query Engine implements the PostgreSQL wire protocol via the `query-pgwire` crate, allowing you to:

- Connect with **psql**, **pgAdmin**, **DBeaver**, and other PostgreSQL clients
- Execute SQL queries using familiar tools
- Load CSV data and query it over the network

## Quick Start

### Start the Server

```bash
# Default: listen on 0.0.0.0:5432
qe pg-server

# Custom port with pre-loaded data
qe pg-server --port 5433 --load users=data/users.csv --load orders=data/orders.csv
```

### Connect with psql

```bash
psql -h localhost -p 5432
```

### Execute Queries

```sql
SELECT * FROM users WHERE age > 25;
SELECT name, SUM(amount) FROM orders GROUP BY name;
```

## CLI Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--port` | `-p` | Port to listen on | 5432 |
| `--host` | `-H` | Host to bind to | 0.0.0.0 |
| `--load` | `-l` | CSV files to load (format: `name=path`) | None |
| `--verbose` | `-v` | Enable verbose logging | false |

### Loading Data

Load multiple CSV files at startup:

```bash
qe pg-server \
  --load customers=data/customers.csv \
  --load orders=data/orders.csv \
  --load products=data/products.csv
```

## Client Compatibility

### Tested Clients

| Client | Status | Notes |
|--------|--------|-------|
| psql | ✅ Works | Command-line PostgreSQL client |
| pgAdmin | ✅ Works | GUI administration tool |
| DBeaver | ✅ Works | Universal database tool |
| Python psycopg2 | ✅ Works | Python PostgreSQL adapter |

### Connection Settings

For GUI clients, use these settings:

- **Host**: `localhost` (or your server IP)
- **Port**: `5432` (or your custom port)
- **Database**: Any name (ignored)
- **User**: Any name (no authentication)
- **Password**: Leave empty

## Supported SQL

The PostgreSQL server supports all SQL features of Query Engine:

- `SELECT` with `WHERE`, `GROUP BY`, `ORDER BY`, `LIMIT`
- All `JOIN` types: INNER, LEFT, RIGHT, FULL OUTER, CROSS
- Aggregate functions: COUNT, SUM, AVG, MIN, MAX
- Window functions: ROW_NUMBER, RANK, LAG, LEAD
- Subqueries and Common Table Expressions (CTEs)
- Scalar functions: UPPER, LOWER, LENGTH, CONCAT, etc.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   PostgreSQL Clients                     │
│              (psql, pgAdmin, DBeaver, etc.)             │
└─────────────────────────┬───────────────────────────────┘
                          │ PostgreSQL Wire Protocol
                          ▼
┌─────────────────────────────────────────────────────────┐
│                      query-pgwire                        │
│  ┌──────────────┐  ┌───────────────┐  ┌──────────────┐  │
│  │  PgServer    │  │ QueryBackend  │  │ TypeConvert  │  │
│  │  (TCP/TLS)   │  │ (SQL Handler) │  │ (Arrow→PG)   │  │
│  └──────────────┘  └───────────────┘  └──────────────┘  │
└─────────────────────────┬───────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│                    Query Engine Core                     │
│         (Parser → Planner → Optimizer → Executor)       │
└─────────────────────────────────────────────────────────┘
```

## Authentication

Query Engine supports MD5 password authentication to secure your PostgreSQL connections.

### Enabling Authentication

```bash
# Start with authentication enabled
qe pg-server --user admin --password secret123

# Connect with credentials
psql -h localhost -p 5432 -U admin
# (prompts for password)
```

### Programmatic Authentication

```rust
use query_pgwire::{PgServer, AuthConfig};

// Single user
let server = PgServer::new("0.0.0.0", 5432)
    .with_auth("admin", "secret123");

// Multiple users
let auth = AuthConfig::new()
    .add_user("admin", "admin_pass")
    .add_user("readonly", "readonly_pass");

let server = PgServer::new("0.0.0.0", 5432)
    .with_auth_config(auth);
```

### Connection Settings (with Authentication)

For GUI clients, use these settings:

- **Host**: `localhost` (or your server IP)
- **Port**: `5432` (or your custom port)
- **Database**: Any name (ignored)
- **User**: Your configured username
- **Password**: Your configured password

## Extended Query Protocol

Query Engine now supports the PostgreSQL Extended Query Protocol, enabling:

- **Prepared Statements**: Parse once, execute many times
- **Parameter Binding**: Use `$1`, `$2` style placeholders
- **Binary Protocol**: More efficient data transfer

### Python Example with Parameters

```python
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    user="admin",
    password="secret123"
)
cursor = conn.cursor()

# Prepared statement with parameters
cursor.execute(
    "SELECT * FROM users WHERE age > %s AND name LIKE %s",
    (25, 'A%')
)
for row in cursor.fetchall():
    print(row)
```

## Limitations

Current limitations and planned future improvements:

### Implemented (v0.3)

| Feature | Description |
|---------|-------------|
| TLS/SSL | Encrypted connections via `--tls-cert` and `--tls-key` |
| Transactions | BEGIN, COMMIT, ROLLBACK support (auto-commit mode) |
| CREATE TABLE | Create in-memory tables with typed columns |
| INSERT | Insert rows with multi-row VALUES support |
| UPDATE | Update column values with WHERE clause filtering |
| DELETE | Delete rows with WHERE clause filtering |
| System Catalogs | `pg_catalog.pg_tables`, `pg_attribute`, `pg_type`, and `information_schema.columns` |
| COPY Command | `COPY table TO STDOUT` and `COPY table FROM STDIN` with CSV format |
| Server-Side Cursors | DECLARE, FETCH, CLOSE cursor commands |
| SCRAM-SHA-256 | More secure authentication (use `.with_method(AuthMethod::ScramSha256)`) |
| Named Portals | Result pagination via extended protocol |

### Not Yet Implemented

The following PostgreSQL features are **not supported** in this implementation:

| Category | Feature | Notes |
|----------|---------|-------|
| | UPSERT (ON CONFLICT) | **Implemented** - `DO NOTHING` and `DO UPDATE SET` supported |
| | Recursive CTEs | **Implemented** - WITH RECURSIVE with UNION ALL and fixed-point iteration |
| | DISTINCT ON | Only standard DISTINCT |

| | DISTINCT ON | Only standard DISTINCT |
| | FULL TEXT SEARCH | No tsvector/tsquery |
| **Transactions** | SAVEPOINT | Only BEGIN/COMMIT/ROLLBACK |
| | Nested transactions | Not supported |
| | Isolation levels | Single-connection only |
| | Two-phase commit (PREPARE TRANSACTION) | Not implemented |
| **Schema** | ALTER TABLE | Cannot modify existing tables |
| | DROP TABLE | Tables persist until server restart |
| | Foreign keys | No referential integrity |
| | CHECK constraints | Not enforced |
| | DEFAULT values | Not supported in CREATE TABLE |
| | Sequences/SERIAL | Use explicit INT64 |
| | Views | CREATE VIEW not supported |
| | Materialized Views | Not supported |
| **Server Features** | Multiple databases | Single database per server |
| | Roles/permissions | No GRANT/REVOKE |
| | LISTEN/NOTIFY | No pub/sub |
| | Large Objects | Not supported |
| | Server-side functions | No CREATE FUNCTION/PL/pgSQL |
| | Triggers | Not supported |
| | Rules | Not supported |
| **Replication** | Logical replication | Not supported |
| | Physical replication | Not supported |

> [!NOTE]
> This is an in-memory query engine focused on analytics. Window functions (ROW_NUMBER, RANK, etc.), CTEs (WITH clause), and JOINs are fully supported. Use PostgreSQL for full RDBMS features.

### System Catalogs

Query metadata about registered tables:

```sql
-- List all tables
SELECT * FROM pg_catalog.pg_tables;

-- Get column information
SELECT * FROM pg_catalog.pg_attribute WHERE tablename = 'users';

-- List data types
SELECT * FROM pg_catalog.pg_type;

-- Using information_schema (PostgreSQL standard)
SELECT * FROM information_schema.columns WHERE table_name = 'users';
```

### COPY Command

Export and import table data in CSV format:

```sql
-- Export all data as CSV
COPY users TO STDOUT;

-- Export with header row
COPY users TO STDOUT WITH (HEADER);

-- Import data from CSV (inline data format)
COPY users FROM STDIN;
1,Alice,25
2,Bob,30
\.

-- Import with header (skips first line)
COPY users FROM STDIN WITH (HEADER);
id,name,age
1,Alice,25
2,Bob,30
\.
\.
```

### Server-Side Cursors

Paginate through large result sets:

```sql
-- Must be in a transaction
BEGIN;

-- Declare a cursor
DECLARE my_cursor CURSOR FOR SELECT * FROM users WHERE age > 21;

-- Fetch rows in batches
FETCH 10 FROM my_cursor;
FETCH 10 FROM my_cursor;

-- Fetch all remaining
FETCH ALL FROM my_cursor;

-- Close when done
CLOSE my_cursor;
COMMIT;
```

### Special Commands

| Command | Description |
|---------|-------------|
| `SHOW TABLES` | List all registered tables |
| `DESCRIBE <table>` | Show table schema (columns and types) |

## Examples

### Python Connection

```python
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="query_engine",
    user="user"
)
cursor = conn.cursor()
cursor.execute("SELECT * FROM users WHERE age > 25")
for row in cursor.fetchall():
    print(row)
```

### Node.js Connection

```javascript
const { Client } = require('pg');

const client = new Client({
  host: 'localhost',
  port: 5432,
  database: 'query_engine',
  user: 'user'
});

await client.connect();
const result = await client.query('SELECT * FROM users');
console.log(result.rows);
```

## Troubleshooting

### Connection Refused

Ensure the server is running and the port is not in use:

```bash
# Check if port is in use
lsof -i :5432

# Start on a different port
qe pg-server --port 5433
```

### Query Errors

Enable verbose logging to see detailed error messages:

```bash
qe pg-server --verbose
```

## See Also

- [CLI Reference](CLI_REFERENCE.md) - Full CLI documentation
- [SQL Reference](SQL_REFERENCE.md) - SQL syntax guide
- [Flight Guide](FLIGHT_GUIDE.md) - Arrow Flight protocol
