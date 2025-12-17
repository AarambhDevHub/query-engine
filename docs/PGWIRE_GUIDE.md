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

## Limitations

Current limitations and planned future improvements:

### Not Yet Implemented

| Feature | Priority | Description |
|---------|----------|-------------|
| Extended Query Protocol | High | Prepared statements, parameter binding |
| Authentication | High | MD5/SCRAM-SHA-256 password authentication |
| TLS/SSL | Medium | Encrypted connections |
| Transactions | Medium | BEGIN, COMMIT, ROLLBACK support |
| CREATE/INSERT/UPDATE/DELETE | Medium | Write operations (currently read-only) |
| System Catalogs | Low | `pg_catalog` tables for introspection |
| COPY Command | Low | Bulk data import/export |
| Cursors | Low | Server-side cursor support |

### Current Workarounds

- **No Authentication**: Use firewall rules to restrict access
- **No Transactions**: Each query auto-commits
- **No COPY**: Use `--load` flag or `register_table()` API
- **No System Catalogs**: Use `SHOW TABLES` and `DESCRIBE table`

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
