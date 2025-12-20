# Changelog

All notable changes to Query Engine are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [0.3.0] - 2025-12-19

### Added

#### TLS/SSL Support
- **TLS encryption** for PostgreSQL wire protocol via `--tls-cert` and `--tls-key` flags
- tokio-rustls integration for secure connections
- Compatible with `sslmode=require` in psql and other clients

#### Authentication
- **MD5 password authentication** via `--user` and `--password` flags
- **SCRAM-SHA-256 authentication** for enhanced security (use `AuthMethod::ScramSha256`)
- No-authentication mode for development (default)

#### Write Operations
- **CREATE TABLE** statement with data type support (INT, VARCHAR, BOOLEAN, etc.)
- **INSERT INTO** with multi-row VALUES support
- **UPDATE** with SET clause and WHERE filtering
- **DELETE FROM** with WHERE filtering
- **TRUE/FALSE** boolean literals in parser

#### Transaction Commands
- **BEGIN/COMMIT/ROLLBACK** command recognition (auto-commit mode)

#### System Catalogs
- **pg_catalog.pg_tables** - List all registered tables
- **pg_catalog.pg_attribute** - Column metadata
- **pg_catalog.pg_type** - PostgreSQL type OIDs
- **information_schema.columns** - Standard SQL column info

#### COPY Command
- **COPY TO STDOUT** - Export table data as CSV
- **COPY FROM STDIN** - Import CSV data into tables
- HEADER option support

#### Server-Side Cursors
- **DECLARE cursor CURSOR FOR** - Create named cursors
- **FETCH [count] FROM cursor** - Paginate through results
- **CLOSE cursor** - Release cursor resources

#### Named Portals
- **PortalStore** for persistent portal storage
- Result pagination via extended query protocol

#### Extended Data Types
- **UUID** - Universally Unique Identifier type
- **DECIMAL/NUMERIC** - High-precision decimal with precision and scale
- **JSON/JSONB** - JSON document storage
- **INTERVAL** - Time interval type
- **BYTEA/BLOB** - Large binary data
- **ARRAY types** - `INT[]`, `TEXT[]` array syntax
- **Geometric types** - POINT, LINE, LSEG, BOX, PATH, POLYGON, CIRCLE
- **ENUM** - User-defined enumeration type

---

## [0.2.0] - 2025-12-15

### Added

#### Arrow Flight Integration
- **FlightServer** for hosting tables via gRPC Flight protocol
- **FlightClient** for remote SQL query execution
- **All Flight methods** implemented:
  - `do_get`: Execute query and stream results
  - `do_put`: Upload RecordBatches as tables
  - `do_exchange`: Bidirectional data streaming
  - `poll_flight_info`: Query status polling
  - `list_flights`, `get_flight_info`, `get_schema`
  - `do_action`, `list_actions`, `handshake`
- **FlightDataSource**: Use remote Flight server as DataSource trait impl
- **FlightStreamSource**: Stream data from Flight servers
- **CLI Commands**: `flight-server` and `flight-query`

#### PostgreSQL Wire Protocol
- **PgServer** for PostgreSQL-compatible server hosting
- **SimpleQueryHandler** implementation for SQL processing
- **Arrow to PostgreSQL type conversion** for result encoding
- **CSV loading** at server startup via `--load` flag
- **CLI Command**: `pg-server` with host, port, and load options
- **Compatible Clients**: psql, pgAdmin, DBeaver, and other PostgreSQL clients

#### Real-time Streaming Queries
- **StreamSource** trait for async data streams
- **ChannelStreamSource** for Tokio channel-based streaming
- **MemoryStreamSource** for testing
- **StreamingQuery** processor with pause/resume/stop controls
- **Window Types**: Tumbling, Sliding, Session windows
- **Watermark** support for event-time processing
- **LateEventPolicy** for handling late data
- **StreamConfig** with batch size, window, watermark interval

---

## [0.1.0] - 2025-12-14

### Added

#### Query Caching
- **LRU Cache** with configurable capacity (default: 1000 entries)
- **TTL Expiration** (default: 5 minutes) for automatic cache entry expiry
- **Memory Limits** (default: 100MB) to prevent excessive memory usage
- **CachedQueryExecutor** wrapper for transparent caching
- **CLI Commands**: `.cache stats` and `.cache clear`
- **CacheInvalidator** trait for data change notifications
- **CacheStats** with atomic counters for hits/misses/evictions

#### Distributed Execution
- **Coordinator/Worker** architecture for distributed query processing
- **Partitioning Strategies**: Hash, Range, and Round-Robin
- **Task Scheduler** with load balancing
- **Multi-stage** query execution plans
- **Exchange and Merge** operators for data shuffles
- **Fault Tolerance** with retry and checkpointing

#### Index Support
- **B-Tree Indexes** for range queries and equality lookups
- **Hash Indexes** for O(1) equality lookups
- `CREATE INDEX` and `DROP INDEX` SQL syntax
- **IndexManager** for lifecycle management
- **Unique constraint** enforcement
- **Multi-column index** support

#### Scalar Functions (UDFs)
- **String**: UPPER, LOWER, LENGTH, CONCAT, SUBSTRING, TRIM, REPLACE
- **Math**: ABS, CEIL, FLOOR, ROUND, SQRT, POWER
- **Null handling**: COALESCE, NULLIF

#### Window Functions
- **Ranking**: ROW_NUMBER, RANK, DENSE_RANK, NTILE
- **Navigation**: LAG, LEAD, FIRST_VALUE, LAST_VALUE
- PARTITION BY and ORDER BY clauses
- Window frame specifications (ROWS/RANGE BETWEEN)

#### Subqueries and CTEs
- Common Table Expressions with `WITH ... AS (...)`
- Derived tables in FROM clause
- Scalar subqueries in SELECT
- IN and EXISTS subqueries in WHERE

#### JOIN Operations
- INNER, LEFT, RIGHT, FULL OUTER, CROSS JOINs
- Table aliases and qualified column names
- Multiple JOINs in single query

#### Core Features
- SQL parsing with comprehensive syntax support
- Query planning and optimization
- Apache Arrow vectorized execution
- CSV and Parquet file support
- Interactive CLI REPL

---

## Version History

| Version | Date | Highlights |
|---------|------|------------|
| 0.3.0 | 2025-12-19 | TLS/SSL, Authentication, Write Operations (CREATE/INSERT/UPDATE/DELETE) |
| 0.2.0 | 2025-12-17 | Arrow Flight, PostgreSQL Protocol, Real-time streaming |
| 0.1.0 | 2025-12-14 | Initial release with caching, distributed execution, indexes |

