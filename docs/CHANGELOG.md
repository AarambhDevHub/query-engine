# Changelog

All notable changes to Query Engine are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [0.1.0] - 2024-12-14

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
| 0.1.0 | 2024-12-14 | Initial release with caching, distributed execution, indexes |
