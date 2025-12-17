# Roadmap

Query Engine development roadmap and planned features.

## ✅ v0.1.0 (Completed)

| Feature | Status | Description |
|---------|--------|-------------|
| SQL Parsing | ✅ Done | SELECT, WHERE, GROUP BY, ORDER BY, LIMIT |
| JOIN Operations | ✅ Done | INNER, LEFT, RIGHT, FULL OUTER, CROSS |
| Subqueries & CTEs | ✅ Done | WITH clause, derived tables, IN/EXISTS |
| Window Functions | ✅ Done | ROW_NUMBER, RANK, LAG, LEAD, etc. |
| Scalar Functions | ✅ Done | String, Math, Null handling functions |
| Index Support | ✅ Done | B-Tree and Hash indexes, CREATE/DROP INDEX |
| Distributed Execution | ✅ Done | Coordinator/Worker, partitioning, scheduling |
| Query Caching | ✅ Done | LRU cache with TTL, CLI commands |

---

## ✅ v0.2.0 (Current)

| Feature | Status | Priority | Description |
|---------|--------|----------|-------------|
| Real-time Streaming | ✅ Done | High | Stream processing for live data |
| Arrow Flight | ✅ Done | High | Network data transfer protocol |
| PostgreSQL Protocol | ✅ Done | Medium | Wire-compatible with psql clients |
| Web UI Dashboard | 🚧 In Progress | Medium | Browser-based query interface |

---

## 📋 v0.3.0 (Planned)

| Feature | Priority | Description |
|---------|----------|-------------|
| Materialized Views | Medium | Cached query results with auto-refresh |
| Query History | Low | Persistent query logging |
| Cost-Based Optimizer | High | Statistics-driven query planning |
| Parallel Query Execution | High | Multi-threaded local execution |

---

## 🔮 v0.4.0 (Future)

| Feature | Priority | Description |
|---------|----------|-------------|
| JDBC/ODBC Drivers | Medium | Database connectivity |
| Cloud Storage | Medium | S3, GCS, Azure Blob support |
| Data Catalog | Low | Schema discovery and metadata |
| Machine Learning | Low | ML model integration |

---

## Feature Requests

Have a feature request? Open an issue on [GitHub](https://github.com/AarambhDevHub/query-engine/issues).

---

## Contributing

Want to help? Check [CONTRIBUTING.md](../CONTRIBUTING.md) for guidelines.

Priority areas:
1. Performance benchmarks
2. Test coverage
3. Documentation
4. Bug fixes
