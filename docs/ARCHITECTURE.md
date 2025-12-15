# Query Engine Architecture

This document provides a comprehensive overview of the Query Engine's architecture and design principles.

## Overview

Query Engine is a high-performance SQL query engine built in Rust, using Apache Arrow for vectorized execution. The system is designed with a modular, layered architecture that separates concerns and enables extensibility.

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              User Interface                              │
│                         (CLI / REPL / API)                               │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                            Query Parser                                  │
│                         SQL → AST → Statements                           │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           Query Planner                                  │
│                    AST → Logical Plan → Optimized Plan                   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
┌───────────────────────┐ ┌─────────────────┐ ┌───────────────────────┐
│   Local Executor      │ │  Index Manager  │ │ Distributed Executor  │
│  (Single Node)        │ │ (B-Tree/Hash)   │ │ (Multi-Node)          │
└───────────────────────┘ └─────────────────┘ └───────────────────────┘
                    │               │               │
                    └───────────────┼───────────────┘
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          Storage Layer                                   │
│                     CSV / Parquet / Memory                               │
└─────────────────────────────────────────────────────────────────────────┘
```

## Crate Architecture

### Workspace Structure

```
query-engine/
├── crates/
│   ├── query-core/         # Core types and error handling
│   ├── query-parser/       # SQL parsing
│   ├── query-planner/      # Query planning and optimization
│   ├── query-executor/     # Query execution
│   ├── query-storage/      # Data sources
│   ├── query-index/        # Index implementations
│   ├── query-cache/        # Query result caching
│   ├── query-streaming/    # Real-time stream processing
│   ├── query-distributed/  # Distributed execution
│   ├── query-flight/       # Arrow Flight server/client
│   └── query-cli/          # CLI interface
└── examples-package/       # Usage examples
```

### Dependency Graph

```
                    query-cli
                        │
        ┌───────────────┼───────────────┐
        ▼               ▼               ▼
  query-executor  query-storage  query-distributed
        │               │               │
        ├───────────────┤       ┌───────┘
        ▼               ▼       ▼
  query-cache     query-planner   query-index
        │               │               │
        └───────┬───────┴───────────────┘
                ▼
          query-parser
                │
                ▼
           query-core
```

## Component Details

### 1. Query Core (`query-core`)

The foundation crate providing:

- **Schema**: Table and column definitions
- **DataType**: Supported data types (Int64, Float64, Utf8, etc.)
- **Field**: Column metadata with nullability
- **QueryError**: Unified error types
- **ScalarValue**: Runtime values for expressions

### 2. Query Parser (`query-parser`)

Converts SQL text into an Abstract Syntax Tree (AST):

```
SQL String → Lexer → Tokens → Parser → AST
```

**Components:**
- **Lexer**: Tokenizes SQL input
- **Parser**: Recursive descent parser
- **AST**: Statement, Expression, and Clause nodes

**Supported Statements:**
- SELECT (with JOINs, CTEs, subqueries)
- CREATE TABLE / INDEX
- DROP TABLE / INDEX
- INSERT, UPDATE, DELETE

### 3. Query Planner (`query-planner`)

Transforms AST into optimized logical plans:

```
AST → Logical Plan → Optimizer → Optimized Plan
```

**Logical Plan Nodes:**
- `TableScan`: Read from data source
- `Filter`: Apply WHERE predicates
- `Projection`: SELECT column list
- `Aggregate`: GROUP BY with aggregates
- `Join`: All JOIN types
- `Sort`: ORDER BY
- `Limit`: LIMIT/OFFSET
- `WindowFunction`: Window operations

**Optimizations:**
- Predicate pushdown
- Projection pushdown
- Join reordering (future)

### 4. Query Executor (`query-executor`)

Executes logical plans and produces results:

```
Logical Plan → Physical Operators → Arrow RecordBatches
```

**Execution Model:**
- Pull-based iterator model
- Vectorized execution with Arrow
- Lazy evaluation

**Physical Operators:**
- Scan operators (CSV, Parquet, Memory)
- Filter operator (predicate evaluation)
- Projection operator
- Hash Aggregate
- Hash Join, Nested Loop Join
- Sort (in-memory)
- Window function executor

**Caching:**
- `CachedQueryExecutor` wrapper for transparent caching
- Integrates with `query-cache` for LRU result caching

### 5. Query Storage (`query-storage`)

Provides data source abstractions:

**Supported Sources:**
- **CSV**: Streaming CSV reader with schema inference
- **Parquet**: Apache Parquet columnar format
- **Memory**: In-memory tables

**Interfaces:**
- `DataSource` trait for custom sources
- Schema discovery
- Projection pushdown support

### 6. Query Index (`query-index`)

Index implementations for fast lookups:

**Index Types:**

| Type | Use Case | Complexity |
|------|----------|------------|
| B-Tree | Range queries, ORDER BY | O(log n) |
| Hash | Equality lookups | O(1) avg |

**Features:**
- Unique constraint enforcement
- Multi-column indexes
- Thread-safe operations (RwLock)
- Bulk loading

### 7. Query Distributed (`query-distributed`)

Distributed execution framework:

```
┌─────────────────────────────────────────────┐
│              Coordinator                    │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐   │
│  │ Planner  │ │Scheduler │ │  Fault   │   │
│  │          │ │          │ │ Manager  │   │
│  └──────────┘ └──────────┘ └──────────┘   │
└─────────────────────────────────────────────┘
              │           │           │
              ▼           ▼           ▼
        ┌─────────┐ ┌─────────┐ ┌─────────┐
        │Worker 1 │ │Worker 2 │ │Worker 3 │
        └─────────┘ └─────────┘ └─────────┘
```

**Components:**
- **Coordinator**: Query orchestration
- **Worker**: Task execution
- **Scheduler**: Load balancing
- **Partitioner**: Data distribution (Hash, Range, Round-Robin)
- **Exchange/Merge**: Shuffle operators
- **FaultManager**: Retry and recovery
- **FlightTransport**: Arrow Flight-based network transport

### 8. Query Flight (`query-flight`)

Apache Arrow Flight integration for network data transfer:

```
┌────────────────────────────────────────────────────┐
│                 FlightServer                        │
│  ┌──────────────┐  ┌──────────────┐               │
│  │ TableStore   │  │ FlightService│               │
│  │ (in-memory)  │  │ (gRPC impl)  │               │
│  └──────────────┘  └──────────────┘               │
└────────────────────────────────────────────────────┘
          │                   ▲
          │    gRPC/Flight    │
          ▼                   │
┌────────────────────────────────────────────────────┐
│                 FlightClient                        │
│  execute_sql, upload_table, exchange, list_tables  │
└────────────────────────────────────────────────────┘
```

**Server Features:**
- Register tables and serve via gRPC
- Execute SQL queries over the network
- All Flight protocol methods implemented

**Client Features:**
- `execute_sql()`: Query remote tables
- `upload_table()`: Upload RecordBatches to server
- `exchange()`: Bidirectional data streaming
- `list_tables()`, `get_table_schema()`

**Data Sources:**
- `FlightDataSource`: Use remote Flight server as DataSource
- `FlightStreamSource`: Stream data from Flight servers

**Protocol Methods:**
| Method | Status | Description |
|--------|--------|-------------|
| handshake | ✅ | Authentication (no-op) |
| list_flights | ✅ | List available tables |
| get_flight_info | ✅ | Get query/table info |
| get_schema | ✅ | Get table schema |
| do_get | ✅ | Execute query, stream results |
| do_put | ✅ | Upload data as table |
| do_action | ✅ | clear_tables, list_tables |
| list_actions | ✅ | List available actions |
| poll_flight_info | ✅ | Poll query status |
| do_exchange | ✅ | Bidirectional streaming |

### 9. Query Cache (`query-cache`)

LRU-based result caching for repeated queries:

**Features:**
- **LRU Eviction**: Least recently used entries evicted at capacity
- **TTL Expiration**: Time-to-live for automatic expiry (default: 5 min)
- **Memory Limits**: Configurable max memory (default: 100MB)
- **Thread-Safe**: `RwLock` for concurrent access
- **Statistics**: Track hits, misses, evictions, hit rate

**Components:**
- `QueryCache`: Main cache structure
- `CacheKey`: SQL hash-based lookup key
- `CacheEntry`: Cached RecordBatch with metadata
- `CacheStats`: Atomic counters for metrics
- `CacheInvalidator`: Trait for data change notifications

### 10. Query Streaming (`query-streaming`)

Real-time stream processing for continuous data:

**Features:**
- **StreamSource trait**: Async iterator over RecordBatch
- **Window Types**: Tumbling, Sliding, Session windows
- **Watermarks**: Event-time processing with late event handling
- **Stream Control**: Pause, resume, stop operations

**Components:**
- `StreamingQuery`: Main stream processor
- `StreamConfig`: Batch size, window, watermark interval
- `ChannelStreamSource`: Tokio channel-based input
- `MemoryStreamSource`: In-memory testing source
- `Watermark`: Event-time tracking
- `LateEventPolicy`: Drop, SideOutput, or Allow

### 11. Query CLI (`query-cli`)

Interactive command-line interface:

**Features:**
- REPL with history
- Data loading commands
- Table management
- Query timing/planning display
- Multiple output formats (table, JSON, CSV)

## Data Flow

### Query Execution Flow

```
1. User enters SQL query
          │
          ▼
2. Parser tokenizes and parses SQL
          │
          ▼
3. Planner creates logical plan
          │
          ▼
4. Optimizer applies transformations
          │
          ▼
5. Executor creates physical plan
          │
          ▼
6. Operators pull data through pipeline
          │
          ▼
7. Results returned as Arrow RecordBatch
```

### Example: SELECT Query

```sql
SELECT name, SUM(amount)
FROM orders
WHERE status = 'completed'
GROUP BY name
ORDER BY SUM(amount) DESC
LIMIT 10
```

**Logical Plan:**
```
Limit (10)
  └── Sort (SUM(amount) DESC)
        └── Aggregate (GROUP BY name, SUM(amount))
              └── Filter (status = 'completed')
                    └── TableScan (orders)
```

## Key Design Decisions

### 1. Apache Arrow

- Columnar in-memory format
- Zero-copy data sharing
- SIMD-optimized operations
- Interoperability with other systems

### 2. Rust

- Memory safety without GC
- Fearless concurrency
- Zero-cost abstractions
- Strong type system

### 3. Modular Crates

- Separation of concerns
- Independent testing
- Flexible deployment
- Clear dependencies

### 4. Pull-Based Execution

- Lazy evaluation
- Memory efficiency
- Natural backpressure
- Composable operators

## Performance Considerations

### Optimizations Applied

1. **Vectorization**: Batch processing with Arrow arrays
2. **Predicate Pushdown**: Filter early in pipeline
3. **Projection Pushdown**: Read only needed columns
4. **Hash-Based Operations**: O(n) joins and aggregates
5. **Index Acceleration**: Skip full scans when possible

### Memory Management

- Streaming execution (no full materialization)
- Reference counting (Arc) for shared data
- Batch size configuration
- Spill to disk (future)

## Extensibility Points

1. **New Data Sources**: Implement `DataSource` trait
2. **Custom Functions**: Add to expression evaluator
3. **New Index Types**: Implement `Index` trait
4. **Optimization Rules**: Add to planner optimizer
5. **Output Formats**: Extend CLI formatter

## Future Architecture

- **Streaming**: Real-time query processing ✅ (implemented)
- **Arrow Flight**: Network data transfer ✅ (fully implemented)
- **PostgreSQL Protocol**: Wire compatibility
- **Cost-Based Optimizer**: Statistics-driven query planning
