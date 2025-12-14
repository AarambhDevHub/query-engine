# Query Cache Guide

Query Engine includes an LRU-based cache for storing query results to improve performance for repeated queries.

## Features

- **LRU Eviction**: Least recently used entries are automatically evicted when capacity is reached
- **TTL Support**: Time-to-live expiration for cache entries (default: 5 minutes)
- **Memory Limits**: Configurable maximum memory usage (default: 100MB)
- **Thread-Safe**: Safe for concurrent access using `RwLock`
- **Statistics**: Track cache hits, misses, evictions, and hit rate

## Quick Start

### Using CachedQueryExecutor

```rust
use query_executor::{CachedQueryExecutor, PhysicalPlan};
use query_cache::CacheConfig;

// Create executor with default cache settings
let executor = CachedQueryExecutor::with_defaults();

// Or with custom configuration
let config = CacheConfig::default()
    .with_max_entries(500)
    .with_max_memory(50 * 1024 * 1024)  // 50MB
    .with_ttl(Duration::from_secs(60));
let executor = CachedQueryExecutor::new(config);

// Execute with caching
let result = executor.execute_cached("SELECT * FROM users", &plan).await?;

// Check cache stats
let stats = executor.cache_stats();
println!("Hit rate: {:.2}%", stats.hit_rate() * 100.0);
```

### Using QueryCache Directly

```rust
use query_cache::{QueryCache, CacheKey, CacheConfig};
use std::sync::Arc;

// Create cache
let cache = Arc::new(QueryCache::with_defaults());

// Create cache key from SQL
let key = CacheKey::from_sql("SELECT * FROM users WHERE id = 1");

// Check cache
if let Some(result) = cache.get(&key) {
    return Ok(result);
}

// Execute query and cache result
let result = executor.execute(&plan).await?;
cache.put(key, result.clone());
```

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `max_entries` | 1000 | Maximum number of cached queries |
| `max_memory_bytes` | 100MB | Maximum memory for cached results |
| `ttl` | 5 minutes | Time-to-live for cache entries |
| `enabled` | true | Enable/disable caching |

### Configuration Examples

```rust
// Disable caching
let config = CacheConfig::disabled();

// Small cache for development
let config = CacheConfig::default()
    .with_max_entries(100)
    .with_max_memory(10 * 1024 * 1024);

// Long TTL for read-heavy workloads
let config = CacheConfig::default()
    .with_ttl(Duration::from_secs(3600));  // 1 hour
```

## Cache Statistics

```rust
let stats = cache.stats();

println!("Hits: {}", stats.hits());
println!("Misses: {}", stats.misses());
println!("Hit Rate: {:.2}%", stats.hit_rate() * 100.0);
println!("Evictions: {}", stats.evictions());
println!("Expirations: {}", stats.expirations());
println!("Entries: {}", stats.entry_count());
println!("Memory: {} bytes", stats.memory_bytes());
```

## Cache Management

```rust
// Invalidate a specific query
cache.invalidate(&CacheKey::from_sql("SELECT * FROM users"));

// Clear all cached entries
cache.clear();

// Manually expire stale entries
let expired_count = cache.expire_stale();
```

## Best Practices

1. **Use for read-heavy workloads**: Caching benefits queries that are repeated frequently
2. **Invalidate on writes**: Clear relevant cache entries when underlying data changes
3. **Monitor hit rate**: A low hit rate may indicate the cache is too small
4. **Set appropriate TTL**: Balance freshness vs. performance
5. **Use memory limits**: Prevent cache from consuming too much memory

## Cache Keys

Cache keys are generated from the SQL query string using a fast hash (AHash). Two identical SQL strings will produce the same cache key.

```rust
// These produce the same key
let key1 = CacheKey::from_sql("SELECT * FROM users");
let key2 = CacheKey::from_sql("SELECT * FROM users");
assert_eq!(key1, key2);

// With plan hash for normalized queries
let key = CacheKey::new("SELECT * FROM users", Some(plan_hash));
```

## Thread Safety

`QueryCache` is thread-safe and can be shared across multiple threads:

```rust
let cache = Arc::new(QueryCache::with_defaults());

// Share across threads
let cache_clone = Arc::clone(&cache);
tokio::spawn(async move {
    cache_clone.put(key, results);
});
```

## Performance Considerations

- Cache lookups are O(1) average time
- Memory estimation is approximate (based on Arrow array sizes)
- TTL checks happen on access, not in background
- Use `expire_stale()` for periodic cleanup if needed
