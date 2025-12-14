//! Query Caching Example
//!
//! Demonstrates LRU query caching with TTL and statistics.

use anyhow::Result;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use query_cache::{CacheConfig, CacheKey, QueryCache};
use std::sync::Arc;
use std::time::Duration;

fn main() -> Result<()> {
    println!("=== Query Caching Example ===\n");

    // Example 1: Basic cache operations
    basic_caching()?;

    // Example 2: Cache with TTL
    cache_with_ttl()?;

    // Example 3: Cache statistics
    cache_statistics()?;

    println!("\n=== All caching examples completed! ===");
    Ok(())
}

/// Example 1: Basic cache put/get operations
fn basic_caching() -> Result<()> {
    println!("--- Example 1: Basic Caching ---\n");

    // Create cache with default config
    let config = CacheConfig::default();
    let cache = QueryCache::new(config);

    println!("Cache created with:");
    println!("  Max entries: 1000");
    println!("  TTL: 5 minutes");
    println!("  Max memory: 100 MB\n");

    // Create sample query result
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )?;

    // Store in cache
    let sql = "SELECT id, name FROM users WHERE active = true";
    let key = CacheKey::from_sql(sql);

    println!("Caching query: {}", sql);
    cache.put(key.clone(), vec![batch]);

    // Retrieve from cache
    if let Some(cached) = cache.get(&key) {
        println!("Cache HIT! Retrieved {} batches", cached.len());
        println!("  Rows in cached result: {}", cached[0].num_rows());
    } else {
        println!("Cache MISS");
    }

    // Try non-existent key
    let missing_key = CacheKey::from_sql("SELECT * FROM other_table");
    if cache.get(&missing_key).is_none() {
        println!("Expected MISS for uncached query");
    }

    Ok(())
}

/// Example 2: Cache with custom TTL
fn cache_with_ttl() -> Result<()> {
    println!("\n--- Example 2: Cache with TTL ---\n");

    // Create cache with short TTL for demo
    let config = CacheConfig {
        enabled: true,
        max_entries: 100,
        max_memory_bytes: 10 * 1024 * 1024, // 10 MB
        ttl: Duration::from_millis(100),    // Very short TTL for demo
    };

    let cache = QueryCache::new(config);

    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![42]))])?;

    let key = CacheKey::from_sql("SELECT x FROM t");

    // Cache the result
    cache.put(key.clone(), vec![batch]);
    println!("Cached result with 100ms TTL");

    // Immediate retrieval should hit
    if cache.get(&key).is_some() {
        println!("Immediate get: HIT");
    }

    // Wait for TTL to expire
    println!("Waiting for TTL expiration...");
    std::thread::sleep(Duration::from_millis(150));

    // Should miss after TTL
    if cache.get(&key).is_none() {
        println!("After TTL: MISS (entry expired)");
    }

    Ok(())
}

/// Example 3: Cache statistics
fn cache_statistics() -> Result<()> {
    println!("\n--- Example 3: Cache Statistics ---\n");

    let cache = QueryCache::new(CacheConfig::default());

    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));

    // Perform various cache operations
    for i in 0..5 {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![i as i64]))],
        )?;
        let key = CacheKey::from_sql(&format!("SELECT {} FROM t", i));
        cache.put(key, vec![batch]);
    }
    println!("Cached 5 different queries");

    // Generate some hits and misses
    for i in 0..10 {
        let key = CacheKey::from_sql(&format!("SELECT {} FROM t", i % 7));
        let _ = cache.get(&key);
    }
    println!("Performed 10 lookups (mix of hits and misses)\n");

    // Display statistics
    let stats = cache.stats();
    println!("Cache Statistics:");
    println!("  Hits:      {}", stats.hits());
    println!("  Misses:    {}", stats.misses());
    println!("  Hit Rate:  {:.1}%", stats.hit_rate() * 100.0);
    println!("  Evictions: {}", stats.evictions());
    println!("  Entries:   {}", stats.entry_count());
    println!("  Memory:    {} bytes", stats.memory_bytes());

    // Clear cache
    cache.clear();
    println!("\nCache cleared!");

    let stats_after = cache.stats();
    println!("After clear:");
    println!("  Entries: {}", stats_after.entry_count());

    Ok(())
}
