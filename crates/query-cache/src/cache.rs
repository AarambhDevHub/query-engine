//! LRU Query Cache implementation

use crate::config::CacheConfig;
use crate::stats::CacheStats;
use ahash::AHasher;
use arrow::record_batch::RecordBatch;
use lru::LruCache;
use parking_lot::RwLock;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Key for cache lookups, derived from query SQL and/or plan hash
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    /// Hash of the SQL query string
    sql_hash: u64,
    /// Optional hash of the logical plan (for normalized queries)
    plan_hash: Option<u64>,
}

impl CacheKey {
    /// Create a new cache key from SQL string
    pub fn from_sql(sql: &str) -> Self {
        let mut hasher = AHasher::default();
        sql.hash(&mut hasher);
        Self {
            sql_hash: hasher.finish(),
            plan_hash: None,
        }
    }

    /// Create a cache key from SQL with an optional plan hash
    pub fn new(sql: &str, plan_hash: Option<u64>) -> Self {
        let mut hasher = AHasher::default();
        sql.hash(&mut hasher);
        Self {
            sql_hash: hasher.finish(),
            plan_hash,
        }
    }

    /// Create a cache key from pre-computed hashes
    pub fn from_hashes(sql_hash: u64, plan_hash: Option<u64>) -> Self {
        Self {
            sql_hash,
            plan_hash,
        }
    }
}

/// Entry stored in the cache
#[derive(Debug, Clone)]
pub struct CacheEntry {
    /// Cached query results
    pub batches: Vec<RecordBatch>,
    /// When this entry was created
    pub created_at: Instant,
    /// Approximate size in bytes
    pub size_bytes: usize,
    /// Number of times this entry was accessed
    pub hit_count: u64,
}

impl CacheEntry {
    /// Create a new cache entry
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        let size_bytes = estimate_batch_size(&batches);
        Self {
            batches,
            created_at: Instant::now(),
            size_bytes,
            hit_count: 0,
        }
    }

    /// Check if this entry has expired based on TTL
    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.created_at.elapsed() > ttl
    }

    /// Get the age of this entry
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }
}

/// Estimate the size of record batches in bytes
fn estimate_batch_size(batches: &[RecordBatch]) -> usize {
    batches
        .iter()
        .map(|batch| {
            batch
                .columns()
                .iter()
                .map(|col| col.get_array_memory_size())
                .sum::<usize>()
        })
        .sum()
}

/// Thread-safe LRU cache for query results
pub struct QueryCache {
    /// The underlying LRU cache
    cache: RwLock<LruCache<CacheKey, CacheEntry>>,
    /// Cache configuration
    config: CacheConfig,
    /// Cache statistics
    stats: Arc<CacheStats>,
    /// Current memory usage
    memory_used: RwLock<usize>,
}

impl QueryCache {
    /// Create a new query cache with the given configuration
    pub fn new(config: CacheConfig) -> Self {
        let capacity = NonZeroUsize::new(config.max_entries).unwrap_or(NonZeroUsize::MIN);
        Self {
            cache: RwLock::new(LruCache::new(capacity)),
            config,
            stats: Arc::new(CacheStats::new()),
            memory_used: RwLock::new(0),
        }
    }

    /// Create a cache with default configuration
    pub fn with_defaults() -> Self {
        Self::new(CacheConfig::default())
    }

    /// Check if caching is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get an entry from the cache
    pub fn get(&self, key: &CacheKey) -> Option<Vec<RecordBatch>> {
        if !self.config.enabled {
            return None;
        }

        let mut cache = self.cache.write();

        // Check if entry exists and get a clone of batches if valid
        let result = if let Some(entry) = cache.get_mut(key) {
            // Check TTL expiration
            if entry.is_expired(self.config.ttl) {
                None
            } else {
                // Record hit and clone batches
                entry.hit_count += 1;
                Some(entry.batches.clone())
            }
        } else {
            None
        };

        // Handle expired entry removal
        if result.is_none() {
            if cache
                .peek(key)
                .map(|e| e.is_expired(self.config.ttl))
                .unwrap_or(false)
            {
                cache.pop(key);
                self.stats.record_expiration();
            }
            self.stats.record_miss();
            return None;
        }

        // Update stats after releasing mutable borrow on entry
        self.stats.record_hit();
        self.stats.set_entry_count(cache.len() as u64);
        self.stats.set_memory_bytes(*self.memory_used.read() as u64);

        result
    }

    /// Insert an entry into the cache
    pub fn put(&self, key: CacheKey, batches: Vec<RecordBatch>) {
        if !self.config.enabled {
            return;
        }

        let entry = CacheEntry::new(batches);
        let entry_size = entry.size_bytes;

        // Check if this single entry exceeds memory limit
        if entry_size > self.config.max_memory_bytes {
            return; // Don't cache entries that are too large
        }

        let mut cache = self.cache.write();
        let mut memory_used = self.memory_used.write();

        // Evict entries if we would exceed memory limit
        while *memory_used + entry_size > self.config.max_memory_bytes && !cache.is_empty() {
            if let Some((_, evicted)) = cache.pop_lru() {
                *memory_used = memory_used.saturating_sub(evicted.size_bytes);
                self.stats.record_eviction();
            }
        }

        // If key already exists, update memory tracking
        if let Some(old_entry) = cache.peek(&key) {
            *memory_used = memory_used.saturating_sub(old_entry.size_bytes);
        }

        // Insert the new entry
        if let Some(evicted) = cache.push(key, entry) {
            *memory_used = memory_used.saturating_sub(evicted.1.size_bytes);
            self.stats.record_eviction();
        }

        *memory_used += entry_size;

        // Capture values before releasing locks
        let entry_count = cache.len() as u64;
        let mem_bytes = *memory_used as u64;

        // Release locks
        drop(memory_used);
        drop(cache);

        // Update stats without holding locks
        self.stats.set_entry_count(entry_count);
        self.stats.set_memory_bytes(mem_bytes);
    }

    /// Remove an entry from the cache
    pub fn invalidate(&self, key: &CacheKey) -> bool {
        let removed;
        let entry_count;
        let mem_used;
        {
            let mut cache = self.cache.write();
            let mut memory_used = self.memory_used.write();

            if let Some(entry) = cache.pop(key) {
                *memory_used = memory_used.saturating_sub(entry.size_bytes);
                removed = true;
            } else {
                removed = false;
            }
            entry_count = cache.len() as u64;
            mem_used = *memory_used as u64;
        }

        if removed {
            self.stats.set_entry_count(entry_count);
            self.stats.set_memory_bytes(mem_used);
        }
        removed
    }

    /// Clear all entries from the cache
    pub fn clear(&self) {
        let eviction_count;
        {
            let mut cache = self.cache.write();
            let mut memory_used = self.memory_used.write();

            eviction_count = cache.len() as u64;
            cache.clear();
            *memory_used = 0;
        }

        // Record evictions after releasing locks
        for _ in 0..eviction_count {
            self.stats.record_eviction();
        }

        // Update stats after locks are released
        self.stats.set_entry_count(0);
        self.stats.set_memory_bytes(0);
    }

    /// Get cache statistics
    pub fn stats(&self) -> Arc<CacheStats> {
        Arc::clone(&self.stats)
    }

    /// Get current number of entries
    pub fn len(&self) -> usize {
        self.cache.read().len()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.cache.read().is_empty()
    }

    /// Get current memory usage in bytes
    pub fn memory_used(&self) -> usize {
        *self.memory_used.read()
    }

    /// Get the cache configuration
    pub fn config(&self) -> &CacheConfig {
        &self.config
    }

    /// Update internal statistics
    fn update_stats(&self, cache: &LruCache<CacheKey, CacheEntry>) {
        self.stats.set_entry_count(cache.len() as u64);
        self.stats.set_memory_bytes(*self.memory_used.read() as u64);
    }

    /// Expire entries that have exceeded TTL
    pub fn expire_stale(&self) -> usize {
        let mut cache = self.cache.write();
        let mut memory_used = self.memory_used.write();
        let ttl = self.config.ttl;

        // Collect expired keys
        let expired_keys: Vec<CacheKey> = cache
            .iter()
            .filter(|(_, entry)| entry.is_expired(ttl))
            .map(|(key, _)| key.clone())
            .collect();

        let count = expired_keys.len();

        // Remove expired entries
        for key in expired_keys {
            if let Some(entry) = cache.pop(&key) {
                *memory_used = memory_used.saturating_sub(entry.size_bytes);
                self.stats.record_expiration();
            }
        }

        self.update_stats(&cache);
        count
    }
}

impl std::fmt::Debug for QueryCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryCache")
            .field("enabled", &self.config.enabled)
            .field("max_entries", &self.config.max_entries)
            .field("max_memory_bytes", &self.config.max_memory_bytes)
            .field("ttl", &self.config.ttl)
            .field("current_entries", &self.len())
            .field("memory_used", &self.memory_used())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::thread;
    use std::time::Duration;

    fn create_test_batch(values: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let array = Arc::new(Int64Array::from(values));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    #[test]
    fn test_cache_key_from_sql() {
        let key1 = CacheKey::from_sql("SELECT * FROM users");
        let key2 = CacheKey::from_sql("SELECT * FROM users");
        let key3 = CacheKey::from_sql("SELECT * FROM orders");

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_cache_put_get() {
        let cache = QueryCache::with_defaults();
        let key = CacheKey::from_sql("SELECT * FROM users");
        let batch = create_test_batch(vec![1, 2, 3]);

        cache.put(key.clone(), vec![batch.clone()]);

        let result = cache.get(&key);
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn test_cache_miss() {
        let cache = QueryCache::with_defaults();
        let key = CacheKey::from_sql("SELECT * FROM nonexistent");

        let result = cache.get(&key);
        assert!(result.is_none());
        assert_eq!(cache.stats().misses(), 1);
    }

    #[test]
    fn test_cache_hit_stats() {
        let cache = QueryCache::with_defaults();
        let key = CacheKey::from_sql("SELECT * FROM users");
        let batch = create_test_batch(vec![1, 2, 3]);

        cache.put(key.clone(), vec![batch]);

        // First get
        cache.get(&key);
        // Second get
        cache.get(&key);

        assert_eq!(cache.stats().hits(), 2);
        assert_eq!(cache.stats().hit_rate(), 1.0);
    }

    #[test]
    fn test_cache_invalidate() {
        let cache = QueryCache::with_defaults();
        let key = CacheKey::from_sql("SELECT * FROM users");
        let batch = create_test_batch(vec![1, 2, 3]);

        cache.put(key.clone(), vec![batch]);
        assert_eq!(cache.len(), 1);

        let removed = cache.invalidate(&key);
        assert!(removed);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_cache_clear() {
        let cache = QueryCache::with_defaults();

        for i in 0..10 {
            let key = CacheKey::from_sql(&format!("SELECT {} FROM users", i));
            let batch = create_test_batch(vec![i]);
            cache.put(key, vec![batch]);
        }

        assert_eq!(cache.len(), 10);
        cache.clear();
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.memory_used(), 0);
    }

    #[test]
    fn test_cache_disabled() {
        let config = CacheConfig::disabled();
        let cache = QueryCache::new(config);
        let key = CacheKey::from_sql("SELECT * FROM users");
        let batch = create_test_batch(vec![1, 2, 3]);

        cache.put(key.clone(), vec![batch]);
        assert_eq!(cache.len(), 0);

        let result = cache.get(&key);
        assert!(result.is_none());
    }

    #[test]
    fn test_cache_lru_eviction() {
        let config = CacheConfig::default().with_max_entries(3);
        let cache = QueryCache::new(config);

        // Insert 4 entries with capacity 3
        for i in 0..4 {
            let key = CacheKey::from_sql(&format!("SELECT {} FROM users", i));
            let batch = create_test_batch(vec![i]);
            cache.put(key, vec![batch]);
        }

        // Should have evicted the oldest entry
        assert_eq!(cache.len(), 3);
        assert!(cache.stats().evictions() >= 1);
    }

    #[test]
    fn test_cache_ttl_expiration() {
        let config = CacheConfig::default().with_ttl(Duration::from_millis(50));
        let cache = QueryCache::new(config);
        let key = CacheKey::from_sql("SELECT * FROM users");
        let batch = create_test_batch(vec![1, 2, 3]);

        cache.put(key.clone(), vec![batch]);

        // Should get it immediately
        assert!(cache.get(&key).is_some());

        // Wait for TTL to expire
        thread::sleep(Duration::from_millis(100));

        // Should be expired now
        assert!(cache.get(&key).is_none());
        assert_eq!(cache.stats().expirations(), 1);
    }

    #[test]
    fn test_concurrent_access() {
        let cache = Arc::new(QueryCache::with_defaults());
        let mut handles = vec![];

        // Spawn multiple threads accessing the cache
        for i in 0..10 {
            let cache_clone = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                let key = CacheKey::from_sql(&format!("SELECT {} FROM users", i));
                let batch = create_test_batch(vec![i]);
                cache_clone.put(key.clone(), vec![batch]);
                cache_clone.get(&key);
            }));
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Should have all entries
        assert_eq!(cache.len(), 10);
    }

    #[test]
    fn test_memory_tracking() {
        let cache = QueryCache::with_defaults();
        let key = CacheKey::from_sql("SELECT * FROM users");
        let batch = create_test_batch(vec![1, 2, 3, 4, 5]);

        assert_eq!(cache.memory_used(), 0);

        cache.put(key.clone(), vec![batch]);

        assert!(cache.memory_used() > 0);

        cache.invalidate(&key);
        assert_eq!(cache.memory_used(), 0);
    }

    #[test]
    fn test_expire_stale() {
        let config = CacheConfig::default().with_ttl(Duration::from_millis(50));
        let cache = QueryCache::new(config);

        // Insert entries
        for i in 0..5 {
            let key = CacheKey::from_sql(&format!("SELECT {} FROM users", i));
            let batch = create_test_batch(vec![i]);
            cache.put(key, vec![batch]);
        }

        assert_eq!(cache.len(), 5);

        // Wait for TTL
        thread::sleep(Duration::from_millis(100));

        // Expire stale entries
        let expired = cache.expire_stale();
        assert_eq!(expired, 5);
        assert_eq!(cache.len(), 0);
    }
}
