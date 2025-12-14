//! Cache statistics tracking

use std::sync::atomic::{AtomicU64, Ordering};

/// Statistics for cache performance monitoring
#[derive(Debug, Default)]
pub struct CacheStats {
    /// Number of cache hits
    hits: AtomicU64,
    /// Number of cache misses
    misses: AtomicU64,
    /// Number of entries evicted
    evictions: AtomicU64,
    /// Number of entries expired by TTL
    expirations: AtomicU64,
    /// Current number of entries
    entry_count: AtomicU64,
    /// Approximate memory usage in bytes
    memory_bytes: AtomicU64,
}

impl CacheStats {
    /// Create new cache statistics
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a cache hit
    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache miss
    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an eviction
    pub fn record_eviction(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a TTL expiration
    pub fn record_expiration(&self) {
        self.expirations.fetch_add(1, Ordering::Relaxed);
    }

    /// Update entry count
    pub fn set_entry_count(&self, count: u64) {
        self.entry_count.store(count, Ordering::Relaxed);
    }

    /// Update memory usage
    pub fn set_memory_bytes(&self, bytes: u64) {
        self.memory_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Get hit count
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Get miss count
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Get eviction count
    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    /// Get expiration count
    pub fn expirations(&self) -> u64 {
        self.expirations.load(Ordering::Relaxed)
    }

    /// Get current entry count
    pub fn entry_count(&self) -> u64 {
        self.entry_count.load(Ordering::Relaxed)
    }

    /// Get memory usage in bytes
    pub fn memory_bytes(&self) -> u64 {
        self.memory_bytes.load(Ordering::Relaxed)
    }

    /// Calculate hit rate (0.0 to 1.0)
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits() as f64;
        let total = hits + self.misses() as f64;
        if total == 0.0 {
            0.0
        } else {
            hits / total
        }
    }

    /// Get total requests (hits + misses)
    pub fn total_requests(&self) -> u64 {
        self.hits() + self.misses()
    }

    /// Reset all statistics
    pub fn reset(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.evictions.store(0, Ordering::Relaxed);
        self.expirations.store(0, Ordering::Relaxed);
    }
}

impl Clone for CacheStats {
    fn clone(&self) -> Self {
        Self {
            hits: AtomicU64::new(self.hits()),
            misses: AtomicU64::new(self.misses()),
            evictions: AtomicU64::new(self.evictions()),
            expirations: AtomicU64::new(self.expirations()),
            entry_count: AtomicU64::new(self.entry_count()),
            memory_bytes: AtomicU64::new(self.memory_bytes()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats_recording() {
        let stats = CacheStats::new();

        stats.record_hit();
        stats.record_hit();
        stats.record_miss();

        assert_eq!(stats.hits(), 2);
        assert_eq!(stats.misses(), 1);
        assert_eq!(stats.total_requests(), 3);
    }

    #[test]
    fn test_hit_rate() {
        let stats = CacheStats::new();

        // Empty stats should return 0.0
        assert_eq!(stats.hit_rate(), 0.0);

        // 2 hits, 2 misses = 50% hit rate
        stats.record_hit();
        stats.record_hit();
        stats.record_miss();
        stats.record_miss();

        assert!((stats.hit_rate() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_reset() {
        let stats = CacheStats::new();
        stats.record_hit();
        stats.record_miss();
        stats.record_eviction();

        stats.reset();

        assert_eq!(stats.hits(), 0);
        assert_eq!(stats.misses(), 0);
        assert_eq!(stats.evictions(), 0);
    }

    #[test]
    fn test_clone() {
        let stats = CacheStats::new();
        stats.record_hit();
        stats.record_hit();
        stats.record_miss();

        let cloned = stats.clone();
        assert_eq!(cloned.hits(), 2);
        assert_eq!(cloned.misses(), 1);
    }
}
