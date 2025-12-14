//! Cache configuration options

use std::time::Duration;

/// Configuration for the query cache
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Maximum number of entries in the cache
    pub max_entries: usize,
    /// Maximum memory usage in bytes (approximate)
    pub max_memory_bytes: usize,
    /// Time-to-live for cache entries
    pub ttl: Duration,
    /// Whether caching is enabled
    pub enabled: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 1000,
            max_memory_bytes: 100 * 1024 * 1024, // 100 MB
            ttl: Duration::from_secs(300),       // 5 minutes
            enabled: true,
        }
    }
}

impl CacheConfig {
    /// Create a new cache configuration with custom settings
    pub fn new(max_entries: usize, max_memory_bytes: usize, ttl_secs: u64) -> Self {
        Self {
            max_entries,
            max_memory_bytes,
            ttl: Duration::from_secs(ttl_secs),
            enabled: true,
        }
    }

    /// Create a disabled cache configuration
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }

    /// Set the maximum number of entries
    pub fn with_max_entries(mut self, max_entries: usize) -> Self {
        self.max_entries = max_entries;
        self
    }

    /// Set the maximum memory usage
    pub fn with_max_memory(mut self, max_memory_bytes: usize) -> Self {
        self.max_memory_bytes = max_memory_bytes;
        self
    }

    /// Set the TTL duration
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Enable or disable the cache
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = CacheConfig::default();
        assert_eq!(config.max_entries, 1000);
        assert_eq!(config.max_memory_bytes, 100 * 1024 * 1024);
        assert_eq!(config.ttl, Duration::from_secs(300));
        assert!(config.enabled);
    }

    #[test]
    fn test_disabled_config() {
        let config = CacheConfig::disabled();
        assert!(!config.enabled);
    }

    #[test]
    fn test_builder_pattern() {
        let config = CacheConfig::default()
            .with_max_entries(500)
            .with_max_memory(50 * 1024 * 1024)
            .with_ttl(Duration::from_secs(60))
            .with_enabled(true);

        assert_eq!(config.max_entries, 500);
        assert_eq!(config.max_memory_bytes, 50 * 1024 * 1024);
        assert_eq!(config.ttl, Duration::from_secs(60));
    }
}
