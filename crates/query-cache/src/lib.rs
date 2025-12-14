//! LRU Query Result Cache for Query Engine
//!
//! This crate provides an LRU-based cache for storing query results to improve
//! performance for repeated queries.
//!
//! # Features
//!
//! - **LRU Eviction**: Least recently used entries are evicted when capacity is reached
//! - **TTL Support**: Time-to-live expiration for cache entries
//! - **Memory Limits**: Configurable memory usage limits
//! - **Thread-Safe**: Safe for concurrent access using `RwLock`
//! - **Statistics**: Track hits, misses, and evictions
//! - **Invalidation**: Support for cache invalidation on data changes
//!
//! # Example
//!
//! ```ignore
//! use query_cache::{QueryCache, CacheConfig};
//! use std::sync::Arc;
//!
//! let config = CacheConfig::default();
//! let cache = Arc::new(QueryCache::new(config));
//!
//! // Check cache before executing query
//! if let Some(result) = cache.get(&cache_key) {
//!     return Ok(result);
//! }
//!
//! // Execute query and cache result
//! let result = executor.execute(&plan).await?;
//! cache.put(cache_key, result.clone());
//! ```

pub mod cache;
pub mod config;
pub mod invalidation;
pub mod stats;

pub use cache::{CacheEntry, CacheKey, QueryCache};
pub use config::CacheConfig;
pub use invalidation::{CacheInvalidator, InvalidationEvent, NoOpInvalidator};
pub use stats::CacheStats;
