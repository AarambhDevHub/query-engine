//! Flight-specific cache utilities
//!
//! Utilities for caching queries from Arrow Flight sources.

use crate::CacheKey;
use ahash::AHasher;
use std::hash::{Hash, Hasher};

/// Create a cache key for a Flight query
///
/// This creates a cache key that includes both the endpoint and the query,
/// ensuring that the same query to different servers is cached separately.
pub fn flight_cache_key(endpoint: &str, query: &str) -> CacheKey {
    let mut hasher = AHasher::default();
    endpoint.hash(&mut hasher);
    query.hash(&mut hasher);
    let combined_hash = hasher.finish();

    CacheKey::from_hashes(combined_hash, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flight_cache_key() {
        let key1 = flight_cache_key("http://localhost:50051", "users");
        let key2 = flight_cache_key("http://localhost:50051", "users");
        let key3 = flight_cache_key("http://localhost:50052", "users");
        let key4 = flight_cache_key("http://localhost:50051", "orders");

        // Same endpoint and query should produce same key
        assert_eq!(key1, key2);
        // Different endpoint should produce different key
        assert_ne!(key1, key3);
        // Different query should produce different key
        assert_ne!(key1, key4);
    }
}
