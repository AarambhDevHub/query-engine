//! Cached Query Executor
//!
//! Wraps the standard QueryExecutor with an LRU cache for repeated queries.

use crate::executor::QueryExecutor;
use crate::physical_plan::PhysicalPlan;
use arrow::record_batch::RecordBatch;
use query_cache::{CacheConfig, CacheKey, CacheStats, QueryCache};
use query_core::Result;
use std::sync::Arc;

/// A query executor with built-in caching support
pub struct CachedQueryExecutor {
    /// The underlying query executor
    executor: QueryExecutor,
    /// The query cache
    cache: Arc<QueryCache>,
}

impl CachedQueryExecutor {
    /// Create a new cached executor with the given cache configuration
    pub fn new(config: CacheConfig) -> Self {
        Self {
            executor: QueryExecutor::new(),
            cache: Arc::new(QueryCache::new(config)),
        }
    }

    /// Create a cached executor with default configuration
    pub fn with_defaults() -> Self {
        Self::new(CacheConfig::default())
    }

    /// Create a cached executor with a shared cache
    pub fn with_cache(cache: Arc<QueryCache>) -> Self {
        Self {
            executor: QueryExecutor::new(),
            cache,
        }
    }

    /// Execute a query with caching support
    ///
    /// # Arguments
    /// * `sql` - The original SQL query string (used for cache key)
    /// * `plan` - The physical plan to execute
    ///
    /// # Returns
    /// The query results, either from cache or freshly executed
    pub async fn execute_cached(&self, sql: &str, plan: &PhysicalPlan) -> Result<Vec<RecordBatch>> {
        let cache_key = CacheKey::from_sql(sql);

        // Try cache first
        if let Some(cached_result) = self.cache.get(&cache_key) {
            return Ok(cached_result);
        }

        // Execute the query
        let result = self.executor.execute(plan).await?;

        // Cache the result
        self.cache.put(cache_key, result.clone());

        Ok(result)
    }

    /// Execute a query with a custom cache key
    pub async fn execute_with_key(
        &self,
        cache_key: CacheKey,
        plan: &PhysicalPlan,
    ) -> Result<Vec<RecordBatch>> {
        // Try cache first
        if let Some(cached_result) = self.cache.get(&cache_key) {
            return Ok(cached_result);
        }

        // Execute the query
        let result = self.executor.execute(plan).await?;

        // Cache the result
        self.cache.put(cache_key, result.clone());

        Ok(result)
    }

    /// Execute without caching (bypass cache)
    pub async fn execute_uncached(&self, plan: &PhysicalPlan) -> Result<Vec<RecordBatch>> {
        self.executor.execute(plan).await
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> Arc<CacheStats> {
        self.cache.stats()
    }

    /// Get the underlying cache
    pub fn cache(&self) -> &Arc<QueryCache> {
        &self.cache
    }

    /// Invalidate a cached query by its SQL
    pub fn invalidate(&self, sql: &str) -> bool {
        let cache_key = CacheKey::from_sql(sql);
        self.cache.invalidate(&cache_key)
    }

    /// Clear all cached entries
    pub fn clear_cache(&self) {
        self.cache.clear();
    }

    /// Check if caching is enabled
    pub fn is_cache_enabled(&self) -> bool {
        self.cache.is_enabled()
    }

    /// Get the number of cached entries
    pub fn cache_size(&self) -> usize {
        self.cache.len()
    }

    /// Get current cache memory usage in bytes
    pub fn cache_memory_used(&self) -> usize {
        self.cache.memory_used()
    }
}

impl Default for CachedQueryExecutor {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::DataSource;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use query_core::Schema;
    use std::sync::Arc as StdArc;
    use std::time::Duration;

    struct TestDataSource {
        batches: Vec<RecordBatch>,
        schema: Schema,
    }

    impl std::fmt::Debug for TestDataSource {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("TestDataSource").finish()
        }
    }

    impl DataSource for TestDataSource {
        fn scan(&self) -> Result<Vec<RecordBatch>> {
            Ok(self.batches.clone())
        }

        fn schema(&self) -> &Schema {
            &self.schema
        }
    }

    fn create_test_batch() -> RecordBatch {
        let schema = StdArc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let array = StdArc::new(Int64Array::from(vec![1, 2, 3]));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    fn create_test_schema() -> Schema {
        Schema::from_arrow(&ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]))
    }

    #[tokio::test]
    async fn test_cached_execution() {
        let executor = CachedQueryExecutor::with_defaults();
        let batch = create_test_batch();
        let schema = create_test_schema();
        let source: Arc<dyn DataSource> = Arc::new(TestDataSource {
            batches: vec![batch],
            schema: schema.clone(),
        });

        let plan = PhysicalPlan::Scan { source, schema };

        // First execution - cache miss
        let result1 = executor.execute_cached("SELECT * FROM test", &plan).await;
        assert!(result1.is_ok());
        assert_eq!(executor.cache_stats().misses(), 1);
        assert_eq!(executor.cache_stats().hits(), 0);

        // Second execution with same SQL - cache hit
        let result2 = executor.execute_cached("SELECT * FROM test", &plan).await;
        assert!(result2.is_ok());
        assert_eq!(executor.cache_stats().hits(), 1);
    }

    #[tokio::test]
    async fn test_cache_stats() {
        let executor = CachedQueryExecutor::with_defaults();

        assert_eq!(executor.cache_size(), 0);
        assert!(executor.is_cache_enabled());
        assert_eq!(executor.cache_memory_used(), 0);
    }

    #[tokio::test]
    async fn test_cache_disabled() {
        let config = CacheConfig::disabled();
        let executor = CachedQueryExecutor::new(config);

        assert!(!executor.is_cache_enabled());
    }

    #[tokio::test]
    async fn test_invalidate_and_clear() {
        let executor = CachedQueryExecutor::with_defaults();

        // Manually put something in cache to test invalidation
        executor.cache.put(
            CacheKey::from_sql("SELECT * FROM users"),
            vec![create_test_batch()],
        );

        assert_eq!(executor.cache_size(), 1);

        executor.invalidate("SELECT * FROM users");
        assert_eq!(executor.cache_size(), 0);

        // Add entries and clear
        executor
            .cache
            .put(CacheKey::from_sql("SELECT 1"), vec![create_test_batch()]);
        executor
            .cache
            .put(CacheKey::from_sql("SELECT 2"), vec![create_test_batch()]);
        assert_eq!(executor.cache_size(), 2);

        executor.clear_cache();
        assert_eq!(executor.cache_size(), 0);
    }
}
