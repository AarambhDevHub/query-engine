//! Cache invalidation support
//!
//! Provides traits and utilities for invalidating cached query results
//! when underlying data changes.

/// Trait for components that can trigger cache invalidation
pub trait CacheInvalidator: Send + Sync {
    /// Invalidate all cache entries for a specific table
    fn invalidate_table(&self, table_name: &str);

    /// Invalidate all cache entries
    fn invalidate_all(&self);

    /// Check if the invalidator is enabled
    fn is_enabled(&self) -> bool;
}

/// Events that can trigger cache invalidation
#[derive(Debug, Clone)]
pub enum InvalidationEvent {
    /// A table's data was modified (insert, update, delete)
    TableModified { table_name: String },
    /// A table was dropped
    TableDropped { table_name: String },
    /// Schema changed
    SchemaChanged { table_name: String },
    /// Force invalidation of everything
    InvalidateAll,
}

impl InvalidationEvent {
    /// Create a table modification event
    pub fn table_modified(table_name: impl Into<String>) -> Self {
        Self::TableModified {
            table_name: table_name.into(),
        }
    }

    /// Create a table dropped event
    pub fn table_dropped(table_name: impl Into<String>) -> Self {
        Self::TableDropped {
            table_name: table_name.into(),
        }
    }
}

/// A simple no-op invalidator that does nothing
#[derive(Debug, Default, Clone)]
pub struct NoOpInvalidator;

impl CacheInvalidator for NoOpInvalidator {
    fn invalidate_table(&self, _table_name: &str) {
        // No-op
    }

    fn invalidate_all(&self) {
        // No-op
    }

    fn is_enabled(&self) -> bool {
        false
    }
}

/// Generate a cache key prefix for table-based invalidation
pub fn table_cache_prefix(table_name: &str) -> String {
    format!("table:{}", table_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalidation_event() {
        let event = InvalidationEvent::table_modified("users");
        match event {
            InvalidationEvent::TableModified { table_name } => {
                assert_eq!(table_name, "users");
            }
            _ => panic!("Expected TableModified event"),
        }
    }

    #[test]
    fn test_noop_invalidator() {
        let invalidator = NoOpInvalidator;
        invalidator.invalidate_table("users"); // Should not panic
        invalidator.invalidate_all(); // Should not panic
        assert!(!invalidator.is_enabled());
    }
}
