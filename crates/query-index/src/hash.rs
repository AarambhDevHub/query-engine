//! Hash index implementation
//!
//! Hash indexes provide O(1) average-case lookup time for equality queries.
//! They do NOT support range queries.
//!
//! Best used for:
//! - Primary key lookups
//! - Foreign key joins
//! - Columns with high cardinality where only equality is needed

use crate::types::{Index, IndexKey, IndexLookupResult, IndexMetadata, IndexType};
use ahash::AHashMap;
use parking_lot::RwLock;
use query_core::{QueryError, Result};

/// Hash index implementation using `ahash::AHashMap` for fast lookups
#[derive(Debug)]
pub struct HashIndex {
    /// Index metadata
    metadata: IndexMetadata,
    /// The hash map storing key -> row_ids mappings
    map: RwLock<AHashMap<IndexKey, Vec<usize>>>,
}

impl HashIndex {
    /// Create a new Hash index
    pub fn new(metadata: IndexMetadata) -> Self {
        debug_assert_eq!(metadata.index_type, IndexType::Hash);
        Self {
            metadata,
            map: RwLock::new(AHashMap::new()),
        }
    }

    /// Create a new Hash index with the given metadata
    pub fn with_metadata(
        name: impl Into<String>,
        table_name: impl Into<String>,
        columns: Vec<String>,
        unique: bool,
    ) -> Self {
        let metadata = IndexMetadata::new(name, table_name, columns, IndexType::Hash, unique);
        Self::new(metadata)
    }

    /// Bulk load entries into the index
    pub fn bulk_load(&self, entries: impl IntoIterator<Item = (IndexKey, usize)>) -> Result<()> {
        let mut map = self.map.write();

        if self.metadata.unique {
            for (key, row_id) in entries {
                if let Some(existing) = map.get(&key) {
                    if !existing.is_empty() {
                        return Err(QueryError::ExecutionError(format!(
                            "Unique constraint violation: duplicate key in index '{}'",
                            self.metadata.name
                        )));
                    }
                }
                map.entry(key).or_insert_with(Vec::new).push(row_id);
            }
        } else {
            for (key, row_id) in entries {
                map.entry(key).or_insert_with(Vec::new).push(row_id);
            }
        }

        Ok(())
    }

    /// Get all keys (for debugging)
    pub fn keys(&self) -> Vec<IndexKey> {
        let map = self.map.read();
        map.keys().cloned().collect()
    }
}

impl Index for HashIndex {
    fn metadata(&self) -> &IndexMetadata {
        &self.metadata
    }

    fn lookup(&self, key: &IndexKey) -> IndexLookupResult {
        let map = self.map.read();
        match map.get(key) {
            Some(row_ids) => IndexLookupResult::exact(row_ids.clone()),
            None => IndexLookupResult::empty(),
        }
    }

    fn range_scan(&self, _start: Option<&IndexKey>, _end: Option<&IndexKey>) -> IndexLookupResult {
        // Hash indexes do not support range scans
        IndexLookupResult::empty()
    }

    fn insert(&self, key: IndexKey, row_id: usize) -> Result<()> {
        let mut map = self.map.write();

        if self.metadata.unique {
            if let Some(existing) = map.get(&key) {
                if !existing.is_empty() {
                    return Err(QueryError::ExecutionError(format!(
                        "Unique constraint violation: duplicate key in index '{}'",
                        self.metadata.name
                    )));
                }
            }
        }

        map.entry(key).or_insert_with(Vec::new).push(row_id);
        Ok(())
    }

    fn delete(&self, key: &IndexKey, row_id: usize) -> Result<()> {
        let mut map = self.map.write();

        if let Some(row_ids) = map.get_mut(key) {
            row_ids.retain(|&id| id != row_id);
            if row_ids.is_empty() {
                map.remove(key);
            }
        }

        Ok(())
    }

    fn supports_range(&self) -> bool {
        false
    }

    fn len(&self) -> usize {
        let map = self.map.read();
        map.values().map(|v| v.len()).sum()
    }

    fn clear(&self) {
        let mut map = self.map.write();
        map.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_index() -> HashIndex {
        HashIndex::with_metadata("idx_test", "test_table", vec!["id".to_string()], false)
    }

    #[test]
    fn test_insert_and_lookup() {
        let index = create_test_index();

        index.insert(IndexKey::from_i64(1), 0).unwrap();
        index.insert(IndexKey::from_i64(2), 1).unwrap();
        index.insert(IndexKey::from_i64(3), 2).unwrap();

        let result = index.lookup(&IndexKey::from_i64(2));
        assert_eq!(result.row_ids, vec![1]);
        assert!(result.exact);
    }

    #[test]
    fn test_duplicate_keys() {
        let index = create_test_index();

        index.insert(IndexKey::from_i64(1), 0).unwrap();
        index.insert(IndexKey::from_i64(1), 1).unwrap();
        index.insert(IndexKey::from_i64(1), 2).unwrap();

        let result = index.lookup(&IndexKey::from_i64(1));
        assert_eq!(result.row_ids.len(), 3);
        assert!(result.row_ids.contains(&0));
        assert!(result.row_ids.contains(&1));
        assert!(result.row_ids.contains(&2));
    }

    #[test]
    fn test_unique_constraint() {
        let index =
            HashIndex::with_metadata("idx_unique", "test_table", vec!["id".to_string()], true);

        index.insert(IndexKey::from_i64(1), 0).unwrap();
        let result = index.insert(IndexKey::from_i64(1), 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_range_scan_not_supported() {
        let index = create_test_index();

        for i in 0..10 {
            index.insert(IndexKey::from_i64(i), i as usize).unwrap();
        }

        // Range scan should return empty for hash index
        let result = index.range_scan(Some(&IndexKey::from_i64(3)), Some(&IndexKey::from_i64(7)));
        assert!(result.row_ids.is_empty());
        assert!(!index.supports_range());
    }

    #[test]
    fn test_delete() {
        let index = create_test_index();

        index.insert(IndexKey::from_i64(1), 0).unwrap();
        index.insert(IndexKey::from_i64(1), 1).unwrap();

        index.delete(&IndexKey::from_i64(1), 0).unwrap();

        let result = index.lookup(&IndexKey::from_i64(1));
        assert_eq!(result.row_ids, vec![1]);
    }

    #[test]
    fn test_string_keys() {
        let index =
            HashIndex::with_metadata("idx_name", "test_table", vec!["name".to_string()], false);

        index.insert(IndexKey::from_string("alice"), 0).unwrap();
        index.insert(IndexKey::from_string("bob"), 1).unwrap();
        index.insert(IndexKey::from_string("charlie"), 2).unwrap();

        let result = index.lookup(&IndexKey::from_string("bob"));
        assert_eq!(result.row_ids, vec![1]);
    }

    #[test]
    fn test_bulk_load() {
        let index = create_test_index();

        let entries: Vec<_> = (0..100)
            .map(|i| (IndexKey::from_i64(i), i as usize))
            .collect();

        index.bulk_load(entries).unwrap();
        assert_eq!(index.len(), 100);

        let result = index.lookup(&IndexKey::from_i64(50));
        assert_eq!(result.row_ids, vec![50]);
    }

    #[test]
    fn test_lookup_not_found() {
        let index = create_test_index();

        index.insert(IndexKey::from_i64(1), 0).unwrap();

        let result = index.lookup(&IndexKey::from_i64(999));
        assert!(result.row_ids.is_empty());
    }

    #[test]
    fn test_clear() {
        let index = create_test_index();

        index.insert(IndexKey::from_i64(1), 0).unwrap();
        index.insert(IndexKey::from_i64(2), 1).unwrap();

        assert_eq!(index.len(), 2);

        index.clear();

        assert_eq!(index.len(), 0);
        assert!(index.is_empty());
    }
}
