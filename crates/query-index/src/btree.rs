//! B-Tree index implementation
//!
//! B-Tree indexes support both equality and range queries. They maintain keys in
//! sorted order, making them ideal for:
//! - Equality lookups (=)
//! - Range queries (<, >, <=, >=, BETWEEN)
//! - ORDER BY optimization

use crate::types::{Index, IndexKey, IndexLookupResult, IndexMetadata, IndexType};
use parking_lot::RwLock;
use query_core::{QueryError, Result};
use std::collections::BTreeMap;

/// B-Tree index implementation using `std::collections::BTreeMap`
#[derive(Debug)]
pub struct BTreeIndex {
    /// Index metadata
    metadata: IndexMetadata,
    /// The actual B-Tree storing key -> row_ids mappings
    /// Using RwLock for thread-safe concurrent access
    tree: RwLock<BTreeMap<IndexKey, Vec<usize>>>,
}

impl BTreeIndex {
    /// Create a new B-Tree index
    pub fn new(metadata: IndexMetadata) -> Self {
        debug_assert_eq!(metadata.index_type, IndexType::BTree);
        Self {
            metadata,
            tree: RwLock::new(BTreeMap::new()),
        }
    }

    /// Create a new B-Tree index with the given metadata
    pub fn with_metadata(
        name: impl Into<String>,
        table_name: impl Into<String>,
        columns: Vec<String>,
        unique: bool,
    ) -> Self {
        let metadata = IndexMetadata::new(name, table_name, columns, IndexType::BTree, unique);
        Self::new(metadata)
    }

    /// Bulk load entries into the index
    pub fn bulk_load(&self, entries: impl IntoIterator<Item = (IndexKey, usize)>) -> Result<()> {
        let mut tree = self.tree.write();

        if self.metadata.unique {
            // For unique indexes, check for duplicates
            for (key, row_id) in entries {
                if let Some(existing) = tree.get(&key) {
                    if !existing.is_empty() {
                        return Err(QueryError::ExecutionError(format!(
                            "Unique constraint violation: duplicate key in index '{}'",
                            self.metadata.name
                        )));
                    }
                }
                tree.entry(key).or_insert_with(Vec::new).push(row_id);
            }
        } else {
            for (key, row_id) in entries {
                tree.entry(key).or_insert_with(Vec::new).push(row_id);
            }
        }

        Ok(())
    }

    /// Get all entries in sorted order (for debugging)
    pub fn entries(&self) -> Vec<(IndexKey, Vec<usize>)> {
        let tree = self.tree.read();
        tree.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }
}

impl Index for BTreeIndex {
    fn metadata(&self) -> &IndexMetadata {
        &self.metadata
    }

    fn lookup(&self, key: &IndexKey) -> IndexLookupResult {
        let tree = self.tree.read();
        match tree.get(key) {
            Some(row_ids) => IndexLookupResult::exact(row_ids.clone()),
            None => IndexLookupResult::empty(),
        }
    }

    fn range_scan(&self, start: Option<&IndexKey>, end: Option<&IndexKey>) -> IndexLookupResult {
        let tree = self.tree.read();

        let range_iter: Box<dyn Iterator<Item = (&IndexKey, &Vec<usize>)>> = match (start, end) {
            (None, None) => Box::new(tree.iter()),
            (Some(s), None) => Box::new(tree.range(s.clone()..)),
            (None, Some(e)) => Box::new(tree.range(..=e.clone())),
            (Some(s), Some(e)) => Box::new(tree.range(s.clone()..=e.clone())),
        };

        let row_ids: Vec<usize> = range_iter
            .flat_map(|(_, ids)| ids.iter().copied())
            .collect();

        IndexLookupResult::exact(row_ids)
    }

    fn insert(&self, key: IndexKey, row_id: usize) -> Result<()> {
        let mut tree = self.tree.write();

        if self.metadata.unique {
            if let Some(existing) = tree.get(&key) {
                if !existing.is_empty() {
                    return Err(QueryError::ExecutionError(format!(
                        "Unique constraint violation: duplicate key in index '{}'",
                        self.metadata.name
                    )));
                }
            }
        }

        tree.entry(key).or_insert_with(Vec::new).push(row_id);
        Ok(())
    }

    fn delete(&self, key: &IndexKey, row_id: usize) -> Result<()> {
        let mut tree = self.tree.write();

        if let Some(row_ids) = tree.get_mut(key) {
            row_ids.retain(|&id| id != row_id);
            if row_ids.is_empty() {
                tree.remove(key);
            }
        }

        Ok(())
    }

    fn supports_range(&self) -> bool {
        true
    }

    fn len(&self) -> usize {
        let tree = self.tree.read();
        tree.values().map(|v| v.len()).sum()
    }

    fn clear(&self) {
        let mut tree = self.tree.write();
        tree.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_index() -> BTreeIndex {
        BTreeIndex::with_metadata("idx_test", "test_table", vec!["id".to_string()], false)
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
        assert_eq!(result.row_ids, vec![0, 1, 2]);
    }

    #[test]
    fn test_unique_constraint() {
        let index =
            BTreeIndex::with_metadata("idx_unique", "test_table", vec!["id".to_string()], true);

        index.insert(IndexKey::from_i64(1), 0).unwrap();
        let result = index.insert(IndexKey::from_i64(1), 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_range_scan() {
        let index = create_test_index();

        for i in 0..10 {
            index.insert(IndexKey::from_i64(i), i as usize).unwrap();
        }

        // Range [3, 7]
        let result = index.range_scan(Some(&IndexKey::from_i64(3)), Some(&IndexKey::from_i64(7)));
        assert_eq!(result.row_ids, vec![3, 4, 5, 6, 7]);
    }

    #[test]
    fn test_range_scan_unbounded() {
        let index = create_test_index();

        for i in 0..5 {
            index.insert(IndexKey::from_i64(i), i as usize).unwrap();
        }

        // From 2 to end
        let result = index.range_scan(Some(&IndexKey::from_i64(2)), None);
        assert_eq!(result.row_ids, vec![2, 3, 4]);

        // From start to 2
        let result = index.range_scan(None, Some(&IndexKey::from_i64(2)));
        assert_eq!(result.row_ids, vec![0, 1, 2]);
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
            BTreeIndex::with_metadata("idx_name", "test_table", vec!["name".to_string()], false);

        index.insert(IndexKey::from_string("alice"), 0).unwrap();
        index.insert(IndexKey::from_string("bob"), 1).unwrap();
        index.insert(IndexKey::from_string("charlie"), 2).unwrap();

        let result = index.lookup(&IndexKey::from_string("bob"));
        assert_eq!(result.row_ids, vec![1]);

        // Range scan should return in sorted order
        let result = index.range_scan(
            Some(&IndexKey::from_string("alice")),
            Some(&IndexKey::from_string("bob")),
        );
        assert_eq!(result.row_ids, vec![0, 1]);
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
}
