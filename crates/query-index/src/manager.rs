//! Index manager for creating, dropping, and retrieving indexes
//!
//! The IndexManager is responsible for:
//! - Creating and dropping indexes
//! - Storing index metadata
//! - Providing access to indexes for query execution
//! - Managing index lifecycle

use crate::btree::BTreeIndex;
use crate::hash::HashIndex;
use crate::types::{Index, IndexMetadata, IndexType};
use parking_lot::RwLock;
use query_core::{QueryError, Result};
use std::collections::HashMap;
use std::sync::Arc;

/// Manager for all indexes in the query engine
#[derive(Debug, Default)]
pub struct IndexManager {
    /// Map of index name -> index implementation
    indexes: RwLock<HashMap<String, Arc<dyn Index>>>,
    /// Map of table name -> list of index names
    table_indexes: RwLock<HashMap<String, Vec<String>>>,
}

impl IndexManager {
    /// Create a new index manager
    pub fn new() -> Self {
        Self {
            indexes: RwLock::new(HashMap::new()),
            table_indexes: RwLock::new(HashMap::new()),
        }
    }

    /// Create a new B-Tree index
    pub fn create_btree_index(
        &self,
        name: impl Into<String>,
        table_name: impl Into<String>,
        columns: Vec<String>,
        unique: bool,
    ) -> Result<Arc<dyn Index>> {
        let name = name.into();
        let table_name = table_name.into();

        // Check if index already exists
        {
            let indexes = self.indexes.read();
            if indexes.contains_key(&name) {
                return Err(QueryError::ExecutionError(format!(
                    "Index '{}' already exists",
                    name
                )));
            }
        }

        let metadata = IndexMetadata::new(&name, &table_name, columns, IndexType::BTree, unique);
        let index = Arc::new(BTreeIndex::new(metadata));

        // Store the index
        {
            let mut indexes = self.indexes.write();
            indexes.insert(name.clone(), index.clone());
        }

        // Track table -> index mapping
        {
            let mut table_indexes = self.table_indexes.write();
            table_indexes
                .entry(table_name)
                .or_insert_with(Vec::new)
                .push(name);
        }

        Ok(index)
    }

    /// Create a new Hash index
    pub fn create_hash_index(
        &self,
        name: impl Into<String>,
        table_name: impl Into<String>,
        columns: Vec<String>,
        unique: bool,
    ) -> Result<Arc<dyn Index>> {
        let name = name.into();
        let table_name = table_name.into();

        // Check if index already exists
        {
            let indexes = self.indexes.read();
            if indexes.contains_key(&name) {
                return Err(QueryError::ExecutionError(format!(
                    "Index '{}' already exists",
                    name
                )));
            }
        }

        let metadata = IndexMetadata::new(&name, &table_name, columns, IndexType::Hash, unique);
        let index = Arc::new(HashIndex::new(metadata));

        // Store the index
        {
            let mut indexes = self.indexes.write();
            indexes.insert(name.clone(), index.clone());
        }

        // Track table -> index mapping
        {
            let mut table_indexes = self.table_indexes.write();
            table_indexes
                .entry(table_name)
                .or_insert_with(Vec::new)
                .push(name);
        }

        Ok(index)
    }

    /// Create an index based on metadata
    pub fn create_index(&self, metadata: IndexMetadata) -> Result<Arc<dyn Index>> {
        match metadata.index_type {
            IndexType::BTree => self.create_btree_index(
                &metadata.name,
                &metadata.table_name,
                metadata.columns,
                metadata.unique,
            ),
            IndexType::Hash => self.create_hash_index(
                &metadata.name,
                &metadata.table_name,
                metadata.columns,
                metadata.unique,
            ),
        }
    }

    /// Drop an index by name
    pub fn drop_index(&self, name: &str) -> Result<()> {
        let table_name;

        // Remove from indexes map
        {
            let mut indexes = self.indexes.write();
            let index = indexes.remove(name).ok_or_else(|| {
                QueryError::ExecutionError(format!("Index '{}' not found", name))
            })?;
            table_name = index.metadata().table_name.clone();
        }

        // Remove from table -> index mapping
        {
            let mut table_indexes = self.table_indexes.write();
            if let Some(index_names) = table_indexes.get_mut(&table_name) {
                index_names.retain(|n| n != name);
                if index_names.is_empty() {
                    table_indexes.remove(&table_name);
                }
            }
        }

        Ok(())
    }

    /// Drop an index if it exists, without error if not found
    pub fn drop_index_if_exists(&self, name: &str) -> Result<bool> {
        {
            let indexes = self.indexes.read();
            if !indexes.contains_key(name) {
                return Ok(false);
            }
        }
        self.drop_index(name)?;
        Ok(true)
    }

    /// Get an index by name
    pub fn get_index(&self, name: &str) -> Option<Arc<dyn Index>> {
        let indexes = self.indexes.read();
        indexes.get(name).cloned()
    }

    /// Get all indexes for a specific table
    pub fn get_indexes_for_table(&self, table_name: &str) -> Vec<Arc<dyn Index>> {
        let table_indexes = self.table_indexes.read();
        let indexes = self.indexes.read();

        table_indexes
            .get(table_name)
            .map(|names| {
                names
                    .iter()
                    .filter_map(|name| indexes.get(name).cloned())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get index metadata for all indexes on a table
    pub fn get_index_metadata_for_table(&self, table_name: &str) -> Vec<IndexMetadata> {
        self.get_indexes_for_table(table_name)
            .into_iter()
            .map(|idx| idx.metadata().clone())
            .collect()
    }

    /// Find an index that can accelerate a lookup on the given column
    pub fn find_index_for_column(
        &self,
        table_name: &str,
        column: &str,
    ) -> Option<Arc<dyn Index>> {
        self.get_indexes_for_table(table_name)
            .into_iter()
            .find(|idx| idx.metadata().can_accelerate(column))
    }

    /// Find the best index for a set of columns
    /// Returns the index that covers the most columns from the start
    pub fn find_best_index_for_columns(
        &self,
        table_name: &str,
        columns: &[String],
    ) -> Option<Arc<dyn Index>> {
        let indexes = self.get_indexes_for_table(table_name);

        // Score each index based on how many columns it covers
        indexes
            .into_iter()
            .filter(|idx| idx.metadata().columns.first() == columns.first())
            .max_by_key(|idx| {
                let idx_cols = &idx.metadata().columns;
                columns
                    .iter()
                    .zip(idx_cols.iter())
                    .take_while(|(a, b)| a == b)
                    .count()
            })
    }

    /// List all index names
    pub fn list_indexes(&self) -> Vec<String> {
        let indexes = self.indexes.read();
        indexes.keys().cloned().collect()
    }

    /// List all indexes with their metadata
    pub fn list_all_metadata(&self) -> Vec<IndexMetadata> {
        let indexes = self.indexes.read();
        indexes.values().map(|idx| idx.metadata().clone()).collect()
    }

    /// Check if an index exists
    pub fn index_exists(&self, name: &str) -> bool {
        let indexes = self.indexes.read();
        indexes.contains_key(name)
    }

    /// Get the total number of indexes
    pub fn index_count(&self) -> usize {
        let indexes = self.indexes.read();
        indexes.len()
    }

    /// Clear all indexes
    pub fn clear(&self) {
        let mut indexes = self.indexes.write();
        let mut table_indexes = self.table_indexes.write();
        indexes.clear();
        table_indexes.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::IndexKey;

    #[test]
    fn test_create_btree_index() {
        let manager = IndexManager::new();

        let index = manager
            .create_btree_index("idx_id", "employees", vec!["id".to_string()], false)
            .unwrap();

        assert_eq!(index.metadata().name, "idx_id");
        assert_eq!(index.metadata().table_name, "employees");
        assert_eq!(index.metadata().index_type, IndexType::BTree);
        assert!(index.supports_range());
    }

    #[test]
    fn test_create_hash_index() {
        let manager = IndexManager::new();

        let index = manager
            .create_hash_index("idx_id", "employees", vec!["id".to_string()], true)
            .unwrap();

        assert_eq!(index.metadata().name, "idx_id");
        assert_eq!(index.metadata().index_type, IndexType::Hash);
        assert!(index.metadata().unique);
        assert!(!index.supports_range());
    }

    #[test]
    fn test_duplicate_index_name() {
        let manager = IndexManager::new();

        manager
            .create_btree_index("idx_id", "employees", vec!["id".to_string()], false)
            .unwrap();

        let result =
            manager.create_btree_index("idx_id", "departments", vec!["id".to_string()], false);

        assert!(result.is_err());
    }

    #[test]
    fn test_drop_index() {
        let manager = IndexManager::new();

        manager
            .create_btree_index("idx_id", "employees", vec!["id".to_string()], false)
            .unwrap();

        assert!(manager.index_exists("idx_id"));

        manager.drop_index("idx_id").unwrap();

        assert!(!manager.index_exists("idx_id"));
    }

    #[test]
    fn test_drop_index_if_exists() {
        let manager = IndexManager::new();

        // Should return false if index doesn't exist
        assert!(!manager.drop_index_if_exists("nonexistent").unwrap());

        manager
            .create_btree_index("idx_id", "employees", vec!["id".to_string()], false)
            .unwrap();

        // Should return true after dropping existing index
        assert!(manager.drop_index_if_exists("idx_id").unwrap());
    }

    #[test]
    fn test_get_indexes_for_table() {
        let manager = IndexManager::new();

        manager
            .create_btree_index("idx_id", "employees", vec!["id".to_string()], false)
            .unwrap();
        manager
            .create_hash_index("idx_name", "employees", vec!["name".to_string()], false)
            .unwrap();
        manager
            .create_btree_index("idx_dept", "departments", vec!["id".to_string()], false)
            .unwrap();

        let emp_indexes = manager.get_indexes_for_table("employees");
        assert_eq!(emp_indexes.len(), 2);

        let dept_indexes = manager.get_indexes_for_table("departments");
        assert_eq!(dept_indexes.len(), 1);
    }

    #[test]
    fn test_find_index_for_column() {
        let manager = IndexManager::new();

        manager
            .create_btree_index("idx_id", "employees", vec!["id".to_string()], false)
            .unwrap();
        manager
            .create_hash_index("idx_name", "employees", vec!["name".to_string()], false)
            .unwrap();

        let id_index = manager.find_index_for_column("employees", "id");
        assert!(id_index.is_some());
        assert_eq!(id_index.unwrap().metadata().name, "idx_id");

        let name_index = manager.find_index_for_column("employees", "name");
        assert!(name_index.is_some());
        assert_eq!(name_index.unwrap().metadata().name, "idx_name");

        let missing = manager.find_index_for_column("employees", "salary");
        assert!(missing.is_none());
    }

    #[test]
    fn test_index_operations() {
        let manager = IndexManager::new();

        let index = manager
            .create_btree_index("idx_id", "employees", vec!["id".to_string()], false)
            .unwrap();

        // Insert some values
        index.insert(IndexKey::from_i64(1), 0).unwrap();
        index.insert(IndexKey::from_i64(2), 1).unwrap();
        index.insert(IndexKey::from_i64(3), 2).unwrap();

        // Lookup
        let result = index.lookup(&IndexKey::from_i64(2));
        assert_eq!(result.row_ids, vec![1]);

        // Get same index from manager
        let same_index = manager.get_index("idx_id").unwrap();
        let result = same_index.lookup(&IndexKey::from_i64(2));
        assert_eq!(result.row_ids, vec![1]);
    }

    #[test]
    fn test_list_indexes() {
        let manager = IndexManager::new();

        manager
            .create_btree_index("idx_a", "table1", vec!["a".to_string()], false)
            .unwrap();
        manager
            .create_btree_index("idx_b", "table2", vec!["b".to_string()], false)
            .unwrap();

        let names = manager.list_indexes();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"idx_a".to_string()));
        assert!(names.contains(&"idx_b".to_string()));
    }

    #[test]
    fn test_clear() {
        let manager = IndexManager::new();

        manager
            .create_btree_index("idx_a", "table1", vec!["a".to_string()], false)
            .unwrap();
        manager
            .create_btree_index("idx_b", "table2", vec!["b".to_string()], false)
            .unwrap();

        assert_eq!(manager.index_count(), 2);

        manager.clear();

        assert_eq!(manager.index_count(), 0);
    }
}
