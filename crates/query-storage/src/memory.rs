//! In-memory data source with index support
//!
//! Provides a memory-backed data source that supports table scans
//! and index-based lookups for faster data retrieval.

use arrow::array::*;
use arrow::record_batch::RecordBatch;
use query_core::{Result, Schema};
use query_executor::physical_plan::DataSource;
use query_index::{Index, IndexKey, IndexManager};
use std::collections::HashMap;
use std::sync::Arc;

/// In-memory data source with optional index support
#[derive(Debug)]
pub struct MemoryDataSource {
    schema: Schema,
    batches: Vec<RecordBatch>,
    /// Index manager for this data source
    index_manager: Arc<IndexManager>,
    /// Table name (used for index lookups)
    table_name: String,
}

impl MemoryDataSource {
    /// Create a new memory data source
    pub fn new(schema: Schema, batches: Vec<RecordBatch>) -> Self {
        Self::with_table_name(schema, batches, "memory_table")
    }

    /// Create a new memory data source with a specific table name
    pub fn with_table_name(
        schema: Schema,
        batches: Vec<RecordBatch>,
        table_name: impl Into<String>,
    ) -> Self {
        Self {
            schema,
            batches,
            index_manager: Arc::new(IndexManager::new()),
            table_name: table_name.into(),
        }
    }

    /// Get the table name
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Get the index manager
    pub fn index_manager(&self) -> &Arc<IndexManager> {
        &self.index_manager
    }

    /// Create a B-Tree index on the specified column
    pub fn create_btree_index(
        &self,
        index_name: &str,
        column_name: &str,
        unique: bool,
    ) -> Result<()> {
        // Find column index
        let col_idx = self
            .schema
            .fields()
            .iter()
            .position(|f| f.name() == column_name)
            .ok_or_else(|| {
                query_core::QueryError::ExecutionError(format!(
                    "Column '{}' not found",
                    column_name
                ))
            })?;

        // Create the index
        let index = self.index_manager.create_btree_index(
            index_name,
            &self.table_name,
            vec![column_name.to_string()],
            unique,
        )?;

        // Build the index from data
        self.build_index(&*index, col_idx)?;

        Ok(())
    }

    /// Create a Hash index on the specified column
    pub fn create_hash_index(
        &self,
        index_name: &str,
        column_name: &str,
        unique: bool,
    ) -> Result<()> {
        // Find column index
        let col_idx = self
            .schema
            .fields()
            .iter()
            .position(|f| f.name() == column_name)
            .ok_or_else(|| {
                query_core::QueryError::ExecutionError(format!(
                    "Column '{}' not found",
                    column_name
                ))
            })?;

        // Create the index
        let index = self.index_manager.create_hash_index(
            index_name,
            &self.table_name,
            vec![column_name.to_string()],
            unique,
        )?;

        // Build the index from data
        self.build_index(&*index, col_idx)?;

        Ok(())
    }

    /// Build an index from the data in the batches
    fn build_index(&self, index: &dyn Index, col_idx: usize) -> Result<()> {
        let mut global_row_id = 0usize;

        for batch in &self.batches {
            let column = batch.column(col_idx);
            let num_rows = batch.num_rows();

            // Extract keys from the column and insert into index
            for row in 0..num_rows {
                if let Some(key) = self.extract_key(column, row) {
                    index.insert(key, global_row_id)?;
                }
                global_row_id += 1;
            }
        }

        Ok(())
    }

    /// Extract an IndexKey from an array at the given row
    fn extract_key(&self, array: &ArrayRef, row: usize) -> Option<IndexKey> {
        if array.is_null(row) {
            return None;
        }

        // Handle different array types
        if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
            Some(IndexKey::from_i64(arr.value(row)))
        } else if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
            Some(IndexKey::from_i64(arr.value(row) as i64))
        } else if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
            Some(IndexKey::from_string(arr.value(row)))
        } else if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
            Some(IndexKey::from_f64(arr.value(row)))
        } else {
            // For unsupported types, skip indexing
            None
        }
    }

    /// Drop an index by name
    pub fn drop_index(&self, index_name: &str) -> Result<()> {
        self.index_manager.drop_index(index_name)
    }

    /// Drop an index if it exists
    pub fn drop_index_if_exists(&self, index_name: &str) -> Result<bool> {
        self.index_manager.drop_index_if_exists(index_name)
    }

    /// List all indexes on this table
    pub fn list_indexes(&self) -> Vec<String> {
        self.index_manager
            .get_indexes_for_table(&self.table_name)
            .iter()
            .map(|idx| idx.metadata().name.clone())
            .collect()
    }

    /// Get index metadata for all indexes
    pub fn get_index_metadata(&self) -> Vec<query_index::IndexMetadata> {
        self.index_manager
            .get_index_metadata_for_table(&self.table_name)
    }

    /// Find an index for a specific column
    pub fn find_index_for_column(&self, column_name: &str) -> Option<Arc<dyn Index>> {
        self.index_manager
            .find_index_for_column(&self.table_name, column_name)
    }

    /// Perform an index lookup and return matching rows
    pub fn index_lookup(&self, index_name: &str, key: &IndexKey) -> Result<Vec<RecordBatch>> {
        let index = self.index_manager.get_index(index_name).ok_or_else(|| {
            query_core::QueryError::ExecutionError(format!("Index '{}' not found", index_name))
        })?;

        let result = index.lookup(key);
        self.fetch_rows(&result.row_ids)
    }

    /// Perform an index range scan and return matching rows
    pub fn index_range_scan(
        &self,
        index_name: &str,
        start_key: Option<&IndexKey>,
        end_key: Option<&IndexKey>,
    ) -> Result<Vec<RecordBatch>> {
        let index = self.index_manager.get_index(index_name).ok_or_else(|| {
            query_core::QueryError::ExecutionError(format!("Index '{}' not found", index_name))
        })?;

        if !index.supports_range() {
            return Err(query_core::QueryError::ExecutionError(format!(
                "Index '{}' does not support range scans",
                index_name
            )));
        }

        let result = index.range_scan(start_key, end_key);
        self.fetch_rows(&result.row_ids)
    }

    /// Fetch specific rows by their global row IDs
    pub fn fetch_rows(&self, row_ids: &[usize]) -> Result<Vec<RecordBatch>> {
        if row_ids.is_empty() {
            return Ok(vec![]);
        }

        // Map global row IDs to (batch_idx, local_row_idx)
        let mut batch_rows: HashMap<usize, Vec<usize>> = HashMap::new();
        let mut cumulative_rows = 0;

        for (batch_idx, batch) in self.batches.iter().enumerate() {
            let batch_size = batch.num_rows();
            let batch_end = cumulative_rows + batch_size;

            for &row_id in row_ids {
                if row_id >= cumulative_rows && row_id < batch_end {
                    let local_row = row_id - cumulative_rows;
                    batch_rows.entry(batch_idx).or_default().push(local_row);
                }
            }

            cumulative_rows = batch_end;
        }

        // Extract rows from each batch
        let mut result = Vec::new();
        for (batch_idx, local_rows) in batch_rows {
            let batch = &self.batches[batch_idx];
            let indices =
                UInt32Array::from(local_rows.iter().map(|&r| r as u32).collect::<Vec<_>>());

            let columns: Vec<ArrayRef> = batch
                .columns()
                .iter()
                .map(|col| arrow::compute::take(col.as_ref(), &indices, None))
                .collect::<std::result::Result<Vec<_>, _>>()?;

            let result_batch = RecordBatch::try_new(batch.schema(), columns)?;
            result.push(result_batch);
        }

        Ok(result)
    }

    /// Get total row count
    pub fn row_count(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Add more data to the source (and update indexes)
    pub fn append(&mut self, batch: RecordBatch) -> Result<()> {
        let start_row = self.row_count();

        // Update all indexes with new data
        for index in self.index_manager.get_indexes_for_table(&self.table_name) {
            let metadata = index.metadata();
            if let Some(col_name) = metadata.columns.first() {
                if let Some(col_idx) = self
                    .schema
                    .fields()
                    .iter()
                    .position(|f| f.name() == col_name)
                {
                    let column = batch.column(col_idx);
                    for row in 0..batch.num_rows() {
                        if let Some(key) = self.extract_key(column, row) {
                            index.insert(key, start_row + row)?;
                        }
                    }
                }
            }
        }

        self.batches.push(batch);
        Ok(())
    }
}

impl DataSource for MemoryDataSource {
    fn scan(&self) -> Result<Vec<RecordBatch>> {
        Ok(self.batches.clone())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use query_core::{DataType, Field};

    fn create_test_data() -> (Schema, Vec<RecordBatch>) {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, false),
            ArrowField::new("name", ArrowDataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec![
                    "alice", "bob", "charlie", "dave", "eve",
                ])),
            ],
        )
        .unwrap();

        (schema, vec![batch])
    }

    #[test]
    fn test_create_btree_index() {
        let (schema, batches) = create_test_data();
        let source = MemoryDataSource::with_table_name(schema, batches, "test");

        source.create_btree_index("idx_id", "id", true).unwrap();

        let indexes = source.list_indexes();
        assert_eq!(indexes.len(), 1);
        assert!(indexes.contains(&"idx_id".to_string()));
    }

    #[test]
    fn test_create_hash_index() {
        let (schema, batches) = create_test_data();
        let source = MemoryDataSource::with_table_name(schema, batches, "test");

        source.create_hash_index("idx_name", "name", false).unwrap();

        let indexes = source.list_indexes();
        assert_eq!(indexes.len(), 1);
    }

    #[test]
    fn test_index_lookup() {
        let (schema, batches) = create_test_data();
        let source = MemoryDataSource::with_table_name(schema, batches, "test");

        source.create_btree_index("idx_id", "id", true).unwrap();

        let key = IndexKey::from_i64(3);
        let result = source.index_lookup("idx_id", &key).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 1);

        // Check the returned row
        let id_col = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 3);
    }

    #[test]
    fn test_index_range_scan() {
        let (schema, batches) = create_test_data();
        let source = MemoryDataSource::with_table_name(schema, batches, "test");

        source.create_btree_index("idx_id", "id", false).unwrap();

        let start = IndexKey::from_i64(2);
        let end = IndexKey::from_i64(4);
        let result = source
            .index_range_scan("idx_id", Some(&start), Some(&end))
            .unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3); // rows 2, 3, 4
    }

    #[test]
    fn test_drop_index() {
        let (schema, batches) = create_test_data();
        let source = MemoryDataSource::with_table_name(schema, batches, "test");

        source.create_btree_index("idx_id", "id", true).unwrap();
        assert_eq!(source.list_indexes().len(), 1);

        source.drop_index("idx_id").unwrap();
        assert_eq!(source.list_indexes().len(), 0);
    }

    #[test]
    fn test_find_index_for_column() {
        let (schema, batches) = create_test_data();
        let source = MemoryDataSource::with_table_name(schema, batches, "test");

        source.create_btree_index("idx_id", "id", true).unwrap();
        source.create_hash_index("idx_name", "name", false).unwrap();

        let id_index = source.find_index_for_column("id");
        assert!(id_index.is_some());
        assert_eq!(id_index.unwrap().metadata().name, "idx_id");

        let name_index = source.find_index_for_column("name");
        assert!(name_index.is_some());
        assert_eq!(name_index.unwrap().metadata().name, "idx_name");

        let missing = source.find_index_for_column("nonexistent");
        assert!(missing.is_none());
    }
}
