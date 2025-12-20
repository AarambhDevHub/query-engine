//! Server-side Cursor Support
//!
//! Provides DECLARE, FETCH, and CLOSE cursor commands for large result set handling.

use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// A server-side cursor holding query results
#[derive(Debug, Clone)]
pub struct Cursor {
    /// Cursor name
    pub name: String,
    /// Full result set
    batches: Vec<RecordBatch>,
    /// Current position (row index across all batches)
    position: usize,
    /// Total row count
    total_rows: usize,
}

impl Cursor {
    /// Create a new cursor from query results
    pub fn new(name: String, batches: Vec<RecordBatch>) -> Self {
        let total_rows = batches.iter().map(|b| b.num_rows()).sum();
        Self {
            name,
            batches,
            position: 0,
            total_rows,
        }
    }

    /// Fetch next `count` rows from the cursor
    pub fn fetch(&mut self, count: usize) -> Vec<RecordBatch> {
        if self.position >= self.total_rows {
            return vec![];
        }

        let mut result_batches = Vec::new();
        let mut rows_needed = count;
        let mut current_row = 0;
        let mut batch_start_row = 0;

        for batch in &self.batches {
            let batch_rows = batch.num_rows();
            let batch_end_row = batch_start_row + batch_rows;

            // Skip batches before current position
            if batch_end_row <= self.position {
                batch_start_row = batch_end_row;
                continue;
            }

            // Calculate start offset within this batch
            let start_offset = if self.position > batch_start_row {
                self.position - batch_start_row
            } else {
                0
            };

            // Calculate how many rows to take from this batch
            let available_rows = batch_rows - start_offset;
            let take_rows = available_rows.min(rows_needed);

            if take_rows > 0 {
                let sliced = batch.slice(start_offset, take_rows);
                result_batches.push(sliced);
                rows_needed -= take_rows;
                current_row += take_rows;
            }

            if rows_needed == 0 {
                break;
            }

            batch_start_row = batch_end_row;
        }

        self.position += current_row;
        result_batches
    }

    /// Get remaining row count
    pub fn remaining(&self) -> usize {
        self.total_rows.saturating_sub(self.position)
    }

    /// Check if cursor is exhausted
    pub fn is_exhausted(&self) -> bool {
        self.position >= self.total_rows
    }
}

/// Manages server-side cursors for a connection
#[derive(Debug, Default)]
pub struct CursorManager {
    cursors: Arc<RwLock<HashMap<String, Cursor>>>,
}

impl CursorManager {
    /// Create a new cursor manager
    pub fn new() -> Self {
        Self {
            cursors: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Declare a new cursor
    pub async fn declare(&self, name: &str, batches: Vec<RecordBatch>) {
        let cursor = Cursor::new(name.to_string(), batches);
        let mut cursors = self.cursors.write().await;
        info!("Declared cursor: {} ({} rows)", name, cursor.total_rows);
        cursors.insert(name.to_string(), cursor);
    }

    /// Fetch rows from a cursor
    pub async fn fetch(&self, name: &str, count: usize) -> Option<Vec<RecordBatch>> {
        let mut cursors = self.cursors.write().await;
        if let Some(cursor) = cursors.get_mut(name) {
            let batches = cursor.fetch(count);
            info!(
                "Fetched {} batches from cursor {} ({} remaining)",
                batches.len(),
                name,
                cursor.remaining()
            );
            Some(batches)
        } else {
            None
        }
    }

    /// Close and remove a cursor
    pub async fn close(&self, name: &str) -> bool {
        let mut cursors = self.cursors.write().await;
        if cursors.remove(name).is_some() {
            info!("Closed cursor: {}", name);
            true
        } else {
            false
        }
    }

    /// Check if a cursor exists
    pub async fn exists(&self, name: &str) -> bool {
        let cursors = self.cursors.read().await;
        cursors.contains_key(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn create_test_batch(start: i64, count: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let values: Vec<i64> = (start..start + count as i64).collect();
        let array = Int64Array::from(values);
        RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    #[test]
    fn test_cursor_fetch() {
        let batches = vec![create_test_batch(1, 5), create_test_batch(6, 5)];
        let mut cursor = Cursor::new("test".to_string(), batches);

        assert_eq!(cursor.total_rows, 10);
        assert_eq!(cursor.remaining(), 10);

        let result = cursor.fetch(3);
        assert_eq!(result.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
        assert_eq!(cursor.remaining(), 7);

        let result = cursor.fetch(10);
        assert_eq!(result.iter().map(|b| b.num_rows()).sum::<usize>(), 7);
        assert_eq!(cursor.remaining(), 0);
        assert!(cursor.is_exhausted());
    }

    #[tokio::test]
    async fn test_cursor_manager() {
        let manager = CursorManager::new();
        let batches = vec![create_test_batch(1, 10)];

        manager.declare("my_cursor", batches).await;
        assert!(manager.exists("my_cursor").await);

        let result = manager.fetch("my_cursor", 5).await;
        assert!(result.is_some());

        assert!(manager.close("my_cursor").await);
        assert!(!manager.exists("my_cursor").await);
    }
}
