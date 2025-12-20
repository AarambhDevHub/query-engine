//! Named Portals for Extended Query Protocol
//!
//! Provides persistent portal storage for result pagination across
//! multiple EXECUTE calls.

use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// A named portal that holds query results for pagination
#[derive(Debug)]
pub struct NamedPortal {
    /// Portal name
    pub name: String,
    /// Result batches
    batches: Vec<RecordBatch>,
    /// Current row position across all batches
    position: usize,
    /// Total row count
    total_rows: usize,
}

impl NamedPortal {
    /// Create a new named portal from query results
    pub fn new(name: String, batches: Vec<RecordBatch>) -> Self {
        let total_rows = batches.iter().map(|b| b.num_rows()).sum();
        Self {
            name,
            batches,
            position: 0,
            total_rows,
        }
    }

    /// Fetch next `max_rows` from the portal
    /// Returns (batches, is_complete)
    pub fn fetch(&mut self, max_rows: usize) -> (Vec<RecordBatch>, bool) {
        if self.position >= self.total_rows {
            return (vec![], true);
        }

        let mut result_batches = Vec::new();
        let mut rows_needed = if max_rows == 0 { usize::MAX } else { max_rows };
        let mut batch_start_row = 0;
        let mut rows_fetched = 0;

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
                rows_fetched += take_rows;
            }

            if rows_needed == 0 {
                break;
            }

            batch_start_row = batch_end_row;
        }

        self.position += rows_fetched;
        let is_complete = self.position >= self.total_rows;

        (result_batches, is_complete)
    }

    /// Check if portal is exhausted
    pub fn is_exhausted(&self) -> bool {
        self.position >= self.total_rows
    }

    /// Get remaining row count
    pub fn remaining(&self) -> usize {
        self.total_rows.saturating_sub(self.position)
    }
}

/// Thread-safe storage for named portals
#[derive(Debug, Default)]
pub struct PortalStore {
    portals: Arc<RwLock<HashMap<String, NamedPortal>>>,
}

impl PortalStore {
    /// Create a new portal store
    pub fn new() -> Self {
        Self {
            portals: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Store a portal
    pub async fn store(&self, portal: NamedPortal) {
        let name = portal.name.clone();
        let total = portal.total_rows;
        let mut portals = self.portals.write().await;
        info!("Stored portal '{}' with {} rows", name, total);
        portals.insert(name, portal);
    }

    /// Fetch from a portal
    pub async fn fetch(&self, name: &str, max_rows: usize) -> Option<(Vec<RecordBatch>, bool)> {
        let mut portals = self.portals.write().await;
        if let Some(portal) = portals.get_mut(name) {
            let (batches, complete) = portal.fetch(max_rows);
            info!(
                "Fetched {} batches from portal '{}' (complete: {})",
                batches.len(),
                name,
                complete
            );
            Some((batches, complete))
        } else {
            None
        }
    }

    /// Close and remove a portal
    pub async fn close(&self, name: &str) -> bool {
        let mut portals = self.portals.write().await;
        if portals.remove(name).is_some() {
            info!("Closed portal: {}", name);
            true
        } else {
            false
        }
    }

    /// Check if a portal exists
    pub async fn exists(&self, name: &str) -> bool {
        let portals = self.portals.read().await;
        portals.contains_key(name)
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
    fn test_portal_fetch() {
        let batches = vec![create_test_batch(1, 5), create_test_batch(6, 5)];
        let mut portal = NamedPortal::new("test".to_string(), batches);

        assert_eq!(portal.total_rows, 10);

        let (result, complete) = portal.fetch(3);
        assert_eq!(result.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
        assert!(!complete);

        let (result, complete) = portal.fetch(10);
        assert_eq!(result.iter().map(|b| b.num_rows()).sum::<usize>(), 7);
        assert!(complete);
    }

    #[tokio::test]
    async fn test_portal_store() {
        let store = PortalStore::new();
        let batches = vec![create_test_batch(1, 10)];
        let portal = NamedPortal::new("my_portal".to_string(), batches);

        store.store(portal).await;
        assert!(store.exists("my_portal").await);

        let result = store.fetch("my_portal", 5).await;
        assert!(result.is_some());

        assert!(store.close("my_portal").await);
        assert!(!store.exists("my_portal").await);
    }
}
