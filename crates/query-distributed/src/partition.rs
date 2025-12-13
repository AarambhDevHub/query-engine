//! Data partitioning strategies for distributed execution

use crate::error::{DistributedError, Result};
use crate::types::WorkerId;
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// A serializable boundary value for range partitioning
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RangeBoundary {
    Int64(i64),
    Float64(f64),
    String(String),
}

/// Partitioning strategy for distributing data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitionStrategy {
    /// Hash-based partitioning on specified columns
    Hash {
        /// Columns to hash for partitioning
        key_columns: Vec<String>,
        /// Number of partitions
        num_partitions: usize,
    },

    /// Range-based partitioning on a single column
    Range {
        /// Column to use for range partitioning
        key_column: String,
        /// Boundary values between partitions
        boundaries: Vec<RangeBoundary>,
    },

    /// Round-robin distribution
    RoundRobin {
        /// Number of partitions
        num_partitions: usize,
    },

    /// Single partition (no distribution)
    Single,
}

impl Default for PartitionStrategy {
    fn default() -> Self {
        PartitionStrategy::RoundRobin { num_partitions: 4 }
    }
}

/// A partition of data
#[derive(Debug, Clone)]
pub struct Partition {
    /// Partition index
    pub index: usize,
    /// Data in this partition
    pub batches: Vec<RecordBatch>,
    /// Target worker (if assigned)
    pub target_worker: Option<WorkerId>,
}

impl Partition {
    /// Create a new partition
    pub fn new(index: usize) -> Self {
        Self {
            index,
            batches: Vec::new(),
            target_worker: None,
        }
    }

    /// Add a batch to this partition
    pub fn add_batch(&mut self, batch: RecordBatch) {
        self.batches.push(batch);
    }

    /// Get total row count
    pub fn row_count(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Assign to a worker
    pub fn assign_to(&mut self, worker_id: WorkerId) {
        self.target_worker = Some(worker_id);
    }
}

/// Partitioner for distributing data across workers
#[derive(Debug, Clone)]
pub struct Partitioner {
    strategy: PartitionStrategy,
}

impl Partitioner {
    /// Create a new partitioner with the given strategy
    pub fn new(strategy: PartitionStrategy) -> Self {
        Self { strategy }
    }

    /// Create a hash partitioner
    pub fn hash(key_columns: Vec<String>, num_partitions: usize) -> Self {
        Self::new(PartitionStrategy::Hash {
            key_columns,
            num_partitions,
        })
    }

    /// Create a round-robin partitioner
    pub fn round_robin(num_partitions: usize) -> Self {
        Self::new(PartitionStrategy::RoundRobin { num_partitions })
    }

    /// Get the number of partitions
    pub fn num_partitions(&self) -> usize {
        match &self.strategy {
            PartitionStrategy::Hash { num_partitions, .. } => *num_partitions,
            PartitionStrategy::Range { boundaries, .. } => boundaries.len() + 1,
            PartitionStrategy::RoundRobin { num_partitions } => *num_partitions,
            PartitionStrategy::Single => 1,
        }
    }

    /// Partition the given data
    pub fn partition(&self, batches: &[RecordBatch]) -> Result<Vec<Partition>> {
        match &self.strategy {
            PartitionStrategy::Hash {
                key_columns,
                num_partitions,
            } => self.partition_by_hash(batches, key_columns, *num_partitions),
            PartitionStrategy::RoundRobin { num_partitions } => {
                self.partition_round_robin(batches, *num_partitions)
            }
            PartitionStrategy::Range {
                key_column,
                boundaries,
            } => self.partition_by_range(batches, key_column, boundaries),
            PartitionStrategy::Single => {
                let mut partition = Partition::new(0);
                for batch in batches {
                    partition.add_batch(batch.clone());
                }
                Ok(vec![partition])
            }
        }
    }

    /// Hash-based partitioning
    fn partition_by_hash(
        &self,
        batches: &[RecordBatch],
        key_columns: &[String],
        num_partitions: usize,
    ) -> Result<Vec<Partition>> {
        let mut partitions: Vec<Partition> =
            (0..num_partitions).map(|i| Partition::new(i)).collect();

        for batch in batches {
            // Find column indices
            let col_indices: Vec<usize> = key_columns
                .iter()
                .filter_map(|name| {
                    batch
                        .schema()
                        .fields()
                        .iter()
                        .position(|f| f.name() == name)
                })
                .collect();

            if col_indices.is_empty() {
                return Err(DistributedError::PartitionError(
                    "No key columns found in batch".to_string(),
                ));
            }

            // For each row, compute hash and assign to partition
            let num_rows = batch.num_rows();
            let mut partition_assignments: Vec<Vec<usize>> = vec![Vec::new(); num_partitions];

            for row in 0..num_rows {
                let hash = self.compute_row_hash(batch, &col_indices, row);
                let partition_idx = (hash as usize) % num_partitions;
                partition_assignments[partition_idx].push(row);
            }

            // Create batches for each partition
            for (partition_idx, rows) in partition_assignments.iter().enumerate() {
                if rows.is_empty() {
                    continue;
                }

                let indices = UInt32Array::from(rows.iter().map(|&r| r as u32).collect::<Vec<_>>());

                let columns: Vec<ArrayRef> = batch
                    .columns()
                    .iter()
                    .map(|col| arrow::compute::take(col.as_ref(), &indices, None))
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|e| DistributedError::PartitionError(e.to_string()))?;

                let partition_batch = RecordBatch::try_new(batch.schema(), columns)
                    .map_err(|e| DistributedError::PartitionError(e.to_string()))?;

                partitions[partition_idx].add_batch(partition_batch);
            }
        }

        Ok(partitions)
    }

    /// Round-robin partitioning
    fn partition_round_robin(
        &self,
        batches: &[RecordBatch],
        num_partitions: usize,
    ) -> Result<Vec<Partition>> {
        let mut partitions: Vec<Partition> =
            (0..num_partitions).map(|i| Partition::new(i)).collect();

        for (batch_idx, batch) in batches.iter().enumerate() {
            let partition_idx = batch_idx % num_partitions;
            partitions[partition_idx].add_batch(batch.clone());
        }

        Ok(partitions)
    }

    /// Range-based partitioning
    fn partition_by_range(
        &self,
        batches: &[RecordBatch],
        key_column: &str,
        boundaries: &[RangeBoundary],
    ) -> Result<Vec<Partition>> {
        let num_partitions = boundaries.len() + 1;
        let mut partitions: Vec<Partition> =
            (0..num_partitions).map(|i| Partition::new(i)).collect();

        for batch in batches {
            // Find key column
            let col_idx = batch
                .schema()
                .fields()
                .iter()
                .position(|f| f.name() == key_column)
                .ok_or_else(|| {
                    DistributedError::PartitionError(format!(
                        "Key column '{}' not found",
                        key_column
                    ))
                })?;

            let column = batch.column(col_idx);
            let num_rows = batch.num_rows();
            let mut partition_assignments: Vec<Vec<usize>> = vec![Vec::new(); num_partitions];

            // Assign each row to a partition based on boundaries
            for row in 0..num_rows {
                let partition_idx = self.find_range_partition(column, row, boundaries);
                partition_assignments[partition_idx].push(row);
            }

            // Create batches for each partition
            for (partition_idx, rows) in partition_assignments.iter().enumerate() {
                if rows.is_empty() {
                    continue;
                }

                let indices = UInt32Array::from(rows.iter().map(|&r| r as u32).collect::<Vec<_>>());

                let columns: Vec<ArrayRef> = batch
                    .columns()
                    .iter()
                    .map(|col| arrow::compute::take(col.as_ref(), &indices, None))
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|e| DistributedError::PartitionError(e.to_string()))?;

                let partition_batch = RecordBatch::try_new(batch.schema(), columns)
                    .map_err(|e| DistributedError::PartitionError(e.to_string()))?;

                partitions[partition_idx].add_batch(partition_batch);
            }
        }

        Ok(partitions)
    }

    /// Compute hash for a row
    fn compute_row_hash(&self, batch: &RecordBatch, col_indices: &[usize], row: usize) -> u64 {
        let mut hasher = DefaultHasher::new();

        for &col_idx in col_indices {
            let column = batch.column(col_idx);

            // Hash based on column type
            if let Some(arr) = column.as_any().downcast_ref::<Int64Array>() {
                if !arr.is_null(row) {
                    arr.value(row).hash(&mut hasher);
                }
            } else if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
                if !arr.is_null(row) {
                    arr.value(row).hash(&mut hasher);
                }
            } else if let Some(arr) = column.as_any().downcast_ref::<Int32Array>() {
                if !arr.is_null(row) {
                    arr.value(row).hash(&mut hasher);
                }
            }
            // Add more types as needed
        }

        hasher.finish()
    }

    /// Find partition for a value based on range boundaries
    fn find_range_partition(
        &self,
        column: &ArrayRef,
        row: usize,
        boundaries: &[RangeBoundary],
    ) -> usize {
        // Simple implementation - compare against boundaries
        if let Some(arr) = column.as_any().downcast_ref::<Int64Array>() {
            if !arr.is_null(row) {
                let value = arr.value(row);
                for (i, boundary) in boundaries.iter().enumerate() {
                    if let RangeBoundary::Int64(b) = boundary {
                        if value < *b {
                            return i;
                        }
                    }
                }
                return boundaries.len();
            }
        }
        0
    }

    /// Determine which partition a key belongs to
    pub fn route(&self, key: &[u8]) -> usize {
        match &self.strategy {
            PartitionStrategy::Hash { num_partitions, .. } => {
                let mut hasher = DefaultHasher::new();
                key.hash(&mut hasher);
                (hasher.finish() as usize) % num_partitions
            }
            PartitionStrategy::RoundRobin { num_partitions } => {
                let mut hasher = DefaultHasher::new();
                key.hash(&mut hasher);
                (hasher.finish() as usize) % num_partitions
            }
            PartitionStrategy::Range { .. } => 0, // TODO: Implement
            PartitionStrategy::Single => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_batch(ids: Vec<i64>, names: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_round_robin_partition() {
        let batches = vec![
            create_test_batch(vec![1, 2], vec!["a", "b"]),
            create_test_batch(vec![3, 4], vec!["c", "d"]),
            create_test_batch(vec![5, 6], vec!["e", "f"]),
            create_test_batch(vec![7, 8], vec!["g", "h"]),
        ];

        let partitioner = Partitioner::round_robin(2);
        let partitions = partitioner.partition(&batches).unwrap();

        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[0].batches.len(), 2); // batches 0, 2
        assert_eq!(partitions[1].batches.len(), 2); // batches 1, 3
    }

    #[test]
    fn test_hash_partition() {
        let batches = vec![create_test_batch(
            vec![1, 2, 3, 4, 5],
            vec!["a", "b", "c", "d", "e"],
        )];

        let partitioner = Partitioner::hash(vec!["id".to_string()], 3);
        let partitions = partitioner.partition(&batches).unwrap();

        assert_eq!(partitions.len(), 3);

        // Total rows should be 5
        let total_rows: usize = partitions.iter().map(|p| p.row_count()).sum();
        assert_eq!(total_rows, 5);
    }

    #[test]
    fn test_single_partition() {
        let batches = vec![
            create_test_batch(vec![1, 2], vec!["a", "b"]),
            create_test_batch(vec![3, 4], vec!["c", "d"]),
        ];

        let partitioner = Partitioner::new(PartitionStrategy::Single);
        let partitions = partitioner.partition(&batches).unwrap();

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].batches.len(), 2);
        assert_eq!(partitions[0].row_count(), 4);
    }

    #[test]
    fn test_num_partitions() {
        assert_eq!(Partitioner::round_robin(4).num_partitions(), 4);
        assert_eq!(Partitioner::hash(vec![], 8).num_partitions(), 8);
        assert_eq!(
            Partitioner::new(PartitionStrategy::Single).num_partitions(),
            1
        );
    }
}
