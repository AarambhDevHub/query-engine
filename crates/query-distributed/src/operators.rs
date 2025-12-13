//! Exchange and merge operators for distributed execution
//!
//! These operators handle data movement between query stages.

use crate::error::{DistributedError, Result};
use crate::partition::{Partition, PartitionStrategy, Partitioner, RangeBoundary};
use crate::types::WorkerId;
use arrow::array::*;
use arrow::compute;
use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

/// Exchange operator for shuffling data between workers
#[derive(Debug)]
pub struct Exchange {
    /// Partitioning strategy
    strategy: PartitionStrategy,
    /// Number of output partitions
    num_partitions: usize,
}

impl Exchange {
    /// Create a new exchange operator
    pub fn new(strategy: PartitionStrategy, num_partitions: usize) -> Self {
        Self {
            strategy,
            num_partitions,
        }
    }

    /// Create a hash exchange
    pub fn hash(columns: Vec<String>, num_partitions: usize) -> Self {
        Self::new(
            PartitionStrategy::Hash {
                key_columns: columns,
                num_partitions,
            },
            num_partitions,
        )
    }

    /// Create a round-robin exchange
    pub fn round_robin(num_partitions: usize) -> Self {
        Self::new(
            PartitionStrategy::RoundRobin { num_partitions },
            num_partitions,
        )
    }

    /// Create an exchange that gathers all data to a single partition
    pub fn gather() -> Self {
        Self::new(PartitionStrategy::Single, 1)
    }

    /// Execute the exchange operation
    pub fn execute(&self, batches: &[RecordBatch]) -> Result<Vec<Partition>> {
        let partitioner = Partitioner::new(self.strategy.clone());
        partitioner.partition(batches)
    }

    /// Get target partition for a key
    pub fn route(&self, key: &[u8]) -> usize {
        let partitioner = Partitioner::new(self.strategy.clone());
        partitioner.route(key)
    }

    /// Get number of output partitions
    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }
}

/// Merge operator for combining results from multiple partitions
#[derive(Debug)]
pub struct Merge {
    /// Output schema
    schema: Arc<ArrowSchema>,
    /// Merge strategy
    strategy: MergeStrategy,
}

/// Strategy for merging partition results
#[derive(Debug, Clone)]
pub enum MergeStrategy {
    /// Simple concatenation (no ordering)
    Concat,
    /// Merge sorted streams (for ORDER BY)
    SortedMerge { sort_columns: Vec<SortColumn> },
    /// Union with deduplication
    UnionDistinct { key_columns: Vec<String> },
}

/// Sort column specification
#[derive(Debug, Clone)]
pub struct SortColumn {
    /// Column name
    pub name: String,
    /// Ascending order
    pub ascending: bool,
    /// Nulls first
    pub nulls_first: bool,
}

impl Merge {
    /// Create a new merge operator
    pub fn new(schema: Arc<ArrowSchema>, strategy: MergeStrategy) -> Self {
        Self { schema, strategy }
    }

    /// Create a concatenation merge
    pub fn concat(schema: Arc<ArrowSchema>) -> Self {
        Self::new(schema, MergeStrategy::Concat)
    }

    /// Create a sorted merge
    pub fn sorted(schema: Arc<ArrowSchema>, sort_columns: Vec<SortColumn>) -> Self {
        Self::new(schema, MergeStrategy::SortedMerge { sort_columns })
    }

    /// Execute the merge operation
    pub fn execute(&self, partitions: Vec<Vec<RecordBatch>>) -> Result<Vec<RecordBatch>> {
        match &self.strategy {
            MergeStrategy::Concat => self.concat_merge(partitions),
            MergeStrategy::SortedMerge { sort_columns } => {
                self.sorted_merge(partitions, sort_columns)
            }
            MergeStrategy::UnionDistinct { key_columns } => {
                self.union_distinct(partitions, key_columns)
            }
        }
    }

    /// Simple concatenation of all batches
    fn concat_merge(&self, partitions: Vec<Vec<RecordBatch>>) -> Result<Vec<RecordBatch>> {
        Ok(partitions.into_iter().flatten().collect())
    }

    /// Merge sorted streams
    fn sorted_merge(
        &self,
        partitions: Vec<Vec<RecordBatch>>,
        sort_columns: &[SortColumn],
    ) -> Result<Vec<RecordBatch>> {
        // First concat all batches
        let all_batches: Vec<RecordBatch> = partitions.into_iter().flatten().collect();

        if all_batches.is_empty() {
            return Ok(vec![]);
        }

        // Concatenate into a single batch for sorting
        let combined = Self::concat_batches(&all_batches)?;

        // Build sort options
        let sort_columns_arrow: Vec<arrow::compute::SortColumn> = sort_columns
            .iter()
            .filter_map(|sc| {
                let col_idx = combined
                    .schema()
                    .fields()
                    .iter()
                    .position(|f| f.name() == &sc.name)?;
                Some(arrow::compute::SortColumn {
                    values: combined.column(col_idx).clone(),
                    options: Some(arrow::compute::SortOptions {
                        descending: !sc.ascending,
                        nulls_first: sc.nulls_first,
                    }),
                })
            })
            .collect();

        if sort_columns_arrow.is_empty() {
            return Ok(vec![combined]);
        }

        // Sort
        let indices = compute::lexsort_to_indices(&sort_columns_arrow, None)
            .map_err(|e| DistributedError::PartitionError(e.to_string()))?;

        let sorted_columns: Vec<ArrayRef> = combined
            .columns()
            .iter()
            .map(|col| compute::take(col.as_ref(), &indices, None))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| DistributedError::PartitionError(e.to_string()))?;

        let sorted = RecordBatch::try_new(combined.schema(), sorted_columns)
            .map_err(|e| DistributedError::PartitionError(e.to_string()))?;

        Ok(vec![sorted])
    }

    /// Union with deduplication
    fn union_distinct(
        &self,
        partitions: Vec<Vec<RecordBatch>>,
        _key_columns: &[String],
    ) -> Result<Vec<RecordBatch>> {
        // For now, just concatenate (proper deduplication would use a hash set)
        // TODO: Implement proper deduplication
        self.concat_merge(partitions)
    }

    /// Concatenate multiple batches into one
    fn concat_batches(batches: &[RecordBatch]) -> Result<RecordBatch> {
        if batches.is_empty() {
            return Err(DistributedError::PartitionError(
                "No batches to concatenate".to_string(),
            ));
        }

        let schema = batches[0].schema();
        arrow::compute::concat_batches(&schema, batches)
            .map_err(|e| DistributedError::PartitionError(e.to_string()))
    }

    /// Get output schema
    pub fn schema(&self) -> &ArrowSchema {
        &self.schema
    }
}

/// Result collector for gathering partition results
#[derive(Debug)]
pub struct ResultCollector {
    /// Expected number of partitions
    expected_partitions: usize,
    /// Collected results by partition index
    results: HashMap<usize, Vec<RecordBatch>>,
    /// Merge operator
    merge: Merge,
}

impl ResultCollector {
    /// Create a new result collector
    pub fn new(expected_partitions: usize, schema: Arc<ArrowSchema>) -> Self {
        Self {
            expected_partitions,
            results: HashMap::new(),
            merge: Merge::concat(schema),
        }
    }

    /// Create with sorted merge
    pub fn with_sorted_merge(
        expected_partitions: usize,
        schema: Arc<ArrowSchema>,
        sort_columns: Vec<SortColumn>,
    ) -> Self {
        Self {
            expected_partitions,
            results: HashMap::new(),
            merge: Merge::sorted(schema, sort_columns),
        }
    }

    /// Add result from a partition
    pub fn add_result(&mut self, partition_index: usize, batches: Vec<RecordBatch>) {
        self.results.insert(partition_index, batches);
    }

    /// Check if all results are collected
    pub fn is_complete(&self) -> bool {
        self.results.len() >= self.expected_partitions
    }

    /// Get number of collected partitions
    pub fn collected_count(&self) -> usize {
        self.results.len()
    }

    /// Finalize and get merged results
    pub fn finalize(self) -> Result<Vec<RecordBatch>> {
        if !self.is_complete() {
            return Err(DistributedError::PartitionError(format!(
                "Only {} of {} partitions collected",
                self.results.len(),
                self.expected_partitions
            )));
        }

        // Sort by partition index and collect batches
        let mut partitions: Vec<(usize, Vec<RecordBatch>)> = self.results.into_iter().collect();
        partitions.sort_by_key(|(idx, _)| *idx);

        let ordered_partitions: Vec<Vec<RecordBatch>> =
            partitions.into_iter().map(|(_, batches)| batches).collect();

        self.merge.execute(ordered_partitions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn create_test_batch(ids: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids))]).unwrap()
    }

    #[test]
    fn test_exchange_hash() {
        let exchange = Exchange::hash(vec!["id".to_string()], 4);
        let batches = vec![create_test_batch(vec![1, 2, 3, 4, 5])];

        let partitions = exchange.execute(&batches).unwrap();
        assert_eq!(partitions.len(), 4);

        let total_rows: usize = partitions.iter().map(|p| p.row_count()).sum();
        assert_eq!(total_rows, 5);
    }

    #[test]
    fn test_exchange_gather() {
        let exchange = Exchange::gather();
        let batches = vec![create_test_batch(vec![1, 2]), create_test_batch(vec![3, 4])];

        let partitions = exchange.execute(&batches).unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].row_count(), 4);
    }

    #[test]
    fn test_merge_concat() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let merge = Merge::concat(schema);

        let partitions = vec![
            vec![create_test_batch(vec![1, 2])],
            vec![create_test_batch(vec![3, 4])],
        ];

        let result = merge.execute(partitions).unwrap();
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4);
    }

    #[test]
    fn test_merge_sorted() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let merge = Merge::sorted(
            schema,
            vec![SortColumn {
                name: "id".to_string(),
                ascending: true,
                nulls_first: false,
            }],
        );

        let partitions = vec![
            vec![create_test_batch(vec![3, 1])],
            vec![create_test_batch(vec![4, 2])],
        ];

        let result = merge.execute(partitions).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 4);

        // Check sorted order
        let id_col = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(1), 2);
        assert_eq!(id_col.value(2), 3);
        assert_eq!(id_col.value(3), 4);
    }

    #[test]
    fn test_result_collector() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let mut collector = ResultCollector::new(2, schema);

        assert!(!collector.is_complete());

        collector.add_result(0, vec![create_test_batch(vec![1, 2])]);
        assert!(!collector.is_complete());

        collector.add_result(1, vec![create_test_batch(vec![3, 4])]);
        assert!(collector.is_complete());

        let result = collector.finalize().unwrap();
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4);
    }
}
