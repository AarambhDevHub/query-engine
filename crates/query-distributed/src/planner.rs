//! Distributed query planner

use crate::error::{DistributedError, Result};
use crate::partition::PartitionStrategy;
use query_planner::LogicalPlan;
use serde::{Deserialize, Serialize};

/// Distributed execution plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DistributedPlan {
    /// Execute locally (single node)
    Local {
        /// Original logical plan
        plan: SerializedPlan,
    },

    /// Distributed execution with stages
    Distributed {
        /// Query stages to execute
        stages: Vec<QueryStage>,
        /// Final aggregation stage (if needed)
        final_stage: Option<QueryStage>,
    },
}

/// Serialized plan representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedPlan {
    /// Serialized plan data
    pub data: Vec<u8>,
    /// Plan description for debugging
    pub description: String,
}

impl SerializedPlan {
    /// Create from description (placeholder)
    pub fn from_description(description: impl Into<String>) -> Self {
        Self {
            data: Vec::new(),
            description: description.into(),
        }
    }
}

/// A stage in distributed query execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryStage {
    /// Stage ID
    pub id: usize,
    /// Plan fragment to execute
    pub plan: SerializedPlan,
    /// Partitioning strategy for this stage
    pub partition_strategy: PartitionStrategy,
    /// Number of partitions
    pub num_partitions: usize,
    /// Dependencies on other stages
    pub dependencies: Vec<usize>,
    /// Whether this stage requires a shuffle
    pub requires_shuffle: bool,
}

impl QueryStage {
    /// Create a new query stage
    pub fn new(id: usize, plan: SerializedPlan, num_partitions: usize) -> Self {
        Self {
            id,
            plan,
            partition_strategy: PartitionStrategy::default(),
            num_partitions,
            dependencies: Vec::new(),
            requires_shuffle: false,
        }
    }

    /// Set partition strategy
    pub fn with_partition_strategy(mut self, strategy: PartitionStrategy) -> Self {
        self.partition_strategy = strategy;
        self
    }

    /// Add dependency
    pub fn with_dependency(mut self, stage_id: usize) -> Self {
        self.dependencies.push(stage_id);
        self
    }

    /// Mark as requiring shuffle
    pub fn with_shuffle(mut self) -> Self {
        self.requires_shuffle = true;
        self
    }
}

/// Distributed query planner
pub struct DistributedPlanner {
    /// Minimum rows to trigger distribution
    #[allow(dead_code)]
    min_rows_for_distribution: usize,
    /// Default number of partitions
    default_partitions: usize,
}

impl DistributedPlanner {
    /// Create a new distributed planner
    pub fn new() -> Self {
        Self {
            min_rows_for_distribution: 10000,
            default_partitions: 4,
        }
    }

    /// Create with custom configuration
    pub fn with_config(min_rows: usize, default_partitions: usize) -> Self {
        Self {
            min_rows_for_distribution: min_rows,
            default_partitions,
        }
    }

    /// Create a distributed execution plan
    pub fn plan(&self, logical_plan: &LogicalPlan, num_workers: usize) -> Result<DistributedPlan> {
        if num_workers == 0 {
            return Err(DistributedError::NoWorkersAvailable);
        }

        // Analyze the plan to determine if it should be distributed
        let should_distribute = self.should_distribute(logical_plan, num_workers);

        if !should_distribute {
            return Ok(DistributedPlan::Local {
                plan: self.serialize_plan(logical_plan),
            });
        }

        // Create distributed plan
        let stages = self.create_stages(logical_plan, num_workers)?;

        Ok(DistributedPlan::Distributed {
            stages,
            final_stage: None,
        })
    }

    /// Determine if a plan should be distributed
    fn should_distribute(&self, plan: &LogicalPlan, num_workers: usize) -> bool {
        // Simple heuristic: distribute if we have multiple workers
        // In production, this would analyze data size, plan complexity, etc.
        if num_workers <= 1 {
            return false;
        }

        // Check plan type
        match plan {
            LogicalPlan::TableScan { .. } => true,
            LogicalPlan::Filter { input, .. } => self.should_distribute(input, num_workers),
            LogicalPlan::Projection { input, .. } => self.should_distribute(input, num_workers),
            LogicalPlan::Aggregate { .. } => true, // Aggregates benefit from distribution
            LogicalPlan::Join { .. } => true,      // Joins can be distributed
            LogicalPlan::Sort { .. } => false,     // Sort requires all data
            LogicalPlan::Limit { .. } => false,    // Limit is local
            _ => false,
        }
    }

    /// Create execution stages from a logical plan
    fn create_stages(&self, plan: &LogicalPlan, num_workers: usize) -> Result<Vec<QueryStage>> {
        let mut stages = Vec::new();
        let num_partitions = num_workers.min(self.default_partitions);

        match plan {
            LogicalPlan::TableScan { table_name, .. } => {
                // Single stage for table scan
                let stage = QueryStage::new(
                    0,
                    SerializedPlan::from_description(format!("TableScan: {}", table_name)),
                    num_partitions,
                )
                .with_partition_strategy(PartitionStrategy::RoundRobin { num_partitions });

                stages.push(stage);
            }

            LogicalPlan::Filter { input, .. } => {
                // Create stages for input first
                let input_stages = self.create_stages(input, num_workers)?;
                stages.extend(input_stages);

                // Filter runs on same partitions as input
                let stage_id = stages.len();
                let stage = QueryStage::new(
                    stage_id,
                    SerializedPlan::from_description("Filter"),
                    num_partitions,
                )
                .with_dependency(stage_id - 1);

                stages.push(stage);
            }

            LogicalPlan::Aggregate { input, .. } => {
                // Create stages for input
                let input_stages = self.create_stages(input, num_workers)?;
                stages.extend(input_stages);

                // Partial aggregation on partitions
                let partial_stage_id = stages.len();
                let partial_stage = QueryStage::new(
                    partial_stage_id,
                    SerializedPlan::from_description("PartialAggregate"),
                    num_partitions,
                )
                .with_dependency(partial_stage_id - 1);

                stages.push(partial_stage);

                // Final aggregation (single partition)
                let final_stage = QueryStage::new(
                    partial_stage_id + 1,
                    SerializedPlan::from_description("FinalAggregate"),
                    1,
                )
                .with_dependency(partial_stage_id)
                .with_shuffle();

                stages.push(final_stage);
            }

            LogicalPlan::Join { left, right, .. } => {
                // Create stages for both inputs
                let left_stages = self.create_stages(left, num_workers)?;
                let left_last_id = left_stages.len();
                stages.extend(left_stages);

                let right_stages = self.create_stages(right, num_workers)?;
                let right_last_id = stages.len() + right_stages.len();
                stages.extend(right_stages);

                // Join stage
                let join_stage = QueryStage::new(
                    stages.len(),
                    SerializedPlan::from_description("HashJoin"),
                    num_partitions,
                )
                .with_dependency(left_last_id - 1)
                .with_dependency(right_last_id - 1)
                .with_shuffle();

                stages.push(join_stage);
            }

            _ => {
                // Default: single stage
                let stage = QueryStage::new(
                    0,
                    SerializedPlan::from_description(format!("{:?}", plan)),
                    1,
                );
                stages.push(stage);
            }
        }

        Ok(stages)
    }

    /// Serialize a logical plan (placeholder)
    fn serialize_plan(&self, plan: &LogicalPlan) -> SerializedPlan {
        // TODO: Implement actual serialization
        SerializedPlan::from_description(format!("{:?}", plan))
    }

    /// Identify exchange points in a plan
    pub fn identify_exchanges(&self, plan: &LogicalPlan) -> Vec<ExchangePoint> {
        let mut exchanges = Vec::new();
        self.find_exchanges(plan, &mut exchanges);
        exchanges
    }

    /// Recursively find exchange points
    fn find_exchanges(&self, plan: &LogicalPlan, exchanges: &mut Vec<ExchangePoint>) {
        match plan {
            LogicalPlan::Aggregate {
                input, group_exprs, ..
            } => {
                // Aggregation may require shuffle by group key
                if !group_exprs.is_empty() {
                    exchanges.push(ExchangePoint {
                        location: "Before final aggregation".to_string(),
                        reason: ExchangeReason::Aggregation,
                    });
                }
                self.find_exchanges(input, exchanges);
            }

            LogicalPlan::Join { left, right, .. } => {
                // Join requires shuffle of both inputs
                exchanges.push(ExchangePoint {
                    location: "Before join".to_string(),
                    reason: ExchangeReason::Join,
                });
                self.find_exchanges(left, exchanges);
                self.find_exchanges(right, exchanges);
            }

            LogicalPlan::Sort { input, .. } => {
                // Sort requires all data on one node
                exchanges.push(ExchangePoint {
                    location: "Before sort".to_string(),
                    reason: ExchangeReason::Sort,
                });
                self.find_exchanges(input, exchanges);
            }

            LogicalPlan::Filter { input, .. } => {
                self.find_exchanges(input, exchanges);
            }

            LogicalPlan::Projection { input, .. } => {
                self.find_exchanges(input, exchanges);
            }

            LogicalPlan::Limit { input, .. } => {
                self.find_exchanges(input, exchanges);
            }

            _ => {}
        }
    }
}

impl Default for DistributedPlanner {
    fn default() -> Self {
        Self::new()
    }
}

/// A point where data exchange (shuffle) is needed
#[derive(Debug, Clone)]
pub struct ExchangePoint {
    /// Location in the plan
    pub location: String,
    /// Reason for exchange
    pub reason: ExchangeReason,
}

/// Reason for data exchange
#[derive(Debug, Clone, Copy)]
pub enum ExchangeReason {
    /// Repartition for aggregation
    Aggregation,
    /// Repartition for join
    Join,
    /// Gather for sort
    Sort,
    /// Explicit repartition
    Repartition,
}

#[cfg(test)]
mod tests {
    use super::*;
    use query_core::Schema;

    fn create_test_plan() -> LogicalPlan {
        LogicalPlan::TableScan {
            table_name: "test".to_string(),
            schema: Schema::new(vec![]),
            projection: None,
        }
    }

    #[test]
    fn test_local_plan_single_worker() {
        let planner = DistributedPlanner::new();
        let plan = create_test_plan();

        let result = planner.plan(&plan, 1).unwrap();

        match result {
            DistributedPlan::Local { .. } => {}
            _ => panic!("Expected local plan for single worker"),
        }
    }

    #[test]
    fn test_distributed_plan_multiple_workers() {
        let planner = DistributedPlanner::new();
        let plan = create_test_plan();

        let result = planner.plan(&plan, 4).unwrap();

        match result {
            DistributedPlan::Distributed { stages, .. } => {
                assert!(!stages.is_empty());
            }
            _ => panic!("Expected distributed plan for multiple workers"),
        }
    }

    #[test]
    fn test_no_workers_error() {
        let planner = DistributedPlanner::new();
        let plan = create_test_plan();

        let result = planner.plan(&plan, 0);

        assert!(result.is_err());
    }

    #[test]
    fn test_query_stage_builder() {
        let stage = QueryStage::new(0, SerializedPlan::from_description("Test"), 4)
            .with_partition_strategy(PartitionStrategy::Hash {
                key_columns: vec!["id".to_string()],
                num_partitions: 4,
            })
            .with_dependency(1)
            .with_shuffle();

        assert_eq!(stage.id, 0);
        assert_eq!(stage.num_partitions, 4);
        assert!(stage.dependencies.contains(&1));
        assert!(stage.requires_shuffle);
    }
}
