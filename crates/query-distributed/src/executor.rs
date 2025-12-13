//! Distributed query executor
//!
//! Orchestrates distributed query execution across workers.

use crate::coordinator::Coordinator;
use crate::error::{DistributedError, Result};
use crate::operators::{Exchange, Merge};
use crate::planner::{DistributedPlan, DistributedPlanner, QueryStage};
use crate::types::{QueryId, QueryTask};
use arrow::record_batch::RecordBatch;
use parking_lot::RwLock;
use query_planner::LogicalPlan;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

/// Distributed query executor
pub struct DistributedExecutor {
    /// Coordinator reference
    coordinator: Arc<Coordinator>,
    /// Execution configuration
    config: ExecutorConfig,
    /// Active queries
    active_queries: RwLock<HashMap<QueryId, QueryExecution>>,
}

/// Configuration for distributed executor
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    /// Maximum concurrent queries
    pub max_concurrent_queries: usize,
    /// Task timeout in milliseconds
    pub task_timeout_ms: u64,
    /// Batch size for data transfer
    pub batch_size: usize,
    /// Enable adaptive execution
    pub adaptive_execution: bool,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            max_concurrent_queries: 10,
            task_timeout_ms: 300000, // 5 minutes
            batch_size: 8192,
            adaptive_execution: false,
        }
    }
}

/// Tracks execution state of a distributed query
#[derive(Debug)]
pub struct QueryExecution {
    /// Query ID
    pub query_id: QueryId,
    /// Current stage being executed
    pub current_stage: usize,
    /// Total stages
    pub total_stages: usize,
    /// Start time
    pub start_time: Instant,
    /// Stage results (stage_id -> batches)
    pub stage_results: HashMap<usize, Vec<RecordBatch>>,
    /// Execution status
    pub status: QueryExecutionStatus,
}

/// Status of query execution
#[derive(Debug, Clone, PartialEq)]
pub enum QueryExecutionStatus {
    /// Query is pending
    Pending,
    /// Query is running
    Running,
    /// Query completed successfully
    Completed,
    /// Query failed
    Failed(String),
    /// Query was cancelled
    Cancelled,
}

impl DistributedExecutor {
    /// Create a new distributed executor
    pub fn new(coordinator: Arc<Coordinator>) -> Self {
        Self {
            coordinator,
            config: ExecutorConfig::default(),
            active_queries: RwLock::new(HashMap::new()),
        }
    }

    /// Create with custom configuration
    pub fn with_config(coordinator: Arc<Coordinator>, config: ExecutorConfig) -> Self {
        Self {
            coordinator,
            config,
            active_queries: RwLock::new(HashMap::new()),
        }
    }

    /// Execute a distributed query
    pub async fn execute(&self, plan: &LogicalPlan) -> Result<Vec<RecordBatch>> {
        let query_id = QueryId::new();
        let start_time = Instant::now();

        // Get worker count
        let num_workers = self.coordinator.active_worker_count();
        if num_workers == 0 {
            return Err(DistributedError::NoWorkersAvailable);
        }

        // Create distributed plan
        let planner = DistributedPlanner::new();
        let distributed_plan = planner.plan(plan, num_workers)?;

        // Execute based on plan type
        let result = match distributed_plan {
            DistributedPlan::Local {
                plan: serialized_plan,
            } => {
                // Execute locally on coordinator
                self.execute_local(&serialized_plan.description).await
            }
            DistributedPlan::Distributed {
                stages,
                final_stage,
            } => {
                // Execute distributed
                self.execute_distributed(query_id, stages, final_stage)
                    .await
            }
        };

        let elapsed = start_time.elapsed();
        tracing::info!("Query {} completed in {:?}", query_id, elapsed);

        result
    }

    /// Execute a query locally
    async fn execute_local(&self, _plan_desc: &str) -> Result<Vec<RecordBatch>> {
        // For now, return empty - actual implementation would deserialize and execute
        Ok(vec![])
    }

    /// Execute a distributed query with stages
    async fn execute_distributed(
        &self,
        query_id: QueryId,
        stages: Vec<QueryStage>,
        final_stage: Option<QueryStage>,
    ) -> Result<Vec<RecordBatch>> {
        let total_stages = stages.len() + if final_stage.is_some() { 1 } else { 0 };

        // Track execution
        {
            let mut active = self.active_queries.write();
            active.insert(
                query_id,
                QueryExecution {
                    query_id,
                    current_stage: 0,
                    total_stages,
                    start_time: Instant::now(),
                    stage_results: HashMap::new(),
                    status: QueryExecutionStatus::Running,
                },
            );
        }

        // Execute each stage
        let mut stage_results: HashMap<usize, Vec<RecordBatch>> = HashMap::new();

        for (stage_idx, stage) in stages.iter().enumerate() {
            let result = self.execute_stage(query_id, stage, &stage_results).await?;
            stage_results.insert(stage.id, result);

            // Update progress
            {
                let mut active = self.active_queries.write();
                if let Some(exec) = active.get_mut(&query_id) {
                    exec.current_stage = stage_idx + 1;
                }
            }
        }

        // Execute final stage if present
        let final_result = if let Some(final_stage) = final_stage {
            self.execute_stage(query_id, &final_stage, &stage_results)
                .await?
        } else {
            // Return results from last stage
            stages
                .last()
                .and_then(|s| stage_results.get(&s.id).cloned())
                .unwrap_or_default()
        };

        // Mark complete
        {
            let mut active = self.active_queries.write();
            if let Some(exec) = active.get_mut(&query_id) {
                exec.status = QueryExecutionStatus::Completed;
            }
        }

        Ok(final_result)
    }

    /// Execute a single stage
    async fn execute_stage(
        &self,
        query_id: QueryId,
        stage: &QueryStage,
        previous_results: &HashMap<usize, Vec<RecordBatch>>,
    ) -> Result<Vec<RecordBatch>> {
        tracing::debug!("Executing stage {} for query {}", stage.id, query_id);

        // Get input data from dependencies
        let input_batches: Vec<RecordBatch> = stage
            .dependencies
            .iter()
            .filter_map(|dep_id| previous_results.get(dep_id))
            .flatten()
            .cloned()
            .collect();

        // Partition data
        let exchange = Exchange::new(stage.partition_strategy.clone(), stage.num_partitions);
        let partitions = exchange.execute(&input_batches)?;

        // Create tasks for each partition
        let scheduler = self.coordinator.scheduler();
        let mut tasks = Vec::new();

        for (partition_idx, partition) in partitions.iter().enumerate() {
            let task = QueryTask::new(query_id, partition_idx, stage.num_partitions);
            tasks.push((task, partition.batches.clone()));
        }

        // Submit tasks (in real implementation, this would send to workers)
        for (task, _batches) in &tasks {
            scheduler.submit(task.clone());
        }

        // Collect results (simulated - in real implementation would receive from workers)
        let mut results = Vec::new();
        for (_task, batches) in tasks {
            results.extend(batches);
        }

        // If stage requires shuffle, merge results
        if stage.requires_shuffle {
            let schema = if let Some(batch) = results.first() {
                batch.schema()
            } else {
                return Ok(vec![]);
            };

            let merge = Merge::concat(schema);
            results = merge.execute(vec![results])?;
        }

        Ok(results)
    }

    /// Cancel a running query
    pub fn cancel_query(&self, query_id: QueryId) -> Result<()> {
        let mut active = self.active_queries.write();
        if let Some(exec) = active.get_mut(&query_id) {
            exec.status = QueryExecutionStatus::Cancelled;
            Ok(())
        } else {
            Err(DistributedError::TaskExecutionFailed(format!(
                "Query {} not found",
                query_id
            )))
        }
    }

    /// Get query execution status
    pub fn get_query_status(&self, query_id: QueryId) -> Option<QueryExecutionStatus> {
        let active = self.active_queries.read();
        active.get(&query_id).map(|e| e.status.clone())
    }

    /// Get number of active queries
    pub fn active_query_count(&self) -> usize {
        let active = self.active_queries.read();
        active
            .values()
            .filter(|e| e.status == QueryExecutionStatus::Running)
            .count()
    }

    /// Clean up completed queries
    pub fn cleanup_completed(&self) {
        let mut active = self.active_queries.write();
        active.retain(|_, e| e.status == QueryExecutionStatus::Running);
    }
}

/// Statistics for distributed execution
#[derive(Debug, Clone, Default)]
pub struct ExecutionStats {
    /// Total queries executed
    pub total_queries: usize,
    /// Successful queries
    pub successful_queries: usize,
    /// Failed queries
    pub failed_queries: usize,
    /// Total data processed (bytes)
    pub total_bytes_processed: usize,
    /// Average query time (ms)
    pub avg_query_time_ms: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_config_default() {
        let config = ExecutorConfig::default();
        assert_eq!(config.max_concurrent_queries, 10);
        assert_eq!(config.task_timeout_ms, 300000);
    }

    #[test]
    fn test_query_execution_status() {
        let status = QueryExecutionStatus::Running;
        assert_eq!(status, QueryExecutionStatus::Running);

        let failed_status = QueryExecutionStatus::Failed("error".to_string());
        assert!(matches!(failed_status, QueryExecutionStatus::Failed(_)));
    }

    #[tokio::test]
    async fn test_executor_creation() {
        let coordinator = Arc::new(Coordinator::default());
        let executor = DistributedExecutor::new(coordinator);

        assert_eq!(executor.active_query_count(), 0);
    }
}
