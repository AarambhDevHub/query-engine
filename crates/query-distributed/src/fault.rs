//! Fault tolerance for distributed execution
//!
//! Provides failure detection, task retry, and recovery mechanisms.

use crate::types::{QueryId, QueryTask, TaskId, WorkerId};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Fault tolerance manager
pub struct FaultManager {
    /// Configuration
    config: FaultConfig,
    /// Failed task tracking
    failed_tasks: DashMap<TaskId, FailedTask>,
    /// Worker failure tracking
    worker_failures: DashMap<WorkerId, WorkerFailure>,
    /// Task retry queue
    retry_queue: RwLock<VecDeque<QueryTask>>,
    /// Checkpoints
    checkpoints: DashMap<QueryId, QueryCheckpoint>,
}

/// Configuration for fault tolerance
#[derive(Debug, Clone)]
pub struct FaultConfig {
    /// Maximum retries per task
    pub max_task_retries: usize,
    /// Retry delay in milliseconds
    pub retry_delay_ms: u64,
    /// Worker failure threshold (consecutive failures)
    pub worker_failure_threshold: usize,
    /// Checkpoint interval in milliseconds
    pub checkpoint_interval_ms: u64,
    /// Enable checkpointing
    pub enable_checkpoints: bool,
}

impl Default for FaultConfig {
    fn default() -> Self {
        Self {
            max_task_retries: 3,
            retry_delay_ms: 1000,
            worker_failure_threshold: 3,
            checkpoint_interval_ms: 30000, // 30 seconds
            enable_checkpoints: true,
        }
    }
}

/// Information about a failed task
#[derive(Debug, Clone)]
pub struct FailedTask {
    /// Task that failed
    pub task: QueryTask,
    /// Worker that was executing
    pub worker_id: WorkerId,
    /// Failure reason
    pub reason: String,
    /// Failure time
    pub failed_at: Instant,
    /// Retry count
    pub retry_count: usize,
}

/// Information about worker failures
#[derive(Debug, Clone)]
pub struct WorkerFailure {
    /// Worker ID
    pub worker_id: WorkerId,
    /// Consecutive failure count
    pub consecutive_failures: usize,
    /// Last failure time
    pub last_failure: Instant,
    /// Total failures
    pub total_failures: usize,
}

/// Checkpoint for query state
#[derive(Debug, Clone)]
pub struct QueryCheckpoint {
    /// Query ID
    pub query_id: QueryId,
    /// Completed stage IDs
    pub completed_stages: Vec<usize>,
    /// Checkpoint time
    pub checkpoint_time: Instant,
    /// Intermediate results (serialized)
    pub intermediate_data: Vec<Vec<u8>>,
}

impl FaultManager {
    /// Create a new fault manager
    pub fn new(config: FaultConfig) -> Self {
        Self {
            config,
            failed_tasks: DashMap::new(),
            worker_failures: DashMap::new(),
            retry_queue: RwLock::new(VecDeque::new()),
            checkpoints: DashMap::new(),
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(FaultConfig::default())
    }

    /// Handle a task failure
    pub fn handle_task_failure(
        &self,
        task: QueryTask,
        worker_id: WorkerId,
        reason: String,
    ) -> TaskRecoveryAction {
        let task_id = task.id;

        // Record failure
        let retry_count = task.retry_count + 1;
        self.failed_tasks.insert(
            task_id,
            FailedTask {
                task: task.clone(),
                worker_id,
                reason: reason.clone(),
                failed_at: Instant::now(),
                retry_count,
            },
        );

        // Update worker failure count
        self.record_worker_failure(worker_id);

        // Decide on recovery action
        if retry_count <= self.config.max_task_retries {
            // Schedule retry
            let mut updated_task = task;
            updated_task.retry_count = retry_count;
            self.retry_queue.write().push_back(updated_task);

            TaskRecoveryAction::Retry {
                delay_ms: self.config.retry_delay_ms,
            }
        } else {
            TaskRecoveryAction::Fail {
                reason: format!(
                    "Task {} failed after {} retries: {}",
                    task_id, retry_count, reason
                ),
            }
        }
    }

    /// Handle worker failure
    pub fn handle_worker_failure(&self, worker_id: WorkerId) -> WorkerRecoveryAction {
        let failure = self.record_worker_failure(worker_id);

        if failure.consecutive_failures >= self.config.worker_failure_threshold {
            WorkerRecoveryAction::Remove {
                worker_id,
                reason: format!(
                    "Worker {} exceeded failure threshold ({} failures)",
                    worker_id, failure.consecutive_failures
                ),
            }
        } else {
            WorkerRecoveryAction::MarkUnhealthy { worker_id }
        }
    }

    /// Record a worker failure
    fn record_worker_failure(&self, worker_id: WorkerId) -> WorkerFailure {
        let mut entry = self
            .worker_failures
            .entry(worker_id)
            .or_insert_with(|| WorkerFailure {
                worker_id,
                consecutive_failures: 0,
                last_failure: Instant::now(),
                total_failures: 0,
            });

        entry.consecutive_failures += 1;
        entry.total_failures += 1;
        entry.last_failure = Instant::now();

        entry.clone()
    }

    /// Record successful task execution (resets failure count)
    pub fn record_task_success(&self, worker_id: WorkerId) {
        if let Some(mut entry) = self.worker_failures.get_mut(&worker_id) {
            entry.consecutive_failures = 0;
        }
    }

    /// Get next task to retry
    pub fn get_retry_task(&self) -> Option<QueryTask> {
        self.retry_queue.write().pop_front()
    }

    /// Get pending retry count
    pub fn pending_retry_count(&self) -> usize {
        self.retry_queue.read().len()
    }

    /// Create a checkpoint for a query
    pub fn create_checkpoint(
        &self,
        query_id: QueryId,
        completed_stages: Vec<usize>,
        data: Vec<Vec<u8>>,
    ) {
        if !self.config.enable_checkpoints {
            return;
        }

        let checkpoint = QueryCheckpoint {
            query_id,
            completed_stages,
            checkpoint_time: Instant::now(),
            intermediate_data: data,
        };

        self.checkpoints.insert(query_id, checkpoint);
        tracing::debug!("Created checkpoint for query {}", query_id);
    }

    /// Get checkpoint for a query
    pub fn get_checkpoint(&self, query_id: QueryId) -> Option<QueryCheckpoint> {
        self.checkpoints.get(&query_id).map(|c| c.clone())
    }

    /// Remove checkpoint
    pub fn remove_checkpoint(&self, query_id: QueryId) {
        self.checkpoints.remove(&query_id);
    }

    /// Recover query from checkpoint
    pub fn recover_from_checkpoint(&self, query_id: QueryId) -> Option<RecoveryPlan> {
        let checkpoint = self.checkpoints.get(&query_id)?;

        Some(RecoveryPlan {
            query_id,
            resume_from_stage: checkpoint.completed_stages.last().copied().unwrap_or(0),
            intermediate_data: checkpoint.intermediate_data.clone(),
        })
    }

    /// Check if a worker should be considered healthy
    pub fn is_worker_healthy(&self, worker_id: WorkerId) -> bool {
        match self.worker_failures.get(&worker_id) {
            Some(failure) => failure.consecutive_failures < self.config.worker_failure_threshold,
            None => true,
        }
    }

    /// Get failure statistics
    pub fn get_stats(&self) -> FaultStats {
        FaultStats {
            total_failed_tasks: self.failed_tasks.len(),
            pending_retries: self.pending_retry_count(),
            unhealthy_workers: self
                .worker_failures
                .iter()
                .filter(|f| f.consecutive_failures >= self.config.worker_failure_threshold)
                .count(),
            active_checkpoints: self.checkpoints.len(),
        }
    }

    /// Clean up old failure records
    pub fn cleanup(&self, max_age: Duration) {
        let now = Instant::now();

        // Clean up old failed tasks
        self.failed_tasks
            .retain(|_, v| now.duration_since(v.failed_at) < max_age);

        // Clean up old worker failures
        self.worker_failures
            .retain(|_, v| now.duration_since(v.last_failure) < max_age);
    }
}

/// Action to take for task recovery
#[derive(Debug, Clone)]
pub enum TaskRecoveryAction {
    /// Retry the task
    Retry { delay_ms: u64 },
    /// Fail the task permanently
    Fail { reason: String },
}

/// Action to take for worker recovery
#[derive(Debug, Clone)]
pub enum WorkerRecoveryAction {
    /// Mark worker as unhealthy
    MarkUnhealthy { worker_id: WorkerId },
    /// Remove worker from cluster
    Remove { worker_id: WorkerId, reason: String },
}

/// Plan for recovering a query
#[derive(Debug, Clone)]
pub struct RecoveryPlan {
    /// Query ID
    pub query_id: QueryId,
    /// Stage to resume from
    pub resume_from_stage: usize,
    /// Intermediate data from checkpoint
    pub intermediate_data: Vec<Vec<u8>>,
}

/// Fault tolerance statistics
#[derive(Debug, Clone, Default)]
pub struct FaultStats {
    /// Total failed tasks
    pub total_failed_tasks: usize,
    /// Pending retries
    pub pending_retries: usize,
    /// Unhealthy worker count
    pub unhealthy_workers: usize,
    /// Active checkpoints
    pub active_checkpoints: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_task() -> QueryTask {
        QueryTask::new(QueryId::new(), 0, 4)
    }

    #[test]
    fn test_fault_manager_creation() {
        let fm = FaultManager::with_defaults();
        assert_eq!(fm.pending_retry_count(), 0);
    }

    #[test]
    fn test_task_failure_retry() {
        let fm = FaultManager::with_defaults();
        let task = create_test_task();
        let worker_id = WorkerId::new();

        let action = fm.handle_task_failure(task, worker_id, "Test error".to_string());

        assert!(matches!(action, TaskRecoveryAction::Retry { .. }));
        assert_eq!(fm.pending_retry_count(), 1);
    }

    #[test]
    fn test_task_failure_max_retries() {
        let config = FaultConfig {
            max_task_retries: 1,
            ..Default::default()
        };
        let fm = FaultManager::new(config);
        let worker_id = WorkerId::new();

        // First failure - should retry
        let mut task = create_test_task();
        let action = fm.handle_task_failure(task.clone(), worker_id, "Error".to_string());
        assert!(matches!(action, TaskRecoveryAction::Retry { .. }));

        // Get retry task
        let retry_task = fm.get_retry_task().unwrap();
        assert_eq!(retry_task.retry_count, 1);

        // Second failure - should fail
        let action = fm.handle_task_failure(retry_task, worker_id, "Error".to_string());
        assert!(matches!(action, TaskRecoveryAction::Fail { .. }));
    }

    #[test]
    fn test_worker_failure_threshold() {
        let config = FaultConfig {
            worker_failure_threshold: 2,
            ..Default::default()
        };
        let fm = FaultManager::new(config);
        let worker_id = WorkerId::new();

        // First failure
        let action = fm.handle_worker_failure(worker_id);
        assert!(matches!(action, WorkerRecoveryAction::MarkUnhealthy { .. }));
        assert!(fm.is_worker_healthy(worker_id));

        // Second failure - exceeds threshold
        let action = fm.handle_worker_failure(worker_id);
        assert!(matches!(action, WorkerRecoveryAction::Remove { .. }));
        assert!(!fm.is_worker_healthy(worker_id));
    }

    #[test]
    fn test_task_success_resets_failures() {
        let fm = FaultManager::with_defaults();
        let worker_id = WorkerId::new();

        // Record some failures
        fm.handle_worker_failure(worker_id);
        fm.handle_worker_failure(worker_id);

        // Record success
        fm.record_task_success(worker_id);

        // Worker should be healthy now
        assert!(fm.is_worker_healthy(worker_id));
    }

    #[test]
    fn test_checkpoint_creation() {
        let fm = FaultManager::with_defaults();
        let query_id = QueryId::new();

        fm.create_checkpoint(query_id, vec![0, 1], vec![vec![1, 2, 3]]);

        let checkpoint = fm.get_checkpoint(query_id);
        assert!(checkpoint.is_some());
        assert_eq!(checkpoint.unwrap().completed_stages, vec![0, 1]);
    }

    #[test]
    fn test_recovery_plan() {
        let fm = FaultManager::with_defaults();
        let query_id = QueryId::new();

        fm.create_checkpoint(query_id, vec![0, 1, 2], vec![]);

        let plan = fm.recover_from_checkpoint(query_id);
        assert!(plan.is_some());
        assert_eq!(plan.unwrap().resume_from_stage, 2);
    }

    #[test]
    fn test_fault_stats() {
        let fm = FaultManager::with_defaults();
        let stats = fm.get_stats();

        assert_eq!(stats.total_failed_tasks, 0);
        assert_eq!(stats.pending_retries, 0);
        assert_eq!(stats.unhealthy_workers, 0);
    }
}
