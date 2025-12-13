//! Worker node for distributed query execution

use crate::error::{DistributedError, Result};
use crate::types::{QueryTask, TaskResult, TaskStatus, WorkerId, WorkerStatus};
use arrow::record_batch::RecordBatch;
use parking_lot::RwLock;
use query_executor::QueryExecutor;
use std::sync::Arc;

/// Worker node that executes query partitions
pub struct Worker {
    /// Unique worker ID
    id: WorkerId,
    /// Network address
    address: String,
    /// Coordinator address
    coordinator_address: Option<String>,
    /// Local query executor
    #[allow(dead_code)]
    executor: QueryExecutor,
    /// Current status
    status: RwLock<WorkerStatus>,
    /// Active task count
    active_tasks: RwLock<usize>,
    /// Maximum concurrent tasks
    max_tasks: usize,
}

impl Worker {
    /// Create a new worker
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            id: WorkerId::new(),
            address: address.into(),
            coordinator_address: None,
            executor: QueryExecutor::new(),
            status: RwLock::new(WorkerStatus::Active),
            active_tasks: RwLock::new(0),
            max_tasks: 4,
        }
    }

    /// Create worker with coordinator address
    pub fn with_coordinator(address: impl Into<String>, coordinator: impl Into<String>) -> Self {
        let mut worker = Self::new(address);
        worker.coordinator_address = Some(coordinator.into());
        worker
    }

    /// Get worker ID
    pub fn id(&self) -> WorkerId {
        self.id
    }

    /// Get worker address
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Get current status
    pub fn status(&self) -> WorkerStatus {
        *self.status.read()
    }

    /// Set worker status
    pub fn set_status(&self, status: WorkerStatus) {
        *self.status.write() = status;
    }

    /// Get active task count
    pub fn active_task_count(&self) -> usize {
        *self.active_tasks.read()
    }

    /// Check if worker can accept more tasks
    pub fn can_accept_task(&self) -> bool {
        let status = *self.status.read();
        let active = *self.active_tasks.read();
        status == WorkerStatus::Active && active < self.max_tasks
    }

    /// Execute a query task (partition)
    pub async fn execute_task(&self, task: QueryTask) -> Result<TaskResult> {
        if !self.can_accept_task() {
            return Err(DistributedError::WorkerUnavailable(
                "Worker at capacity".to_string(),
            ));
        }

        // Increment active tasks
        {
            let mut count = self.active_tasks.write();
            *count += 1;
        }

        let start = std::time::Instant::now();

        // TODO: Deserialize and execute the plan fragment
        // For now, simulate execution
        let result = self.execute_plan_fragment(&task).await;

        // Decrement active tasks
        {
            let mut count = self.active_tasks.write();
            *count = count.saturating_sub(1);
        }

        let execution_time_ms = start.elapsed().as_millis() as u64;

        match result {
            Ok(batches) => {
                let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                Ok(TaskResult {
                    task_id: task.id,
                    status: TaskStatus::Completed,
                    row_count,
                    execution_time_ms,
                    error: None,
                })
            }
            Err(e) => Ok(TaskResult {
                task_id: task.id,
                status: TaskStatus::Failed,
                row_count: 0,
                execution_time_ms,
                error: Some(e.to_string()),
            }),
        }
    }

    /// Execute a plan fragment (placeholder)
    async fn execute_plan_fragment(&self, _task: &QueryTask) -> Result<Vec<RecordBatch>> {
        // TODO: Deserialize plan from task.plan_fragment
        // Execute using self.executor
        // Return results
        Ok(vec![])
    }

    /// Start the worker and register with coordinator
    pub async fn start(&self) -> Result<()> {
        tracing::info!("Starting worker {} at {}", self.id, self.address);

        if let Some(ref _coordinator) = self.coordinator_address {
            // TODO: Register with coordinator via gRPC
            tracing::info!("Would register with coordinator");
        }

        self.set_status(WorkerStatus::Active);
        Ok(())
    }

    /// Shutdown the worker
    pub async fn shutdown(&self) -> Result<()> {
        tracing::info!("Shutting down worker {}", self.id);
        self.set_status(WorkerStatus::Draining);

        // Wait for active tasks to complete
        while self.active_task_count() > 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        self.set_status(WorkerStatus::Removed);
        Ok(())
    }

    /// Send heartbeat to coordinator
    pub async fn send_heartbeat(&self) -> Result<()> {
        if self.coordinator_address.is_none() {
            return Ok(());
        }

        // TODO: Send heartbeat via gRPC
        tracing::debug!("Would send heartbeat from {}", self.id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_creation() {
        let worker = Worker::new("localhost:50051");

        assert_eq!(worker.address(), "localhost:50051");
        assert_eq!(worker.status(), WorkerStatus::Active);
        assert_eq!(worker.active_task_count(), 0);
        assert!(worker.can_accept_task());
    }

    #[test]
    fn test_worker_with_coordinator() {
        let worker = Worker::with_coordinator("localhost:50051", "coordinator:50050");

        assert_eq!(worker.address(), "localhost:50051");
        assert!(worker.coordinator_address.is_some());
    }

    #[test]
    fn test_worker_status_change() {
        let worker = Worker::new("localhost:50051");

        assert_eq!(worker.status(), WorkerStatus::Active);

        worker.set_status(WorkerStatus::Busy);
        assert_eq!(worker.status(), WorkerStatus::Busy);

        worker.set_status(WorkerStatus::Draining);
        assert_eq!(worker.status(), WorkerStatus::Draining);
    }

    #[tokio::test]
    async fn test_execute_task() {
        let worker = Worker::new("localhost:50051");
        let task = QueryTask::new(crate::types::QueryId::new(), 0, 4);

        let result = worker.execute_task(task).await.unwrap();

        assert_eq!(result.status, TaskStatus::Completed);
        assert_eq!(result.row_count, 0); // Placeholder returns empty
    }
}
