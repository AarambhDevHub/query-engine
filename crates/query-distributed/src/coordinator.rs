//! Coordinator node for distributed query execution

use crate::error::{DistributedError, Result};
use crate::planner::DistributedPlanner;
use crate::scheduler::TaskScheduler;
use crate::types::{ClusterConfig, ClusterStatus, QueryId, WorkerId, WorkerInfo, WorkerStatus};
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use query_planner::LogicalPlan;
use std::sync::Arc;

/// Coordinator node that manages distributed query execution
pub struct Coordinator {
    /// Registered workers
    workers: DashMap<WorkerId, WorkerInfo>,
    /// Distributed query planner
    planner: DistributedPlanner,
    /// Task scheduler
    scheduler: Arc<TaskScheduler>,
    /// Cluster configuration
    config: ClusterConfig,
    /// Query results cache
    #[allow(dead_code)]
    results: DashMap<QueryId, Vec<RecordBatch>>,
}

impl Coordinator {
    /// Create a new coordinator
    pub fn new(config: ClusterConfig) -> Self {
        Self {
            workers: DashMap::new(),
            planner: DistributedPlanner::new(),
            scheduler: Arc::new(TaskScheduler::new()),
            config,
            results: DashMap::new(),
        }
    }

    /// Create with default configuration
    pub fn default_config() -> Self {
        Self::new(ClusterConfig::default())
    }

    /// Register a worker node
    pub fn register_worker(&self, address: &str) -> Result<WorkerId> {
        // Check if worker already registered
        for worker in self.workers.iter() {
            if worker.address == address {
                return Err(DistributedError::WorkerAlreadyRegistered(
                    address.to_string(),
                ));
            }
        }

        let worker_info = WorkerInfo::new(address);
        let worker_id = worker_info.id;
        self.workers.insert(worker_id, worker_info);

        tracing::info!("Registered worker {} at {}", worker_id, address);
        Ok(worker_id)
    }

    /// Unregister a worker node
    pub fn unregister_worker(&self, worker_id: WorkerId) -> Result<()> {
        if self.workers.remove(&worker_id).is_some() {
            tracing::info!("Unregistered worker {}", worker_id);
            Ok(())
        } else {
            Err(DistributedError::WorkerNotFound(worker_id.to_string()))
        }
    }

    /// Get worker info
    pub fn get_worker(&self, worker_id: WorkerId) -> Option<WorkerInfo> {
        self.workers.get(&worker_id).map(|r| r.clone())
    }

    /// List all workers
    pub fn list_workers(&self) -> Vec<WorkerInfo> {
        self.workers.iter().map(|r| r.clone()).collect()
    }

    /// Get cluster status
    pub fn cluster_status(&self) -> ClusterStatus {
        let workers: Vec<WorkerInfo> = self.workers.iter().map(|r| r.clone()).collect();
        let total_workers = workers.len();
        let active_workers = workers
            .iter()
            .filter(|w| w.status == WorkerStatus::Active)
            .count();
        let unhealthy_workers = workers
            .iter()
            .filter(|w| w.status == WorkerStatus::Unhealthy)
            .count();
        let total_tasks: usize = workers.iter().map(|w| w.active_tasks).sum();
        let total_capacity: usize = workers.iter().map(|w| w.max_tasks).sum();

        ClusterStatus {
            total_workers,
            active_workers,
            unhealthy_workers,
            total_tasks,
            total_capacity,
            workers,
        }
    }

    /// Update worker heartbeat
    pub fn worker_heartbeat(&self, worker_id: WorkerId) -> Result<()> {
        if let Some(mut worker) = self.workers.get_mut(&worker_id) {
            worker.update_heartbeat();
            if worker.status == WorkerStatus::Unhealthy {
                worker.status = WorkerStatus::Active;
            }
            Ok(())
        } else {
            Err(DistributedError::WorkerNotFound(worker_id.to_string()))
        }
    }

    /// Check for stale workers and mark as unhealthy
    pub fn check_worker_health(&self) {
        for mut worker in self.workers.iter_mut() {
            if worker.is_stale(self.config.heartbeat_timeout_ms) {
                if worker.status == WorkerStatus::Active {
                    tracing::warn!("Worker {} is now unhealthy", worker.id);
                    worker.status = WorkerStatus::Unhealthy;
                }
            }
        }
    }

    /// Execute a query in distributed mode
    pub async fn execute(&self, plan: &LogicalPlan) -> Result<Vec<RecordBatch>> {
        // Check if we have workers
        let active_workers = self.get_active_workers();
        if active_workers.is_empty() {
            return Err(DistributedError::NoWorkersAvailable);
        }

        // Create distributed plan
        let _distributed_plan = self.planner.plan(plan, active_workers.len())?;

        // Create query ID
        let query_id = QueryId::new();
        tracing::info!("Starting distributed query {}", query_id);

        // For now, return empty result - actual execution would:
        // 1. Serialize plan fragments
        // 2. Distribute to workers
        // 3. Collect and merge results

        // This is a placeholder for the actual distributed execution
        Ok(vec![])
    }

    /// Get list of active workers
    fn get_active_workers(&self) -> Vec<WorkerInfo> {
        self.workers
            .iter()
            .filter(|w| w.status == WorkerStatus::Active)
            .map(|w| w.clone())
            .collect()
    }

    /// Get scheduler reference
    pub fn scheduler(&self) -> Arc<TaskScheduler> {
        Arc::clone(&self.scheduler)
    }

    /// Get cluster config
    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    /// Get number of registered workers
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Get number of active workers
    pub fn active_worker_count(&self) -> usize {
        self.workers
            .iter()
            .filter(|w| w.status == WorkerStatus::Active)
            .count()
    }
}

impl Default for Coordinator {
    fn default() -> Self {
        Self::default_config()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_worker() {
        let coordinator = Coordinator::default();

        let worker_id = coordinator.register_worker("localhost:50051").unwrap();
        assert_eq!(coordinator.worker_count(), 1);

        let worker = coordinator.get_worker(worker_id);
        assert!(worker.is_some());
        assert_eq!(worker.unwrap().address, "localhost:50051");
    }

    #[test]
    fn test_register_duplicate_worker() {
        let coordinator = Coordinator::default();

        coordinator.register_worker("localhost:50051").unwrap();
        let result = coordinator.register_worker("localhost:50051");

        assert!(result.is_err());
    }

    #[test]
    fn test_unregister_worker() {
        let coordinator = Coordinator::default();

        let worker_id = coordinator.register_worker("localhost:50051").unwrap();
        assert_eq!(coordinator.worker_count(), 1);

        coordinator.unregister_worker(worker_id).unwrap();
        assert_eq!(coordinator.worker_count(), 0);
    }

    #[test]
    fn test_cluster_status() {
        let coordinator = Coordinator::default();

        coordinator.register_worker("worker1:50051").unwrap();
        coordinator.register_worker("worker2:50051").unwrap();

        let status = coordinator.cluster_status();
        assert_eq!(status.total_workers, 2);
        assert_eq!(status.active_workers, 2);
        assert_eq!(status.unhealthy_workers, 0);
    }

    #[test]
    fn test_worker_heartbeat() {
        let coordinator = Coordinator::default();

        let worker_id = coordinator.register_worker("localhost:50051").unwrap();

        // Heartbeat should succeed
        assert!(coordinator.worker_heartbeat(worker_id).is_ok());

        // Unknown worker should fail
        let unknown_id = WorkerId::new();
        assert!(coordinator.worker_heartbeat(unknown_id).is_err());
    }

    #[test]
    fn test_list_workers() {
        let coordinator = Coordinator::default();

        coordinator.register_worker("worker1:50051").unwrap();
        coordinator.register_worker("worker2:50051").unwrap();
        coordinator.register_worker("worker3:50051").unwrap();

        let workers = coordinator.list_workers();
        assert_eq!(workers.len(), 3);
    }
}
