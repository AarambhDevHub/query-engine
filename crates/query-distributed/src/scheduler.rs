//! Task scheduler for distributed query execution

use crate::error::{DistributedError, Result};
use crate::types::{QueryTask, TaskId, TaskStatus, WorkerId, WorkerInfo};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::VecDeque;

/// Task scheduler for distributing work across workers
pub struct TaskScheduler {
    /// Pending tasks queue
    pending_tasks: RwLock<VecDeque<QueryTask>>,
    /// Running tasks: task_id -> (task, worker_id)
    running_tasks: DashMap<TaskId, (QueryTask, WorkerId)>,
    /// Completed tasks
    completed_tasks: DashMap<TaskId, TaskStatus>,
}

impl TaskScheduler {
    /// Create a new task scheduler
    pub fn new() -> Self {
        Self {
            pending_tasks: RwLock::new(VecDeque::new()),
            running_tasks: DashMap::new(),
            completed_tasks: DashMap::new(),
        }
    }

    /// Submit a task for scheduling
    pub fn submit(&self, task: QueryTask) {
        let mut pending = self.pending_tasks.write();
        pending.push_back(task);
    }

    /// Submit multiple tasks
    pub fn submit_all(&self, tasks: Vec<QueryTask>) {
        let mut pending = self.pending_tasks.write();
        for task in tasks {
            pending.push_back(task);
        }
    }

    /// Get next task for a worker
    pub fn get_next_task(&self, _worker_id: WorkerId) -> Option<QueryTask> {
        let mut pending = self.pending_tasks.write();
        pending.pop_front()
    }

    /// Schedule a task to a worker
    pub fn schedule(&self, task: QueryTask, worker_id: WorkerId) {
        self.running_tasks.insert(task.id, (task, worker_id));
    }

    /// Mark task as completed
    pub fn complete(&self, task_id: TaskId, status: TaskStatus) {
        self.running_tasks.remove(&task_id);
        self.completed_tasks.insert(task_id, status);
    }

    /// Get count of pending tasks
    pub fn pending_count(&self) -> usize {
        self.pending_tasks.read().len()
    }

    /// Get count of running tasks
    pub fn running_count(&self) -> usize {
        self.running_tasks.len()
    }

    /// Check if all tasks for a query are complete
    pub fn is_query_complete(&self, query_id: crate::types::QueryId) -> bool {
        // Check if any tasks for this query are still running or pending
        let has_running = self
            .running_tasks
            .iter()
            .any(|r| r.value().0.query_id == query_id);
        let has_pending = self
            .pending_tasks
            .read()
            .iter()
            .any(|t| t.query_id == query_id);
        !has_running && !has_pending
    }

    /// Get failed tasks for a query (for retry)
    pub fn get_failed_tasks(&self, _query_id: crate::types::QueryId) -> Vec<TaskId> {
        self.completed_tasks
            .iter()
            .filter(|r| *r.value() == TaskStatus::Failed)
            .map(|r| *r.key())
            .collect()
    }

    /// Reschedule failed tasks
    pub fn reschedule_failed(&self, task_id: TaskId) -> Result<()> {
        if let Some((_, (mut task, _))) = self.running_tasks.remove(&task_id) {
            task.retry_count += 1;
            self.submit(task);
            Ok(())
        } else {
            Err(DistributedError::TaskExecutionFailed(format!(
                "Task {} not found for rescheduling",
                task_id
            )))
        }
    }

    /// Clear all tasks
    pub fn clear(&self) {
        self.pending_tasks.write().clear();
        self.running_tasks.clear();
        self.completed_tasks.clear();
    }

    /// Choose best worker for a task based on load
    pub fn choose_worker(&self, workers: &[WorkerInfo]) -> Option<WorkerId> {
        // Find worker with least load that can accept tasks
        workers
            .iter()
            .filter(|w| w.can_accept_task())
            .min_by_key(|w| w.active_tasks)
            .map(|w| w.id)
    }
}

impl Default for TaskScheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::QueryId;

    #[test]
    fn test_submit_and_get_task() {
        let scheduler = TaskScheduler::new();
        let query_id = QueryId::new();
        let task = QueryTask::new(query_id, 0, 4);
        let task_id = task.id;

        scheduler.submit(task);
        assert_eq!(scheduler.pending_count(), 1);

        let worker_id = WorkerId::new();
        let retrieved = scheduler.get_next_task(worker_id);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().id, task_id);
        assert_eq!(scheduler.pending_count(), 0);
    }

    #[test]
    fn test_schedule_and_complete() {
        let scheduler = TaskScheduler::new();
        let query_id = QueryId::new();
        let task = QueryTask::new(query_id, 0, 4);
        let task_id = task.id;
        let worker_id = WorkerId::new();

        scheduler.schedule(task, worker_id);
        assert_eq!(scheduler.running_count(), 1);

        scheduler.complete(task_id, TaskStatus::Completed);
        assert_eq!(scheduler.running_count(), 0);
    }

    #[test]
    fn test_choose_worker() {
        let scheduler = TaskScheduler::new();

        let mut workers = vec![
            WorkerInfo::new("worker1:50051"),
            WorkerInfo::new("worker2:50051"),
            WorkerInfo::new("worker3:50051"),
        ];

        // Set different loads
        workers[0].active_tasks = 3;
        workers[1].active_tasks = 1;
        workers[2].active_tasks = 2;

        let chosen = scheduler.choose_worker(&workers);
        assert!(chosen.is_some());
        assert_eq!(chosen.unwrap(), workers[1].id); // worker2 has least load
    }

    #[test]
    fn test_submit_all() {
        let scheduler = TaskScheduler::new();
        let query_id = QueryId::new();

        let tasks: Vec<QueryTask> = (0..4).map(|i| QueryTask::new(query_id, i, 4)).collect();

        scheduler.submit_all(tasks);
        assert_eq!(scheduler.pending_count(), 4);
    }
}
