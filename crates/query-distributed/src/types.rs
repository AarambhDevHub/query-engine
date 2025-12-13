//! Core types for distributed execution

use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Unique identifier for a worker node
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerId(pub Uuid);

impl WorkerId {
    /// Create a new random worker ID
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Create from a UUID
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl Default for WorkerId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "worker-{}", &self.0.to_string()[..8])
    }
}

/// Unique identifier for a query
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QueryId(pub Uuid);

impl QueryId {
    /// Create a new random query ID
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for QueryId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for QueryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "query-{}", &self.0.to_string()[..8])
    }
}

/// Unique identifier for a task (partition of a query)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskId(pub Uuid);

impl TaskId {
    /// Create a new random task ID
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for TaskId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "task-{}", &self.0.to_string()[..8])
    }
}

/// Status of a worker node
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkerStatus {
    /// Worker is healthy and accepting tasks
    Active,
    /// Worker is busy executing tasks
    Busy,
    /// Worker is not responding
    Unhealthy,
    /// Worker is shutting down
    Draining,
    /// Worker has been removed from cluster
    Removed,
}

impl fmt::Display for WorkerStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkerStatus::Active => write!(f, "Active"),
            WorkerStatus::Busy => write!(f, "Busy"),
            WorkerStatus::Unhealthy => write!(f, "Unhealthy"),
            WorkerStatus::Draining => write!(f, "Draining"),
            WorkerStatus::Removed => write!(f, "Removed"),
        }
    }
}

/// Information about a worker node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerInfo {
    /// Unique worker ID
    pub id: WorkerId,
    /// Network address (host:port)
    pub address: String,
    /// Current status
    pub status: WorkerStatus,
    /// Number of active tasks
    pub active_tasks: usize,
    /// Maximum concurrent tasks
    pub max_tasks: usize,
    /// Last heartbeat timestamp (Unix millis)
    pub last_heartbeat: u64,
}

impl WorkerInfo {
    /// Create new worker info
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            id: WorkerId::new(),
            address: address.into(),
            status: WorkerStatus::Active,
            active_tasks: 0,
            max_tasks: 4, // Default concurrent tasks
            last_heartbeat: Self::now_millis(),
        }
    }

    /// Check if worker can accept more tasks
    pub fn can_accept_task(&self) -> bool {
        self.status == WorkerStatus::Active && self.active_tasks < self.max_tasks
    }

    /// Get current time in milliseconds
    fn now_millis() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// Update heartbeat timestamp
    pub fn update_heartbeat(&mut self) {
        self.last_heartbeat = Self::now_millis();
    }

    /// Check if worker is stale (no heartbeat for given duration)
    pub fn is_stale(&self, timeout_ms: u64) -> bool {
        let now = Self::now_millis();
        now.saturating_sub(self.last_heartbeat) > timeout_ms
    }
}

/// Status of the distributed cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStatus {
    /// Total number of workers
    pub total_workers: usize,
    /// Number of active workers
    pub active_workers: usize,
    /// Number of unhealthy workers
    pub unhealthy_workers: usize,
    /// Total tasks across all workers
    pub total_tasks: usize,
    /// Cluster capacity (max tasks)
    pub total_capacity: usize,
    /// List of worker info
    pub workers: Vec<WorkerInfo>,
}

impl ClusterStatus {
    /// Create empty cluster status
    pub fn empty() -> Self {
        Self {
            total_workers: 0,
            active_workers: 0,
            unhealthy_workers: 0,
            total_tasks: 0,
            total_capacity: 0,
            workers: Vec::new(),
        }
    }

    /// Calculate cluster utilization (0.0 - 1.0)
    pub fn utilization(&self) -> f64 {
        if self.total_capacity == 0 {
            0.0
        } else {
            self.total_tasks as f64 / self.total_capacity as f64
        }
    }
}

/// Configuration for the distributed cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Heartbeat interval in milliseconds
    pub heartbeat_interval_ms: u64,
    /// Heartbeat timeout before marking worker unhealthy
    pub heartbeat_timeout_ms: u64,
    /// Maximum retries for failed tasks
    pub max_task_retries: usize,
    /// Default partition count
    pub default_partitions: usize,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: 5000, // 5 seconds
            heartbeat_timeout_ms: 15000, // 15 seconds
            max_task_retries: 3,
            default_partitions: 4,
        }
    }
}

/// A task to be executed on a worker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTask {
    /// Unique task ID
    pub id: TaskId,
    /// Parent query ID
    pub query_id: QueryId,
    /// Partition index
    pub partition_index: usize,
    /// Total partitions
    pub total_partitions: usize,
    /// Serialized plan fragment
    pub plan_fragment: Vec<u8>,
    /// Retry count
    pub retry_count: usize,
}

impl QueryTask {
    /// Create a new query task
    pub fn new(query_id: QueryId, partition_index: usize, total_partitions: usize) -> Self {
        Self {
            id: TaskId::new(),
            query_id,
            partition_index,
            total_partitions,
            plan_fragment: Vec::new(),
            retry_count: 0,
        }
    }
}

/// Status of a query task
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    /// Task is pending execution
    Pending,
    /// Task is running on a worker
    Running,
    /// Task completed successfully
    Completed,
    /// Task failed
    Failed,
    /// Task was cancelled
    Cancelled,
}

/// Result of a task execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResult {
    /// Task ID
    pub task_id: TaskId,
    /// Status
    pub status: TaskStatus,
    /// Number of rows returned
    pub row_count: usize,
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
    /// Error message if failed
    pub error: Option<String>,
}
