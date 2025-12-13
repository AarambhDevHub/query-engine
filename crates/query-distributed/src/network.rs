//! Network communication for distributed execution
//!
//! Provides message types and traits for coordinator-worker communication.

use crate::error::{DistributedError, Result};
use crate::types::{QueryTask, TaskId, TaskResult, TaskStatus, WorkerId, WorkerInfo};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

/// Messages from coordinator to worker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CoordinatorMessage {
    /// Execute a query task
    ExecuteTask(QueryTask),
    /// Cancel a running task
    CancelTask(TaskId),
    /// Request worker status
    StatusRequest,
    /// Shutdown worker gracefully
    Shutdown,
    /// Ping for health check
    Ping,
}

/// Messages from worker to coordinator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkerMessage {
    /// Worker registration request
    Register { address: String },
    /// Task execution result
    TaskComplete(TaskResult),
    /// Task progress update
    TaskProgress { task_id: TaskId, progress: f32 },
    /// Worker status response
    StatusResponse { info: WorkerInfo },
    /// Heartbeat
    Heartbeat { worker_id: WorkerId },
    /// Pong response to ping
    Pong,
}

/// Serialized record batch for network transfer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedBatch {
    /// IPC-encoded Arrow data
    pub data: Vec<u8>,
    /// Number of rows
    pub num_rows: usize,
}

impl SerializedBatch {
    /// Serialize a record batch using Arrow IPC format
    pub fn from_batch(batch: &RecordBatch) -> Result<Self> {
        let mut buffer = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())
                .map_err(|e| DistributedError::SerializationError(e.to_string()))?;
            writer
                .write(batch)
                .map_err(|e| DistributedError::SerializationError(e.to_string()))?;
            writer
                .finish()
                .map_err(|e| DistributedError::SerializationError(e.to_string()))?;
        }

        Ok(Self {
            data: buffer,
            num_rows: batch.num_rows(),
        })
    }

    /// Deserialize to a record batch
    pub fn to_batch(&self) -> Result<RecordBatch> {
        let cursor = Cursor::new(&self.data);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|e| DistributedError::SerializationError(e.to_string()))?;

        // Read first (and only) batch
        for batch_result in reader {
            let batch =
                batch_result.map_err(|e| DistributedError::SerializationError(e.to_string()))?;
            return Ok(batch);
        }

        Err(DistributedError::SerializationError(
            "No batch found in serialized data".to_string(),
        ))
    }

    /// Serialize multiple batches
    pub fn from_batches(batches: &[RecordBatch]) -> Result<Vec<Self>> {
        batches.iter().map(Self::from_batch).collect()
    }

    /// Deserialize multiple batches
    pub fn to_batches(serialized: &[Self]) -> Result<Vec<RecordBatch>> {
        serialized.iter().map(|s| s.to_batch()).collect()
    }
}

/// Task execution request with serialized data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskExecutionRequest {
    /// Task metadata
    pub task: QueryTask,
    /// Input data (serialized batches)
    pub input_batches: Vec<SerializedBatch>,
}

/// Task execution response with serialized results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskExecutionResponse {
    /// Task ID
    pub task_id: TaskId,
    /// Execution status
    pub status: TaskStatus,
    /// Result batches (if successful)
    pub result_batches: Vec<SerializedBatch>,
    /// Error message (if failed)
    pub error: Option<String>,
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
}

impl TaskExecutionResponse {
    /// Create a success response
    pub fn success(task_id: TaskId, batches: Vec<RecordBatch>, time_ms: u64) -> Result<Self> {
        Ok(Self {
            task_id,
            status: TaskStatus::Completed,
            result_batches: SerializedBatch::from_batches(&batches)?,
            error: None,
            execution_time_ms: time_ms,
        })
    }

    /// Create a failure response
    pub fn failure(task_id: TaskId, error: String, time_ms: u64) -> Self {
        Self {
            task_id,
            status: TaskStatus::Failed,
            result_batches: Vec::new(),
            error: Some(error),
            execution_time_ms: time_ms,
        }
    }

    /// Get result batches (deserialize)
    pub fn get_batches(&self) -> Result<Vec<RecordBatch>> {
        SerializedBatch::to_batches(&self.result_batches)
    }
}

/// Cluster topology information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterTopology {
    /// Coordinator address
    pub coordinator_address: String,
    /// List of worker addresses
    pub worker_addresses: Vec<String>,
    /// Number of partitions
    pub num_partitions: usize,
}

/// Network configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// Connection timeout in milliseconds
    pub connect_timeout_ms: u64,
    /// Request timeout in milliseconds
    pub request_timeout_ms: u64,
    /// Maximum message size in bytes
    pub max_message_size: usize,
    /// Retry count for failed requests
    pub retry_count: usize,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            connect_timeout_ms: 5000,
            request_timeout_ms: 30000,
            max_message_size: 64 * 1024 * 1024, // 64 MB
            retry_count: 3,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_serialize_deserialize_batch() {
        let batch = create_test_batch();
        let serialized = SerializedBatch::from_batch(&batch).unwrap();
        assert_eq!(serialized.num_rows, 3);

        let deserialized = serialized.to_batch().unwrap();
        assert_eq!(deserialized.num_rows(), 3);
        assert_eq!(deserialized.num_columns(), 2);
    }

    #[test]
    fn test_serialize_multiple_batches() {
        let batches = vec![create_test_batch(), create_test_batch()];
        let serialized = SerializedBatch::from_batches(&batches).unwrap();
        assert_eq!(serialized.len(), 2);

        let deserialized = SerializedBatch::to_batches(&serialized).unwrap();
        assert_eq!(deserialized.len(), 2);
    }

    #[test]
    fn test_task_execution_response_success() {
        let batches = vec![create_test_batch()];
        let task_id = TaskId::new();

        let response = TaskExecutionResponse::success(task_id, batches, 100).unwrap();
        assert_eq!(response.status, TaskStatus::Completed);
        assert!(response.error.is_none());

        let result_batches = response.get_batches().unwrap();
        assert_eq!(result_batches.len(), 1);
    }

    #[test]
    fn test_task_execution_response_failure() {
        let task_id = TaskId::new();
        let response = TaskExecutionResponse::failure(task_id, "Test error".into(), 50);

        assert_eq!(response.status, TaskStatus::Failed);
        assert_eq!(response.error, Some("Test error".into()));
    }
}
