//! Distributed execution for Query Engine
//!
//! This crate provides distributed query execution capabilities, enabling
//! queries to be executed across multiple worker nodes.
//!
//! # Architecture
//!
//! The distributed system consists of:
//! - **Coordinator**: Central node that plans queries and coordinates workers
//! - **Workers**: Execute query partitions and return results
//! - **Scheduler**: Distributes tasks across workers
//! - **Exchange/Merge**: Shuffle data between stages
//! - **FaultManager**: Handle failures and recovery
//!
//! # Example
//!
//! ```ignore
//! use query_distributed::{Coordinator, Worker, DistributedExecutor, ClusterConfig};
//! use std::sync::Arc;
//!
//! // Start coordinator
//! let coordinator = Arc::new(Coordinator::new(ClusterConfig::default()));
//!
//! // Register workers
//! coordinator.register_worker("worker1:50051")?;
//! coordinator.register_worker("worker2:50051")?;
//!
//! // Create executor
//! let executor = DistributedExecutor::new(coordinator);
//!
//! // Execute distributed query
//! let results = executor.execute(&plan).await?;
//! ```
//!
//! # Modules
//!
//! - [`coordinator`]: Coordinator node management
//! - [`worker`]: Worker node implementation
//! - [`scheduler`]: Task scheduling
//! - [`partition`]: Data partitioning strategies
//! - [`planner`]: Distributed query planning
//! - [`operators`]: Exchange and merge operators
//! - [`executor`]: Distributed execution orchestration
//! - [`fault`]: Fault tolerance and recovery
//! - [`network`]: Network communication types
//! - [`flight_transport`]: Arrow Flight transport layer

pub mod coordinator;
pub mod error;
pub mod executor;
pub mod fault;
pub mod flight_transport;
pub mod network;
pub mod operators;
pub mod partition;
pub mod planner;
pub mod scheduler;
pub mod types;
pub mod worker;

// Re-exports
pub use coordinator::Coordinator;
pub use error::DistributedError;
pub use executor::{DistributedExecutor, ExecutorConfig, QueryExecution, QueryExecutionStatus};
pub use fault::{FaultConfig, FaultManager, FaultStats, TaskRecoveryAction, WorkerRecoveryAction};
pub use flight_transport::{DistributedTransport, FlightTransport};
pub use network::{
    ClusterTopology, CoordinatorMessage, NetworkConfig, SerializedBatch, TaskExecutionRequest,
    TaskExecutionResponse, WorkerMessage,
};
pub use operators::{Exchange, Merge, MergeStrategy, ResultCollector, SortColumn};
pub use partition::{Partition, PartitionStrategy, Partitioner, RangeBoundary};
pub use planner::{DistributedPlan, DistributedPlanner, ExchangePoint, ExchangeReason, QueryStage};
pub use scheduler::TaskScheduler;
pub use types::*;
pub use worker::Worker;
