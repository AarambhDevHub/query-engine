# Distributed Execution Guide

Guide to distributed query execution in Query Engine.

## Overview

The distributed execution framework enables queries to be partitioned and executed across multiple worker nodes, with fault tolerance and automatic recovery.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Coordinator                              │
│                                                                   │
│  ┌──────────────────┐  ┌──────────────────┐  ┌────────────────┐ │
│  │ DistributedPlanner│  │  TaskScheduler   │  │  FaultManager  │ │
│  │  - Plan analysis  │  │  - Load balance  │  │  - Retries     │ │
│  │  - Stage creation │  │  - Task queue    │  │  - Checkpoints │ │
│  └──────────────────┘  └──────────────────┘  └────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                              │
            ┌─────────────────┼─────────────────┐
            ▼                 ▼                 ▼
    ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
    │   Worker 1   │  │   Worker 2   │  │   Worker 3   │
    │ ┌──────────┐ │  │ ┌──────────┐ │  │ ┌──────────┐ │
    │ │ Executor │ │  │ │ Executor │ │  │ │ Executor │ │
    │ └──────────┘ │  │ └──────────┘ │  │ └──────────┘ │
    │ Partition 0  │  │ Partition 1  │  │ Partition 2  │
    └──────────────┘  └──────────────┘  └──────────────┘
```

## Quick Start

```rust
use query_distributed::{
    Coordinator, DistributedExecutor, Worker, 
    Partitioner, PartitionStrategy
};
use std::sync::Arc;

// 1. Create coordinator
let coordinator = Arc::new(Coordinator::default());

// 2. Register workers
coordinator.register_worker("worker1:50051")?;
coordinator.register_worker("worker2:50051")?;
coordinator.register_worker("worker3:50051")?;

// 3. Check cluster status
let status = coordinator.cluster_status();
println!("Workers: {}", status.active_workers);
println!("Capacity: {} tasks", status.total_capacity);

// 4. Create executor
let executor = DistributedExecutor::new(Arc::clone(&coordinator));

// 5. Execute query
let results = executor.execute(&logical_plan).await?;
```

## Components

### Coordinator

Central node that orchestrates distributed execution:

```rust
let coordinator = Coordinator::new(ClusterConfig {
    heartbeat_interval_ms: 5000,
    heartbeat_timeout_ms: 15000,
    max_task_retries: 3,
    default_partitions: 4,
});

// Register workers
let worker_id = coordinator.register_worker("host:port")?;

// Monitor health
coordinator.worker_heartbeat(worker_id)?;
coordinator.check_worker_health();

// Get status
let status = coordinator.cluster_status();

// Unregister
coordinator.unregister_worker(worker_id)?;
```

### Worker

Executes query partitions:

```rust
// Create worker
let worker = Worker::new("localhost:50051");

// Or with coordinator
let worker = Worker::with_coordinator(
    "localhost:50051",
    "coordinator:50050"
);

// Check status
println!("Worker ID: {}", worker.id());
println!("Can accept: {}", worker.can_accept_task());

// Execute task
let result = worker.execute_task(task).await?;
```

### Distributed Executor

Orchestrates multi-stage query execution:

```rust
let config = ExecutorConfig {
    max_concurrent_queries: 10,
    task_timeout_ms: 300000, // 5 minutes
    batch_size: 8192,
    adaptive_execution: false,
};

let executor = DistributedExecutor::with_config(
    Arc::clone(&coordinator),
    config
);

// Execute query
let results = executor.execute(&plan).await?;

// Check status
let count = executor.active_query_count();

// Cancel query
executor.cancel_query(query_id)?;
```

## Partitioning

### Hash Partitioning

Distributes data by hashing key column(s):

```rust
let partitioner = Partitioner::hash(
    vec!["customer_id".to_string()],
    4  // num partitions
);

let partitions = partitioner.partition(&record_batches)?;
```

**Best for:** Joins, aggregations by key

### Range Partitioning

Distributes data by value ranges:

```rust
use query_distributed::RangeBoundary;

let strategy = PartitionStrategy::Range {
    key_column: "date".to_string(),
    boundaries: vec![
        RangeBoundary::Int64(1000),
        RangeBoundary::Int64(2000),
    ],
};

let partitioner = Partitioner::new(strategy);
// Creates 3 partitions: [<1000, 1000-2000, >2000]
```

**Best for:** Sorted data, range queries

### Round-Robin Partitioning

Evenly distributes batches:

```rust
let partitioner = Partitioner::round_robin(3);
let partitions = partitioner.partition(&record_batches)?;
```

**Best for:** Even load distribution

## Operators

### Exchange

Shuffles data between stages:

```rust
use query_distributed::Exchange;

// Hash exchange for shuffle
let exchange = Exchange::hash(vec!["key".into()], 4);
let partitions = exchange.execute(&batches)?;

// Gather to single partition
let exchange = Exchange::gather();
let single = exchange.execute(&batches)?;
```

### Merge

Combines partition results:

```rust
use query_distributed::{Merge, SortColumn};

// Simple concatenation
let merge = Merge::concat(schema);
let result = merge.execute(partitions)?;

// Sorted merge
let merge = Merge::sorted(schema, vec![
    SortColumn {
        name: "timestamp".into(),
        ascending: true,
        nulls_first: false,
    }
]);
let sorted_result = merge.execute(partitions)?;
```

## Fault Tolerance

### FaultManager

Handles failures and recovery:

```rust
use query_distributed::{FaultManager, FaultConfig};

let config = FaultConfig {
    max_task_retries: 3,
    retry_delay_ms: 1000,
    worker_failure_threshold: 3,
    checkpoint_interval_ms: 30000,
    enable_checkpoints: true,
};

let fault_manager = FaultManager::new(config);
```

### Task Failure Handling

```rust
// Handle task failure
let action = fault_manager.handle_task_failure(
    task, worker_id, "Connection lost".into()
);

match action {
    TaskRecoveryAction::Retry { delay_ms } => {
        // Wait and retry
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        let retry_task = fault_manager.get_retry_task();
    }
    TaskRecoveryAction::Fail { reason } => {
        // Abort query
        eprintln!("Query failed: {}", reason);
    }
}
```

### Worker Failure Handling

```rust
let action = fault_manager.handle_worker_failure(worker_id);

match action {
    WorkerRecoveryAction::MarkUnhealthy { worker_id } => {
        // Mark worker, redistribute tasks
    }
    WorkerRecoveryAction::Remove { worker_id, reason } => {
        // Remove from cluster
        coordinator.unregister_worker(worker_id)?;
    }
}
```

### Checkpointing

```rust
// Create checkpoint
fault_manager.create_checkpoint(
    query_id,
    vec![0, 1, 2],  // completed stages
    intermediate_data
);

// Recover from checkpoint
if let Some(plan) = fault_manager.recover_from_checkpoint(query_id) {
    println!("Resume from stage {}", plan.resume_from_stage);
}
```

## Query Stages

Distributed plans are split into stages:

```rust
use query_distributed::{QueryStage, SerializedPlan};

let stage = QueryStage::new(
    0,  // stage id
    SerializedPlan::from_description("TableScan: orders"),
    4   // num partitions
)
.with_partition_strategy(PartitionStrategy::Hash {
    key_columns: vec!["customer_id".into()],
    num_partitions: 4,
})
.with_dependency(1)  // depends on stage 1
.with_shuffle();     // requires data exchange
```

## Network Communication

### Message Types

```rust
use query_distributed::{CoordinatorMessage, WorkerMessage};

// Coordinator → Worker
let msg = CoordinatorMessage::ExecuteTask(task);
let msg = CoordinatorMessage::CancelTask(task_id);
let msg = CoordinatorMessage::Ping;

// Worker → Coordinator
let msg = WorkerMessage::TaskComplete(result);
let msg = WorkerMessage::Heartbeat { worker_id };
let msg = WorkerMessage::TaskProgress { task_id, progress: 0.5 };
```

### Serialization

Arrow IPC format for efficient data transfer:

```rust
use query_distributed::SerializedBatch;

// Serialize
let serialized = SerializedBatch::from_batch(&record_batch)?;

// Deserialize
let batch = serialized.to_batch()?;

// Multiple batches
let serialized = SerializedBatch::from_batches(&batches)?;
let batches = SerializedBatch::to_batches(&serialized)?;
```

## Example: Distributed Aggregation

```sql
SELECT customer_id, SUM(amount)
FROM orders
GROUP BY customer_id
```

**Execution:**

```
Stage 0: TableScan (parallel)
    ├── Worker 1: Partition 0
    ├── Worker 2: Partition 1
    └── Worker 3: Partition 2
            │
            ▼ (shuffle by customer_id)
            
Stage 1: Partial Aggregate (parallel)
    ├── Worker 1: Sum for customer_ids hash % 3 == 0
    ├── Worker 2: Sum for customer_ids hash % 3 == 1
    └── Worker 3: Sum for customer_ids hash % 3 == 2
            │
            ▼ (gather)
            
Stage 2: Final Aggregate (coordinator)
    └── Merge results
```

## Monitoring

```rust
// Cluster status
let status = coordinator.cluster_status();
println!("Workers: {}/{}", status.active_workers, status.total_workers);
println!("Utilization: {:.1}%", status.utilization() * 100.0);

// Fault stats
let stats = fault_manager.get_stats();
println!("Failed tasks: {}", stats.total_failed_tasks);
println!("Pending retries: {}", stats.pending_retries);

// Query status
let status = executor.get_query_status(query_id);
```
