//! Distributed query execution example
//!
//! This example demonstrates a simple distributed execution scenario
//! with a coordinator and simulated workers.

use query_distributed::{
    Coordinator, DistributedExecutor, ExecutorConfig, FaultManager, PartitionStrategy, Partitioner,
    Worker, WorkerStatus,
};
use std::sync::Arc;

fn main() {
    println!("=== Distributed Query Engine Demo ===\n");

    // Create a coordinator
    println!("1. Creating Coordinator...");
    let coordinator = Arc::new(Coordinator::default());
    println!("   Coordinator created with default configuration\n");

    // Register workers
    println!("2. Registering Workers...");
    let worker1_id = coordinator.register_worker("worker1:50051").unwrap();
    let worker2_id = coordinator.register_worker("worker2:50051").unwrap();
    let worker3_id = coordinator.register_worker("worker3:50051").unwrap();
    println!("   Registered 3 workers: worker1, worker2, worker3\n");

    // Check cluster status
    println!("3. Cluster Status:");
    let status = coordinator.cluster_status();
    println!("   Total workers: {}", status.total_workers);
    println!("   Active workers: {}", status.active_workers);
    println!("   Total capacity: {} tasks\n", status.total_capacity);

    // Create distributed executor
    println!("4. Creating DistributedExecutor...");
    let config = ExecutorConfig::default();
    let executor = DistributedExecutor::with_config(Arc::clone(&coordinator), config);
    println!("   Executor created with config:");
    println!("   - Max concurrent queries: 10");
    println!("   - Task timeout: 5 minutes\n");

    // Demonstrate partitioning
    println!("5. Partitioning Demonstration:");

    // Hash partitioning
    let hash_partitioner = Partitioner::hash(vec!["customer_id".to_string()], 4);
    println!(
        "   Hash partitioner: {} partitions on 'customer_id'",
        hash_partitioner.num_partitions()
    );

    // Round-robin partitioning
    let rr_partitioner = Partitioner::round_robin(3);
    println!(
        "   Round-robin partitioner: {} partitions",
        rr_partitioner.num_partitions()
    );

    // Range partitioning
    use query_distributed::RangeBoundary;
    let range_strategy = PartitionStrategy::Range {
        key_column: "date".to_string(),
        boundaries: vec![RangeBoundary::Int64(1000), RangeBoundary::Int64(2000)],
    };
    let range_partitioner = Partitioner::new(range_strategy);
    println!(
        "   Range partitioner: {} partitions on 'date'\n",
        range_partitioner.num_partitions()
    );

    // Demonstrate fault tolerance
    println!("6. Fault Tolerance:");
    let fault_manager = FaultManager::with_defaults();
    println!("   FaultManager configured with:");
    println!("   - Max task retries: 3");
    println!("   - Worker failure threshold: 3");
    println!("   - Checkpointing: enabled\n");

    // Simulate worker heartbeats
    println!("7. Simulating Worker Heartbeats...");
    coordinator.worker_heartbeat(worker1_id).unwrap();
    coordinator.worker_heartbeat(worker2_id).unwrap();
    coordinator.worker_heartbeat(worker3_id).unwrap();
    println!("   All workers reported healthy\n");

    // Check health
    coordinator.check_worker_health();
    let status = coordinator.cluster_status();
    println!("8. Health Check Results:");
    println!("   Active workers: {}", status.active_workers);
    println!("   Unhealthy workers: {}\n", status.unhealthy_workers);

    // Create local workers (simulation)
    println!("9. Creating Local Worker Instances...");
    let local_worker1 = Worker::new("localhost:50051");
    let local_worker2 = Worker::with_coordinator("localhost:50052", "coordinator:50050");
    println!(
        "   Worker {} at {}",
        local_worker1.id(),
        local_worker1.address()
    );
    println!(
        "   Worker {} at {} (with coordinator)",
        local_worker2.id(),
        local_worker2.address()
    );
    println!(
        "   Worker 1 can accept tasks: {}\n",
        local_worker1.can_accept_task()
    );

    // List all workers
    println!("10. Registered Workers:");
    for worker in coordinator.list_workers() {
        println!(
            "    - {} at {} (status: {:?}, tasks: {}/{})",
            worker.id, worker.address, worker.status, worker.active_tasks, worker.max_tasks
        );
    }
    println!();

    // Cleanup
    println!("11. Cleanup...");
    coordinator.unregister_worker(worker1_id).unwrap();
    coordinator.unregister_worker(worker2_id).unwrap();
    coordinator.unregister_worker(worker3_id).unwrap();
    println!("    All workers unregistered\n");

    let final_status = coordinator.cluster_status();
    println!("12. Final Status:");
    println!("    Total workers: {}", final_status.total_workers);
    println!("    Active queries: {}", executor.active_query_count());

    println!("\n=== Demo Complete ===");
}
