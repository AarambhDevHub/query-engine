//! Real-time Streaming Query Example
//!
//! Demonstrates streaming query processing with windows and watermarks.

use anyhow::Result;
use arrow::array::{Int64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use query_streaming::{
    ChannelStreamSource, MemoryStreamSource, StreamConfig, StreamingQuery, WindowType,
};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Real-time Streaming Query Example ===\n");

    // Example 1: Basic Memory Stream
    basic_memory_stream().await?;

    // Example 2: Channel-based Stream
    channel_stream().await?;

    // Example 3: Windowed Stream
    windowed_stream().await?;

    println!("\n=== All streaming examples completed! ===");
    Ok(())
}

/// Example 1: Simple memory-based stream processing
async fn basic_memory_stream() -> Result<()> {
    println!("--- Example 1: Basic Memory Stream ---\n");

    // Create sample data
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )?;

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![4, 5])),
            Arc::new(StringArray::from(vec!["David", "Eve"])),
            Arc::new(Int64Array::from(vec![400, 500])),
        ],
    )?;

    // Create memory stream source
    let source = MemoryStreamSource::new(vec![batch1, batch2]);
    let config = StreamConfig::default();
    let mut query = StreamingQuery::new(source, config);

    println!("Processing stream...");

    // Process all batches
    let mut total_rows = 0;
    while let Some(result) = query.next().await {
        let batch = result?;
        total_rows += batch.num_rows();
        println!("  Received batch with {} rows", batch.num_rows());
    }

    let stats = query.stats();
    println!("\nStream completed!");
    println!("  Status: {:?}", query.status());
    println!("  Total batches: {}", stats.batches_processed);
    println!("  Total rows: {}", total_rows);

    Ok(())
}

/// Example 2: Channel-based streaming with async producer
async fn channel_stream() -> Result<()> {
    println!("\n--- Example 2: Channel-based Stream ---\n");

    let schema = Arc::new(Schema::new(vec![
        Field::new("sensor_id", DataType::Int64, false),
        Field::new("reading", DataType::Int64, false),
    ]));

    // Create channel stream
    let (tx, source) = ChannelStreamSource::new(100);
    let config = StreamConfig::default();
    let mut query = StreamingQuery::new(source, config);

    // Spawn producer task
    let schema_clone = schema.clone();
    tokio::spawn(async move {
        for i in 0..3 {
            let batch = RecordBatch::try_new(
                schema_clone.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![i, i + 10])),
                    Arc::new(Int64Array::from(vec![i * 100, i * 100 + 50])),
                ],
            )
            .unwrap();

            if tx.send(batch).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Drop sender to close stream
    });

    // Consume stream
    println!("Consuming channel stream...");
    while let Some(result) = query.next().await {
        let batch = result?;
        println!("  Received {} rows from channel", batch.num_rows());
    }

    println!("Channel stream completed: {:?}", query.status());
    Ok(())
}

/// Example 3: Windowed stream processing
async fn windowed_stream() -> Result<()> {
    println!("\n--- Example 3: Windowed Stream ---\n");

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("value", DataType::Int64, false),
    ]));

    // Use current time in millis
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![
                now,
                now + 1000,
                now + 2000,
            ])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
        ],
    )?;

    // Create windowed stream with tumbling window
    let source = MemoryStreamSource::new(vec![batch]);
    let config = StreamConfig::default()
        .with_window(WindowType::tumbling(Duration::from_secs(5)))
        .with_batch_size(100);

    let mut query = StreamingQuery::new(source, config);

    println!("Processing with tumbling window (5s)...");

    while let Some(result) = query.next().await {
        let batch = result?;
        println!("  Window output: {} rows", batch.num_rows());
    }

    // Demonstrate pause/resume
    let source2 = MemoryStreamSource::new(vec![]);
    let mut query2 = StreamingQuery::new(source2, StreamConfig::default());

    query2.pause();
    println!("\nStream control:");
    println!("  After pause: {:?}", query2.status());

    query2.resume();
    println!("  After resume: {:?}", query2.status());

    query2.stop();
    println!("  After stop: {:?}", query2.status());

    Ok(())
}
