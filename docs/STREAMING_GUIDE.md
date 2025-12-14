# Streaming Query Guide

Real-time streaming query processing for continuous data streams.

## Features

- **Stream Sources**: Async streams of RecordBatch data
- **Windowing**: Tumbling, Sliding, and Session windows
- **Watermarks**: Event-time processing with late event handling
- **Statistics**: Track batches, rows, and processing metrics

## Quick Start

```rust
use query_streaming::{StreamingQuery, StreamConfig, ChannelStreamSource};
use tokio::sync::mpsc;

// Create a channel-based stream source
let (tx, source) = ChannelStreamSource::new(100);
let config = StreamConfig::default();
let mut query = StreamingQuery::new(source, config);

// Send data to the stream
tx.send(record_batch).await?;

// Process stream
while let Some(result) = query.next().await {
    let batch = result?;
    process_batch(batch);
}
```

## Stream Sources

### ChannelStreamSource
For receiving data via Tokio channels:
```rust
let (tx, source) = ChannelStreamSource::new(buffer_size);
```

### MemoryStreamSource
For testing with pre-loaded data:
```rust
let source = MemoryStreamSource::new(vec![batch1, batch2]);
```

## Configuration

```rust
let config = StreamConfig::default()
    .with_batch_size(1000)
    .with_window(WindowType::tumbling(Duration::from_secs(60)))
    .with_watermark_interval(Duration::from_secs(1))
    .with_max_lateness(Duration::from_secs(30));
```

| Option | Default | Description |
|--------|---------|-------------|
| `batch_size` | 1000 | Max rows to accumulate |
| `window` | None | Window type |
| `watermark_interval` | 1s | How often to advance watermark |
| `max_lateness` | 60s | Max allowed late events |

## Window Types

### Tumbling Window
Fixed-size, non-overlapping windows:
```rust
WindowType::tumbling(Duration::from_secs(60))
```

### Sliding Window
Overlapping windows with size and slide:
```rust
WindowType::sliding(
    Duration::from_secs(60),  // window size
    Duration::from_secs(10)   // slide interval
)
```

### Session Window
Gap-based grouping:
```rust
WindowType::session(Duration::from_secs(30))
```

## Watermarks

Track event-time progress:
```rust
let watermark = Watermark::new();
watermark.advance(timestamp_ms);

// Check for late events
if watermark.is_late(event_timestamp) {
    // Handle late event
}
```

### Late Event Policies
```rust
LateEventPolicy::Drop           // Discard late events
LateEventPolicy::SideOutput     // Route to side output
LateEventPolicy::Allow { max_lateness_ms: 1000 }
```

## Stream Control

```rust
query.pause();   // Pause processing
query.resume();  // Resume processing
query.stop();    // Stop stream

// Check status
match query.status() {
    StreamStatus::Running => { }
    StreamStatus::Paused => { }
    StreamStatus::Completed => { }
    StreamStatus::Failed(err) => { }
}

// Get statistics
let stats = query.stats();
println!("Processed: {} batches, {} rows", 
    stats.batches_processed, 
    stats.rows_processed);
```
