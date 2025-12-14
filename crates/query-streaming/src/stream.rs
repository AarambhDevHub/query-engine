//! Streaming query processor
//!
//! Provides the main streaming query processing logic.

use crate::source::StreamSource;
use crate::watermark::Watermark;
use crate::window::{Window, WindowType};
use arrow::record_batch::RecordBatch;
use query_core::Result;
use std::time::Duration;

/// Configuration for streaming queries
#[derive(Debug, Clone)]
pub struct StreamConfig {
    /// Maximum batch size to accumulate before processing
    pub batch_size: usize,
    /// Window configuration
    pub window: Option<WindowType>,
    /// Watermark interval for event-time processing
    pub watermark_interval: Duration,
    /// Maximum allowed lateness for events
    pub max_lateness: Duration,
    /// Enable checkpointing
    pub checkpointing: bool,
    /// Checkpoint interval
    pub checkpoint_interval: Duration,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            window: None,
            watermark_interval: Duration::from_secs(1),
            max_lateness: Duration::from_secs(60),
            checkpointing: false,
            checkpoint_interval: Duration::from_secs(30),
        }
    }
}

impl StreamConfig {
    /// Create a new stream configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the batch size
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set the window type
    pub fn with_window(mut self, window: WindowType) -> Self {
        self.window = Some(window);
        self
    }

    /// Set the watermark interval
    pub fn with_watermark_interval(mut self, interval: Duration) -> Self {
        self.watermark_interval = interval;
        self
    }

    /// Set the maximum lateness
    pub fn with_max_lateness(mut self, lateness: Duration) -> Self {
        self.max_lateness = lateness;
        self
    }

    /// Enable checkpointing
    pub fn with_checkpointing(mut self, enabled: bool) -> Self {
        self.checkpointing = enabled;
        self
    }
}

/// Status of a streaming query
#[derive(Debug, Clone, PartialEq)]
pub enum StreamStatus {
    /// Stream is running
    Running,
    /// Stream is paused
    Paused,
    /// Stream has completed
    Completed,
    /// Stream encountered an error
    Failed(String),
}

/// Statistics for a streaming query
#[derive(Debug, Clone, Default)]
pub struct StreamStats {
    /// Total batches processed
    pub batches_processed: u64,
    /// Total rows processed
    pub rows_processed: u64,
    /// Late events dropped
    pub late_events_dropped: u64,
    /// Current watermark timestamp (in millis since epoch)
    pub current_watermark_ms: i64,
    /// Processing latency in milliseconds
    pub avg_latency_ms: f64,
}

/// A streaming query processor
pub struct StreamingQuery<S: StreamSource> {
    /// The stream source
    source: S,
    /// Stream configuration
    config: StreamConfig,
    /// Current watermark
    watermark: Watermark,
    /// Active window (if windowed)
    window: Option<Box<dyn Window>>,
    /// Current status
    status: StreamStatus,
    /// Statistics
    stats: StreamStats,
    /// Accumulated batches for windowing
    buffer: Vec<RecordBatch>,
}

impl<S: StreamSource> StreamingQuery<S> {
    /// Create a new streaming query
    pub fn new(source: S, config: StreamConfig) -> Self {
        let window: Option<Box<dyn Window>> = config.window.as_ref().map(|w| w.create_window());

        Self {
            source,
            config,
            watermark: Watermark::new(),
            window,
            status: StreamStatus::Running,
            stats: StreamStats::default(),
            buffer: Vec::new(),
        }
    }

    /// Get the next processed batch from the stream
    pub async fn next(&mut self) -> Option<Result<RecordBatch>> {
        if self.status != StreamStatus::Running {
            return None;
        }

        match self.source.next_batch().await {
            Some(Ok(batch)) => {
                self.stats.batches_processed += 1;
                self.stats.rows_processed += batch.num_rows() as u64;

                // If windowed, accumulate in buffer
                if self.window.is_some() {
                    self.buffer.push(batch.clone());

                    // Check if window should trigger
                    let should_trigger = self
                        .window
                        .as_ref()
                        .map(|w| w.should_trigger())
                        .unwrap_or(false);

                    if should_trigger {
                        // Process window - clear buffer first
                        let result_batch = if !self.buffer.is_empty() {
                            let b = self.buffer[0].clone();
                            self.buffer.clear();
                            Ok(b)
                        } else {
                            Err(query_core::QueryError::ExecutionError(
                                "No data in window".to_string(),
                            ))
                        };

                        // Advance window
                        if let Some(ref mut window) = self.window {
                            window.advance();
                        }
                        return Some(result_batch);
                    }
                }

                Some(Ok(batch))
            }
            Some(Err(e)) => {
                self.status = StreamStatus::Failed(e.to_string());
                Some(Err(e))
            }
            None => {
                self.status = StreamStatus::Completed;

                // Flush remaining window data
                if !self.buffer.is_empty() && self.window.is_some() {
                    let batch = self.buffer[0].clone();
                    self.buffer.clear();
                    return Some(Ok(batch));
                }

                None
            }
        }
    }

    /// Get current stream status
    pub fn status(&self) -> &StreamStatus {
        &self.status
    }

    /// Get stream statistics
    pub fn stats(&self) -> &StreamStats {
        &self.stats
    }

    /// Pause the stream
    pub fn pause(&mut self) {
        if self.status == StreamStatus::Running {
            self.status = StreamStatus::Paused;
        }
    }

    /// Resume the stream
    pub fn resume(&mut self) {
        if self.status == StreamStatus::Paused {
            self.status = StreamStatus::Running;
        }
    }

    /// Stop the stream
    pub fn stop(&mut self) {
        self.status = StreamStatus::Completed;
    }

    /// Get the current watermark
    pub fn watermark(&self) -> &Watermark {
        &self.watermark
    }

    /// Update the watermark
    pub fn advance_watermark(&mut self, timestamp_ms: i64) {
        self.watermark.advance(timestamp_ms);
        self.stats.current_watermark_ms = self.watermark.current();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::MemoryStreamSource;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_batch(values: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let array = Arc::new(Int64Array::from(values));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    #[tokio::test]
    async fn test_streaming_query_basic() {
        let batches = vec![
            create_test_batch(vec![1, 2, 3]),
            create_test_batch(vec![4, 5, 6]),
        ];
        let source = MemoryStreamSource::new(batches);
        let config = StreamConfig::default();
        let mut query = StreamingQuery::new(source, config);

        assert_eq!(*query.status(), StreamStatus::Running);

        let batch1 = query.next().await.unwrap().unwrap();
        assert_eq!(batch1.num_rows(), 3);

        let batch2 = query.next().await.unwrap().unwrap();
        assert_eq!(batch2.num_rows(), 3);

        assert!(query.next().await.is_none());
        assert_eq!(*query.status(), StreamStatus::Completed);
        assert_eq!(query.stats().batches_processed, 2);
        assert_eq!(query.stats().rows_processed, 6);
    }

    #[tokio::test]
    async fn test_pause_resume() {
        let batches = vec![create_test_batch(vec![1, 2, 3])];
        let source = MemoryStreamSource::new(batches);
        let mut query = StreamingQuery::new(source, StreamConfig::default());

        query.pause();
        assert_eq!(*query.status(), StreamStatus::Paused);

        query.resume();
        assert_eq!(*query.status(), StreamStatus::Running);
    }
}
