//! Stream source implementations
//!
//! Provides traits and implementations for streaming data sources.

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use query_core::Result;
use tokio::sync::mpsc;

/// Trait for streaming data sources
#[async_trait]
pub trait StreamSource: Send + Sync {
    /// Get the next batch from the stream
    /// Returns None when the stream is exhausted
    async fn next_batch(&mut self) -> Option<Result<RecordBatch>>;

    /// Check if the stream has more data
    fn is_exhausted(&self) -> bool;

    /// Get stream metadata
    fn name(&self) -> &str;
}

/// Stream source backed by a tokio mpsc channel
pub struct ChannelStreamSource {
    receiver: mpsc::Receiver<RecordBatch>,
    name: String,
    exhausted: bool,
}

impl ChannelStreamSource {
    /// Create a new channel stream source with the given buffer size
    /// Returns (sender, source) tuple
    pub fn new(buffer_size: usize) -> (mpsc::Sender<RecordBatch>, Self) {
        let (tx, rx) = mpsc::channel(buffer_size);
        let source = Self {
            receiver: rx,
            name: "channel".to_string(),
            exhausted: false,
        };
        (tx, source)
    }

    /// Create with a custom name
    pub fn with_name(
        buffer_size: usize,
        name: impl Into<String>,
    ) -> (mpsc::Sender<RecordBatch>, Self) {
        let (tx, rx) = mpsc::channel(buffer_size);
        let source = Self {
            receiver: rx,
            name: name.into(),
            exhausted: false,
        };
        (tx, source)
    }
}

#[async_trait]
impl StreamSource for ChannelStreamSource {
    async fn next_batch(&mut self) -> Option<Result<RecordBatch>> {
        match self.receiver.recv().await {
            Some(batch) => Some(Ok(batch)),
            None => {
                self.exhausted = true;
                None
            }
        }
    }

    fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// In-memory stream source for testing
pub struct MemoryStreamSource {
    batches: Vec<RecordBatch>,
    position: usize,
    name: String,
}

impl MemoryStreamSource {
    /// Create a new memory stream source
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self {
            batches,
            position: 0,
            name: "memory".to_string(),
        }
    }

    /// Create with a custom name
    pub fn with_name(batches: Vec<RecordBatch>, name: impl Into<String>) -> Self {
        Self {
            batches,
            position: 0,
            name: name.into(),
        }
    }

    /// Reset the stream to the beginning
    pub fn reset(&mut self) {
        self.position = 0;
    }
}

#[async_trait]
impl StreamSource for MemoryStreamSource {
    async fn next_batch(&mut self) -> Option<Result<RecordBatch>> {
        if self.position < self.batches.len() {
            let batch = self.batches[self.position].clone();
            self.position += 1;
            Some(Ok(batch))
        } else {
            None
        }
    }

    fn is_exhausted(&self) -> bool {
        self.position >= self.batches.len()
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc as StdArc;

    fn create_test_batch(values: Vec<i64>) -> RecordBatch {
        let schema = StdArc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let array = StdArc::new(Int64Array::from(values));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    #[tokio::test]
    async fn test_memory_stream_source() {
        let batches = vec![
            create_test_batch(vec![1, 2, 3]),
            create_test_batch(vec![4, 5, 6]),
        ];
        let mut source = MemoryStreamSource::new(batches);

        assert!(!source.is_exhausted());

        let batch1 = source.next_batch().await.unwrap().unwrap();
        assert_eq!(batch1.num_rows(), 3);

        let batch2 = source.next_batch().await.unwrap().unwrap();
        assert_eq!(batch2.num_rows(), 3);

        assert!(source.next_batch().await.is_none());
        assert!(source.is_exhausted());
    }

    #[tokio::test]
    async fn test_channel_stream_source() {
        let (tx, mut source) = ChannelStreamSource::new(10);

        // Send batches
        let batch = create_test_batch(vec![1, 2, 3]);
        tx.send(batch.clone()).await.unwrap();
        tx.send(batch.clone()).await.unwrap();
        drop(tx); // Close channel

        // Receive batches
        let received1 = source.next_batch().await.unwrap().unwrap();
        assert_eq!(received1.num_rows(), 3);

        let received2 = source.next_batch().await.unwrap().unwrap();
        assert_eq!(received2.num_rows(), 3);

        assert!(source.next_batch().await.is_none());
        assert!(source.is_exhausted());
    }
}
