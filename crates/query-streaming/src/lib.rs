//! Real-time Streaming Query Processing
//!
//! This crate provides streaming query capabilities for processing
//! continuous data streams with windowing and watermark support.
//!
//! # Features
//!
//! - **Stream Sources**: Async streams of RecordBatch data
//! - **Windowing**: Tumbling, Sliding, and Session windows
//! - **Watermarks**: Event-time processing with late event handling
//! - **Aggregations**: Windowed aggregations over streams
//!
//! # Example
//!
//! ```ignore
//! use query_streaming::{StreamingQuery, StreamConfig, ChannelStreamSource};
//!
//! let (tx, source) = ChannelStreamSource::new(100);
//! let config = StreamConfig::default();
//! let query = StreamingQuery::new(source, config);
//!
//! // Process stream
//! while let Some(batch) = query.next().await {
//!     process_batch(batch?);
//! }
//! ```

pub mod source;
pub mod stream;
pub mod watermark;
pub mod window;

pub use source::{ChannelStreamSource, MemoryStreamSource, StreamSource};
pub use stream::{StreamConfig, StreamStats, StreamStatus, StreamingQuery};
pub use watermark::{LateEventPolicy, Watermark};
pub use window::{SessionWindow, SlidingWindow, TumblingWindow, Window, WindowType};
