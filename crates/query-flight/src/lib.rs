//! Arrow Flight server and client for Query Engine
//!
//! This crate provides Apache Arrow Flight support for high-performance
//! network data transfer, enabling remote SQL query execution over gRPC.
//!
//! # Features
//!
//! - **Flight Server**: Host a Query Engine as a Flight service
//! - **Flight Client**: Connect to remote Flight servers
//! - **SQL Execution**: Execute SQL queries over the network
//! - **Streaming**: Stream RecordBatches efficiently
//! - **Data Sources**: Use Flight as a DataSource or StreamSource
//!
//! # Example
//!
//! ```ignore
//! use query_flight::{FlightServer, FlightClient};
//! use std::net::SocketAddr;
//!
//! // Start a Flight server
//! let addr: SocketAddr = "0.0.0.0:50051".parse()?;
//! let server = FlightServer::new();
//! server.serve(addr).await?;
//!
//! // Connect with a client
//! let client = FlightClient::connect("http://localhost:50051").await?;
//! let batches = client.execute_sql("SELECT * FROM users").await?;
//! ```

pub mod client;
pub mod data_source;
pub mod error;
pub mod server;
pub mod stream_source;

pub use client::FlightClient;
pub use data_source::FlightDataSource;
pub use error::FlightError;
pub use server::{FlightServer, FlightServiceImpl};
pub use stream_source::FlightStreamSource;

/// Result type for Flight operations
pub type Result<T> = std::result::Result<T, FlightError>;
