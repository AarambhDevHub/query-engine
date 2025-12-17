//! PostgreSQL Wire Protocol for Query Engine
//!
//! This crate provides PostgreSQL protocol compatibility, allowing standard
//! PostgreSQL clients (psql, pgAdmin, DBeaver, etc.) to connect and query
//! the Query Engine.
//!
//! # Example
//!
//! ```no_run
//! use query_pgwire::PgServer;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let server = PgServer::new("0.0.0.0", 5432);
//!     server.start().await?;
//!     Ok(())
//! }
//! ```

pub mod backend;
pub mod result;
pub mod server;

pub use backend::{QueryBackend, QueryServerHandlers};
pub use result::{arrow_to_pg_type, record_batch_to_rows, schema_to_field_info};
pub use server::PgServer;
