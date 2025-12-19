//! PostgreSQL Wire Protocol for Query Engine
//!
//! This crate provides PostgreSQL protocol compatibility, allowing standard
//! PostgreSQL clients (psql, pgAdmin, DBeaver, etc.) to connect and query
//! the Query Engine.
//!
//! # Features
//!
//! - **Simple Query Protocol**: Text-based SQL queries
//! - **Extended Query Protocol**: Prepared statements and parameter binding
//! - **MD5 Authentication**: Password-based authentication
//! - **TLS/SSL**: Encrypted connections
//!
//! # Example - Basic Server (No Authentication)
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
//!
//! # Example - Server with Authentication
//!
//! ```no_run
//! use query_pgwire::{PgServer, AuthConfig};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Simple single-user auth
//!     let server = PgServer::new("0.0.0.0", 5432)
//!         .with_auth("admin", "secret123");
//!     server.start().await?;
//!     Ok(())
//! }
//! ```
//!
//! # Example - Server with TLS
//!
//! ```no_run
//! use query_pgwire::{PgServer, TlsConfig};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let tls = TlsConfig::new("server.crt", "server.key");
//!     let server = PgServer::new("0.0.0.0", 5432)
//!         .with_tls(tls);
//!     server.start().await?;
//!     Ok(())
//! }
//! ```

pub mod auth;
pub mod backend;
pub mod extended;
pub mod result;
pub mod server;
pub mod tls;

// Authentication exports
pub use auth::AuthConfig;

// Backend exports
pub use backend::{AuthQueryServerHandlers, QueryBackend, QueryServerHandlers};

// Extended query exports
pub use extended::QueryExtendedHandler;

// Result conversion exports
pub use result::{arrow_to_pg_type, record_batch_to_rows, schema_to_field_info};

// Server exports
pub use server::PgServer;

// TLS exports
pub use tls::TlsConfig;
