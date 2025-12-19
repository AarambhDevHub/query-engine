//! PostgreSQL server implementation

use crate::auth::AuthConfig;
use crate::backend::{AuthQueryServerHandlers, QueryBackend, QueryServerHandlers, TableEntry};
use arrow::csv::ReaderBuilder;
use pgwire::tokio::process_socket;
use query_core::Schema;
use query_storage::MemoryDataSource;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

/// PostgreSQL-compatible server for Query Engine
///
/// # Example
///
/// ```no_run
/// use query_pgwire::PgServer;
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let server = PgServer::new("0.0.0.0", 5432);
///     server.load_csv("data/users.csv", "users").await?;
///     server.start().await?;
///     Ok(())
/// }
/// ```
pub struct PgServer {
    host: String,
    port: u16,
    tables: Arc<RwLock<HashMap<String, TableEntry>>>,
    auth_config: Option<AuthConfig>,
}

impl PgServer {
    /// Create a new PgServer instance (no authentication)
    pub fn new(host: &str, port: u16) -> Self {
        Self {
            host: host.to_string(),
            port,
            tables: Arc::new(RwLock::new(HashMap::new())),
            auth_config: None,
        }
    }

    /// Enable MD5 password authentication with a single user
    ///
    /// # Example
    ///
    /// ```no_run
    /// use query_pgwire::PgServer;
    ///
    /// let server = PgServer::new("0.0.0.0", 5432)
    ///     .with_auth("admin", "secret123");
    /// ```
    pub fn with_auth(mut self, username: &str, password: &str) -> Self {
        let config = AuthConfig::new().add_user(username, password);
        self.auth_config = Some(config);
        self
    }

    /// Enable MD5 password authentication with a custom configuration
    ///
    /// # Example
    ///
    /// ```no_run
    /// use query_pgwire::{PgServer, AuthConfig};
    ///
    /// let auth = AuthConfig::new()
    ///     .add_user("admin", "admin_pass")
    ///     .add_user("readonly", "readonly_pass");
    ///
    /// let server = PgServer::new("0.0.0.0", 5432)
    ///     .with_auth_config(auth);
    /// ```
    pub fn with_auth_config(mut self, config: AuthConfig) -> Self {
        self.auth_config = Some(config);
        self
    }

    /// Register a table with Arrow RecordBatches
    pub async fn register_table(
        &self,
        table_name: &str,
        batches: Vec<arrow::record_batch::RecordBatch>,
    ) -> anyhow::Result<()> {
        if batches.is_empty() {
            anyhow::bail!("Cannot register empty table");
        }

        let arrow_schema = batches[0].schema();
        let schema = Schema::from_arrow(&arrow_schema);
        let source = Arc::new(MemoryDataSource::new(schema.clone(), batches));

        let mut tables = self.tables.write().await;
        tables.insert(table_name.to_string(), TableEntry { schema, source });

        info!("Registered table '{}' with {} batches", table_name, 1);
        Ok(())
    }

    /// Load a CSV file as a table
    pub async fn load_csv(&self, path: &str, table_name: &str) -> anyhow::Result<()> {
        let path = Path::new(path);
        if !path.exists() {
            anyhow::bail!("File not found: {}", path.display());
        }

        info!(
            "Loading CSV file: {} as table '{}'",
            path.display(),
            table_name
        );

        // First, infer the schema by reading CSV headers and sample data
        let file = File::open(path)?;

        // Use arrow's CSV reader with schema inference
        let format = arrow::csv::reader::Format::default().with_header(true);
        let (arrow_schema, _) = format.infer_schema(file, Some(100))?;
        let arrow_schema_ref = Arc::new(arrow_schema);

        // Convert Arrow schema to query_core::Schema
        let schema = Schema::from_arrow(&arrow_schema_ref);

        // Reopen file and create reader with inferred schema
        let file = File::open(path)?;
        let reader = ReaderBuilder::new(Arc::clone(&arrow_schema_ref))
            .with_header(true)
            .build(file)?;

        // Read all batches
        let batches: Vec<_> = reader.into_iter().filter_map(|r| r.ok()).collect();

        if batches.is_empty() {
            anyhow::bail!("No data in CSV file");
        }

        // Create memory data source with query_core::Schema
        let source = Arc::new(MemoryDataSource::new(schema.clone(), batches));

        // Register table
        let mut tables = self.tables.write().await;
        tables.insert(table_name.to_string(), TableEntry { schema, source });

        info!("Loaded table '{}' from {}", table_name, path.display());
        Ok(())
    }

    /// Start the PostgreSQL server
    pub async fn start(self) -> anyhow::Result<()> {
        let addr = format!("{}:{}", self.host, self.port);
        let listener = TcpListener::bind(&addr).await?;

        info!("PostgreSQL server listening on {}", addr);
        info!("Connect with: psql -h {} -p {}", self.host, self.port);

        // Check if authentication is enabled
        let auth_enabled = self
            .auth_config
            .as_ref()
            .map(|c| c.is_enabled())
            .unwrap_or(false);
        if auth_enabled {
            info!("MD5 password authentication is ENABLED");
        } else {
            warn!("Authentication is DISABLED - server accepts all connections");
        }

        // Take ownership of auth_config before consuming self
        let auth_config = self.auth_config;
        let tables = self.tables;

        // Choose connection handling based on auth config
        match auth_config {
            Some(config) if config.is_enabled() => {
                Self::run_with_auth_static(tables, listener, config).await
            }
            _ => Self::run_without_auth_static(tables, listener).await,
        }
    }

    /// Run the server without authentication (static version)
    async fn run_without_auth_static(
        tables: Arc<RwLock<HashMap<String, TableEntry>>>,
        listener: TcpListener,
    ) -> anyhow::Result<()> {
        loop {
            match listener.accept().await {
                Ok((socket, peer_addr)) => {
                    info!("New connection from {}", peer_addr);
                    let tables = Arc::clone(&tables);

                    tokio::spawn(async move {
                        // Create a fresh handler for each connection
                        let backend = Arc::new(QueryBackend::with_tables(tables));
                        let handlers = Arc::new(QueryServerHandlers::new(backend));

                        if let Err(e) = process_socket(socket, None, handlers).await {
                            error!("Connection error: {}", e);
                        }
                        info!("Connection from {} closed", peer_addr);
                    });
                }
                Err(e) => {
                    error!("Accept error: {}", e);
                }
            }
        }
    }

    /// Run the server with MD5 password authentication (static version)
    async fn run_with_auth_static(
        tables: Arc<RwLock<HashMap<String, TableEntry>>>,
        listener: TcpListener,
        auth_config: AuthConfig,
    ) -> anyhow::Result<()> {
        let auth_config = Arc::new(auth_config);

        loop {
            match listener.accept().await {
                Ok((socket, peer_addr)) => {
                    info!(
                        "New connection from {} (authentication required)",
                        peer_addr
                    );
                    let tables = Arc::clone(&tables);
                    let auth = (*auth_config).clone();

                    tokio::spawn(async move {
                        // Create a fresh handler for each connection with auth
                        let backend = Arc::new(QueryBackend::with_tables(tables));
                        let handlers = Arc::new(AuthQueryServerHandlers::new(backend, auth));

                        if let Err(e) = process_socket(socket, None, handlers).await {
                            error!("Connection error: {}", e);
                        }
                        info!("Connection from {} closed", peer_addr);
                    });
                }
                Err(e) => {
                    error!("Accept error: {}", e);
                }
            }
        }
    }
}
