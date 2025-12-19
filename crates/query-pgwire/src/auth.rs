//! Authentication support for PostgreSQL wire protocol
//!
//! This module provides MD5 password authentication for PostgreSQL clients.
//!
//! # Example
//!
//! ```no_run
//! use query_pgwire::{PgServer, AuthConfig};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let auth = AuthConfig::new()
//!         .add_user("admin", "secret123")
//!         .add_user("readonly", "password");
//!
//!     let server = PgServer::new("0.0.0.0", 5432)
//!         .with_auth_config(auth);
//!     server.start().await?;
//!     Ok(())
//! }
//! ```

use async_trait::async_trait;
use pgwire::api::auth::md5pass::{Md5PasswordAuthStartupHandler, hash_md5_password};
use pgwire::api::auth::{AuthSource, DefaultServerParameterProvider, LoginInfo, Password};
use pgwire::error::PgWireResult;
use std::collections::HashMap;
use std::sync::Arc;

/// Configuration for PostgreSQL authentication
#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// Map of username to password
    users: HashMap<String, String>,
    /// Whether authentication is enabled
    enabled: bool,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthConfig {
    /// Create a new authentication configuration
    pub fn new() -> Self {
        Self {
            users: HashMap::new(),
            enabled: true,
        }
    }

    /// Add a user with password
    pub fn add_user(mut self, username: &str, password: &str) -> Self {
        self.users
            .insert(username.to_string(), password.to_string());
        self
    }

    /// Check if a user exists
    pub fn has_user(&self, username: &str) -> bool {
        self.users.contains_key(username)
    }

    /// Get the password for a user
    pub fn get_password(&self, username: &str) -> Option<&str> {
        self.users.get(username).map(|s| s.as_str())
    }

    /// Enable or disable authentication
    pub fn set_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Check if authentication is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled && !self.users.is_empty()
    }
}

/// Authentication source that validates credentials against AuthConfig
#[derive(Debug)]
pub struct QueryAuthSource {
    config: AuthConfig,
}

impl QueryAuthSource {
    /// Create a new auth source with the given configuration
    pub fn new(config: AuthConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl AuthSource for QueryAuthSource {
    async fn get_password(&self, login_info: &LoginInfo) -> PgWireResult<Password> {
        // Get the username from login info
        // Must bind to a local variable to extend lifetime
        let user_option = login_info.user();
        let username = user_option.as_deref().unwrap_or("");

        // Get the password for this user
        let password = self
            .config
            .get_password(username)
            .unwrap_or("invalid_password_hash");

        // Generate random salt for MD5 hashing
        let salt: [u8; 4] = rand::random();
        let salt_vec = salt.to_vec();

        // Hash the password with MD5
        let hash = hash_md5_password(username, password, &salt);

        Ok(Password::new(Some(salt_vec), hash.as_bytes().to_vec()))
    }
}

/// Create an MD5 password authentication startup handler
pub fn create_md5_auth_handler(
    config: AuthConfig,
) -> Md5PasswordAuthStartupHandler<QueryAuthSource, DefaultServerParameterProvider> {
    let auth_source = Arc::new(QueryAuthSource::new(config));
    let parameter_provider = Arc::new(DefaultServerParameterProvider::default());

    Md5PasswordAuthStartupHandler::new(auth_source, parameter_provider)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_config() {
        let config = AuthConfig::new()
            .add_user("admin", "password123")
            .add_user("readonly", "readonly");

        assert!(config.has_user("admin"));
        assert!(config.has_user("readonly"));
        assert!(!config.has_user("unknown"));

        assert_eq!(config.get_password("admin"), Some("password123"));
        assert_eq!(config.get_password("readonly"), Some("readonly"));
        assert_eq!(config.get_password("unknown"), None);
    }

    #[test]
    fn test_auth_enabled() {
        let config = AuthConfig::new();
        // Empty config means auth is not really enabled
        assert!(!config.is_enabled());

        let config = AuthConfig::new().add_user("admin", "pass");
        assert!(config.is_enabled());

        let config = AuthConfig::new()
            .add_user("admin", "pass")
            .set_enabled(false);
        assert!(!config.is_enabled());
    }
}
