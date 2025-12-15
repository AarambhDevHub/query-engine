//! Flight configuration and endpoint types
//!
//! Common types for Arrow Flight integration used across crates.

use serde::{Deserialize, Serialize};

/// Configuration for an Arrow Flight server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlightConfig {
    /// Host to bind to
    pub host: String,
    /// Port to listen on
    pub port: u16,
    /// Enable TLS
    pub tls_enabled: bool,
    /// TLS certificate path (if TLS enabled)
    pub tls_cert_path: Option<String>,
    /// TLS key path (if TLS enabled)
    pub tls_key_path: Option<String>,
    /// Maximum concurrent connections
    pub max_connections: usize,
    /// Request timeout in seconds
    pub timeout_secs: u64,
}

impl Default for FlightConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 50051,
            tls_enabled: false,
            tls_cert_path: None,
            tls_key_path: None,
            max_connections: 100,
            timeout_secs: 30,
        }
    }
}

impl FlightConfig {
    /// Create a new config with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a config for local development
    pub fn local(port: u16) -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port,
            ..Default::default()
        }
    }

    /// Get the address string (host:port)
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// Get the full URL
    pub fn url(&self) -> String {
        let scheme = if self.tls_enabled { "https" } else { "http" };
        format!("{}://{}:{}", scheme, self.host, self.port)
    }
}

/// A remote Flight endpoint descriptor
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FlightEndpoint {
    /// Endpoint URL
    pub url: String,
    /// Optional authentication token
    pub auth_token: Option<String>,
    /// Whether to verify TLS certificates
    pub verify_tls: bool,
}

impl FlightEndpoint {
    /// Create a new endpoint from a URL
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            auth_token: None,
            verify_tls: true,
        }
    }

    /// Create an endpoint with authentication
    pub fn with_auth(url: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            auth_token: Some(token.into()),
            verify_tls: true,
        }
    }

    /// Set TLS verification
    pub fn verify_tls(mut self, verify: bool) -> Self {
        self.verify_tls = verify;
        self
    }
}

impl From<&str> for FlightEndpoint {
    fn from(url: &str) -> Self {
        Self::new(url)
    }
}

impl From<String> for FlightEndpoint {
    fn from(url: String) -> Self {
        Self::new(url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flight_config_default() {
        let config = FlightConfig::default();
        assert_eq!(config.port, 50051);
        assert_eq!(config.host, "0.0.0.0");
        assert!(!config.tls_enabled);
    }

    #[test]
    fn test_flight_config_url() {
        let config = FlightConfig::local(8080);
        assert_eq!(config.url(), "http://127.0.0.1:8080");
    }

    #[test]
    fn test_flight_endpoint() {
        let endpoint = FlightEndpoint::new("http://localhost:50051");
        assert_eq!(endpoint.url, "http://localhost:50051");
        assert!(endpoint.auth_token.is_none());
    }
}
