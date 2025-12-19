//! TLS/SSL support for PostgreSQL wire protocol
//!
//! This module provides TLS encryption for PostgreSQL connections using rustls.
//!
//! # Example
//!
//! ```ignore
//! use query_pgwire::{PgServer, TlsConfig};
//!
//! let tls = TlsConfig::new("server.crt", "server.key")?;
//! let server = PgServer::new("0.0.0.0", 5432)
//!     .with_tls(tls);
//! ```

use std::fs::File;
use std::io::{BufReader, Error as IoError, ErrorKind};
use std::path::Path;
use std::sync::Arc;

use rustls_pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;

/// TLS configuration for PostgreSQL server
#[derive(Clone)]
pub struct TlsConfig {
    /// Path to the certificate file (PEM format)
    pub cert_path: String,
    /// Path to the private key file (PEM format)
    pub key_path: String,
}

impl TlsConfig {
    /// Create a new TLS configuration
    ///
    /// # Arguments
    /// * `cert_path` - Path to the PEM certificate file
    /// * `key_path` - Path to the PEM private key file
    pub fn new(cert_path: impl Into<String>, key_path: impl Into<String>) -> Self {
        Self {
            cert_path: cert_path.into(),
            key_path: key_path.into(),
        }
    }

    /// Create a TLS acceptor from this configuration
    pub fn create_acceptor(&self) -> Result<TlsAcceptor, IoError> {
        create_tls_acceptor(&self.cert_path, &self.key_path)
    }
}

/// Load certificates from a PEM file
fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, IoError> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    certs(&mut reader).collect()
}

/// Load private key from a PEM file (supports PKCS8 and RSA formats)
fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, IoError> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    // Try PKCS8 format first
    let pkcs8_keys: Vec<_> = pkcs8_private_keys(&mut reader)
        .filter_map(|r| r.ok())
        .collect();

    if let Some(key) = pkcs8_keys.into_iter().next() {
        return Ok(PrivateKeyDer::Pkcs8(key));
    }

    // Try RSA format
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let rsa_keys: Vec<_> = rsa_private_keys(&mut reader)
        .filter_map(|r| r.ok())
        .collect();

    if let Some(key) = rsa_keys.into_iter().next() {
        return Ok(PrivateKeyDer::Pkcs1(key));
    }

    Err(IoError::new(
        ErrorKind::InvalidData,
        "No valid private key found in file",
    ))
}

/// Create a TLS acceptor from certificate and key files
pub fn create_tls_acceptor(cert_path: &str, key_path: &str) -> Result<TlsAcceptor, IoError> {
    let cert_path = Path::new(cert_path);
    let key_path = Path::new(key_path);

    // Load certificates
    let certs = load_certs(cert_path)?;
    if certs.is_empty() {
        return Err(IoError::new(
            ErrorKind::InvalidData,
            "No certificates found in file",
        ));
    }

    // Load private key
    let key = load_private_key(key_path)?;

    // Build server config
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|err| IoError::new(ErrorKind::InvalidInput, err))?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tls_config_creation() {
        let config = TlsConfig::new("test.crt", "test.key");
        assert_eq!(config.cert_path, "test.crt");
        assert_eq!(config.key_path, "test.key");
    }
}
