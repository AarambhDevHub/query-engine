//! Core types and traits for index implementations

use query_core::Result;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Supported index types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexType {
    /// B-Tree index - supports range queries and equality
    BTree,
    /// Hash index - optimized for equality lookups only
    Hash,
}

impl Default for IndexType {
    fn default() -> Self {
        IndexType::BTree
    }
}

impl std::fmt::Display for IndexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexType::BTree => write!(f, "BTREE"),
            IndexType::Hash => write!(f, "HASH"),
        }
    }
}

/// Index metadata stored per table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMetadata {
    /// Name of the index
    pub name: String,
    /// Table the index belongs to
    pub table_name: String,
    /// Columns included in the index (in order)
    pub columns: Vec<String>,
    /// Type of index (BTree or Hash)
    pub index_type: IndexType,
    /// Whether the index enforces uniqueness
    pub unique: bool,
}

impl IndexMetadata {
    /// Create new index metadata
    pub fn new(
        name: impl Into<String>,
        table_name: impl Into<String>,
        columns: Vec<String>,
        index_type: IndexType,
        unique: bool,
    ) -> Self {
        Self {
            name: name.into(),
            table_name: table_name.into(),
            columns,
            index_type,
            unique,
        }
    }

    /// Check if this index can accelerate a lookup on the given column
    pub fn can_accelerate(&self, column: &str) -> bool {
        // Index can accelerate if the column is the first column in the index
        // (or the only column for single-column indexes)
        self.columns.first().map(|c| c == column).unwrap_or(false)
    }

    /// Check if this index covers all the given columns (in order)
    pub fn covers_columns(&self, columns: &[String]) -> bool {
        if columns.len() > self.columns.len() {
            return false;
        }
        columns.iter().zip(&self.columns).all(|(a, b)| a == b)
    }
}

/// Key used for index lookups
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct IndexKey(pub Vec<u8>);

impl IndexKey {
    /// Create a new index key from bytes
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// Create an index key from an i64 value
    pub fn from_i64(value: i64) -> Self {
        Self(value.to_be_bytes().to_vec())
    }

    /// Create an index key from a string
    pub fn from_string(value: &str) -> Self {
        Self(value.as_bytes().to_vec())
    }

    /// Create an index key from an f64 value
    pub fn from_f64(value: f64) -> Self {
        // Use bit representation for proper ordering
        let bits = value.to_bits();
        let adjusted = if value.is_sign_negative() {
            !bits
        } else {
            bits ^ (1u64 << 63)
        };
        Self(adjusted.to_be_bytes().to_vec())
    }

    /// Get the underlying bytes
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<[u8]> for IndexKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Result of an index lookup operation
#[derive(Debug, Clone)]
pub struct IndexLookupResult {
    /// Row IDs matching the lookup
    pub row_ids: Vec<usize>,
    /// Whether the result is exact (vs approximate)
    pub exact: bool,
}

impl IndexLookupResult {
    /// Create a new exact lookup result
    pub fn exact(row_ids: Vec<usize>) -> Self {
        Self {
            row_ids,
            exact: true,
        }
    }

    /// Create an empty result
    pub fn empty() -> Self {
        Self {
            row_ids: Vec::new(),
            exact: true,
        }
    }
}

/// Trait for index implementations
pub trait Index: Debug + Send + Sync {
    /// Get the index metadata
    fn metadata(&self) -> &IndexMetadata;

    /// Look up row IDs for an exact key match
    fn lookup(&self, key: &IndexKey) -> IndexLookupResult;

    /// Range scan from start to end (inclusive)
    /// Returns empty result for Hash indexes
    fn range_scan(&self, start: Option<&IndexKey>, end: Option<&IndexKey>) -> IndexLookupResult;

    /// Insert a key-rowid mapping
    fn insert(&self, key: IndexKey, row_id: usize) -> Result<()>;

    /// Delete a key-rowid mapping
    fn delete(&self, key: &IndexKey, row_id: usize) -> Result<()>;

    /// Check if the index supports range queries
    fn supports_range(&self) -> bool;

    /// Get the number of entries in the index
    fn len(&self) -> usize;

    /// Check if the index is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear all entries from the index
    fn clear(&self);
}

/// Bounds for range scans
#[derive(Debug, Clone)]
pub enum Bound<T> {
    /// No bound (unbounded)
    Unbounded,
    /// Inclusive bound
    Included(T),
    /// Exclusive bound
    Excluded(T),
}

impl<T> Bound<T> {
    /// Convert to Option, losing the inclusive/exclusive distinction
    pub fn as_option(&self) -> Option<&T> {
        match self {
            Bound::Unbounded => None,
            Bound::Included(v) | Bound::Excluded(v) => Some(v),
        }
    }
}
