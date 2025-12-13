//! Index implementations for Query Engine
//!
//! This crate provides B-Tree and Hash index implementations for fast data retrieval.
//!
//! # Index Types
//!
//! - **B-Tree Index**: Supports range queries and equality comparisons. Best for columns
//!   with high cardinality and range queries.
//!
//! - **Hash Index**: Optimized for equality lookups. O(1) average time complexity.
//!   Does not support range queries.
//!
//! # Example
//!
//! ```ignore
//! use query_index::{BTreeIndex, HashIndex, Index, IndexManager};
//!
//! let mut manager = IndexManager::new();
//! manager.create_btree_index("idx_name", "employees", vec!["name".to_string()], false)?;
//! ```

pub mod btree;
pub mod hash;
pub mod manager;
pub mod types;

pub use btree::BTreeIndex;
pub use hash::HashIndex;
pub use manager::IndexManager;
pub use types::*;
