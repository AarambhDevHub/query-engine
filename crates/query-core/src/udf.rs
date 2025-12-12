//! User-Defined Functions (UDF) infrastructure
//!
//! This module provides the core types and traits for defining and registering
//! scalar user-defined functions.

use crate::{DataType, Result};
use arrow::array::ArrayRef;
use std::collections::HashMap;
use std::sync::Arc;

/// Signature of a user-defined function
#[derive(Debug, Clone)]
pub struct UdfSignature {
    /// Name of the function
    pub name: String,
    /// Expected argument types
    pub arg_types: Vec<DataType>,
    /// Return type of the function
    pub return_type: DataType,
    /// Whether the function accepts variadic arguments
    pub variadic: bool,
}

impl UdfSignature {
    /// Create a new UDF signature
    pub fn new(name: impl Into<String>, arg_types: Vec<DataType>, return_type: DataType) -> Self {
        Self {
            name: name.into(),
            arg_types,
            return_type,
            variadic: false,
        }
    }

    /// Create a variadic function signature
    pub fn variadic(name: impl Into<String>, arg_type: DataType, return_type: DataType) -> Self {
        Self {
            name: name.into(),
            arg_types: vec![arg_type],
            return_type,
            variadic: true,
        }
    }
}

/// Trait for scalar user-defined functions
///
/// Scalar functions process data row-by-row, producing one output value
/// for each input row.
pub trait ScalarUdf: Send + Sync + std::fmt::Debug {
    /// Returns the name of the function
    fn name(&self) -> &str;

    /// Returns the signature of the function
    fn signature(&self) -> &UdfSignature;

    /// Invokes the function with the given arguments
    ///
    /// Each argument is an Arrow array, and the function should return
    /// an Arrow array of the result.
    fn invoke(&self, args: &[ArrayRef]) -> Result<ArrayRef>;
}

/// Registry for storing and looking up user-defined functions
#[derive(Debug, Default)]
pub struct UdfRegistry {
    /// Map of function name to function implementation
    functions: HashMap<String, Arc<dyn ScalarUdf>>,
}

impl UdfRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            functions: HashMap::new(),
        }
    }

    /// Register a user-defined function
    pub fn register(&mut self, udf: Arc<dyn ScalarUdf>) {
        let name = udf.name().to_lowercase();
        self.functions.insert(name, udf);
    }

    /// Look up a function by name
    pub fn get(&self, name: &str) -> Option<Arc<dyn ScalarUdf>> {
        self.functions.get(&name.to_lowercase()).cloned()
    }

    /// Check if a function is registered
    pub fn contains(&self, name: &str) -> bool {
        self.functions.contains_key(&name.to_lowercase())
    }

    /// Get all registered function names
    pub fn names(&self) -> Vec<&str> {
        self.functions.keys().map(|s| s.as_str()).collect()
    }

    /// Number of registered functions
    pub fn len(&self) -> usize {
        self.functions.len()
    }

    /// Check if registry is empty
    pub fn is_empty(&self) -> bool {
        self.functions.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    /// Example custom UDF for testing
    #[derive(Debug)]
    struct MyUpperUdf {
        signature: UdfSignature,
    }

    impl MyUpperUdf {
        fn new() -> Self {
            Self {
                signature: UdfSignature::new("my_upper", vec![DataType::Utf8], DataType::Utf8),
            }
        }
    }

    impl ScalarUdf for MyUpperUdf {
        fn name(&self) -> &str {
            &self.signature.name
        }

        fn signature(&self) -> &UdfSignature {
            &self.signature
        }

        fn invoke(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
            let string_array = args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected StringArray");

            let result: StringArray = string_array
                .iter()
                .map(|opt| opt.map(|s| s.to_uppercase()))
                .collect();

            Ok(Arc::new(result) as ArrayRef)
        }
    }

    #[test]
    fn test_udf_registry() {
        let mut registry = UdfRegistry::new();
        assert!(registry.is_empty());

        // Register a custom UDF
        registry.register(Arc::new(MyUpperUdf::new()));
        assert_eq!(registry.len(), 1);
        assert!(registry.contains("my_upper"));
        assert!(registry.contains("MY_UPPER")); // Case insensitive

        // Look up the function
        let udf = registry.get("my_upper").unwrap();
        assert_eq!(udf.name(), "my_upper");
        assert_eq!(udf.signature().return_type, DataType::Utf8);
    }

    #[test]
    fn test_udf_invoke() {
        let udf = MyUpperUdf::new();

        let input = Arc::new(StringArray::from(vec!["hello", "world"])) as ArrayRef;
        let result = udf.invoke(&[input]).unwrap();

        let string_result = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(string_result.value(0), "HELLO");
        assert_eq!(string_result.value(1), "WORLD");
    }
}
