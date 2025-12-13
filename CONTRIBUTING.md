# Contributing to Query Engine

Thank you for your interest in contributing to Query Engine! This document provides guidelines and instructions for contributing.

## 🚀 Getting Started

### Prerequisites

- **Rust 1.70+** - Install from [rustup.rs](https://rustup.rs/)
- **Git** - For version control

### Development Setup

```bash
# Clone the repository
git clone https://github.com/AarambhDevHub/query-engine.git
cd query-engine

# Build the project
cargo build

# Run tests
cargo test

# Run the CLI
cargo run -p query-cli
```

## 📂 Project Structure

```
query-engine/
├── crates/
│   ├── query-core/         # Core types, errors, schema definitions
│   ├── query-parser/       # SQL lexer and parser
│   ├── query-planner/      # Logical plan creation and optimization
│   ├── query-executor/     # Physical execution engine
│   ├── query-storage/      # Data source implementations (CSV, Parquet)
│   ├── query-index/        # B-Tree and Hash index implementations
│   ├── query-distributed/  # Distributed execution framework
│   └── query-cli/          # Command-line interface
├── examples-package/       # Example usage code
├── docs/                   # Documentation
└── Cargo.toml             # Workspace configuration
```

## 🔧 Development Workflow

### 1. Create a Branch

```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/bug-description
```

### 2. Make Changes

- Write clean, idiomatic Rust code
- Follow existing code patterns
- Add tests for new functionality
- Update documentation as needed

### 3. Code Quality

```bash
# Format code
cargo fmt

# Run clippy lints
cargo clippy

# Run all tests
cargo test --workspace

# Check specific crate
cargo check -p query-parser
```

### 4. Commit Guidelines

Use conventional commit messages:

```
feat: Add new SQL function SUBSTRING
fix: Handle NULL values in JOIN operations
docs: Update README with new examples
refactor: Simplify expression evaluation logic
test: Add tests for window functions
```

### 5. Submit Pull Request

1. Push your branch: `git push origin feature/your-feature-name`
2. Open a Pull Request on GitHub
3. Fill in the PR template with details
4. Wait for review

## 🧪 Testing Guidelines

### Running Tests

```bash
# All tests
cargo test --workspace

# Specific crate
cargo test -p query-executor

# With output
cargo test -- --nocapture

# Single test
cargo test test_join_queries
```

### Writing Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_function_name() {
        // Arrange
        let input = create_test_data();
        
        // Act
        let result = function_under_test(input);
        
        // Assert
        assert_eq!(result, expected);
    }
}
```

## 📝 Documentation

- Add doc comments (`///`) to all public items
- Update README.md for user-facing changes
- Add examples in the `examples-package/` directory

```rust
/// Executes a SQL query and returns the results.
///
/// # Arguments
///
/// * `sql` - The SQL query string to execute
///
/// # Returns
///
/// A `Result` containing a vector of `RecordBatch` on success
///
/// # Example
///
/// ```ignore
/// let results = executor.execute("SELECT * FROM users")?;
/// ```
pub fn execute(&self, sql: &str) -> Result<Vec<RecordBatch>> {
    // ...
}
```

## 🏗️ Architecture Guidelines

### Adding a New SQL Feature

1. **Parser** (`query-parser`): Add lexer tokens and parser grammar
2. **AST** (`query-parser`): Define AST nodes for the feature
3. **Planner** (`query-planner`): Create logical plan nodes
4. **Executor** (`query-executor`): Implement physical execution
5. **Tests**: Add unit and integration tests
6. **Examples**: Create usage examples
7. **Docs**: Update documentation

### Adding a New Data Source

1. Implement the `DataSource` trait in `query-storage`
2. Add file loading logic
3. Register with the CLI
4. Add tests and examples

## 🐛 Bug Reports

When reporting bugs, please include:

- Rust version (`rustc --version`)
- Operating system
- Steps to reproduce
- Expected vs actual behavior
- Error messages (full stack trace if available)

## 💡 Feature Requests

For feature requests:

1. Check existing issues for duplicates
2. Describe the use case
3. Propose a solution if possible
4. Be open to discussion

## 📋 Code Review Process

All submissions require review. We use GitHub pull requests for this purpose:

1. Maintainers will review your PR
2. Address any feedback
3. Once approved, your PR will be merged

## 🎯 Areas for Contribution

- **SQL Features**: New SQL functions, operators, clauses
- **Performance**: Optimization, benchmarking
- **Documentation**: Tutorials, examples, API docs
- **Testing**: Increased coverage, edge cases
- **Bug Fixes**: Help squash bugs!

## 📜 License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.

---

Thank you for contributing! 🙏
