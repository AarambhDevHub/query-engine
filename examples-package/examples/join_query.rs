use arrow::array::*;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use query_core::{DataType, Field, Schema};
use query_parser::Parser;
use query_planner::{Optimizer, Planner};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== JOIN Operations Example ===\n");

    // Create users table
    let users_schema = Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("department_id", DataType::Int64, false),
    ]);

    let users_batch = RecordBatch::try_new(
        Arc::new(users_schema.to_arrow()),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Alice", "Bob", "Charlie", "Diana", "Eve",
            ])),
            Arc::new(Int64Array::from(vec![10, 20, 10, 30, 20])),
        ],
    )?;

    // Create departments table
    let dept_schema = Schema::new(vec![
        Field::new("department_id", DataType::Int64, false),
        Field::new("department_name", DataType::Utf8, false),
        Field::new("location", DataType::Utf8, false),
    ]);

    let dept_batch = RecordBatch::try_new(
        Arc::new(dept_schema.to_arrow()),
        vec![
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(StringArray::from(vec!["Engineering", "Sales", "HR"])),
            Arc::new(StringArray::from(vec![
                "Building A",
                "Building B",
                "Building C",
            ])),
        ],
    )?;

    println!("Users Table:");
    print_batches(&[users_batch])?;
    println!();

    println!("Departments Table:");
    print_batches(&[dept_batch])?;
    println!();

    // Test various JOIN queries
    let test_queries = vec![
        (
            "INNER JOIN",
            "SELECT users.name, departments.department_name FROM users INNER JOIN departments ON users.department_id = departments.department_id",
        ),
        (
            "LEFT JOIN",
            "SELECT users.name, departments.department_name FROM users LEFT JOIN departments ON users.department_id = departments.department_id",
        ),
        (
            "RIGHT JOIN",
            "SELECT users.name, departments.department_name FROM users RIGHT JOIN departments ON users.department_id = departments.department_id",
        ),
        (
            "FULL OUTER JOIN",
            "SELECT users.name, departments.department_name FROM users FULL OUTER JOIN departments ON users.department_id = departments.department_id",
        ),
        (
            "CROSS JOIN",
            "SELECT users.name, departments.department_name FROM users CROSS JOIN departments",
        ),
        (
            "Multiple JOINs",
            "SELECT u.name, d.department_name FROM users u JOIN departments d ON u.department_id = d.department_id WHERE d.location = 'Building A'",
        ),
    ];

    for (description, sql) in test_queries {
        println!("\n{}", "=".repeat(80));
        println!("Test: {}", description);
        println!("SQL: {}", sql);
        println!("{}", "-".repeat(80));

        match Parser::new(sql) {
            Ok(mut parser) => match parser.parse() {
                Ok(statement) => {
                    let mut planner = Planner::new();
                    planner.register_table("users", users_schema.clone());
                    planner.register_table("departments", dept_schema.clone());

                    match planner.create_logical_plan(&statement) {
                        Ok(logical_plan) => {
                            let optimizer = Optimizer::new();
                            match optimizer.optimize(&logical_plan) {
                                Ok(optimized) => {
                                    println!("✓ Success!");
                                    println!("Logical Plan: {:#?}", optimized);
                                }
                                Err(e) => println!("✗ Optimization Error: {}", e),
                            }
                        }
                        Err(e) => println!("✗ Planning Error: {}", e),
                    }
                }
                Err(e) => println!("✗ Parse Error: {}", e),
            },
            Err(e) => println!("✗ Lexer Error: {}", e),
        }
    }

    println!("\n{}", "=".repeat(80));
    println!("JOIN Demo completed!");

    Ok(())
}
