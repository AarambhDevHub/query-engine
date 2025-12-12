use query_core::{DataType, Field, Schema};
use query_parser::Parser;
use query_planner::{Optimizer, Planner};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Subqueries and CTEs Example ===\n");

    // Create sample schema for testing
    let employees_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("dept_id", DataType::Int64, false),
        Field::new("salary", DataType::Float64, false),
    ]);

    let departments_schema = Schema::new(vec![
        Field::new("dept_id", DataType::Int64, false),
        Field::new("dept_name", DataType::Utf8, false),
        Field::new("location", DataType::Utf8, false),
    ]);

    println!("Employees Schema: {:?}", employees_schema);
    println!("Departments Schema: {:?}\n", departments_schema);

    // Test queries demonstrating new capabilities
    let test_queries = vec![
        (
            "Simple CTE",
            "WITH eng AS (SELECT id, name, salary FROM employees) SELECT name, salary FROM eng",
        ),
        (
            "CTE with alias",
            "WITH high_salary AS (SELECT name, salary FROM employees) SELECT * FROM high_salary AS hs",
        ),
        (
            "Subquery in FROM clause",
            "SELECT sub.name FROM (SELECT id, name FROM employees) AS sub",
        ),
        (
            "Scalar subquery (parsing only)",
            "SELECT name, (SELECT MAX(salary) FROM employees) AS max_sal FROM employees",
        ),
        (
            "IN subquery (parsing only)",
            "SELECT name FROM employees WHERE dept_id IN (SELECT dept_id FROM departments)",
        ),
        (
            "EXISTS subquery (parsing only)",
            "SELECT dept_name FROM departments WHERE EXISTS (SELECT 1 FROM employees)",
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
                    println!("✓ Parsing succeeded!");
                    println!("AST: {:#?}", statement);

                    let mut planner = Planner::new();
                    planner.register_table("employees", employees_schema.clone());
                    planner.register_table("departments", departments_schema.clone());
                    // Register CTE names as virtual tables for planning
                    planner.register_table("eng", employees_schema.clone());
                    planner.register_table("high_salary", employees_schema.clone());

                    match planner.create_logical_plan(&statement) {
                        Ok(logical_plan) => {
                            println!("\n✓ Planning succeeded!");
                            println!("Logical Plan: {:#?}", logical_plan);

                            let optimizer = Optimizer::new();
                            match optimizer.optimize(&logical_plan) {
                                Ok(optimized) => {
                                    println!("\n✓ Optimization succeeded!");
                                    println!("Optimized Plan: {:#?}", optimized);
                                }
                                Err(e) => println!("\n✗ Optimization Error: {}", e),
                            }
                        }
                        Err(e) => println!("\n✗ Planning Error: {}", e),
                    }
                }
                Err(e) => println!("✗ Parse Error: {}", e),
            },
            Err(e) => println!("✗ Lexer Error: {}", e),
        }
    }

    println!("\n{}", "=".repeat(80));
    println!("Subqueries and CTEs Demo completed!");
    println!("\nNote: Full execution of scalar/IN/EXISTS subqueries requires additional");
    println!("async context integration which is scaffolded but not fully implemented.");

    Ok(())
}
