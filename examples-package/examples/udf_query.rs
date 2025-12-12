use query_core::{DataType, Field, Schema};
use query_parser::Parser;
use query_planner::{Optimizer, Planner};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== User-Defined Functions (UDFs) Example ===\n");

    // Create sample schema
    let users_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, false),
        Field::new("salary", DataType::Float64, false),
    ]);

    println!("Users Schema: {:?}\n", users_schema);

    // Test queries demonstrating scalar functions
    let test_queries = vec![
        ("UPPER() - Uppercase", "SELECT UPPER(name) FROM users"),
        ("LOWER() - Lowercase", "SELECT LOWER(name) FROM users"),
        (
            "LENGTH() - String length",
            "SELECT name, LENGTH(name) FROM users",
        ),
        (
            "CONCAT() - String concatenation",
            "SELECT CONCAT(name, email) FROM users",
        ),
        ("ABS() - Absolute value", "SELECT ABS(salary) FROM users"),
        (
            "ROUND() - Round to integer",
            "SELECT ROUND(salary) FROM users",
        ),
        (
            "CEIL() and FLOOR()",
            "SELECT CEIL(salary), FLOOR(salary) FROM users",
        ),
        ("SQRT() - Square root", "SELECT SQRT(salary) FROM users"),
        (
            "POWER() - Exponentiation",
            "SELECT POWER(salary, 2) FROM users",
        ),
        (
            "COALESCE() - Null handling",
            "SELECT COALESCE(name, email) FROM users",
        ),
        (
            "Multiple functions combined",
            "SELECT UPPER(name), LENGTH(email), ROUND(salary) FROM users",
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
                    planner.register_table("users", users_schema.clone());

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
    println!("UDFs Demo completed!");
    println!("\nSupported functions:");
    println!("  String: UPPER, LOWER, LENGTH, CONCAT, SUBSTRING, TRIM, REPLACE");
    println!("  Math:   ABS, CEIL, FLOOR, ROUND, SQRT, POWER");
    println!("  Null:   COALESCE, NULLIF");

    Ok(())
}
