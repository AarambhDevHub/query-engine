use query_core::{DataType, Field, Schema};
use query_parser::Parser;
use query_planner::{Optimizer, Planner};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Window Functions Example ===\n");

    // Create sample schema for testing
    let employees_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("dept_id", DataType::Int64, false),
        Field::new("salary", DataType::Float64, false),
        Field::new("hire_date", DataType::Utf8, false),
    ]);

    println!("Employees Schema: {:?}\n", employees_schema);

    // Test queries demonstrating window functions
    let test_queries = vec![
        (
            "ROW_NUMBER() with ORDER BY",
            "SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) FROM employees",
        ),
        (
            "RANK() with PARTITION BY",
            "SELECT name, dept_id, salary, RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) FROM employees",
        ),
        (
            "DENSE_RANK()",
            "SELECT name, salary, DENSE_RANK() OVER (ORDER BY salary DESC) FROM employees",
        ),
        (
            "LAG() - previous row value",
            "SELECT name, salary, LAG(salary, 1) OVER (ORDER BY hire_date) FROM employees",
        ),
        (
            "LEAD() - next row value",
            "SELECT name, salary, LEAD(salary, 1) OVER (ORDER BY hire_date) FROM employees",
        ),
        (
            "FIRST_VALUE() in partition",
            "SELECT name, dept_id, FIRST_VALUE(name) OVER (PARTITION BY dept_id ORDER BY salary DESC) FROM employees",
        ),
        (
            "LAST_VALUE() in partition",
            "SELECT name, dept_id, LAST_VALUE(name) OVER (PARTITION BY dept_id ORDER BY salary DESC) FROM employees",
        ),
        (
            "Multiple window functions",
            "SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary), RANK() OVER (ORDER BY salary) FROM employees",
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
    println!("Window Functions Demo completed!");
    println!(
        "\nSupported functions: ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE"
    );
    println!("Supported clauses: PARTITION BY, ORDER BY, ROWS/RANGE BETWEEN");

    Ok(())
}
