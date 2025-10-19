
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use query_core::{DataType, Field, Schema};
use query_parser::Parser;
use query_planner::{Optimizer, Planner};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Full Query Engine Demo ===\n");

    // Create a realistic sales dataset
    let schema = Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_name", DataType::Utf8, false),
        Field::new("product", DataType::Utf8, false),
        Field::new("quantity", DataType::Int64, false),
        Field::new("price", DataType::Float64, false),
        Field::new("region", DataType::Utf8, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.to_arrow()),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])),
            Arc::new(StringArray::from(vec![
                "Alice", "Bob", "Charlie", "Diana", "Eve",
                "Frank", "Grace", "Henry", "Iris", "Jack",
            ])),
            Arc::new(StringArray::from(vec![
                "Laptop", "Mouse", "Keyboard", "Monitor", "Headphones",
                "Laptop", "Mouse", "Monitor", "Keyboard", "Headphones",
            ])),
            Arc::new(Int64Array::from(vec![1, 5, 2, 1, 3, 2, 10, 1, 3, 2])),
            Arc::new(Float64Array::from(vec![
                999.99, 25.50, 75.00, 299.99, 89.99,
                999.99, 25.50, 299.99, 75.00, 89.99,
            ])),
            Arc::new(StringArray::from(vec![
                "East", "West", "East", "West", "East",
                "West", "East", "West", "East", "West",
            ])),
        ],
    )?;

    println!("Sales Data:");
    print_batches(&[batch])?;
    println!();

    // Test various SQL queries
    let test_queries = vec![
        ("Simple SELECT", "SELECT customer_name, product, price FROM orders"),
        ("WHERE clause", "SELECT customer_name, product FROM orders WHERE price > 100"),
        ("Aggregate", "SELECT region, SUM(quantity) FROM orders GROUP BY region"),
        ("COUNT", "SELECT product, COUNT(order_id) FROM orders GROUP BY product"),
        ("Multiple aggregates", "SELECT region, COUNT(order_id), SUM(quantity), AVG(price) FROM orders GROUP BY region"),
        ("ORDER BY", "SELECT customer_name, price FROM orders ORDER BY price DESC"),
        ("LIMIT", "SELECT customer_name, product FROM orders LIMIT 5"),
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
                    planner.register_table("orders", schema.clone());

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
    println!("Demo completed!");

    Ok(())
}
