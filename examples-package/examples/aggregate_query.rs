use arrow::array::*;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use query_core::{DataType, Field, Schema};
use query_parser::Parser;
use query_planner::Planner;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Aggregate Query Example ===\n");

    // Simple schema with just department and salary
    let schema = Schema::new(vec![
        Field::new("department", DataType::Utf8, false),
        Field::new("salary", DataType::Int64, false),
    ]);

    // Create matching data
    let batch = RecordBatch::try_new(
        Arc::new(schema.to_arrow()),
        vec![
            Arc::new(StringArray::from(vec![
                "Engineering",
                "Sales",
                "Engineering",
                "Sales",
                "HR",
                "Engineering",
                "HR",
            ])),
            Arc::new(Int64Array::from(vec![
                100000, 80000, 95000, 85000, 75000, 110000, 78000,
            ])),
        ],
    )?;

    println!("Sample Employee Data:");
    print_batches(&[batch])?;
    println!();

    // Test multiple queries
    let queries = vec![
        "SELECT department, SUM(salary) FROM employees GROUP BY department",
        "SELECT department, AVG(salary) FROM employees GROUP BY department",
        "SELECT department, COUNT(salary) FROM employees GROUP BY department",
        "SELECT department, MIN(salary), MAX(salary) FROM employees GROUP BY department",
    ];

    for sql in queries {
        println!("SQL Query: {}\n", sql);

        let mut parser = Parser::new(sql)?;
        let statement = parser.parse()?;

        let mut planner = Planner::new();
        planner.register_table("employees", schema.clone());
        let logical_plan = planner.create_logical_plan(&statement)?;

        println!("Logical Plan:");
        println!("{:#?}\n", logical_plan);
        println!("{}", "=".repeat(80));
        println!();
    }

    println!("✓ All aggregate queries parsed and planned successfully!");

    Ok(())
}
