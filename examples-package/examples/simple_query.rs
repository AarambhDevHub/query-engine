use arrow::array::*;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use query_core::{DataType, Field, Schema};
use query_parser::Parser;
use query_planner::Planner;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Simple Query Example ===\n");

    // Create sample data
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.to_arrow()),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "Diana", "Eve"])),
            Arc::new(Int64Array::from(vec![25, 30, 35, 28, 32])),
        ],
    )?;

    println!("Sample Data:");
    print_batches(&[batch.clone()])?;
    println!();

    // Parse SQL
    let sql = "SELECT name, age FROM users WHERE age > 28";
    println!("SQL Query: {}\n", sql);

    let mut parser = Parser::new(sql)?;
    let statement = parser.parse()?;
    println!("Parsed Statement: {:#?}\n", statement);

    // Create logical plan
    let mut planner = Planner::new();
    planner.register_table("users", schema);
    let logical_plan = planner.create_logical_plan(&statement)?;

    println!("Logical Plan:");
    println!("{:#?}\n", logical_plan);

    println!("✓ Query parsed and planned successfully!");

    Ok(())
}
