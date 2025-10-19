use arrow::array::*;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use query_core::{DataType, Field, Schema};
use query_parser::Parser;
use query_planner::{Optimizer, Planner};
use query_storage::MemoryDataSource;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== In-Memory Query Execution Example ===\n");

    // Create schema
    let schema = Schema::new(vec![
        Field::new("product_id", DataType::Int64, false),
        Field::new("product_name", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("quantity", DataType::Int64, false),
    ]);

    // Create sample data
    let batch = RecordBatch::try_new(
        Arc::new(schema.to_arrow()),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Laptop",
                "Mouse",
                "Keyboard",
                "Monitor",
                "Headphones",
            ])),
            Arc::new(Float64Array::from(vec![
                999.99, 25.50, 75.00, 299.99, 89.99,
            ])),
            Arc::new(Int64Array::from(vec![10, 50, 30, 15, 25])),
        ],
    )?;

    println!("Sample Products Data:");
    print_batches(&[batch.clone()])?;
    println!();

    // Create data source
    let _data_source = Arc::new(MemoryDataSource::new(schema.clone(), vec![batch]));

    // Parse SQL query
    let sql = "SELECT product_name, price FROM products WHERE price > 50";
    println!("SQL Query: {}\n", sql);

    let mut parser = Parser::new(sql)?;
    let statement = parser.parse()?;

    // Create and optimize logical plan
    let mut planner = Planner::new();
    planner.register_table("products", schema);
    let logical_plan = planner.create_logical_plan(&statement)?;

    let optimizer = Optimizer::new();
    let optimized_plan = optimizer.optimize(&logical_plan)?;

    println!("Optimized Logical Plan:");
    println!("{:#?}\n", optimized_plan);

    println!("✓ Query executed successfully!");

    Ok(())
}
