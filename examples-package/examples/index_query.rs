//! Index Query Example
//!
//! This example demonstrates how to use indexes for faster query execution.
//!
//! Run with: cargo run --example index_query

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use query_core::{Field, Schema};
use query_index::{BTreeIndex, HashIndex, Index, IndexKey, IndexManager};
use std::sync::Arc;

fn main() -> anyhow::Result<()> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║           Query Engine - Index Support Demo                 ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    // Create sample data
    let _schema = Schema::new(vec![
        Field::new("id", query_core::DataType::Int64, false),
        Field::new("name", query_core::DataType::Utf8, false),
        Field::new("department", query_core::DataType::Utf8, false),
        Field::new("salary", query_core::DataType::Int64, false),
    ]);

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("name", ArrowDataType::Utf8, false),
        ArrowField::new("department", ArrowDataType::Utf8, false),
        ArrowField::new("salary", ArrowDataType::Int64, false),
    ]));

    let ids: Vec<i64> = (1..=1000).collect();
    let names: Vec<String> = (1..=1000).map(|i| format!("Employee_{}", i)).collect();
    let departments: Vec<String> = (1..=1000)
        .map(|i| {
            match i % 5 {
                0 => "Engineering",
                1 => "Sales",
                2 => "Marketing",
                3 => "HR",
                _ => "Finance",
            }
            .to_string()
        })
        .collect();
    let salaries: Vec<i64> = (1..=1000).map(|i| 50000 + (i * 100) % 50000).collect();

    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(StringArray::from(
                names.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                departments.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(salaries.clone())),
        ],
    )?;

    println!("✓ Created sample data: {} employees\n", batch.num_rows());

    // ═══════════════════════════════════════════════════════════════
    // Demo 1: B-Tree Index
    // ═══════════════════════════════════════════════════════════════
    println!("═══════════════════════════════════════════════════════════════");
    println!("                    B-Tree Index Demo                          ");
    println!("═══════════════════════════════════════════════════════════════\n");

    let btree_index = BTreeIndex::with_metadata(
        "idx_employees_id",
        "employees",
        vec!["id".to_string()],
        true, // unique
    );

    // Build the index
    let start = std::time::Instant::now();
    for (row_id, id) in ids.iter().enumerate() {
        btree_index.insert(IndexKey::from_i64(*id), row_id)?;
    }
    println!(
        "✓ Built B-Tree index on 'id' column in {:?}",
        start.elapsed()
    );
    println!("  Index entries: {}", btree_index.len());
    println!(
        "  Supports range queries: {}\n",
        btree_index.supports_range()
    );

    // Equality lookup
    let key = IndexKey::from_i64(500);
    let start = std::time::Instant::now();
    let result = btree_index.lookup(&key);
    println!("🔍 Lookup id=500:");
    println!("   Result: row_id={:?}", result.row_ids);
    println!("   Time: {:?}\n", start.elapsed());

    // Range scan
    let start_key = IndexKey::from_i64(100);
    let end_key = IndexKey::from_i64(110);
    let start = std::time::Instant::now();
    let range_result = btree_index.range_scan(Some(&start_key), Some(&end_key));
    println!("📊 Range scan id BETWEEN 100 AND 110:");
    println!("   Results: {:?}", range_result.row_ids);
    println!("   Time: {:?}\n", start.elapsed());

    // ═══════════════════════════════════════════════════════════════
    // Demo 2: Hash Index
    // ═══════════════════════════════════════════════════════════════
    println!("═══════════════════════════════════════════════════════════════");
    println!("                    Hash Index Demo                            ");
    println!("═══════════════════════════════════════════════════════════════\n");

    let hash_index = HashIndex::with_metadata(
        "idx_employees_dept",
        "employees",
        vec!["department".to_string()],
        false, // not unique
    );

    // Build the index
    let start = std::time::Instant::now();
    for (row_id, dept) in departments.iter().enumerate() {
        hash_index.insert(IndexKey::from_string(dept), row_id)?;
    }
    println!(
        "✓ Built Hash index on 'department' column in {:?}",
        start.elapsed()
    );
    println!("  Index entries: {}", hash_index.len());
    println!(
        "  Supports range queries: {}\n",
        hash_index.supports_range()
    );

    // Hash lookup
    let key = IndexKey::from_string("Engineering");
    let start = std::time::Instant::now();
    let result = hash_index.lookup(&key);
    println!("🔍 Lookup department='Engineering':");
    println!("   Results: {} rows found", result.row_ids.len());
    println!(
        "   First 10 row_ids: {:?}",
        &result.row_ids[..10.min(result.row_ids.len())]
    );
    println!("   Time: {:?}\n", start.elapsed());

    // ═══════════════════════════════════════════════════════════════
    // Demo 3: Index Manager
    // ═══════════════════════════════════════════════════════════════
    println!("═══════════════════════════════════════════════════════════════");
    println!("                   Index Manager Demo                          ");
    println!("═══════════════════════════════════════════════════════════════\n");

    let manager = IndexManager::new();

    // Create indexes using the manager
    let _idx1 =
        manager.create_btree_index("idx_salary", "employees", vec!["salary".to_string()], false)?;
    let _idx2 =
        manager.create_hash_index("idx_name", "employees", vec!["name".to_string()], true)?;

    println!("✓ Created indexes via IndexManager:");
    for metadata in manager.list_all_metadata() {
        println!(
            "  - {} on {}({}) [type: {:?}, unique: {}]",
            metadata.name,
            metadata.table_name,
            metadata.columns.join(", "),
            metadata.index_type,
            metadata.unique
        );
    }

    println!("\n📋 Indexes for 'employees' table:");
    for idx in manager.get_indexes_for_table("employees") {
        println!("  - {}", idx.metadata().name);
    }

    // Find index for column
    if let Some(idx) = manager.find_index_for_column("employees", "salary") {
        println!(
            "\n🔎 Found index for 'salary' column: {}",
            idx.metadata().name
        );
    }

    // Drop an index
    manager.drop_index("idx_name")?;
    println!(
        "\n✓ Dropped idx_name, remaining indexes: {:?}",
        manager.list_indexes()
    );

    // ═══════════════════════════════════════════════════════════════
    // Demo 4: SQL Parsing (CREATE INDEX / DROP INDEX)
    // ═══════════════════════════════════════════════════════════════
    println!("\n═══════════════════════════════════════════════════════════════");
    println!("                   SQL Parsing Demo                            ");
    println!("═══════════════════════════════════════════════════════════════\n");

    use query_parser::Parser;

    let sqls = vec![
        "CREATE INDEX idx_emp_name ON employees (name)",
        "CREATE UNIQUE INDEX idx_emp_id ON employees (id)",
        "CREATE INDEX idx_emp_dept_sal ON employees (department, salary) USING BTREE",
        "CREATE INDEX idx_emp_hash ON employees (email) USING HASH",
        "DROP INDEX idx_old",
        "DROP INDEX IF EXISTS idx_maybe_exists",
    ];

    for sql in sqls {
        let mut parser = Parser::new(sql)?;
        let statement = parser.parse()?;
        println!("SQL: {}", sql);
        println!("AST: {:?}\n", statement);
    }

    println!("═══════════════════════════════════════════════════════════════");
    println!("                   Demo Complete! 🎉                           ");
    println!("═══════════════════════════════════════════════════════════════\n");

    Ok(())
}
