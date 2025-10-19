use anyhow::Result;
use arrow::csv::Writer as CsvWriter;
use arrow::json::LineDelimitedWriter;
use arrow::record_batch::RecordBatch;
use colored::Colorize;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use query_core::{DataType, Field, Schema};
use query_parser::Parser;
use query_planner::{Optimizer, Planner};
use query_storage::{CsvDataSource, ParquetDataSource};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

pub async fn execute_query(
    sql: &str,
    table: &str,
    file: &Path,
    _output_format: &str,
) -> Result<()> {
    let start = Instant::now();

    println!("{} Executing query...", "→".bright_blue());

    if !file.exists() {
        anyhow::bail!("File not found: {:?}", file);
    }

    // Infer schema from file
    let schema = infer_schema_from_file(file)?;

    // Create data source - convert Path to String
    let file_path = file
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Invalid file path"))?
        .to_string();

    let _data_source: Arc<dyn query_executor::physical_plan::DataSource> =
        if file.extension().and_then(|s| s.to_str()) == Some("csv") {
            Arc::new(CsvDataSource::new(file_path, schema.clone()))
        } else if file.extension().and_then(|s| s.to_str()) == Some("parquet") {
            Arc::new(ParquetDataSource::new(file_path, schema.clone()))
        } else {
            anyhow::bail!("Unsupported file format. Use .csv or .parquet");
        };

    // Parse SQL
    let mut parser = Parser::new(sql)?;
    let statement = parser.parse()?;

    // Create and optimize plan
    let mut planner = Planner::new();
    planner.register_table(table, schema.clone());
    let logical_plan = planner.create_logical_plan(&statement)?;

    let optimizer = Optimizer::new();
    let optimized_plan = optimizer.optimize(&logical_plan)?;

    let elapsed = start.elapsed();

    println!("{} Query planned successfully!", "✓".bright_green());
    println!("Logical Plan:");
    println!("{:#?}", optimized_plan);
    println!();
    println!(
        "{} {:.2}ms",
        "Planning time:".bright_yellow(),
        elapsed.as_secs_f64() * 1000.0
    );

    Ok(())
}

pub async fn register_table(name: &str, file: &Path, file_type: &str) -> Result<()> {
    println!(
        "{} Registering table '{}' from {:?}",
        "→".bright_blue(),
        name.bright_cyan(),
        file
    );

    if !file.exists() {
        anyhow::bail!("File not found: {:?}", file);
    }

    // Verify file type
    let extension = file
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown");

    match file_type {
        "csv" if extension == "csv" => {
            let schema = infer_schema_from_file(file)?;
            println!("Schema inferred: {} columns", schema.fields().len());
        }
        "parquet" if extension == "parquet" => {
            let schema = infer_schema_from_file(file)?;
            println!("Schema inferred: {} columns", schema.fields().len());
        }
        _ => anyhow::bail!("File type mismatch or unsupported format"),
    }

    println!("{} Table registered successfully!", "✓".bright_green());
    println!("Use the REPL to query this table with:");
    println!("  {}", format!("SELECT * FROM {}", name).bright_cyan());

    Ok(())
}

pub async fn show_tables() -> Result<()> {
    println!("{}", "Registered Tables:".bright_yellow().bold());
    println!(
        "{}",
        "  Use the REPL to load and manage tables".bright_black()
    );
    println!("  Start REPL with: {}", "qe repl".bright_cyan());
    Ok(())
}

pub async fn describe_table(table: &str) -> Result<()> {
    println!(
        "{} {}",
        "Describing table:".bright_yellow(),
        table.bright_cyan()
    );
    println!("{}", "  Use the REPL to describe tables".bright_black());
    println!("  Start REPL with: {}", "qe repl".bright_cyan());
    println!(
        "  Then use: {}",
        format!(".describe {}", table).bright_cyan()
    );
    Ok(())
}

pub async fn run_benchmark(query_file: &Path, iterations: usize) -> Result<()> {
    println!(
        "{} Running benchmark with {} iterations...",
        "→".bright_blue(),
        iterations
    );

    if !query_file.exists() {
        anyhow::bail!("Query file not found: {:?}", query_file);
    }

    let sql = std::fs::read_to_string(query_file)?;
    println!("Query: {}\n", sql.bright_white());

    let mut total_time = std::time::Duration::ZERO;
    let mut parse_times = Vec::new();

    for i in 1..=iterations {
        let start = Instant::now();

        // Parse query
        let mut parser = Parser::new(&sql)?;
        let _statement = parser.parse()?;

        let elapsed = start.elapsed();
        total_time += elapsed;
        parse_times.push(elapsed.as_secs_f64());

        if i % 10 == 0 || i == iterations {
            print!("\r  Progress: {}/{} iterations", i, iterations);
            std::io::Write::flush(&mut std::io::stdout())?;
        }
    }

    println!("\n");

    // Calculate statistics
    let avg_time = total_time.as_secs_f64() / iterations as f64;
    let mut sorted_times = parse_times.clone();
    sorted_times.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let min_time = sorted_times.first().unwrap();
    let max_time = sorted_times.last().unwrap();
    let median_time = sorted_times[sorted_times.len() / 2];
    let p95_time = sorted_times[(sorted_times.len() as f64 * 0.95) as usize];
    let p99_time = sorted_times[(sorted_times.len() as f64 * 0.99) as usize];

    println!("{}", "Benchmark Results:".bright_green().bold());
    println!("{}", "─".repeat(50));
    println!("  Iterations:     {}", iterations);
    println!("  Total time:     {:.2}s", total_time.as_secs_f64());
    println!("  Average:        {:.2}ms", avg_time * 1000.0);
    println!("  Median:         {:.2}ms", median_time * 1000.0);
    println!("  Min:            {:.2}ms", min_time * 1000.0);
    println!("  Max:            {:.2}ms", max_time * 1000.0);
    println!("  95th percentile: {:.2}ms", p95_time * 1000.0);
    println!("  99th percentile: {:.2}ms", p99_time * 1000.0);
    println!("  QPS:            {:.2}", 1.0 / avg_time);
    println!("{}", "─".repeat(50));

    Ok(())
}

pub async fn export_results(
    sql: &str,
    table: &str,
    input: &Path,
    output: &Path,
    format: &str,
) -> Result<()> {
    println!(
        "{} Exporting results to {:?} ({})",
        "→".bright_blue(),
        output,
        format
    );

    if !input.exists() {
        anyhow::bail!("Input file not found: {:?}", input);
    }

    // Infer schema and create data source
    let schema = infer_schema_from_file(input)?;

    // Convert Path to String
    let input_path = input
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Invalid input file path"))?
        .to_string();

    let data_source: Arc<dyn query_executor::physical_plan::DataSource> =
        if input.extension().and_then(|s| s.to_str()) == Some("csv") {
            Arc::new(CsvDataSource::new(input_path, schema.clone()))
        } else {
            Arc::new(ParquetDataSource::new(input_path, schema.clone()))
        };

    // Parse and plan query
    let mut parser = Parser::new(sql)?;
    let statement = parser.parse()?;

    let mut planner = Planner::new();
    planner.register_table(table, schema.clone());
    let logical_plan = planner.create_logical_plan(&statement)?;

    let optimizer = Optimizer::new();
    let _optimized_plan = optimizer.optimize(&logical_plan)?;

    // Execute query (simplified - would need full physical execution)
    let batches = data_source.scan()?;

    // Export results
    match format {
        "csv" => export_to_csv(&batches, output)?,
        "parquet" => export_to_parquet(&batches, output)?,
        "json" => export_to_json(&batches, output)?,
        _ => anyhow::bail!("Unsupported export format: {}", format),
    }

    println!(
        "{} Results exported successfully! ({} rows)",
        "✓".bright_green(),
        batches.iter().map(|b| b.num_rows()).sum::<usize>()
    );

    Ok(())
}

fn infer_schema_from_file(file: &Path) -> Result<Schema> {
    let extension = file
        .extension()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow::anyhow!("No file extension"))?;

    match extension {
        "csv" => {
            let file_handle = File::open(file)?;
            let mut reader = csv::Reader::from_reader(file_handle);
            let headers = reader.headers()?;

            let fields: Vec<Field> = headers
                .iter()
                .map(|name| Field::new(name, DataType::Utf8, true))
                .collect();

            Ok(Schema::new(fields))
        }
        "parquet" => {
            let file_handle = File::open(file)?;
            let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                file_handle,
            )?;
            let arrow_schema = builder.schema();
            Ok(Schema::from_arrow(&arrow_schema))
        }
        _ => anyhow::bail!("Unsupported file format: {}", extension),
    }
}

fn export_to_csv(batches: &[RecordBatch], output: &Path) -> Result<()> {
    let file = File::create(output)?;
    let mut writer = CsvWriter::new(file);

    for batch in batches {
        writer.write(batch)?;
    }

    Ok(())
}

fn export_to_parquet(batches: &[RecordBatch], output: &Path) -> Result<()> {
    if batches.is_empty() {
        anyhow::bail!("No data to export");
    }

    let file = File::create(output)?;
    let schema = batches[0].schema();

    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;

    for batch in batches {
        writer.write(batch)?;
    }

    writer.close()?;
    Ok(())
}

fn export_to_json(batches: &[RecordBatch], output: &Path) -> Result<()> {
    let file = File::create(output)?;
    let mut writer = LineDelimitedWriter::new(file);

    for batch in batches {
        writer.write(batch)?;
    }

    writer.finish()?;
    Ok(())
}
