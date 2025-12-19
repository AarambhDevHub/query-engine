use anyhow::Result;
use arrow::csv::Writer as CsvWriter;
use arrow::json::LineDelimitedWriter;
use arrow::record_batch::RecordBatch;
use colored::Colorize;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use query_core::{DataType, Field, Schema};
use query_executor::physical_plan::DataSource;
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

    // Parse SQL query
    let mut parser = Parser::new(sql)?;
    let statement = parser.parse()?;

    // Create logical plan
    let mut planner = Planner::new();
    planner.register_table(table, schema.clone());
    let logical_plan = planner.create_logical_plan(&statement)?;

    // Optimize logical plan
    let optimizer = Optimizer::new();
    let optimized_plan = optimizer.optimize(&logical_plan)?;

    // Convert logical plan to physical plan
    let physical_plan = logical_to_physical_plan(&optimized_plan, data_source.clone())?;

    // Execute the physical plan with QueryExecutor
    let executor = query_executor::QueryExecutor::new();
    let batches = executor.execute(&physical_plan).await?;

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

// ✅ NEW: Helper function to convert logical plan to physical plan
fn logical_to_physical_plan(
    logical_plan: &query_planner::LogicalPlan,
    data_source: Arc<dyn query_executor::physical_plan::DataSource>,
) -> Result<query_executor::physical_plan::PhysicalPlan> {
    use query_executor::physical_plan::PhysicalPlan;
    use query_planner::{LogicalExpr, LogicalPlan};

    match logical_plan {
        LogicalPlan::TableScan { schema, .. } => Ok(PhysicalPlan::Scan {
            source: data_source.clone(),
            schema: schema.clone(),
        }),
        LogicalPlan::Filter { input, predicate } => {
            let input_plan = logical_to_physical_plan(input, data_source)?;
            let physical_predicate = logical_expr_to_physical(predicate)?;

            Ok(PhysicalPlan::Filter {
                input: Arc::new(input_plan),
                predicate: physical_predicate,
            })
        }
        LogicalPlan::Projection {
            input,
            exprs,
            schema,
        } => {
            let input_plan = logical_to_physical_plan(input, data_source)?;
            let physical_exprs: Result<Vec<_>> =
                exprs.iter().map(logical_expr_to_physical).collect();

            Ok(PhysicalPlan::Projection {
                input: Arc::new(input_plan),
                exprs: physical_exprs?,
                schema: schema.clone(),
            })
        }
        LogicalPlan::Limit { input, skip, fetch } => {
            let input_plan = logical_to_physical_plan(input, data_source)?;

            Ok(PhysicalPlan::Limit {
                input: Arc::new(input_plan),
                skip: *skip,
                fetch: *fetch,
            })
        }
        LogicalPlan::Sort {
            input,
            exprs,
            ascending,
        } => {
            let input_plan = logical_to_physical_plan(input, data_source)?;
            let physical_exprs: Result<Vec<_>> =
                exprs.iter().map(logical_expr_to_physical).collect();

            Ok(PhysicalPlan::Sort {
                input: Arc::new(input_plan),
                exprs: physical_exprs?,
                ascending: ascending.clone(),
            })
        }
        LogicalPlan::Aggregate {
            input,
            group_exprs,
            aggr_exprs,
            ..
        } => {
            let input_plan = logical_to_physical_plan(input, data_source)?;
            let physical_group: Result<Vec<_>> =
                group_exprs.iter().map(logical_expr_to_physical).collect();

            let physical_aggr: Result<Vec<_>> = aggr_exprs
                .iter()
                .map(|expr| {
                    if let LogicalExpr::AggregateFunction { func, expr: inner } = expr {
                        Ok(query_executor::physical_plan::AggregateExpr {
                            func: (*func).into(),
                            expr: logical_expr_to_physical(inner)?,
                        })
                    } else {
                        anyhow::bail!("Expected aggregate function")
                    }
                })
                .collect();

            Ok(PhysicalPlan::HashAggregate {
                input: Arc::new(input_plan),
                group_exprs: physical_group?,
                aggr_exprs: physical_aggr?,
            })
        }
        _ => anyhow::bail!("Unsupported logical plan type for export"),
    }
}

// ✅ NEW: Helper function to convert logical expressions to physical expressions
fn logical_expr_to_physical(
    logical_expr: &query_planner::LogicalExpr,
) -> Result<query_executor::physical_plan::PhysicalExpr> {
    use query_executor::physical_plan::PhysicalExpr;
    use query_planner::LogicalExpr;

    match logical_expr {
        LogicalExpr::Column { name, index } => Ok(PhysicalExpr::Column {
            name: name.clone(),
            index: *index,
        }),
        LogicalExpr::Literal(val) => Ok(PhysicalExpr::Literal(val.clone())),
        LogicalExpr::BinaryExpr { left, op, right } => Ok(PhysicalExpr::BinaryExpr {
            left: Box::new(logical_expr_to_physical(left)?),
            op: (*op).into(),
            right: Box::new(logical_expr_to_physical(right)?),
        }),
        LogicalExpr::UnaryExpr { op, expr } => Ok(PhysicalExpr::UnaryExpr {
            op: (*op).into(),
            expr: Box::new(logical_expr_to_physical(expr)?),
        }),
        LogicalExpr::Alias { expr, .. } => {
            // For aliases, just convert the inner expression
            logical_expr_to_physical(expr)
        }
        _ => anyhow::bail!("Unsupported logical expression type: {:?}", logical_expr),
    }
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
            let headers = reader.headers()?.clone();

            // ✅ Sample first 1000 rows to infer types
            let mut sample_records: Vec<csv::StringRecord> = Vec::new();
            for result in reader.records().take(1000) {
                if let Ok(record) = result {
                    sample_records.push(record);
                }
            }

            // ✅ Infer data type for each column
            let fields: Vec<Field> = headers
                .iter()
                .enumerate()
                .map(|(col_idx, name)| {
                    let inferred_type = infer_column_type_for_file(&sample_records, col_idx);
                    Field::new(name, inferred_type, true)
                })
                .collect();

            Ok(Schema::new(fields))
        }
        "parquet" => {
            let file_handle = File::open(file)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file_handle)?;
            let arrow_schema = builder.schema();
            Ok(Schema::from_arrow(&arrow_schema))
        }
        _ => anyhow::bail!("Unsupported file format: {}", extension),
    }
}

// ✅ NEW: Helper function for file schema inference
fn infer_column_type_for_file(sample_records: &[csv::StringRecord], col_idx: usize) -> DataType {
    use DataType::*;

    let mut has_float = false;
    let mut has_int = false;
    let mut all_bool = true;
    let mut all_numeric = true;
    let mut non_empty_count = 0;

    for record in sample_records {
        if let Some(value) = record.get(col_idx) {
            let trimmed = value.trim();

            // Skip empty values
            if trimmed.is_empty() {
                continue;
            }

            non_empty_count += 1;

            // Check if boolean
            if !matches!(
                trimmed.to_lowercase().as_str(),
                "true" | "false" | "t" | "f" | "yes" | "no" | "1" | "0"
            ) {
                all_bool = false;
            }

            // Try to parse as number
            if trimmed.parse::<i64>().is_ok() {
                has_int = true;
            } else if trimmed.parse::<f64>().is_ok() {
                has_float = true;
                has_int = false; // Has decimal point
            } else {
                all_numeric = false;
            }
        }
    }

    // Determine the most appropriate type
    if non_empty_count == 0 {
        return Utf8; // Default for empty columns
    }

    if all_numeric {
        if has_float {
            Float64
        } else if has_int {
            Int64
        } else {
            Utf8
        }
    } else if all_bool && non_empty_count > 2 {
        Boolean
    } else {
        Utf8
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

/// Start an Arrow Flight server
pub async fn start_flight_server(host: &str, port: u16, load_files: &[String]) -> Result<()> {
    use query_flight::FlightServer;
    use query_storage::CsvDataSource;
    use std::net::SocketAddr;

    println!(
        "{} Starting Arrow Flight server on {}:{}",
        "→".bright_blue(),
        host.bright_cyan(),
        port.to_string().bright_cyan()
    );

    let server = FlightServer::new();

    // Load any specified CSV files
    for spec in load_files {
        if let Some((name, path)) = spec.split_once('=') {
            let path = Path::new(path);
            if path.exists() {
                let schema = infer_schema_from_file(path)?;
                let csv_source =
                    CsvDataSource::new(path.to_str().unwrap().to_string(), schema.clone());
                let batches = csv_source.scan()?;
                server.service().register_table(name, schema, batches);
                println!(
                    "  {} Loaded table '{}' from {:?}",
                    "✓".bright_green(),
                    name.bright_cyan(),
                    path
                );
            } else {
                println!("  {} File not found: {:?}", "⚠".bright_yellow(), path);
            }
        } else {
            println!(
                "  {} Invalid format '{}' (expected: name=path)",
                "⚠".bright_yellow(),
                spec
            );
        }
    }

    let addr: SocketAddr = format!("{}:{}", host, port).parse()?;

    println!("{} Server ready, press Ctrl+C to stop", "✓".bright_green());
    println!();
    println!("Connect with:");
    println!(
        "  {}",
        format!(
            "qe flight-query --connect http://{}:{} --sql \"SELECT * FROM table\"",
            host, port
        )
        .bright_cyan()
    );

    server.serve(addr).await?;

    Ok(())
}

/// Query a remote Flight server
pub async fn flight_query(connect: &str, sql: &str, output_format: &str) -> Result<()> {
    use arrow::util::pretty::print_batches;
    use query_flight::FlightClient;

    println!(
        "{} Connecting to Flight server at {}",
        "→".bright_blue(),
        connect.bright_cyan()
    );

    let mut client = FlightClient::connect(connect).await?;

    println!("{} Connected!", "✓".bright_green());
    println!("{} Executing: {}", "→".bright_blue(), sql.bright_white());

    let start = std::time::Instant::now();
    let batches = client.execute_sql(sql).await?;
    let elapsed = start.elapsed();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    match output_format {
        "json" => {
            for batch in &batches {
                let file = std::io::stdout();
                let mut writer = arrow::json::LineDelimitedWriter::new(file);
                writer.write(batch)?;
                writer.finish()?;
            }
        }
        "csv" => {
            for batch in &batches {
                let file = std::io::stdout();
                let mut writer = arrow::csv::Writer::new(file);
                writer.write(batch)?;
            }
        }
        _ => {
            // Default: table format
            print_batches(&batches)?;
        }
    }

    println!();
    println!(
        "{} {} rows returned in {:.2}ms",
        "✓".bright_green(),
        total_rows,
        elapsed.as_secs_f64() * 1000.0
    );

    Ok(())
}

/// Start a PostgreSQL-compatible server
pub async fn start_pg_server(
    host: &str,
    port: u16,
    load_files: &[String],
    user: Option<String>,
    password: Option<String>,
) -> Result<()> {
    use query_pgwire::PgServer;
    use std::path::Path;

    println!(
        "{} Starting PostgreSQL server on {}:{}",
        "→".bright_blue(),
        host.bright_cyan(),
        port.to_string().bright_cyan()
    );

    // Create server with or without authentication
    let server = match (&user, &password) {
        (Some(u), Some(p)) => {
            println!(
                "  {} Authentication enabled for user '{}'",
                "🔒".bright_yellow(),
                u.bright_cyan()
            );
            PgServer::new(host, port).with_auth(u, p)
        }
        (Some(u), None) => {
            println!(
                "  {} Warning: No password provided for user '{}', authentication disabled",
                "⚠".bright_yellow(),
                u
            );
            PgServer::new(host, port)
        }
        _ => {
            println!(
                "  {} Authentication disabled (no --user specified)",
                "⚠".bright_yellow()
            );
            PgServer::new(host, port)
        }
    };

    // Load any specified CSV files
    for spec in load_files {
        if let Some((name, path)) = spec.split_once('=') {
            let path_obj = Path::new(path);
            if path_obj.exists() {
                server.load_csv(path, name).await?;
                println!(
                    "  {} Loaded table '{}' from {:?}",
                    "✓".bright_green(),
                    name.bright_cyan(),
                    path
                );
            } else {
                println!("  {} File not found: {:?}", "⚠".bright_yellow(), path);
            }
        } else {
            println!(
                "  {} Invalid format '{}' (expected: name=path)",
                "⚠".bright_yellow(),
                spec
            );
        }
    }

    println!("{} Server ready, press Ctrl+C to stop", "✓".bright_green());
    println!();
    println!("Connect with:");
    if user.is_some() {
        println!(
            "  {}",
            format!(
                "psql -h {} -p {} -U {} -d query_engine",
                host,
                port,
                user.as_deref().unwrap_or("postgres")
            )
            .bright_cyan()
        );
        println!("  (You will be prompted for password)");
    } else {
        println!(
            "  {}",
            format!("psql -h {} -p {} -U postgres -d query_engine", host, port).bright_cyan()
        );
    }

    server.start().await?;

    Ok(())
}
