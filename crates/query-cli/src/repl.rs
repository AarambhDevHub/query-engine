use crate::config::Config;
use anyhow::{Context, Result};
use colored::Colorize;
use comfy_table::{Cell, Color, Table as ComfyTable};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use query_core::{DataType, Field, Schema};
use query_executor::QueryExecutor;
use query_parser::Parser;
use query_planner::{Optimizer, Planner};
use query_storage::MemoryDataSource;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

pub struct Repl {
    config: Config,
    db_path: Option<PathBuf>,
    editor: DefaultEditor,
    planner: Planner,
    executor: QueryExecutor,
    tables: HashMap<String, TableInfo>,
    history_file: PathBuf,
}

struct TableInfo {
    schema: Schema,
    source: TableSource,
    row_count: Option<usize>,
}

enum TableSource {
    Csv(PathBuf),
    Parquet(PathBuf),
    Memory(Arc<MemoryDataSource>),
}

impl Repl {
    pub fn new(config: Config, db_path: Option<PathBuf>) -> Result<Self> {
        let history_file = Self::get_history_file()?;
        let mut editor = DefaultEditor::new()?;

        // Load history
        let _ = editor.load_history(&history_file);

        Ok(Self {
            config,
            db_path,
            editor,
            planner: Planner::new(),
            executor: QueryExecutor::new(),
            tables: HashMap::new(),
            history_file,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        println!("{}", "Interactive Query REPL".bright_green().bold());
        println!("{}", "Commands:".bright_yellow());
        println!("  {}  - Show available commands", ".help".bright_cyan());
        println!("  {}  - Exit the REPL", ".quit".bright_cyan());
        println!("  {} - List registered tables", ".tables".bright_cyan());
        println!(
            "  {} - Load CSV file",
            ".load csv <path> [name]".bright_cyan()
        );
        println!(
            "  {} - Load Parquet file",
            ".load parquet <path> [name]".bright_cyan()
        );
        println!();

        loop {
            let prompt = format!("{} ", "qe>".bright_green().bold());
            let readline = self.editor.readline(&prompt);

            match readline {
                Ok(line) => {
                    let line = line.trim();

                    if line.is_empty() {
                        continue;
                    }

                    self.editor.add_history_entry(line)?;

                    if let Err(e) = self.handle_input(line).await {
                        eprintln!("{} {}", "Error:".bright_red().bold(), e);
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    println!("{}", "^C".bright_yellow());
                    continue;
                }
                Err(ReadlineError::Eof) => {
                    println!("{}", "exit".bright_yellow());
                    break;
                }
                Err(err) => {
                    eprintln!("{} {:?}", "Error:".bright_red().bold(), err);
                    break;
                }
            }
        }

        // Save history
        self.editor.save_history(&self.history_file)?;

        println!("{}", "Goodbye!".bright_cyan());
        Ok(())
    }

    async fn handle_input(&mut self, input: &str) -> Result<()> {
        if input.starts_with('.') {
            self.handle_command(input).await
        } else {
            self.handle_sql(input).await
        }
    }

    async fn handle_command(&mut self, cmd: &str) -> Result<()> {
        let parts: Vec<&str> = cmd.split_whitespace().collect();

        match parts[0] {
            ".help" | ".h" => {
                self.show_help();
                Ok(())
            }
            ".quit" | ".q" | ".exit" => {
                std::process::exit(0);
            }
            ".tables" => {
                self.show_tables();
                Ok(())
            }
            ".load" => {
                if parts.len() < 3 {
                    anyhow::bail!("Usage: .load <csv|parquet> <file_path> [table_name]");
                }
                let file_type = parts[1];
                let file_path = parts[2];
                let table_name = parts.get(3).map(|s| s.to_string());
                self.load_file(file_type, file_path, table_name).await
            }
            ".describe" | ".desc" => {
                if parts.len() < 2 {
                    anyhow::bail!("Usage: .describe <table_name>");
                }
                self.describe_table(parts[1]);
                Ok(())
            }
            ".schema" => {
                if parts.len() < 2 {
                    anyhow::bail!("Usage: .schema <table_name>");
                }
                self.show_schema(parts[1]);
                Ok(())
            }
            ".clear" => {
                print!("\x1B[2J\x1B[1;1H");
                Ok(())
            }
            ".timing" => {
                self.config.show_timing = !self.config.show_timing;
                println!(
                    "Timing is now {}",
                    if self.config.show_timing {
                        "ON".bright_green()
                    } else {
                        "OFF".bright_red()
                    }
                );
                Ok(())
            }
            ".plan" => {
                self.config.show_plan = !self.config.show_plan;
                println!(
                    "Plan display is now {}",
                    if self.config.show_plan {
                        "ON".bright_green()
                    } else {
                        "OFF".bright_red()
                    }
                );
                Ok(())
            }
            ".format" => {
                if parts.len() < 2 {
                    println!("Current format: {:?}", self.config.output_format);
                    println!("Usage: .format <table|json|csv>");
                } else {
                    self.config.output_format = match parts[1] {
                        "table" => crate::config::OutputFormat::Table,
                        "json" => crate::config::OutputFormat::Json,
                        "csv" => crate::config::OutputFormat::Csv,
                        _ => anyhow::bail!("Unknown format. Use: table, json, or csv"),
                    };
                    println!("Output format set to: {}", parts[1].bright_cyan());
                }
                Ok(())
            }
            ".drop" => {
                if parts.len() < 2 {
                    anyhow::bail!("Usage: .drop <table_name>");
                }
                self.drop_table(parts[1]);
                Ok(())
            }
            _ => {
                anyhow::bail!(
                    "Unknown command: {}. Type .help for available commands",
                    parts[0]
                );
            }
        }
    }

    async fn handle_sql(&mut self, sql: &str) -> Result<()> {
        let start = Instant::now();

        // Parse SQL
        let mut parser = Parser::new(sql).context("Failed to create parser")?;
        let statement = parser.parse().context("Failed to parse SQL")?;

        if self.config.show_plan {
            println!("{}", "Parsed Statement:".bright_blue());
            println!("{:#?}\n", statement);
        }

        // Create logical plan
        let logical_plan = self
            .planner
            .create_logical_plan(&statement)
            .context("Failed to create logical plan")?;

        if self.config.show_plan {
            println!("{}", "Logical Plan:".bright_blue());
            println!("{:#?}\n", logical_plan);
        }

        // Optimize plan
        let optimizer = Optimizer::new();
        let optimized_plan = optimizer
            .optimize(&logical_plan)
            .context("Failed to optimize plan")?;

        if self.config.show_plan {
            println!("{}", "Optimized Plan:".bright_blue());
            println!("{:#?}\n", optimized_plan);
        }

        let elapsed = start.elapsed();

        println!(
            "{}",
            "✓ Query parsed and planned successfully!".bright_green()
        );

        if self.config.show_timing {
            println!(
                "{} {:.2}ms",
                "Planning time:".bright_yellow(),
                elapsed.as_secs_f64() * 1000.0
            );
        }

        Ok(())
    }

    async fn load_file(
        &mut self,
        file_type: &str,
        file_path: &str,
        table_name: Option<String>,
    ) -> Result<()> {
        let path = PathBuf::from(file_path);
        if !path.exists() {
            anyhow::bail!("File not found: {}", file_path);
        }

        println!("{} Loading file...", "→".bright_blue());

        let table_name = table_name.unwrap_or_else(|| {
            path.file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("table")
                .to_string()
        });

        let (schema, row_count) = self.infer_schema(&path, file_type)?;

        // Convert PathBuf to String for data sources
        let _path_str = path
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid file path"))?
            .to_string();

        let source = match file_type {
            "csv" => TableSource::Csv(path.clone()),
            "parquet" => TableSource::Parquet(path.clone()),
            _ => anyhow::bail!("Unsupported file type: {}", file_type),
        };

        self.planner.register_table(&table_name, schema.clone());
        self.tables.insert(
            table_name.clone(),
            TableInfo {
                schema,
                source,
                row_count: Some(row_count),
            },
        );

        println!(
            "{} Loaded table '{}' from {} ({} rows, {} columns)",
            "✓".bright_green(),
            table_name.bright_cyan(),
            file_path,
            row_count,
            self.tables[&table_name].schema.fields().len()
        );

        Ok(())
    }

    fn infer_schema(&self, path: &Path, file_type: &str) -> Result<(Schema, usize)> {
        match file_type {
            "csv" => self.infer_csv_schema(path),
            "parquet" => self.infer_parquet_schema(path),
            _ => anyhow::bail!("Unsupported file type"),
        }
    }

    fn infer_csv_schema(&self, path: &Path) -> Result<(Schema, usize)> {
        let file = File::open(path)?;

        // Read CSV to infer schema
        let mut reader = csv::Reader::from_reader(file);
        let headers = reader.headers()?.clone();

        // Count rows
        let row_count = reader.records().count() + 1; // +1 for header

        // Create fields (all as Utf8 for simplicity - proper inference would check values)
        let fields: Vec<Field> = headers
            .iter()
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect();

        let schema = Schema::new(fields);

        Ok((schema, row_count))
    }

    fn infer_parquet_schema(&self, path: &Path) -> Result<(Schema, usize)> {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

        let arrow_schema = builder.schema();
        let metadata = builder.metadata();

        // Count total rows
        let row_count = metadata
            .row_groups()
            .iter()
            .map(|rg| rg.num_rows() as usize)
            .sum();

        // Convert Arrow schema to our Schema
        let schema = Schema::from_arrow(&arrow_schema);

        Ok((schema, row_count))
    }

    fn show_help(&self) {
        let mut table = ComfyTable::new();
        table.set_header(vec![
            Cell::new("Command").fg(Color::Cyan),
            Cell::new("Description").fg(Color::Yellow),
        ]);

        let commands = vec![
            (".help, .h", "Show this help message"),
            (".quit, .q, .exit", "Exit the REPL"),
            (".tables", "List all registered tables"),
            (".load <type> <path> [name]", "Load CSV or Parquet file"),
            (".describe <table>", "Show table schema"),
            (".schema <table>", "Show CREATE TABLE statement"),
            (".drop <table>", "Remove a table from memory"),
            (".clear", "Clear the screen"),
            (".timing", "Toggle query timing display"),
            (".plan", "Toggle query plan display"),
            (".format <type>", "Set output format (table|json|csv)"),
        ];

        for (cmd, desc) in commands {
            table.add_row(vec![cmd, desc]);
        }

        println!("{}", table);
        println!();
        println!("{}", "SQL Commands:".bright_yellow().bold());
        println!("  SELECT, FROM, WHERE, GROUP BY, ORDER BY, LIMIT");
        println!();
        println!("{}", "Aggregate Functions:".bright_yellow().bold());
        println!("  COUNT, SUM, AVG, MIN, MAX");
        println!();
        println!("{}", "JOIN types:".bright_yellow().bold());
        println!("  INNER, LEFT, RIGHT, FULL OUTER, CROSS");
        println!("  Table aliases and qualified column names");
        println!();
    }

    fn show_tables(&self) {
        if self.tables.is_empty() {
            println!("{}", "No tables registered".bright_yellow());
            println!("Use {} to load a file", ".load csv <path>".bright_cyan());
            return;
        }

        let mut table = ComfyTable::new();
        table.set_header(vec![
            Cell::new("Table Name").fg(Color::Cyan),
            Cell::new("Type").fg(Color::Yellow),
            Cell::new("Columns").fg(Color::Green),
            Cell::new("Rows").fg(Color::Magenta),
        ]);

        for (name, info) in &self.tables {
            let source_type = match &info.source {
                TableSource::Csv(_) => "CSV",
                TableSource::Parquet(_) => "Parquet",
                TableSource::Memory(_) => "Memory",
            };

            let row_count = info
                .row_count
                .map(|c| c.to_string())
                .unwrap_or_else(|| "?".to_string());

            table.add_row(vec![
                name,
                source_type,
                &info.schema.fields().len().to_string(),
                &row_count,
            ]);
        }

        println!("{}", table);
    }

    fn describe_table(&self, table_name: &str) {
        if let Some(info) = self.tables.get(table_name) {
            println!(
                "\n{} {}",
                "Table:".bright_yellow().bold(),
                table_name.bright_cyan()
            );

            let source_info = match &info.source {
                TableSource::Csv(path) => format!("CSV file: {}", path.display()),
                TableSource::Parquet(path) => format!("Parquet file: {}", path.display()),
                TableSource::Memory(_) => "In-memory table".to_string(),
            };
            println!(
                "{} {}",
                "Source:".bright_yellow(),
                source_info.bright_black()
            );

            if let Some(rows) = info.row_count {
                println!(
                    "{} {}",
                    "Rows:".bright_yellow(),
                    rows.to_string().bright_white()
                );
            }

            println!();

            let mut table = ComfyTable::new();
            table.set_header(vec![
                Cell::new("Column").fg(Color::Cyan),
                Cell::new("Type").fg(Color::Yellow),
                Cell::new("Nullable").fg(Color::Green),
            ]);

            for field in info.schema.fields() {
                table.add_row(vec![
                    field.name(),
                    &format!("{:?}", field.data_type()),
                    if field.nullable() { "YES" } else { "NO" },
                ]);
            }

            println!("{}", table);
        } else {
            println!("{} Table '{}' not found", "✗".bright_red(), table_name);
            println!("Available tables:");
            for name in self.tables.keys() {
                println!("  - {}", name.bright_cyan());
            }
        }
    }

    fn show_schema(&self, table_name: &str) {
        if let Some(info) = self.tables.get(table_name) {
            println!();
            println!(
                "{} {}",
                "CREATE TABLE".bright_yellow().bold(),
                table_name.bright_cyan()
            );
            println!("(");

            for (i, field) in info.schema.fields().iter().enumerate() {
                let comma = if i < info.schema.fields().len() - 1 {
                    ","
                } else {
                    ""
                };

                let type_str = format!("{:?}", field.data_type());
                let nullable_str = if field.nullable() { "NULL" } else { "NOT NULL" };

                println!(
                    "  {} {} {}{}",
                    field.name().bright_white(),
                    type_str.bright_blue(),
                    nullable_str.bright_black(),
                    comma
                );
            }

            println!(");");
            println!();
        } else {
            println!("{} Table '{}' not found", "✗".bright_red(), table_name);
        }
    }

    fn drop_table(&mut self, table_name: &str) {
        if self.tables.remove(table_name).is_some() {
            println!(
                "{} Dropped table '{}'",
                "✓".bright_green(),
                table_name.bright_cyan()
            );
        } else {
            println!("{} Table '{}' not found", "✗".bright_red(), table_name);
        }
    }

    fn get_history_file() -> Result<PathBuf> {
        let home = home::home_dir().context("Could not find home directory")?;
        let history_dir = home.join(".query_engine");
        std::fs::create_dir_all(&history_dir)?;
        Ok(history_dir.join("history.txt"))
    }
}
