use anyhow::Result;
use clap::{Parser, Subcommand};
use colored::Colorize;
use std::path::PathBuf;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod commands;
mod config;
mod repl;

use commands::*;
use config::Config;
use repl::Repl;

#[derive(Parser)]
#[command(name = "qe")]
#[command(author, version, about = "Query Engine - High-performance SQL query engine", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Enable verbose logging
    #[arg(short, long, global = true)]
    verbose: bool,

    /// Configuration file path
    #[arg(short, long, global = true)]
    config: Option<PathBuf>,
}

#[derive(Subcommand)]
enum Commands {
    /// Start interactive REPL
    Repl {
        /// Database directory
        #[arg(short, long)]
        db_path: Option<PathBuf>,
    },

    /// Execute a SQL query
    Query {
        /// SQL query to execute
        #[arg(short, long)]
        sql: String,

        /// Table name to query
        #[arg(short, long)]
        table: String,

        /// Data source file (CSV or Parquet)
        #[arg(short, long)]
        file: PathBuf,

        /// Output format (table, json, csv)
        #[arg(short, long, default_value = "table")]
        output: String,
    },

    /// Register a data source
    Register {
        /// Table name
        #[arg(short, long)]
        name: String,

        /// File path (CSV or Parquet)
        #[arg(short, long)]
        file: PathBuf,

        /// File type (csv, parquet)
        #[arg(short = 't', long, default_value = "csv")]
        file_type: String,
    },

    /// Show registered tables
    Tables,

    /// Describe table schema
    Describe {
        /// Table name
        table: String,
    },

    /// Run benchmark
    Bench {
        /// SQL query file
        #[arg(short, long)]
        query_file: PathBuf,

        /// Number of iterations
        #[arg(short, long, default_value = "10")]
        iterations: usize,
    },

    /// Export query results
    Export {
        /// SQL query
        #[arg(short, long)]
        sql: String,

        /// Table name
        #[arg(short, long)]
        table: String,

        /// Input file
        #[arg(short, long)]
        input: PathBuf,

        /// Output file
        #[arg(short, long)]
        output: PathBuf,

        /// Output format (csv, parquet, json)
        #[arg(short = 'f', long, default_value = "csv")]
        format: String,
    },

    /// Start Arrow Flight server
    FlightServer {
        /// Port to listen on
        #[arg(short, long, default_value = "50051")]
        port: u16,

        /// Host to bind to
        #[arg(short = 'H', long, default_value = "0.0.0.0")]
        host: String,

        /// CSV files to load (format: name=path)
        #[arg(short, long)]
        load: Vec<String>,
    },

    /// Query a remote Flight server
    FlightQuery {
        /// Flight server URL
        #[arg(short, long)]
        connect: String,

        /// SQL query to execute
        #[arg(short, long)]
        sql: String,

        /// Output format (table, json, csv)
        #[arg(short, long, default_value = "table")]
        output: String,
    },

    /// Start PostgreSQL-compatible server
    PgServer {
        /// Port to listen on
        #[arg(short, long, default_value = "5432")]
        port: u16,

        /// Host to bind to
        #[arg(short = 'H', long, default_value = "0.0.0.0")]
        host: String,

        /// CSV files to load (format: name=path)
        #[arg(short, long)]
        load: Vec<String>,

        /// Username for authentication (enables MD5 password auth)
        #[arg(short, long)]
        user: Option<String>,

        /// Password for authentication (requires --user)
        #[arg(long)]
        password: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging
    init_logging(cli.verbose)?;

    // Load configuration
    let config = if let Some(config_path) = cli.config {
        Config::from_file(&config_path)?
    } else {
        Config::default()
    };

    // Print banner
    print_banner();

    // Execute command
    match cli.command {
        Some(Commands::Repl { db_path }) => {
            let mut repl = Repl::new(config, db_path)?;
            repl.run().await?;
        }
        Some(Commands::Query {
            sql,
            table,
            file,
            output,
        }) => {
            execute_query(&sql, &table, &file, &output).await?;
        }
        Some(Commands::Register {
            name,
            file,
            file_type,
        }) => {
            register_table(&name, &file, &file_type).await?;
        }
        Some(Commands::Tables) => {
            show_tables().await?;
        }
        Some(Commands::Describe { table }) => {
            describe_table(&table).await?;
        }
        Some(Commands::Bench {
            query_file,
            iterations,
        }) => {
            run_benchmark(&query_file, iterations).await?;
        }
        Some(Commands::Export {
            sql,
            table,
            input,
            output,
            format,
        }) => {
            export_results(&sql, &table, &input, &output, &format).await?;
        }
        Some(Commands::FlightServer { port, host, load }) => {
            start_flight_server(&host, port, &load).await?;
        }
        Some(Commands::FlightQuery {
            connect,
            sql,
            output,
        }) => {
            flight_query(&connect, &sql, &output).await?;
        }
        Some(Commands::PgServer {
            port,
            host,
            load,
            user,
            password,
        }) => {
            start_pg_server(&host, port, &load, user, password).await?;
        }
        None => {
            // Default: Start REPL
            let mut repl = Repl::new(config, None)?;
            repl.run().await?;
        }
    }

    Ok(())
}

fn init_logging(verbose: bool) -> Result<()> {
    let filter = if verbose {
        "query_cli=debug,query_core=debug,query_parser=debug,query_planner=debug,query_executor=debug"
    } else {
        "query_cli=info"
    };

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| filter.into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    Ok(())
}

fn print_banner() {
    println!(
        "{}",
        r#"
   ___                          _____            _
  / _ \ _   _  ___ _ __ _   _  | ____|_ __   __ _(_)_ __   ___
 | | | | | | |/ _ \ '__| | | | |  _| | '_ \ / _` | | '_ \ / _ \
 | |_| | |_| |  __/ |  | |_| | | |___| | | | (_| | | | | |  __/
  \__\_\\__,_|\___|_|   \__, | |_____|_| |_|\__, |_|_| |_|\___|
                        |___/               |___/
    "#
        .bright_cyan()
    );
    println!(
        "{}",
        "High-Performance SQL Query Engine v0.1.0".bright_yellow()
    );
    println!("{}", "Type 'help' for available commands\n".bright_black());
}
