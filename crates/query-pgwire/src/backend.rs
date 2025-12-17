//! Query processing backend for PostgreSQL protocol

use crate::result::{record_batch_to_rows, schema_to_field_info};
use async_trait::async_trait;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{QueryResponse, Response};
use pgwire::api::{ClientInfo, NoopErrorHandler, PgWireServerHandlers};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use query_core::Schema;
use query_executor::QueryExecutor;
use query_executor::physical_plan::{AggregateExpr, DataSource, PhysicalExpr, PhysicalPlan};
use query_parser::Parser;
use query_planner::{LogicalExpr, LogicalPlan, Optimizer, Planner};
use query_storage::MemoryDataSource;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

/// Query backend that processes PostgreSQL protocol queries
pub struct QueryBackend {
    /// Registered tables with their schemas and data sources
    tables: Arc<RwLock<HashMap<String, TableEntry>>>,
}

pub struct TableEntry {
    pub schema: Schema,
    pub source: Arc<MemoryDataSource>,
}

/// Create a user error with proper ErrorInfo
fn user_error(message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "42000".to_string(),
        message,
    )))
}

impl QueryBackend {
    /// Create a new query backend
    pub fn new() -> Self {
        Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a new query backend with shared tables
    pub fn with_tables(tables: Arc<RwLock<HashMap<String, TableEntry>>>) -> Self {
        Self { tables }
    }

    /// Register a table with schema and data source
    pub async fn register_table(&self, name: &str, schema: Schema, source: Arc<MemoryDataSource>) {
        let mut tables = self.tables.write().await;
        tables.insert(name.to_string(), TableEntry { schema, source });
        info!("Registered table: {}", name);
    }

    /// Get a cloned reference to the tables map
    pub fn tables(&self) -> Arc<RwLock<HashMap<String, TableEntry>>> {
        Arc::clone(&self.tables)
    }

    /// Execute query synchronously (called from blocking thread)
    fn execute_query_sync(
        sql: &str,
        tables: HashMap<String, TableEntry>,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
        // Parse SQL
        let mut parser = Parser::new(sql).map_err(|e| format!("Parse error: {}", e))?;
        let statement = parser.parse().map_err(|e| format!("Parse error: {}", e))?;

        // Create planner and register tables
        let mut planner = Planner::new();
        for (name, entry) in tables.iter() {
            planner.register_table(name, entry.schema.clone());
        }

        // Create data sources map
        let data_sources: HashMap<String, Arc<dyn DataSource>> = tables
            .iter()
            .map(|(name, entry)| {
                (
                    name.clone(),
                    Arc::clone(&entry.source) as Arc<dyn DataSource>,
                )
            })
            .collect();

        // Create logical plan
        let logical_plan = planner
            .create_logical_plan(&statement)
            .map_err(|e| format!("Planning error: {}", e))?;

        // Optimize plan
        let optimizer = Optimizer::new();
        let optimized_plan = optimizer
            .optimize(&logical_plan)
            .map_err(|e| format!("Optimization error: {}", e))?;

        // Convert to physical plan
        let physical_plan = logical_to_physical_plan(&optimized_plan, &data_sources)?;

        // Execute query using tokio runtime
        let executor = QueryExecutor::new();
        let rt = tokio::runtime::Handle::current();
        rt.block_on(async { executor.execute(&physical_plan).await })
            .map_err(|e| format!("Execution error: {}", e))
    }

    /// Execute a SQL query and return results
    async fn execute_query(&self, sql: &str) -> PgWireResult<Vec<Response<'static>>> {
        debug!("Executing query: {}", sql);

        // Clone tables for the blocking task
        let tables_snapshot: HashMap<String, TableEntry> = {
            let tables = self.tables.read().await;
            tables
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        TableEntry {
                            schema: v.schema.clone(),
                            source: Arc::clone(&v.source),
                        },
                    )
                })
                .collect()
        };

        let sql_owned = sql.to_string();

        // Run query in blocking task to avoid Send issues
        let batches = tokio::task::spawn_blocking(move || {
            Self::execute_query_sync(&sql_owned, tables_snapshot)
        })
        .await
        .map_err(|e| user_error(format!("Task join error: {}", e)))?
        .map_err(|e| {
            error!("Query error: {}", e);
            user_error(e)
        })?;

        // Build response
        if batches.is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }

        // Get schema from first batch
        let schema = batches[0].schema();
        let field_info = schema_to_field_info(&schema);

        // Convert all batches to rows
        let mut all_rows = Vec::new();
        for batch in &batches {
            let rows = record_batch_to_rows(batch, &field_info)?;
            for row in rows {
                all_rows.push(row.finish());
            }
        }

        let row_count = all_rows.len();
        debug!("Query returned {} rows", row_count);

        Ok(vec![Response::Query(QueryResponse::new(
            Arc::new(field_info),
            futures::stream::iter(all_rows),
        ))])
    }
}

impl Default for QueryBackend {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert logical plan to physical plan
fn logical_to_physical_plan(
    logical_plan: &LogicalPlan,
    data_sources: &HashMap<String, Arc<dyn DataSource>>,
) -> Result<PhysicalPlan, String> {
    match logical_plan {
        LogicalPlan::TableScan {
            table_name, schema, ..
        } => {
            let source = data_sources
                .get(table_name)
                .ok_or_else(|| format!("Table not found: {}", table_name))?;
            Ok(PhysicalPlan::Scan {
                source: Arc::clone(source),
                schema: schema.clone(),
            })
        }
        LogicalPlan::Filter { input, predicate } => {
            let input_plan = logical_to_physical_plan(input, data_sources)?;
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
            let input_plan = logical_to_physical_plan(input, data_sources)?;
            let physical_exprs: Result<Vec<_>, _> =
                exprs.iter().map(logical_expr_to_physical).collect();
            Ok(PhysicalPlan::Projection {
                input: Arc::new(input_plan),
                exprs: physical_exprs?,
                schema: schema.clone(),
            })
        }
        LogicalPlan::Limit { input, skip, fetch } => {
            let input_plan = logical_to_physical_plan(input, data_sources)?;
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
            let input_plan = logical_to_physical_plan(input, data_sources)?;
            let physical_exprs: Result<Vec<_>, _> =
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
            let input_plan = logical_to_physical_plan(input, data_sources)?;
            let physical_group: Result<Vec<_>, _> =
                group_exprs.iter().map(logical_expr_to_physical).collect();
            let physical_aggr: Result<Vec<_>, _> = aggr_exprs
                .iter()
                .map(|expr| {
                    if let LogicalExpr::AggregateFunction { func, expr: inner } = expr {
                        Ok(AggregateExpr {
                            func: (*func).into(),
                            expr: logical_expr_to_physical(inner)?,
                        })
                    } else {
                        Err("Expected aggregate function".to_string())
                    }
                })
                .collect();
            Ok(PhysicalPlan::HashAggregate {
                input: Arc::new(input_plan),
                group_exprs: physical_group?,
                aggr_exprs: physical_aggr?,
            })
        }
        LogicalPlan::Join {
            left,
            right,
            join_type,
            on,
            ..
        } => {
            let left_plan = logical_to_physical_plan(left, data_sources)?;
            let right_plan = logical_to_physical_plan(right, data_sources)?;
            let physical_on = on
                .as_ref()
                .map(|expr| logical_expr_to_physical(expr))
                .transpose()?;
            Ok(PhysicalPlan::HashJoin {
                left: Arc::new(left_plan),
                right: Arc::new(right_plan),
                join_type: *join_type,
                on: physical_on,
            })
        }
        _ => Err(format!("Unsupported logical plan type: {:?}", logical_plan)),
    }
}

/// Convert logical expression to physical expression
fn logical_expr_to_physical(logical_expr: &LogicalExpr) -> Result<PhysicalExpr, String> {
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
        LogicalExpr::Alias { expr, .. } => logical_expr_to_physical(expr),
        LogicalExpr::AggregateFunction { func, .. } => {
            // For aggregate in expressions, create a placeholder column reference
            Ok(PhysicalExpr::Column {
                name: format!("{:?}(...)", func),
                index: 0,
            })
        }
        _ => Err(format!(
            "Unsupported logical expression type: {:?}",
            logical_expr
        )),
    }
}

#[async_trait]
impl SimpleQueryHandler for QueryBackend {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send,
    {
        // Handle multiple statements separated by semicolons
        let queries: Vec<&str> = query
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();

        let mut responses = Vec::new();

        for sql in queries {
            // Handle special commands
            let sql_upper = sql.to_uppercase();

            if sql_upper == "SHOW TABLES" || sql_upper == "\\DT" {
                // Return list of tables
                match self.handle_show_tables().await {
                    Ok(mut resp) => {
                        for r in resp.drain(..) {
                            responses.push(unsafe { std::mem::transmute(r) });
                        }
                    }
                    Err(e) => return Err(e),
                }
                continue;
            }

            if sql_upper.starts_with("DESCRIBE ") || sql_upper.starts_with("\\D ") {
                let table_name = sql.split_whitespace().nth(1).unwrap_or("");
                match self.handle_describe_table(table_name).await {
                    Ok(mut resp) => {
                        for r in resp.drain(..) {
                            responses.push(unsafe { std::mem::transmute(r) });
                        }
                    }
                    Err(e) => return Err(e),
                }
                continue;
            }

            match self.execute_query(sql).await {
                Ok(mut resp) => {
                    for r in resp.drain(..) {
                        // Safety: Response<'static> can be safely used as Response<'a>
                        responses.push(unsafe { std::mem::transmute(r) });
                    }
                }
                Err(e) => return Err(e),
            }
        }

        if responses.is_empty() {
            Ok(vec![Response::EmptyQuery])
        } else {
            Ok(responses)
        }
    }
}

impl QueryBackend {
    /// Handle SHOW TABLES command
    async fn handle_show_tables(&self) -> PgWireResult<Vec<Response<'static>>> {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use arrow::record_batch::RecordBatch;

        let tables = self.tables.read().await;
        let table_names: Vec<&str> = tables.keys().map(|s| s.as_str()).collect();

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "table_name",
            DataType::Utf8,
            false,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(table_names)) as arrow::array::ArrayRef],
        )
        .map_err(|e| user_error(format!("Error creating result: {}", e)))?;

        let field_info = schema_to_field_info(&schema);
        let rows = record_batch_to_rows(&batch, &field_info)?;
        let all_rows: Vec<_> = rows.into_iter().map(|r| r.finish()).collect();

        Ok(vec![Response::Query(QueryResponse::new(
            Arc::new(field_info),
            futures::stream::iter(all_rows),
        ))])
    }

    /// Handle DESCRIBE table command
    async fn handle_describe_table(
        &self,
        table_name: &str,
    ) -> PgWireResult<Vec<Response<'static>>> {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use arrow::record_batch::RecordBatch;

        let tables = self.tables.read().await;
        let entry = tables
            .get(table_name)
            .ok_or_else(|| user_error(format!("Table not found: {}", table_name)))?;

        let column_names: Vec<&str> = entry.schema.fields().iter().map(|f| f.name()).collect();
        let column_types: Vec<String> = entry
            .schema
            .fields()
            .iter()
            .map(|f| format!("{:?}", f.data_type()))
            .collect();

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("column_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(column_names)) as arrow::array::ArrayRef,
                Arc::new(StringArray::from(column_types)) as arrow::array::ArrayRef,
            ],
        )
        .map_err(|e| user_error(format!("Error creating result: {}", e)))?;

        let field_info = schema_to_field_info(&schema);
        let rows = record_batch_to_rows(&batch, &field_info)?;
        let all_rows: Vec<_> = rows.into_iter().map(|r| r.finish()).collect();

        Ok(vec![Response::Query(QueryResponse::new(
            Arc::new(field_info),
            futures::stream::iter(all_rows),
        ))])
    }
}

/// Simple startup handler that accepts all connections
pub struct SimpleStartupHandler;

impl NoopStartupHandler for SimpleStartupHandler {}

/// Server handlers for PostgreSQL protocol
pub struct QueryServerHandlers {
    startup_handler: Arc<SimpleStartupHandler>,
    simple_query_handler: Arc<QueryBackend>,
    extended_query_handler: Arc<PlaceholderExtendedQueryHandler>,
    copy_handler: Arc<NoopCopyHandler>,
    error_handler: Arc<NoopErrorHandler>,
}

impl QueryServerHandlers {
    pub fn new(backend: Arc<QueryBackend>) -> Self {
        Self {
            startup_handler: Arc::new(SimpleStartupHandler),
            simple_query_handler: backend,
            extended_query_handler: Arc::new(PlaceholderExtendedQueryHandler),
            copy_handler: Arc::new(NoopCopyHandler),
            error_handler: Arc::new(NoopErrorHandler),
        }
    }
}

impl PgWireServerHandlers for QueryServerHandlers {
    type StartupHandler = SimpleStartupHandler;
    type SimpleQueryHandler = QueryBackend;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;
    type ErrorHandler = NoopErrorHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        Arc::clone(&self.simple_query_handler)
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::clone(&self.extended_query_handler)
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        Arc::clone(&self.startup_handler)
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::clone(&self.copy_handler)
    }

    fn error_handler(&self) -> Arc<Self::ErrorHandler> {
        Arc::clone(&self.error_handler)
    }
}
