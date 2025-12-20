//! Query processing backend for PostgreSQL protocol

use crate::auth::{AuthConfig, create_md5_auth_handler};
use crate::cursor::CursorManager;
use crate::extended::QueryExtendedHandler;
use crate::result::{record_batch_to_rows, schema_to_field_info};
use async_trait::async_trait;
use pgwire::api::auth::DefaultServerParameterProvider;
use pgwire::api::auth::md5pass::Md5PasswordAuthStartupHandler;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::SimpleQueryHandler;
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
    /// Cursor manager for server-side cursors
    cursors: CursorManager,
}

#[derive(Clone)]
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

/// Build a RETURNING response from a batch and select items
fn build_returning_response(
    batch: &arrow::record_batch::RecordBatch,
    returning: &[query_parser::SelectItem],
) -> PgWireResult<Response<'static>> {
    use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;

    // Check if we have RETURNING *
    let return_all = returning
        .iter()
        .any(|item| matches!(item, query_parser::SelectItem::Wildcard));

    if return_all {
        // Return all columns
        let field_info = crate::result::schema_to_field_info(&batch.schema());
        let rows = crate::result::record_batch_to_rows(batch, &field_info)?;
        let all_rows: Vec<_> = rows.into_iter().map(|r| r.finish()).collect();
        return Ok(Response::Query(pgwire::api::results::QueryResponse::new(
            Arc::new(field_info),
            futures::stream::iter(all_rows),
        )));
    }

    // Get specific columns
    let mut selected_fields: Vec<Field> = Vec::new();
    let mut selected_columns: Vec<arrow::array::ArrayRef> = Vec::new();

    let schema = batch.schema();
    for item in returning {
        match item {
            query_parser::SelectItem::UnnamedExpr(query_parser::Expr::Column(col_name)) => {
                // Find column in batch
                if let Ok(col_idx) = schema.index_of(col_name) {
                    selected_fields.push(schema.field(col_idx).clone());
                    selected_columns.push(batch.column(col_idx).clone());
                }
            }
            query_parser::SelectItem::ExprWithAlias { expr, alias } => {
                if let query_parser::Expr::Column(col_name) = expr {
                    if let Ok(col_idx) = schema.index_of(col_name) {
                        let field = schema.field(col_idx);
                        selected_fields.push(Field::new(
                            alias,
                            field.data_type().clone(),
                            field.is_nullable(),
                        ));
                        selected_columns.push(batch.column(col_idx).clone());
                    }
                }
            }
            _ => {}
        }
    }

    if selected_columns.is_empty() {
        // Fallback to all columns if no specific columns matched
        let field_info = crate::result::schema_to_field_info(&batch.schema());
        let rows = crate::result::record_batch_to_rows(batch, &field_info)?;
        let all_rows: Vec<_> = rows.into_iter().map(|r| r.finish()).collect();
        return Ok(Response::Query(pgwire::api::results::QueryResponse::new(
            Arc::new(field_info),
            futures::stream::iter(all_rows),
        )));
    }

    let result_schema = Arc::new(ArrowSchema::new(selected_fields));
    let result_batch = RecordBatch::try_new(result_schema.clone(), selected_columns)
        .map_err(|e| user_error(format!("Failed to build RETURNING result: {}", e)))?;

    let field_info = crate::result::schema_to_field_info(&result_schema);
    let rows = crate::result::record_batch_to_rows(&result_batch, &field_info)?;
    let all_rows: Vec<_> = rows.into_iter().map(|r| r.finish()).collect();
    Ok(Response::Query(pgwire::api::results::QueryResponse::new(
        Arc::new(field_info),
        futures::stream::iter(all_rows),
    )))
}

impl QueryBackend {
    /// Create a new query backend
    pub fn new() -> Self {
        Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
            cursors: CursorManager::new(),
        }
    }

    /// Create a new query backend with shared tables
    pub fn with_tables(tables: Arc<RwLock<HashMap<String, TableEntry>>>) -> Self {
        Self {
            tables,
            cursors: CursorManager::new(),
        }
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
        use arrow::compute::concat_batches;
        use query_parser::{SetOperation, Statement};

        // Parse SQL
        let mut parser = Parser::new(sql).map_err(|e| format!("Parse error: {}", e))?;
        let statement = parser.parse().map_err(|e| format!("Parse error: {}", e))?;

        // Check for recursive CTE
        if let Statement::WithSelect {
            ref with,
            ref select,
        } = statement
        {
            if with.recursive && !with.ctes.is_empty() {
                return Self::execute_recursive_cte_sync(&with.ctes[0], select, tables);
            }
        }

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

    /// Execute a recursive CTE with fixed-point iteration
    fn execute_recursive_cte_sync(
        cte: &query_parser::CteDefinition,
        main_select: &query_parser::SelectStatement,
        mut tables: HashMap<String, TableEntry>,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
        use arrow::compute::concat_batches;
        use query_parser::SetOperation;

        let cte_name = &cte.name;
        let cte_query = &cte.query;

        // Check if the CTE has a UNION clause (required for recursion)
        if cte_query.union_clause.is_none() {
            return Err("Recursive CTE must have UNION or UNION ALL".to_string());
        }

        let union_clause = cte_query.union_clause.as_ref().unwrap();

        // Base query is the left part (before UNION ALL)
        // Recursive query is the right part (after UNION ALL)
        let base_select = query_parser::SelectStatement {
            distinct_on: None,
            projection: cte_query.projection.clone(),
            from: cte_query.from.clone(),
            joins: cte_query.joins.clone(),
            selection: cte_query.selection.clone(),
            group_by: cte_query.group_by.clone(),
            having: cte_query.having.clone(),
            order_by: vec![],
            limit: None,
            offset: None,
            union_clause: None,
        };

        // Execute base query
        let base_sql = format!(
            "SELECT {} {}",
            Self::projection_to_sql(&base_select.projection),
            Self::from_to_sql(&base_select.from, &base_select.selection)
        );

        // For a simple recursive CTE like SELECT 1 AS n, we handle it directly
        let mut working_table: Vec<arrow::record_batch::RecordBatch> = Vec::new();

        // Check if base query is a simple literal (SELECT 1 AS n)
        if base_select.from.is_none() && base_select.projection.len() == 1 {
            // Simple literal base case
            if let Some(batch) = Self::execute_literal_select(&base_select) {
                working_table.push(batch);
            }
        } else {
            // Execute base query through normal path
            let base_result = Self::execute_query_sync(&base_sql, tables.clone())?;
            working_table.extend(base_result);
        }

        if working_table.is_empty() {
            return Err("Recursive CTE base query returned no rows".to_string());
        }

        // Get schema from first batch
        let schema = working_table[0].schema();

        // Register CTE as a temporary table for recursive queries
        let cte_source = Arc::new(MemoryDataSource::new(
            query_core::Schema::from_arrow(&schema),
            working_table.clone(),
        ));
        let cte_entry = TableEntry {
            schema: query_core::Schema::from_arrow(&schema),
            source: cte_source,
        };
        tables.insert(cte_name.clone(), cte_entry);

        // Fixed-point iteration
        let max_iterations = 1000; // Safety limit
        let mut iteration = 0;
        let mut all_results: Vec<arrow::record_batch::RecordBatch> = working_table.clone();

        loop {
            iteration += 1;
            if iteration > max_iterations {
                return Err(format!(
                    "Recursive CTE exceeded {} iterations",
                    max_iterations
                ));
            }

            // Build recursive query SQL
            let recursive_select = &union_clause.select;
            let recursive_sql = format!(
                "SELECT {} {}",
                Self::projection_to_sql(&recursive_select.projection),
                Self::from_to_sql(&recursive_select.from, &recursive_select.selection)
            );

            // Update CTE table with current working table
            let cte_source = Arc::new(MemoryDataSource::new(
                query_core::Schema::from_arrow(&schema),
                working_table.clone(),
            ));
            tables.insert(
                cte_name.clone(),
                TableEntry {
                    schema: query_core::Schema::from_arrow(&schema),
                    source: cte_source,
                },
            );

            // Execute recursive query
            let new_rows = Self::execute_simple_query_sync(&recursive_sql, &tables)?;

            if new_rows.is_empty() {
                // Fixed point reached - no new rows
                break;
            }

            let new_row_count: usize = new_rows.iter().map(|b| b.num_rows()).sum();
            if new_row_count == 0 {
                break;
            }

            // Add new rows to results
            all_results.extend(new_rows.clone());
            working_table = new_rows;
        }

        // Now execute the main SELECT against the complete CTE result
        let final_cte_source = Arc::new(MemoryDataSource::new(
            query_core::Schema::from_arrow(&schema),
            all_results.clone(),
        ));
        tables.insert(
            cte_name.clone(),
            TableEntry {
                schema: query_core::Schema::from_arrow(&schema),
                source: final_cte_source,
            },
        );

        // Execute main select
        let main_sql = format!(
            "SELECT {} {}",
            Self::projection_to_sql(&main_select.projection),
            Self::from_to_sql(&main_select.from, &main_select.selection)
        );

        Self::execute_simple_query_sync(&main_sql, &tables)
    }

    /// Execute a simple query without recursive CTE handling
    fn execute_simple_query_sync(
        sql: &str,
        tables: &HashMap<String, TableEntry>,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
        let mut parser = Parser::new(sql).map_err(|e| format!("Parse error: {}", e))?;
        let statement = parser.parse().map_err(|e| format!("Parse error: {}", e))?;

        let mut planner = Planner::new();
        for (name, entry) in tables.iter() {
            planner.register_table(name, entry.schema.clone());
        }

        let data_sources: HashMap<String, Arc<dyn DataSource>> = tables
            .iter()
            .map(|(name, entry)| {
                (
                    name.clone(),
                    Arc::clone(&entry.source) as Arc<dyn DataSource>,
                )
            })
            .collect();

        let logical_plan = planner
            .create_logical_plan(&statement)
            .map_err(|e| format!("Planning error: {}", e))?;

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer
            .optimize(&logical_plan)
            .map_err(|e| format!("Optimization error: {}", e))?;

        let physical_plan = logical_to_physical_plan(&optimized_plan, &data_sources)?;

        let executor = QueryExecutor::new();
        let rt = tokio::runtime::Handle::current();
        rt.block_on(async { executor.execute(&physical_plan).await })
            .map_err(|e| format!("Execution error: {}", e))
    }

    /// Helper: Convert projection to SQL string
    fn projection_to_sql(projection: &[query_parser::SelectItem]) -> String {
        projection
            .iter()
            .map(|item| match item {
                query_parser::SelectItem::Wildcard => "*".to_string(),
                query_parser::SelectItem::QualifiedWildcard(t) => format!("{}.*", t),
                query_parser::SelectItem::UnnamedExpr(e) => Self::expr_to_sql(e),
                query_parser::SelectItem::ExprWithAlias { expr, alias } => {
                    format!("{} AS {}", Self::expr_to_sql(expr), alias)
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Helper: Convert FROM clause to SQL string
    fn from_to_sql(
        from: &Option<query_parser::TableReference>,
        selection: &Option<query_parser::Expr>,
    ) -> String {
        let from_sql = match from {
            Some(query_parser::TableReference::Table { name, alias }) => match alias {
                Some(a) => format!("FROM {} AS {}", name, a),
                None => format!("FROM {}", name),
            },
            Some(query_parser::TableReference::Subquery { .. }) => String::new(),
            None => String::new(),
        };

        let where_sql = match selection {
            Some(expr) => format!(" WHERE {}", Self::expr_to_sql(expr)),
            None => String::new(),
        };

        format!("{}{}", from_sql, where_sql)
    }

    /// Helper: Convert expression to SQL string
    fn expr_to_sql(expr: &query_parser::Expr) -> String {
        match expr {
            query_parser::Expr::Column(name) => name.clone(),
            query_parser::Expr::QualifiedColumn { table, column } => {
                format!("{}.{}", table, column)
            }
            query_parser::Expr::Literal(lit) => match lit {
                query_parser::Literal::Number(n) => n.clone(),
                query_parser::Literal::String(s) => format!("'{}'", s),
                query_parser::Literal::Boolean(b) => b.to_string(),
                query_parser::Literal::Null => "NULL".to_string(),
            },
            query_parser::Expr::BinaryOp { left, op, right } => {
                let op_str = match op {
                    query_parser::BinaryOperator::Plus => "+",
                    query_parser::BinaryOperator::Minus => "-",
                    query_parser::BinaryOperator::Multiply => "*",
                    query_parser::BinaryOperator::Divide => "/",
                    query_parser::BinaryOperator::Equal => "=",
                    query_parser::BinaryOperator::NotEqual => "<>",
                    query_parser::BinaryOperator::Less => "<",
                    query_parser::BinaryOperator::LessEqual => "<=",
                    query_parser::BinaryOperator::Greater => ">",
                    query_parser::BinaryOperator::GreaterEqual => ">=",
                    query_parser::BinaryOperator::And => "AND",
                    query_parser::BinaryOperator::Or => "OR",
                    _ => "?",
                };
                format!(
                    "{} {} {}",
                    Self::expr_to_sql(left),
                    op_str,
                    Self::expr_to_sql(right)
                )
            }
            _ => "?".to_string(),
        }
    }

    /// Helper: Execute a simple literal SELECT (e.g., SELECT 1 AS n)
    fn execute_literal_select(
        select: &query_parser::SelectStatement,
    ) -> Option<arrow::record_batch::RecordBatch> {
        use arrow::array::{ArrayRef, Int64Builder, StringBuilder};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

        if select.projection.len() != 1 {
            return None;
        }

        match &select.projection[0] {
            query_parser::SelectItem::ExprWithAlias { expr, alias } => match expr {
                query_parser::Expr::Literal(query_parser::Literal::Number(n)) => {
                    let val: i64 = n.parse().ok()?;
                    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
                        alias,
                        DataType::Int64,
                        false,
                    )]));
                    let mut builder = Int64Builder::new();
                    builder.append_value(val);
                    let array = Arc::new(builder.finish()) as ArrayRef;
                    arrow::record_batch::RecordBatch::try_new(schema, vec![array]).ok()
                }
                query_parser::Expr::Literal(query_parser::Literal::String(s)) => {
                    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
                        alias,
                        DataType::Utf8,
                        false,
                    )]));
                    let mut builder = StringBuilder::new();
                    builder.append_value(s);
                    let array = Arc::new(builder.finish()) as ArrayRef;
                    arrow::record_batch::RecordBatch::try_new(schema, vec![array]).ok()
                }
                _ => None,
            },
            query_parser::SelectItem::UnnamedExpr(query_parser::Expr::Literal(
                query_parser::Literal::Number(n),
            )) => {
                let val: i64 = n.parse().ok()?;
                let schema = Arc::new(ArrowSchema::new(vec![Field::new(
                    "?column?",
                    DataType::Int64,
                    false,
                )]));
                let mut builder = Int64Builder::new();
                builder.append_value(val);
                let array = Arc::new(builder.finish()) as ArrayRef;
                arrow::record_batch::RecordBatch::try_new(schema, vec![array]).ok()
            }
            _ => None,
        }
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

            // Transaction commands - currently no-ops since we're read-only
            // but we acknowledge them for client compatibility
            if sql_upper == "BEGIN"
                || sql_upper == "BEGIN TRANSACTION"
                || sql_upper == "START TRANSACTION"
            {
                info!("Transaction started (no-op for read-only operations)");
                responses.push(Response::Execution(pgwire::api::results::Tag::new("BEGIN")));
                continue;
            }

            if sql_upper == "COMMIT" || sql_upper == "END" || sql_upper == "END TRANSACTION" {
                info!("Transaction committed (no-op for read-only operations)");
                responses.push(Response::Execution(pgwire::api::results::Tag::new(
                    "COMMIT",
                )));
                continue;
            }

            if sql_upper == "ROLLBACK" || sql_upper == "ABORT" {
                info!("Transaction rolled back (no-op for read-only operations)");
                responses.push(Response::Execution(pgwire::api::results::Tag::new(
                    "ROLLBACK",
                )));
                continue;
            }

            // Handle pg_catalog queries (system catalogs)
            if sql_upper.contains("PG_CATALOG.")
                || sql_upper.contains("PG_TABLES")
                || sql_upper.contains("PG_ATTRIBUTE")
                || sql_upper.contains("PG_TYPE")
                || sql_upper.contains("INFORMATION_SCHEMA.")
            {
                match crate::catalog::handle_pg_catalog_query(sql, &self.tables).await {
                    Ok(mut resp) => {
                        for r in resp.drain(..) {
                            responses.push(unsafe { std::mem::transmute(r) });
                        }
                    }
                    Err(e) => return Err(e),
                }
                continue;
            }

            // Handle COPY command
            if sql_upper.starts_with("COPY ") {
                match self.handle_copy(sql).await {
                    Ok(mut resp) => {
                        for r in resp.drain(..) {
                            responses.push(unsafe { std::mem::transmute(r) });
                        }
                    }
                    Err(e) => return Err(e),
                }
                continue;
            }

            // Handle DECLARE cursor
            if sql_upper.starts_with("DECLARE ") {
                match self.handle_declare_cursor(sql).await {
                    Ok(resp) => {
                        responses.push(resp);
                    }
                    Err(e) => return Err(e),
                }
                continue;
            }

            // Handle FETCH from cursor
            if sql_upper.starts_with("FETCH ") {
                match self.handle_fetch_cursor(sql).await {
                    Ok(mut resp) => {
                        for r in resp.drain(..) {
                            responses.push(unsafe { std::mem::transmute(r) });
                        }
                    }
                    Err(e) => return Err(e),
                }
                continue;
            }

            // Handle CLOSE cursor
            if sql_upper.starts_with("CLOSE ") {
                match self.handle_close_cursor(sql).await {
                    Ok(resp) => {
                        responses.push(resp);
                    }
                    Err(e) => return Err(e),
                }
                continue;
            }

            // Handle CREATE TABLE
            if sql_upper.starts_with("CREATE TABLE") {
                match self.handle_create_table(sql).await {
                    Ok(resp) => {
                        responses.push(resp);
                    }
                    Err(e) => return Err(e),
                }
                continue;
            }

            // Handle INSERT
            if sql_upper.starts_with("INSERT INTO") || sql_upper.starts_with("INSERT ") {
                match self.handle_insert(sql).await {
                    Ok(resp) => {
                        responses.push(resp);
                    }
                    Err(e) => return Err(e),
                }
                continue;
            }

            // Handle UPDATE
            if sql_upper.starts_with("UPDATE ") {
                match self.handle_update(sql).await {
                    Ok(resp) => {
                        responses.push(resp);
                    }
                    Err(e) => return Err(e),
                }
                continue;
            }

            // Handle DELETE
            if sql_upper.starts_with("DELETE FROM") || sql_upper.starts_with("DELETE ") {
                match self.handle_delete(sql).await {
                    Ok(resp) => {
                        responses.push(resp);
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

    /// Handle CREATE TABLE command
    async fn handle_create_table(&self, sql: &str) -> PgWireResult<Response<'static>> {
        use query_parser::Parser;

        let mut parser = Parser::new(sql).map_err(|e| user_error(format!("Parse error: {}", e)))?;
        let statement = parser
            .parse()
            .map_err(|e| user_error(format!("Parse error: {}", e)))?;

        if let query_parser::Statement::CreateTable(create) = statement {
            // Check if table already exists
            let tables = self.tables.read().await;
            if tables.contains_key(&create.name) {
                if create.if_not_exists {
                    return Ok(Response::Execution(pgwire::api::results::Tag::new(
                        "CREATE TABLE",
                    )));
                } else {
                    return Err(user_error(format!(
                        "Table '{}' already exists",
                        create.name
                    )));
                }
            }
            drop(tables);

            // Build schema from column definitions
            let fields: Vec<query_core::Field> = create
                .columns
                .iter()
                .map(|col| query_core::Field::new(&col.name, col.data_type.clone(), col.nullable))
                .collect();
            let schema = query_core::Schema::new(fields);

            // Create empty memory data source
            let source = Arc::new(MemoryDataSource::new(schema.clone(), vec![]));

            // Register the table
            let mut tables = self.tables.write().await;
            tables.insert(create.name.clone(), TableEntry { schema, source });

            info!("Created table: {}", create.name);
            Ok(Response::Execution(pgwire::api::results::Tag::new(
                "CREATE TABLE",
            )))
        } else {
            Err(user_error("Invalid CREATE TABLE statement".to_string()))
        }
    }

    /// Handle INSERT command with ON CONFLICT support
    async fn handle_insert(&self, sql: &str) -> PgWireResult<Response<'static>> {
        use arrow::array::{
            ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int64Array,
            Int64Builder, StringArray, StringBuilder,
        };
        use arrow::record_batch::RecordBatch;
        use query_parser::{ConflictAction, Parser};

        let mut parser = Parser::new(sql).map_err(|e| user_error(format!("Parse error: {}", e)))?;
        let statement = parser
            .parse()
            .map_err(|e| user_error(format!("Parse error: {}", e)))?;

        if let query_parser::Statement::Insert(insert) = statement {
            // Get existing table info and batches
            let (schema, existing_batches, table_name) = {
                let tables = self.tables.read().await;
                let entry = tables
                    .get(&insert.table)
                    .ok_or_else(|| user_error(format!("Table not found: {}", insert.table)))?;

                // Scan existing data
                let batches = entry
                    .source
                    .scan()
                    .map_err(|e| user_error(format!("Failed to read table: {}", e)))?;
                (entry.schema.clone(), batches, insert.table.clone())
            };

            // Get conflict column indices if ON CONFLICT specified
            let conflict_col_indices: Option<Vec<usize>> = insert.on_conflict.as_ref().map(|oc| {
                oc.columns
                    .iter()
                    .filter_map(|col_name| {
                        schema.fields().iter().position(|f| f.name() == col_name)
                    })
                    .collect()
            });

            // Track which existing rows have been updated (for DO UPDATE)
            let mut updated_existing_rows: std::collections::HashSet<(usize, usize)> =
                std::collections::HashSet::new();
            let mut rows_to_insert: Vec<usize> = Vec::new(); // indices of values to actually insert
            let mut inserted_count = 0usize;
            let mut updated_count = 0usize;

            // For each new row, check for conflicts
            for (row_idx, row) in insert.values.iter().enumerate() {
                let mut has_conflict = false;

                if let Some(ref col_indices) = conflict_col_indices {
                    // Extract conflict key values from new row
                    let new_key_values: Vec<String> = col_indices
                        .iter()
                        .map(|&idx| {
                            if idx < row.len() {
                                match &row[idx] {
                                    query_parser::Expr::Literal(query_parser::Literal::Number(
                                        n,
                                    )) => n.clone(),
                                    query_parser::Expr::Literal(query_parser::Literal::String(
                                        s,
                                    )) => s.clone(),
                                    query_parser::Expr::Literal(
                                        query_parser::Literal::Boolean(b),
                                    ) => b.to_string(),
                                    _ => String::new(),
                                }
                            } else {
                                String::new()
                            }
                        })
                        .collect();

                    // Check existing batches for conflicts
                    'batch_loop: for (batch_idx, batch) in existing_batches.iter().enumerate() {
                        for existing_row in 0..batch.num_rows() {
                            let mut matches = true;
                            for (key_idx, &col_idx) in col_indices.iter().enumerate() {
                                let existing_val =
                                    self.get_value_as_string(batch, col_idx, existing_row);
                                if existing_val != new_key_values[key_idx] {
                                    matches = false;
                                    break;
                                }
                            }
                            if matches {
                                has_conflict = true;
                                updated_existing_rows.insert((batch_idx, existing_row));
                                break 'batch_loop;
                            }
                        }
                    }
                }

                if has_conflict {
                    match insert.on_conflict.as_ref().map(|oc| &oc.action) {
                        Some(ConflictAction::DoNothing) => {
                            // Skip this row
                        }
                        Some(ConflictAction::DoUpdate { .. }) => {
                            updated_count += 1;
                        }
                        None => {
                            // No ON CONFLICT, should not happen here
                            rows_to_insert.push(row_idx);
                            inserted_count += 1;
                        }
                    }
                } else {
                    rows_to_insert.push(row_idx);
                    inserted_count += 1;
                }
            }

            // Build arrays for rows to insert
            let mut new_batch: Option<RecordBatch> = None;
            if !rows_to_insert.is_empty() {
                let mut arrays: Vec<ArrayRef> = Vec::new();
                for (col_idx, field) in schema.fields().iter().enumerate() {
                    match field.data_type() {
                        query_core::DataType::Int64 => {
                            let mut builder = Int64Builder::new();
                            for &row_idx in &rows_to_insert {
                                let row = &insert.values[row_idx];
                                if col_idx < row.len() {
                                    if let query_parser::Expr::Literal(
                                        query_parser::Literal::Number(n),
                                    ) = &row[col_idx]
                                    {
                                        builder.append_value(n.parse::<i64>().unwrap_or(0));
                                    } else {
                                        builder.append_null();
                                    }
                                } else {
                                    builder.append_null();
                                }
                            }
                            arrays.push(Arc::new(builder.finish()) as ArrayRef);
                        }
                        query_core::DataType::Float64 => {
                            let mut builder = Float64Builder::new();
                            for &row_idx in &rows_to_insert {
                                let row = &insert.values[row_idx];
                                if col_idx < row.len() {
                                    if let query_parser::Expr::Literal(
                                        query_parser::Literal::Number(n),
                                    ) = &row[col_idx]
                                    {
                                        builder.append_value(n.parse::<f64>().unwrap_or(0.0));
                                    } else {
                                        builder.append_null();
                                    }
                                } else {
                                    builder.append_null();
                                }
                            }
                            arrays.push(Arc::new(builder.finish()) as ArrayRef);
                        }
                        query_core::DataType::Utf8 => {
                            let mut builder = StringBuilder::new();
                            for &row_idx in &rows_to_insert {
                                let row = &insert.values[row_idx];
                                if col_idx < row.len() {
                                    if let query_parser::Expr::Literal(
                                        query_parser::Literal::String(s),
                                    ) = &row[col_idx]
                                    {
                                        builder.append_value(s);
                                    } else if let query_parser::Expr::Literal(
                                        query_parser::Literal::Number(n),
                                    ) = &row[col_idx]
                                    {
                                        builder.append_value(n);
                                    } else {
                                        builder.append_null();
                                    }
                                } else {
                                    builder.append_null();
                                }
                            }
                            arrays.push(Arc::new(builder.finish()) as ArrayRef);
                        }
                        query_core::DataType::Boolean => {
                            let mut builder = BooleanBuilder::new();
                            for &row_idx in &rows_to_insert {
                                let row = &insert.values[row_idx];
                                if col_idx < row.len() {
                                    if let query_parser::Expr::Literal(
                                        query_parser::Literal::Boolean(b),
                                    ) = &row[col_idx]
                                    {
                                        builder.append_value(*b);
                                    } else {
                                        builder.append_null();
                                    }
                                } else {
                                    builder.append_null();
                                }
                            }
                            arrays.push(Arc::new(builder.finish()) as ArrayRef);
                        }
                        _ => {
                            let mut builder = StringBuilder::new();
                            for _ in &rows_to_insert {
                                builder.append_null();
                            }
                            arrays.push(Arc::new(builder.finish()) as ArrayRef);
                        }
                    }
                }

                if !arrays.is_empty() {
                    let arrow_schema = Arc::new(schema.to_arrow());
                    new_batch = Some(
                        RecordBatch::try_new(arrow_schema, arrays)
                            .map_err(|e| user_error(format!("Failed to create batch: {}", e)))?,
                    );
                }
            }

            // Handle DO UPDATE: update existing rows that had conflicts
            let mut final_batches: Vec<RecordBatch> = Vec::new();
            if let Some(ConflictAction::DoUpdate { assignments }) =
                insert.on_conflict.as_ref().map(|oc| &oc.action)
            {
                // Build assignment map
                let assignment_map: std::collections::HashMap<String, &query_parser::Expr> =
                    assignments
                        .iter()
                        .map(|a| (a.column.clone(), &a.value))
                        .collect();

                for (batch_idx, batch) in existing_batches.iter().enumerate() {
                    let num_rows = batch.num_rows();
                    let mut arrays: Vec<ArrayRef> = Vec::new();

                    for (col_idx, field) in schema.fields().iter().enumerate() {
                        let col_name = field.name();

                        if let Some(new_value_expr) = assignment_map.get(col_name) {
                            let new_value = match new_value_expr {
                                query_parser::Expr::Literal(query_parser::Literal::Number(n)) => {
                                    n.clone()
                                }
                                query_parser::Expr::Literal(query_parser::Literal::String(s)) => {
                                    s.clone()
                                }
                                query_parser::Expr::Literal(query_parser::Literal::Boolean(b)) => {
                                    b.to_string()
                                }
                                _ => String::new(),
                            };

                            match field.data_type() {
                                query_core::DataType::Int64 => {
                                    let mut builder = Int64Builder::new();
                                    let orig_arr =
                                        batch.column(col_idx).as_any().downcast_ref::<Int64Array>();
                                    let new_val: i64 = new_value.parse().unwrap_or(0);
                                    for row in 0..num_rows {
                                        if updated_existing_rows.contains(&(batch_idx, row)) {
                                            builder.append_value(new_val);
                                        } else if let Some(arr) = orig_arr {
                                            builder.append_value(arr.value(row));
                                        } else {
                                            builder.append_value(0);
                                        }
                                    }
                                    arrays.push(Arc::new(builder.finish()) as ArrayRef);
                                }
                                query_core::DataType::Float64 => {
                                    let mut builder = Float64Builder::new();
                                    let orig_arr = batch
                                        .column(col_idx)
                                        .as_any()
                                        .downcast_ref::<Float64Array>();
                                    let new_val: f64 = new_value.parse().unwrap_or(0.0);
                                    for row in 0..num_rows {
                                        if updated_existing_rows.contains(&(batch_idx, row)) {
                                            builder.append_value(new_val);
                                        } else if let Some(arr) = orig_arr {
                                            builder.append_value(arr.value(row));
                                        } else {
                                            builder.append_value(0.0);
                                        }
                                    }
                                    arrays.push(Arc::new(builder.finish()) as ArrayRef);
                                }
                                query_core::DataType::Utf8 => {
                                    let mut builder = StringBuilder::new();
                                    let orig_arr = batch
                                        .column(col_idx)
                                        .as_any()
                                        .downcast_ref::<StringArray>();
                                    for row in 0..num_rows {
                                        if updated_existing_rows.contains(&(batch_idx, row)) {
                                            builder.append_value(&new_value);
                                        } else if let Some(arr) = orig_arr {
                                            builder.append_value(arr.value(row));
                                        } else {
                                            builder.append_value("");
                                        }
                                    }
                                    arrays.push(Arc::new(builder.finish()) as ArrayRef);
                                }
                                query_core::DataType::Boolean => {
                                    let mut builder = BooleanBuilder::new();
                                    let orig_arr = batch
                                        .column(col_idx)
                                        .as_any()
                                        .downcast_ref::<BooleanArray>();
                                    let bool_val = new_value == "true" || new_value == "1";
                                    for row in 0..num_rows {
                                        if updated_existing_rows.contains(&(batch_idx, row)) {
                                            builder.append_value(bool_val);
                                        } else if let Some(arr) = orig_arr {
                                            builder.append_value(arr.value(row));
                                        } else {
                                            builder.append_value(false);
                                        }
                                    }
                                    arrays.push(Arc::new(builder.finish()) as ArrayRef);
                                }
                                _ => {
                                    arrays.push(Arc::clone(batch.column(col_idx)));
                                }
                            }
                        } else {
                            arrays.push(Arc::clone(batch.column(col_idx)));
                        }
                    }

                    let arrow_schema = Arc::new(schema.to_arrow());
                    let updated_batch = RecordBatch::try_new(arrow_schema, arrays)
                        .map_err(|e| user_error(format!("Failed to create batch: {}", e)))?;
                    final_batches.push(updated_batch);
                }
            } else {
                // No DO UPDATE - keep existing batches as-is
                final_batches = existing_batches;
            }

            // Add new batch if any
            if let Some(batch) = new_batch {
                final_batches.push(batch);
            }

            // Create new data source with combined data
            let new_source = Arc::new(MemoryDataSource::new(schema.clone(), final_batches));

            // Replace the table entry
            let mut tables = self.tables.write().await;
            tables.insert(
                table_name.clone(),
                TableEntry {
                    schema,
                    source: new_source,
                },
            );

            if updated_count > 0 {
                info!(
                    "Upserted {} rows ({} inserted, {} updated) in {}",
                    inserted_count + updated_count,
                    inserted_count,
                    updated_count,
                    table_name
                );
            } else {
                info!("Inserted {} rows into {}", inserted_count, table_name);
            }

            // Handle RETURNING clause (return inserted rows or empty for now)
            if insert.returning.is_some() {
                // For UPSERT, RETURNING is complex - for now return inserted count
                return Ok(Response::Execution(pgwire::api::results::Tag::new(
                    &format!("INSERT 0 {}", inserted_count),
                )));
            }

            Ok(Response::Execution(pgwire::api::results::Tag::new(
                &format!("INSERT 0 {}", inserted_count),
            )))
        } else {
            Err(user_error("Invalid INSERT statement".to_string()))
        }
    }

    /// Helper to get a cell value as string for comparison
    fn get_value_as_string(
        &self,
        batch: &arrow::record_batch::RecordBatch,
        col_idx: usize,
        row: usize,
    ) -> String {
        use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};

        let column = batch.column(col_idx);

        if let Some(arr) = column.as_any().downcast_ref::<Int64Array>() {
            arr.value(row).to_string()
        } else if let Some(arr) = column.as_any().downcast_ref::<Float64Array>() {
            arr.value(row).to_string()
        } else if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
            arr.value(row).to_string()
        } else if let Some(arr) = column.as_any().downcast_ref::<BooleanArray>() {
            arr.value(row).to_string()
        } else {
            String::new()
        }
    }

    /// Evaluate a simple WHERE condition for a single row
    fn evaluate_where_condition(
        &self,
        condition: &query_parser::Expr,
        batch: &arrow::record_batch::RecordBatch,
        row: usize,
        schema: &query_core::Schema,
    ) -> bool {
        use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};

        match condition {
            query_parser::Expr::BinaryOp { left, op, right } => {
                // Get column name from left side
                let col_name = match left.as_ref() {
                    query_parser::Expr::Column(name) => name.clone(),
                    _ => return true, // Complex expression - match all
                };

                // Get literal value from right side
                let literal_str = match right.as_ref() {
                    query_parser::Expr::Literal(query_parser::Literal::Number(n)) => n.clone(),
                    query_parser::Expr::Literal(query_parser::Literal::String(s)) => s.clone(),
                    query_parser::Expr::Literal(query_parser::Literal::Boolean(b)) => {
                        if *b {
                            "true".to_string()
                        } else {
                            "false".to_string()
                        }
                    }
                    _ => return true,
                };

                // Find column index
                let col_idx = schema.fields().iter().position(|f| f.name() == &col_name);
                let col_idx = match col_idx {
                    Some(idx) => idx,
                    None => return true,
                };

                let column = batch.column(col_idx);

                // Compare based on type
                let matches = if let Some(arr) = column.as_any().downcast_ref::<Int64Array>() {
                    let val = arr.value(row);
                    let cmp_val: i64 = literal_str.parse().unwrap_or(0);
                    match op {
                        query_parser::BinaryOperator::Equal => val == cmp_val,
                        query_parser::BinaryOperator::NotEqual => val != cmp_val,
                        query_parser::BinaryOperator::Less => val < cmp_val,
                        query_parser::BinaryOperator::LessEqual => val <= cmp_val,
                        query_parser::BinaryOperator::Greater => val > cmp_val,
                        query_parser::BinaryOperator::GreaterEqual => val >= cmp_val,
                        _ => true,
                    }
                } else if let Some(arr) = column.as_any().downcast_ref::<Float64Array>() {
                    let val = arr.value(row);
                    let cmp_val: f64 = literal_str.parse().unwrap_or(0.0);
                    match op {
                        query_parser::BinaryOperator::Equal => (val - cmp_val).abs() < f64::EPSILON,
                        query_parser::BinaryOperator::NotEqual => {
                            (val - cmp_val).abs() >= f64::EPSILON
                        }
                        query_parser::BinaryOperator::Less => val < cmp_val,
                        query_parser::BinaryOperator::LessEqual => val <= cmp_val,
                        query_parser::BinaryOperator::Greater => val > cmp_val,
                        query_parser::BinaryOperator::GreaterEqual => val >= cmp_val,
                        _ => true,
                    }
                } else if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
                    let val = arr.value(row);
                    match op {
                        query_parser::BinaryOperator::Equal => val == literal_str,
                        query_parser::BinaryOperator::NotEqual => val != literal_str,
                        _ => true,
                    }
                } else if let Some(arr) = column.as_any().downcast_ref::<BooleanArray>() {
                    let val = arr.value(row);
                    let cmp_val = literal_str == "true" || literal_str == "1";
                    match op {
                        query_parser::BinaryOperator::Equal => val == cmp_val,
                        query_parser::BinaryOperator::NotEqual => val != cmp_val,
                        _ => true,
                    }
                } else {
                    true
                };

                matches
            }
            _ => true, // Complex conditions - match all for safety
        }
    }

    /// Handle UPDATE command with WHERE clause support
    async fn handle_update(&self, sql: &str) -> PgWireResult<Response<'static>> {
        use arrow::array::{
            ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int64Array,
            Int64Builder, StringArray, StringBuilder,
        };
        use arrow::record_batch::RecordBatch;
        use query_parser::Parser;

        let mut parser = Parser::new(sql).map_err(|e| user_error(format!("Parse error: {}", e)))?;
        let statement = parser
            .parse()
            .map_err(|e| user_error(format!("Parse error: {}", e)))?;

        if let query_parser::Statement::Update(update) = statement {
            // Get existing table info and batches
            let (schema, existing_batches, table_name) = {
                let tables = self.tables.read().await;
                let entry = tables
                    .get(&update.table)
                    .ok_or_else(|| user_error(format!("Table not found: {}", update.table)))?;

                let batches = entry
                    .source
                    .scan()
                    .map_err(|e| user_error(format!("Failed to read table: {}", e)))?;
                (entry.schema.clone(), batches, update.table.clone())
            };

            // Build assignment map
            let mut assignment_map: std::collections::HashMap<String, &query_parser::Expr> =
                std::collections::HashMap::new();
            for assign in &update.assignments {
                assignment_map.insert(assign.column.clone(), &assign.value);
            }

            let mut updated_rows = 0;
            let mut new_batches = Vec::new();

            // Process each batch
            for batch in existing_batches {
                let num_rows = batch.num_rows();
                let mut arrays: Vec<ArrayRef> = Vec::new();

                // Determine which rows match the WHERE clause
                let mut row_matches: Vec<bool> = vec![true; num_rows];
                if let Some(ref condition) = update.selection {
                    for row in 0..num_rows {
                        row_matches[row] =
                            self.evaluate_where_condition(condition, &batch, row, &schema);
                    }
                }

                for (col_idx, field) in schema.fields().iter().enumerate() {
                    let col_name = field.name();

                    // Check if this column has an update assignment
                    if let Some(new_value_expr) = assignment_map.get(col_name) {
                        // Extract new value
                        let new_value = match new_value_expr {
                            query_parser::Expr::Literal(query_parser::Literal::Number(n)) => {
                                n.clone()
                            }
                            query_parser::Expr::Literal(query_parser::Literal::String(s)) => {
                                s.clone()
                            }
                            query_parser::Expr::Literal(query_parser::Literal::Boolean(b)) => {
                                if *b {
                                    "true".to_string()
                                } else {
                                    "false".to_string()
                                }
                            }
                            _ => "".to_string(),
                        };

                        // Apply to matching rows only
                        match field.data_type() {
                            query_core::DataType::Int64 => {
                                let mut builder = Int64Builder::new();
                                let orig_arr =
                                    batch.column(col_idx).as_any().downcast_ref::<Int64Array>();
                                let new_val: i64 = new_value.parse().unwrap_or(0);
                                for row in 0..num_rows {
                                    if row_matches[row] {
                                        builder.append_value(new_val);
                                        updated_rows += 1;
                                    } else if let Some(arr) = orig_arr {
                                        builder.append_value(arr.value(row));
                                    } else {
                                        builder.append_value(0);
                                    }
                                }
                                arrays.push(Arc::new(builder.finish()) as ArrayRef);
                            }
                            query_core::DataType::Float64 => {
                                let mut builder = Float64Builder::new();
                                let orig_arr = batch
                                    .column(col_idx)
                                    .as_any()
                                    .downcast_ref::<Float64Array>();
                                let new_val: f64 = new_value.parse().unwrap_or(0.0);
                                for row in 0..num_rows {
                                    if row_matches[row] {
                                        builder.append_value(new_val);
                                        updated_rows += 1;
                                    } else if let Some(arr) = orig_arr {
                                        builder.append_value(arr.value(row));
                                    } else {
                                        builder.append_value(0.0);
                                    }
                                }
                                arrays.push(Arc::new(builder.finish()) as ArrayRef);
                            }
                            query_core::DataType::Utf8 => {
                                let mut builder = StringBuilder::new();
                                let orig_arr =
                                    batch.column(col_idx).as_any().downcast_ref::<StringArray>();
                                for row in 0..num_rows {
                                    if row_matches[row] {
                                        builder.append_value(&new_value);
                                        updated_rows += 1;
                                    } else if let Some(arr) = orig_arr {
                                        builder.append_value(arr.value(row));
                                    } else {
                                        builder.append_value("");
                                    }
                                }
                                arrays.push(Arc::new(builder.finish()) as ArrayRef);
                            }
                            query_core::DataType::Boolean => {
                                let mut builder = BooleanBuilder::new();
                                let orig_arr = batch
                                    .column(col_idx)
                                    .as_any()
                                    .downcast_ref::<BooleanArray>();
                                let bool_val = new_value == "true" || new_value == "1";
                                for row in 0..num_rows {
                                    if row_matches[row] {
                                        builder.append_value(bool_val);
                                        updated_rows += 1;
                                    } else if let Some(arr) = orig_arr {
                                        builder.append_value(arr.value(row));
                                    } else {
                                        builder.append_value(false);
                                    }
                                }
                                arrays.push(Arc::new(builder.finish()) as ArrayRef);
                            }
                            _ => {
                                // Keep original column for unsupported types
                                arrays.push(Arc::clone(batch.column(col_idx)));
                            }
                        }
                    } else {
                        // Keep original column
                        arrays.push(Arc::clone(batch.column(col_idx)));
                    }
                }

                let arrow_schema = Arc::new(schema.to_arrow());
                let new_batch = RecordBatch::try_new(arrow_schema, arrays)
                    .map_err(|e| user_error(format!("Failed to create batch: {}", e)))?;
                new_batches.push(new_batch);
            }

            // Save batches for RETURNING before move
            let return_batch = if !new_batches.is_empty() {
                Some(new_batches[0].clone())
            } else {
                None
            };

            // Create new data source with updated data
            let new_source = Arc::new(MemoryDataSource::new(schema.clone(), new_batches));

            // Replace the table entry
            let mut tables = self.tables.write().await;
            tables.insert(
                table_name.clone(),
                TableEntry {
                    schema: schema.clone(),
                    source: new_source,
                },
            );

            // Adjust count (updated_rows counts each column update, divide by assignments)
            let actual_rows = updated_rows / update.assignments.len().max(1);
            info!("Updated {} rows in {}", actual_rows, table_name);

            // Handle RETURNING clause
            if let Some(returning) = &update.returning {
                if let Some(batch) = return_batch {
                    return build_returning_response(&batch, returning);
                }
            }

            Ok(Response::Execution(pgwire::api::results::Tag::new(
                &format!("UPDATE {}", actual_rows),
            )))
        } else {
            Err(user_error("Invalid UPDATE statement".to_string()))
        }
    }

    /// Handle DELETE command with WHERE clause support
    async fn handle_delete(&self, sql: &str) -> PgWireResult<Response<'static>> {
        use arrow::array::{
            ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int64Array,
            Int64Builder, StringArray, StringBuilder,
        };
        use arrow::record_batch::RecordBatch;
        use query_parser::Parser;

        let mut parser = Parser::new(sql).map_err(|e| user_error(format!("Parse error: {}", e)))?;
        let statement = parser
            .parse()
            .map_err(|e| user_error(format!("Parse error: {}", e)))?;

        if let query_parser::Statement::Delete(delete) = statement {
            // Get existing table info
            let (schema, existing_batches, table_name) = {
                let tables = self.tables.read().await;
                let entry = tables
                    .get(&delete.table)
                    .ok_or_else(|| user_error(format!("Table not found: {}", delete.table)))?;

                let batches = entry
                    .source
                    .scan()
                    .map_err(|e| user_error(format!("Failed to read table: {}", e)))?;
                (entry.schema.clone(), batches, delete.table.clone())
            };

            // If no WHERE clause, delete all rows
            if delete.selection.is_none() {
                let total_rows: usize = existing_batches.iter().map(|b| b.num_rows()).sum();

                // Create empty data source
                let new_source = Arc::new(MemoryDataSource::new(schema.clone(), vec![]));

                // Replace the table entry
                let mut tables = self.tables.write().await;
                tables.insert(
                    table_name.clone(),
                    TableEntry {
                        schema,
                        source: new_source,
                    },
                );

                info!("Deleted {} rows from {}", total_rows, table_name);
                return Ok(Response::Execution(pgwire::api::results::Tag::new(
                    &format!("DELETE {}", total_rows),
                )));
            }

            // With WHERE clause - filter and keep non-matching rows
            let condition = delete.selection.as_ref().unwrap();
            let mut deleted_rows = 0;
            let mut new_batches = Vec::new();
            let mut deleted_batches: Vec<RecordBatch> = Vec::new(); // For RETURNING

            for batch in existing_batches {
                let num_rows = batch.num_rows();

                // Determine which rows match the WHERE clause (to delete)
                let mut row_matches: Vec<bool> = vec![false; num_rows];
                for row in 0..num_rows {
                    row_matches[row] =
                        self.evaluate_where_condition(condition, &batch, row, &schema);
                    if row_matches[row] {
                        deleted_rows += 1;
                    }
                }

                // Count kept rows
                let kept_rows: usize = row_matches.iter().filter(|&&m| !m).count();
                if kept_rows == 0 {
                    continue; // All rows deleted from this batch
                }

                // Build new arrays with only non-matching (kept) rows
                let mut arrays: Vec<ArrayRef> = Vec::new();

                for (col_idx, field) in schema.fields().iter().enumerate() {
                    match field.data_type() {
                        query_core::DataType::Int64 => {
                            let mut builder = Int64Builder::new();
                            let arr = batch.column(col_idx).as_any().downcast_ref::<Int64Array>();
                            for row in 0..num_rows {
                                if !row_matches[row] {
                                    if let Some(a) = arr {
                                        builder.append_value(a.value(row));
                                    }
                                }
                            }
                            arrays.push(Arc::new(builder.finish()) as ArrayRef);
                        }
                        query_core::DataType::Float64 => {
                            let mut builder = Float64Builder::new();
                            let arr = batch
                                .column(col_idx)
                                .as_any()
                                .downcast_ref::<Float64Array>();
                            for row in 0..num_rows {
                                if !row_matches[row] {
                                    if let Some(a) = arr {
                                        builder.append_value(a.value(row));
                                    }
                                }
                            }
                            arrays.push(Arc::new(builder.finish()) as ArrayRef);
                        }
                        query_core::DataType::Utf8 => {
                            let mut builder = StringBuilder::new();
                            let arr = batch.column(col_idx).as_any().downcast_ref::<StringArray>();
                            for row in 0..num_rows {
                                if !row_matches[row] {
                                    if let Some(a) = arr {
                                        builder.append_value(a.value(row));
                                    }
                                }
                            }
                            arrays.push(Arc::new(builder.finish()) as ArrayRef);
                        }
                        query_core::DataType::Boolean => {
                            let mut builder = BooleanBuilder::new();
                            let arr = batch
                                .column(col_idx)
                                .as_any()
                                .downcast_ref::<BooleanArray>();
                            for row in 0..num_rows {
                                if !row_matches[row] {
                                    if let Some(a) = arr {
                                        builder.append_value(a.value(row));
                                    }
                                }
                            }
                            arrays.push(Arc::new(builder.finish()) as ArrayRef);
                        }
                        _ => {
                            // For unsupported types, skip (shouldn't happen often)
                            arrays.push(Arc::clone(batch.column(col_idx)));
                        }
                    }
                }

                let arrow_schema = Arc::new(schema.to_arrow());
                let new_batch = RecordBatch::try_new(arrow_schema, arrays)
                    .map_err(|e| user_error(format!("Failed to create batch: {}", e)))?;
                new_batches.push(new_batch);
            }

            // Create new data source with remaining data
            let new_source = Arc::new(MemoryDataSource::new(schema.clone(), new_batches));

            // Replace the table entry
            let mut tables = self.tables.write().await;
            tables.insert(
                table_name.clone(),
                TableEntry {
                    schema: schema.clone(),
                    source: new_source,
                },
            );

            info!("Deleted {} rows from {}", deleted_rows, table_name);

            // Handle RETURNING clause - for DELETE we'd need to capture deleted rows
            // For now, if RETURNING is present, we return count (simplified)
            if let Some(returning) = &delete.returning {
                if !deleted_batches.is_empty() {
                    return build_returning_response(&deleted_batches[0], returning);
                }
            }

            Ok(Response::Execution(pgwire::api::results::Tag::new(
                &format!("DELETE {}", deleted_rows),
            )))
        } else {
            Err(user_error("Invalid DELETE statement".to_string()))
        }
    }

    /// Handle COPY command - currently supports COPY TO STDOUT
    async fn handle_copy(&self, sql: &str) -> PgWireResult<Vec<Response<'static>>> {
        use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use arrow::record_batch::RecordBatch;

        let sql_upper = sql.to_uppercase();

        // Parse COPY syntax: COPY table TO STDOUT [WITH (options)]
        // or: COPY (query) TO STDOUT [WITH (options)]
        let parts: Vec<&str> = sql.split_whitespace().collect();

        if parts.len() < 4 {
            return Err(user_error(
                "Invalid COPY syntax. Use: COPY table TO STDOUT [WITH (FORMAT csv)]".to_string(),
            ));
        }

        // Check if it's COPY TO (export) or COPY FROM (import)
        let to_idx = parts.iter().position(|p| p.to_uppercase() == "TO");
        let from_idx = parts.iter().position(|p| p.to_uppercase() == "FROM");

        if let Some(idx) = to_idx {
            // COPY TO (export)
            let table_name = parts[1].trim_matches(|c| c == '(' || c == ')');

            // Check for STDOUT
            if idx + 1 >= parts.len() || parts[idx + 1].to_uppercase() != "STDOUT" {
                return Err(user_error("Only COPY TO STDOUT is supported".to_string()));
            }

            // Parse options
            let with_header = sql_upper.contains("HEADER");
            let _is_csv = sql_upper.contains("CSV") || sql_upper.contains("FORMAT CSV");

            // Get table data
            let tables = self.tables.read().await;
            let entry = tables
                .get(table_name)
                .ok_or_else(|| user_error(format!("Table not found: {}", table_name)))?;

            let batches = entry
                .source
                .scan()
                .map_err(|e| user_error(format!("Failed to read table: {}", e)))?;

            // Build CSV output
            let mut csv_lines: Vec<String> = Vec::new();

            // Add header if requested
            if with_header {
                let header: Vec<String> = entry
                    .schema
                    .fields()
                    .iter()
                    .map(|f| f.name().to_string())
                    .collect();
                csv_lines.push(header.join(","));
            }

            // Convert batches to CSV rows
            for batch in &batches {
                let num_rows = batch.num_rows();
                for row in 0..num_rows {
                    let mut row_values: Vec<String> = Vec::new();
                    for col_idx in 0..batch.num_columns() {
                        let column = batch.column(col_idx);
                        let value = if let Some(arr) = column.as_any().downcast_ref::<Int64Array>()
                        {
                            if arr.is_null(row) {
                                String::new()
                            } else {
                                arr.value(row).to_string()
                            }
                        } else if let Some(arr) = column.as_any().downcast_ref::<Float64Array>() {
                            if arr.is_null(row) {
                                String::new()
                            } else {
                                arr.value(row).to_string()
                            }
                        } else if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
                            if arr.is_null(row) {
                                String::new()
                            } else {
                                // Escape CSV values with quotes if they contain commas
                                let val = arr.value(row);
                                if val.contains(',') || val.contains('"') || val.contains('\n') {
                                    format!("\"{}\"", val.replace('"', "\"\""))
                                } else {
                                    val.to_string()
                                }
                            }
                        } else if let Some(arr) = column.as_any().downcast_ref::<BooleanArray>() {
                            if arr.is_null(row) {
                                String::new()
                            } else {
                                if arr.value(row) { "t" } else { "f" }.to_string()
                            }
                        } else {
                            // Fallback for other types
                            String::new()
                        };
                        row_values.push(value);
                    }
                    csv_lines.push(row_values.join(","));
                }
            }

            // Return as single-column result with CSV data
            let schema = Arc::new(ArrowSchema::new(vec![Field::new(
                "copy_data",
                DataType::Utf8,
                false,
            )]));

            let csv_data: Vec<&str> = csv_lines.iter().map(|s| s.as_str()).collect();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(StringArray::from(csv_data)) as arrow::array::ArrayRef],
            )
            .map_err(|e| user_error(format!("Error creating COPY result: {}", e)))?;

            let field_info = schema_to_field_info(&schema);
            let rows = record_batch_to_rows(&batch, &field_info)?;
            let all_rows: Vec<_> = rows.into_iter().map(|r| r.finish()).collect();

            info!(
                "COPY: exported {} rows from {}",
                csv_lines.len(),
                table_name
            );

            Ok(vec![Response::Query(QueryResponse::new(
                Arc::new(field_info),
                futures::stream::iter(all_rows),
            ))])
        } else if let Some(idx) = from_idx {
            // COPY FROM (import)
            let table_name = parts[1].trim_matches(|c| c == '(' || c == ')');

            // Check for STDIN
            if idx + 1 >= parts.len() || parts[idx + 1].to_uppercase() != "STDIN" {
                return Err(user_error("Only COPY FROM STDIN is supported".to_string()));
            }

            // Parse options
            let with_header = sql_upper.contains("HEADER");
            let delimiter = if sql_upper.contains("DELIMITER") {
                // Try to extract delimiter - default to comma
                ','
            } else {
                ','
            };

            // For COPY FROM, we expect the data to follow after the command
            // PostgreSQL protocol: data comes in subsequent messages terminated by \.
            // For simple query protocol, we'll parse inline data after the semicolon

            // Look for data after the command - check if there's data in the SQL
            // Format: COPY table FROM STDIN;\ndata\n\.
            let sql_with_data = sql;
            let data_start = sql_with_data.find('\n');

            if data_start.is_none() {
                // No inline data - return instruction message
                return Err(user_error(
                    "COPY FROM STDIN requires data. Format:\nCOPY table FROM STDIN;\nrow1col1,row1col2\nrow2col1,row2col2\n\\.".to_string(),
                ));
            }

            let data_section = &sql_with_data[data_start.unwrap() + 1..];

            // Parse CSV lines (terminated by \. or end of input)
            let mut csv_rows: Vec<Vec<String>> = Vec::new();
            let mut skip_header = with_header;

            for line in data_section.lines() {
                let trimmed = line.trim();
                if trimmed == "\\." || trimmed.is_empty() {
                    continue;
                }
                if skip_header {
                    skip_header = false;
                    continue;
                }

                // Parse CSV row (simple parsing - handles quotes)
                let row = parse_csv_row(trimmed, delimiter);
                csv_rows.push(row);
            }

            if csv_rows.is_empty() {
                return Ok(vec![Response::Execution(pgwire::api::results::Tag::new(
                    "COPY 0",
                ))]);
            }

            // Get table schema
            let (schema, mut existing_batches) = {
                let tables = self.tables.read().await;
                let entry = tables
                    .get(table_name)
                    .ok_or_else(|| user_error(format!("Table not found: {}", table_name)))?;
                let batches = entry
                    .source
                    .scan()
                    .map_err(|e| user_error(format!("Failed to read table: {}", e)))?;
                (entry.schema.clone(), batches)
            };

            let row_count = csv_rows.len();

            // Build arrays from CSV data
            use arrow::array::{
                ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
            };

            let mut arrays: Vec<ArrayRef> = Vec::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                match field.data_type() {
                    query_core::DataType::Int64 => {
                        let mut builder = Int64Builder::new();
                        for row in &csv_rows {
                            if col_idx < row.len() && !row[col_idx].is_empty() {
                                let val = row[col_idx].parse::<i64>().unwrap_or(0);
                                builder.append_value(val);
                            } else {
                                builder.append_null();
                            }
                        }
                        arrays.push(Arc::new(builder.finish()) as ArrayRef);
                    }
                    query_core::DataType::Float64 => {
                        let mut builder = Float64Builder::new();
                        for row in &csv_rows {
                            if col_idx < row.len() && !row[col_idx].is_empty() {
                                let val = row[col_idx].parse::<f64>().unwrap_or(0.0);
                                builder.append_value(val);
                            } else {
                                builder.append_null();
                            }
                        }
                        arrays.push(Arc::new(builder.finish()) as ArrayRef);
                    }
                    query_core::DataType::Utf8 => {
                        let mut builder = StringBuilder::new();
                        for row in &csv_rows {
                            if col_idx < row.len() {
                                builder.append_value(&row[col_idx]);
                            } else {
                                builder.append_null();
                            }
                        }
                        arrays.push(Arc::new(builder.finish()) as ArrayRef);
                    }
                    query_core::DataType::Boolean => {
                        let mut builder = BooleanBuilder::new();
                        for row in &csv_rows {
                            if col_idx < row.len() && !row[col_idx].is_empty() {
                                let val = matches!(
                                    row[col_idx].to_lowercase().as_str(),
                                    "true" | "t" | "1" | "yes" | "y"
                                );
                                builder.append_value(val);
                            } else {
                                builder.append_null();
                            }
                        }
                        arrays.push(Arc::new(builder.finish()) as ArrayRef);
                    }
                    _ => {
                        // Default to string for unknown types
                        let mut builder = StringBuilder::new();
                        for row in &csv_rows {
                            if col_idx < row.len() {
                                builder.append_value(&row[col_idx]);
                            } else {
                                builder.append_null();
                            }
                        }
                        arrays.push(Arc::new(builder.finish()) as ArrayRef);
                    }
                }
            }

            let arrow_schema = Arc::new(schema.to_arrow());
            let new_batch = RecordBatch::try_new(arrow_schema, arrays)
                .map_err(|e| user_error(format!("Failed to create batch: {}", e)))?;

            // Add new batch to existing data
            existing_batches.push(new_batch);

            // Create new data source
            let new_source = Arc::new(MemoryDataSource::new(schema.clone(), existing_batches));

            // Update table
            let mut tables = self.tables.write().await;
            tables.insert(
                table_name.to_string(),
                TableEntry {
                    schema,
                    source: new_source,
                },
            );
            info!("COPY: imported {} rows into {}", row_count, table_name);

            Ok(vec![Response::Execution(pgwire::api::results::Tag::new(
                &format!("COPY {}", row_count),
            ))])
        } else {
            Err(user_error(
                "Invalid COPY syntax. Use: COPY table TO STDOUT or COPY table FROM STDIN"
                    .to_string(),
            ))
        }
    }

    /// Handle DECLARE cursor command
    /// Syntax: DECLARE cursor_name CURSOR FOR SELECT ...
    async fn handle_declare_cursor<'a>(&self, sql: &str) -> PgWireResult<Response<'a>> {
        let sql_upper = sql.to_uppercase();

        // Parse: DECLARE cursor_name CURSOR FOR query
        let cursor_pos = sql_upper.find(" CURSOR ");
        if cursor_pos.is_none() {
            return Err(user_error(
                "Invalid DECLARE syntax. Use: DECLARE cursor_name CURSOR FOR SELECT ..."
                    .to_string(),
            ));
        }

        // Extract cursor name
        let name_start = "DECLARE ".len();
        let name_end = cursor_pos.unwrap();
        let cursor_name = sql[name_start..name_end].trim().to_string();

        // Extract the query after "CURSOR FOR"
        let for_pos = sql_upper.find(" FOR ");
        if for_pos.is_none() {
            return Err(user_error(
                "Invalid DECLARE syntax. Missing FOR clause.".to_string(),
            ));
        }

        let query_start = for_pos.unwrap() + 5; // " FOR " length
        let query = &sql[query_start..];

        // Execute the query and store results in cursor
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

        let query_owned = query.to_string();
        let batches = tokio::task::spawn_blocking(move || {
            Self::execute_query_sync(&query_owned, tables_snapshot)
        })
        .await
        .map_err(|e| user_error(format!("Task join error: {}", e)))?
        .map_err(|e| user_error(e))?;

        // Declare the cursor
        self.cursors.declare(&cursor_name, batches).await;

        Ok(Response::Execution(pgwire::api::results::Tag::new(
            "DECLARE CURSOR",
        )))
    }

    /// Handle FETCH from cursor command
    /// Syntax: FETCH [count] FROM cursor_name
    async fn handle_fetch_cursor(&self, sql: &str) -> PgWireResult<Vec<Response<'static>>> {
        let parts: Vec<&str> = sql.split_whitespace().collect();

        // Parse: FETCH [count] FROM cursor_name
        // Or: FETCH ALL FROM cursor_name
        // Or: FETCH cursor_name (fetch 1)
        let (count, cursor_name) = if parts.len() == 2 {
            // FETCH cursor_name
            (1usize, parts[1].to_string())
        } else if parts.len() >= 4 && parts[2].to_uppercase() == "FROM" {
            // FETCH count FROM cursor_name or FETCH ALL FROM cursor_name
            let count = if parts[1].to_uppercase() == "ALL" {
                usize::MAX
            } else {
                parts[1].parse::<usize>().unwrap_or(1)
            };
            (count, parts[3].to_string())
        } else if parts.len() == 3 && parts[1].to_uppercase() == "FROM" {
            // FETCH FROM cursor_name (fetch 1)
            (1usize, parts[2].to_string())
        } else {
            return Err(user_error(
                "Invalid FETCH syntax. Use: FETCH [count] FROM cursor_name".to_string(),
            ));
        };

        // Fetch from cursor
        let batches = self.cursors.fetch(&cursor_name, count).await;
        if batches.is_none() {
            return Err(user_error(format!("Cursor not found: {}", cursor_name)));
        }

        let batches = batches.unwrap();
        if batches.is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }

        // Build response
        let schema = batches[0].schema();
        let field_info = schema_to_field_info(&schema);

        let mut all_rows = Vec::new();
        for batch in &batches {
            let rows = record_batch_to_rows(batch, &field_info)?;
            for row in rows {
                all_rows.push(row.finish());
            }
        }

        Ok(vec![Response::Query(QueryResponse::new(
            Arc::new(field_info),
            futures::stream::iter(all_rows),
        ))])
    }

    /// Handle CLOSE cursor command
    /// Syntax: CLOSE cursor_name
    async fn handle_close_cursor<'a>(&self, sql: &str) -> PgWireResult<Response<'a>> {
        let parts: Vec<&str> = sql.split_whitespace().collect();

        if parts.len() != 2 {
            return Err(user_error(
                "Invalid CLOSE syntax. Use: CLOSE cursor_name".to_string(),
            ));
        }

        let cursor_name = parts[1];

        if cursor_name.to_uppercase() == "ALL" {
            // CLOSE ALL - would need additional implementation
            return Ok(Response::Execution(pgwire::api::results::Tag::new(
                "CLOSE CURSOR",
            )));
        }

        if !self.cursors.close(cursor_name).await {
            return Err(user_error(format!("Cursor not found: {}", cursor_name)));
        }

        Ok(Response::Execution(pgwire::api::results::Tag::new(
            "CLOSE CURSOR",
        )))
    }
}

/// Parse a CSV row, handling quoted fields
fn parse_csv_row(line: &str, delimiter: char) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current_field = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(c) = chars.next() {
        if in_quotes {
            if c == '"' {
                // Check for escaped quote
                if chars.peek() == Some(&'"') {
                    current_field.push('"');
                    chars.next(); // consume the second quote
                } else {
                    in_quotes = false;
                }
            } else {
                current_field.push(c);
            }
        } else {
            if c == '"' {
                in_quotes = true;
            } else if c == delimiter {
                fields.push(current_field.trim().to_string());
                current_field = String::new();
            } else {
                current_field.push(c);
            }
        }
    }

    // Don't forget the last field
    fields.push(current_field.trim().to_string());

    fields
}

/// Simple startup handler that accepts all connections (no authentication)
pub struct SimpleStartupHandler;

impl NoopStartupHandler for SimpleStartupHandler {}

// ============================================================================
// Server Handlers - No Authentication
// ============================================================================

/// Server handlers for PostgreSQL protocol (without authentication)
pub struct QueryServerHandlers {
    startup_handler: Arc<SimpleStartupHandler>,
    simple_query_handler: Arc<QueryBackend>,
    extended_query_handler: Arc<QueryExtendedHandler>,
    copy_handler: Arc<NoopCopyHandler>,
    error_handler: Arc<NoopErrorHandler>,
}

impl QueryServerHandlers {
    pub fn new(backend: Arc<QueryBackend>) -> Self {
        let tables = backend.tables();
        Self {
            startup_handler: Arc::new(SimpleStartupHandler),
            simple_query_handler: backend,
            extended_query_handler: Arc::new(QueryExtendedHandler::new(tables)),
            copy_handler: Arc::new(NoopCopyHandler),
            error_handler: Arc::new(NoopErrorHandler),
        }
    }
}

impl PgWireServerHandlers for QueryServerHandlers {
    type StartupHandler = SimpleStartupHandler;
    type SimpleQueryHandler = QueryBackend;
    type ExtendedQueryHandler = QueryExtendedHandler;
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

// ============================================================================
// Server Handlers - With MD5 Authentication
// ============================================================================

use crate::auth::QueryAuthSource;

/// Server handlers for PostgreSQL protocol with MD5 password authentication
pub struct AuthQueryServerHandlers {
    startup_handler:
        Arc<Md5PasswordAuthStartupHandler<QueryAuthSource, DefaultServerParameterProvider>>,
    simple_query_handler: Arc<QueryBackend>,
    extended_query_handler: Arc<QueryExtendedHandler>,
    copy_handler: Arc<NoopCopyHandler>,
    error_handler: Arc<NoopErrorHandler>,
}

impl AuthQueryServerHandlers {
    pub fn new(backend: Arc<QueryBackend>, auth_config: AuthConfig) -> Self {
        let tables = backend.tables();
        Self {
            startup_handler: Arc::new(create_md5_auth_handler(auth_config)),
            simple_query_handler: backend,
            extended_query_handler: Arc::new(QueryExtendedHandler::new(tables)),
            copy_handler: Arc::new(NoopCopyHandler),
            error_handler: Arc::new(NoopErrorHandler),
        }
    }
}

impl PgWireServerHandlers for AuthQueryServerHandlers {
    type StartupHandler =
        Md5PasswordAuthStartupHandler<QueryAuthSource, DefaultServerParameterProvider>;
    type SimpleQueryHandler = QueryBackend;
    type ExtendedQueryHandler = QueryExtendedHandler;
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
