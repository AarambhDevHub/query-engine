//! Extended Query Protocol support for PostgreSQL wire protocol
//!
//! This module implements prepared statements and parameter binding for
//! PostgreSQL clients using the extended query protocol.
//!
//! # Extended Query Protocol
//!
//! The extended query protocol allows:
//! - Prepared statements (parse once, execute many times)
//! - Parameter binding ($1, $2, etc.)
//! - Binary data transfer
//! - Statement caching

use crate::backend::TableEntry;
use crate::result::{record_batch_to_rows, schema_to_field_info};
use async_trait::async_trait;
use pgwire::api::portal::Portal;
use pgwire::api::query::ExtendedQueryHandler;
use pgwire::api::results::{
    DescribePortalResponse, DescribeStatementResponse, FieldInfo, QueryResponse, Response,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use query_core::Schema;
use query_executor::QueryExecutor;
use query_executor::physical_plan::{AggregateExpr, DataSource, PhysicalExpr, PhysicalPlan};
use query_parser::Parser;
use query_planner::{LogicalExpr, LogicalPlan, Optimizer, Planner};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error};

/// Extended query handler for Query Engine
pub struct QueryExtendedHandler {
    /// Shared table storage
    tables: Arc<RwLock<HashMap<String, TableEntry>>>,
    /// Query parser for prepared statements
    query_parser: Arc<NoopQueryParser>,
}

impl QueryExtendedHandler {
    /// Create a new extended query handler with shared tables
    pub fn new(tables: Arc<RwLock<HashMap<String, TableEntry>>>) -> Self {
        Self {
            tables,
            query_parser: Arc::new(NoopQueryParser::new()),
        }
    }

    /// Execute a SQL query with optional parameter substitution
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

        // Execute query
        let executor = QueryExecutor::new();
        let rt = tokio::runtime::Handle::current();
        rt.block_on(async { executor.execute(&physical_plan).await })
            .map_err(|e| format!("Execution error: {}", e))
    }

    /// Get the schema for a SQL statement
    fn get_statement_schema(
        sql: &str,
        tables: &HashMap<String, TableEntry>,
    ) -> Result<Vec<FieldInfo>, String> {
        // Parse SQL
        let mut parser = Parser::new(sql).map_err(|e| format!("Parse error: {}", e))?;
        let statement = parser.parse().map_err(|e| format!("Parse error: {}", e))?;

        // Create planner and register tables
        let mut planner = Planner::new();
        for (name, entry) in tables.iter() {
            planner.register_table(name, entry.schema.clone());
        }

        // Create logical plan
        let logical_plan = planner
            .create_logical_plan(&statement)
            .map_err(|e| format!("Planning error: {}", e))?;

        // Get schema from logical plan
        let schema = get_plan_schema(&logical_plan);

        // Convert to FieldInfo
        Ok(schema_to_field_info(&schema.to_arrow()))
    }
}

/// Create a user error with proper ErrorInfo
fn user_error(message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "42000".to_string(),
        message,
    )))
}

/// Extract parameters from a Portal and convert to strings for substitution
fn extract_params(portal: &Portal<String>) -> Vec<String> {
    let mut params = Vec::with_capacity(portal.parameter_len());

    for i in 0..portal.parameter_len() {
        // Get parameter type from statement, defaulting to UNKNOWN
        let param_type = portal
            .statement
            .parameter_types
            .get(i)
            .cloned()
            .unwrap_or(Type::UNKNOWN);

        // Extract parameter value based on type
        // portal.parameter returns Result<Option<T>, PgWireError>
        let value = match &param_type {
            t if *t == Type::BOOL => portal
                .parameter::<bool>(i, &param_type)
                .ok()
                .flatten()
                .map(|v| v.to_string())
                .unwrap_or_else(|| "NULL".to_string()),
            t if *t == Type::INT2 => portal
                .parameter::<i16>(i, &param_type)
                .ok()
                .flatten()
                .map(|v| v.to_string())
                .unwrap_or_else(|| "NULL".to_string()),
            t if *t == Type::INT4 => portal
                .parameter::<i32>(i, &param_type)
                .ok()
                .flatten()
                .map(|v| v.to_string())
                .unwrap_or_else(|| "NULL".to_string()),
            t if *t == Type::INT8 => portal
                .parameter::<i64>(i, &param_type)
                .ok()
                .flatten()
                .map(|v| v.to_string())
                .unwrap_or_else(|| "NULL".to_string()),
            t if *t == Type::FLOAT4 => portal
                .parameter::<f32>(i, &param_type)
                .ok()
                .flatten()
                .map(|v| v.to_string())
                .unwrap_or_else(|| "NULL".to_string()),
            t if *t == Type::FLOAT8 => portal
                .parameter::<f64>(i, &param_type)
                .ok()
                .flatten()
                .map(|v| v.to_string())
                .unwrap_or_else(|| "NULL".to_string()),
            t if *t == Type::TEXT || *t == Type::VARCHAR => portal
                .parameter::<String>(i, &param_type)
                .ok()
                .flatten()
                .map(|v| format!("'{}'", v.replace('\'', "''")))
                .unwrap_or_else(|| "NULL".to_string()),
            _ => {
                // Try to get as string for unknown types
                portal
                    .parameter::<String>(i, &param_type)
                    .ok()
                    .flatten()
                    .map(|v| format!("'{}'", v.replace('\'', "''")))
                    .unwrap_or_else(|| "NULL".to_string())
            }
        };

        params.push(value);
    }

    params
}

/// Substitute $1, $2, etc. placeholders with actual parameter values
fn substitute_params(sql: &str, params: &[String]) -> String {
    let mut result = sql.to_string();

    // Replace parameters in reverse order to handle $10, $11, etc. correctly
    for (i, param) in params.iter().enumerate().rev() {
        let placeholder = format!("${}", i + 1);
        result = result.replace(&placeholder, param);
    }

    result
}

#[async_trait]
impl ExtendedQueryHandler for QueryExtendedHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<'a, 'b, C>(
        &'b self,
        _client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        'b: 'a,
        C: ClientInfo + Unpin + Send + Sync,
    {
        let query = &portal.statement.statement;
        debug!("Extended query: {}", query);

        // Extract parameters
        let params = extract_params(portal);

        // Substitute parameters into the query
        let final_query = if params.is_empty() {
            query.clone()
        } else {
            substitute_params(query, &params)
        };

        debug!("Final query after param substitution: {}", final_query);

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

        // Run query in blocking task
        let batches = tokio::task::spawn_blocking(move || {
            Self::execute_query_sync(&final_query, tables_snapshot)
        })
        .await
        .map_err(|e| user_error(format!("Task join error: {}", e)))?
        .map_err(|e| {
            error!("Query error: {}", e);
            user_error(e)
        })?;

        // Build response
        if batches.is_empty() {
            return Ok(Response::EmptyQuery);
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
        debug!("Extended query returned {} rows", row_count);

        Ok(Response::Query(QueryResponse::new(
            Arc::new(field_info),
            futures::stream::iter(all_rows),
        )))
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let sql = &stmt.statement;
        debug!("Describe statement: {}", sql);

        // Get tables snapshot
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

        // Get parameter types from stored statement
        let param_types: Vec<Type> = stmt.parameter_types.clone();

        // Get result columns
        let fields = Self::get_statement_schema(sql, &tables_snapshot).map_err(user_error)?;

        Ok(DescribeStatementResponse::new(param_types, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let sql = &portal.statement.statement;
        debug!("Describe portal: {}", sql);

        // Get tables snapshot
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

        // Get result columns with proper format
        let fields = Self::get_statement_schema(sql, &tables_snapshot).map_err(user_error)?;

        // Apply result column format from portal
        let fields: Vec<FieldInfo> = fields
            .into_iter()
            .enumerate()
            .map(|(i, f)| {
                FieldInfo::new(
                    f.name().to_string(),
                    f.table_id(),
                    f.column_id(),
                    f.datatype().clone(),
                    portal.result_column_format.format_for(i),
                )
            })
            .collect();

        Ok(DescribePortalResponse::new(fields))
    }
}

/// Get schema from a logical plan
fn get_plan_schema(plan: &LogicalPlan) -> Schema {
    match plan {
        LogicalPlan::TableScan { schema, .. } => schema.clone(),
        LogicalPlan::Filter { input, .. } => get_plan_schema(input),
        LogicalPlan::Projection { schema, .. } => schema.clone(),
        LogicalPlan::Limit { input, .. } => get_plan_schema(input),
        LogicalPlan::Sort { input, .. } => get_plan_schema(input),
        LogicalPlan::Aggregate { schema, .. } => schema.clone(),
        LogicalPlan::Join { schema, .. } => schema.clone(),
        _ => Schema::empty(),
    }
}

/// Convert logical plan to physical plan (duplicated from backend.rs for now)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_substitute_params() {
        let sql = "SELECT * FROM users WHERE age > $1 AND name = $2";
        let params = vec!["25".to_string(), "'Alice'".to_string()];
        let result = substitute_params(sql, &params);
        assert_eq!(
            result,
            "SELECT * FROM users WHERE age > 25 AND name = 'Alice'"
        );
    }

    #[test]
    fn test_substitute_params_multiple_digits() {
        let sql = "SELECT $1, $2, $10, $11";
        let params: Vec<String> = (1..=11).map(|i| i.to_string()).collect();
        let result = substitute_params(sql, &params);
        assert_eq!(result, "SELECT 1, 2, 10, 11");
    }

    #[test]
    fn test_substitute_params_empty() {
        let sql = "SELECT * FROM users";
        let params: Vec<String> = vec![];
        let result = substitute_params(sql, &params);
        assert_eq!(result, "SELECT * FROM users");
    }
}
