use crate::logical_plan::{LogicalExpr, LogicalPlan, ScalarValue};
use query_core::{Field, QueryError, Result, Schema};
use query_parser::{Expr, Literal, SelectItem, Statement};
use std::collections::HashMap;
use std::sync::Arc;

pub struct Planner {
    schemas: HashMap<String, Schema>,
}

impl Planner {
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
        }
    }

    pub fn register_table(&mut self, name: impl Into<String>, schema: Schema) {
        self.schemas.insert(name.into(), schema);
    }

    pub fn create_logical_plan(&self, statement: &Statement) -> Result<LogicalPlan> {
        match statement {
            Statement::Select(select) => self.plan_select(select),
        }
    }

    fn plan_select(&self, select: &query_parser::SelectStatement) -> Result<LogicalPlan> {
        let mut table_aliases = HashMap::new();

        let mut plan = if let Some(table_ref) = &select.from {
            let schema = self
                .schemas
                .get(&table_ref.name)
                .ok_or_else(|| QueryError::TableNotFound(table_ref.name.clone()))?
                .clone();

            // Register table alias
            if let Some(alias) = &table_ref.alias {
                table_aliases.insert(alias.clone(), table_ref.name.clone());
            }
            table_aliases.insert(table_ref.name.clone(), table_ref.name.clone());

            // Create schema with prefixed column names for JOINs
            let prefixed_schema = self.prefix_schema_with_table(&schema, &table_ref.name);

            LogicalPlan::TableScan {
                table_name: table_ref.name.clone(),
                schema: prefixed_schema,
                projection: None,
            }
        } else {
            LogicalPlan::EmptyRelation {
                schema: Schema::empty(),
            }
        };

        // Process JOINs
        for join in &select.joins {
            let right_schema = self
                .schemas
                .get(&join.right.name)
                .ok_or_else(|| QueryError::TableNotFound(join.right.name.clone()))?
                .clone();

            // Register right table alias
            if let Some(alias) = &join.right.alias {
                table_aliases.insert(alias.clone(), join.right.name.clone());
            }
            table_aliases.insert(join.right.name.clone(), join.right.name.clone());

            // Create schema with prefixed column names for JOINs
            let prefixed_right_schema =
                self.prefix_schema_with_table(&right_schema, &join.right.name);

            let right_plan = LogicalPlan::TableScan {
                table_name: join.right.name.clone(),
                schema: prefixed_right_schema.clone(),
                projection: None,
            };

            // Build joined schema
            let left_schema = plan.schema().clone();
            let joined_schema = self.merge_schemas(&left_schema, &prefixed_right_schema)?;

            // Parse ON condition if exists
            let on_expr = if let Some(on) = &join.on {
                Some(self.create_logical_expr_with_context(on, &joined_schema, &table_aliases)?)
            } else {
                None
            };

            plan = LogicalPlan::Join {
                left: Arc::new(plan),
                right: Arc::new(right_plan),
                join_type: join.join_type,
                on: on_expr,
                schema: joined_schema,
            };
        }

        let base_schema = plan.schema().clone();

        // Apply WHERE clause
        if let Some(selection) = &select.selection {
            let predicate =
                self.create_logical_expr_with_context(selection, &base_schema, &table_aliases)?;
            plan = LogicalPlan::Filter {
                input: Arc::new(plan),
                predicate,
            };
        }

        // Check if we have aggregates
        let has_aggregates = !select.group_by.is_empty() || self.has_aggregates(&select.projection);

        if has_aggregates {
            // Build GROUP BY expressions
            let group_exprs_result: Result<Vec<_>> = select
                .group_by
                .iter()
                .map(|e| self.create_logical_expr_with_context(e, &base_schema, &table_aliases))
                .collect();
            let group_exprs = group_exprs_result?;

            // Build aggregate expressions and collect all projection items
            let mut aggr_exprs = Vec::new();
            let mut result_fields = Vec::new();

            for item in &select.projection {
                match item {
                    SelectItem::Wildcard => {
                        for field in base_schema.fields().iter() {
                            result_fields.push(field.clone());
                        }
                    }
                    SelectItem::QualifiedWildcard(table) => {
                        // Find all fields belonging to this table
                        let actual_table = table_aliases.get(table).unwrap_or(table);
                        for field in base_schema.fields().iter() {
                            if field.name().starts_with(&format!("{}.", actual_table)) {
                                result_fields.push(field.clone());
                            }
                        }
                    }
                    SelectItem::UnnamedExpr(expr) => {
                        if self.is_aggregate(expr) {
                            let logical_expr = self.create_logical_expr_with_context(
                                expr,
                                &base_schema,
                                &table_aliases,
                            )?;
                            let field = self.aggregate_expr_to_field(&logical_expr, "aggregate")?;
                            aggr_exprs.push(logical_expr);
                            result_fields.push(field);
                        } else {
                            let logical_expr = self.create_logical_expr_with_context(
                                expr,
                                &base_schema,
                                &table_aliases,
                            )?;
                            let field = self.expr_to_field(&logical_expr, &base_schema)?;
                            result_fields.push(field);
                        }
                    }
                    SelectItem::ExprWithAlias { expr, alias } => {
                        if self.is_aggregate(expr) {
                            let logical_expr = self.create_logical_expr_with_context(
                                expr,
                                &base_schema,
                                &table_aliases,
                            )?;
                            let data_type = query_core::DataType::Float64;
                            let field = Field::new(alias, data_type, true);
                            aggr_exprs.push(logical_expr);
                            result_fields.push(field);
                        } else {
                            let logical_expr = self.create_logical_expr_with_context(
                                expr,
                                &base_schema,
                                &table_aliases,
                            )?;
                            let data_type = self.expr_data_type(&logical_expr, &base_schema)?;
                            let field = Field::new(alias, data_type, true);
                            result_fields.push(field);
                        }
                    }
                }
            }

            let schema = Schema::new(result_fields);

            plan = LogicalPlan::Aggregate {
                input: Arc::new(plan),
                group_exprs,
                aggr_exprs,
                schema,
            };
        } else {
            // No aggregates - apply normal projection
            let (proj_exprs, proj_schema) = self.plan_projection_with_context(
                &select.projection,
                plan.schema(),
                &table_aliases,
            )?;
            plan = LogicalPlan::Projection {
                input: Arc::new(plan),
                exprs: proj_exprs,
                schema: proj_schema,
            };
        }

        // Apply ORDER BY
        if !select.order_by.is_empty() {
            let exprs: Result<Vec<_>> = select
                .order_by
                .iter()
                .map(|order| {
                    self.create_logical_expr_with_context(
                        &order.expr,
                        plan.schema(),
                        &table_aliases,
                    )
                })
                .collect();
            let ascending: Vec<bool> = select.order_by.iter().map(|order| order.asc).collect();

            plan = LogicalPlan::Sort {
                input: Arc::new(plan),
                exprs: exprs?,
                ascending,
            };
        }

        // Apply LIMIT and OFFSET
        if select.limit.is_some() || select.offset.is_some() {
            plan = LogicalPlan::Limit {
                input: Arc::new(plan),
                skip: select.offset.unwrap_or(0),
                fetch: select.limit,
            };
        }

        Ok(plan)
    }

    fn prefix_schema_with_table(&self, schema: &Schema, table_name: &str) -> Schema {
        let prefixed_fields: Vec<Field> = schema
            .fields()
            .iter()
            .map(|field| {
                Field::new(
                    format!("{}.{}", table_name, field.name()),
                    field.data_type().clone(),
                    field.nullable(),
                )
            })
            .collect();

        Schema::new(prefixed_fields)
    }

    fn merge_schemas(&self, left: &Schema, right: &Schema) -> Result<Schema> {
        let mut fields = Vec::new();

        // Add all fields from left schema
        for field in left.fields() {
            fields.push(field.clone());
        }

        // Add all fields from right schema
        for field in right.fields() {
            fields.push(field.clone());
        }

        Ok(Schema::new(fields))
    }

    fn create_logical_expr_with_context(
        &self,
        expr: &Expr,
        schema: &Schema,
        table_aliases: &HashMap<String, String>,
    ) -> Result<LogicalExpr> {
        match expr {
            Expr::Column(name) => {
                // Try to find the column in schema (could be unqualified)
                let index = schema.index_of(name).or_else(|_| {
                    // If not found directly, try to find it with any table prefix
                    for field in schema.fields() {
                        if field.name().ends_with(&format!(".{}", name)) {
                            return schema.index_of(field.name());
                        }
                    }
                    Err(QueryError::ColumnNotFound(name.clone()))
                })?;

                let field_name = schema
                    .field(index)
                    .ok_or_else(|| QueryError::ColumnNotFound(name.clone()))?
                    .name()
                    .to_string();

                Ok(LogicalExpr::Column {
                    name: field_name,
                    index,
                })
            }
            Expr::QualifiedColumn { table, column } => {
                // Resolve table alias
                let actual_table = table_aliases.get(table).unwrap_or(table);
                let qualified_name = format!("{}.{}", actual_table, column);

                // Try to find the qualified column
                let index = schema.index_of(&qualified_name).or_else(|_| {
                    // Fallback: try to find by column name only
                    schema.index_of(column).or_else(|_| {
                        // Last resort: find any field ending with this column name
                        for field in schema.fields() {
                            if field.name().ends_with(&format!(".{}", column)) {
                                return schema.index_of(field.name());
                            }
                        }
                        Err(QueryError::ColumnNotFound(qualified_name.clone()))
                    })
                })?;

                let field_name = schema
                    .field(index)
                    .ok_or_else(|| QueryError::ColumnNotFound(qualified_name.clone()))?
                    .name()
                    .to_string();

                Ok(LogicalExpr::Column {
                    name: field_name,
                    index,
                })
            }
            Expr::Literal(lit) => Ok(LogicalExpr::Literal(self.create_scalar_value(lit)?)),
            Expr::BinaryOp { left, op, right } => Ok(LogicalExpr::BinaryExpr {
                left: Box::new(self.create_logical_expr_with_context(
                    left,
                    schema,
                    table_aliases,
                )?),
                op: *op,
                right: Box::new(self.create_logical_expr_with_context(
                    right,
                    schema,
                    table_aliases,
                )?),
            }),
            Expr::UnaryOp { op, expr } => Ok(LogicalExpr::UnaryExpr {
                op: *op,
                expr: Box::new(self.create_logical_expr_with_context(
                    expr,
                    schema,
                    table_aliases,
                )?),
            }),
            Expr::AggregateFunction { func, expr } => Ok(LogicalExpr::AggregateFunction {
                func: *func,
                expr: Box::new(self.create_logical_expr_with_context(
                    expr,
                    schema,
                    table_aliases,
                )?),
            }),
            Expr::Cast { expr, data_type } => Ok(LogicalExpr::Cast {
                expr: Box::new(self.create_logical_expr_with_context(
                    expr,
                    schema,
                    table_aliases,
                )?),
                data_type: data_type.clone(),
            }),
        }
    }

    fn plan_projection_with_context(
        &self,
        items: &[SelectItem],
        input_schema: &Schema,
        table_aliases: &HashMap<String, String>,
    ) -> Result<(Vec<LogicalExpr>, Schema)> {
        let mut exprs = Vec::new();
        let mut fields = Vec::new();

        for item in items {
            match item {
                SelectItem::Wildcard => {
                    for (i, field) in input_schema.fields().iter().enumerate() {
                        exprs.push(LogicalExpr::Column {
                            name: field.name().to_string(),
                            index: i,
                        });
                        fields.push(field.clone());
                    }
                }
                SelectItem::QualifiedWildcard(table) => {
                    let actual_table = table_aliases.get(table).unwrap_or(table);
                    for (i, field) in input_schema.fields().iter().enumerate() {
                        if field.name().starts_with(&format!("{}.", actual_table)) {
                            exprs.push(LogicalExpr::Column {
                                name: field.name().to_string(),
                                index: i,
                            });
                            fields.push(field.clone());
                        }
                    }
                }
                SelectItem::UnnamedExpr(expr) => {
                    let logical_expr =
                        self.create_logical_expr_with_context(expr, input_schema, table_aliases)?;
                    let field = self.expr_to_field(&logical_expr, input_schema)?;
                    exprs.push(logical_expr);
                    fields.push(field);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let mut logical_expr =
                        self.create_logical_expr_with_context(expr, input_schema, table_aliases)?;
                    let data_type = self.expr_data_type(&logical_expr, input_schema)?;
                    logical_expr = LogicalExpr::Alias {
                        expr: Box::new(logical_expr),
                        alias: alias.clone(),
                    };
                    let field = Field::new(alias, data_type, true);
                    exprs.push(logical_expr);
                    fields.push(field);
                }
            }
        }

        Ok((exprs, Schema::new(fields)))
    }

    fn _create_logical_expr(&self, expr: &Expr, schema: &Schema) -> Result<LogicalExpr> {
        match expr {
            Expr::Column(name) => {
                let index = schema.index_of(name)?;
                Ok(LogicalExpr::Column {
                    name: name.clone(),
                    index,
                })
            }
            Expr::QualifiedColumn { table, column } => {
                let qualified_name = format!("{}.{}", table, column);
                let index = schema
                    .index_of(&qualified_name)
                    .or_else(|_| schema.index_of(column))?;
                Ok(LogicalExpr::Column {
                    name: qualified_name,
                    index,
                })
            }
            Expr::Literal(lit) => Ok(LogicalExpr::Literal(self.create_scalar_value(lit)?)),
            Expr::BinaryOp { left, op, right } => Ok(LogicalExpr::BinaryExpr {
                left: Box::new(self._create_logical_expr(left, schema)?),
                op: *op,
                right: Box::new(self._create_logical_expr(right, schema)?),
            }),
            Expr::UnaryOp { op, expr } => Ok(LogicalExpr::UnaryExpr {
                op: *op,
                expr: Box::new(self._create_logical_expr(expr, schema)?),
            }),
            Expr::AggregateFunction { func, expr } => Ok(LogicalExpr::AggregateFunction {
                func: *func,
                expr: Box::new(self._create_logical_expr(expr, schema)?),
            }),
            Expr::Cast { expr, data_type } => Ok(LogicalExpr::Cast {
                expr: Box::new(self._create_logical_expr(expr, schema)?),
                data_type: data_type.clone(),
            }),
        }
    }

    fn create_scalar_value(&self, lit: &Literal) -> Result<ScalarValue> {
        match lit {
            Literal::Number(n) => {
                if n.contains('.') {
                    Ok(ScalarValue::Float64(Some(n.parse().map_err(|_| {
                        QueryError::ParseError(format!("Invalid float: {}", n))
                    })?)))
                } else {
                    Ok(ScalarValue::Int64(Some(n.parse().map_err(|_| {
                        QueryError::ParseError(format!("Invalid integer: {}", n))
                    })?)))
                }
            }
            Literal::String(s) => Ok(ScalarValue::Utf8(Some(s.clone()))),
            Literal::Boolean(b) => Ok(ScalarValue::Boolean(Some(*b))),
            Literal::Null => Ok(ScalarValue::Null),
        }
    }

    fn has_aggregates(&self, items: &[SelectItem]) -> bool {
        items.iter().any(|item| match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                self.is_aggregate(expr)
            }
            _ => false,
        })
    }

    fn is_aggregate(&self, expr: &Expr) -> bool {
        matches!(expr, Expr::AggregateFunction { .. })
    }

    fn expr_to_field(&self, expr: &LogicalExpr, schema: &Schema) -> Result<Field> {
        match expr {
            LogicalExpr::Column { name, .. } => Ok(schema.field_with_name(name)?.clone()),
            LogicalExpr::Alias { alias, expr } => {
                let data_type = self.expr_data_type(expr, schema)?;
                Ok(Field::new(alias, data_type, true))
            }
            LogicalExpr::AggregateFunction { .. } => {
                Ok(Field::new("aggregate", query_core::DataType::Float64, true))
            }
            _ => {
                let data_type = self.expr_data_type(expr, schema)?;
                Ok(Field::new("?column?", data_type, true))
            }
        }
    }

    fn aggregate_expr_to_field(&self, expr: &LogicalExpr, default_name: &str) -> Result<Field> {
        match expr {
            LogicalExpr::AggregateFunction { func, .. } => {
                let name = format!("{:?}", func).to_lowercase();
                Ok(Field::new(name, query_core::DataType::Float64, true))
            }
            LogicalExpr::Alias { alias, .. } => {
                Ok(Field::new(alias, query_core::DataType::Float64, true))
            }
            _ => Ok(Field::new(
                default_name,
                query_core::DataType::Float64,
                true,
            )),
        }
    }

    fn expr_data_type(&self, expr: &LogicalExpr, schema: &Schema) -> Result<query_core::DataType> {
        use query_core::DataType;

        match expr {
            LogicalExpr::Column { index, .. } => {
                Ok(schema.field(*index).unwrap().data_type().clone())
            }
            LogicalExpr::Literal(val) => Ok(self.scalar_value_type(val)),
            LogicalExpr::BinaryExpr { left, right, .. } => {
                let left_type = self.expr_data_type(left, schema)?;
                let right_type = self.expr_data_type(right, schema)?;
                Ok(self.coerce_types(left_type, right_type))
            }
            LogicalExpr::UnaryExpr { expr, .. } => self.expr_data_type(expr, schema),
            LogicalExpr::AggregateFunction { .. } => Ok(DataType::Float64),
            LogicalExpr::Cast { data_type, .. } => Ok(data_type.clone()),
            LogicalExpr::Alias { expr, .. } => self.expr_data_type(expr, schema),
        }
    }

    fn scalar_value_type(&self, val: &ScalarValue) -> query_core::DataType {
        use query_core::DataType;

        match val {
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::Int8(_) => DataType::Int8,
            ScalarValue::Int16(_) => DataType::Int16,
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::UInt8(_) => DataType::UInt8,
            ScalarValue::UInt16(_) => DataType::UInt16,
            ScalarValue::UInt32(_) => DataType::UInt32,
            ScalarValue::UInt64(_) => DataType::UInt64,
            ScalarValue::Float32(_) => DataType::Float32,
            ScalarValue::Float64(_) => DataType::Float64,
            ScalarValue::Utf8(_) => DataType::Utf8,
            ScalarValue::Null => DataType::Null,
        }
    }

    fn coerce_types(
        &self,
        left: query_core::DataType,
        right: query_core::DataType,
    ) -> query_core::DataType {
        use query_core::DataType;

        if left == right {
            return left;
        }

        match (&left, &right) {
            (DataType::Float64, _) | (_, DataType::Float64) => DataType::Float64,
            (DataType::Float32, _) | (_, DataType::Float32) => DataType::Float32,
            (DataType::Int64, _) | (_, DataType::Int64) => DataType::Int64,
            _ => left,
        }
    }
}

impl Default for Planner {
    fn default() -> Self {
        Self::new()
    }
}
