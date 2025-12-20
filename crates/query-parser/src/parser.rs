use crate::ast::*;
use crate::lexer::{Lexer, Token};
use query_core::{QueryError, Result};

pub struct Parser {
    tokens: Vec<Token>,
    position: usize,
}

impl Parser {
    pub fn new(sql: &str) -> Result<Self> {
        let mut lexer = Lexer::new(sql);
        let tokens = lexer.tokenize()?;
        Ok(Self {
            tokens,
            position: 0,
        })
    }

    pub fn parse(&mut self) -> Result<Statement> {
        // Check for CREATE statement
        if self.current_token() == &Token::Create {
            return self.parse_create_statement();
        }
        // Check for DROP statement
        if self.current_token() == &Token::Drop {
            return self.parse_drop_statement();
        }
        // Check for INSERT statement
        if self.current_token() == &Token::Insert {
            return self.parse_insert_statement();
        }
        // Check for UPDATE statement
        if self.current_token() == &Token::Update {
            return self.parse_update_statement();
        }
        // Check for DELETE statement
        if self.current_token() == &Token::Delete {
            return self.parse_delete_statement();
        }
        // Check for WITH clause (CTE)
        if self.current_token() == &Token::With {
            return self.parse_with_statement();
        }
        self.parse_select()
    }

    /// Parse CREATE statement (CREATE INDEX or CREATE TABLE)
    fn parse_create_statement(&mut self) -> Result<Statement> {
        self.expect_token(&Token::Create)?;

        // Check for TABLE keyword
        if self.match_token(&Token::Table) {
            return self.parse_create_table();
        }

        // Check for UNIQUE modifier (for INDEX)
        let unique = self.match_token(&Token::Unique);

        // Expect INDEX keyword
        self.expect_token(&Token::Index)?;

        // Parse index name
        let name = self.parse_identifier()?;

        // Expect ON keyword
        self.expect_token(&Token::On)?;

        // Parse table name
        let table = self.parse_identifier()?;

        // Parse column list
        self.expect_token(&Token::LeftParen)?;
        let mut columns = Vec::new();
        loop {
            columns.push(self.parse_identifier()?);
            if !self.match_token(&Token::Comma) {
                break;
            }
        }
        self.expect_token(&Token::RightParen)?;

        // Parse optional USING clause
        let index_type = if self.match_token(&Token::Using) {
            if self.match_token(&Token::BTree) {
                IndexType::BTree
            } else if self.match_token(&Token::Hash) {
                IndexType::Hash
            } else {
                return Err(QueryError::ParseError(
                    "Expected BTREE or HASH after USING".to_string(),
                ));
            }
        } else {
            IndexType::default()
        };

        Ok(Statement::CreateIndex(CreateIndexStatement {
            name,
            table,
            columns,
            unique,
            index_type,
        }))
    }

    /// Parse CREATE TABLE name (col1 type, col2 type, ...)
    fn parse_create_table(&mut self) -> Result<Statement> {
        // Check for IF NOT EXISTS
        let if_not_exists = if self.match_token(&Token::If) {
            self.expect_token(&Token::Not)?;
            self.expect_token(&Token::Exists)?;
            true
        } else {
            false
        };

        // Parse table name
        let name = self.parse_identifier()?;

        // Parse column definitions
        self.expect_token(&Token::LeftParen)?;
        let mut columns = Vec::new();
        loop {
            let col_name = self.parse_identifier()?;
            let data_type = self.parse_data_type()?;

            // Check for NOT NULL or NULL
            let nullable = if self.match_token(&Token::Not) {
                self.expect_token(&Token::Null)?;
                false
            } else {
                self.match_token(&Token::Null); // consume optional NULL
                true
            };

            columns.push(ColumnDef {
                name: col_name,
                data_type,
                nullable,
            });

            if !self.match_token(&Token::Comma) {
                break;
            }
        }
        self.expect_token(&Token::RightParen)?;

        Ok(Statement::CreateTable(CreateTableStatement {
            name,
            columns,
            if_not_exists,
        }))
    }

    /// Parse a data type (INT, VARCHAR, FLOAT, UUID, JSON, etc.)
    fn parse_data_type(&mut self) -> Result<query_core::DataType> {
        use query_core::DataType;

        match self.current_token() {
            Token::Identifier(s) => {
                let type_name = s.to_uppercase();
                self.advance();

                let base_type = match type_name.as_str() {
                    "INT" | "INTEGER" | "BIGINT" | "INT8" => DataType::Int64,
                    "SMALLINT" | "INT2" => DataType::Int16,
                    "INT4" => DataType::Int32,
                    "TINYINT" => DataType::Int8,
                    "FLOAT" | "DOUBLE" | "REAL" | "FLOAT8" => DataType::Float64,
                    "FLOAT4" => DataType::Float32,
                    "DECIMAL" | "NUMERIC" => {
                        // Parse optional precision and scale: DECIMAL(p, s)
                        let (precision, scale) = if self.match_token(&Token::LeftParen) {
                            let p = self.parse_number()? as u8;
                            let s = if self.match_token(&Token::Comma) {
                                self.parse_number()? as i8
                            } else {
                                0
                            };
                            self.expect_token(&Token::RightParen)?;
                            (p, s)
                        } else {
                            (38, 9) // PostgreSQL default
                        };
                        DataType::Decimal128 { precision, scale }
                    }
                    "VARCHAR" | "CHAR" | "TEXT" | "STRING" => {
                        // Optionally consume (length)
                        if self.match_token(&Token::LeftParen) {
                            self.parse_number()?; // ignore length
                            self.expect_token(&Token::RightParen)?;
                        }
                        DataType::Utf8
                    }
                    "BOOLEAN" | "BOOL" => DataType::Boolean,
                    "DATE" => DataType::Date32,
                    "TIMESTAMP" | "DATETIME" | "TIMESTAMPTZ" => DataType::Timestamp,
                    // New types
                    "UUID" => DataType::Uuid,
                    "JSON" | "JSONB" => DataType::Json,
                    "BYTEA" | "BLOB" | "BINARY" => DataType::LargeBinary,
                    "INTERVAL" => DataType::Interval,
                    // Geometric types
                    "POINT" => DataType::Point,
                    "LINE" => DataType::Line,
                    "LSEG" => DataType::LineSegment,
                    "BOX" => DataType::Box,
                    "PATH" => DataType::Path,
                    "POLYGON" => DataType::Polygon,
                    "CIRCLE" => DataType::Circle,
                    _ => {
                        return Err(QueryError::ParseError(format!(
                            "Unknown data type: {}",
                            type_name
                        )));
                    }
                };

                // Check for array suffix: INT[]
                if self.match_token(&Token::LeftBracket) {
                    self.expect_token(&Token::RightBracket)?;
                    Ok(DataType::List(Box::new(base_type)))
                } else {
                    Ok(base_type)
                }
            }
            _ => Err(QueryError::ParseError("Expected data type".to_string())),
        }
    }

    /// Parse INSERT INTO table [(columns)] VALUES (values), ...
    fn parse_insert_statement(&mut self) -> Result<Statement> {
        self.expect_token(&Token::Insert)?;
        self.expect_token(&Token::Into)?;

        // Parse table name
        let table = self.parse_identifier()?;

        // Parse optional column list
        let columns = if self.match_token(&Token::LeftParen) {
            let mut cols = Vec::new();
            loop {
                cols.push(self.parse_identifier()?);
                if !self.match_token(&Token::Comma) {
                    break;
                }
            }
            self.expect_token(&Token::RightParen)?;
            Some(cols)
        } else {
            None
        };

        // Expect VALUES keyword
        self.expect_token(&Token::Values)?;

        // Parse value rows
        let mut values = Vec::new();
        loop {
            self.expect_token(&Token::LeftParen)?;
            let row = self.parse_expr_list()?;
            self.expect_token(&Token::RightParen)?;
            values.push(row);

            if !self.match_token(&Token::Comma) {
                break;
            }
        }

        Ok(Statement::Insert(InsertStatement {
            table,
            columns,
            values,
        }))
    }

    /// Parse UPDATE table SET col=value, ... [WHERE condition]
    fn parse_update_statement(&mut self) -> Result<Statement> {
        self.expect_token(&Token::Update)?;

        // Parse table name
        let table = self.parse_identifier()?;

        // Expect SET keyword
        self.expect_token(&Token::Set)?;

        // Parse assignments
        let mut assignments = Vec::new();
        loop {
            let column = self.parse_identifier()?;
            self.expect_token(&Token::Equal)?;
            let value = self.parse_expr()?;
            assignments.push(Assignment { column, value });

            if !self.match_token(&Token::Comma) {
                break;
            }
        }

        // Parse optional WHERE clause
        let selection = if self.match_token(&Token::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Statement::Update(UpdateStatement {
            table,
            assignments,
            selection,
        }))
    }

    /// Parse DELETE FROM table [WHERE condition]
    fn parse_delete_statement(&mut self) -> Result<Statement> {
        self.expect_token(&Token::Delete)?;
        self.expect_token(&Token::From)?;

        // Parse table name
        let table = self.parse_identifier()?;

        // Parse optional WHERE clause
        let selection = if self.match_token(&Token::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Statement::Delete(DeleteStatement { table, selection }))
    }

    /// Parse DROP statement (currently only DROP INDEX)
    fn parse_drop_statement(&mut self) -> Result<Statement> {
        self.expect_token(&Token::Drop)?;

        // Expect INDEX keyword
        self.expect_token(&Token::Index)?;

        // Check for IF EXISTS
        let if_exists = if self.match_token(&Token::If) {
            self.expect_token(&Token::Exists)?;
            true
        } else {
            false
        };

        // Parse index name
        let name = self.parse_identifier()?;

        Ok(Statement::DropIndex(DropIndexStatement { name, if_exists }))
    }

    /// Parse WITH clause: WITH [RECURSIVE] cte_name [(col1, ...)] AS (SELECT ...), ...
    fn parse_with_statement(&mut self) -> Result<Statement> {
        self.expect_token(&Token::With)?;

        let recursive = self.match_token(&Token::Recursive);
        let mut ctes = Vec::new();

        loop {
            let name = self.parse_identifier()?;

            // Optional column list: WITH cte_name (col1, col2) AS (...)
            let columns = if self.match_token(&Token::LeftParen) {
                let mut cols = Vec::new();
                loop {
                    cols.push(self.parse_identifier()?);
                    if !self.match_token(&Token::Comma) {
                        break;
                    }
                }
                self.expect_token(&Token::RightParen)?;
                Some(cols)
            } else {
                None
            };

            self.expect_token(&Token::As)?;
            self.expect_token(&Token::LeftParen)?;

            // Parse the CTE query
            let query = Box::new(self.parse_select_statement()?);

            self.expect_token(&Token::RightParen)?;

            ctes.push(CteDefinition {
                name,
                columns,
                query,
            });

            if !self.match_token(&Token::Comma) {
                break;
            }
        }

        // Now parse the main SELECT
        let select = self.parse_select_statement()?;

        Ok(Statement::WithSelect {
            with: WithClause { recursive, ctes },
            select,
        })
    }

    fn parse_select(&mut self) -> Result<Statement> {
        Ok(Statement::Select(self.parse_select_statement()?))
    }

    /// Parse a SELECT statement (without wrapping in Statement enum)
    fn parse_select_statement(&mut self) -> Result<SelectStatement> {
        self.expect_token(&Token::Select)?;

        let projection = self.parse_projection()?;

        let from = if self.match_token(&Token::From) {
            Some(self.parse_table_reference()?)
        } else {
            None
        };

        // Parse multiple JOINs
        let mut joins = Vec::new();
        while self.is_join_keyword() {
            joins.push(self.parse_join()?);
        }

        let selection = if self.match_token(&Token::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let group_by = if self.match_token(&Token::Group) {
            self.expect_token(&Token::By)?;
            self.parse_expr_list()?
        } else {
            vec![]
        };

        let having = if self.match_token(&Token::Having) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let order_by = if self.match_token(&Token::Order) {
            self.expect_token(&Token::By)?;
            self.parse_order_by()?
        } else {
            vec![]
        };

        let limit = if self.match_token(&Token::Limit) {
            Some(self.parse_number()?)
        } else {
            None
        };

        let offset = if self.match_token(&Token::Offset) {
            Some(self.parse_number()?)
        } else {
            None
        };

        Ok(SelectStatement {
            projection,
            from,
            joins,
            selection,
            group_by,
            having,
            order_by,
            limit,
            offset,
        })
    }

    fn is_join_keyword(&self) -> bool {
        matches!(
            self.current_token(),
            Token::Join | Token::Inner | Token::Left | Token::Right | Token::Full | Token::Cross
        )
    }

    fn parse_join(&mut self) -> Result<Join> {
        let join_type = self.parse_join_type()?;

        // Expect JOIN keyword
        self.expect_token(&Token::Join)?;

        let right = self.parse_table_reference()?;

        let on = if join_type != JoinType::Cross && self.match_token(&Token::On) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Join {
            join_type,
            right,
            on,
        })
    }

    fn parse_join_type(&mut self) -> Result<JoinType> {
        let join_type = match self.current_token() {
            Token::Cross => {
                self.advance();
                JoinType::Cross
            }
            Token::Inner => {
                self.advance();
                JoinType::Inner
            }
            Token::Left => {
                self.advance();
                self.match_token(&Token::Outer); // OUTER is optional
                JoinType::Left
            }
            Token::Right => {
                self.advance();
                self.match_token(&Token::Outer); // OUTER is optional
                JoinType::Right
            }
            Token::Full => {
                self.advance();
                self.match_token(&Token::Outer); // OUTER is optional
                JoinType::Full
            }
            Token::Join => {
                // Don't advance, let parse_join handle it
                JoinType::Inner // Default to INNER JOIN
            }
            _ => return Err(QueryError::ParseError("Expected JOIN keyword".to_string())),
        };

        Ok(join_type)
    }

    fn parse_projection(&mut self) -> Result<Vec<SelectItem>> {
        let mut items = vec![];

        loop {
            if self.current_token() == &Token::Star {
                self.advance();
                items.push(SelectItem::Wildcard);
            } else if let Token::Identifier(name) = self.current_token() {
                let name_clone = name.clone();
                self.advance();

                // Check for qualified wildcard (table.*)
                if self.match_token(&Token::Dot) {
                    if self.match_token(&Token::Star) {
                        items.push(SelectItem::QualifiedWildcard(name_clone));
                        if !self.match_token(&Token::Comma) {
                            break;
                        }
                        continue;
                    } else {
                        // Qualified column (table.column)
                        let column = self.parse_identifier()?;
                        let expr = Expr::QualifiedColumn {
                            table: name_clone,
                            column,
                        };

                        if self.match_token(&Token::As) {
                            let alias = self.parse_identifier()?;
                            items.push(SelectItem::ExprWithAlias { expr, alias });
                        } else {
                            items.push(SelectItem::UnnamedExpr(expr));
                        }
                    }
                } else {
                    // Regular column or expression
                    self.position -= 1; // Go back to re-parse as expression
                    let expr = self.parse_expr()?;

                    if self.match_token(&Token::As) {
                        let alias = self.parse_identifier()?;
                        items.push(SelectItem::ExprWithAlias { expr, alias });
                    } else {
                        items.push(SelectItem::UnnamedExpr(expr));
                    }
                }
            } else {
                let expr = self.parse_expr()?;

                if self.match_token(&Token::As) {
                    let alias = self.parse_identifier()?;
                    items.push(SelectItem::ExprWithAlias { expr, alias });
                } else {
                    items.push(SelectItem::UnnamedExpr(expr));
                }
            }

            if !self.match_token(&Token::Comma) {
                break;
            }
        }

        Ok(items)
    }

    fn parse_table_reference(&mut self) -> Result<TableReference> {
        // Check for subquery: (SELECT ...) AS alias
        if self.match_token(&Token::LeftParen) {
            let query = Box::new(self.parse_select_statement()?);
            self.expect_token(&Token::RightParen)?;

            // Subquery requires an alias
            let alias = if self.match_token(&Token::As) {
                self.parse_identifier()?
            } else if let Token::Identifier(id) = self.current_token() {
                let alias = id.clone();
                self.advance();
                alias
            } else {
                return Err(QueryError::ParseError(
                    "Subquery in FROM clause requires an alias".to_string(),
                ));
            };

            return Ok(TableReference::Subquery { query, alias });
        }

        // Simple table reference
        let name = self.parse_identifier()?;

        let alias = if self.match_token(&Token::As) {
            Some(self.parse_identifier()?)
        } else if let Token::Identifier(id) = self.current_token() {
            // Support implicit alias (without AS keyword)
            // But be careful not to consume keywords
            if !self.is_keyword(self.current_token()) {
                let alias = id.clone();
                self.advance();
                Some(alias)
            } else {
                None
            }
        } else {
            None
        };

        Ok(TableReference::Table { name, alias })
    }

    /// Check if token is a reserved keyword (not a valid alias)
    fn is_keyword(&self, token: &Token) -> bool {
        matches!(
            token,
            Token::Select
                | Token::From
                | Token::Where
                | Token::Join
                | Token::Inner
                | Token::Left
                | Token::Right
                | Token::Full
                | Token::Cross
                | Token::On
                | Token::Group
                | Token::Order
                | Token::Having
                | Token::Limit
                | Token::Offset
                | Token::And
                | Token::Or
                | Token::With
        )
    }

    fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_or_expr()
    }

    fn parse_or_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_and_expr()?;

        while self.match_token(&Token::Or) {
            let right = self.parse_and_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Or,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_and_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_comparison_expr()?;

        while self.match_token(&Token::And) {
            let right = self.parse_comparison_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::And,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_comparison_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_additive_expr()?;

        // Check for IN subquery: expr [NOT] IN (SELECT ...)
        let negated = self.match_token(&Token::Not);
        if self.match_token(&Token::In) {
            self.expect_token(&Token::LeftParen)?;
            if self.current_token() == &Token::Select {
                let subquery = Box::new(self.parse_select_statement()?);
                self.expect_token(&Token::RightParen)?;
                return Ok(Expr::InSubquery {
                    expr: Box::new(left),
                    subquery,
                    negated,
                });
            } else {
                // TODO: Handle IN (value_list) - for now error
                return Err(QueryError::ParseError(
                    "IN with value list not yet supported, use IN (SELECT ...)".to_string(),
                ));
            }
        } else if negated {
            // We consumed NOT but no IN followed - this is an error
            return Err(QueryError::ParseError("Expected IN after NOT".to_string()));
        }

        if let Some(op) = self.match_comparison_op() {
            let right = self.parse_additive_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_additive_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_multiplicative_expr()?;

        while let Some(op) = self.match_additive_op() {
            let right = self.parse_multiplicative_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_multiplicative_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_unary_expr()?;

        while let Some(op) = self.match_multiplicative_op() {
            let right = self.parse_unary_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_unary_expr(&mut self) -> Result<Expr> {
        // Handle NOT EXISTS (...)
        if self.match_token(&Token::Not) {
            // Check if followed by EXISTS
            if self.match_token(&Token::Exists) {
                self.expect_token(&Token::LeftParen)?;
                let subquery = Box::new(self.parse_select_statement()?);
                self.expect_token(&Token::RightParen)?;
                return Ok(Expr::Exists {
                    subquery,
                    negated: true,
                });
            }
            // Regular NOT expression
            let expr = self.parse_unary_expr()?;
            return Ok(Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(expr),
            });
        }

        // Handle EXISTS (...)
        if self.match_token(&Token::Exists) {
            self.expect_token(&Token::LeftParen)?;
            let subquery = Box::new(self.parse_select_statement()?);
            self.expect_token(&Token::RightParen)?;
            return Ok(Expr::Exists {
                subquery,
                negated: false,
            });
        }

        if self.match_token(&Token::Minus) {
            let expr = self.parse_unary_expr()?;
            return Ok(Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(expr),
            });
        }

        self.parse_primary_expr()
    }

    fn parse_primary_expr(&mut self) -> Result<Expr> {
        match self.current_token() {
            Token::Number(n) => {
                let num = n.clone();
                self.advance();
                Ok(Expr::Literal(Literal::Number(num)))
            }
            Token::String(s) => {
                let str = s.clone();
                self.advance();
                Ok(Expr::Literal(Literal::String(str)))
            }
            Token::Identifier(id) => {
                let name = id.clone();
                self.advance();

                // Check for qualified column (table.column)
                if self.match_token(&Token::Dot) {
                    let column = self.parse_identifier()?;
                    Ok(Expr::QualifiedColumn {
                        table: name,
                        column,
                    })
                } else {
                    Ok(Expr::Column(name))
                }
            }
            Token::Count | Token::Sum | Token::Avg | Token::Min | Token::Max => {
                self.parse_aggregate_function()
            }
            // Window functions
            Token::RowNumber
            | Token::Rank
            | Token::DenseRank
            | Token::Ntile
            | Token::Lag
            | Token::Lead
            | Token::FirstValue
            | Token::LastValue => self.parse_window_function(),
            // Scalar functions
            Token::Upper
            | Token::Lower
            | Token::Length
            | Token::Concat
            | Token::Substring
            | Token::Trim
            | Token::Replace
            | Token::Abs
            | Token::Ceil
            | Token::Floor
            | Token::Round
            | Token::Sqrt
            | Token::Power
            | Token::Coalesce
            | Token::Nullif => self.parse_scalar_function(),
            Token::LeftParen => {
                self.advance();
                // Check if this is a scalar subquery
                if self.current_token() == &Token::Select {
                    let subquery = Box::new(self.parse_select_statement()?);
                    self.expect_token(&Token::RightParen)?;
                    Ok(Expr::Subquery(subquery))
                } else {
                    // Regular parenthesized expression
                    let expr = self.parse_expr()?;
                    self.expect_token(&Token::RightParen)?;
                    Ok(expr)
                }
            }
            Token::Null => {
                self.advance();
                Ok(Expr::Literal(Literal::Null))
            }
            Token::True => {
                self.advance();
                Ok(Expr::Literal(Literal::Boolean(true)))
            }
            Token::False => {
                self.advance();
                Ok(Expr::Literal(Literal::Boolean(false)))
            }
            _ => Err(QueryError::ParseError(format!(
                "Unexpected token: {:?}",
                self.current_token()
            ))),
        }
    }

    fn parse_aggregate_function(&mut self) -> Result<Expr> {
        let func = match self.current_token() {
            Token::Count => AggregateFunction::Count,
            Token::Sum => AggregateFunction::Sum,
            Token::Avg => AggregateFunction::Avg,
            Token::Min => AggregateFunction::Min,
            Token::Max => AggregateFunction::Max,
            _ => {
                return Err(QueryError::ParseError(
                    "Expected aggregate function".to_string(),
                ));
            }
        };

        self.advance();
        self.expect_token(&Token::LeftParen)?;
        let expr = self.parse_expr()?;
        self.expect_token(&Token::RightParen)?;

        Ok(Expr::AggregateFunction {
            func,
            expr: Box::new(expr),
        })
    }

    fn parse_expr_list(&mut self) -> Result<Vec<Expr>> {
        let mut exprs = vec![];

        loop {
            exprs.push(self.parse_expr()?);

            if !self.match_token(&Token::Comma) {
                break;
            }
        }

        Ok(exprs)
    }

    /// Parse a scalar function call: UPPER(x), CONCAT(a, b), etc.
    fn parse_scalar_function(&mut self) -> Result<Expr> {
        let func = match self.current_token() {
            Token::Upper => ScalarFunction::Upper,
            Token::Lower => ScalarFunction::Lower,
            Token::Length => ScalarFunction::Length,
            Token::Concat => ScalarFunction::Concat,
            Token::Substring => ScalarFunction::Substring,
            Token::Trim => ScalarFunction::Trim,
            Token::Replace => ScalarFunction::Replace,
            Token::Abs => ScalarFunction::Abs,
            Token::Ceil => ScalarFunction::Ceil,
            Token::Floor => ScalarFunction::Floor,
            Token::Round => ScalarFunction::Round,
            Token::Sqrt => ScalarFunction::Sqrt,
            Token::Power => ScalarFunction::Power,
            Token::Coalesce => ScalarFunction::Coalesce,
            Token::Nullif => ScalarFunction::Nullif,
            _ => {
                return Err(QueryError::ParseError(
                    "Expected scalar function".to_string(),
                ));
            }
        };
        self.advance();

        // Parse function arguments
        self.expect_token(&Token::LeftParen)?;
        let args = if self.current_token() != &Token::RightParen {
            self.parse_expr_list()?
        } else {
            vec![]
        };
        self.expect_token(&Token::RightParen)?;

        Ok(Expr::ScalarFunction { func, args })
    }

    fn parse_order_by(&mut self) -> Result<Vec<OrderByExpr>> {
        let mut order_by = vec![];

        loop {
            let expr = self.parse_expr()?;
            let asc = if self.match_token(&Token::Desc) {
                false
            } else {
                self.match_token(&Token::Asc);
                true
            };

            order_by.push(OrderByExpr { expr, asc });

            if !self.match_token(&Token::Comma) {
                break;
            }
        }

        Ok(order_by)
    }

    /// Parse a window function: func(...) OVER (...)
    fn parse_window_function(&mut self) -> Result<Expr> {
        let func = match self.current_token() {
            Token::RowNumber => WindowFunctionType::RowNumber,
            Token::Rank => WindowFunctionType::Rank,
            Token::DenseRank => WindowFunctionType::DenseRank,
            Token::Ntile => WindowFunctionType::Ntile,
            Token::Lag => WindowFunctionType::Lag,
            Token::Lead => WindowFunctionType::Lead,
            Token::FirstValue => WindowFunctionType::FirstValue,
            Token::LastValue => WindowFunctionType::LastValue,
            _ => {
                return Err(QueryError::ParseError(
                    "Expected window function".to_string(),
                ));
            }
        };
        self.advance();

        // Parse function arguments
        self.expect_token(&Token::LeftParen)?;
        let args = if self.current_token() != &Token::RightParen {
            self.parse_expr_list()?
        } else {
            vec![]
        };
        self.expect_token(&Token::RightParen)?;

        // Parse OVER clause
        self.expect_token(&Token::Over)?;
        let over = self.parse_window_spec()?;

        Ok(Expr::WindowFunction { func, args, over })
    }

    /// Parse window specification: OVER (PARTITION BY ... ORDER BY ... [ROWS/RANGE ...])
    fn parse_window_spec(&mut self) -> Result<WindowSpec> {
        self.expect_token(&Token::LeftParen)?;

        // Parse PARTITION BY
        let partition_by = if self.match_token(&Token::Partition) {
            self.expect_token(&Token::By)?;
            self.parse_expr_list()?
        } else {
            vec![]
        };

        // Parse ORDER BY
        let order_by = if self.match_token(&Token::Order) {
            self.expect_token(&Token::By)?;
            self.parse_order_by()?
        } else {
            vec![]
        };

        // Parse optional window frame (ROWS/RANGE ...)
        let frame = if self.current_token() == &Token::Rows || self.current_token() == &Token::Range
        {
            Some(self.parse_window_frame()?)
        } else {
            None
        };

        self.expect_token(&Token::RightParen)?;

        Ok(WindowSpec {
            partition_by,
            order_by,
            frame,
        })
    }

    /// Parse window frame: ROWS/RANGE [BETWEEN] frame_bound [AND frame_bound]
    fn parse_window_frame(&mut self) -> Result<WindowFrame> {
        let mode = if self.match_token(&Token::Rows) {
            WindowFrameMode::Rows
        } else if self.match_token(&Token::Range) {
            WindowFrameMode::Range
        } else {
            return Err(QueryError::ParseError("Expected ROWS or RANGE".to_string()));
        };

        // Check for BETWEEN
        let has_between = self.match_token(&Token::Between);

        let start = self.parse_window_frame_bound()?;

        let end = if has_between {
            self.expect_token(&Token::And)?;
            Some(self.parse_window_frame_bound()?)
        } else {
            None
        };

        Ok(WindowFrame { mode, start, end })
    }

    /// Parse window frame bound: UNBOUNDED PRECEDING/FOLLOWING | n PRECEDING/FOLLOWING | CURRENT ROW
    fn parse_window_frame_bound(&mut self) -> Result<WindowFrameBound> {
        if self.match_token(&Token::Unbounded) {
            if self.match_token(&Token::Preceding) {
                Ok(WindowFrameBound::Preceding(None))
            } else if self.match_token(&Token::Following) {
                Ok(WindowFrameBound::Following(None))
            } else {
                Err(QueryError::ParseError(
                    "Expected PRECEDING or FOLLOWING after UNBOUNDED".to_string(),
                ))
            }
        } else if self.match_token(&Token::Current) {
            // Consume ROW token if present (can be separate Row token or part of identifier)
            if let Token::Identifier(id) = self.current_token() {
                if id.to_uppercase() == "ROW" {
                    self.advance();
                }
            }
            Ok(WindowFrameBound::CurrentRow)
        } else if let Token::Number(_) = self.current_token() {
            let n = self.parse_number()?;
            if self.match_token(&Token::Preceding) {
                Ok(WindowFrameBound::Preceding(Some(n)))
            } else if self.match_token(&Token::Following) {
                Ok(WindowFrameBound::Following(Some(n)))
            } else {
                Err(QueryError::ParseError(
                    "Expected PRECEDING or FOLLOWING after number".to_string(),
                ))
            }
        } else {
            Err(QueryError::ParseError(
                "Invalid window frame bound".to_string(),
            ))
        }
    }

    fn parse_identifier(&mut self) -> Result<String> {
        match self.current_token() {
            Token::Identifier(id) => {
                let name = id.clone();
                self.advance();
                Ok(name)
            }
            _ => Err(QueryError::ParseError("Expected identifier".to_string())),
        }
    }

    fn parse_number(&mut self) -> Result<usize> {
        match self.current_token() {
            Token::Number(n) => {
                let num = n
                    .parse()
                    .map_err(|_| QueryError::ParseError(format!("Invalid number: {}", n)))?;
                self.advance();
                Ok(num)
            }
            _ => Err(QueryError::ParseError("Expected number".to_string())),
        }
    }

    fn match_comparison_op(&mut self) -> Option<BinaryOperator> {
        let op = match self.current_token() {
            Token::Equal => Some(BinaryOperator::Equal),
            Token::NotEqual => Some(BinaryOperator::NotEqual),
            Token::Less => Some(BinaryOperator::Less),
            Token::LessEqual => Some(BinaryOperator::LessEqual),
            Token::Greater => Some(BinaryOperator::Greater),
            Token::GreaterEqual => Some(BinaryOperator::GreaterEqual),
            _ => None,
        };

        if op.is_some() {
            self.advance();
        }

        op
    }

    fn match_additive_op(&mut self) -> Option<BinaryOperator> {
        let op = match self.current_token() {
            Token::Plus => Some(BinaryOperator::Plus),
            Token::Minus => Some(BinaryOperator::Minus),
            _ => None,
        };

        if op.is_some() {
            self.advance();
        }

        op
    }

    fn match_multiplicative_op(&mut self) -> Option<BinaryOperator> {
        let op = match self.current_token() {
            Token::Star => Some(BinaryOperator::Multiply),
            Token::Slash => Some(BinaryOperator::Divide),
            Token::Percent => Some(BinaryOperator::Modulo),
            _ => None,
        };

        if op.is_some() {
            self.advance();
        }

        op
    }

    fn current_token(&self) -> &Token {
        &self.tokens[self.position]
    }

    fn advance(&mut self) {
        if self.position < self.tokens.len() - 1 {
            self.position += 1;
        }
    }

    fn match_token(&mut self, token: &Token) -> bool {
        if self.current_token() == token {
            self.advance();
            true
        } else {
            false
        }
    }

    fn expect_token(&mut self, token: &Token) -> Result<()> {
        if self.current_token() == token {
            self.advance();
            Ok(())
        } else {
            Err(QueryError::ParseError(format!(
                "Expected {:?}, found {:?}",
                token,
                self.current_token()
            )))
        }
    }
}
