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
        self.parse_select()
    }

    fn parse_select(&mut self) -> Result<Statement> {
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

        Ok(Statement::Select(SelectStatement {
            projection,
            from,
            joins,
            selection,
            group_by,
            having,
            order_by,
            limit,
            offset,
        }))
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
        let name = self.parse_identifier()?;

        let alias = if self.match_token(&Token::As) {
            Some(self.parse_identifier()?)
        } else if let Token::Identifier(id) = self.current_token() {
            // Support implicit alias (without AS keyword)
            let alias = id.clone();
            self.advance();
            Some(alias)
        } else {
            None
        };

        Ok(TableReference { name, alias })
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
        if self.match_token(&Token::Not) {
            let expr = self.parse_unary_expr()?;
            return Ok(Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(expr),
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
            Token::LeftParen => {
                self.advance();
                let expr = self.parse_expr()?;
                self.expect_token(&Token::RightParen)?;
                Ok(expr)
            }
            Token::Null => {
                self.advance();
                Ok(Expr::Literal(Literal::Null))
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
