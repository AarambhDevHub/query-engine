use query_core::DataType;

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
    WithSelect {
        with: WithClause,
        select: SelectStatement,
    },
}

/// WITH clause containing one or more CTEs
#[derive(Debug, Clone, PartialEq)]
pub struct WithClause {
    pub recursive: bool,
    pub ctes: Vec<CteDefinition>,
}

/// CTE definition: name [(columns)] AS (subquery)
#[derive(Debug, Clone, PartialEq)]
pub struct CteDefinition {
    pub name: String,
    pub columns: Option<Vec<String>>,
    pub query: Box<SelectStatement>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    pub projection: Vec<SelectItem>,
    pub from: Option<TableReference>,
    pub joins: Vec<Join>,
    pub selection: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    Wildcard,
    QualifiedWildcard(String),
    UnnamedExpr(Expr),
    ExprWithAlias { expr: Expr, alias: String },
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableReference {
    /// Simple table reference: table_name [AS alias]
    Table { name: String, alias: Option<String> },
    /// Subquery in FROM clause: (SELECT ...) AS alias
    Subquery {
        query: Box<SelectStatement>,
        alias: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    pub join_type: JoinType,
    pub right: TableReference,
    pub on: Option<Expr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column(String),
    QualifiedColumn {
        table: String,
        column: String,
    },
    Literal(Literal),
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    AggregateFunction {
        func: AggregateFunction,
        expr: Box<Expr>,
    },
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
    },
    /// Scalar subquery: (SELECT ...)
    Subquery(Box<SelectStatement>),
    /// IN subquery: expr [NOT] IN (SELECT ...)
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<SelectStatement>,
        negated: bool,
    },
    /// EXISTS subquery: [NOT] EXISTS (SELECT ...)
    Exists {
        subquery: Box<SelectStatement>,
        negated: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Number(String),
    String(String),
    Boolean(bool),
    Null,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOperator {
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    And,
    Or,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOperator {
    Not,
    Minus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub asc: bool,
}
