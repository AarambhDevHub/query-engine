use query_core::DataType;

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
    WithSelect {
        with: WithClause,
        select: SelectStatement,
    },
    /// CREATE INDEX statement
    CreateIndex(CreateIndexStatement),
    /// DROP INDEX statement
    DropIndex(DropIndexStatement),
    /// CREATE TABLE statement
    CreateTable(CreateTableStatement),
    /// INSERT statement
    Insert(InsertStatement),
    /// UPDATE statement
    Update(UpdateStatement),
    /// DELETE statement
    Delete(DeleteStatement),
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
    /// Window function: func(...) OVER (...)
    WindowFunction {
        func: WindowFunctionType,
        args: Vec<Expr>,
        over: WindowSpec,
    },
    /// Scalar function call: UPPER(x), CONCAT(a, b), etc.
    ScalarFunction {
        func: ScalarFunction,
        args: Vec<Expr>,
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

/// Built-in scalar function types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarFunction {
    // String functions
    Upper,
    Lower,
    Length,
    Concat,
    Substring,
    Trim,
    Replace,
    // Math functions
    Abs,
    Ceil,
    Floor,
    Round,
    Sqrt,
    Power,
    // Null handling
    Coalesce,
    Nullif,
}

/// Window function types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFunctionType {
    RowNumber,
    Rank,
    DenseRank,
    Ntile,
    Lag,
    Lead,
    FirstValue,
    LastValue,
}

/// Window specification: OVER (PARTITION BY ... ORDER BY ... [frame])
#[derive(Debug, Clone, PartialEq)]
pub struct WindowSpec {
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub frame: Option<WindowFrame>,
}

/// Window frame: ROWS/RANGE BETWEEN ... AND ...
#[derive(Debug, Clone, PartialEq)]
pub struct WindowFrame {
    pub mode: WindowFrameMode,
    pub start: WindowFrameBound,
    pub end: Option<WindowFrameBound>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFrameMode {
    Rows,
    Range,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WindowFrameBound {
    CurrentRow,
    Preceding(Option<usize>), // None = UNBOUNDED
    Following(Option<usize>), // None = UNBOUNDED
}

/// Index type for CREATE INDEX
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexType {
    /// B-Tree index (default) - supports range queries
    BTree,
    /// Hash index - optimized for equality lookups
    Hash,
}

impl Default for IndexType {
    fn default() -> Self {
        IndexType::BTree
    }
}

/// CREATE INDEX statement
/// Syntax: CREATE [UNIQUE] INDEX name ON table (col1, col2, ...) [USING BTREE|HASH]
#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexStatement {
    /// Index name
    pub name: String,
    /// Table name
    pub table: String,
    /// Columns to index (in order)
    pub columns: Vec<String>,
    /// Whether this is a unique index
    pub unique: bool,
    /// Index type (defaults to BTree)
    pub index_type: IndexType,
}

/// DROP INDEX statement
/// Syntax: DROP INDEX [IF EXISTS] name
#[derive(Debug, Clone, PartialEq)]
pub struct DropIndexStatement {
    /// Index name to drop
    pub name: String,
    /// If true, don't error if index doesn't exist
    pub if_exists: bool,
}

/// CREATE TABLE statement
/// Syntax: CREATE TABLE [IF NOT EXISTS] name (col1 type, col2 type, ...)
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStatement {
    /// Table name
    pub name: String,
    /// Column definitions
    pub columns: Vec<ColumnDef>,
    /// If true, don't error if table already exists
    pub if_not_exists: bool,
}

/// Column definition for CREATE TABLE
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    /// Column name
    pub name: String,
    /// Column data type
    pub data_type: DataType,
    /// Whether column is nullable
    pub nullable: bool,
}

/// INSERT statement
/// Syntax: INSERT INTO table [(columns)] VALUES (values), ... [RETURNING ...]
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement {
    /// Table name
    pub table: String,
    /// Column names (optional, if None insert all columns)
    pub columns: Option<Vec<String>>,
    /// Values to insert (each inner Vec is one row)
    pub values: Vec<Vec<Expr>>,
    /// RETURNING clause (columns to return from inserted rows)
    pub returning: Option<Vec<SelectItem>>,
}

/// UPDATE statement
/// Syntax: UPDATE table SET col=value, ... [WHERE condition] [RETURNING ...]
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStatement {
    /// Table name
    pub table: String,
    /// Column assignments
    pub assignments: Vec<Assignment>,
    /// Optional WHERE clause
    pub selection: Option<Expr>,
    /// RETURNING clause (columns to return from updated rows)
    pub returning: Option<Vec<SelectItem>>,
}

/// Column assignment for UPDATE
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    /// Column name
    pub column: String,
    /// Value expression
    pub value: Expr,
}

/// DELETE statement
/// Syntax: DELETE FROM table [WHERE condition] [RETURNING ...]
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStatement {
    /// Table name
    pub table: String,
    /// Optional WHERE clause
    pub selection: Option<Expr>,
    /// RETURNING clause (columns to return from deleted rows)
    pub returning: Option<Vec<SelectItem>>,
}
