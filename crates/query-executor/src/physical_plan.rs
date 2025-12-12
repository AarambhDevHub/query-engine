use arrow::record_batch::RecordBatch;
use query_core::{Result, Schema};
use query_parser::JoinType; // ADD THIS
use query_planner::ScalarValue;
use std::fmt::Debug;
use std::sync::Arc;

pub trait DataSource: Debug + Send + Sync {
    fn scan(&self) -> Result<Vec<RecordBatch>>;
    fn schema(&self) -> &Schema;
}

#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    Scan {
        source: Arc<dyn DataSource>,
        schema: Schema,
    },
    Projection {
        input: Arc<PhysicalPlan>,
        exprs: Vec<PhysicalExpr>,
        schema: Schema,
    },
    Filter {
        input: Arc<PhysicalPlan>,
        predicate: PhysicalExpr,
    },
    HashJoin {
        // ADD THIS
        left: Arc<PhysicalPlan>,
        right: Arc<PhysicalPlan>,
        join_type: JoinType,
        on: Option<PhysicalExpr>,
    },
    HashAggregate {
        input: Arc<PhysicalPlan>,
        group_exprs: Vec<PhysicalExpr>,
        aggr_exprs: Vec<AggregateExpr>,
    },
    Sort {
        input: Arc<PhysicalPlan>,
        exprs: Vec<PhysicalExpr>,
        ascending: Vec<bool>,
    },
    Limit {
        input: Arc<PhysicalPlan>,
        skip: usize,
        fetch: Option<usize>,
    },
    /// Subquery scan for derived tables
    SubqueryScan {
        subquery: Arc<PhysicalPlan>,
        schema: Schema,
    },
    /// Window function computation
    Window {
        input: Arc<PhysicalPlan>,
        window_exprs: Vec<WindowExpr>,
        schema: Schema,
    },
}

#[derive(Debug, Clone)]
pub enum PhysicalExpr {
    Column {
        name: String,
        index: usize,
    },
    Literal(ScalarValue),
    BinaryExpr {
        left: Box<PhysicalExpr>,
        op: BinaryOp,
        right: Box<PhysicalExpr>,
    },
    UnaryExpr {
        op: UnaryOp,
        expr: Box<PhysicalExpr>,
    },
    /// Scalar subquery - executes and returns single value
    ScalarSubquery {
        plan: Arc<PhysicalPlan>,
    },
    /// IN subquery - checks if expr is in subquery results
    InSubquery {
        expr: Box<PhysicalExpr>,
        plan: Arc<PhysicalPlan>,
        negated: bool,
    },
    /// EXISTS subquery - checks if subquery returns any rows
    Exists {
        plan: Arc<PhysicalPlan>,
        negated: bool,
    },
    /// Window function expression
    WindowFunction {
        func: WindowFunctionType,
        args: Vec<PhysicalExpr>,
        partition_by: Vec<PhysicalExpr>,
        order_by: Vec<PhysicalExpr>,
    },
    /// Scalar function expression
    ScalarFunction {
        func: ScalarFunctionType,
        args: Vec<PhysicalExpr>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Subtract,
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
pub enum UnaryOp {
    Not,
    Minus,
}

#[derive(Debug, Clone)]
pub struct AggregateExpr {
    pub func: AggregateFunction,
    pub expr: PhysicalExpr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// Physical window function types
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

/// Physical window expression
#[derive(Debug, Clone)]
pub struct WindowExpr {
    pub func: WindowFunctionType,
    pub args: Vec<PhysicalExpr>,
    pub partition_by: Vec<PhysicalExpr>,
    pub order_by: Vec<PhysicalExpr>,
}

/// Physical scalar function types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarFunctionType {
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

impl From<query_parser::BinaryOperator> for BinaryOp {
    fn from(op: query_parser::BinaryOperator) -> Self {
        match op {
            query_parser::BinaryOperator::Plus => BinaryOp::Add,
            query_parser::BinaryOperator::Minus => BinaryOp::Subtract,
            query_parser::BinaryOperator::Multiply => BinaryOp::Multiply,
            query_parser::BinaryOperator::Divide => BinaryOp::Divide,
            query_parser::BinaryOperator::Modulo => BinaryOp::Modulo,
            query_parser::BinaryOperator::Equal => BinaryOp::Equal,
            query_parser::BinaryOperator::NotEqual => BinaryOp::NotEqual,
            query_parser::BinaryOperator::Less => BinaryOp::Less,
            query_parser::BinaryOperator::LessEqual => BinaryOp::LessEqual,
            query_parser::BinaryOperator::Greater => BinaryOp::Greater,
            query_parser::BinaryOperator::GreaterEqual => BinaryOp::GreaterEqual,
            query_parser::BinaryOperator::And => BinaryOp::And,
            query_parser::BinaryOperator::Or => BinaryOp::Or,
        }
    }
}

impl From<query_parser::UnaryOperator> for UnaryOp {
    fn from(op: query_parser::UnaryOperator) -> Self {
        match op {
            query_parser::UnaryOperator::Not => UnaryOp::Not,
            query_parser::UnaryOperator::Minus => UnaryOp::Minus,
        }
    }
}

impl From<query_parser::AggregateFunction> for AggregateFunction {
    fn from(func: query_parser::AggregateFunction) -> Self {
        match func {
            query_parser::AggregateFunction::Count => AggregateFunction::Count,
            query_parser::AggregateFunction::Sum => AggregateFunction::Sum,
            query_parser::AggregateFunction::Avg => AggregateFunction::Avg,
            query_parser::AggregateFunction::Min => AggregateFunction::Min,
            query_parser::AggregateFunction::Max => AggregateFunction::Max,
        }
    }
}
