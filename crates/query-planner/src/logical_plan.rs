use query_core::{DataType, Schema};
use query_parser::{
    AggregateFunction, BinaryOperator, JoinType, ScalarFunction, UnaryOperator, WindowFunctionType,
};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    TableScan {
        table_name: String,
        schema: Schema,
        projection: Option<Vec<usize>>,
    },
    Projection {
        input: Arc<LogicalPlan>,
        exprs: Vec<LogicalExpr>,
        schema: Schema,
    },
    Filter {
        input: Arc<LogicalPlan>,
        predicate: LogicalExpr,
    },
    Join {
        left: Arc<LogicalPlan>,
        right: Arc<LogicalPlan>,
        join_type: JoinType,
        on: Option<LogicalExpr>,
        schema: Schema,
    },
    Aggregate {
        input: Arc<LogicalPlan>,
        group_exprs: Vec<LogicalExpr>,
        aggr_exprs: Vec<LogicalExpr>,
        schema: Schema,
    },
    Sort {
        input: Arc<LogicalPlan>,
        exprs: Vec<LogicalExpr>,
        ascending: Vec<bool>,
    },
    Limit {
        input: Arc<LogicalPlan>,
        skip: usize,
        fetch: Option<usize>,
    },
    EmptyRelation {
        schema: Schema,
    },
    /// Subquery as a table source (derived table)
    SubqueryScan {
        subquery: Arc<LogicalPlan>,
        alias: String,
        schema: Schema,
    },
    /// Window function computation
    Window {
        input: Arc<LogicalPlan>,
        window_exprs: Vec<LogicalExpr>,
        schema: Schema,
    },
    /// Index-based scan for optimized queries
    IndexScan {
        table_name: String,
        index_name: String,
        schema: Schema,
        /// Predicates that can be evaluated using the index
        index_predicates: Vec<LogicalExpr>,
        /// Remaining predicates for post-filtering (if any)
        residual_predicate: Option<Box<LogicalExpr>>,
    },
}

impl LogicalPlan {
    pub fn schema(&self) -> &Schema {
        match self {
            LogicalPlan::TableScan { schema, .. } => schema,
            LogicalPlan::Projection { schema, .. } => schema,
            LogicalPlan::Filter { input, .. } => input.schema(),
            LogicalPlan::Join { schema, .. } => schema,
            LogicalPlan::Aggregate { schema, .. } => schema,
            LogicalPlan::Sort { input, .. } => input.schema(),
            LogicalPlan::Limit { input, .. } => input.schema(),
            LogicalPlan::EmptyRelation { schema } => schema,
            LogicalPlan::SubqueryScan { schema, .. } => schema,
            LogicalPlan::Window { schema, .. } => schema,
            LogicalPlan::IndexScan { schema, .. } => schema,
        }
    }
}

#[derive(Debug, Clone)]
pub enum LogicalExpr {
    Column {
        name: String,
        index: usize,
    },
    Literal(ScalarValue),
    BinaryExpr {
        left: Box<LogicalExpr>,
        op: BinaryOperator,
        right: Box<LogicalExpr>,
    },
    UnaryExpr {
        op: UnaryOperator,
        expr: Box<LogicalExpr>,
    },
    AggregateFunction {
        func: AggregateFunction,
        expr: Box<LogicalExpr>,
    },
    Cast {
        expr: Box<LogicalExpr>,
        data_type: DataType,
    },
    Alias {
        expr: Box<LogicalExpr>,
        alias: String,
    },
    /// Scalar subquery: returns a single value
    ScalarSubquery(Arc<LogicalPlan>),
    /// IN subquery: expr [NOT] IN (subquery)
    InSubquery {
        expr: Box<LogicalExpr>,
        subquery: Arc<LogicalPlan>,
        negated: bool,
    },
    /// EXISTS: [NOT] EXISTS (subquery)
    Exists {
        subquery: Arc<LogicalPlan>,
        negated: bool,
    },
    /// Window function expression
    WindowFunction {
        func: WindowFunctionType,
        args: Vec<Box<LogicalExpr>>,
        partition_by: Vec<LogicalExpr>,
        order_by: Vec<LogicalExpr>,
    },
    /// Scalar function expression
    ScalarFunction {
        func: ScalarFunction,
        args: Vec<Box<LogicalExpr>>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    Boolean(Option<bool>),
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    UInt8(Option<u8>),
    UInt16(Option<u16>),
    UInt32(Option<u32>),
    UInt64(Option<u64>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Utf8(Option<String>),
    Null,
}
