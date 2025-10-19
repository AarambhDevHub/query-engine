use query_core::{DataType, Schema};
use query_parser::{AggregateFunction, BinaryOperator, JoinType, UnaryOperator};
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
