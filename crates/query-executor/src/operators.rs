use crate::physical_plan::{
    AggregateFunction, BinaryOp, PhysicalExpr, ScalarFunctionType, UnaryOp,
};
use arrow::array::*;
use arrow::compute;
use arrow::compute::kernels::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow::compute::kernels::numeric::{add, div, mul, sub};
use arrow::record_batch::RecordBatch;
use query_core::{QueryError, Result};
use query_planner::ScalarValue;
use std::sync::Arc;

pub fn evaluate_expr(expr: &PhysicalExpr, batch: &RecordBatch) -> Result<ArrayRef> {
    match expr {
        PhysicalExpr::Column { index, .. } => {
            if *index >= batch.num_columns() {
                return Err(QueryError::ExecutionError(format!(
                    "Column index {} out of bounds",
                    index
                )));
            }
            Ok(batch.column(*index).clone())
        }
        PhysicalExpr::Literal(val) => create_literal_array(val, batch.num_rows()),
        PhysicalExpr::BinaryExpr { left, op, right } => {
            let left_array = evaluate_expr(left, batch)?;
            let right_array = evaluate_expr(right, batch)?;
            evaluate_binary_op(&left_array, *op, &right_array)
        }
        PhysicalExpr::UnaryExpr { op, expr } => {
            let array = evaluate_expr(expr, batch)?;
            evaluate_unary_op(&array, *op)
        }
        PhysicalExpr::ScalarSubquery { .. } => {
            // Scalar subquery evaluation would require async execution context
            // For now, return an error - full implementation would need executor access
            Err(QueryError::ExecutionError(
                "Scalar subquery execution not yet implemented in synchronous context".to_string(),
            ))
        }
        PhysicalExpr::InSubquery { .. } => {
            // IN subquery evaluation would require async execution context
            Err(QueryError::ExecutionError(
                "IN subquery execution not yet implemented in synchronous context".to_string(),
            ))
        }
        PhysicalExpr::Exists { .. } => {
            // EXISTS subquery evaluation would require async execution context
            Err(QueryError::ExecutionError(
                "EXISTS subquery execution not yet implemented in synchronous context".to_string(),
            ))
        }
        PhysicalExpr::WindowFunction { .. } => {
            // Window function evaluation requires full batch context
            // This is handled at the plan level, not expression level
            Err(QueryError::ExecutionError(
                "Window function execution not yet implemented".to_string(),
            ))
        }
        PhysicalExpr::ScalarFunction { func, args } => evaluate_scalar_function(*func, args, batch),
    }
}

fn evaluate_scalar_function(
    func: ScalarFunctionType,
    args: &[PhysicalExpr],
    batch: &RecordBatch,
) -> Result<ArrayRef> {
    // Evaluate all arguments
    let evaluated_args: Result<Vec<ArrayRef>> =
        args.iter().map(|arg| evaluate_expr(arg, batch)).collect();
    let evaluated_args = evaluated_args?;

    match func {
        ScalarFunctionType::Upper => {
            let arg = evaluated_args.get(0).ok_or_else(|| {
                QueryError::ExecutionError("UPPER requires 1 argument".to_string())
            })?;
            let string_array = arg.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                QueryError::ExecutionError("UPPER requires string argument".to_string())
            })?;
            let result: StringArray = string_array
                .iter()
                .map(|opt| opt.map(|s| s.to_uppercase()))
                .collect();
            Ok(Arc::new(result) as ArrayRef)
        }
        ScalarFunctionType::Lower => {
            let arg = evaluated_args.get(0).ok_or_else(|| {
                QueryError::ExecutionError("LOWER requires 1 argument".to_string())
            })?;
            let string_array = arg.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                QueryError::ExecutionError("LOWER requires string argument".to_string())
            })?;
            let result: StringArray = string_array
                .iter()
                .map(|opt| opt.map(|s| s.to_lowercase()))
                .collect();
            Ok(Arc::new(result) as ArrayRef)
        }
        ScalarFunctionType::Length => {
            let arg = evaluated_args.get(0).ok_or_else(|| {
                QueryError::ExecutionError("LENGTH requires 1 argument".to_string())
            })?;
            let string_array = arg.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                QueryError::ExecutionError("LENGTH requires string argument".to_string())
            })?;
            let result: Int64Array = string_array
                .iter()
                .map(|opt| opt.map(|s| s.len() as i64))
                .collect();
            Ok(Arc::new(result) as ArrayRef)
        }
        ScalarFunctionType::Concat => {
            if evaluated_args.is_empty() {
                return Err(QueryError::ExecutionError(
                    "CONCAT requires at least 1 argument".to_string(),
                ));
            }
            let string_arrays: Result<Vec<&StringArray>> = evaluated_args
                .iter()
                .map(|a| {
                    a.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                        QueryError::ExecutionError("CONCAT requires string arguments".to_string())
                    })
                })
                .collect();
            let string_arrays = string_arrays?;

            let len = string_arrays[0].len();
            let result: StringArray = (0..len)
                .map(|i| {
                    let parts: Vec<&str> = string_arrays
                        .iter()
                        .filter_map(|arr| arr.value(i).into())
                        .collect();
                    if parts.is_empty() {
                        None
                    } else {
                        Some(parts.join(""))
                    }
                })
                .collect();
            Ok(Arc::new(result) as ArrayRef)
        }
        ScalarFunctionType::Abs => {
            let arg = evaluated_args
                .get(0)
                .ok_or_else(|| QueryError::ExecutionError("ABS requires 1 argument".to_string()))?;
            if let Some(float_array) = arg.as_any().downcast_ref::<Float64Array>() {
                let result: Float64Array =
                    float_array.iter().map(|opt| opt.map(|v| v.abs())).collect();
                Ok(Arc::new(result) as ArrayRef)
            } else if let Some(int_array) = arg.as_any().downcast_ref::<Int64Array>() {
                let result: Int64Array = int_array.iter().map(|opt| opt.map(|v| v.abs())).collect();
                Ok(Arc::new(result) as ArrayRef)
            } else {
                Err(QueryError::ExecutionError(
                    "ABS requires numeric argument".to_string(),
                ))
            }
        }
        ScalarFunctionType::Ceil => {
            let arg = evaluated_args.get(0).ok_or_else(|| {
                QueryError::ExecutionError("CEIL requires 1 argument".to_string())
            })?;
            let float_array = arg.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                QueryError::ExecutionError("CEIL requires float argument".to_string())
            })?;
            let result: Float64Array = float_array
                .iter()
                .map(|opt| opt.map(|v| v.ceil()))
                .collect();
            Ok(Arc::new(result) as ArrayRef)
        }
        ScalarFunctionType::Floor => {
            let arg = evaluated_args.get(0).ok_or_else(|| {
                QueryError::ExecutionError("FLOOR requires 1 argument".to_string())
            })?;
            let float_array = arg.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                QueryError::ExecutionError("FLOOR requires float argument".to_string())
            })?;
            let result: Float64Array = float_array
                .iter()
                .map(|opt| opt.map(|v| v.floor()))
                .collect();
            Ok(Arc::new(result) as ArrayRef)
        }
        ScalarFunctionType::Round => {
            let arg = evaluated_args.get(0).ok_or_else(|| {
                QueryError::ExecutionError("ROUND requires 1 argument".to_string())
            })?;
            let float_array = arg.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                QueryError::ExecutionError("ROUND requires float argument".to_string())
            })?;
            let result: Float64Array = float_array
                .iter()
                .map(|opt| opt.map(|v| v.round()))
                .collect();
            Ok(Arc::new(result) as ArrayRef)
        }
        ScalarFunctionType::Sqrt => {
            let arg = evaluated_args.get(0).ok_or_else(|| {
                QueryError::ExecutionError("SQRT requires 1 argument".to_string())
            })?;
            let float_array = arg.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                QueryError::ExecutionError("SQRT requires float argument".to_string())
            })?;
            let result: Float64Array = float_array
                .iter()
                .map(|opt| opt.map(|v| v.sqrt()))
                .collect();
            Ok(Arc::new(result) as ArrayRef)
        }
        ScalarFunctionType::Power => {
            if evaluated_args.len() < 2 {
                return Err(QueryError::ExecutionError(
                    "POWER requires 2 arguments".to_string(),
                ));
            }
            let base = evaluated_args[0]
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    QueryError::ExecutionError("POWER requires float arguments".to_string())
                })?;
            let exp = evaluated_args[1]
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    QueryError::ExecutionError("POWER requires float arguments".to_string())
                })?;
            let result: Float64Array = base
                .iter()
                .zip(exp.iter())
                .map(|(b, e)| match (b, e) {
                    (Some(b), Some(e)) => Some(b.powf(e)),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(result) as ArrayRef)
        }
        ScalarFunctionType::Coalesce => {
            if evaluated_args.is_empty() {
                return Err(QueryError::ExecutionError(
                    "COALESCE requires at least 1 argument".to_string(),
                ));
            }
            // For simplicity, return the first non-null or first argument
            Ok(evaluated_args.into_iter().next().unwrap())
        }
        // Placeholder implementations for remaining functions
        ScalarFunctionType::Substring
        | ScalarFunctionType::Trim
        | ScalarFunctionType::Replace
        | ScalarFunctionType::Nullif => Err(QueryError::ExecutionError(format!(
            "{:?} function not yet fully implemented",
            func
        ))),
    }
}

fn create_literal_array(val: &ScalarValue, size: usize) -> Result<ArrayRef> {
    match val {
        ScalarValue::Boolean(Some(v)) => {
            Ok(Arc::new(BooleanArray::from(vec![*v; size])) as ArrayRef)
        }
        ScalarValue::Int8(Some(v)) => Ok(Arc::new(Int8Array::from(vec![*v; size])) as ArrayRef),
        ScalarValue::Int16(Some(v)) => Ok(Arc::new(Int16Array::from(vec![*v; size])) as ArrayRef),
        ScalarValue::Int32(Some(v)) => Ok(Arc::new(Int32Array::from(vec![*v; size])) as ArrayRef),
        ScalarValue::Int64(Some(v)) => Ok(Arc::new(Int64Array::from(vec![*v; size])) as ArrayRef),
        ScalarValue::UInt8(Some(v)) => Ok(Arc::new(UInt8Array::from(vec![*v; size])) as ArrayRef),
        ScalarValue::UInt16(Some(v)) => Ok(Arc::new(UInt16Array::from(vec![*v; size])) as ArrayRef),
        ScalarValue::UInt32(Some(v)) => Ok(Arc::new(UInt32Array::from(vec![*v; size])) as ArrayRef),
        ScalarValue::UInt64(Some(v)) => Ok(Arc::new(UInt64Array::from(vec![*v; size])) as ArrayRef),
        ScalarValue::Float32(Some(v)) => {
            Ok(Arc::new(Float32Array::from(vec![*v; size])) as ArrayRef)
        }
        ScalarValue::Float64(Some(v)) => {
            Ok(Arc::new(Float64Array::from(vec![*v; size])) as ArrayRef)
        }
        ScalarValue::Utf8(Some(s)) => {
            Ok(Arc::new(StringArray::from(vec![s.as_str(); size])) as ArrayRef)
        }
        ScalarValue::Null => Ok(Arc::new(NullArray::new(size)) as ArrayRef),
        _ => Ok(Arc::new(NullArray::new(size)) as ArrayRef),
    }
}

fn evaluate_unary_op(array: &ArrayRef, op: UnaryOp) -> Result<ArrayRef> {
    match op {
        UnaryOp::Not => {
            let bool_array = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    QueryError::ExecutionError("NOT operator requires boolean array".to_string())
                })?;
            Ok(Arc::new(compute::not(bool_array)?) as ArrayRef)
        }
        UnaryOp::Minus => {
            if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
                let result: Int64Array = arr.iter().map(|v| v.map(|x| -x)).collect();
                Ok(Arc::new(result) as ArrayRef)
            } else if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
                let result: Float64Array = arr.iter().map(|v| v.map(|x| -x)).collect();
                Ok(Arc::new(result) as ArrayRef)
            } else if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
                let result: Int32Array = arr.iter().map(|v| v.map(|x| -x)).collect();
                Ok(Arc::new(result) as ArrayRef)
            } else if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
                let result: Float32Array = arr.iter().map(|v| v.map(|x| -x)).collect();
                Ok(Arc::new(result) as ArrayRef)
            } else {
                Err(QueryError::ExecutionError(
                    "Unsupported type for negation".to_string(),
                ))
            }
        }
    }
}

fn evaluate_binary_op(left: &ArrayRef, op: BinaryOp, right: &ArrayRef) -> Result<ArrayRef> {
    match op {
        BinaryOp::Add => {
            if let (Some(l), Some(r)) = (
                left.as_any().downcast_ref::<Int64Array>(),
                right.as_any().downcast_ref::<Int64Array>(),
            ) {
                let result = add(l, r)?;
                Ok(Arc::new(result) as ArrayRef)
            } else if let (Some(l), Some(r)) = (
                left.as_any().downcast_ref::<Int32Array>(),
                right.as_any().downcast_ref::<Int32Array>(),
            ) {
                let result = add(l, r)?;
                Ok(Arc::new(result) as ArrayRef)
            } else if let (Some(l), Some(r)) = (
                left.as_any().downcast_ref::<Float64Array>(),
                right.as_any().downcast_ref::<Float64Array>(),
            ) {
                let result = add(l, r)?;
                Ok(Arc::new(result) as ArrayRef)
            } else if let (Some(l), Some(r)) = (
                left.as_any().downcast_ref::<Float32Array>(),
                right.as_any().downcast_ref::<Float32Array>(),
            ) {
                let result = add(l, r)?;
                Ok(Arc::new(result) as ArrayRef)
            } else {
                Err(QueryError::ExecutionError(
                    "Unsupported types for addition".to_string(),
                ))
            }
        }
        BinaryOp::Subtract => {
            if let (Some(l), Some(r)) = (
                left.as_any().downcast_ref::<Int64Array>(),
                right.as_any().downcast_ref::<Int64Array>(),
            ) {
                let result = sub(l, r)?;
                Ok(Arc::new(result) as ArrayRef)
            } else if let (Some(l), Some(r)) = (
                left.as_any().downcast_ref::<Int32Array>(),
                right.as_any().downcast_ref::<Int32Array>(),
            ) {
                let result = sub(l, r)?;
                Ok(Arc::new(result) as ArrayRef)
            } else if let (Some(l), Some(r)) = (
                left.as_any().downcast_ref::<Float64Array>(),
                right.as_any().downcast_ref::<Float64Array>(),
            ) {
                let result = sub(l, r)?;
                Ok(Arc::new(result) as ArrayRef)
            } else if let (Some(l), Some(r)) = (
                left.as_any().downcast_ref::<Float32Array>(),
                right.as_any().downcast_ref::<Float32Array>(),
            ) {
                let result = sub(l, r)?;
                Ok(Arc::new(result) as ArrayRef)
            } else {
                Err(QueryError::ExecutionError(
                    "Unsupported types for subtraction".to_string(),
                ))
            }
        }
        BinaryOp::Multiply => {
            if let (Some(l), Some(r)) = (
                left.as_any().downcast_ref::<Int64Array>(),
                right.as_any().downcast_ref::<Int64Array>(),
            ) {
                let result = mul(l, r)?;
                Ok(Arc::new(result) as ArrayRef)
            } else if let (Some(l), Some(r)) = (
                left.as_any().downcast_ref::<Int32Array>(),
                right.as_any().downcast_ref::<Int32Array>(),
            ) {
                let result = mul(l, r)?;
                Ok(Arc::new(result) as ArrayRef)
            } else if let (Some(l), Some(r)) = (
                left.as_any().downcast_ref::<Float64Array>(),
                right.as_any().downcast_ref::<Float64Array>(),
            ) {
                let result = mul(l, r)?;
                Ok(Arc::new(result) as ArrayRef)
            } else if let (Some(l), Some(r)) = (
                left.as_any().downcast_ref::<Float32Array>(),
                right.as_any().downcast_ref::<Float32Array>(),
            ) {
                let result = mul(l, r)?;
                Ok(Arc::new(result) as ArrayRef)
            } else {
                Err(QueryError::ExecutionError(
                    "Unsupported types for multiplication".to_string(),
                ))
            }
        }
        BinaryOp::Divide => {
            if let (Some(l), Some(r)) = (
                left.as_any().downcast_ref::<Int64Array>(),
                right.as_any().downcast_ref::<Int64Array>(),
            ) {
                let result = div(l, r)?;
                Ok(Arc::new(result) as ArrayRef)
            } else if let (Some(l), Some(r)) = (
                left.as_any().downcast_ref::<Int32Array>(),
                right.as_any().downcast_ref::<Int32Array>(),
            ) {
                let result = div(l, r)?;
                Ok(Arc::new(result) as ArrayRef)
            } else if let (Some(l), Some(r)) = (
                left.as_any().downcast_ref::<Float64Array>(),
                right.as_any().downcast_ref::<Float64Array>(),
            ) {
                let result = div(l, r)?;
                Ok(Arc::new(result) as ArrayRef)
            } else if let (Some(l), Some(r)) = (
                left.as_any().downcast_ref::<Float32Array>(),
                right.as_any().downcast_ref::<Float32Array>(),
            ) {
                let result = div(l, r)?;
                Ok(Arc::new(result) as ArrayRef)
            } else {
                Err(QueryError::ExecutionError(
                    "Unsupported types for division".to_string(),
                ))
            }
        }
        BinaryOp::Modulo => modulo_op(left, right),
        BinaryOp::Equal => {
            let result = eq(left, right)?;
            Ok(Arc::new(result) as ArrayRef)
        }
        BinaryOp::NotEqual => {
            let result = neq(left, right)?;
            Ok(Arc::new(result) as ArrayRef)
        }
        BinaryOp::Less => {
            let result = lt(left, right)?;
            Ok(Arc::new(result) as ArrayRef)
        }
        BinaryOp::LessEqual => {
            let result = lt_eq(left, right)?;
            Ok(Arc::new(result) as ArrayRef)
        }
        BinaryOp::Greater => {
            let result = gt(left, right)?;
            Ok(Arc::new(result) as ArrayRef)
        }
        BinaryOp::GreaterEqual => {
            let result = gt_eq(left, right)?;
            Ok(Arc::new(result) as ArrayRef)
        }
        BinaryOp::And => {
            let l = left
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    QueryError::ExecutionError("AND requires boolean arrays".to_string())
                })?;
            let r = right
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    QueryError::ExecutionError("AND requires boolean arrays".to_string())
                })?;
            let result = compute::and(l, r)?;
            Ok(Arc::new(result) as ArrayRef)
        }
        BinaryOp::Or => {
            let l = left
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    QueryError::ExecutionError("OR requires boolean arrays".to_string())
                })?;
            let r = right
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    QueryError::ExecutionError("OR requires boolean arrays".to_string())
                })?;
            let result = compute::or(l, r)?;
            Ok(Arc::new(result) as ArrayRef)
        }
    }
}

fn modulo_op(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef> {
    if let (Some(l), Some(r)) = (
        left.as_any().downcast_ref::<Int64Array>(),
        right.as_any().downcast_ref::<Int64Array>(),
    ) {
        let result: Int64Array = l
            .iter()
            .zip(r.iter())
            .map(|(left_val, right_val)| match (left_val, right_val) {
                (Some(l), Some(r)) if r != 0 => Some(l % r),
                _ => None,
            })
            .collect();
        Ok(Arc::new(result) as ArrayRef)
    } else if let (Some(l), Some(r)) = (
        left.as_any().downcast_ref::<Int32Array>(),
        right.as_any().downcast_ref::<Int32Array>(),
    ) {
        let result: Int32Array = l
            .iter()
            .zip(r.iter())
            .map(|(left_val, right_val)| match (left_val, right_val) {
                (Some(l), Some(r)) if r != 0 => Some(l % r),
                _ => None,
            })
            .collect();
        Ok(Arc::new(result) as ArrayRef)
    } else {
        Err(QueryError::ExecutionError(
            "Modulo operation requires integer arrays".to_string(),
        ))
    }
}

pub fn evaluate_aggregate(func: AggregateFunction, array: &ArrayRef) -> Result<ScalarValue> {
    match func {
        AggregateFunction::Count => {
            let non_null_count = array.len() - array.null_count();
            Ok(ScalarValue::Int64(Some(non_null_count as i64)))
        }
        AggregateFunction::Sum => {
            if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
                let sum_val = compute::sum(arr);
                Ok(ScalarValue::Int64(sum_val))
            } else if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
                let sum_val = compute::sum(arr).map(|v| v as i64);
                Ok(ScalarValue::Int64(sum_val))
            } else if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
                let sum_val = compute::sum(arr);
                Ok(ScalarValue::Float64(sum_val))
            } else if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
                let sum_val = compute::sum(arr).map(|v| v as f64);
                Ok(ScalarValue::Float64(sum_val))
            } else {
                Err(QueryError::ExecutionError(
                    "Unsupported type for SUM".to_string(),
                ))
            }
        }
        AggregateFunction::Avg => {
            if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
                let sum_val = compute::sum(arr).unwrap_or(0) as f64;
                let count = (arr.len() - arr.null_count()) as f64;
                Ok(ScalarValue::Float64(if count > 0.0 {
                    Some(sum_val / count)
                } else {
                    None
                }))
            } else if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
                let sum_val = compute::sum(arr).unwrap_or(0) as f64;
                let count = (arr.len() - arr.null_count()) as f64;
                Ok(ScalarValue::Float64(if count > 0.0 {
                    Some(sum_val / count)
                } else {
                    None
                }))
            } else if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
                let sum_val = compute::sum(arr).unwrap_or(0.0);
                let count = (arr.len() - arr.null_count()) as f64;
                Ok(ScalarValue::Float64(if count > 0.0 {
                    Some(sum_val / count)
                } else {
                    None
                }))
            } else if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
                let sum_val = compute::sum(arr).unwrap_or(0.0) as f64;
                let count = (arr.len() - arr.null_count()) as f64;
                Ok(ScalarValue::Float64(if count > 0.0 {
                    Some(sum_val / count)
                } else {
                    None
                }))
            } else {
                Err(QueryError::ExecutionError(
                    "Unsupported type for AVG".to_string(),
                ))
            }
        }
        AggregateFunction::Min => {
            if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
                let min_val = compute::min(arr);
                Ok(ScalarValue::Int64(min_val))
            } else if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
                let min_val = compute::min(arr);
                Ok(ScalarValue::Int32(min_val))
            } else if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
                let min_val = compute::min(arr);
                Ok(ScalarValue::Float64(min_val))
            } else if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
                let min_val = compute::min(arr);
                Ok(ScalarValue::Float32(min_val))
            } else {
                Err(QueryError::ExecutionError(
                    "Unsupported type for MIN".to_string(),
                ))
            }
        }
        AggregateFunction::Max => {
            if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
                let max_val = compute::max(arr);
                Ok(ScalarValue::Int64(max_val))
            } else if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
                let max_val = compute::max(arr);
                Ok(ScalarValue::Int32(max_val))
            } else if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
                let max_val = compute::max(arr);
                Ok(ScalarValue::Float64(max_val))
            } else if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
                let max_val = compute::max(arr);
                Ok(ScalarValue::Float32(max_val))
            } else {
                Err(QueryError::ExecutionError(
                    "Unsupported type for MAX".to_string(),
                ))
            }
        }
    }
}
