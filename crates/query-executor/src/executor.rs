use crate::operators::{evaluate_aggregate, evaluate_expr};
use crate::physical_plan::{AggregateExpr, PhysicalPlan};
use arrow::array::*;
use arrow::compute::kernels::concat;
use arrow::compute::kernels::filter::filter_record_batch;
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use query_core::{QueryError, Result};
use std::sync::Arc;

pub struct QueryExecutor;

impl QueryExecutor {
    pub fn new() -> Self {
        Self
    }

    pub async fn execute(&self, plan: &PhysicalPlan) -> Result<Vec<RecordBatch>> {
        Box::pin(self.execute_plan(plan)).await
    }

    fn execute_plan<'a>(
        &'a self,
        plan: &'a PhysicalPlan,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<RecordBatch>>> + 'a>> {
        Box::pin(async move {
            match plan {
                PhysicalPlan::Scan { source, .. } => source.scan(),
                PhysicalPlan::Projection { input, exprs } => {
                    let input_batches = self.execute_plan(input).await?;
                    self.execute_projection(input_batches, exprs)
                }
                PhysicalPlan::Filter { input, predicate } => {
                    let input_batches = self.execute_plan(input).await?;
                    self.execute_filter(input_batches, predicate)
                }
                PhysicalPlan::HashAggregate {
                    input,
                    group_exprs,
                    aggr_exprs,
                } => {
                    let input_batches = self.execute_plan(input).await?;
                    self.execute_aggregate(input_batches, group_exprs, aggr_exprs)
                }
                PhysicalPlan::Sort {
                    input,
                    exprs,
                    ascending,
                } => {
                    let input_batches = self.execute_plan(input).await?;
                    self.execute_sort(input_batches, exprs, ascending)
                }
                PhysicalPlan::Limit { input, skip, fetch } => {
                    let input_batches = self.execute_plan(input).await?;
                    self.execute_limit(input_batches, *skip, *fetch)
                }
            }
        })
    }

    fn execute_projection(
        &self,
        batches: Vec<RecordBatch>,
        exprs: &[crate::physical_plan::PhysicalExpr],
    ) -> Result<Vec<RecordBatch>> {
        let mut result_batches = Vec::new();

        for batch in batches {
            let arrays: Result<Vec<_>> = exprs
                .iter()
                .map(|expr| evaluate_expr(expr, &batch))
                .collect();

            let arrays = arrays?;

            if arrays.is_empty() {
                continue;
            }

            let fields: Vec<ArrowField> = arrays
                .iter()
                .enumerate()
                .map(|(i, array)| {
                    ArrowField::new(format!("col_{}", i), array.data_type().clone(), true)
                })
                .collect();

            let schema = Arc::new(ArrowSchema::new(fields));
            let result_batch = RecordBatch::try_new(schema, arrays)?;
            result_batches.push(result_batch);
        }

        Ok(result_batches)
    }

    fn execute_filter(
        &self,
        batches: Vec<RecordBatch>,
        predicate: &crate::physical_plan::PhysicalExpr,
    ) -> Result<Vec<RecordBatch>> {
        let mut result_batches = Vec::new();

        for batch in batches {
            let filter_array = evaluate_expr(predicate, &batch)?;
            let boolean_array = filter_array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    QueryError::ExecutionError("Filter predicate must return boolean".to_string())
                })?;

            let filtered_batch = filter_record_batch(&batch, boolean_array)?;

            if filtered_batch.num_rows() > 0 {
                result_batches.push(filtered_batch);
            }
        }

        Ok(result_batches)
    }

    fn execute_aggregate(
        &self,
        batches: Vec<RecordBatch>,
        group_exprs: &[crate::physical_plan::PhysicalExpr],
        aggr_exprs: &[AggregateExpr],
    ) -> Result<Vec<RecordBatch>> {
        if aggr_exprs.is_empty() {
            return Ok(vec![]);
        }

        if group_exprs.is_empty() {
            let mut aggregate_results = Vec::new();

            for aggr_expr in aggr_exprs {
                let mut all_values: Vec<ArrayRef> = Vec::new();

                for batch in &batches {
                    let array = evaluate_expr(&aggr_expr.expr, batch)?;
                    all_values.push(array);
                }

                if let Some(_first) = all_values.first() {
                    let refs: Vec<&dyn Array> = all_values.iter().map(|a| a.as_ref()).collect();
                    let concatenated = concat::concat(&refs)?;
                    let result = evaluate_aggregate(aggr_expr.func, &concatenated)?;
                    aggregate_results.push(result);
                }
            }

            return self.scalar_values_to_batch(&aggregate_results);
        }

        Ok(vec![])
    }

    fn scalar_values_to_batch(
        &self,
        values: &[query_planner::ScalarValue],
    ) -> Result<Vec<RecordBatch>> {
        if values.is_empty() {
            return Ok(vec![]);
        }

        let mut arrays: Vec<ArrayRef> = Vec::new();
        let mut fields: Vec<ArrowField> = Vec::new();

        for (i, value) in values.iter().enumerate() {
            let (array, field) = self.scalar_value_to_array(value, &format!("col_{}", i))?;
            arrays.push(array);
            fields.push(field);
        }

        let schema = Arc::new(ArrowSchema::new(fields));
        let batch = RecordBatch::try_new(schema, arrays)?;
        Ok(vec![batch])
    }

    fn scalar_value_to_array(
        &self,
        value: &query_planner::ScalarValue,
        name: &str,
    ) -> Result<(ArrayRef, ArrowField)> {
        use query_planner::ScalarValue;

        match value {
            ScalarValue::Boolean(v) => {
                let array = Arc::new(BooleanArray::from(vec![*v])) as ArrayRef;
                let field = ArrowField::new(name, ArrowDataType::Boolean, true);
                Ok((array, field))
            }
            ScalarValue::Int8(v) => {
                let array = Arc::new(Int8Array::from(vec![*v])) as ArrayRef;
                let field = ArrowField::new(name, ArrowDataType::Int8, true);
                Ok((array, field))
            }
            ScalarValue::Int16(v) => {
                let array = Arc::new(Int16Array::from(vec![*v])) as ArrayRef;
                let field = ArrowField::new(name, ArrowDataType::Int16, true);
                Ok((array, field))
            }
            ScalarValue::Int32(v) => {
                let array = Arc::new(Int32Array::from(vec![*v])) as ArrayRef;
                let field = ArrowField::new(name, ArrowDataType::Int32, true);
                Ok((array, field))
            }
            ScalarValue::Int64(v) => {
                let array = Arc::new(Int64Array::from(vec![*v])) as ArrayRef;
                let field = ArrowField::new(name, ArrowDataType::Int64, true);
                Ok((array, field))
            }
            ScalarValue::UInt8(v) => {
                let array = Arc::new(UInt8Array::from(vec![*v])) as ArrayRef;
                let field = ArrowField::new(name, ArrowDataType::UInt8, true);
                Ok((array, field))
            }
            ScalarValue::UInt16(v) => {
                let array = Arc::new(UInt16Array::from(vec![*v])) as ArrayRef;
                let field = ArrowField::new(name, ArrowDataType::UInt16, true);
                Ok((array, field))
            }
            ScalarValue::UInt32(v) => {
                let array = Arc::new(UInt32Array::from(vec![*v])) as ArrayRef;
                let field = ArrowField::new(name, ArrowDataType::UInt32, true);
                Ok((array, field))
            }
            ScalarValue::UInt64(v) => {
                let array = Arc::new(UInt64Array::from(vec![*v])) as ArrayRef;
                let field = ArrowField::new(name, ArrowDataType::UInt64, true);
                Ok((array, field))
            }
            ScalarValue::Float32(v) => {
                let array = Arc::new(Float32Array::from(vec![*v])) as ArrayRef;
                let field = ArrowField::new(name, ArrowDataType::Float32, true);
                Ok((array, field))
            }
            ScalarValue::Float64(v) => {
                let array = Arc::new(Float64Array::from(vec![*v])) as ArrayRef;
                let field = ArrowField::new(name, ArrowDataType::Float64, true);
                Ok((array, field))
            }
            ScalarValue::Utf8(v) => {
                let array = Arc::new(StringArray::from(vec![v.as_deref()])) as ArrayRef;
                let field = ArrowField::new(name, ArrowDataType::Utf8, true);
                Ok((array, field))
            }
            ScalarValue::Null => {
                let array = Arc::new(NullArray::new(1)) as ArrayRef;
                let field = ArrowField::new(name, ArrowDataType::Null, true);
                Ok((array, field))
            }
        }
    }

    fn execute_sort(
        &self,
        batches: Vec<RecordBatch>,
        _exprs: &[crate::physical_plan::PhysicalExpr],
        _ascending: &[bool],
    ) -> Result<Vec<RecordBatch>> {
        Ok(batches)
    }

    fn execute_limit(
        &self,
        batches: Vec<RecordBatch>,
        skip: usize,
        fetch: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        let mut total_rows = 0;
        let mut result_batches = Vec::new();
        let mut remaining_skip = skip;

        for batch in batches {
            let batch_rows = batch.num_rows();

            if remaining_skip >= batch_rows {
                remaining_skip -= batch_rows;
                continue;
            }

            let start = remaining_skip;
            remaining_skip = 0;

            let end = if let Some(limit) = fetch {
                let remaining = limit.saturating_sub(total_rows);
                (start + remaining).min(batch_rows)
            } else {
                batch_rows
            };

            if start < end {
                let sliced = batch.slice(start, end - start);
                total_rows += sliced.num_rows();
                result_batches.push(sliced);

                if let Some(limit) = fetch {
                    if total_rows >= limit {
                        break;
                    }
                }
            }
        }

        Ok(result_batches)
    }
}

impl Default for QueryExecutor {
    fn default() -> Self {
        Self::new()
    }
}
