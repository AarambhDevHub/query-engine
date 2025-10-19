use crate::operators::{evaluate_aggregate, evaluate_expr};
use crate::physical_plan::{AggregateExpr, PhysicalPlan};
use arrow::array::*;
use arrow::compute::kernels::concat;
use arrow::compute::kernels::filter::filter_record_batch;
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use query_core::{QueryError, Result};
use query_parser::JoinType;
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
                PhysicalPlan::HashJoin {
                    left,
                    right,
                    join_type,
                    on,
                } => {
                    let left_batches = self.execute_plan(left).await?;
                    let right_batches = self.execute_plan(right).await?;
                    self.execute_join(left_batches, right_batches, *join_type, on.as_ref())
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

    fn execute_join(
        &self,
        left_batches: Vec<RecordBatch>,
        right_batches: Vec<RecordBatch>,
        join_type: JoinType,
        on: Option<&crate::physical_plan::PhysicalExpr>,
    ) -> Result<Vec<RecordBatch>> {
        if left_batches.is_empty() || right_batches.is_empty() {
            return Ok(vec![]);
        }

        match join_type {
            JoinType::Inner => self.execute_inner_join(left_batches, right_batches, on),
            JoinType::Left => self.execute_left_join(left_batches, right_batches, on),
            JoinType::Right => self.execute_right_join(left_batches, right_batches, on),
            JoinType::Full => self.execute_full_join(left_batches, right_batches, on),
            JoinType::Cross => self.execute_cross_join(left_batches, right_batches),
        }
    }

    fn execute_inner_join(
        &self,
        left_batches: Vec<RecordBatch>,
        right_batches: Vec<RecordBatch>,
        _on: Option<&crate::physical_plan::PhysicalExpr>,
    ) -> Result<Vec<RecordBatch>> {
        let mut result_batches = Vec::new();

        for left_batch in &left_batches {
            for right_batch in &right_batches {
                let joined = self.join_batches(left_batch, right_batch)?;
                if joined.num_rows() > 0 {
                    result_batches.push(joined);
                }
            }
        }

        Ok(result_batches)
    }

    fn execute_left_join(
        &self,
        left_batches: Vec<RecordBatch>,
        right_batches: Vec<RecordBatch>,
        _on: Option<&crate::physical_plan::PhysicalExpr>,
    ) -> Result<Vec<RecordBatch>> {
        let mut result_batches = Vec::new();

        for left_batch in &left_batches {
            for right_batch in &right_batches {
                let joined = self.join_batches(left_batch, right_batch)?;
                result_batches.push(joined);
            }
        }

        Ok(result_batches)
    }

    fn execute_right_join(
        &self,
        left_batches: Vec<RecordBatch>,
        right_batches: Vec<RecordBatch>,
        _on: Option<&crate::physical_plan::PhysicalExpr>,
    ) -> Result<Vec<RecordBatch>> {
        let mut result_batches = Vec::new();

        for right_batch in &right_batches {
            for left_batch in &left_batches {
                let joined = self.join_batches(left_batch, right_batch)?;
                result_batches.push(joined);
            }
        }

        Ok(result_batches)
    }

    fn execute_full_join(
        &self,
        left_batches: Vec<RecordBatch>,
        right_batches: Vec<RecordBatch>,
        _on: Option<&crate::physical_plan::PhysicalExpr>,
    ) -> Result<Vec<RecordBatch>> {
        let mut result_batches = Vec::new();

        for left_batch in &left_batches {
            for right_batch in &right_batches {
                let joined = self.join_batches(left_batch, right_batch)?;
                result_batches.push(joined);
            }
        }

        Ok(result_batches)
    }

    fn execute_cross_join(
        &self,
        left_batches: Vec<RecordBatch>,
        right_batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>> {
        let mut result_batches = Vec::new();

        for left_batch in &left_batches {
            for right_batch in &right_batches {
                // Cross join: every row from left with every row from right
                let left_rows = left_batch.num_rows();
                let right_rows = right_batch.num_rows();

                let mut left_columns = Vec::new();
                let mut right_columns = Vec::new();

                // Repeat left columns for each right row
                for col in left_batch.columns() {
                    let mut builder = Vec::new();
                    for _ in 0..right_rows {
                        for i in 0..left_rows {
                            builder.push(i);
                        }
                    }
                    // Take indices to repeat rows
                    let indices =
                        UInt32Array::from(builder.iter().map(|&i| i as u32).collect::<Vec<_>>());
                    let repeated = arrow::compute::take(col.as_ref(), &indices, None)?;
                    left_columns.push(repeated);
                }

                // Repeat right columns for each left row
                for col in right_batch.columns() {
                    let mut builder = Vec::new();
                    for right_idx in 0..right_rows {
                        for _ in 0..left_rows {
                            builder.push(right_idx);
                        }
                    }
                    let indices =
                        UInt32Array::from(builder.iter().map(|&i| i as u32).collect::<Vec<_>>());
                    let repeated = arrow::compute::take(col.as_ref(), &indices, None)?;
                    right_columns.push(repeated);
                }

                let mut all_columns = left_columns;
                all_columns.extend(right_columns);

                // Create merged schema
                let left_schema = left_batch.schema();
                let right_schema = right_batch.schema();
                let mut fields = left_schema.fields().to_vec();
                fields.extend(right_schema.fields().to_vec());
                let merged_schema = Arc::new(ArrowSchema::new(fields));

                let result_batch = RecordBatch::try_new(merged_schema, all_columns)?;
                result_batches.push(result_batch);
            }
        }

        Ok(result_batches)
    }

    fn join_batches(&self, left: &RecordBatch, right: &RecordBatch) -> Result<RecordBatch> {
        let left_rows = left.num_rows();
        let right_rows = right.num_rows();

        let mut all_columns = Vec::new();

        // Add left columns (repeated for each right row)
        for col in left.columns() {
            let mut builder = Vec::new();
            for left_idx in 0..left_rows {
                for _ in 0..right_rows {
                    builder.push(left_idx as u32);
                }
            }
            let indices = UInt32Array::from(builder);
            let repeated = arrow::compute::take(col.as_ref(), &indices, None)?;
            all_columns.push(repeated);
        }

        // Add right columns (cycled for each left row)
        for col in right.columns() {
            let mut builder = Vec::new();
            for _ in 0..left_rows {
                for right_idx in 0..right_rows {
                    builder.push(right_idx as u32);
                }
            }
            let indices = UInt32Array::from(builder);
            let repeated = arrow::compute::take(col.as_ref(), &indices, None)?;
            all_columns.push(repeated);
        }

        // Create merged schema
        let left_schema = left.schema();
        let right_schema = right.schema();
        let mut fields = left_schema.fields().to_vec();
        fields.extend(right_schema.fields().to_vec());
        let merged_schema = Arc::new(ArrowSchema::new(fields));

        RecordBatch::try_new(merged_schema, all_columns).map_err(|e| e.into())
    }
}

impl Default for QueryExecutor {
    fn default() -> Self {
        Self::new()
    }
}
