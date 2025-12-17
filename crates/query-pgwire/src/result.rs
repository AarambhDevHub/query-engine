//! Arrow to PostgreSQL type conversion utilities

use arrow::array::*;
use arrow::datatypes::DataType as ArrowDataType;
use arrow::record_batch::RecordBatch;
use pgwire::api::Type;
use pgwire::api::results::{DataRowEncoder, FieldInfo};
use pgwire::error::{PgWireError, PgWireResult};

/// Convert Arrow DataType to PostgreSQL Type
pub fn arrow_to_pg_type(arrow_type: &ArrowDataType) -> Type {
    match arrow_type {
        ArrowDataType::Boolean => Type::BOOL,
        ArrowDataType::Int8 => Type::INT2,
        ArrowDataType::Int16 => Type::INT2,
        ArrowDataType::Int32 => Type::INT4,
        ArrowDataType::Int64 => Type::INT8,
        ArrowDataType::UInt8 => Type::INT2,
        ArrowDataType::UInt16 => Type::INT4,
        ArrowDataType::UInt32 => Type::INT8,
        ArrowDataType::UInt64 => Type::INT8,
        ArrowDataType::Float16 => Type::FLOAT4,
        ArrowDataType::Float32 => Type::FLOAT4,
        ArrowDataType::Float64 => Type::FLOAT8,
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => Type::TEXT,
        ArrowDataType::Binary | ArrowDataType::LargeBinary => Type::BYTEA,
        ArrowDataType::Date32 | ArrowDataType::Date64 => Type::DATE,
        ArrowDataType::Timestamp(_, _) => Type::TIMESTAMP,
        ArrowDataType::Time32(_) | ArrowDataType::Time64(_) => Type::TIME,
        ArrowDataType::Null => Type::TEXT,
        _ => Type::TEXT, // Default to TEXT for unsupported types
    }
}

/// Build FieldInfo vector from Arrow schema
pub fn schema_to_field_info(schema: &arrow::datatypes::Schema) -> Vec<FieldInfo> {
    schema
        .fields()
        .iter()
        .map(|field| {
            let pg_type = arrow_to_pg_type(field.data_type());
            FieldInfo::new(
                field.name().clone(),
                None,
                None,
                pg_type,
                pgwire::api::results::FieldFormat::Text,
            )
        })
        .collect()
}

use std::sync::Arc;

/// Convert Arrow RecordBatch rows to pgwire DataRow format
pub fn record_batch_to_rows(
    batch: &RecordBatch,
    field_info: &[FieldInfo],
) -> PgWireResult<Vec<DataRowEncoder>> {
    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();
    let mut rows = Vec::with_capacity(num_rows);

    // Create Arc for field info to share across encoders
    let field_info_arc: Arc<Vec<FieldInfo>> = Arc::new(field_info.to_vec());

    for row_idx in 0..num_rows {
        let mut encoder = DataRowEncoder::new(Arc::clone(&field_info_arc));

        for col_idx in 0..num_cols {
            let column = batch.column(col_idx);
            encode_value(&mut encoder, column, row_idx)?;
        }

        rows.push(encoder);
    }

    Ok(rows)
}

/// Encode a single value from an Arrow array into the DataRowEncoder
fn encode_value(
    encoder: &mut DataRowEncoder,
    array: &ArrayRef,
    row_idx: usize,
) -> PgWireResult<()> {
    if array.is_null(row_idx) {
        encoder.encode_field::<Option<&str>>(&None)?;
        return Ok(());
    }

    match array.data_type() {
        ArrowDataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            encoder.encode_field(&arr.value(row_idx))?;
        }
        ArrowDataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            encoder.encode_field(&(arr.value(row_idx) as i16))?;
        }
        ArrowDataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            encoder.encode_field(&arr.value(row_idx))?;
        }
        ArrowDataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            encoder.encode_field(&arr.value(row_idx))?;
        }
        ArrowDataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            encoder.encode_field(&arr.value(row_idx))?;
        }
        ArrowDataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            encoder.encode_field(&(arr.value(row_idx) as i16))?;
        }
        ArrowDataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            encoder.encode_field(&(arr.value(row_idx) as i32))?;
        }
        ArrowDataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            encoder.encode_field(&(arr.value(row_idx) as i64))?;
        }
        ArrowDataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            // Convert to string to avoid overflow
            encoder.encode_field(&arr.value(row_idx).to_string())?;
        }
        ArrowDataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            encoder.encode_field(&arr.value(row_idx))?;
        }
        ArrowDataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            encoder.encode_field(&arr.value(row_idx))?;
        }
        ArrowDataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            encoder.encode_field(&arr.value(row_idx))?;
        }
        ArrowDataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            encoder.encode_field(&arr.value(row_idx))?;
        }
        ArrowDataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            // Convert days since epoch to a date string
            let days = arr.value(row_idx);
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719163)
                .map(|d| d.to_string())
                .unwrap_or_else(|| "NULL".to_string());
            encoder.encode_field(&date)?;
        }
        ArrowDataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
            // Convert milliseconds since epoch to date string
            let ms = arr.value(row_idx);
            let secs = ms / 1000;
            let nsecs = ((ms % 1000) * 1_000_000) as u32;
            if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs) {
                encoder.encode_field(&dt.format("%Y-%m-%d").to_string())?;
            } else {
                encoder.encode_field(&"NULL")?;
            }
        }
        _ => {
            // Default: convert to string representation
            let formatted = arrow::util::display::array_value_to_string(array, row_idx)
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            encoder.encode_field(&formatted)?;
        }
    }

    Ok(())
}
