//! PostgreSQL System Catalog Support
//!
//! Provides virtual pg_catalog tables for PostgreSQL client compatibility.
//! These tables are built dynamically from registered tables.

use crate::backend::TableEntry;
use crate::result::{record_batch_to_rows, schema_to_field_info};
use arrow::array::{ArrayRef, BooleanArray, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use pgwire::api::results::{QueryResponse, Response};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Create a user error with proper ErrorInfo
fn user_error(message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "42000".to_string(),
        message,
    )))
}

/// Handle queries to pg_catalog virtual tables
pub async fn handle_pg_catalog_query(
    sql: &str,
    tables: &Arc<RwLock<HashMap<String, TableEntry>>>,
) -> PgWireResult<Vec<Response<'static>>> {
    let sql_upper = sql.to_uppercase();

    // Determine which catalog table is being queried
    if sql_upper.contains("PG_TABLES") || sql_upper.contains("PG_CATALOG.PG_TABLES") {
        return handle_pg_tables(tables).await;
    }

    if sql_upper.contains("PG_ATTRIBUTE") || sql_upper.contains("PG_CATALOG.PG_ATTRIBUTE") {
        return handle_pg_attribute(tables).await;
    }

    if sql_upper.contains("PG_TYPE") || sql_upper.contains("PG_CATALOG.PG_TYPE") {
        return handle_pg_type().await;
    }

    // information_schema.columns is commonly used
    if sql_upper.contains("INFORMATION_SCHEMA.COLUMNS") {
        return handle_information_schema_columns(tables).await;
    }

    Err(user_error(format!("Unsupported catalog query: {}", sql)))
}

/// Handle SELECT * FROM pg_catalog.pg_tables
async fn handle_pg_tables(
    tables: &Arc<RwLock<HashMap<String, TableEntry>>>,
) -> PgWireResult<Vec<Response<'static>>> {
    let tables_guard = tables.read().await;

    let table_names: Vec<&str> = tables_guard.keys().map(|s| s.as_str()).collect();
    let count = table_names.len();

    // Build pg_tables result
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("schemaname", DataType::Utf8, false),
        Field::new("tablename", DataType::Utf8, false),
        Field::new("tableowner", DataType::Utf8, true),
        Field::new("tablespace", DataType::Utf8, true),
        Field::new("hasindexes", DataType::Boolean, false),
        Field::new("hasrules", DataType::Boolean, false),
        Field::new("hastriggers", DataType::Boolean, false),
        Field::new("rowsecurity", DataType::Boolean, false),
    ]));

    let schemanames: Vec<&str> = vec!["public"; count];
    let owners: Vec<Option<&str>> = vec![Some("query_engine"); count];
    let tablespaces: Vec<Option<&str>> = vec![None; count];
    let hasindexes: Vec<bool> = vec![false; count];
    let hasrules: Vec<bool> = vec![false; count];
    let hastriggers: Vec<bool> = vec![false; count];
    let rowsecurity: Vec<bool> = vec![false; count];

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(schemanames)) as ArrayRef,
            Arc::new(StringArray::from(table_names)) as ArrayRef,
            Arc::new(StringArray::from(owners)) as ArrayRef,
            Arc::new(StringArray::from(tablespaces)) as ArrayRef,
            Arc::new(BooleanArray::from(hasindexes)) as ArrayRef,
            Arc::new(BooleanArray::from(hasrules)) as ArrayRef,
            Arc::new(BooleanArray::from(hastriggers)) as ArrayRef,
            Arc::new(BooleanArray::from(rowsecurity)) as ArrayRef,
        ],
    )
    .map_err(|e| user_error(format!("Error creating pg_tables result: {}", e)))?;

    build_query_response(batch, schema)
}

/// Handle SELECT * FROM pg_catalog.pg_attribute
async fn handle_pg_attribute(
    tables: &Arc<RwLock<HashMap<String, TableEntry>>>,
) -> PgWireResult<Vec<Response<'static>>> {
    let tables_guard = tables.read().await;

    let mut attrelids: Vec<i64> = Vec::new();
    let mut attnames: Vec<String> = Vec::new();
    let mut atttypids: Vec<i32> = Vec::new();
    let mut attnums: Vec<i32> = Vec::new();
    let mut attnotnulls: Vec<bool> = Vec::new();
    let mut table_names: Vec<String> = Vec::new();

    let mut table_oid: i64 = 16384; // PostgreSQL starts user table OIDs around here

    for (table_name, entry) in tables_guard.iter() {
        for (col_idx, field) in entry.schema.fields().iter().enumerate() {
            attrelids.push(table_oid);
            attnames.push(field.name().to_string());
            atttypids.push(datatype_to_pg_oid(field.data_type()));
            attnums.push((col_idx + 1) as i32);
            attnotnulls.push(!field.nullable());
            table_names.push(table_name.clone());
        }
        table_oid += 1;
    }

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("attrelid", DataType::Int64, false),
        Field::new("attname", DataType::Utf8, false),
        Field::new("atttypid", DataType::Int32, false),
        Field::new("attnum", DataType::Int32, false),
        Field::new("attnotnull", DataType::Boolean, false),
        Field::new("tablename", DataType::Utf8, false), // Extension for usability
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(attrelids)) as ArrayRef,
            Arc::new(StringArray::from(attnames)) as ArrayRef,
            Arc::new(Int32Array::from(atttypids)) as ArrayRef,
            Arc::new(Int32Array::from(attnums)) as ArrayRef,
            Arc::new(BooleanArray::from(attnotnulls)) as ArrayRef,
            Arc::new(StringArray::from(table_names)) as ArrayRef,
        ],
    )
    .map_err(|e| user_error(format!("Error creating pg_attribute result: {}", e)))?;

    build_query_response(batch, schema)
}

/// Handle SELECT * FROM pg_catalog.pg_type
async fn handle_pg_type() -> PgWireResult<Vec<Response<'static>>> {
    // Define standard PostgreSQL type mappings
    let types = vec![
        (23, "int4", 4),        // INT32
        (20, "int8", 8),        // INT64
        (21, "int2", 2),        // INT16
        (700, "float4", 4),     // FLOAT32
        (701, "float8", 8),     // FLOAT64
        (25, "text", -1),       // TEXT/UTF8
        (1043, "varchar", -1),  // VARCHAR
        (16, "bool", 1),        // BOOLEAN
        (1082, "date", 4),      // DATE
        (1114, "timestamp", 8), // TIMESTAMP
        (17, "bytea", -1),      // BINARY
        // New types
        (2950, "uuid", 16),     // UUID
        (1700, "numeric", -1),  // DECIMAL/NUMERIC
        (1186, "interval", 16), // INTERVAL
        (114, "json", -1),      // JSON
        (3802, "jsonb", -1),    // JSONB
        // Geometric types
        (600, "point", 16),   // POINT
        (628, "line", 24),    // LINE
        (601, "lseg", 32),    // LSEG
        (603, "box", 32),     // BOX
        (602, "path", -1),    // PATH
        (604, "polygon", -1), // POLYGON
        (718, "circle", 24),  // CIRCLE
    ];

    let oids: Vec<i32> = types.iter().map(|(oid, _, _)| *oid).collect();
    let typnames: Vec<&str> = types.iter().map(|(_, name, _)| *name).collect();
    let typlens: Vec<i32> = types.iter().map(|(_, _, len)| *len).collect();

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("oid", DataType::Int32, false),
        Field::new("typname", DataType::Utf8, false),
        Field::new("typlen", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(oids)) as ArrayRef,
            Arc::new(StringArray::from(typnames)) as ArrayRef,
            Arc::new(Int32Array::from(typlens)) as ArrayRef,
        ],
    )
    .map_err(|e| user_error(format!("Error creating pg_type result: {}", e)))?;

    build_query_response(batch, schema)
}

/// Handle SELECT * FROM information_schema.columns
async fn handle_information_schema_columns(
    tables: &Arc<RwLock<HashMap<String, TableEntry>>>,
) -> PgWireResult<Vec<Response<'static>>> {
    let tables_guard = tables.read().await;

    let mut table_catalogs: Vec<&str> = Vec::new();
    let mut table_schemas: Vec<&str> = Vec::new();
    let mut table_names: Vec<String> = Vec::new();
    let mut column_names: Vec<String> = Vec::new();
    let mut ordinal_positions: Vec<i32> = Vec::new();
    let mut is_nullables: Vec<&str> = Vec::new();
    let mut data_types: Vec<String> = Vec::new();

    for (table_name, entry) in tables_guard.iter() {
        for (col_idx, field) in entry.schema.fields().iter().enumerate() {
            table_catalogs.push("query_engine");
            table_schemas.push("public");
            table_names.push(table_name.clone());
            column_names.push(field.name().to_string());
            ordinal_positions.push((col_idx + 1) as i32);
            is_nullables.push(if field.nullable() { "YES" } else { "NO" });
            data_types.push(datatype_to_pg_name(field.data_type()));
        }
    }

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("table_catalog", DataType::Utf8, false),
        Field::new("table_schema", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("column_name", DataType::Utf8, false),
        Field::new("ordinal_position", DataType::Int32, false),
        Field::new("is_nullable", DataType::Utf8, false),
        Field::new("data_type", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(table_catalogs)) as ArrayRef,
            Arc::new(StringArray::from(table_schemas)) as ArrayRef,
            Arc::new(StringArray::from(table_names)) as ArrayRef,
            Arc::new(StringArray::from(column_names)) as ArrayRef,
            Arc::new(Int32Array::from(ordinal_positions)) as ArrayRef,
            Arc::new(StringArray::from(is_nullables)) as ArrayRef,
            Arc::new(StringArray::from(data_types)) as ArrayRef,
        ],
    )
    .map_err(|e| user_error(format!("Error creating columns result: {}", e)))?;

    build_query_response(batch, schema)
}

/// Convert query-core DataType to PostgreSQL type OID
fn datatype_to_pg_oid(dt: &query_core::DataType) -> i32 {
    match dt {
        query_core::DataType::Int8 | query_core::DataType::Int16 => 21, // int2
        query_core::DataType::Int32 => 23,                              // int4
        query_core::DataType::Int64 => 20,                              // int8
        query_core::DataType::UInt8 | query_core::DataType::UInt16 => 21,
        query_core::DataType::UInt32 => 23,
        query_core::DataType::UInt64 => 20,
        query_core::DataType::Float32 => 700,    // float4
        query_core::DataType::Float64 => 701,    // float8
        query_core::DataType::Utf8 => 25,        // text
        query_core::DataType::Binary => 17,      // bytea
        query_core::DataType::LargeBinary => 17, // bytea
        query_core::DataType::Boolean => 16,     // bool
        query_core::DataType::Date32 | query_core::DataType::Date64 => 1082, // date
        query_core::DataType::Timestamp => 1114, // timestamp
        // New types
        query_core::DataType::Uuid => 2950,              // uuid
        query_core::DataType::Decimal128 { .. } => 1700, // numeric
        query_core::DataType::Interval => 1186,          // interval
        query_core::DataType::Json => 114,               // json
        query_core::DataType::List(_) => 0,              // arrays need special handling
        // Geometric types
        query_core::DataType::Point => 600,       // point
        query_core::DataType::Line => 628,        // line
        query_core::DataType::LineSegment => 601, // lseg
        query_core::DataType::Box => 603,         // box
        query_core::DataType::Path => 602,        // path
        query_core::DataType::Polygon => 604,     // polygon
        query_core::DataType::Circle => 718,      // circle
        // Enum (user-defined)
        query_core::DataType::Enum { .. } => 0, // custom enum OID
        query_core::DataType::Null => 0,
    }
}

/// Convert query-core DataType to PostgreSQL type name
fn datatype_to_pg_name(dt: &query_core::DataType) -> String {
    match dt {
        query_core::DataType::Int8 => "smallint".to_string(),
        query_core::DataType::Int16 => "smallint".to_string(),
        query_core::DataType::Int32 => "integer".to_string(),
        query_core::DataType::Int64 => "bigint".to_string(),
        query_core::DataType::UInt8 => "smallint".to_string(),
        query_core::DataType::UInt16 => "smallint".to_string(),
        query_core::DataType::UInt32 => "integer".to_string(),
        query_core::DataType::UInt64 => "bigint".to_string(),
        query_core::DataType::Float32 => "real".to_string(),
        query_core::DataType::Float64 => "double precision".to_string(),
        query_core::DataType::Utf8 => "text".to_string(),
        query_core::DataType::Binary => "bytea".to_string(),
        query_core::DataType::LargeBinary => "bytea".to_string(),
        query_core::DataType::Boolean => "boolean".to_string(),
        query_core::DataType::Date32 | query_core::DataType::Date64 => "date".to_string(),
        query_core::DataType::Timestamp => "timestamp".to_string(),
        // New types
        query_core::DataType::Uuid => "uuid".to_string(),
        query_core::DataType::Decimal128 { precision, scale } => {
            format!("numeric({},{})", precision, scale)
        }
        query_core::DataType::Interval => "interval".to_string(),
        query_core::DataType::Json => "json".to_string(),
        query_core::DataType::List(inner) => format!("{}[]", datatype_to_pg_name(inner)),
        // Geometric types
        query_core::DataType::Point => "point".to_string(),
        query_core::DataType::Line => "line".to_string(),
        query_core::DataType::LineSegment => "lseg".to_string(),
        query_core::DataType::Box => "box".to_string(),
        query_core::DataType::Path => "path".to_string(),
        query_core::DataType::Polygon => "polygon".to_string(),
        query_core::DataType::Circle => "circle".to_string(),
        // Enum
        query_core::DataType::Enum { name, .. } => name.clone(),
        query_core::DataType::Null => "unknown".to_string(),
    }
}

/// Helper to build query response from RecordBatch
fn build_query_response(
    batch: RecordBatch,
    schema: Arc<ArrowSchema>,
) -> PgWireResult<Vec<Response<'static>>> {
    let field_info = schema_to_field_info(&schema);
    let rows = record_batch_to_rows(&batch, &field_info)?;
    let all_rows: Vec<_> = rows.into_iter().map(|r| r.finish()).collect();

    Ok(vec![Response::Query(QueryResponse::new(
        Arc::new(field_info),
        futures::stream::iter(all_rows),
    ))])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datatype_to_pg_oid() {
        assert_eq!(datatype_to_pg_oid(&query_core::DataType::Int64), 20);
        assert_eq!(datatype_to_pg_oid(&query_core::DataType::Utf8), 25);
        assert_eq!(datatype_to_pg_oid(&query_core::DataType::Boolean), 16);
    }

    #[test]
    fn test_datatype_to_pg_name() {
        assert_eq!(datatype_to_pg_name(&query_core::DataType::Int64), "bigint");
        assert_eq!(datatype_to_pg_name(&query_core::DataType::Utf8), "text");
        assert_eq!(
            datatype_to_pg_name(&query_core::DataType::Float64),
            "double precision"
        );
    }
}
