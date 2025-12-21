use arrow::datatypes::DataType as ArrowDataType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Utf8,
    Binary,
    LargeBinary,
    Date32,
    Date64,
    Timestamp,
    Null,
    // PostgreSQL-compatible types
    Uuid,                                    // UUID (stored as FixedSizeBinary(16))
    Decimal128 { precision: u8, scale: i8 }, // NUMERIC/DECIMAL
    Interval,                                // Time interval
    Json,                                    // JSON/JSONB (stored as Utf8)
    List(Box<DataType>),                     // ARRAY types
    // Geometric types (stored as Utf8/text representation)
    Point,       // (x, y) coordinate
    Line,        // Infinite line {A,B,C}
    LineSegment, // Line segment [(x1,y1),(x2,y2)]
    Box,         // Rectangle ((x1,y1),(x2,y2))
    Path,        // Path [(x1,y1),...]
    Polygon,     // Polygon ((x1,y1),...)
    Circle,      // Circle <(x,y),r>
    // Enum type
    Enum { name: String, values: Vec<String> }, // User-defined enum
    // Full text search types
    TsVector, // Text search document vector (stored as Utf8)
    TsQuery,  // Text search query (stored as Utf8)
}

impl DataType {
    pub fn to_arrow(&self) -> ArrowDataType {
        match self {
            DataType::Boolean => ArrowDataType::Boolean,
            DataType::Int8 => ArrowDataType::Int8,
            DataType::Int16 => ArrowDataType::Int16,
            DataType::Int32 => ArrowDataType::Int32,
            DataType::Int64 => ArrowDataType::Int64,
            DataType::UInt8 => ArrowDataType::UInt8,
            DataType::UInt16 => ArrowDataType::UInt16,
            DataType::UInt32 => ArrowDataType::UInt32,
            DataType::UInt64 => ArrowDataType::UInt64,
            DataType::Float32 => ArrowDataType::Float32,
            DataType::Float64 => ArrowDataType::Float64,
            DataType::Utf8 => ArrowDataType::Utf8,
            DataType::Binary => ArrowDataType::Binary,
            DataType::LargeBinary => ArrowDataType::LargeBinary,
            DataType::Date32 => ArrowDataType::Date32,
            DataType::Date64 => ArrowDataType::Date64,
            DataType::Timestamp => {
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
            }
            DataType::Null => ArrowDataType::Null,
            // New types
            DataType::Uuid => ArrowDataType::FixedSizeBinary(16),
            DataType::Decimal128 { precision, scale } => {
                ArrowDataType::Decimal128(*precision, *scale)
            }
            DataType::Interval => {
                ArrowDataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano)
            }
            DataType::Json => ArrowDataType::Utf8, // JSON stored as text
            DataType::List(inner) => ArrowDataType::List(std::sync::Arc::new(
                arrow::datatypes::Field::new("item", inner.to_arrow(), true),
            )),
            // Geometric types stored as text
            DataType::Point
            | DataType::Line
            | DataType::LineSegment
            | DataType::Box
            | DataType::Path
            | DataType::Polygon
            | DataType::Circle => ArrowDataType::Utf8,
            // Enum stored as text
            DataType::Enum { .. } => ArrowDataType::Utf8,
            // Full text search types stored as text
            DataType::TsVector | DataType::TsQuery => ArrowDataType::Utf8,
        }
    }

    pub fn from_arrow(dt: &ArrowDataType) -> Self {
        match dt {
            ArrowDataType::Boolean => DataType::Boolean,
            ArrowDataType::Int8 => DataType::Int8,
            ArrowDataType::Int16 => DataType::Int16,
            ArrowDataType::Int32 => DataType::Int32,
            ArrowDataType::Int64 => DataType::Int64,
            ArrowDataType::UInt8 => DataType::UInt8,
            ArrowDataType::UInt16 => DataType::UInt16,
            ArrowDataType::UInt32 => DataType::UInt32,
            ArrowDataType::UInt64 => DataType::UInt64,
            ArrowDataType::Float32 => DataType::Float32,
            ArrowDataType::Float64 => DataType::Float64,
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => DataType::Utf8,
            ArrowDataType::Binary => DataType::Binary,
            ArrowDataType::LargeBinary => DataType::LargeBinary,
            ArrowDataType::Date32 => DataType::Date32,
            ArrowDataType::Date64 => DataType::Date64,
            ArrowDataType::Timestamp(_, _) => DataType::Timestamp,
            // New types
            ArrowDataType::FixedSizeBinary(16) => DataType::Uuid,
            ArrowDataType::Decimal128(p, s) => DataType::Decimal128 {
                precision: *p,
                scale: *s,
            },
            ArrowDataType::Interval(_) => DataType::Interval,
            ArrowDataType::List(field) => {
                DataType::List(Box::new(DataType::from_arrow(field.data_type())))
            }
            _ => DataType::Null,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl ColumnInfo {
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
        }
    }
}
