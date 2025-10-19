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
    Date32,
    Date64,
    Timestamp,
    Null,
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
            DataType::Date32 => ArrowDataType::Date32,
            DataType::Date64 => ArrowDataType::Date64,
            DataType::Timestamp => {
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
            }
            DataType::Null => ArrowDataType::Null,
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
            ArrowDataType::Binary | ArrowDataType::LargeBinary => DataType::Binary,
            ArrowDataType::Date32 => DataType::Date32,
            ArrowDataType::Date64 => DataType::Date64,
            ArrowDataType::Timestamp(_, _) => DataType::Timestamp,
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
