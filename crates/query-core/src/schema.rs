use crate::error::{QueryError, Result};
use crate::types::DataType;
use arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    name: String,
    data_type: DataType,
    nullable: bool,
}

impl Field {
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn nullable(&self) -> bool {
        self.nullable
    }

    pub fn to_arrow(&self) -> ArrowField {
        ArrowField::new(self.name.clone(), self.data_type.to_arrow(), self.nullable)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    fields: Vec<Field>,
}

impl Schema {
    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    pub fn empty() -> Self {
        Self { fields: vec![] }
    }

    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    pub fn field(&self, index: usize) -> Option<&Field> {
        self.fields.get(index)
    }

    pub fn index_of(&self, name: &str) -> Result<usize> {
        self.fields
            .iter()
            .position(|f| f.name() == name)
            .ok_or_else(|| QueryError::ColumnNotFound(name.to_string()))
    }

    pub fn field_with_name(&self, name: &str) -> Result<&Field> {
        self.fields
            .iter()
            .find(|f| f.name() == name)
            .ok_or_else(|| QueryError::ColumnNotFound(name.to_string()))
    }

    pub fn to_arrow(&self) -> ArrowSchema {
        let fields: Vec<ArrowField> = self.fields.iter().map(|f| f.to_arrow()).collect();
        ArrowSchema::new(fields)
    }

    pub fn from_arrow(schema: &ArrowSchema) -> Self {
        let fields = schema
            .fields()
            .iter()
            .map(|f| {
                Field::new(
                    f.name(),
                    DataType::from_arrow(f.data_type()),
                    f.is_nullable(),
                )
            })
            .collect();
        Self { fields }
    }
}
