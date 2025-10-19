use arrow::record_batch::RecordBatch;
use query_core::{Result, Schema};
use query_executor::physical_plan::DataSource;

#[derive(Debug)]
pub struct MemoryDataSource {
    schema: Schema,
    batches: Vec<RecordBatch>,
}

impl MemoryDataSource {
    pub fn new(schema: Schema, batches: Vec<RecordBatch>) -> Self {
        Self { schema, batches }
    }
}

impl DataSource for MemoryDataSource {
    fn scan(&self) -> Result<Vec<RecordBatch>> {
        Ok(self.batches.clone())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
