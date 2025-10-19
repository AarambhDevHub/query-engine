use arrow::csv::ReaderBuilder;
use arrow::record_batch::RecordBatch;
use query_core::{Result, Schema};
use query_executor::physical_plan::DataSource;
use std::fs::File;
use std::sync::Arc;

#[derive(Debug)]
pub struct CsvDataSource {
    schema: Schema,
    path: String,
}

impl CsvDataSource {
    pub fn new(path: impl Into<String>, schema: Schema) -> Self {
        Self {
            path: path.into(),
            schema,
        }
    }
}

impl DataSource for CsvDataSource {
    fn scan(&self) -> Result<Vec<RecordBatch>> {
        let file = File::open(&self.path)?;
        let arrow_schema = Arc::new(self.schema.to_arrow());

        let reader = ReaderBuilder::new(arrow_schema)
            .with_header(true)
            .build(file)?;

        let batches: Result<Vec<_>> = reader
            .into_iter()
            .map(|batch| batch.map_err(|e| e.into()))
            .collect();

        batches
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
