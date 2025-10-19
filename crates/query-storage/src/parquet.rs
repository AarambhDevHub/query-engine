use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use query_core::{Result, Schema};
use query_executor::physical_plan::DataSource;
use std::fs::File;

#[derive(Debug)]
pub struct ParquetDataSource {
    schema: Schema,
    path: String,
}

impl ParquetDataSource {
    pub fn new(path: impl Into<String>, schema: Schema) -> Self {
        Self {
            path: path.into(),
            schema,
        }
    }
}

impl DataSource for ParquetDataSource {
    fn scan(&self) -> Result<Vec<RecordBatch>> {
        let file = File::open(&self.path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;

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
