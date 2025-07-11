// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable slaw or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use futures::stream::StreamExt;
use iceberg::expr::{Predicate, PredicateOperator, Reference, UnaryExpression};
use iceberg::io::FileIOBuilder;
use iceberg::spec::{DataFile, NestedField, PrimitiveType, Schema, SchemaRef, Type};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::base_writer::equality_delete_writer::{
    EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, TableCreation, TableIdent};
use iceberg_catalog_memory::MemoryCatalog;
use parquet::file::properties::WriterProperties;
use tracing_subscriber;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().json().init();

    let file_io = FileIOBuilder::new("memory").build().unwrap();
    let catalog = MemoryCatalog::new(file_io, None);

    let table_ident =
        TableIdent::from_strs(vec!["my-namespace".to_string(), "test".to_string()]).unwrap();
    let table = create_table(&catalog, &table_ident).await;

    let schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );

    let data_files = write_optimistically(&table, &schema).await;
    let table = commit_append(&catalog, &table, data_files).await;

    let delete_files = delete_by_equality(&table).await;
    let table = commit_delete(&catalog, &table, delete_files).await;

    let scan = table
        .scan()
        .select_all()
        .with_filter(Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNull,
            Reference::new("col1"),
        )))
        .build()
        .unwrap();
    let reader = scan.to_arrow().await.unwrap();
    let buf = reader.collect::<Vec<_>>().await;

    println!("Table has {} batches.", buf.len());
}

async fn create_table(catalog: &MemoryCatalog, table_ident: &TableIdent) -> Table {
    catalog
        .create_namespace(table_ident.namespace(), HashMap::new())
        .await
        .unwrap();

    let table_creation = TableCreation::builder()
        .name(table_ident.name().to_string())
        .location("memory://tmp".to_string())
        .schema(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "col1", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(2, "col2", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(3, "col3", Type::Primitive(PrimitiveType::Boolean))
                        .into(),
                ])
                .build()
                .unwrap(),
        )
        .build();

    catalog
        .create_table(table_ident.namespace(), table_creation)
        .await
        .unwrap()
}

async fn write_optimistically(table: &Table, schema: &Arc<arrow_schema::Schema>) -> Vec<DataFile> {
    let parquet_writer_builder = new_parquet_writer_builder(table, None);

    let mut data_file_writer = DataFileWriterBuilder::new(parquet_writer_builder, None, 0)
        .build()
        .await
        .unwrap();
    let col1 = StringArray::from(vec![Some("foo"), Some("bar"), None, Some("baz")]);
    let col2 = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
    let col3 = BooleanArray::from(vec![Some(true), Some(false), None, Some(false)]);
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(col1) as ArrayRef,
        Arc::new(col2) as ArrayRef,
        Arc::new(col3) as ArrayRef,
    ])
    .unwrap();

    data_file_writer.write(batch.clone()).await.unwrap();

    data_file_writer.close().await.unwrap()
}

async fn delete_by_equality(table: &Table) -> Vec<DataFile> {
    let boolean_column = table.metadata().current_schema().field_by_id(3).unwrap();
    let partial_schema = Arc::new(
        Schema::builder()
            .with_fields(vec![Arc::clone(boolean_column)])
            .build()
            .unwrap(),
    );

    let parquet_writer_builder = new_parquet_writer_builder(table, Some(&partial_schema));

    let config =
        EqualityDeleteWriterConfig::new(vec![3], Arc::clone(&partial_schema), None, 0).unwrap();

    let mut equality_delete_writer =
        EqualityDeleteFileWriterBuilder::new(parquet_writer_builder, config)
            .build()
            .await
            .unwrap();

    let partial_arrow_schema: arrow_schema::Schema = partial_schema.as_ref().try_into().unwrap();

    let delete_column = Arc::new(BooleanArray::from(vec![false])) as ArrayRef;
    let to_write =
        RecordBatch::try_new(Arc::new(partial_arrow_schema), vec![delete_column]).unwrap();

    equality_delete_writer.write(to_write).await.unwrap();
    equality_delete_writer.close().await.unwrap()
}

async fn commit_append(catalog: &MemoryCatalog, table: &Table, data_files: Vec<DataFile>) -> Table {
    let tx = Transaction::new(&table);
    tx.fast_append()
        .add_data_files(data_files)
        .apply(tx)
        .unwrap()
        .commit(catalog)
        .await
        .unwrap()
}

async fn commit_delete(
    catalog: &MemoryCatalog,
    table: &Table,
    _delete_files: Vec<DataFile>,
) -> Table {
    let tx = Transaction::new(&table);
    // TODO: Delete not yet implemented on Transaction

    tx.commit(catalog).await.unwrap()
}

fn new_parquet_writer_builder(
    table: &Table,
    schema: Option<&SchemaRef>,
) -> ParquetWriterBuilder<DefaultLocationGenerator, DefaultFileNameGenerator> {
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        "test".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );

    let schema = schema.unwrap_or(table.metadata().current_schema());

    ParquetWriterBuilder::new(
        WriterProperties::default(),
        Arc::clone(&schema),
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    )
}
