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
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use common::{random_ns, test_schema};
use futures::{StreamExt, TryStreamExt};
use iceberg::cow_rewrite::{CowBatchRewrite, CowBatchRewriter, CowRewriteBuilder};
use iceberg::expr::Reference;
use iceberg::metrics::{MetricsReport, MetricsReporter};
use iceberg::spec::{DataFileFormat, Datum};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, TableCreation};
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder};
use iceberg_integration_tests::get_test_fixture;
use iceberg_storage_opendal::OpenDalStorageFactory;
use serde_json::{Value, json};
use tokio::sync::{Notify, mpsc};

#[derive(Debug)]
struct RecordingReporter {
    tx: mpsc::UnboundedSender<(Value, Duration)>,
    gate: Option<(Arc<Notify>, Arc<Notify>)>,
    panic_after_delivery: bool,
}

#[async_trait::async_trait]
impl MetricsReporter for RecordingReporter {
    async fn report(&self, report: &MetricsReport) {
        let MetricsReport::Scan(report) = report else {
            panic!("expected scan report")
        };
        if let Some((started, released)) = &self.gate {
            started.notify_one();
            released.notified().await;
        }
        self.tx
            .send((
                json!({
                    "table": report.table_ident().to_string(),
                    "snapshot": report.snapshot_id(),
                    "schema": report.schema_id(),
                    "filter": report.filter().map(ToString::to_string),
                    "ids": report.projected_field_ids(),
                    "names": report.projected_field_names(),
                    "files": report.metrics().result_data_files(),
                    "bytes": report.metrics().total_file_size_in_bytes(),
                    "metadata": report.metadata(),
                }),
                report.metrics().total_planning_duration(),
            ))
            .unwrap();
        assert!(!self.panic_after_delivery, "injected reporter panic");
    }
}

type Reports = mpsc::UnboundedReceiver<(Value, Duration)>;

fn recording_reporter() -> (RecordingReporter, Reports) {
    let (tx, rx) = mpsc::unbounded_channel();
    (
        RecordingReporter {
            tx,
            gate: None,
            panic_after_delivery: false,
        },
        rx,
    )
}

async fn received(rx: &mut Reports) -> (Value, Duration) {
    tokio::time::timeout(Duration::from_secs(20), rx.recv())
        .await
        .unwrap()
        .unwrap()
}

async fn closed(mut rx: Reports) {
    assert!(
        tokio::time::timeout(Duration::from_secs(20), rx.recv())
            .await
            .unwrap()
            .is_none()
    );
}

async fn catalog() -> RestCatalog {
    RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .load("rest", get_test_fixture().catalog_config.clone())
        .await
        .unwrap()
}

async fn populated_table(catalog: &RestCatalog) -> Table {
    let namespace = random_ns().await;
    let table = catalog
        .create_table(
            namespace.name(),
            TableCreation::builder()
                .name("reports".into())
                .schema(test_schema())
                .build(),
        )
        .await
        .unwrap();
    let schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    let mut files = Vec::new();
    for values in [[1, 2], [3, 4]] {
        let writer = RollingFileWriterBuilder::new_with_default_file_size(
            ParquetWriterBuilder::new(Default::default(), table.current_schema_ref()),
            table.file_io().clone(),
            DefaultLocationGenerator::new(table.metadata()).unwrap(),
            DefaultFileNameGenerator::new(
                uuid::Uuid::new_v4().to_string(),
                None,
                DataFileFormat::Parquet,
            ),
        );
        let mut writer = DataFileWriterBuilder::new(writer)
            .build(None)
            .await
            .unwrap();
        let batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
            Arc::new(Int32Array::from(values.to_vec())) as ArrayRef,
            Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef,
        ])
        .unwrap();
        writer.write(batch).await.unwrap();
        files.extend(writer.close().await.unwrap());
    }
    let tx = Transaction::new(&table);
    let tx = tx.fast_append().add_data_files(files).apply(tx).unwrap();
    let table = tx.commit(catalog).await.unwrap();
    catalog.load_table(table.identifier()).await.unwrap()
}

fn expected(table: &Table, filter: Option<&str>, files: usize, bytes: u64) -> Value {
    json!({
        "table": table.identifier().to_string(),
        "snapshot": table.metadata().current_snapshot().unwrap().snapshot_id(),
        "schema": table.current_schema_ref().schema_id(),
        "filter": filter,
        "ids": [2], "names": ["bar"], "files": files, "bytes": bytes, "metadata": {},
    })
}

async fn rows(table: &Table) -> Vec<i32> {
    let mut batches = table
        .scan()
        .select(["bar"])
        .build()
        .unwrap()
        .to_arrow(Default::default())
        .await
        .unwrap();
    let mut rows = Vec::new();
    while let Some(batch) = batches.try_next().await.unwrap() {
        rows.extend(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
        );
    }
    rows.sort();
    rows
}

struct KeepAll;

impl CowBatchRewriter for KeepAll {
    fn rewrite_batch(&self, batch: RecordBatch) -> iceberg::Result<CowBatchRewrite> {
        Ok(CowBatchRewrite {
            output: Some(batch),
            changed: false,
        })
    }
}

#[tokio::test]
async fn reports_preserve_rest_arrow_and_cow_results() {
    let catalog = catalog().await;
    let table = populated_table(&catalog).await;
    let baseline = rows(&table).await;
    assert_eq!(baseline, vec![1, 2, 3, 4]);
    let tasks: Vec<_> = table
        .scan()
        .build()
        .unwrap()
        .plan_files()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let bytes = tasks.iter().map(|task| task.file_size_in_bytes()).sum();
    let filtered_tasks: Vec<_> = table
        .scan()
        .with_filter(Reference::new("bar").less_than(Datum::int(3)))
        .build()
        .unwrap()
        .plan_files()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let filtered_bytes = filtered_tasks
        .iter()
        .map(|task| task.file_size_in_bytes())
        .sum();
    let (reporter, mut reports) = recording_reporter();
    let table = table.with_metrics_reporter(Arc::new(reporter));
    assert_eq!(rows(&table).await, baseline);
    assert_eq!(
        received(&mut reports).await.0,
        expected(&table, None, 2, bytes)
    );

    let (override_reporter, mut override_reports) = recording_reporter();
    let scan = table
        .scan()
        .select(["bar"])
        .with_filter(Reference::new("bar").less_than(Datum::int(3)))
        .with_metrics_reporter(Arc::new(override_reporter))
        .build()
        .unwrap();
    let batches: Vec<_> = scan
        .to_arrow(Default::default())
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let values: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(values, vec![1, 2]);
    assert_eq!(
        received(&mut override_reports).await.0,
        expected(&table, Some("bar < 3"), 1, filtered_bytes)
    );
    drop(scan);
    closed(override_reports).await;

    let scan = table.scan().select(["bar"]).build().unwrap();
    let (left, right) = tokio::join!(scan.plan_files(), scan.plan_files());
    let (left, right) = tokio::join!(
        left.unwrap().try_collect::<Vec<_>>(),
        right.unwrap().try_collect::<Vec<_>>()
    );
    assert_eq!((left.unwrap().len(), right.unwrap().len()), (2, 2));
    for _ in 0..2 {
        assert_eq!(
            received(&mut reports).await.0,
            expected(&table, None, 2, bytes)
        );
    }
    drop(scan);

    let rewrite = CowRewriteBuilder::new(&table)
        .with_rewriter(Arc::new(KeepAll))
        .rewrite()
        .await
        .unwrap();
    assert_eq!(
        (
            rewrite.has_changes(),
            rewrite.stats.candidate_files,
            rewrite.stats.input_rows
        ),
        (false, 2, 4)
    );
    let mut cow_expected = expected(&table, Some("TRUE"), 2, bytes);
    cow_expected["ids"] = json!([1, 2, 3]);
    cow_expected["names"] = json!(["foo", "bar", "baz"]);
    assert_eq!(received(&mut reports).await.0, cow_expected);
    let ident = table.identifier().clone();
    drop(table);
    closed(reports).await;
    catalog.drop_table(&ident).await.unwrap();
    catalog.drop_namespace(ident.namespace()).await.unwrap();
}

#[tokio::test]
async fn reports_require_planning_eof_and_exclude_delivery_time() {
    let catalog = catalog().await;
    let table = populated_table(&catalog).await;
    for count in [0, 1, 2] {
        let (reporter, reports) = recording_reporter();
        let scan = table
            .scan()
            .with_metrics_reporter(Arc::new(reporter))
            .build()
            .unwrap();
        let mut tasks = scan.plan_files().await.unwrap();
        for _ in 0..count {
            tasks.next().await.unwrap().unwrap();
        }
        drop(tasks);
        drop(scan);
        closed(reports).await;
    }
    let started = Arc::new(Notify::new());
    let released = Arc::new(Notify::new());
    let (mut reporter, mut reports) = recording_reporter();
    reporter.gate = Some((started.clone(), released.clone()));
    reporter.panic_after_delivery = true;
    let scan = table
        .scan()
        .with_metrics_reporter(Arc::new(reporter))
        .build()
        .unwrap();
    let planning_started = Instant::now();
    let mut tasks = scan.plan_files().await.unwrap();
    tasks.next().await.unwrap().unwrap();
    let paused = Instant::now();
    tokio::time::sleep(Duration::from_millis(30)).await;
    let pause = paused.elapsed();
    let remaining: Vec<_> = tokio::time::timeout(Duration::from_secs(20), tasks.try_collect())
        .await
        .unwrap()
        .unwrap();
    let planning_elapsed = planning_started.elapsed();
    assert_eq!(remaining.len(), 1);
    tokio::time::timeout(Duration::from_secs(20), started.notified())
        .await
        .unwrap();
    assert!(reports.try_recv().is_err());
    tokio::time::sleep(Duration::from_millis(30)).await;
    released.notify_one();
    let (_, duration) = received(&mut reports).await;
    assert!(duration >= pause);
    assert!(duration <= planning_elapsed);
    drop(scan);
    closed(reports).await;
    assert_eq!(rows(&table).await, vec![1, 2, 3, 4]);
    let ident = table.identifier().clone();
    catalog.drop_table(&ident).await.unwrap();
    catalog.drop_namespace(ident.namespace()).await.unwrap();
}

#[tokio::test]
async fn manifest_failures_and_snapshotless_tables_do_not_report() {
    let catalog = catalog().await;
    let namespace = random_ns().await;
    let table = catalog
        .create_table(
            namespace.name(),
            TableCreation::builder()
                .name("empty".into())
                .schema(test_schema())
                .build(),
        )
        .await
        .unwrap();
    let (reporter, reports) = recording_reporter();
    let scan = table
        .scan()
        .with_metrics_reporter(Arc::new(reporter))
        .build()
        .unwrap();
    assert!(scan.plan_files().await.unwrap().next().await.is_none());
    drop(scan);
    closed(reports).await;
    catalog.drop_table(table.identifier()).await.unwrap();
    catalog.drop_namespace(namespace.name()).await.unwrap();

    let table = populated_table(&catalog).await;
    let snapshot = table.metadata().current_snapshot().unwrap();
    let list = table.manifest_list_reader(snapshot).load().await.unwrap();
    table
        .file_io()
        .delete(&list.entries()[0].manifest_path)
        .await
        .unwrap();
    let (reporter, reports) = recording_reporter();
    let scan = table
        .scan()
        .with_metrics_reporter(Arc::new(reporter))
        .build()
        .unwrap();
    let result = scan
        .plan_files()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await;
    assert!(result.is_err());
    drop(scan);
    closed(reports).await;
    table
        .file_io()
        .delete(snapshot.manifest_list())
        .await
        .unwrap();
    let table = catalog.load_table(table.identifier()).await.unwrap();
    let (reporter, reports) = recording_reporter();
    let scan = table
        .scan()
        .with_metrics_reporter(Arc::new(reporter))
        .build()
        .unwrap();
    assert!(scan.plan_files().await.is_err());
    drop(scan);
    closed(reports).await;
    catalog.drop_table(table.identifier()).await.unwrap();
    catalog
        .drop_namespace(table.identifier().namespace())
        .await
        .unwrap();
}
