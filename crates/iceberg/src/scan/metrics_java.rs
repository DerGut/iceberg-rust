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

use std::fs::{self, File};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use futures::{StreamExt, TryStreamExt};
use parquet::arrow::ArrowWriter;
use serde_json::json;

use super::metrics::PlanningCounters;
use crate::expr::Reference;
use crate::metadata_columns::{
    RESERVED_COL_NAME_FILE, RESERVED_COL_NAME_POS, RESERVED_COL_NAME_SPEC_ID,
};
use crate::metrics::{MetricsReport, MetricsReporter};
use crate::spec::{
    DataContentType, Datum, Literal, ManifestEntry, ManifestListWriter, ManifestStatus,
    ManifestWriterBuilder, NestedField, PrimitiveType, Schema, Struct,
};
use crate::table::Table;
use crate::test_utils::scan::TableTestFixture;
use crate::test_utils::test_runtime;

#[derive(Debug)]
struct RecordingReporter(tokio::sync::mpsc::UnboundedSender<serde_json::Value>);

#[async_trait::async_trait]
impl MetricsReporter for RecordingReporter {
    async fn report(&self, report: &MetricsReport) {
        let MetricsReport::Scan(report) = report;
        let metrics = report.metrics();
        self.0
            .send(json!({
                "table-name": report.table_ident().to_string(),
                "snapshot-id": report.snapshot_id(),
                "schema-id": report.schema_id(),
                "filter": report.filter().map(ToString::to_string),
                "projected-field-ids": report.projected_field_ids(),
                "projected-field-names": report.projected_field_names(),
                "metadata": report.metadata(),
                "duration-ns": metrics.total_planning_duration().as_nanos(),
                "metrics": {
                    "total_data_manifests": metrics.total_data_manifests(),
                    "total_delete_manifests": metrics.total_delete_manifests(),
                    "skipped_data_manifests": metrics.skipped_data_manifests(),
                    "skipped_delete_manifests": metrics.skipped_delete_manifests(),
                    "scanned_data_manifests": metrics.scanned_data_manifests(),
                    "scanned_delete_manifests": metrics.scanned_delete_manifests(),
                    "result_data_files": metrics.result_data_files(),
                    "skipped_data_files": metrics.skipped_data_files(),
                    "total_file_size_in_bytes": metrics.total_file_size_in_bytes(),
                    "result_delete_files": metrics.result_delete_files(),
                    "skipped_delete_files": metrics.skipped_delete_files(),
                    "total_delete_file_size_in_bytes": metrics.total_delete_file_size_in_bytes(),
                    "indexed_delete_files": metrics.indexed_delete_files(),
                    "equality_delete_files": metrics.equality_delete_files(),
                    "positional_delete_files": metrics.positional_delete_files(),
                    "dvs": metrics.dvs(),
                },
            }))
            .unwrap();
    }
}

async fn add_position_delete(fixture: &TableTestFixture, referenced: &str, partition: i64) {
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, ManifestEntry,
        ManifestListWriter, ManifestStatus, ManifestWriterBuilder, Struct,
    };
    let snapshot = fixture.table.metadata().current_snapshot().unwrap();
    let schema = fixture.table.metadata().current_schema().clone();
    let builder = ManifestWriterBuilder::new(
        fixture.next_manifest_file(),
        Some(snapshot.snapshot_id()),
        schema,
        fixture
            .table
            .metadata()
            .default_partition_spec()
            .as_ref()
            .clone(),
    );
    let mut writer = if fixture.table.metadata().format_version() == crate::spec::FormatVersion::V3
    {
        builder.build_v3_deletes()
    } else {
        builder.build_v2_deletes()
    };
    writer
        .add_entry(
            ManifestEntry::builder()
                .status(ManifestStatus::Added)
                .data_file(
                    DataFileBuilder::default()
                        .partition_spec_id(0)
                        .content(DataContentType::PositionDeletes)
                        .file_path(format!("{}/unmatched.parquet", fixture.table_location))
                        .file_format(DataFileFormat::Parquet)
                        .file_size_in_bytes(100)
                        .record_count(1)
                        .referenced_data_file(Some(format!(
                            "{}/{}",
                            fixture.table_location, referenced
                        )))
                        .partition(Struct::from_iter([Some(Literal::long(partition))]))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .unwrap();
    let delete = writer.write_manifest_file().await.unwrap();
    let scan = fixture.table.scan().build().unwrap();
    let list = scan
        .plan_context
        .as_ref()
        .unwrap()
        .get_manifest_list()
        .await
        .unwrap();
    let mut manifests = list.entries().to_vec();
    manifests.push(delete);
    let output = fixture
        .table
        .file_io()
        .new_output(snapshot.manifest_list())
        .unwrap()
        .writer()
        .await
        .unwrap();
    let mut writer = if fixture.table.metadata().format_version() == crate::spec::FormatVersion::V3
    {
        ManifestListWriter::v3(
            output,
            snapshot.snapshot_id(),
            snapshot.parent_snapshot_id(),
            snapshot.sequence_number(),
            Some(42),
        )
    } else {
        ManifestListWriter::v2(
            output,
            snapshot.snapshot_id(),
            snapshot.parent_snapshot_id(),
            snapshot.sequence_number(),
        )
    };
    writer.add_manifests(manifests.into_iter()).unwrap();
    writer.close().await.unwrap();
}

async fn materialize_fixture(fixture: &TableTestFixture, dead_only: bool) {
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    };
    use arrow_schema::DataType;

    use crate::spec::{
        DataContentType, DataFileFormat, FormatVersion, ManifestContentType, ManifestListWriter,
        ManifestStatus, ManifestWriterBuilder, Schema,
    };
    let scan = fixture.table.scan().build().unwrap();
    let context = scan.plan_context.as_ref().unwrap();
    let list = context.get_manifest_list().await.unwrap();
    let snapshot = &context.snapshot;
    let mut manifests = vec![];
    for file in list.entries() {
        let manifest = context.object_cache.get_manifest(file).await.unwrap();
        let schema = manifest.metadata().schema();
        let builder = ManifestWriterBuilder::new(
            fixture.next_manifest_file(),
            Some(snapshot.snapshot_id()),
            schema.clone(),
            fixture
                .table
                .metadata()
                .partition_spec_by_id(file.partition_spec_id)
                .unwrap()
                .as_ref()
                .clone(),
        );
        let mut writer = match (fixture.table.metadata().format_version(), file.content) {
            (FormatVersion::V3, ManifestContentType::Data) => builder.build_v3_data(),
            (FormatVersion::V3, ManifestContentType::Deletes) => builder.build_v3_deletes(),
            (_, ManifestContentType::Data) => builder.build_v2_data(),
            (_, ManifestContentType::Deletes) => builder.build_v2_deletes(),
        };
        for entry in manifest.entries() {
            if dead_only && entry.is_alive() {
                continue;
            }
            let mut entry = entry.as_ref().clone();
            let data = &mut entry.data_file;
            if !std::path::Path::new(data.file_path()).exists() {
                if data.file_format() == DataFileFormat::Puffin {
                    use crate::puffin::{Blob, CompressionCodec, DELETION_VECTOR_V1, PuffinWriter};
                    let blob = crate::test_utils::encode_dv_blob([0]);
                    data.content_size_in_bytes = Some(blob.len() as i64);
                    data.content_offset = Some(4);
                    let output = fixture
                        .table
                        .file_io()
                        .new_output(data.file_path())
                        .unwrap();
                    let mut puffin = PuffinWriter::new(&output, Default::default(), false)
                        .await
                        .unwrap();
                    puffin
                        .add(
                            Blob::builder()
                                .r#type(DELETION_VECTOR_V1.into())
                                .fields(vec![])
                                .snapshot_id(snapshot.snapshot_id())
                                .sequence_number(snapshot.sequence_number())
                                .data(blob)
                                .properties(Default::default())
                                .build(),
                            CompressionCodec::None,
                        )
                        .await
                        .unwrap();
                    puffin.close().await.unwrap();
                } else {
                    let file_schema = match data.content_type() {
                        DataContentType::Data => schema.as_ref().clone(),
                        DataContentType::EqualityDeletes => Schema::builder()
                            .with_fields(
                                data.equality_ids()
                                    .unwrap()
                                    .iter()
                                    .map(|id| schema.field_by_id(*id).unwrap().clone()),
                            )
                            .build()
                            .unwrap(),
                        DataContentType::PositionDeletes => Schema::builder()
                            .with_fields([
                                crate::metadata_columns::get_metadata_field(
                                    crate::metadata_columns::RESERVED_FIELD_ID_DELETE_FILE_PATH,
                                )
                                .unwrap()
                                .clone(),
                                crate::metadata_columns::get_metadata_field(
                                    crate::metadata_columns::RESERVED_FIELD_ID_DELETE_FILE_POS,
                                )
                                .unwrap()
                                .clone(),
                            ])
                            .build()
                            .unwrap(),
                    };
                    let arrow_schema =
                        Arc::new(crate::arrow::schema_to_arrow_schema(&file_schema).unwrap());
                    let x = if data.file_path().ends_with("x7.parquet") {
                        7
                    } else {
                        100
                    };
                    let arrays: Vec<ArrayRef> = arrow_schema
                        .fields()
                        .iter()
                        .map(|field| -> ArrayRef {
                            match field.data_type() {
                                DataType::Int64 => {
                                    Arc::new(Int64Array::from(vec![if field.name() == "x" {
                                        x
                                    } else {
                                        0
                                    }]))
                                }
                                DataType::Int32 => Arc::new(Int32Array::from(vec![x as i32])),
                                DataType::Float64 => Arc::new(Float64Array::from(vec![0.0])),
                                DataType::Boolean => Arc::new(BooleanArray::from(vec![false])),
                                DataType::Utf8 => Arc::new(StringArray::from(vec![
                                    data.referenced_data_file().unwrap_or_else(|| {
                                        format!("{}/0.parquet", fixture.table_location)
                                    }),
                                ])),
                                ty => panic!("unsupported fixture type {ty}"),
                            }
                        })
                        .collect();
                    let batch = RecordBatch::try_new(arrow_schema.clone(), arrays).unwrap();
                    let mut parquet = ArrowWriter::try_new(
                        File::create(data.file_path()).unwrap(),
                        arrow_schema,
                        None,
                    )
                    .unwrap();
                    parquet.write(&batch).unwrap();
                    parquet.close().unwrap();
                }
            }
            data.file_size_in_bytes = fs::metadata(data.file_path()).unwrap().len();
            match entry.status() {
                ManifestStatus::Added => writer.add_entry(entry).unwrap(),
                ManifestStatus::Existing => writer.add_existing_entry(entry).unwrap(),
                ManifestStatus::Deleted => writer.add_delete_entry(entry).unwrap(),
            }
        }
        manifests.push(writer.write_manifest_file().await.unwrap());
    }
    let output = fixture
        .table
        .file_io()
        .new_output(snapshot.manifest_list())
        .unwrap()
        .writer()
        .await
        .unwrap();
    let mut writer = if fixture.table.metadata().format_version() == FormatVersion::V3 {
        ManifestListWriter::v3(
            output,
            snapshot.snapshot_id(),
            snapshot.parent_snapshot_id(),
            snapshot.sequence_number(),
            Some(42),
        )
    } else {
        ManifestListWriter::v2(
            output,
            snapshot.snapshot_id(),
            snapshot.parent_snapshot_id(),
            snapshot.sequence_number(),
        )
    };
    writer.add_manifests(manifests.into_iter()).unwrap();
    writer.close().await.unwrap();
}

async fn writer_statistics_fixture(fixture: &mut TableTestFixture, nested: bool, deletes: bool) {
    use crate::writer::file_writer::{FileWriter, FileWriterBuilder, ParquetWriterBuilder};
    let schema = if nested {
        fixture.table.metadata().current_schema().clone()
    } else {
        Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "x", PrimitiveType::Long).into(),
                    NestedField::required(2, "y", PrimitiveType::Long).into(),
                    NestedField::optional(3, "z", PrimitiveType::Long).into(),
                ])
                .build()
                .unwrap(),
        )
    };
    let mut metadata = fixture.table.metadata().clone();
    metadata.schemas.insert(1, schema.clone());
    metadata.last_column_id = schema.highest_field_id();
    fixture.table = fixture.table.clone().with_metadata(Arc::new(metadata));
    let snapshot = fixture.table.metadata().current_snapshot().unwrap();
    let parent = snapshot.parent_snapshot(fixture.table.metadata()).unwrap();
    let spec = fixture.table.metadata().default_partition_spec();
    let arrow_schema = Arc::new(crate::arrow::schema_to_arrow_schema(&schema).unwrap());
    let mut writer = ManifestWriterBuilder::new(
        fixture.next_manifest_file(),
        Some(snapshot.snapshot_id()),
        schema.clone(),
        spec.as_ref().clone(),
    )
    .build_v2_data();
    for value in [10, 30, 50] {
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![7, 7])),
            Arc::new(Int64Array::from(vec![value, value + 1])),
            Arc::new(Int64Array::from(vec![0, 1])),
        ];
        if nested {
            let arrow_schema::DataType::Struct(fields) = arrow_schema.field(3).data_type() else {
                unreachable!()
            };
            columns.push(Arc::new(arrow_array::StructArray::new(
                fields.clone(),
                vec![Arc::new(Int64Array::from(vec![value, value + 1]))],
                None,
            )));
            for field in arrow_schema.fields().iter().skip(4) {
                columns.push(arrow_array::new_null_array(field.data_type(), 2));
            }
        }
        let batch = RecordBatch::try_new(arrow_schema.clone(), columns).unwrap();
        let mut parquet = ParquetWriterBuilder::new(Default::default(), schema.clone())
            .build(
                fixture
                    .table
                    .file_io()
                    .new_output(format!(
                        "{}/written-{value}.parquet",
                        fixture.table_location
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        parquet.write(&batch).await.unwrap();
        let file = parquet
            .close()
            .await
            .unwrap()
            .pop()
            .unwrap()
            .content(DataContentType::Data)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(7))]))
            .build()
            .unwrap();
        assert_eq!(file.record_count(), 2);
        assert_eq!(file.lower_bounds().get(&2), Some(&Datum::long(value)));
        let entry = ManifestEntry::builder()
            .status(if value == 30 {
                ManifestStatus::Deleted
            } else {
                ManifestStatus::Existing
            })
            .snapshot_id(if value == 30 {
                snapshot.snapshot_id()
            } else {
                parent.snapshot_id()
            })
            .sequence_number(parent.sequence_number())
            .file_sequence_number(parent.sequence_number())
            .data_file(file)
            .build();
        if value == 30 {
            writer.add_delete_entry(entry).unwrap();
        } else {
            writer.add_existing_entry(entry).unwrap();
        }
    }
    let mut manifests = vec![writer.write_manifest_file().await.unwrap()];
    if deletes {
        let delete_schema = Arc::new(
            Schema::builder()
                .with_fields([schema.field_by_id(2).unwrap().clone()])
                .build()
                .unwrap(),
        );
        let mut writer = ManifestWriterBuilder::new(
            fixture.next_manifest_file(),
            Some(snapshot.snapshot_id()),
            schema.clone(),
            spec.as_ref().clone(),
        )
        .build_v2_deletes();
        for value in [10, 99] {
            let arrow_schema =
                Arc::new(crate::arrow::schema_to_arrow_schema(&delete_schema).unwrap());
            let batch =
                RecordBatch::try_new(arrow_schema, vec![Arc::new(Int64Array::from(vec![value]))])
                    .unwrap();
            let mut parquet = ParquetWriterBuilder::new(Default::default(), delete_schema.clone())
                .build(
                    fixture
                        .table
                        .file_io()
                        .new_output(format!("{}/eq-{value}.parquet", fixture.table_location))
                        .unwrap(),
                )
                .await
                .unwrap();
            parquet.write(&batch).await.unwrap();
            let file = parquet
                .close()
                .await
                .unwrap()
                .pop()
                .unwrap()
                .content(DataContentType::EqualityDeletes)
                .equality_ids(Some(vec![2]))
                .partition_spec_id(0)
                .partition(Struct::from_iter([Some(Literal::long(7))]))
                .build()
                .unwrap();
            writer
                .add_entry(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Added)
                        .data_file(file)
                        .build(),
                )
                .unwrap();
        }
        manifests.push(writer.write_manifest_file().await.unwrap());
    }
    let output = fixture
        .table
        .file_io()
        .new_output(snapshot.manifest_list())
        .unwrap()
        .writer()
        .await
        .unwrap();
    let mut list = ManifestListWriter::v2(
        output,
        snapshot.snapshot_id(),
        snapshot.parent_snapshot_id(),
        snapshot.sequence_number(),
    );
    list.add_manifests(manifests.into_iter()).unwrap();
    list.close().await.unwrap();
}

async fn prepare_older_snapshot(fixture: &mut TableTestFixture) -> serde_json::Value {
    let scan = fixture.table.scan().build().unwrap();
    let context = scan.plan_context.as_ref().unwrap();
    let list = context.get_manifest_list().await.unwrap();
    let manifest = context
        .object_cache
        .get_manifest(&list.entries()[0])
        .await
        .unwrap();
    let mut data = manifest
        .entries()
        .iter()
        .find(|entry| entry.is_alive())
        .unwrap()
        .data_file()
        .clone();
    let older_path = format!("{}/older.parquet", fixture.table_location);
    fs::copy(data.file_path(), &older_path).unwrap();
    data.file_path = older_path.clone();
    let mut metadata = fixture.table.metadata().clone();
    let current = metadata.current_snapshot().unwrap().clone();
    let mut older = current.parent_snapshot(&metadata).unwrap().as_ref().clone();
    older.schema_id = Some(1);
    let older_schema = metadata.schema_by_id(1).unwrap().clone();
    let mut writer = ManifestWriterBuilder::new(
        fixture.next_manifest_file(),
        Some(older.snapshot_id()),
        older_schema,
        metadata.default_partition_spec().as_ref().clone(),
    )
    .build_v2_data();
    let older_size = data.file_size_in_bytes();
    writer
        .add_entry(
            ManifestEntry::builder()
                .status(ManifestStatus::Added)
                .data_file(data)
                .build(),
        )
        .unwrap();
    let manifest = writer.write_manifest_file().await.unwrap();
    let output = fixture
        .table
        .file_io()
        .new_output(older.manifest_list())
        .unwrap()
        .writer()
        .await
        .unwrap();
    let mut list = ManifestListWriter::v2(
        output,
        older.snapshot_id(),
        older.parent_snapshot_id(),
        older.sequence_number(),
    );
    list.add_manifests(std::iter::once(manifest)).unwrap();
    list.close().await.unwrap();
    let current_tasks: Vec<_> = scan
        .plan_files()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut current = current.as_ref().clone();
    current.schema_id = Some(metadata.current_schema().schema_id());
    let witness = json!({
        "older_path": older_path, "older_size": older_size,
        "older_snapshot_id": older.snapshot_id(), "current_snapshot_id": current.snapshot_id(),
        "current_schema_id": metadata.current_schema().schema_id(),
        "current_projected_field_ids": metadata.current_schema().as_struct().fields().iter().map(|field| field.id).collect::<Vec<_>>(),
        "current_paths": current_tasks.iter().map(|task| task.data_file_path()).collect::<Vec<_>>(),
    });
    metadata
        .snapshots
        .insert(older.snapshot_id(), Arc::new(older));
    metadata
        .snapshots
        .insert(current.snapshot_id(), Arc::new(current));
    fixture.table = fixture.table.clone().with_metadata(Arc::new(metadata));
    witness
}

#[tokio::test]
#[ignore = "exports shared fixtures for dev/scan-reports/run.sh"]
async fn test_export_java_differential_fixtures() {
    let output = std::path::PathBuf::from(std::env::var("SCAN_REPORT_OUTPUT").unwrap());
    fs::create_dir_all(&output).unwrap();
    let mut cases = vec![];
    for fixture_name in [
        "unfiltered",
        "dead-only",
        "manifest-pruned",
        "file-pruned",
        "all-pruned",
        "projection",
        "current-schema",
        "time-travel",
        "unmatched-delete",
        "shared-position",
        "path-position",
        "equality",
        "dv",
        "dv-supersession",
        "delete-manifest-pruned",
        "writer-unfiltered",
        "writer-file-pruned",
        "writer-all-pruned",
        "writer-equality-pruning",
        "writer-equality-association",
        "nested-all",
        "writer-projection-filter",
        "writer-empty-projection-filter",
        "nested-projection-filter",
        "nested-filter-only",
        "cow-writer-projection-filter",
        "cow-nested-projection-filter",
        "metadata-projection",
        "close-after-one",
        "close-after-last",
        "manifest-read-failure",
        "cow-writer-unfiltered",
        "cow-writer-file-pruned",
        "cow-writer-all-pruned",
        "cow-shared-position",
        "cow-dv",
        "cow-close-after-one",
        "cow-close-after-last",
        "cow-manifest-read-failure",
    ] {
        let cow = fixture_name.starts_with("cow-");
        let name = fixture_name.strip_prefix("cow-").unwrap_or(fixture_name);
        let mut fixture = if matches!(name, "current-schema" | "time-travel") {
            TableTestFixture::new_with_column_added_after_last_write()
        } else if name.starts_with("nested-") {
            TableTestFixture::new_with_nested_schema()
        } else {
            TableTestFixture::new()
        };
        match name {
            "shared-position" => fixture.setup_deadlock_manifests().await,
            "path-position" => fixture.setup_multi_row_group_manifest(&[1, 2]).await,
            "equality" => fixture.setup_promoted_partition_with_eq_delete(false).await,
            "dv" => fixture.setup_promoted_partition_with_dv().await,
            "dv-supersession" => {
                fixture.setup_promoted_partition_with_dv().await;
                add_position_delete(&fixture, "x7.parquet", 7).await;
            }
            "delete-manifest-pruned" => {
                fixture.setup_promoted_partition_with_eq_delete(false).await
            }
            name if name.starts_with("writer-") || name.starts_with("nested-") => {
                writer_statistics_fixture(
                    &mut fixture,
                    name.starts_with("nested-"),
                    matches!(
                        name,
                        "writer-equality-pruning" | "writer-equality-association"
                    ),
                )
                .await;
            }
            "unmatched-delete" => {
                fixture.setup_manifest_files().await;
                add_position_delete(&fixture, "missing.parquet", 100).await;
            }
            _ => fixture.setup_manifest_files().await,
        }
        fixture.table = Table::builder()
            .metadata(fixture.table.metadata().clone())
            .identifier(fixture.table.identifier().clone())
            .file_io(fixture.table.file_io().clone())
            .runtime(test_runtime())
            .build()
            .unwrap();
        materialize_fixture(&fixture, name == "dead-only").await;
        fixture.table = Table::builder()
            .metadata(fixture.table.metadata().clone())
            .identifier(fixture.table.identifier().clone())
            .file_io(fixture.table.file_io().clone())
            .runtime(test_runtime())
            .build()
            .unwrap();
        let time_travel = if name == "time-travel" {
            Some(prepare_older_snapshot(&mut fixture).await)
        } else {
            None
        };
        let requested_snapshot_id = time_travel
            .as_ref()
            .map(|witness| witness["older_snapshot_id"].as_i64().unwrap());
        let delete_files: Vec<_> = if matches!(
            name,
            "writer-equality-pruning" | "writer-equality-association"
        ) {
            [10, 99]
                .into_iter()
                .map(|value| {
                    let path = format!("{}/eq-{value}.parquet", fixture.table_location);
                    json!({"size": fs::metadata(&path).unwrap().len(), "path": path,
                    "content": 2, "content_offset": null, "content_size": null})
                })
                .collect()
        } else {
            Vec::new()
        };
        let threshold = match name {
            "manifest-pruned" => Some(50),
            "file-pruned" => Some(200),
            "delete-manifest-pruned" => Some(1),
            "writer-file-pruned" | "writer-equality-pruning" => Some(20),
            "writer-projection-filter"
            | "writer-empty-projection-filter"
            | "nested-projection-filter"
            | "nested-filter-only" => Some(20),
            "writer-all-pruned" => Some(5),
            _ => None,
        };
        let filter_column = if name == "nested-filter-only" {
            "details.value"
        } else if name.starts_with("writer-") || name == "nested-projection-filter" {
            "y"
        } else {
            "x"
        };
        let missing_manifest = if name == "manifest-read-failure" {
            let scan = fixture.table.scan().build().unwrap();
            let list = scan
                .plan_context
                .as_ref()
                .unwrap()
                .get_manifest_list()
                .await
                .unwrap();
            let path = list.entries()[0].manifest_path.clone();
            fixture.table.file_io().delete(&path).await.unwrap();
            Some(path)
        } else {
            None
        };
        let consume = match name {
            "close-after-one" => Some(1),
            "close-after-last" => Some(2),
            _ => None,
        };
        let (report_tx, mut report_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut builder = fixture
            .table
            .scan()
            .with_concurrency_limit(1)
            .with_metrics_reporter(Arc::new(RecordingReporter(report_tx)));
        if let Some(snapshot_id) = requested_snapshot_id {
            builder = builder.snapshot_id(snapshot_id);
        }
        if let Some(limit) = threshold {
            builder =
                builder.with_filter(Reference::new(filter_column).less_than(Datum::long(limit)));
        }
        if name == "all-pruned" {
            builder = builder.with_filter(Reference::new("x").equal_to(Datum::long(200)));
        }
        match name {
            "projection" | "writer-projection-filter" => {
                builder = builder.select(["X", "Z"]).with_case_sensitive(false);
            }
            "writer-empty-projection-filter" => builder = builder.select_empty(),
            "nested-projection-filter" => {
                builder = builder
                    .select(["DETAILS", "NUMBERS", "RECORDS", "LOOKUP", "OBJECTS"])
                    .with_case_sensitive(false);
            }
            "nested-filter-only" => {
                builder =
                    builder.select(["x", "y", "z", "numbers", "records", "lookup", "objects"]);
            }
            _ => {}
        }
        if name == "metadata-projection" {
            builder = builder.select([
                "x",
                RESERVED_COL_NAME_FILE,
                RESERVED_COL_NAME_POS,
                RESERVED_COL_NAME_SPEC_ID,
            ]);
        }
        let snapshot = fixture
            .table
            .metadata()
            .snapshot_by_id(requested_snapshot_id.unwrap_or_else(|| {
                fixture
                    .table
                    .metadata()
                    .current_snapshot()
                    .unwrap()
                    .snapshot_id()
            }))
            .unwrap();
        let expected_schema = if name == "time-travel" {
            snapshot.schema(fixture.table.metadata()).unwrap()
        } else {
            fixture.table.metadata().current_schema().clone()
        };
        let selected_names: Vec<String> = match name {
            "projection" | "writer-projection-filter" => vec!["x".into(), "z".into()],
            "writer-empty-projection-filter" => vec![],
            "nested-projection-filter" => ["details", "numbers", "records", "lookup", "objects"]
                .map(String::from)
                .to_vec(),
            "nested-filter-only" => ["x", "y", "z", "numbers", "records", "lookup", "objects"]
                .map(String::from)
                .to_vec(),
            "metadata-projection" => [
                "x",
                RESERVED_COL_NAME_FILE,
                RESERVED_COL_NAME_POS,
                RESERVED_COL_NAME_SPEC_ID,
            ]
            .map(String::from)
            .to_vec(),
            _ => expected_schema
                .as_struct()
                .fields()
                .iter()
                .map(|field| field.name.clone())
                .collect(),
        };
        let selected_ids: Vec<i32> = selected_names
            .iter()
            .map(|name| {
                expected_schema
                    .field_by_name(name)
                    .map(|field| field.id)
                    .unwrap_or_else(|| {
                        crate::metadata_columns::get_metadata_field_id(name).unwrap()
                    })
            })
            .collect();
        let expected_report = json!({
            "table-name": fixture.table.identifier().to_string(),
            "snapshot-id": snapshot.snapshot_id(),
            "schema-id": expected_schema.schema_id(),
            "filter": if name == "all-pruned" { Some("x = 200".to_owned()) } else { threshold.map(|limit| format!("{filter_column} < {limit}")) },
            "projected-field-names": selected_names,
            "projected-field-ids": selected_ids,
            "metadata": {},
        });
        let scan = builder.build().unwrap();
        let started = std::time::Instant::now();
        let counters = Arc::new(PlanningCounters::default());
        let mut stream = if cow {
            scan.plan_data_files(counters.clone(), |ctx| ctx.into_cow_rewrite_file())
                .await
                .unwrap()
                .map_ok(|file| file.scan_task)
                .boxed()
        } else {
            scan.plan_data_files(counters.clone(), |ctx| ctx.into_file_scan_task())
                .await
                .unwrap()
        };
        let (tasks, planning_error) = tokio::time::timeout(Duration::from_secs(10), async {
            let mut tasks = Vec::new();
            let mut error = None;
            while consume.is_none_or(|take| tasks.len() < take) {
                match stream.next().await {
                    Some(Ok(task)) => tasks.push(task),
                    Some(Err(err)) => {
                        error = Some(err.to_string());
                        break;
                    }
                    None => break,
                }
            }
            (tasks, error)
        })
        .await
        .unwrap();
        drop(stream);
        let counts = counters.snapshot();
        tokio::time::timeout(Duration::from_secs(10), async {
            while Arc::strong_count(&counters) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fixture counter producers must eventually stop");
        let settled_counts = counters.snapshot();
        if consume.is_none() && planning_error.is_none() {
            assert_eq!(counts, settled_counts);
        }
        drop(scan);
        let reports = tokio::time::timeout(Duration::from_secs(10), async {
            let mut reports = Vec::new();
            while let Some(report) = report_rx.recv().await {
                reports.push(report);
            }
            reports
        })
        .await
        .expect("reporters must release their senders");
        let elapsed_ns = started.elapsed().as_nanos();
        let metadata = output.join(format!("{fixture_name}.metadata.json"));
        fs::write(
            &metadata,
            serde_json::to_vec_pretty(fixture.table.metadata()).unwrap(),
        )
        .unwrap();
        let plans: Vec<_> = tasks.iter().map(|task| {
                let mut plan = json!({
                "path": task.data_file_path(), "size": task.file_size_in_bytes(),
                "deletes": task.deletes().iter().map(|d| json!({"path": d.file_path(), "size": d.file_size_in_bytes(), "content_size": d.content_size_in_bytes(), "content_offset": d.content_offset(), "content": match d.file_type() { DataContentType::Data => 0, DataContentType::PositionDeletes => 1, DataContentType::EqualityDeletes => 2 }})).collect::<Vec<_>>()
                });
                if cow {
                    plan["residual_is_true"] = json!(task.predicate().is_none());
                }
                plan
            }).collect();
        let finalization_supported = consume.is_none() && planning_error.is_none();
        cases.push(json!({"fixture": fixture_name, "cow_rewrite": cow, "metadata": metadata, "table_ident": fixture.table.identifier().to_string(), "threshold": threshold, "filter_column": filter_column, "consume": consume, "missing_manifest": missing_manifest, "planning_error": planning_error, "counts": counts, "settled_counts": settled_counts, "finalization_supported": finalization_supported, "plans": plans, "reports": reports, "expected_report": expected_report, "elapsed_ns": elapsed_ns, "delete_files": delete_files, "requested_snapshot_id": requested_snapshot_id, "time_travel": time_travel}));
    }
    fs::write(
        output.join("rust.json"),
        serde_json::to_vec_pretty(&cases).unwrap(),
    )
    .unwrap();
}
