# Completed scan reports (PR 1)

Reference: Java Iceberg `4e61757dcf568051a13bb8d4c00ef57c9f248947`;
Rust baseline `63d3d522c44aec588ebb7207834a37a141c3837a` (upstream main checked).
Original proposal: https://github.com/apache/iceberg-rust/pull/1496 and
https://github.com/apache/iceberg-rust/issues/1466. This change retains operation
reports without the old per-entry metrics channels or public reporter API.

Java paths below are relative to `core/src/main/java/org/apache/iceberg/` at
https://github.com/apache/iceberg/tree/4e61757dcf568051a13bb8d4c00ef57c9f248947.

| Metric | Unit | Java producer/event | Rust point | Validation fixture |
|---|---|---|---|---|
| total-data-manifests | count | DataTableScan.doPlanFiles: snapshot data manifests | build_manifest_file_contexts | unfiltered, pruning |
| total-delete-manifests | count | DataTableScan.doPlanFiles: snapshot delete manifests | build_manifest_file_contexts | deletes |
| scanned-data-manifests | count | ManifestGroup.entries: surviving manifests traversed | manifest selection | unfiltered, pruning |
| skipped-data-manifests | count | ManifestGroup.entries: partition predicate or no live files | manifest selection | manifest-pruned, dead-only |
| scanned-delete-manifests | count | DeleteFileIndex.Builder.loadDeleteFiles: surviving manifests | manifest selection | deletes |
| skipped-delete-manifests | count | DeleteFileIndex.Builder.loadDeleteFiles: partition predicate or no live files | manifest selection | pruning |
| skipped-data-files | count | ManifestReader.entries: live entries failing predicates; NOT dead entries | process_data_manifest_entry | file-pruned, dead entries |
| skipped-delete-files | count | ManifestReader.entries: live delete entries failing predicates | process_delete_manifest_entry | deletes |
| indexed-delete-files | count | metrics/ScanMetricsUtil.indexedDeleteFile: each indexed delete | accepted delete entry | unmatched and shared deletes |
| equality-delete-files | count | same: equality content | accepted delete entry | equality |
| positional-delete-files | count | same: position content excluding DVs | accepted delete entry | partition/path position |
| dvs | count | same: Puffin position delete | accepted delete entry | DV |
| result-data-files | count | metrics/ScanMetricsUtil.fileTask: selected data file | into_file_scan_task | all |
| result-delete-files | count | same: sum of attached delete array lengths, not unique files | into_file_scan_task | unmatched and shared deletes |
| total-file-size-in-bytes | bytes | same: physical data file size | into_file_scan_task | all |
| total-delete-file-size-in-bytes | bytes | same: attached delete physical size, DV content size (ScanTaskUtil.contentSizeInBytes) | into_file_scan_task | deletes, DV |
| total-planning-duration | ns | SnapshotScan.planFiles: timer start before doPlanFiles, stop in close callback | invocation through exhaustion and successful joins | lifecycle |

There is no additional Java selected-manifest metric. For a completed operation,
scanned means selected and visited, not an object-store cache miss.

## Lifecycle contract

Each plan_files invocation owns fresh counters. A report is finalized only after
full output consumption and successful joins of manifest, entry and delete-index
workers. No completed report on errors, panic, cancellation, or early stream drop
(including dropping after the last item without polling EOF). Report delivery is
synchronous invocation of a private `MetricsReporter` after finalization and before
EOF; the default `LoggingMetricsReporter` uses tracing. Tests use the same delivery
path with a recording implementation. There is no separate report task.
The timer includes manifest-list IO and caller backpressure while consuming tasks,
but excludes report delivery. A blocking tracing subscriber can delay EOF as with
any synchronous tracing event; no new queue, async send, or reporting error enters
planning. Repeated/concurrent invocations do not share accumulators. No snapshot:
no report. Existing snapshot with zero tasks: one completed report. Shared COW
planning: no scan report.

Java SnapshotScan.planFiles uses CloseableIterable.whenComplete, whose close runs
the callback in finally, including partial consumption and close failure. Its
ScanMetrics belongs to the scan object, so reusing that object can accumulate.
Rust intentionally reports only successful, fully consumed invocations and does
not reproduce either behavior. Worker handles are retained and joined; this is a
localized ownership change, not a replacement planner architecture.

## Projection and compatibility

Resolve report names from selected field IDs in the actual scan schema, not the
requested spellings. Current scans use current schema; explicit time travel uses
snapshot schema. Java useSnapshot retains the current schema unless explicitly
projected: the harness must project the snapshot schema to match Rust time travel.
Java reports sanitized filters; Rust stores the predicate privately and logs only
whether a filter is present, without literal values. Nested projection needs special care:
Java TypeUtil.getProjectedIds selects leaf IDs; Rust only accepts direct-child
selection today. Unsupported configurations must not be labeled parity.

Java also filters delete entries using row metrics and sequence-number thresholds;
Rust's current delete entry planner filters partitions only. Do not silently
reinterpret those differences as measured zeros or change delete planning here.

Partial projection difference observed by the harness: Java BaseScan.schema ->
BaseScan.lazyColumnProjection -> TypeUtil.project constructs a new Schema with
its default ID (0). Rust reports the actual source schema ID (1 in `projection`).
This is retained as a mismatch, not normalized away. The intended Rust contract
keeps schema identity useful for resolving the projected IDs against metadata.
