# Scan termination reports (PR 1)

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
| scanned-data-manifests | count | ManifestGroup.entries: surviving manifests traversed | start manifest processing | unfiltered, pruning, partial close |
| skipped-data-manifests | count | ManifestGroup.entries: partition predicate or no live files | manifest selection | manifest-pruned, dead-only |
| scanned-delete-manifests | count | DeleteFileIndex.Builder.loadDeleteFiles: surviving manifests | start manifest processing | deletes |
| skipped-delete-manifests | count | DeleteFileIndex.Builder.loadDeleteFiles: partition predicate or no live files | manifest selection | delete-manifest-pruned |
| skipped-data-files | count | ManifestReader.entries: live entries failing predicates; NOT dead entries | process_data_manifest_entry | file-pruned, dead entries |
| skipped-delete-files | count | ManifestReader.entries: live delete entries failing predicates | process_delete_manifest_entry | writer-equality-pruning (observed difference) |
| indexed-delete-files | count | metrics/ScanMetricsUtil.indexedDeleteFile: each indexed delete | accepted delete entry | unmatched and shared deletes |
| equality-delete-files | count | same: equality content | accepted delete entry | equality |
| positional-delete-files | count | same: position content excluding DVs | accepted delete entry | partition/path position |
| dvs | count | same: Puffin position delete | accepted delete entry | DV |
| result-data-files | count | metrics/ScanMetricsUtil.fileTask: selected data file | into_file_scan_task | all |
| result-delete-files | count | same: sum of attached delete array lengths, not unique files | into_file_scan_task | unmatched and shared deletes, writer-equality-association (observed difference) |
| total-file-size-in-bytes | bytes | same: physical data file size | into_file_scan_task | all |
| total-delete-file-size-in-bytes | bytes | same: attached delete physical size, DV content size (ScanTaskUtil.contentSizeInBytes) | into_file_scan_task | deletes, DV |
| total-planning-duration | ns | SnapshotScan.planFiles: timer start before doPlanFiles, stop in close callback | invocation through consumer termination and worker teardown | lifecycle |

There is no additional Java selected-manifest metric. For a completed operation,
scanned means selected and visited, not an object-store cache miss.

## Lifecycle contract

Each snapshot-bearing plan_files invocation owns fresh counters and produces at most
one immutable report with a typed TableIdent. Reports describe work performed, with
no success, failure, or cancellation tag: they do not certify exhaustive or
successful planning. Successful EOF, early drop (including LIMIT and dropping
after the last task without polling EOF), planning errors, and unwinding panics
release the reporting state. An unpolled planning future has not started and does
not report. Outcome classification is deferred to a possible follow-up.

Successful EOF still waits for manifest, entry, and delete-index joins. Every
spawned future, including nested entry workers and the index, holds a reference
to the operation state from before spawn. After drop or failure, abort-on-drop
handles request cancellation; finalization waits for the last state reference to be released.
The Arc release/acquire barrier synchronizes all contributing futures before
reading final counters, even when a worker is still in a synchronous poll after
cancellation. No new coordinator, accumulator task, or per-file channel is used.
A regression blocks such a worker, requests cancellation, and confirms that
report delivery waits for its final counter update.

Delivery invokes the private synchronous `MetricsReporter` on the thread that
releases the last owner. After successful consumption delivery precedes EOF;
after early drop/error it may happen later, when workers stop. A live runtime
must drive abort cleanup (runtime shutdown also drops task futures). Abrupt
process termination cannot guarantee delivery. Tests await an explicit report
notification with a bounded timeout. The timer includes manifest-list I/O,
consumer backpressure, and worker teardown, but excludes reporter delivery.
A blocking tracing subscriber can delay the releasing thread. There is no new
report queue, async metrics send, or reporting error in the planner.

No snapshot: no report. Existing zero-result snapshot: one report after termination.
Repeated/concurrent calls have independent accumulators. COW planning does not
collect scan reports. Partial counters describe actual planning work, including
buffered tasks the caller did not consume. Scanned manifests count processing
starts, not all selected/queued manifests. Manifest totals are recorded from the full list before evaluating selection,
even if selection fails. Selection and its skipped counts are evaluated eagerly
in Rust; Java can evaluate them lazily.

Java SnapshotScan.planFiles uses CloseableIterable.whenComplete: close invokes
the report callback even after partial iteration or an iteration/close error.
Rust also exposes these terminated operations, and waits for contributors to stop.
Neither implementation tags its report with an outcome. Java ParallelIterable.close
requests cancellation without the same join barrier; scheduling and read-ahead
can produce different partial counters. Do not infer numeric partial-report
parity from common visibility. Errors before Java returns its iterable may
produce no report, while Rust delivers a report when setup fails after the
snapshot-bearing invocation has created its reporting state. Java's metrics belong to the scan object and can accumulate
on reuse; Rust's reports remain operation-scoped.

The differential harness executes close after one of two tasks, close after the
last task without requesting EOF, and a missing-manifest iteration failure.
It compares the observed counters exactly and retains read-ahead differences.
Iteration errors and requested consumption are captured independently of reports.
The original 22 fixtures still fully consume planning without reading rows.

## Planner behavior changes

The lifecycle commit joins the delete-index worker and propagates its result even
when no data task asks for deletes. Joining is required to establish completion;
propagating the inner error is a separate contract choice. Invalid duplicate DVs
now fail a scan whose data files are all pruned. Java ManifestGroup.plan
builds DeleteFileIndex before creating data tasks. Preserving Rust's previous
empty success is possible and independent of reporting. The chosen planner contract
requires all planning stages to succeed. Tests exercise this error for normal and
COW plans, and report delivery after the error.

The planner prerequisite commit skips manifests whose manifest-list counts prove there are
no added or existing files. This matches Java's manifest selection and skipped
manifest counters. It is not required for completion safety. It avoids reading
those files in both normal and shared COW planning; an absent or corrupt dead-only
manifest is no longer opened. It relies on valid manifest-list live-entry counts.
A regression deletes the dead-only manifest and verifies both planners still
produce no tasks, with a skipped-manifest count in the scan report.

## Projection and compatibility

Resolve report names from selected field IDs in the actual scan schema, not the
requested spellings. Current scans use current schema; explicit time travel uses
snapshot schema. At the pinned Java revision, DataTableScan.useSnapshotSchema()
returns true, and SnapshotScan.useSnapshot() selects SnapshotUtil.schemaFor for
that snapshot. The harness therefore calls useSnapshot() without project().
Java reports sanitized filters; Rust stores the predicate privately and logs only
whether a filter is present, without literal values. Nested projection needs special care:
Java TypeUtil.getProjectedIds includes struct IDs and their selected descendants.
The `nested-all` run observes Java IDs [1,2,3,9,10] versus Rust [1,2,3,9]: Rust
currently records its resolved top-level task projection. Both plan the entire
struct, with matching counters. Expanding Rust reports to Java's representation
is an unresolved contract choice; direct nested-subfield selection is unsupported
by the Rust planner. List/map projection reports have not been compared.

Java also filters delete entries using row metrics and sequence-number thresholds;
Rust's current delete entry planner filters partitions only. The writer-equality-pruning
fixture measures this difference: Java skips eq-99 using y < 20, while Rust indexes
and attaches it. The unfiltered writer-equality-association fixture separately
exercises DeleteFileIndex.canContainEqDeletesForFile: Java uses data/delete bounds and
attaches one delete total; Rust uses partition/sequence checks and attaches four.
Their indexed counts both equal two. Counter differences follow actual plans;
no zero, missing value, or normalization hides the difference. These existing
planner optimizations are outside the metrics change. Sequence pruning is still
source-audited, not differentially exercised.

Partial projection difference observed by the harness: Java BaseScan.schema ->
BaseScan.lazyColumnProjection -> TypeUtil.project constructs a new Schema with
its default ID (0). Rust reports the actual source schema ID (1 in `projection`).
This is retained as a mismatch, not normalized away. The intended Rust contract
keeps schema identity useful for resolving the projected IDs against metadata.
