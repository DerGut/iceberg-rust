# Scan termination reports (PR 1)

Reference: Java Iceberg `4e61757dcf568051a13bb8d4c00ef57c9f248947`;
Rust baseline `63d3d522c44aec588ebb7207834a37a141c3837a` (upstream main checked).
Original proposal: https://github.com/apache/iceberg-rust/pull/1496 and
https://github.com/apache/iceberg-rust/issues/1466. This change retains operation
reports without the old per-entry metrics channels. PR #1 keeps the report model
private in `src/metrics.rs`; a separate branch exposes the async reporter API.

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

Finalization takes place in `Drop` after the last contributor releases the state.
Every `MetricsReporter`, including the private default debug logger, receives an
owned `MetricsReport::Scan` in one delivery task on the table's I/O runtime.
Reporter configuration holds a direct `Arc<dyn MetricsReporter>`; a REST catalog reporter
can be implemented and installed in its own crate without a core enum change.
Each poll restores the originating tracing dispatcher. Successful EOF waits for
that task to finish or be cancelled; early drop and planning errors do not wait
for async delivery. The runtime must remain alive for delivery after termination,
including default logging. Runtime shutdown or process termination can lose
reports. Delivery is attempted at most once with no automatic retries; calls may
overlap.

The timer includes manifest-list I/O, consumer backpressure, and worker teardown,
but excludes delivery. A slow callback still delays successful EOF. Callbacks
return `()` and handle/log their own delivery failures; reporting cannot return a
planning error. Callback unwinding panics are contained, including when
finalization is already unwinding. Panic payloads are
forgotten because their destructors can themselves panic. Implementations must
yield during I/O and bound their own waits; blocking or process-aborting callbacks
cannot be contained. Tests use explicit completion notifications and bounded
timeouts, including a gated callback and a shut-down runtime.

The table and scan store a reporter directly, defaulting to the private debug
logger. PR #1's tests inject consumers through the private planning seam; it has
no table/scan setters. There is no `Option`, utility wrapper, or built-in disabling
mode. The public follow-up adds `with_metrics_reporter(Arc<dyn MetricsReporter>)`
on tables and scan builders, together with inheritance/precedence/no-op tests; a custom no-op consumer discards reports. Table
configuration applies to that instance and its future clones; it is not persisted
or inherited by independently loaded tables. Private report accessors return raw
predicates; the public follow-up sanitizes them before delivery. The default
logger does not log literal filter values. There is no per-file metrics send,
accumulator task, global recorder, REST delivery, or commit report.

No snapshot: no report. Existing zero-result snapshot: one report after termination.
Repeated/concurrent calls have independent accumulators. Normal and COW planning
both collect scan reports. Partial counters describe actual planning work, including
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
The other 33 fixtures fully consume planning without reading rows.

COW uses the same reporting lifecycle and counters. Its report retains the filter
used to select candidate files; its tasks clear the row predicate so a rewrite
can read the entire file. Java Spark's
[`SparkScanBuilder.buildCopyOnWriteScan`](https://github.com/apache/iceberg/blob/4e61757dcf568051a13bb8d4c00ef57c9f248947/spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/source/SparkScanBuilder.java#L699-L734)
attaches a metrics reporter to a batch scan with `ignoreResiduals()`. The COW harness follows that core scan setup
with the same snapshot, filter, case sensitivity, and projection as Rust. Seven
fully consumed COW cases cover writer-produced statistics, file/all pruning,
shared positional deletes, DVs, and nested/filter-required report projections.
Three more cover partial close, close after the last task, and a manifest-read error. The plan capture checks each COW task's
actual cleared predicate/always-true residual. This validates scan planning;
it does not execute a Spark job, read rows, rewrite files, or commit changes.


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
Java reports sanitized filters. PR #1 keeps the unsanitized predicate private;
default logging records only whether a filter is present, without literal values.
The public follow-up sanitizes before delivering a report, as described below.

Rust report projection follows Java BaseScan.lazyColumnProjection: expand selected
whole types, add bound filter references, and enumerate the resolved projection
with TypeUtil.getProjectedIds semantics. This includes struct ancestors and selected
descendants, and Java's list/map container rules. Names come from the schema rather
than requested case-sensitive spellings. This changes report fields, not task output
projection. The fixtures include primitive/struct list elements and map values,
filters on unselected columns or nested descendants, empty output projection, case
insensitivity, and COW planning. Direct nested-subfield output selection remains
unsupported by the Rust planner. An unresolved report name asserts in debug builds
and falls back to its numeric ID in release builds.

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

Report schema identity follows Java BaseScan.schema -> BaseScan.lazyColumnProjection
-> TypeUtil.project. A projection that retains the entire source schema preserves
its ID, including when filter-required fields restore otherwise omitted fields.
A reduced schema, including an empty projection of a nonempty source, uses
DEFAULT_SCHEMA_ID (0). Rust keeps the source schema for planning; deriving this
projected ID affects only the report.
Schema IDs are compared exactly alongside projected IDs/names.

Java's select() cannot resolve metadata columns, so the metadata fixture supplies
an explicit Schema containing the selected data and metadata fields. It uses the
Schema constructor's default ID (0), matching a newly constructed projection;
it does not copy an ID from the Rust report or table schema. Both planners select
the same data and metadata fields.

Report field enumeration is infallible and does not alter task projection.
Variant collection traversal has Rust regression coverage only; the pinned Java
schema projection helper rejects explicit variant list/map selection, so that
case is outside the executed comparison.

## Public filter sanitization

The public follow-up sanitizes after stopping the planning timer and before
passing the owned report to any reporter. It preserves operators and references;
numeric values become magnitude descriptions, typed temporal values become
relative descriptions, and remaining values use Java-compatible diagnostic
hashes. These hashes are not cryptographic anonymization. Sanitized predicates
are diagnostic data and must not be evaluated as the original filter.

At the pinned Java revision, `SnapshotScan.planFiles` constructs its report with
`ExpressionUtil.sanitize(schema().asStruct(), filter(), context().caseSensitive())`.
The source history explains the intent:

- [#4672](https://github.com/apache/iceberg/pull/4672), commit
  `26d8c3ed9eceaf0fd7d2429302a4c7ccedbb2111`, introduced masking to avoid leaking
  identifiers and sensitive values through diagnostic logging.
- [#8149](https://github.com/apache/iceberg/pull/8149), commit
  `e209f188f93acf53c34d43ededc40b9daeb040ba`, binds the schema before sanitization
  so pushed-down numeric dates/timestamps retain their temporal meaning.
- [#15224](https://github.com/apache/iceberg/pull/15224), commit
  `45d2ed0e7ea7230c8f54f02d900c0c454b68cf9d`, introduced the pinned bound-set
  regression: raw values are cast to `Literal<?>`, raising `ClassCastException`.
  The public method catches it and sanitizes the unbound expression. The executed
  `timestamp-long-set` fixture proves the resulting numeric/temporal mismatch.
  Rust deliberately retains schema-aware temporal descriptions; it does not
  reproduce that regression.

The 85-case oracle fixes Java's visitor clock and runs its actual binder/visitor,
with the same bound-to-unbound fallback as the public method. Complete normalized
expressions are compared, including set predicates; the separate Rust delivery
test confirms normal and COW consumers receive sanitized filters and Debug output.
Set ordering and repeated sanitized members are normalized because Rust uses a
set. Numeric magnitude, string hash, operator, reference, and temporal descriptions
are compared exactly. Production time buckets use wall-clock time at report
finalization. The fixed-clock comparison checks boundaries without a clock race.

Java rejects NaN equality literals; unary NaN predicates are covered. Transform
and variant terms have no Rust equivalent here. Binary/fixed fixtures correspond
to Java heap ByteBuffers at position zero: Java hashes their `toString()` buffer
description, not content bytes. The test does not establish parity for arbitrary
Java buffer implementations/positions. The public API exposes neither raw filters
nor a public sanitizer, serialized report schema, or construction API.
