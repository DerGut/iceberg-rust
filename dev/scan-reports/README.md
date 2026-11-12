# Java differential validation

This validation branch is stacked on
[`scan-reports-1-collection`](https://github.com/DerGut/iceberg-rust/tree/scan-reports-1-collection).
Only the parent branch is intended for PR #1; this branch retains reproducible
review evidence and the Java harness separately. It contains no PR-description draft.

This harness is an ignored crate unit test plus a Java test copied into a pinned
Iceberg source build. It adds no Rust dependencies or production API.
The private recording reporter implements the public async `MetricsReporter`
trait. Tests inject it through a private planning method, with no test-only field
on `TableScan`. Serialization derives exist only under `cfg(test)` on this
validation branch; they are not part of the public report API.

Java: `4e61757dcf568051a13bb8d4c00ef57c9f248947`.
Rust upstream baseline: `63d3d522c44aec588ebb7207834a37a141c3837a`.
PR #1 has four commits on the parent branch: `420239f78` (planner behavior),
`43ff46807` (counting), `c59926e50` (private reporting with debug logging), and
`aebcf7d45` (public async reporter API and configuration). The planner prerequisite commit unifies worker results and completion, with controlled manifest-read failure and
cancellation tests, unused delete-index failures, and dead-only manifest skipping
for both scan and COW planning. The counting commit instruments both paths without
optional counters. The reporting commit
delivers reports after contributing workers stop, including COW, partial, and failed planning, restoring the originating tracing dispatcher for delivery.
Reports have no outcome tag and do not certify successful or exhaustive planning.
The public API commit preserves these counters and finalization boundaries, adds
table configuration and scan overrides, and includes an
[in-memory reporter example](../../crates/examples/src/scan_metrics.rs).
The [evidence manifest](evidence/manifest.json) pins the executed Rust revision,
Java revision, runtime versions, and command.

Run from the repository root with JDK 17 or 21, Python 3, curl, and the repository
Rust toolchain installed:

```sh
bash dev/scan-reports/run.sh /tmp/iceberg-scan-reports
```

The script records HEAD, the pinned production ancestor, the working-tree patch,
toolchain versions, and check commands/statuses/log hashes, downloads
Java by commit, and uses its Gradle wrapper. The archive needs `version.txt`;
Git-properties generation is disabled because archives have no `.git` directory.
The version marker is build metadata, not a dependency resolution substitute.
Use a fresh output directory for each revision. Gradle tests are explicitly rerun
because external fixture files are not Gradle inputs.

**Expected exit status: 1** for the differences below. A nonzero comparison is
not a Java execution failure. No mismatch is ignored or converted to a pass;
`results/comparison.json` records fixture, metric, both values, and explanation.

## Inputs and comparisons

Rust creates each table once; both runtimes read exactly the same metadata JSON,
manifest lists, manifests, and physical Parquet/Puffin files. Java does not generate
an equivalent table independently. The primary comparisons fully consume planning
tasks without reading rows; six separate lifecycle cases stop early or encounter
an iteration error. The input inventory hashes the shared files.

The 26 fully consumed normal-scan cases cover:

- `unfiltered`, `dead-only`, `manifest-pruned`, `file-pruned`, `all-pruned`: live and
  dead entries, manifest exclusion, and zero-result scans.
- `projection`, `current-schema`, `time-travel`, `nested-all`,
  `metadata-projection`: case-insensitive selection, schema evolution, nested
  structs, primitive/struct list elements and map values, and
  `_file`/`_pos`/`_spec_id` alongside a data column.
- `unmatched-delete`, `shared-position`, `path-position`, `equality`: positional
  deletes attached to zero/one/ten data files and equality deletes across partition
  type promotion.
- `dv`, `dv-supersession`, `delete-manifest-pruned`: DV content bytes versus physical
  Puffin bytes, a DV overriding an otherwise applicable positional delete, and
  exclusion of both data and delete manifests by partition predicates.
- `writer-unfiltered`, `writer-file-pruned`, `writer-all-pruned`,
  `writer-equality-pruning`, `writer-equality-association`: actual Parquet writer
  statistics, dead entries, data-file pruning, delete-file predicate pruning, and
  equality-delete association using column bounds.
- `writer-projection-filter`, `writer-empty-projection-filter`,
  `nested-projection-filter`, `nested-filter-only`: filter-required columns absent
  from output selection, empty output projections, nested filter ancestors, and
  case-insensitive struct/list/map selection.

Ten `cow-` fixtures additionally exercise COW planning. Seven fully consume
`writer-unfiltered`, `writer-file-pruned`, `writer-all-pruned`, `shared-position`,
`dv`, `writer-projection-filter`, and `nested-projection-filter`; three exercise
`close-after-one`, `close-after-last`, and `manifest-read-failure`. Rust uses its COW planning entry point. Java constructs
a batch scan with `ignoreResiduals()`, as Spark's COW scan builder does, matching
the snapshot, filter, case sensitivity, and projection. Reports retain the
selection filter; the captures separately verify that COW tasks clear their row
predicate (Rust) or have an always-true residual (Java). These are planning tests,
not Spark execution, row reading, rewriting, or commit tests.

The writer and nested cases use `ParquetWriterBuilder` to produce files and
manifest statistics from the same record batches. They construct snapshots with
manifest writers; this is not an append/row-delta transaction test. Older fixtures
materialize missing files and physical sizes but retain synthetic statistics, so
those cases establish planner agreement rather than row-level correctness.

For time travel both implementations select the snapshot schema: Java's
`DataTableScan.useSnapshotSchema()` enables this in `SnapshotScan.useSnapshot()`.
The Java fixture calls `useSnapshot()` without an explicit projection.

Report projections follow Java's resolved-field rules: expand selected nested
types and include fields needed by the filter, even when absent from the output
selection. Report fields are resolved independently of Rust's task output
projection. The report preserves the source schema ID when all fields survive,
including when filter-required fields restore the full schema. A reduced schema
uses the default projected-schema ID, 0.

For metadata columns Java uses an explicit projection schema:
its `select()` resolves only table columns. The fixture constructs that schema
with Java's default ID, 0, rather than supplying the table's source schema ID.
Both implementations use the same selected fields, snapshot, filter, and case
sensitivity. The schema ID is compared exactly; it is not copied from Rust or
normalized during comparison.
Java receives its table name from the fixture identifier, independently of the
Rust report. Negative controls independently corrupt each component of the Rust
report identifier, the partial-projection schema ID, a counter, and the plan and
require detection of each change.

Only these representation conversions are made:

- Rust report `TableIdent` is formatted with dot-joined namespace components and
  a dot before the table name, matching its `Display` implementation, for comparison
  with Java's table-name string. Namespace components remain typed in the raw capture.
- Snake-case Rust field names map to Java's hyphenated metric names.
- For COW tasks only, `residual_is_true` compares the absence of a Rust task
  predicate with Java's always-true residual. Each side must independently be
  true. Ordinary scan task residuals are not compared by this harness.
- Plan order, delete order, and paired projected IDs/names are sorted. Paths,
  sizes, delete content types, and DV offsets/sizes are compared exactly.
- Rust predicate enums map to Java expression JSON for the fixture operators.
  Raw Java scan filters are captured and compared exactly with Rust report
  predicates. Java's sanitized report filter is retained but not compared: Rust
  stores a raw predicate and does not implement Java's sanitizer.
- Retained evidence replaces each fixture's temporary table root with
  `<fixture-root>` and the metadata JSON path with `<metadata-file>`. Comparison itself uses exact absolute paths before this step.
- Durations are not compared numerically. Java nanoseconds/count=1 and the
  validity of Rust's serialized Duration (seconds/nanoseconds) are checked
  independently; Rust regression tests exercise successful EOF, errors, cancellation and worker teardown.

Absent metrics use an explicit `ABSENT` marker, never zero. Java environment/options
metadata has no Rust counterpart in PR 1 and is listed as unsupported report data.
Rust counters are untagged integers: there is no runtime unit field to compare.
Units are source-audited in SEMANTICS.md, with byte/count values checked against
shared physical-file plans; Python does not fabricate unit tags for Rust.

## Results and limits

The [archived run](evidence/manifest.json) captured Rust revision
`7a393a73368821709ded13cf2187485cdf69ab98`, including production revision
`aebcf7d45234491d7b26b7e60932c8d09f931503`. Of 39 fixtures, 37 match every
compared field, counter, and plan. All schema IDs and projected field IDs/names
match Java, including partial, empty-output, nested, filter-completed, metadata,
and COW projections. The two mismatched fixtures are existing equality-delete
planning differences. Every mismatch is retained in
[comparison.json](evidence/comparison.json). All six lifecycle cases matched in
this run; partial-counter agreement remains scheduling-dependent.

Of the 33 fully consumed fixtures (26 normal, seven COW), 31 match all compared
fields, counters, and file/delete plans. Two fixtures retain differences:

- `writer-equality-pruning`: for `y < 20`, Java rejects `eq-99.parquet` using its
  actual bounds; Rust's delete-entry planner filters partitions only. Java indexes
  and attaches one delete; Rust indexes and attaches two. Java skipped-delete
  count is 1; Rust's is 0. Delete bytes differ accordingly.
- `writer-equality-association`: with no filter both index two equality deletes.
  Java uses bounds to attach `eq-10` only to `written-10`, and `eq-99` to neither
  file. Rust attaches both deletes to both data files (one versus four attachments).
  This exposes an existing planning difference independently of report counting.

See [SEMANTICS.md](SEMANTICS.md) for producers and lifecycle choices. No blanket
Java-parity claim is made. Direct nested subfield selection is unsupported by
Rust; variant collections, delete sequence pruning, partition-spec evolution
with mixed deletes, and v1 tables remain outside the executed Java matrix. Variant
collections have Rust report regression coverage. The pinned Java projection
helper rejects explicitly selected variant list elements/map values; that case
is source-audited, not differentially executed. Java's
reused-scan accumulation remains source-audited; Rust deliberately keeps each invocation independent.

Rust tests use a manifest reader controlled by explicit notifications to block,
fail, or cancel I/O at concurrency one for normal and COW plans. They observe the blocked read being dropped,
bound all waits, and check exactly one report after contributors stop. COW report tests also check the selection
filter and zero/one/two-file plans, including empty tables and unused-index failures.
A separate test blocks a running worker across cancellation and verifies that its
last counter update precedes delivery. Repeated/concurrent planning, no-snapshot
tables, and cancellation during manifest-list loading also have regression coverage.
Error regressions require the originating validation error and its source to survive
channel shutdown. Scoped-subscriber tests capture real tracing reports on full
consumption and cancellation, for both normal and COW planning. Reporter-panic
regressions cover full consumption, cancellation, and an existing planner unwind,
including a panic payload whose destructor panics. Callback panics are contained
without changing planning results. Finalization runs from `Drop` on the thread
that releases the last owner, potentially during an existing unwind. Default
debug logging runs synchronously there; custom async delivery runs on the table
I/O runtime. Both restore the originating tracing dispatcher. Guards deliberately
forget caught panic payloads so their destructors cannot cause a second panic.
They cannot contain blocking callbacks or aborting panics.

Configuration regressions cover inheritance, replacement, disabling, and
re-enabling for normal and COW scans. Gated async callbacks establish that
successful EOF waits for delivery, consumer drop does not cancel an already
started callback, and planning errors return without waiting. A shut-down runtime
cancels custom delivery without hanging EOF. Disabling delivery still collects
counters. The example exercises the public configuration and read accessors;
the differential exporter continues to use the private test seam.
An unresolved report field name triggers a debug assertion and falls back to its
numeric ID in release builds; Rust regression coverage checks both paths.
These checks do not establish cancellation latency for a network storage
implementation.

Six lifecycle fixtures exercise normal and COW planning; all six match the
compared counters and plans in this run. Both close-after-one cases count one
planned file (3,003 bytes) in Java and Rust. Reports are delivered in all six
lifecycle cases, including failures.

Partial counts depend on worker scheduling. The normal close-after-one case
counted two Rust files versus one Java file in the
[preceding pinned run](https://github.com/DerGut/iceberg-rust/blob/8c55ac82db60b92a6deab1a7d62acac053bc22ac/dev/scan-reports/evidence/comparison.json).
Both close-after-one cases differed in an
[earlier pinned run](https://github.com/DerGut/iceberg-rust/blob/d729ff0aea8070f4e872e0f4ec2f3685ef7d1c8b/dev/scan-reports/evidence/comparison.json).
These observations and their provenance remain available; neither normal nor
COW cases establish exact partial-counter parity. The following lifecycle cases
run in both modes:

- `close-after-one`: consume one of two tasks, then close/drop. Both emit reports.
  Rust may have already planned buffered work; every observed counter difference
  is retained with that explanation. This is not an exact partial-counter parity claim.
- `close-after-last`: consume both tasks and close/drop without polling EOF.
  Both emit reports; neither labels the reason for termination.
- `manifest-read-failure`: both read the same metadata/list referencing an
  intentionally removed manifest. Both fail iteration and deliver a report;
  the removed path and iteration error are recorded in the fixture capture.

All six use the same input files for both implementations. Partial counters
are scheduling-dependent; consumed plans and every counter are still compared
exactly, with differences recorded rather than discarded. Neither report has an
outcome field; iteration errors and requested consumption are captured independently.

Retained [comparison](evidence/comparison.json), [Rust captures](evidence/rust.json),
[Java captures](evidence/java.json), and [input hashes](evidence/inputs.json) are
machine-readable. Archive a run after comparison with:

```sh
python3 dev/scan-reports/archive.py /tmp/iceberg-scan-reports /tmp/scan-evidence
```

`archive.py` reads provenance and check results produced by `run.sh`; it requires
no manual additions to the evidence manifest. The archived production revision is
verified as an ancestor of the captured Rust revision. Re-running the archiver on
the same run directory reproduces all retained evidence files.

No performance claim is made. The old debug enabled/disabled timing and its test
have been removed: they did not compare alternative implementations or exercise
an installed tracing subscriber. Logging delivery overhead has not been measured.
