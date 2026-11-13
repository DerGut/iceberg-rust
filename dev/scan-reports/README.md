# Java differential validation

This validation branch is stacked on
[`scan-reports-1-collection`](https://github.com/DerGut/iceberg-rust/tree/scan-reports-1-collection).
Only the parent branch is intended for PR #1; this branch retains reproducible
review evidence and the Java harness separately. It contains no PR-description draft.

This harness is an ignored crate unit test plus a Java test copied into a pinned
Iceberg source build. It adds no Rust dependencies or production API.
The private recording reporter implements the crate-private async `MetricsReporter`
trait. Tests inject it through a private planning method, with no test-only field
on `TableScan`. Serialization derives exist only under `cfg(test)` on this
validation branch; they are not part of the public report API.

Java: `4e61757dcf568051a13bb8d4c00ef57c9f248947`.
Rust upstream baseline: `63d3d522c44aec588ebb7207834a37a141c3837a`.
PR #1 has three commits on the parent branch: `9d03a810a` (planner behavior),
`9f7bc1a78` (counting), and `beea97fbb` (private async reporting with debug logging).
The planner prerequisite unifies worker results and completion, with controlled
manifest-read failure and cancellation tests, unused delete-index failures, and
dead-only manifest skipping for both scan and COW planning. The existing gated
storage helper stays in the caching delete loader's test module. The counting
commit instruments both paths without optional counters. The reporting commit
delivers reports after contributing workers stop, including COW, partial, and
failed planning, restoring the originating tracing dispatcher for delivery.
Reports have no outcome tag and do not certify successful or exhaustive planning.
Report definitions stay crate-private in `src/metrics.rs`. Public visibility,
table/scan setters, an in-memory example, and filter sanitization are on the
separate [public follow-up branch](https://github.com/DerGut/iceberg-rust/tree/scan-reports-2-public-api).
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
  keeps a raw predicate private on PR #1 and omits literal values from logging.
  The public follow-up sanitizes before delivery; its separate oracle check is
  described below.
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
`541b7c1ecf7366b160ecc10a0ee989682751678d`, including production revision
`980a56de2def52603444eb5c285a3aae1ae6f4dc`. Of 39 fixtures, 35 match every
compared field, counter, and plan. All schema IDs and projected field IDs/names
match Java, including partial, empty-output, nested, filter-completed, metadata,
and COW projections. The four mismatched fixtures are two existing equality-delete
planning differences and two partial-close read-ahead cases. Every mismatch is
retained in [comparison.json](evidence/comparison.json).

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
debug logging and custom reporters share one async delivery path on the table
I/O runtime, restoring the originating tracing dispatcher. Reporter storage is an
direct `Arc<dyn MetricsReporter>`; catalog crates can supply their own implementations without
adding a core enum variant. Callbacks return `()` and handle/log their own delivery
failures. Guards deliberately forget caught panic payloads so their destructors cannot cause a second panic.
They cannot contain blocking callbacks or aborting panics.

The public follow-up contains the table/scan setters and configuration regressions
for inheritance, replacement, custom no-op reporters, and clone/runtime preservation
in normal and COW scans. PR #1's delivery tests use the private planning seam.
Gated async callbacks establish that
successful EOF waits for delivery, consumer drop does not cancel an already
started callback, and planning errors return without waiting. A shut-down runtime
cancels delivery without hanging EOF, for both default logging and a custom
reporter. There is no built-in disabled state; a no-op consumer still receives
a report. The differential exporter uses the private test seam. The separate
public follow-up example exercises the public setters and read accessors.
An unresolved report field name triggers a debug assertion and falls back to its
numeric ID in release builds; Rust regression coverage checks both paths.
These checks do not establish cancellation latency for a network storage
implementation.

Six lifecycle fixtures exercise normal and COW planning; four match the
compared counters and plans in this run. Both normal and COW close-after-one
count two files in Rust (6,006 bytes) versus one in Java (3,003 bytes). Reports are delivered in all six lifecycle cases, including failures.

Partial counts depend on worker scheduling. Both close-after-one cases matched
in the [preceding pinned run](https://github.com/DerGut/iceberg-rust/blob/d467bb6b2201e745b70457dc150473d6a532a3c6/dev/scan-reports/evidence/comparison.json).
Both differed in an
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

## Public filter sanitization follow-up

The primary scan comparison above runs the private parent. A separate check pins
[`scan-reports-2-public-api`](https://github.com/DerGut/iceberg-rust/tree/7b261f9c6aaad03eb2fc8b23c48eca35eadcdc94)
and compares its sanitizer regression assertions against actual Java output:

```sh
python3 dev/scan-reports/check_sanitization.py /tmp/iceberg-filter-sanitization
```

It uses the same pinned Java revision, JDK requirements, and source Gradle wrapper.
The script extracts the public commit's fixture inputs, invokes Java's production
binder and sanitizer visitor, and reruns the public commit's Rust full-expression
assertions, its normal/COW delivery test, and its reporter configuration test in
a detached temporary worktree.
Only Java's sanitizer clock is fixed through reflection. Binding and the public
sanitizer's bound-to-unbound fallback are preserved. The helper does not call the
public sanitizer method directly because that method has no injectable clock.
Rust values in the comparison are verified expected expressions, not a separate
Rust capture; the regression test must pass for results to be archived.

The [sanitizer evidence](sanitization-evidence/manifest.json) records pinned
revisions, runtime/wrapper versions, fixture and log hashes, check commands,
and the harness patch. [Java's raw output](sanitization-evidence/java.json)
retains every case and fallback. There are 85 cases, covering all supported
predicate shapes, schema binding, numeric boundaries, strings/Unicode, malformed
date-like strings, temporal units and relative buckets, decimals, UUIDs,
binary/fixed values, case sensitivity, compound expressions, and sets.
Only set ordering and duplicate sanitized set descriptions are normalized;
Rust's predicate representation stores a set. No literal description is masked
or discarded. The fixed clock is part of the fixture data, not a normalization.

Eighty-four cases match. `timestamp-long-set` retains a pinned Java regression:
its bound-set sanitizer throws `ClassCastException` and falls back to unbound
longs, producing numeric descriptions. Rust keeps timestamp descriptions. The
fixture contains both results and the explanation. **Expected exit status: 1**
records that difference without failing the Java/Rust execution checks. See
[SEMANTICS.md](SEMANTICS.md#public-filter-sanitization) for source history and limits.

After running, retain evidence without editing its fields:

```sh
mkdir -p dev/scan-reports/sanitization-evidence
cp /tmp/iceberg-filter-sanitization/manifest.json /tmp/iceberg-filter-sanitization/java.json dev/scan-reports/sanitization-evidence/
```

This check validates the public sanitizer and delivery seam separately; it does
not rerun the 39 shared-file planning cases on the public follow-up.
