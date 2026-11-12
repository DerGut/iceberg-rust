# Java differential validation

This validation branch is stacked on
[`scan-reports-1-collection`](https://github.com/DerGut/iceberg-rust/tree/scan-reports-1-collection).
Only the parent branch is intended for PR #1; this branch retains reproducible
review evidence and the Java harness separately. It contains no PR-description draft.

This harness is an ignored crate unit test plus a Java test copied into a pinned
Iceberg source build. It uses no additional Rust dependencies or public API.
The private recording reporter implements the same `MetricsReporter` trait as
production logging. Tests inject it through the private planning method, with no
test-only field on `TableScan`. Reporter configuration is not exposed publicly.

Java: `4e61757dcf568051a13bb8d4c00ef57c9f248947`.
Rust upstream baseline: `63d3d522c44aec588ebb7207834a37a141c3837a`.
PR #1 has three commits on the parent branch: `420239f78` (planner behavior),
`548ec0f21` (counting), and `e64996302` (reporting). The planner prerequisite commit unifies worker results and completion, with controlled manifest-read failure and
cancellation tests, unused delete-index failures, and dead-only manifest skipping
for both scan and COW planning. The counting commit instruments both paths without
optional counters. The reporting commit
delivers reports after contributing workers stop, including COW, partial, and failed planning, restoring the originating tracing dispatcher for delivery.
Reports have no outcome tag and do not certify successful or exhaustive planning.
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
an equivalent table independently. Both planners fully consume tasks without
reading rows. The input inventory hashes the shared files.

The 22 cases cover:

- `unfiltered`, `dead-only`, `manifest-pruned`, `file-pruned`, `all-pruned`: live and
  dead entries, manifest exclusion, and zero-result scans.
- `projection`, `current-schema`, `time-travel`, `nested-all`,
  `metadata-projection`: case-insensitive selection, schema evolution, nested
  select-all, and `_file`/`_pos`/`_spec_id` alongside a data column.
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

Eight `cow-` fixtures additionally exercise COW planning. Five fully consume
`writer-unfiltered`, `writer-file-pruned`, `writer-all-pruned`, `shared-position`,
and `dv`; three exercise `close-after-one`, `close-after-last`, and
`manifest-read-failure`. Rust uses its COW planning entry point. Java constructs
a batch scan with `ignoreResiduals()`, as Spark's COW scan builder does, matching
the snapshot, filter, case sensitivity, and projection. Reports retain the
selection filter; the captures separately verify that COW tasks clear their row
predicate (Rust) or have an always-true residual (Java). These are planning tests,
not Spark execution, row reading, rewriting, or commit tests.

The writer cases and `nested-all` use `ParquetWriterBuilder` to produce files and
manifest statistics from the same record batches. They construct snapshots with
manifest writers; this is not an append/row-delta transaction test. Older fixtures
materialize missing files and physical sizes but retain synthetic statistics, so
those cases establish planner agreement rather than row-level correctness.

For time travel both implementations select the snapshot schema: Java's
`DataTableScan.useSnapshotSchema()` enables this in `SnapshotScan.useSnapshot()`.
The Java fixture calls `useSnapshot()` without an explicit projection.
For metadata columns Java uses an explicit projection schema:
its `select()` resolves only table columns. Both implementations use the same
resolved names/IDs, source schema ID, snapshot, filter, and case sensitivity in
that case. This adaptation does not discard a report mismatch.
Java receives its table name from the fixture identifier, independently of the
Rust report. Negative controls independently corrupt each component of the Rust
report identifier, a counter, and the plan and require detection of each change.

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

Of the original 22 fully consumed fixtures, eighteen match all compared fields,
counters, and file/delete plans. Four fixtures retain differences:

- `projection`: Java's partial schema has ID 0; Rust retains source schema ID 1.
- `nested-all`: Java reports the struct and child field IDs; Rust reports the
  resolved top-level task fields. Plans and counters match. Whether Rust should
  expand report IDs to Java's nested representation remains a review decision.
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
Rust; nested list/map reports, delete sequence pruning, partition-spec evolution
with mixed deletes, and v1 tables remain outside the executed matrix. Java's
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
consumption and cancellation, for both normal and COW planning. These checks do not establish cancellation latency
for a network storage implementation.

The five fully consumed COW fixtures also match, giving 23 matches among 27
fully consumed cases. Six lifecycle fixtures exercise normal and COW planning;
four match the compared counters and plans in this run. Both `close-after-one`
and `cow-close-after-one` count one planned file in Java and two in Rust (3,003
versus 6,006 bytes), giving 27 matches and six differences across all 33 cases.
Reports are delivered in all six lifecycle cases, including failures.

Partial counts depend on worker scheduling. The normal `close-after-one` case
matched in the
[previous pinned run](https://github.com/DerGut/iceberg-rust/blob/1635f5b3c6afdc15b78705992fafcc17900b1168/dev/scan-reports/evidence/comparison.json),
before the worker-error and scoped-subscriber fixes.
Both observations and their pinned provenance remain available; neither the
normal nor COW cases establish exact partial-counter parity. The following
lifecycle cases run in both modes:

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
