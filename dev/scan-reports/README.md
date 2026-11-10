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
PR #1 has three commits on the parent branch: `a5dc8d131` (worker lifecycle),
`fead59434` (counting), and `b5769a3d2` (reporting). The lifecycle commit includes
controlled manifest-read failure and cancellation tests. The reporting commit
checks that delayed I/O and partial consumption cannot produce a completed report.
The [evidence manifest](evidence/manifest.json) pins the executed Rust revision,
Java revision, runtime versions, and command.

Run from the repository root with JDK 17 or 21, Python 3, curl, and the repository
Rust toolchain installed:

```sh
bash dev/scan-reports/run.sh /tmp/iceberg-scan-reports
```

The script records HEAD, the working-tree patch, and toolchain versions, downloads
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

The writer cases and `nested-all` use `ParquetWriterBuilder` to produce files and
manifest statistics from the same record batches. They construct snapshots with
manifest writers; this is not an append/row-delta transaction test. Older fixtures
materialize missing files and physical sizes but retain synthetic statistics, so
those cases establish planner agreement rather than row-level correctness.

For time travel Java explicitly projects the snapshot schema to match Rust's
existing behavior. For metadata columns Java uses an explicit projection schema:
its `select()` resolves only table columns. Both implementations use the same
resolved names/IDs, source schema ID, snapshot, filter, and case sensitivity in
that case. Neither adaptation discards a report mismatch.

Only these representation conversions are made:

- Snake-case Rust field names map to Java's hyphenated metric names.
- Plan order, delete order, and paired projected IDs/names are sorted. Paths,
  sizes, delete content types, and DV offsets/sizes are compared exactly.
- Rust predicate enums map to Java expression JSON for the fixture operators.
  Raw Java scan filters are captured and compared exactly. Java's sanitized
  report filters are checked separately against the expected digit-count mask.
- Retained evidence replaces each fixture's temporary table root with
  `<fixture-root>` and the metadata JSON path with `<metadata-file>`. Comparison itself uses exact absolute paths before this step.
- Durations are not compared numerically: Java nanoseconds/count=1 and Rust
  Duration are checked, and Rust regression tests exercise the EOF boundary.

Absent metrics use an explicit `ABSENT` marker, never zero. Java environment/options
metadata has no Rust counterpart in PR 1 and is listed as unsupported report data.

## Results and limits

Eighteen fixtures match all compared fields, counters, and file/delete plans.
Four fixtures retain differences:

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
partial-close/reused-scan lifecycle differs deliberately; it is source-audited,
not exercised by this full-consumption differential harness.

Rust tests use a manifest reader controlled by explicit notifications to block,
fail, or cancel I/O at concurrency one. They observe the blocked read being dropped,
bound all waits, and check completed reports after successful consumption only.
Repeated/concurrent planning, no-snapshot tables, and early stream drop have
separate regression coverage. These checks do not establish cancellation latency
for a network storage implementation.

Retained [comparison](evidence/comparison.json), [Rust captures](evidence/rust.json),
[Java captures](evidence/java.json), and [input hashes](evidence/inputs.json) are
machine-readable. Archive a run after comparison with:

```sh
python3 dev/scan-reports/archive.py /tmp/iceberg-scan-reports /tmp/scan-evidence
```

The optional `benchmark_report_collection` test and [historical samples](evidence/benchmark.json)
retain their own revision pin. They compare reporting enabled/disabled on a tiny
warm-cache plan, not competing collector designs. They are not a performance
acceptance criterion. Per-worker local counters merged after joins are a plausible
alternative to shared atomics if profiling demonstrates contention; no such
contention or faster alternative has been established here.
