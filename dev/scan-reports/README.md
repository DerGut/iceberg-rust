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
PR #1 has three commits on the parent branch:
`e8b42cc9a` (join-handle lifecycle), `e399fa9ad` (counting), and
`59bef006a` (reporting). The final production tree is unchanged by this split.
The [evidence manifest](evidence/manifest.json) records the implementation commit and runtime versions.

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

**Expected exit status: 1**, with only the `projection/schema_id` mismatch explained
below. Any additional mismatch requires investigation. The comparator never
accepts a mismatch silently. It writes `results/comparison.json`, including each
fixture, metric, Java value, Rust value, and explanation.

## Inputs and comparisons

Rust creates fixtures once with `TableTestFixture`, materializes missing synthetic
Parquet/Puffin files, and writes their actual physical sizes back to manifests.
Both runtimes read the exact same metadata JSON and manifest lists, manifests,
and data/delete paths. Java does not generate another table. Neither planner reads
rows. Existing fixtures use synthetic statistics; this validates planning and
reporting, not the consistency of row contents with every supplied statistic.
The recorded input inventory hashes the shared physical files.

Cases cover unfiltered live/dead entries, dead-only manifests, manifest pruning,
file pruning, all live files pruned, case-insensitive partial projection, current
schema versus explicit time travel, unmatched path-indexed position deletes,
a positional delete attached to ten tasks, path-scoped position deletes, equality
deletes across partition type promotion, and a v3 DV with a content range smaller
than the Puffin file. The all-pruned case scans a manifest whose bounds contain
200, but only its dead entry has that partition; the two live entries are skipped.
For time travel Java explicitly projects the snapshot schema to match Rust's
existing contract. Both use the current snapshot for the other cases.

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

The captured thirteen fixtures have matching counters and file/delete plans.
Twelve have matching deterministic report fields. Partial projection differs:
Java `BaseScan.lazyColumnProjection` calls `TypeUtil.project`, constructing a schema
with default ID 0; Rust retains the actual source schema ID 1. This mismatch is
preserved in the machine-readable result. The proposed Rust contract retains
source identity rather than introducing a fabricated schema ID for projections.
See [SEMANTICS.md](SEMANTICS.md) for the producers and lifecycle differences.

This is **not a general Java parity claim**. Nested leaf selection is unsupported
by Rust. Nested select-all, metadata-column projections, deletion vectors that
supersede older positional deletes, Java's additional delete row-metric and
sequence pruning, and arbitrary filter expressions are outside this differential
fixture set. No partial/failed Java report is relabeled as a completed Rust report.
Java reports on close (even partial close); Rust reports only after full successful
consumption and joins. Reusing Java scan objects can accumulate counters; Rust
invocations always own fresh counters. The differential harness creates fresh Java
scans; repeated/concurrent Rust behavior is checked separately.

## Checks and overhead

```sh
cargo test -p iceberg --lib --all-features
cargo clippy -p iceberg --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
make generate-public-api
git diff -- '**/public-api.txt'
cargo test -p iceberg --lib --no-default-features \
  scan::tests::benchmark_report_collection -- --ignored --exact --nocapture
```

Public API generation produced no diff. The warm-cache debug benchmark alternates
20 batches of 100 invocations, with ten data files and one shared delete, tracing
disabled. With the private reporter abstraction, medians were 61,902 ns without
reporting and 63,049 ns with the logging reporter (+1.85%). The measured
path includes reporter construction, collection, finalization, and delivery to a
disabled tracing event. This small, noisy observation does not establish a stable
overhead or speedup. Both paths retain worker joins, so this does not measure the
lifecycle ownership change against upstream's detached tasks. No large-scale or
release-build benchmark was run.

Retained [comparison](evidence/comparison.json), [Rust captures](evidence/rust.json),
[Java captures](evidence/java.json), [input hashes](evidence/inputs.json), and
[benchmark samples](evidence/benchmark.json) are machine-readable. Archive a fresh
run after comparison with:

```sh
python3 dev/scan-reports/archive.py /tmp/iceberg-scan-reports /tmp/scan-evidence
```

Reporter-refactor validation: 2,157 library tests passed with all features on PR #1
(one pre-existing ignored test). All 118 scan tests passed on the validation branch;
the ignored differential exporter and benchmark were run separately. All-target/
all-feature Clippy and formatting passed on both branches. Public API generation
on PR #1 produced no diff. The worker-lifecycle prerequisite commit independently
passed all 106 scan tests before the reporting commit was added. The end-to-end
harness ran on the clean revision recorded in the evidence and returned the
documented nonzero comparison result, not an execution failure.

The counting-only commit independently passed 109 scan tests, all-target/all-feature
Clippy, and formatting. The last change only split commit history: the original
Java execution pins are retained in the evidence manifest, alongside the rewritten
source revision and proof that their Git trees are identical. The Java run was not
repeated for this history-only change.
