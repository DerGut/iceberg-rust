# Completed scan-report validation

This validation-only branch preserves `scan-planning-counters-java` and merges
PR #3736 at `dcf9dfb44baf20a9bdc3ff9e1c26cd7d89444181`.
It exercises actual `MetricsReporter` callbacks as well as the inherited
private-counter and exact file/delete-plan comparison.

The shared-file matrix has **33 fully consumed scans**, each required to emit
exactly one report, and **six early-drop/error scans**, each required to emit
none. Both normal and COW planning are covered. Callback values are captured
through public report getters, then checked against the requested scan and
private counters. The exporter retains private planning entry points to observe
consumer-stop and settled counters; the separate REST tests exercise public
`plan_files()`, `to_arrow()` and `CowRewriteBuilder` entry points.

See [executed results](evidence/RESULTS.md),
[all comparisons and report-contract differences](evidence/comparison.json),
and [revisions, working-tree patch, checks and log hashes](evidence/manifest.json).
The archived patch identifies the validation code executed on the recorded
revision; generating this evidence does not constitute another validation run.

## What is checked

The 39 shared-file scenarios cover pruning, dead entries, schema evolution and
time travel, empty and nested projections, metadata columns, writer-generated
statistics, equality and positional deletes, DVs, COW planning, early drop and
manifest-read failures. Rust creates each table once; Java reads those exact
metadata, manifest, Parquet and Puffin files.

For completed scans the comparator checks:

- All 16 callback counters against the private-counter capture and Java's
  count/byte metrics; exact file/delete plans and four counters derived from
  those plans are independently checked.
- Table identity, snapshot/schema IDs, filter, projection IDs/names and empty
  metadata against explicit expectations for the requested scan.
- Projection list lengths before pairing IDs with names, on both runtimes.
- Positive duration bounded by the enclosing capture, and Java timer units.
- Exact report cardinality; missing or duplicate reports fail validation.

The inherited counter/plan result is **35/39 observed matches**: 31/33 complete
scans and 4/6 lifecycle observations. The two complete differences are existing
Java equality-delete bounds pruning/association differences. Two early-close
counter differences are concurrent read-ahead. These remain visible and are
accepted only with their expected plan shapes and counter invariants.

Full report parity is not claimed. Explicit expectations retain these contract
differences:

- Rust lists requested source fields; Java expands nested fields and includes
  filter-only fields.
- Rust reports the selected source schema ID; Java can report a projected schema
  ID of zero.
- Rust exposes an optional original predicate; Java serializes a sanitized
  expression. Both are checked against the requested filter separately.
- Rust metadata is empty; Java includes `iceberg-version`.
- Rust omits reports for early drop and observed planning errors; Java reports
  on close.

The **63 negative controls** corrupt every private counter and every callback
counter, identity/schema/filter/metadata fields, duration bounds, projection
IDs/names and list lengths, report cardinality, plans and fixture membership.
They also corrupt Java fields inside the explained projection/filter cases, so
those explanations cannot conceal arbitrary differences.

## Public API integration

`crates/integration_tests/tests/completed_scan_reports.rs` creates its own
namespaces and real Parquet files using the REST catalog and S3-compatible
object store. Three tests cover:

- Identical rows with reporting disabled/enabled, selective Arrow reads,
  inherited and overridden reporters, concurrent reuse of a scan, and a
  no-change public COW rewrite.
- No reports after dropping before any task, after one task, or after the last
  task without polling EOF. A gated callback demonstrates that planning can
  finish while delivery waits; measured duration includes a consumer pause and
  ends before delivery is released. An injected callback panic leaves subsequent
  reads usable.
- No reports for snapshotless tables, manifest stream errors, or manifest-list
  setup errors.

Positive delivery waits and negative channel-closure checks have timeouts.
A report describes planning, not row-read success: Arrow can finish planning
before row consumption ends.

## Run

Requirements: the repository Rust toolchain, JDK 17 or 21, Python 3, curl and tar.
Docker is needed for REST integration. Java is pinned to
`4e61757dcf568051a13bb8d4c00ef57c9f248947` and built with its Gradle wrapper.
Use a fresh absolute output directory.

```sh
docker compose -p completed-scan-reports -f dev/docker-compose.yaml up -d --wait object-store rest
docker compose -p completed-scan-reports -f dev/docker-compose.yaml run --rm create-buckets
SCAN_REPORT_RUN_REST=1 bash dev/scan-reports/run.sh /absolute/path/to/fresh-output
python3 dev/scan-reports/archive.py /absolute/path/to/fresh-output dev/scan-reports/evidence
docker compose -p completed-scan-reports -f dev/docker-compose.yaml down --volumes
```

The runner executes formatting, core tests, Clippy, Rust/Java captures and
comparator controls. `SCAN_REPORT_RUN_REST=1` adds the public API integration
suite and its Clippy check against already-running services. Without it, the
REST tests are not executed and do not appear in the evidence's check list.

Exit 0 means all compared counters/plans agree; exit 1 retains explained
counter/plan differences; exit 2 identifies unexpected differences. Any report
contract violation is unexpected. Build/capture failures fail independently.

## Limits

No performance claim is made. General planning-worker panic propagation,
production worker cancellation, partial/error finalization, runtime shutdown
and durable callback delivery are not established. Empty-snapshot behavior
continues to have production unit coverage; the REST empty-table case is
snapshotless. Synthetic fixtures establish planning behavior; the REST suite
adds row execution on its own data, not a cross-engine row-correctness matrix.
EOF and private-counter settlement do not certify all background work succeeded.
