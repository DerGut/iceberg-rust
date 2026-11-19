# Planning-counter comparison with Java

**35/39 scenarios match Java's observed planning counters and file/delete plans**,
**2/39 differ because of existing equality-delete planner behavior**, and
**2/39 differ because of scheduling when a scan is stopped early**.
There are **zero unexplained differences**.
All 39 cases execute, and all 27 comparator corruption controls pass.

See [case-by-case results](evidence/RESULTS.md),
[every differing value and its explanation](evidence/comparison.json), and the
[executed revisions and check results](evidence/manifest.json).
These comparisons cover all 16 numeric planning counters, not report payloads
or delivery guarantees.

## Mismatches

### `writer-equality-pruning`

This scenario filters data to `y < 20` and supplies two equality-delete files
containing `y = 10` and `y = 99`.
Java uses the delete files' column bounds to reject the file containing 99 and
counts one delete, while Rust checks only partitions and counts both.
Adding bounds-based delete pruning requires a separate planner follow-up beyond
this PR's counter collection; reporting integration and cancellation will not
fix this difference.

### `writer-equality-association`

This scenario provides two data files with `y` ranges of 10–11 and 50–51 and two
equality-delete files for values 10 and 99.
Java uses column bounds to attach only the delete for 10 to the first data file,
producing one attachment, while Rust's partition/sequence checks attach both
deletes to both files, producing four.
Correcting those associations requires a separate planner follow-up beyond this
PR's counter collection; reporting integration and cancellation will not fix it.

### `close-after-one`

This scenario plans a scan of two data files, consumes one file task, then drops
the result stream: Java counts one file, but Rust's concurrent workers construct
a second task before they stop.
This PR measures planning work without changing read-ahead or worker cancellation.
A cancellation/finalization follow-up can stop remaining work and freeze the
counters, but cannot undo an already constructed task, so it will not guarantee
exact agreement with Java's partial counts.

### `cow-close-after-one`

This scenario scans two data files using copy-on-write (COW) rewrite planning
and drops the stream after consuming one task; Rust's concurrent planner
constructs two tasks while Java counts one.
Changing COW read-ahead or cancellation is outside this PR's counter-collection scope.
A cancellation/finalization follow-up can establish a stable final snapshot,
but this scheduling difference may remain because buffered tasks still represent
work performed.

## Test setup

This validation-only branch builds on `scan-planning-counters`; only the
production branch is intended for the counter PR.
The harness reuses the 39 shared-file fixtures and Java exporter from
[`scan-reports-1-validation`](https://github.com/DerGut/iceberg-rust/tree/a5a4bcb9fabfcd36bda2fb789fcad19137762363/dev/scan-reports),
with Java pinned to `4e61757dcf568051a13bb8d4c00ef57c9f248947`.
Rust creates each table once, and Java reads those exact metadata, manifest,
Parquet and Puffin files.

### Run

Run from the repository root with its Rust toolchain, JDK 17 or 21, Python 3,
curl and tar:

```sh
bash dev/scan-reports/run.sh /absolute/path/to/fresh-output
python3 dev/scan-reports/archive.py /absolute/path/to/fresh-output dev/scan-reports/evidence
```

The runner executes core tests, Clippy, Rust/Java captures and comparator controls,
using the pinned Java source's Gradle wrapper.
Exit 0 means all compared values agree; exit 1 retains explained differences;
exit 2 identifies unexpected differences, while build/capture errors fail
independently.
Use a fresh output directory for each run.

### Comparison and coverage

The matrix contains 26 fully consumed normal scans, seven fully consumed COW
scans, and six early-close/error cases.
It covers manifest/file pruning, dead entries, schema/projection choices,
writer-produced statistics, shared/unmatched/path deletes, equality deletes,
DV supersession and content bytes.
The inherited synthetic fixtures establish planning behavior, not row execution
correctness; no performance claim is made.

Each captured task is compared by exact path, size, delete content type and DV
coordinates, with only task/delete ordering normalized.
Missing counters are `ABSENT`, never zero; Java's native count/byte unit tags
are checked against the metric contract without fabricating Rust unit tags.
Four result/byte counters are additionally checked against each runtime's
captured plan, and COW tasks must clear their row predicates.
[SEMANTICS.md](SEMANTICS.md) identifies producer events and Java references.

Lifecycle cases capture counters immediately when the consumer stops and again
after a test-only wait for all counter-producer owners to release their `Arc`.
Both observations are compared with Java and retained; equality at either
checkpoint does not establish production finalization or cancellation behavior.
All waits use a timeout, and this observation mechanism makes no guarantee for
blocked network I/O.

No numeric counter category is postponed.
Planning duration, stable partial/error finalization, cancellation and general
worker-failure propagation remain follow-up work; report identity, projection
metadata, filter serialization and delivery are outside this comparison.
Runtime-shutdown/panic propagation, unused invalid-index failures, variant
collections and broad v1/mixed-spec-delete parity are not established by this
Java matrix.
Production unit tests separately cover v1 unknown manifest counts, both planning
modes, concurrent/repeated calls, dead manifests, partition/statistics
rejections, delete kinds and manifest-read errors.

### Evidence and controls

The runner records both revisions, the working-tree patch, runtime versions,
and command/log hashes; the archive hashes input files and normalizes temporary
paths only after exact comparison.
Controls corrupt every counter, remove a metric, alter plan sizes and lengths,
corrupt explained planner fixtures, and remove/duplicate fixtures.
Explained classifications require the expected plan shape and counter
invariants; unrelated differences still fail and all observed differences
remain visible.

The archived production revision `327080489d63c65b60d268c3d2dd2caa7caf5e4b`
and the runner's revision `0678e0fce1816f524664301c5348569e5785d7e0` have
the identical Git tree `cc63d1c2c4e73e29245565b600d705b422dbc19c`.
Evidence retains the revisions actually executed; this equivalence does not
represent another validation run.
