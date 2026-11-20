<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Scan-planning metrics bookkeeping benchmark

This standalone microbenchmark accompanies the [nullability discussion on
PR #3709](https://github.com/apache/iceberg-rust/pull/3709#discussion_r4637222511).
It estimates the cost of collecting Rust scan-planning counters when no
reporter consumes them. It does not execute `TableScan::plan_files()` or measure
Java instrumentation.

## Reproduce

From the repository root, using the Rust toolchain in `rust-toolchain.toml`:

```sh
benchmark_tmp="$(mktemp -d)"
rustc --edition=2024 -C opt-level=3 -C target-cpu=native \
  dev/scan-metrics-overhead/bench.rs -o "$benchmark_tmp/bench"
"$benchmark_tmp/bench"
```

The executable prints CSV with worker count, delete-attachment mode, measurement
mode, and median/minimum/maximum wall-clock nanoseconds per file. No Cargo build,
external crates, network access, or table fixture is required to run it once the
Rust toolchain is installed.

## What is measured

The model shares one `Arc` containing 16 adjacent `AtomicU64` counters. Updates
use `Ordering::Relaxed`, as in the planning-counter implementation. Each
iteration represents an accepted data file: two additions record its count and
bytes. With deletes enabled, two more additions record attachment count and
bytes. Delete attachment bytes are supplied directly, without traversing a list.

The four modes execute the same synthetic input/checksum work:

- `Baseline`: no instrumentation.
- `Disabled`: an opaque `Option` containing no counters, checked before updates.
- `CountersOnly`: two or four shared atomic additions per file.
- `Current`: the same additions plus one `Arc` clone/drop per file.

`Current` names a bookkeeping model, not a call into the actual planner. The
real planner performs other work and can clone counter references at additional
context boundaries. The disabled mode still creates the shared test allocation
and worker references outside the timed loop; it measures the disabled hot path,
not scan setup or allocation savings.

Each sample processes 2,000,000 files in total, split evenly across 1, 4, or 8
workers sharing the counters. Eight rounds rotate the four modes' execution
order. The first round is discarded, leaving seven measurements per case.
Thread creation is outside the timed interval; releasing the start barrier and
joining workers are included. Count assertions run after timing. Inputs and
checksums use `black_box`.

Reported time is elapsed wall-clock time divided by total files across all
workers, not CPU time or individual request latency. Numerically, ns/file also
equals milliseconds per million files.

## Recorded results

[results.json](results.json) preserves the original run from 2026-11-20 on an
arm64 host with `rustc 1.97.0-nightly (e8e4541ff 2026-04-15)`, compiled using the
flags above. The source is the original benchmark with an ASF license header
and rustfmt formatting applied.

- One worker: counter additions took 1.8–2.8 ns/file; including one `Arc`
  clone/drop took 3.6–4.3 ns/file.
- Eight workers: shared counter additions took 44–88 ns/file; including the
  `Arc` operation took 111–171 ns/file.
- Disabled collection stayed near the baseline in this model.

The ranges above span the no-delete and delete-enabled medians, not confidence
intervals. The JSON contains each case's minimum and maximum as well.

## Limits of the result

Workers continuously contend on the same counters and reference count. This is
a contention stress case; real scans interleave updates with manifest I/O,
Avro decoding, pruning, task construction and async scheduling. None of those
costs, report construction/delivery, or scan-level timer costs are measured.
Only one machine/compiler configuration was measured.

The results quantify isolated bookkeeping costs. They do not establish an
end-to-end scan slowdown, demonstrate the benefit of a production patch, or
show that changing metric getters to `Option` is necessary. Disabling collection
for an absent reporter can be independent of the public report value types.
