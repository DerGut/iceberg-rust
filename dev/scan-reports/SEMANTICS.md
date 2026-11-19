# Planning-counter scope

The production change instruments the existing planner. It adds no public API,
reporter, timer, cancellation, worker joining, or additional pruning rules.
Every invocation receives a fresh counter set, including COW planning.

Java references below are pinned to
[`4e61757dcf568051a13bb8d4c00ef57c9f248947`](https://github.com/apache/iceberg/tree/4e61757dcf568051a13bb8d4c00ef57c9f248947).

- `total_data_manifests` and `total_delete_manifests` count entries in the selected
  snapshot's manifest list before selection. Java records the corresponding
  snapshot manifest totals in `DataTableScan.doPlanFiles`.
- `scanned_data_manifests` and `scanned_delete_manifests` increment when processing
  starts, before fetching from the manifest cache. They count attempts, including
  cache hits and failed reads, rather than storage requests. Java's traversal
  sites are `ManifestGroup.entries` and `DeleteFileIndex.Builder.loadDeleteFiles`.
- `skipped_data_manifests` and `skipped_delete_manifests` count rejection by
  partition predicates or known zero live-entry counts. Unknown counts retain
  existing conservative planning. Java uses the corresponding selection gates
  in those same traversal methods. Rust selection is eager; Java can be lazy.
- `skipped_data_files` counts live entries rejected by partition or inclusive
  column-statistics evaluation. `skipped_delete_files` counts live delete entries
  rejected by partition evaluation. Dead entries, files inside rejected manifests,
  sequence-inapplicable deletes and unmatched deletes are not counted as skipped
  files. Java additionally evaluates delete bounds in `ManifestReader`; that
  existing planner difference is retained in `writer-equality-pruning`.
- `indexed_delete_files` counts entries successfully sent to index population,
  with `equality_delete_files`, `positional_delete_files` and `dvs` distinguishing
  equality content, ordinary position content and Puffin position content.
  Acceptance does not certify successful index construction. A delete can be
  indexed without attaching to any selected data file. Java classifies entries in
  [`ScanMetricsUtil.indexedDeleteFile`](https://github.com/apache/iceberg/blob/4e61757dcf568051a13bb8d4c00ef57c9f248947/core/src/main/java/org/apache/iceberg/metrics/ScanMetricsUtil.java).
- `result_data_files` and `total_file_size_in_bytes` increment after task
  construction, before delivery to the consumer. Bytes describe the full planned
  file, not bytes read. `result_delete_files` and
  `total_delete_file_size_in_bytes` sum attachments across those tasks, including
  repeated use of one physical delete file. Ordinary deletes use physical size;
  DVs use content size. The implementation performs no size-discovery I/O; an
  unavailable optional size contributes zero. Java records these four values in
  [`ScanMetricsUtil.fileTask`](https://github.com/apache/iceberg/blob/4e61757dcf568051a13bb8d4c00ef57c9f248947/core/src/main/java/org/apache/iceberg/metrics/ScanMetricsUtil.java),
  using `ScanTaskUtil.contentSizeInBytes` for attached delete bytes.

The two byte metrics use Java's `bytes` unit; the other fourteen use `count`, as
specified by [`ScanMetrics`](https://github.com/apache/iceberg/blob/4e61757dcf568051a13bb8d4c00ef57c9f248947/core/src/main/java/org/apache/iceberg/metrics/ScanMetrics.java).
Rust's private counters have no runtime unit tags.

Successful EOF lets the tests observe completed counter-producing work through
the existing channel lifecycle. This is not a new worker-completion API. Early
consumer termination can leave detached workers updating counters, and individual
atomic loads do not form a coherent in-progress transaction. The validation-only
exporter records that checkpoint and a second observation after counter-producer
ownership has been released. Neither is a production finalized report.

`writer-equality-association` retains another existing planner difference:
[`DeleteFileIndex.canContainEqDeletesForFile`](https://github.com/apache/iceberg/blob/4e61757dcf568051a13bb8d4c00ef57c9f248947/core/src/main/java/org/apache/iceberg/DeleteFileIndex.java)
uses bounds in Java; Rust currently associates by partition/sequence. Partial
`close-after-one` cases can differ after workers settle because Rust has built a
second buffered task. A consumer-stop observation can still happen to match
Java's one-task count. Both observations remain in the evidence.

No numeric counter category is postponed. Planning duration, stable partial/error
finalization, cancellation and general worker-failure propagation are deferred.
Report payload and delivery fields, Arrow row-reading counters, and performance
are outside this comparison's scope.
