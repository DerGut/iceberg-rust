# Executed Java comparison

Public callback payloads and lifecycle for 39 cases; 16 counters plus exact file/delete plans.

**Observed counter/plan agreement: 35/39 cases.**

- Fully consumed scans: **31/33 match**.
- Lifecycle observations: **4/6 match**; production finalization remains deferred.
- **4 cases differ**; each difference is explained below.

## Fully consumed matches: 31

- `unfiltered`: equal (no numeric/plan differences).
- `dead-only`: equal (no numeric/plan differences).
- `manifest-pruned`: equal (no numeric/plan differences).
- `file-pruned`: equal (no numeric/plan differences).
- `all-pruned`: equal (no numeric/plan differences).
- `projection`: equal (no numeric/plan differences).
- `current-schema`: equal (no numeric/plan differences).
- `time-travel`: equal (no numeric/plan differences).
- `unmatched-delete`: equal (no numeric/plan differences).
- `shared-position`: equal (no numeric/plan differences).
- `path-position`: equal (no numeric/plan differences).
- `equality`: equal (no numeric/plan differences).
- `dv`: equal (no numeric/plan differences).
- `dv-supersession`: equal (no numeric/plan differences).
- `delete-manifest-pruned`: equal (no numeric/plan differences).
- `writer-unfiltered`: equal (no numeric/plan differences).
- `writer-file-pruned`: equal (no numeric/plan differences).
- `writer-all-pruned`: equal (no numeric/plan differences).
- `nested-all`: equal (no numeric/plan differences).
- `writer-projection-filter`: equal (no numeric/plan differences).
- `writer-empty-projection-filter`: equal (no numeric/plan differences).
- `nested-projection-filter`: equal (no numeric/plan differences).
- `nested-filter-only`: equal (no numeric/plan differences).
- `cow-writer-projection-filter`: equal (no numeric/plan differences).
- `cow-nested-projection-filter`: equal (no numeric/plan differences).
- `metadata-projection`: equal (no numeric/plan differences).
- `cow-writer-unfiltered`: equal (no numeric/plan differences).
- `cow-writer-file-pruned`: equal (no numeric/plan differences).
- `cow-writer-all-pruned`: equal (no numeric/plan differences).
- `cow-shared-position`: equal (no numeric/plan differences).
- `cow-dv`: equal (no numeric/plan differences).

## Planner differences: 2

- `writer-equality-pruning`: different (planner-behavior).
  - `equality_delete_files`: Java `1`, Rust `2` (planner-behavior).
  - `indexed_delete_files`: Java `1`, Rust `2` (planner-behavior).
  - `result_delete_files`: Java `1`, Rust `2` (planner-behavior).
  - `skipped_delete_files`: Java `1`, Rust `0` (planner-behavior).
  - `total_delete_file_size_in_bytes`: Java `558`, Rust `1116` (planner-behavior).
- `writer-equality-association`: different (planner-behavior).
  - `result_delete_files`: Java `1`, Rust `4` (planner-behavior).
  - `total_delete_file_size_in_bytes`: Java `558`, Rust `2232` (planner-behavior).

## Lifecycle observations (production finalization deferred): 6

- `close-after-one`: different (scheduling).
  - `result_data_files.after_workers_stop`: Java `1`, Rust `2` (scheduling).
  - `total_file_size_in_bytes.after_workers_stop`: Java `2988`, Rust `5976` (scheduling).
- `close-after-last`: equal (no numeric/plan differences).
- `manifest-read-failure`: equal (no numeric/plan differences).
- `cow-close-after-one`: different (scheduling).
  - `result_data_files.after_workers_stop`: Java `1`, Rust `2` (scheduling).
  - `total_file_size_in_bytes.after_workers_stop`: Java `2988`, Rust `5976` (scheduling).
- `cow-close-after-last`: equal (no numeric/plan differences).
- `cow-manifest-read-failure`: equal (no numeric/plan differences).

## Unexplained differences: 0


## Reporter validation

33 completed scans must emit exactly one report; six early-drop/error scans must emit none.
All callback counters must equal the completed private-counter capture. Identity, schema, filter, projection, metadata and duration bounds are checked.
Java projection/schema, filter sanitization, metadata and partial-report differences are checked against explicit expectations and retained below.

- `unfiltered`: 1/1 reports; Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `dead-only`: 1/1 reports; Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `manifest-pruned`: 1/1 reports; Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `file-pruned`: 1/1 reports; Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `all-pruned`: 1/1 reports; Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `projection`: 1/1 reports; Java reports the projected schema ID (0); Rust reports the selected source schema ID. Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `current-schema`: 1/1 reports; Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `time-travel`: 1/1 reports; Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `unmatched-delete`: 1/1 reports; Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `shared-position`: 1/1 reports; Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `path-position`: 1/1 reports; Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `equality`: 1/1 reports; Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `dv`: 1/1 reports; Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `dv-supersession`: 1/1 reports; Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `delete-manifest-pruned`: 1/1 reports; Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `writer-unfiltered`: 1/1 reports; Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `writer-file-pruned`: 1/1 reports; Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `writer-all-pruned`: 1/1 reports; Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `writer-equality-pruning`: 1/1 reports; Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `writer-equality-association`: 1/1 reports; Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `nested-all`: 1/1 reports; Java expands nested fields and includes filter-only fields; Rust lists requested source fields. Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `writer-projection-filter`: 1/1 reports; Java expands nested fields and includes filter-only fields; Rust lists requested source fields. Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `writer-empty-projection-filter`: 1/1 reports; Java reports the projected schema ID (0); Rust reports the selected source schema ID. Java expands nested fields and includes filter-only fields; Rust lists requested source fields. Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `nested-projection-filter`: 1/1 reports; Java reports the projected schema ID (0); Rust reports the selected source schema ID. Java expands nested fields and includes filter-only fields; Rust lists requested source fields. Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `nested-filter-only`: 1/1 reports; Java expands nested fields and includes filter-only fields; Rust lists requested source fields. Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `cow-writer-projection-filter`: 1/1 reports; Java expands nested fields and includes filter-only fields; Rust lists requested source fields. Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `cow-nested-projection-filter`: 1/1 reports; Java reports the projected schema ID (0); Rust reports the selected source schema ID. Java expands nested fields and includes filter-only fields; Rust lists requested source fields. Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `metadata-projection`: 1/1 reports; Java reports the projected schema ID (0); Rust reports the selected source schema ID. Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `close-after-one`: 0/0 reports; Rust emits no report for early drop or observed planning failure; Java reports on close.
- `close-after-last`: 0/0 reports; Rust emits no report for early drop or observed planning failure; Java reports on close.
- `manifest-read-failure`: 0/0 reports; Rust emits no report for early drop or observed planning failure; Java reports on close.
- `cow-writer-unfiltered`: 1/1 reports; Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `cow-writer-file-pruned`: 1/1 reports; Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `cow-writer-all-pruned`: 1/1 reports; Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `cow-shared-position`: 1/1 reports; Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `cow-dv`: 1/1 reports; Java serializes sanitized filters; Rust exposes the original optional predicate. Java includes iceberg-version metadata; Rust currently leaves metadata empty.
- `cow-close-after-one`: 0/0 reports; Rust emits no report for early drop or observed planning failure; Java reports on close.
- `cow-close-after-last`: 0/0 reports; Rust emits no report for early drop or observed planning failure; Java reports on close.
- `cow-manifest-read-failure`: 0/0 reports; Rust emits no report for early drop or observed planning failure; Java reports on close.

Lifecycle counter equality is observational, not a production finalization guarantee.
