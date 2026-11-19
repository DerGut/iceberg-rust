# Executed Java comparison

16 planning counters plus exact file/delete plans; no reporter API is added.

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
  - `result_data_files`: Java `1`, Rust `2` (scheduling).
  - `result_data_files.after_workers_stop`: Java `1`, Rust `2` (scheduling).
  - `total_file_size_in_bytes`: Java `2988`, Rust `5976` (scheduling).
  - `total_file_size_in_bytes.after_workers_stop`: Java `2988`, Rust `5976` (scheduling).
- `close-after-last`: equal (no numeric/plan differences).
- `manifest-read-failure`: equal (no numeric/plan differences).
- `cow-close-after-one`: different (scheduling).
  - `result_data_files.after_workers_stop`: Java `1`, Rust `2` (scheduling).
  - `total_file_size_in_bytes.after_workers_stop`: Java `2988`, Rust `5976` (scheduling).
- `cow-close-after-last`: equal (no numeric/plan differences).
- `cow-manifest-read-failure`: equal (no numeric/plan differences).

## Unexplained differences: 0


Lifecycle equality is observational, not a finalization guarantee. Differences at either checkpoint are retained in comparison.json.
