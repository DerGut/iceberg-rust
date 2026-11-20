#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Compare real counters and plans without suppressing explained differences."""
import json
from pathlib import Path
import sys

METRICS = {
    "total_data_manifests", "total_delete_manifests", "skipped_data_manifests",
    "skipped_delete_manifests", "scanned_data_manifests", "scanned_delete_manifests",
    "result_data_files", "skipped_data_files", "total_file_size_in_bytes",
    "result_delete_files", "skipped_delete_files", "total_delete_file_size_in_bytes",
    "indexed_delete_files", "equality_delete_files", "positional_delete_files", "dvs",
}
PLANNER_FIELDS = {
    "writer-equality-pruning": {
        "equality_delete_files", "indexed_delete_files", "result_delete_files",
        "skipped_delete_files", "total_delete_file_size_in_bytes", "plans",
    },
    "writer-equality-association": {
        "result_delete_files", "total_delete_file_size_in_bytes", "plans",
    },
}
EXPLANATIONS = {
    "writer-equality-pruning": (
        "Java prunes eq-99.parquet with writer-produced bounds for y < 20. "
        "Rust delete-entry pruning checks partitions only. The counters reflect different plans."
    ),
    "writer-equality-association": (
        "Java uses bounds when associating equality deletes with data files. "
        "Rust uses partition and sequence checks, producing four attachments versus Java's one."
    ),
}


def indexed(cases):
    result = {case["fixture"]: case for case in cases}
    assert len(result) == len(cases), "duplicate fixture names"
    return result


def plans(items):
    return sorted([
        dict(p, deletes=sorted(p["deletes"], key=lambda d: json.dumps(d, sort_keys=True)))
        for p in items
    ], key=lambda p: p["path"])


def expected_planner_delta(name, rust, java):
    if name not in PLANNER_FIELDS:
        return False
    if plans([dict(p, deletes=[]) for p in rust]) != plans([dict(p, deletes=[]) for p in java]):
        return False
    def attachments(items):
        return {Path(p["path"]).name: sorted(Path(d["path"]).name for d in p["deletes"]) for p in items}
    if name == "writer-equality-pruning":
        return (attachments(rust) == {"written-10.parquet": ["eq-10.parquet", "eq-99.parquet"]}
                and attachments(java) == {"written-10.parquet": ["eq-10.parquet"]})
    return (attachments(rust) == {
        "written-10.parquet": ["eq-10.parquet", "eq-99.parquet"],
        "written-50.parquet": ["eq-10.parquet", "eq-99.parquet"]}
        and attachments(java) == {"written-10.parquet": ["eq-10.parquet"], "written-50.parquet": []})


def compare(root):
    rust = indexed(json.loads((root / "rust.json").read_text()))
    java = indexed(json.loads((root / "java.json").read_text()))
    assert rust.keys() == java.keys(), "Rust/Java fixture sets differ"
    assert len(rust) == 39, "the complete inherited 39-case matrix is required"
    results = []
    report_results = []
    for name, case in rust.items():
        other = java[name]
        assert "error" not in other, (name, "Java harness failure", other.get("error"))
        lifecycle = case["consume"] is not None or case["missing_manifest"] is not None
        assert case["finalization_supported"] == (not lifecycle)
        counts, settled = case["counts"], case["settled_counts"]
        mismatches = []
        known_plan = expected_planner_delta(name, case["plans"], other["plans"])

        def check(field, expected, actual, counter=None):
            if expected == actual:
                return
            category, explanation = "unexpected", "Unexplained difference; investigate before claiming parity."
            if known_plan and field in PLANNER_FIELDS.get(name, set()):
                category, explanation = "planner-behavior", EXPLANATIONS[name]
            elif (name.removeprefix("cow-").startswith("close-")
                  and (counter or field) in {"result_data_files", "total_file_size_in_bytes"}
                  and actual != "ABSENT"):
                if settled.get(counter or field, "ABSENT") == expected:
                    category = "deferred-finalization"
                    explanation = (
                        "Rust is sampled when the consumer stops, before outstanding producers stop. "
                        "The test-only settled observation agrees; production finalization is deferred."
                    )
                else:
                    category = "scheduling"
                    explanation = (
                        "Even after fixture producers stop, Rust counts concurrent buffered work "
                        "that Java's lazy iterator did not visit. Exact partial-count parity is not promised."
                    )
            mismatch = dict(metric=field, java=expected, rust=actual,
                            category=category, explanation=explanation)
            if (counter or field) in METRICS:
                mismatch["checkpoint"] = "after-workers-stop" if counter else "consumer-stop"
                mismatch["rust_after_workers_stop"] = settled.get(counter or field, "ABSENT")
            mismatches.append(mismatch)

        reports = case.get("reports", [])
        check("report_count", 0 if lifecycle else 1, len(reports))
        report_differences = []
        if not lifecycle and len(reports) == 1:
            report = reports[0]
            expected = case["expected_report"]
            for field, value in expected.items():
                check("report." + field, value, report.get(field, "ABSENT"))
            check("report.metric_fields", sorted(METRICS), sorted(report.get("metrics", {})))
            for metric in sorted(METRICS):
                check("report." + metric, counts.get(metric, "ABSENT"),
                      report.get("metrics", {}).get(metric, "ABSENT"))
            check("report.duration_bounds", True,
                  0 < report.get("duration-ns", -1) <= case["elapsed_ns"])
            jreport = other["report"]
            for field in ["table-name", "snapshot-id"]:
                check("report.java." + field, jreport.get(field, "ABSENT"), report.get(field, "ABSENT"))
            name_without_cow = name.removeprefix("cow-")
            java_schema_zero = name_without_cow in {
                "projection", "writer-empty-projection-filter", "nested-projection-filter", "metadata-projection"
            }
            check("report.java.schema-id", 0 if java_schema_zero else expected["schema-id"],
                  jreport.get("schema-id", "ABSENT"))
            if jreport.get("schema-id") != report.get("schema-id"):
                report_differences.append("Java reports the projected schema ID (0); Rust reports the selected source schema ID.")
            rust_ids, rust_names = report.get("projected-field-ids", []), report.get("projected-field-names", [])
            java_ids, java_names = jreport.get("projected-field-ids", []), jreport.get("projected-field-names", [])
            check("report.projection_lengths", len(rust_ids), len(rust_names))
            check("report.java.projection_lengths", len(java_ids), len(java_names))
            expected_java_pairs = list(zip(expected["projected-field-ids"], expected["projected-field-names"]))
            if name_without_cow == "writer-projection-filter":
                expected_java_pairs = [(1, "x"), (2, "y"), (3, "z")]
            elif name_without_cow == "writer-empty-projection-filter":
                expected_java_pairs = [(2, "y")]
            elif name_without_cow.startswith("nested-"):
                expected_java_pairs = [(1, "x"), (2, "y"), (3, "z"), (9, "details"), (10, "details.value"),
                                       (12, "numbers.element"), (15, "records.element.value"),
                                       (17, "lookup.key"), (18, "lookup.value"), (22, "objects.value.value")]
                if name_without_cow == "nested-projection-filter":
                    expected_java_pairs = [pair for pair in expected_java_pairs if pair[0] not in {1, 3}]
            check("report.java.projection", sorted(expected_java_pairs), sorted(zip(java_ids, java_names)))
            if sorted(zip(rust_ids, rust_names)) != sorted(zip(java_ids, java_names)):
                report_differences.append("Java expands nested fields and includes filter-only fields; Rust lists requested source fields.")
            raw_filter = True
            sanitized_filter = True
            if case["threshold"] is not None or name_without_cow == "all-pruned":
                value = 200 if name_without_cow == "all-pruned" else case["threshold"]
                raw_filter = {"type": "eq" if name_without_cow == "all-pruned" else "lt",
                              "term": case["filter_column"], "value": value}
                sanitized_filter = dict(raw_filter, value=f"({len(str(value))}-digit-int)")
            check("report.java.raw_filter", raw_filter, other.get("raw_filter", "ABSENT"))
            check("report.java.filter", sanitized_filter, jreport.get("filter", "ABSENT"))
            report_differences.append("Java serializes sanitized filters; Rust exposes the original optional predicate.")
            check("report.java.metadata_keys", ["iceberg-version"], sorted(jreport.get("metadata", {})))
            check("report.java.version_present", True, bool(jreport.get("metadata", {}).get("iceberg-version")))
            report_differences.append("Java includes iceberg-version metadata; Rust currently leaves metadata empty.")
            timer = jreport.get("metrics", {}).get("total-planning-duration", {})
            check("report.java.timer", (1, "nanoseconds", True),
                  (timer.get("count"), timer.get("time-unit"), timer.get("total-duration", -1) > 0))
        elif lifecycle:
            report_differences.append("Rust emits no report for early drop or observed planning failure; Java reports on close.")
        report_results.append(dict(fixture=name, expected_report_count=0 if lifecycle else 1,
                                   actual_report_count=len(reports), contract_differences=report_differences))

        check("counter_fields", sorted(METRICS), sorted(counts))
        check("settled_counter_fields", sorted(METRICS), sorted(settled))
        jmetrics = other["report"]["metrics"]
        check("java_counter_fields", sorted(m.replace("_", "-") for m in METRICS),
              sorted(k for k in jmetrics if k != "total-planning-duration"))
        for metric in sorted(METRICS):
            jm = jmetrics.get(metric.replace("_", "-"), {})
            expected_unit = "bytes" if metric.endswith("_in_bytes") else "count"
            check(metric + ".java_unit", expected_unit, jm.get("unit", "ABSENT"))
            check(metric, jm.get("value", "ABSENT"), counts.get(metric, "ABSENT"))
            if not lifecycle:
                check(metric + ".settled", counts.get(metric, "ABSENT"), settled.get(metric, "ABSENT"))
            else:
                check(metric + ".after_workers_stop", jm.get("value", "ABSENT"),
                      settled.get(metric, "ABSENT"), counter=metric)
                check(metric + ".monotonic", True,
                      isinstance(counts.get(metric), int) and isinstance(settled.get(metric), int)
                      and 0 <= counts[metric] <= settled[metric])
        expected_failure = case["missing_manifest"] is not None
        check("rust_planning_failed", expected_failure, bool(case["planning_error"]))
        check("java_planning_failed", expected_failure, bool(other.get("planning_error")))
        if name.removeprefix("cow-").startswith("close-"):
            check("settled_result_data_files.upper_bound", True, settled.get("result_data_files", 3) <= 2)
            check("settled_data_bytes.upper_bound", True,
                  settled.get("total_file_size_in_bytes", -1) <= 2 * case["plans"][0]["size"])
        if case["cow_rewrite"]:
            check("rust_cow_residuals", True, all(p["residual_is_true"] for p in case["plans"]))
            check("java_cow_residuals", True, all(p["residual_is_true"] for p in other["plans"]))
        check("plans", plans(other["plans"]), plans(case["plans"]))
        if not lifecycle:
            for side, captured, metrics in [("rust", case["plans"], counts),
                                             ("java", other["plans"], {k.replace("-", "_"): v.get("value") for k, v in jmetrics.items()})]:
                derived = {
                    "result_data_files": len(captured),
                    "result_delete_files": sum(len(p["deletes"]) for p in captured),
                    "total_file_size_in_bytes": sum(p["size"] for p in captured),
                    "total_delete_file_size_in_bytes": sum(
                        d["content_size"] if d["content_size"] is not None else d["size"]
                        for p in captured for d in p["deletes"]),
                }
                for field, value in derived.items():
                    check(side + "." + field + ".vs_plan", value, metrics.get(field, "ABSENT"))
            if known_plan:
                expected_indexed = (2, 1) if name == "writer-equality-pruning" else (2, 2)
                for field in ["indexed_delete_files", "equality_delete_files"]:
                    check(field + ".known_rust", expected_indexed[0], counts.get(field))
                    check(field + ".known_java", expected_indexed[1], jmetrics[field.replace("_", "-")]["value"])
                if name == "writer-equality-pruning":
                    check("skipped_delete_files.known_rust", 0, counts.get("skipped_delete_files"))
                    check("skipped_delete_files.known_java", 1, jmetrics["skipped-delete-files"]["value"])
        categories = sorted({m["category"] for m in mismatches})
        status = ("unexpected-difference" if "unexpected" in categories else
                  "deferred-lifecycle" if lifecycle else
                  "planner-difference" if mismatches else "match")
        results.append(dict(
            fixture=name, status=status, comparison="different" if mismatches else "equal",
            categories=categories, mismatches=mismatches,
            guarantee=("Consumer-stop and settled checkpoints only; production finalization is deferred."
                       if lifecycle else "All numeric counters and full file/delete plan compared after EOF."),
        ))
    groups = {status: [r["fixture"] for r in results if r["status"] == status] for status in
              ["match", "planner-difference", "deferred-lifecycle", "unexpected-difference"]}
    summary = dict(
        results=results, groups=groups,
        scope="Public callback payloads and lifecycle for 39 cases; 16 counters plus exact file/delete plans.",
        reports=report_results,
        deferred=["exact cross-runtime planning-duration comparison", "production finalization after errors or early drop",
                  "worker cancellation and worker-failure propagation"],
        excluded=["row-group and page metrics", "performance guarantees"],
        normalization="Plan/delete ordering only. Absolute paths and all sizes are compared before archival normalization.",
    )
    (root / "comparison.json").write_text(json.dumps(summary, indent=2) + "\n")
    lifecycle_names = {name for name, case in rust.items()
                       if case["consume"] is not None or case["missing_manifest"] is not None}
    complete_results = [r for r in results if r["fixture"] not in lifecycle_names]
    lifecycle_results = [r for r in results if r["fixture"] in lifecycle_names]
    observed_equal = sum(r["comparison"] == "equal" for r in results)
    complete_equal = sum(r["comparison"] == "equal" for r in complete_results)
    lifecycle_equal = sum(r["comparison"] == "equal" for r in lifecycle_results)
    lines = [
        "# Executed Java comparison", "", summary["scope"], "",
        f"**Observed counter/plan agreement: {observed_equal}/{len(results)} cases.**", "",
        f"- Fully consumed scans: **{complete_equal}/{len(complete_results)} match**.",
        f"- Lifecycle observations: **{lifecycle_equal}/{len(lifecycle_results)} match**; production finalization remains deferred.",
        f"- **{len(results) - observed_equal} cases differ**; each difference is explained below.", "",
    ]
    headings = {
        "match": "Fully consumed matches",
        "planner-difference": "Planner differences",
        "deferred-lifecycle": "Lifecycle observations (production finalization deferred)",
        "unexpected-difference": "Unexplained differences",
    }
    for status, names in groups.items():
        lines += [f"## {headings[status]}: {len(names)}", ""]
        for name in names:
            result = next(r for r in results if r["fixture"] == name)
            categories = ", ".join(result["categories"]) or "no numeric/plan differences"
            lines.append(f"- `{name}`: {result['comparison']} ({categories}).")
            for mismatch in result["mismatches"]:
                if mismatch["metric"] != "plans":
                    lines.append(f"  - `{mismatch['metric']}`: Java `{mismatch['java']}`, Rust `{mismatch['rust']}` ({mismatch['category']}).")
        lines += [""]
    lines += ["## Reporter validation", "",
              "33 completed scans must emit exactly one report; six early-drop/error scans must emit none.",
              "All callback counters must equal the completed private-counter capture. Identity, schema, filter, projection, metadata and duration bounds are checked.",
              "Java projection/schema, filter sanitization, metadata and partial-report differences are checked against explicit expectations and retained below.", ""]
    for result in report_results:
        lines.append(f"- `{result['fixture']}`: {result['actual_report_count']}/{result['expected_report_count']} reports; " + " ".join(result["contract_differences"]))
    lines += ["", "Lifecycle counter equality is observational, not a production finalization guarantee.", ""]
    (root / "RESULTS.md").write_text("\n".join(lines))
    print(json.dumps({k: len(v) for k, v in groups.items()}, indent=2))
    return 2 if groups["unexpected-difference"] else int(any(r["mismatches"] for r in results))


if __name__ == "__main__":
    sys.exit(compare(Path(sys.argv[1])))
