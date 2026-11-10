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

"""Strict deterministic report/plan comparison; see README for normalizations."""
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
rust = json.loads((root / "rust.json").read_text())
java = {case["fixture"]: case for case in json.loads((root / "java.json").read_text())}
results = []
for case in rust:
    name = case["fixture"]
    other = java[name]
    mismatches = []

    def compare(metric, expected, actual, explanation):
        if name == "writer-equality-pruning" and metric in {
            "equality_delete_files", "indexed_delete_files", "result_delete_files",
            "skipped_delete_files", "total_delete_file_size_in_bytes", "plans"
        }:
            explanation = ("Java ManifestReader filters eq-99.parquet using y < 20 and its writer-produced bounds; "
                           "Rust process_delete_manifest_entry filters partitions only, so it indexes and attaches "
                           "both eq-10.parquet and eq-99.parquet. This is an existing planning difference, "
                           "with counters reflecting the different plans, not a normalization.")
        if name == "writer-equality-association" and metric in {
            "result_delete_files", "total_delete_file_size_in_bytes", "plans"
        }:
            explanation = ("Both index two equality deletes. Java DeleteFileIndex.canContainEqDeletes "
                           "uses bounds to attach eq-10 only to written-10 and eq-99 to neither file. "
                           "Rust uses partition and sequence checks, attaching both deletes to both files. "
                           "The report counts actual attachments (Java 1, Rust 4).")
        if name == "nested-all" and metric == "projection":
            explanation = ("Java TypeUtil.getProjectedIds includes the selected struct and its child ID; "
                           "Rust reports the resolved top-level task projection. Both plan the whole struct. "
                           "Nested report representation remains a contract decision for review.")
        if expected != actual:
            mismatches.append(dict(fixture=name, metric=metric, java=expected,
                                   rust=actual, explanation=explanation))

    r = case["report"]
    j = other.get("report", {})
    if "error" in other:
        mismatches.append(dict(fixture=name, metric="planning", java=other["error"],
                               rust="success", explanation="Java planning failed"))
    else:
        for field in ["table_name", "snapshot_id", "schema_id"]:
            compare(field, j[field.replace("_", "-")], r[field], ("Java TypeUtil.project constructs a partial schema with default ID 0; Rust intentionally retains the source schema ID" if field == "schema_id" and name == "projection" else "Report identity must agree"))
        compare("projection", sorted(zip(j["projected-field-ids"], j["projected-field-names"])),
                sorted(zip(r["projected_field_ids"], r["projected_field_names"])),
                "Resolved ID/name pairs are unordered in Java")
        raw = r["filter"]
        if raw == "AlwaysTrue":
            raw = True
        elif raw == "AlwaysFalse":
            raw = False
        else:
            binary = raw["Binary"]
            assert binary["op"] in ("LessThan", "Eq")
            raw = {"type": {"LessThan": "lt", "Eq": "eq"}[binary["op"]], "term": binary["term"]["name"], "value": binary["literal"]["literal"]}
        compare("raw_filter", other["raw_filter"], raw, "Compare actual unsanitized scan options")
        sanitized = raw
        if isinstance(raw, dict):
            sanitized = dict(raw, value=f"({len(str(abs(raw['value'])))}-digit-int)")
        compare("sanitized_filter", j["filter"], sanitized, "Java sanitizes integer literals; raw predicates are checked separately")
        for metric in sorted(set(r["metrics"]) | {k.replace("-", "_") for k in j["metrics"] if k != "total-planning-duration"}):
            jm = j["metrics"].get(metric.replace("_", "-"), {})
            compare(metric, jm.get("value", "ABSENT"), r["metrics"].get(metric, "ABSENT"),
                    "Measured counters must match; ABSENT is not zero")
            compare(metric + ".unit", jm.get("unit", "ABSENT"),
                    "bytes" if metric.endswith("in_bytes") else "count", "Counter units")
        duration = j["metrics"]["total-planning-duration"]
        compare("duration.unit", duration["time-unit"], "nanoseconds", "Unit, not elapsed value")
        compare("duration.count", duration["count"], 1, "One planning invocation")
        assert duration["total-duration"] >= 0 and r["total_planning_duration"]["secs"] >= 0

        def plans(items):
            return sorted([dict(p, deletes=sorted(p["deletes"], key=lambda d: json.dumps(d, sort_keys=True))) for p in items], key=lambda p: p["path"])

        compare("plans", plans(other["plans"]), plans(case["plans"]), "Exact paths, sizes, delete content types and DV ranges; ordering only is normalized")
    results.append(dict(fixture=name, status="mismatch" if mismatches else "match", mismatches=mismatches))
summary = dict(results=results, unsupported_report_fields=["Java environment/options metadata"],
               duration_comparison="unit and invocation count checked; elapsed values intentionally not equal")
(root / "comparison.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary, indent=2))
sys.exit(any(r["mismatches"] for r in results))
