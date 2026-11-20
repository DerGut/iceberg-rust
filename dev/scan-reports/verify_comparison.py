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

"""Prove real captures reject corruption, including in explained planner cases."""
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile

from compare import METRICS

root = Path(sys.argv[1])
original = json.loads((root / "rust.json").read_text())
java = (root / "java.json").read_text()
controls = [("unfiltered", metric, "increment") for metric in sorted(METRICS)]
controls += [
    ("unfiltered", "result_data_files", "missing"),
    ("unfiltered", "plans", "size"),
    ("unfiltered", "plans", "remove"),
    ("writer-equality-pruning", "equality_delete_files", "increment"),
    ("writer-equality-pruning", "result_delete_files", "increment"),
    ("writer-equality-association", "total_delete_file_size_in_bytes", "increment"),
    ("writer-equality-association", "plans", "size"),
    ("close-after-one", "result_data_files", "increment"),
    ("close-after-one", "total_data_manifests", "increment"),
    ("unfiltered", "fixture", "remove"),
    ("unfiltered", "fixture", "duplicate"),
]
for fixture, field, operation in controls:
    cases = copy.deepcopy(original)
    case = next(c for c in cases if c["fixture"] == fixture)
    if field == "fixture":
        if operation == "remove":
            cases.remove(case)
        else:
            cases.append(copy.deepcopy(case))
    elif field == "plans":
        if operation == "size":
            case["plans"][0]["size"] += 1
        else:
            case["plans"].pop()
    elif operation == "missing":
        del case["counts"][field]
    else:
        case["counts"][field] += 1
        case["settled_counts"][field] += 1
    with tempfile.TemporaryDirectory() as temporary:
        output = Path(temporary)
        (output / "rust.json").write_text(json.dumps(cases))
        (output / "java.json").write_text(java)
        result = subprocess.run([sys.executable, str(Path(__file__).with_name("compare.py")), str(output)], capture_output=True, text=True)
        if field == "fixture":
            assert result.returncode != 0, (fixture, operation)
        else:
            assert result.returncode == 2, (fixture, field, result.stdout, result.stderr)
            comparison = json.loads((output / "comparison.json").read_text())
            failed = next(r for r in comparison["results"] if r["fixture"] == fixture)
            assert failed["status"] == "unexpected-difference", failed
        print(f"Detected {fixture}: {field} {operation}")
report_controls = [("rust", "unfiltered", ["reports", 0, "metrics", metric], "increment") for metric in sorted(METRICS)]
report_controls += [
    ("rust", "unfiltered", ["reports"], "clear"),
    ("rust", "unfiltered", ["reports"], "duplicate"),
    ("rust", "unfiltered", ["reports", 0, "table-name"], "replace"),
    ("rust", "unfiltered", ["reports", 0, "snapshot-id"], "increment"),
    ("rust", "unfiltered", ["reports", 0, "schema-id"], "increment"),
    ("rust", "unfiltered", ["reports", 0, "filter"], "replace"),
    ("rust", "unfiltered", ["reports", 0, "metadata"], "replace"),
    ("rust", "unfiltered", ["reports", 0, "duration-ns"], "zero"),
    ("rust", "unfiltered", ["reports", 0, "duration-ns"], "huge"),
    ("rust", "unfiltered", ["reports", 0, "projected-field-ids"], "truncate"),
    ("rust", "unfiltered", ["reports", 0, "projected-field-names"], "truncate"),
    ("rust", "nested-projection-filter", ["reports", 0, "projected-field-ids", 0], "increment"),
    ("rust", "close-after-last", ["reports"], "unexpected"),
    ("java", "projection", ["report", "schema-id"], "increment"),
    ("java", "nested-projection-filter", ["report", "projected-field-ids", 0], "increment"),
    ("java", "nested-projection-filter", ["report", "projected-field-names"], "truncate"),
    ("java", "writer-projection-filter", ["report", "filter", "value"], "replace"),
    ("java", "writer-projection-filter", ["raw_filter", "value"], "increment"),
    ("java", "unfiltered", ["report", "metadata"], "clear"),
    ("java", "unfiltered", ["report", "metrics", "total-planning-duration", "time-unit"], "replace"),
]
for side, fixture, path, operation in report_controls:
    captures = {"rust": copy.deepcopy(original), "java": json.loads(java)}
    case = next(c for c in captures[side] if c["fixture"] == fixture)
    parent = case
    for key in path[:-1]:
        parent = parent[key]
    key = path[-1]
    if operation == "increment": parent[key] += 1
    elif operation == "replace": parent[key] = "corrupted"
    elif operation == "clear": parent[key] = [] if isinstance(parent[key], list) else {}
    elif operation == "duplicate": parent[key].append(copy.deepcopy(parent[key][0]))
    elif operation == "truncate": parent[key].pop()
    elif operation == "zero": parent[key] = 0
    elif operation == "huge": parent[key] = case["elapsed_ns"] + 1
    elif operation == "unexpected": parent[key] = [copy.deepcopy(original[0]["reports"][0])]
    with tempfile.TemporaryDirectory() as temporary:
        output = Path(temporary)
        for target, cases in captures.items():
            (output / f"{target}.json").write_text(json.dumps(cases))
        result = subprocess.run([sys.executable, str(Path(__file__).with_name("compare.py")), str(output)], capture_output=True, text=True)
        assert result.returncode == 2, (side, fixture, path, result.stdout, result.stderr)
        comparison = json.loads((output / "comparison.json").read_text())
        failed = next(r for r in comparison["results"] if r["fixture"] == fixture)
        assert failed["status"] == "unexpected-difference", failed
        print(f"Detected {side} {fixture}: {path} {operation}")
print(f"{len(controls) + len(report_controls)} negative controls passed")
