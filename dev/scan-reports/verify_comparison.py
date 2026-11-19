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
print(f"{len(controls)} negative controls passed")
