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

"""Negative controls against actual captured reports and plans."""
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile

root = Path(sys.argv[1])
original = json.loads((root / "rust.json").read_text())
java = (root / "java.json").read_text()
comparator = Path(__file__).with_name("compare.py")
for field in ["table_ident.name", "table_ident.namespace", "schema_id", "result_data_files", "plans"]:
    metric = "table_name" if field.startswith("table_ident.") else field
    cases = copy.deepcopy(original)
    fixture = "projection" if field == "schema_id" else "unfiltered"
    case = next(c for c in cases if c["fixture"] == fixture)
    if field == "table_ident.name":
        case["report"]["table_ident"]["name"] = "incorrect"
    elif field == "table_ident.namespace":
        case["report"]["table_ident"]["namespace"] = ["incorrect"]
    elif field == "schema_id":
        case["report"][field] += 1
    elif metric == "result_data_files":
        case["report"]["metrics"][metric] += 1
    else:
        case["plans"].pop()
    with tempfile.TemporaryDirectory() as temporary:
        output = Path(temporary)
        (output / "rust.json").write_text(json.dumps(cases))
        (output / "java.json").write_text(java)
        result = subprocess.run([sys.executable, str(comparator), str(output)], capture_output=True, text=True)
        assert result.returncode == 1, result.stderr
        comparison = json.loads((output / "comparison.json").read_text())
        differences = next(c for c in comparison["results"] if c["fixture"] == fixture)["mismatches"]
        assert [difference["metric"] for difference in differences] == [metric], differences
        print(f"Detected independent Rust {field} corruption")
