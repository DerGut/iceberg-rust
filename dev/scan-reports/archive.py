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

"""Archive a run with temporary paths normalized only after exact comparison."""
import hashlib
import json
import pathlib
import sys

run = pathlib.Path(sys.argv[1])
out = pathlib.Path(sys.argv[2])
out.mkdir(parents=True, exist_ok=True)
rust = json.loads((run / "results/rust.json").read_text())
java = json.loads((run / "results/java.json").read_text())
inventory = []
comparison = (run / "results/comparison.json").read_text()
for case, other in zip(rust, java):
    assert case["fixture"] == other["fixture"]
    metadata_path = pathlib.Path(case["metadata"])
    metadata = json.loads(metadata_path.read_text())
    table_root = pathlib.Path(metadata["location"])
    files = [{"path": "<metadata-file>", "sha256": hashlib.sha256(metadata_path.read_bytes()).hexdigest()}]
    for path in sorted(table_root.rglob("*")):
        if path.is_file():
            files.append({"path": str(path.relative_to(table_root)), "size": path.stat().st_size,
                          "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    inventory.append({"fixture": case["fixture"], "files": files})
    comparison = comparison.replace(str(table_root), "<fixture-root>").replace(str(metadata_path), "<metadata-file>")
    for obj in (case, other):
        normalized = json.dumps(obj).replace(str(table_root), "<fixture-root>").replace(str(metadata_path), "<metadata-file>")
        obj.clear()
        obj.update(json.loads(normalized))
for name, value in [("rust", rust), ("java", java), ("inputs", inventory)]:
    (out / (name + ".json")).write_text(json.dumps(value, separators=(",", ":")) + "\n")
(out / "comparison.json").write_text(comparison)
mismatched = [c["fixture"] for c in json.loads(comparison)["results"] if c["mismatches"]]
manifest = {
    "rust_revision": (run / "rust-revision.txt").read_text().strip(),
    "rust_working_tree_patch": (run / "rust-working-tree.patch").read_text(),
    "java_revision": (run / "java-revision.txt").read_text().strip(),
    "runtime": (run / "runtime.txt").read_text().splitlines(),
    "gradle": "8.14.6 (pinned source wrapper)",
    "command": f"bash dev/scan-reports/run.sh {run}",
    "comparison_exit_status": int(bool(mismatched)),
    "mismatched_fixtures": mismatched,
    "explanation": "All observed differences are retained; see comparison.json and SEMANTICS.md"
}
(out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
