#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
"""Reproduce Java sanitizer fixtures and the public branch's Rust assertions."""
import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

JAVA_REVISION = "4e61757dcf568051a13bb8d4c00ef57c9f248947"
PUBLIC_REVISION = "cc97a755404341966f380d6368648fa483890850"
FIXTURE = "crates/iceberg/testdata/metrics/sanitized-filters.json"
repo = Path(subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip())
work = Path(sys.argv[1]).resolve()
work.mkdir(parents=True, exist_ok=True)
checks = []

def run(name, args, cwd=repo, env=None):
    result = subprocess.run(args, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    (work / (name + ".log")).write_bytes(result.stdout)
    checks.append(dict(name=name, command=list(map(str, args)), exit_status=result.returncode,
                       log_sha256=hashlib.sha256(result.stdout).hexdigest()))
    (work / "checks.json").write_text(json.dumps(checks, indent=2) + "\n")
    print(name, result.returncode, flush=True)
    if result.returncode:
        print(result.stdout.decode(errors="replace"))
        raise SystemExit(result.returncode)
    return result.stdout.decode()

def normalize(value):
    value = copy.deepcopy(value)
    if isinstance(value, dict):
        for key, child in value.items():
            value[key] = sorted(set(child)) if key == "values" else normalize(child)
    elif isinstance(value, list):
        value = [normalize(child) for child in value]
    return value

raw = subprocess.check_output(["git", "show", f"{PUBLIC_REVISION}:{FIXTURE}"], cwd=repo)
(work / "cases.json").write_bytes(raw)
fixture = json.loads(raw)
assert fixture["java_revision"] == JAVA_REVISION
java = work / "java"
java.mkdir(exist_ok=True)
if not (java / "gradlew").exists():
    run("download-java", ["curl", "-fL", f"https://api.github.com/repos/apache/iceberg/tarball/{JAVA_REVISION}", "-o", str(work / "java.tar.gz")])
    run("extract-java", ["tar", "xzf", str(work / "java.tar.gz"), "-C", str(java), "--strip-components=1"])
    (java / "version.txt").write_text("1.12.0-SNAPSHOT\n")
shutil.copy(repo / "dev/scan-reports/TestRustFilterSanitization.java", java / "core/src/test/java/org/apache/iceberg/TestRustFilterSanitization.java")
env = os.environ.copy()
env.update(FILTER_CASES=str(work / "cases.json"), FILTER_OUTPUT=str(work / "java.json"))
(work / "java.json").unlink(missing_ok=True)
versions = {name: run(name + "-version", command).splitlines() for name, command in
            [("rustc", ["rustc", "-Vv"]), ("cargo", ["cargo", "-V"]), ("java", ["java", "-version"])]}
run("java-capture", ["./gradlew", ":iceberg-core:test", "--tests", "org.apache.iceberg.TestRustFilterSanitization", "-x", "generateGitProperties", "--rerun-tasks", "--console=plain"], cwd=java, env=env)
actual = json.loads((work / "java.json").read_text())
assert normalize(actual) == normalize(fixture), "Java output differs from the committed Rust reference fixture"
run("rust-checkout", ["git", "worktree", "add", "--detach", str(work / "rust"), PUBLIC_REVISION])
try:
    env["CARGO_TARGET_DIR"] = str(repo / "target")
    run("rust-sanitizer", ["cargo", "test", "-p", "iceberg", "--lib", "--all-features", "metrics::filter::tests::test_sanitized_filters_against_pinned_java", "--", "--exact"], cwd=work / "rust", env=env)
    run("rust-delivery", ["cargo", "test", "-p", "iceberg", "--lib", "--all-features", "scan::tests::test_reports_sanitize_filters_before_delivery", "--", "--exact"], cwd=work / "rust", env=env)
    assert not subprocess.check_output(["git", "status", "--porcelain"], cwd=work / "rust")
finally:
    run("remove-rust-checkout", ["git", "worktree", "remove", str(work / "rust")])
differences = []
for case in actual["cases"]:
    java_value = normalize(case["expected"])
    rust_value = normalize(case.get("rust_expected", case["expected"]))
    if java_value != rust_value:
        assert case.get("known_difference")
        differences.append(dict(fixture=case["name"], java=java_value, rust=rust_value,
                                explanation=case["known_difference"]))
manifest = dict(public_revision=PUBLIC_REVISION, java_revision=JAVA_REVISION,
                harness_revision=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip(),
                harness_working_tree_patch=subprocess.check_output(["git", "diff", "--no-ext-diff", "--binary", "HEAD"], cwd=repo, text=True),
                fixture_sha256=hashlib.sha256(raw).hexdigest(), runtime=versions, checks=checks,
                gradle_wrapper=(java / "gradle/wrapper/gradle-wrapper.properties").read_text(),
                fixtures=len(actual["cases"]), matches=len(actual["cases"]) - len(differences),
                differences=differences, normalization=fixture["normalization"],
                unsupported=fixture["unsupported"],
                clock="Java production visitor clock fixed through reflection; binding and bound-to-unbound fallback preserved",
                rust_values="Verified by the pinned regression test's full-expression assertions, not a separate Rust capture",
                command=f"python3 dev/scan-reports/check_sanitization.py {work}",
                comparison_exit_status=int(bool(differences)))
(work / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
print(f"{manifest['matches']}/{manifest['fixtures']} matched; {len(differences)} retained differences")
sys.exit(manifest["comparison_exit_status"])
