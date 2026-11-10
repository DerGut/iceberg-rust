#!/usr/bin/env bash
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
set -euo pipefail
repo=$(git rev-parse --show-toplevel)
java_revision=4e61757dcf568051a13bb8d4c00ef57c9f248947
work=${1:?Usage: bash dev/scan-reports/run.sh ABSOLUTE_OUTPUT_DIRECTORY}
mkdir -p "$work/java"
export SCAN_REPORT_OUTPUT="$work/results"
mkdir -p "$SCAN_REPORT_OUTPUT"
git rev-parse HEAD > "$work/rust-revision.txt"
git diff --binary HEAD > "$work/rust-working-tree.patch"
(rustc -Vv; cargo -V; java -version; uname -a) > "$work/runtime.txt" 2>&1
printf '%s\n' "$java_revision" > "$work/java-revision.txt"
if [[ ! -f "$work/java/gradlew" ]]; then
  curl -fL "https://api.github.com/repos/apache/iceberg/tarball/$java_revision" -o "$work/java.tar.gz"
  tar xzf "$work/java.tar.gz" -C "$work/java" --strip-components=1
  printf '1.12.0-SNAPSHOT\n' > "$work/java/version.txt"
fi
cargo test -p iceberg --lib --no-default-features scan::tests::export_java_differential_fixtures -- --ignored --exact
cp "$repo/dev/scan-reports/TestRustScanReports.java" "$work/java/core/src/test/java/org/apache/iceberg/"
(cd "$work/java" && ./gradlew :iceberg-core:test --tests org.apache.iceberg.TestRustScanReports -x generateGitProperties --rerun-tasks --console=plain)
python3 "$repo/dev/scan-reports/compare.py" "$SCAN_REPORT_OUTPUT"
