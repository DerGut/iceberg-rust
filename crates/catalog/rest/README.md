<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Apache Iceberg Rest Catalog Official Native Rust Implementation

[![crates.io](https://img.shields.io/crates/v/iceberg.svg)](https://crates.io/crates/iceberg-catalog-rest)
[![docs.rs](https://img.shields.io/docsrs/iceberg.svg)](https://docs.rs/iceberg/latest/iceberg-catalog-rest/)

This crate contains the official Native Rust implementation of Apache Iceberg Rest Catalog.

See the [API documentation](https://docs.rs/iceberg-catalog-rest/latest) for examples and the full API.

## Scan metrics delivery

Tables returned by create, load, register, and update operations use a private
REST metrics reporter when the catalog advertises the metrics endpoint. A
missing or empty endpoint list uses the existing legacy capability set, which
includes metrics. An explicit nonempty list without metrics leaves the default
logging reporter in place. Cached `304` table loads and table clones retain the
reporter. Table-level and scan-level custom reporters replace REST delivery;
independently loaded or updated tables receive the catalog's default again.

Each finalized scan report is sent as a `scan-report` to
`POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics`. Conversion
preserves the report's identity, projection, sanitized predicate, all measured
counters (including zero), and nanosecond planning duration. It does not recount
planning or sanitize again. Metadata that the report does not collect is omitted.
Values outside the REST signed 64-bit range are rejected with a diagnostic
rather than truncated. The wire format follows Iceberg's
[report parser](https://github.com/apache/iceberg/blob/4e61757dcf568051a13bb8d4c00ef57c9f248947/core/src/main/java/org/apache/iceberg/rest/requests/ReportMetricsRequestParser.java)
and [REST specification](https://github.com/apache/iceberg/blob/4e61757dcf568051a13bb8d4c00ef57c9f248947/open-api/rest-catalog-open-api.yaml).
In particular, byte metrics use Java's `total-file-size-in-bytes` and
`total-delete-file-size-in-bytes` keys; the specification's example omits `in`.
The request-level test compares against `testdata/scan_report_request.json`,
whose complete payload round-trips through that Java revision's request parser.

Delivery reuses the configured HTTP client, connection pool, headers, and auth
manager. Authentication is derived at delivery time from the session that
returned the table; the destination remains that table's catalog endpoint.
This uses Rust's existing contextual-auth contract. Java also has a separate
`AuthManager.tableSession` hook for load-response credentials; that hook is not
available in Rust. This change does not introduce table-token exchange or copy
storage credentials or session properties into the report payload.

Delivery is best effort:

- There is no concurrency limit, admission drop, or waiting queue. Each core
  reporting callback attempts its own request. Concurrent scan completions can
  therefore produce concurrent requests; the deadline bounds their lifetime,
  not their number.
- A five-second deadline includes contextual-session derivation,
  authentication, sending the request, and reading its response. HTTP and auth
  failures and timeouts produce a warning. There are no delivery retries.
- Endpoint support is checked when the table's reporter is installed, using
  the catalog configuration described above. `204` acknowledges delivery; all
  other statuses, including `404`, `405`, and `501`, fail that attempt without
  changing reporter configuration or suppressing future reports.
- The REST reporter spawns no additional tasks. It runs in the core reporting
  callback, which planning does not await. A custom auth implementation must
  yield during I/O; a deadline cannot preempt blocking code.
- Dropping the catalog does not invalidate reporters retained by its tables.
  Runtime shutdown cancels in-flight callbacks; there is no synchronous flush
  on drop, durable buffer, or delivery guarantee across shutdown. Applications
  must keep the runtime alive for best-effort delivery.

These transport rules apply equally to complete, partial, and failed scans;
report finalization remains the core reporting API's responsibility.
