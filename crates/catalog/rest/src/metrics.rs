// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;

use iceberg::expr::{Predicate, PredicateOperator};
use iceberg::metrics::{MetricsReport, MetricsReporter, ScanReport};
use iceberg::spec::{Datum, PrimitiveLiteral};
use iceberg::{Error, ErrorKind, Result, SessionContext};
use reqwest::{Method, StatusCode};
use serde_json::{Map, Value, json};

use crate::auth::{AuthManager, AuthSession};
use crate::client::HttpClient;
use crate::request::HttpRequest;

const DELIVERY_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) struct RestMetricsReporter {
    pub(crate) http_client: HttpClient,
    pub(crate) auth_manager: Arc<dyn AuthManager>,
    pub(crate) catalog_session: Arc<dyn AuthSession>,
    pub(crate) context: SessionContext,
    pub(crate) endpoint: String,
}

impl Debug for RestMetricsReporter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestMetricsReporter")
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl MetricsReporter for RestMetricsReporter {
    async fn report(&self, report: &MetricsReport) {
        let MetricsReport::Scan(report) = report else {
            tracing::debug!("Skipping unsupported REST metrics report type");
            return;
        };
        let delivery = async {
            let request = HttpRequest::build(
                self.http_client
                    .request(Method::POST, &self.endpoint)
                    .json(&scan_request(report)?),
            )?;
            let session = self
                .auth_manager
                .contextual_session(&self.context, self.catalog_session.clone())
                .await?;
            self.http_client
                .query_catalog(session.as_ref(), request)
                .await
        };
        match tokio::time::timeout(DELIVERY_TIMEOUT, delivery).await {
            Ok(Ok(response)) => match response.status() {
                StatusCode::NO_CONTENT => {}
                status => tracing::warn!(%status, "REST metrics delivery failed"),
            },
            Ok(Err(error)) => tracing::warn!(kind = ?error.kind(), "REST metrics delivery failed"),
            Err(_) => tracing::warn!("REST metrics delivery timed out"),
        }
    }
}

// Wire names and units follow Iceberg's ReportMetricsRequestParser and ScanMetricsResultParser.
// https://github.com/apache/iceberg/tree/4e61757dcf568051a13bb8d4c00ef57c9f248947/core/src/main/java/org/apache/iceberg
fn scan_request(report: &ScanReport) -> Result<Value> {
    let counters = report.metrics();
    let mut metrics = Map::new();
    for (name, value) in [
        ("total-data-manifests", counters.total_data_manifests()),
        ("total-delete-manifests", counters.total_delete_manifests()),
        ("scanned-data-manifests", counters.scanned_data_manifests()),
        (
            "scanned-delete-manifests",
            counters.scanned_delete_manifests(),
        ),
        ("skipped-data-manifests", counters.skipped_data_manifests()),
        (
            "skipped-delete-manifests",
            counters.skipped_delete_manifests(),
        ),
        ("result-data-files", counters.result_data_files()),
        ("result-delete-files", counters.result_delete_files()),
        ("skipped-data-files", counters.skipped_data_files()),
        ("skipped-delete-files", counters.skipped_delete_files()),
        ("indexed-delete-files", counters.indexed_delete_files()),
        ("equality-delete-files", counters.equality_delete_files()),
        (
            "positional-delete-files",
            counters.positional_delete_files(),
        ),
        ("dvs", counters.dvs()),
    ] {
        metrics.insert(name.to_owned(), counter("count", value)?);
    }
    metrics.insert(
        "total-file-size-in-bytes".to_owned(),
        counter("bytes", counters.total_file_size_in_bytes())?,
    );
    metrics.insert(
        "total-delete-file-size-in-bytes".to_owned(),
        counter("bytes", counters.total_delete_file_size_in_bytes())?,
    );
    metrics.insert(
        "total-planning-duration".to_owned(),
        json!({
            "time-unit": "nanoseconds",
            "count": 1,
            "total-duration": wire_integer(report.total_planning_duration().as_nanos())?,
        }),
    );
    Ok(json!({
        "report-type": "scan-report",
        "table-name": report.table_ident().to_string(),
        "snapshot-id": report.snapshot_id(),
        "schema-id": report.schema_id(),
        "projected-field-ids": report.projected_field_ids(),
        "projected-field-names": report.projected_field_names(),
        "filter": predicate(report.filter())?,
        "metrics": metrics,
    }))
}

fn wire_integer(value: u128) -> Result<i64> {
    i64::try_from(value).map_err(|_| {
        Error::new(
            ErrorKind::DataInvalid,
            "Scan metric exceeds REST int64 range",
        )
    })
}

fn counter(unit: &str, value: u64) -> Result<Value> {
    Ok(json!({"unit": unit, "value": wire_integer(u128::from(value))?}))
}

fn predicate(filter: &Predicate) -> Result<Value> {
    Ok(match filter {
        Predicate::AlwaysTrue => json!(true),
        Predicate::AlwaysFalse => json!(false),
        Predicate::And(expr) | Predicate::Or(expr) => {
            let [left, right] = expr.inputs();
            json!({
                "type": if matches!(filter, Predicate::And(_)) { "and" } else { "or" },
                "left": predicate(left)?, "right": predicate(right)?,
            })
        }
        Predicate::Not(expr) => json!({"type": "not", "child": predicate(expr.inputs()[0])?}),
        Predicate::Unary(expr) => json!({"type": operator(expr.op())?, "term": expr.term().name()}),
        Predicate::Binary(expr) => json!({
            "type": operator(expr.op())?, "term": expr.term().name(),
            "value": sanitized_literal(expr.literal())?,
        }),
        Predicate::Set(expr) => {
            let mut values = expr
                .literals()
                .iter()
                .map(sanitized_literal)
                .collect::<Result<Vec<_>>>()?;
            values.sort_unstable();
            json!({"type": operator(expr.op())?, "term": expr.term().name(), "values": values})
        }
    })
}

fn sanitized_literal(value: &Datum) -> Result<&str> {
    match value.literal() {
        PrimitiveLiteral::String(value) => Ok(value),
        _ => Err(Error::new(
            ErrorKind::DataInvalid,
            "Expected a sanitized scan-report literal",
        )),
    }
}

fn operator(op: PredicateOperator) -> Result<&'static str> {
    Ok(match op {
        PredicateOperator::IsNull => "is-null",
        PredicateOperator::NotNull => "not-null",
        PredicateOperator::IsNan => "is-nan",
        PredicateOperator::NotNan => "not-nan",
        PredicateOperator::LessThan => "lt",
        PredicateOperator::LessThanOrEq => "lt-eq",
        PredicateOperator::GreaterThan => "gt",
        PredicateOperator::GreaterThanOrEq => "gt-eq",
        PredicateOperator::Eq => "eq",
        PredicateOperator::NotEq => "not-eq",
        PredicateOperator::StartsWith => "starts-with",
        PredicateOperator::NotStartsWith => "not-starts-with",
        PredicateOperator::In => "in",
        PredicateOperator::NotIn => "not-in",
        _ => {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Unsupported scan-report predicate",
            ));
        }
    })
}

#[cfg(test)]
mod tests {
    use iceberg::expr::{BinaryExpression, Reference, SetExpression, UnaryExpression};

    use super::*;

    #[test]
    fn test_predicate_wire_operators_and_structure() {
        for (op, name) in [
            (PredicateOperator::IsNull, "is-null"),
            (PredicateOperator::NotNull, "not-null"),
            (PredicateOperator::IsNan, "is-nan"),
            (PredicateOperator::NotNan, "not-nan"),
        ] {
            assert_eq!(
                predicate(&Predicate::Unary(UnaryExpression::new(
                    op,
                    Reference::new("a.b")
                )))
                .unwrap(),
                json!({"type":name,"term":"a.b"})
            );
        }
        for (op, name) in [
            (PredicateOperator::LessThan, "lt"),
            (PredicateOperator::LessThanOrEq, "lt-eq"),
            (PredicateOperator::GreaterThan, "gt"),
            (PredicateOperator::GreaterThanOrEq, "gt-eq"),
            (PredicateOperator::Eq, "eq"),
            (PredicateOperator::NotEq, "not-eq"),
            (PredicateOperator::StartsWith, "starts-with"),
            (PredicateOperator::NotStartsWith, "not-starts-with"),
        ] {
            assert_eq!(
                predicate(&Predicate::Binary(BinaryExpression::new(
                    op,
                    Reference::new("a"),
                    Datum::string("(redacted)")
                )))
                .unwrap(),
                json!({"type":name,"term":"a","value":"(redacted)"})
            );
        }
        for (op, name) in [
            (PredicateOperator::In, "in"),
            (PredicateOperator::NotIn, "not-in"),
        ] {
            assert_eq!(
                predicate(&Predicate::Set(SetExpression::new(
                    op,
                    Reference::new("a"),
                    [Datum::string("b"), Datum::string("a")]
                        .into_iter()
                        .collect()
                )))
                .unwrap(),
                json!({"type":name,"term":"a","values":["a","b"]})
            );
        }
        assert_eq!(predicate(&Predicate::AlwaysTrue).unwrap(), json!(true));
        assert_eq!(predicate(&Predicate::AlwaysFalse).unwrap(), json!(false));
        let filter = Reference::new("a").is_null().and(
            Reference::new("b")
                .is_not_null()
                .or(!Reference::new("c").is_nan()),
        );
        assert_eq!(
            predicate(&filter).unwrap(),
            json!({"type":"and",
            "left":{"type":"is-null","term":"a"},
            "right":{"type":"or","left":{"type":"not-null","term":"b"},
                "right":{"type":"not","child":{"type":"is-nan","term":"c"}}}})
        );
        assert!(predicate(&Reference::new("a").equal_to(Datum::int(123))).is_err());
    }

    #[test]
    fn test_metrics_wire_range_and_units() {
        assert_eq!(
            counter("count", 0).unwrap(),
            json!({"unit":"count","value":0})
        );
        assert_eq!(
            counter("bytes", i64::MAX as u64).unwrap(),
            json!({"unit":"bytes","value":i64::MAX})
        );
        assert!(counter("count", i64::MAX as u64 + 1).is_err());
        assert_eq!(wire_integer(123456789).unwrap(), 123456789);
        assert!(wire_integer(u128::MAX).is_err());
    }
}
