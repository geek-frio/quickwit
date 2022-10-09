// Copyright (C) 2022 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use base64;
use itertools::Itertools;
use prost_types::{Duration as WellKnownDuration, Timestamp as WellKnownTimestamp};
use quickwit_opentelemetry::otlp::{Event as QwEvent, Span as QwSpan};
use quickwit_proto::jaeger::api_v2::{
    KeyValue as JaegerKeyValue, Log as JaegerLog, Process as JaegerProcess, Span as JaegerSpan,
};
use quickwit_proto::jaeger::storage::v1::span_reader_plugin_server::SpanReaderPlugin;
use quickwit_proto::jaeger::storage::v1::{
    FindTraceIDsRequest, FindTraceIDsResponse, FindTracesRequest, GetOperationsRequest,
    GetOperationsResponse, GetServicesRequest, GetServicesResponse, GetTraceRequest, Operation,
    SpansResponseChunk,
};
use quickwit_proto::opentelemetry::proto::trace::v1::Status as OtlpStatus;
use quickwit_proto::{SearchRequest, SearchResponse};
use quickwit_search::SearchService;
use serde_json::Value as JsonValue;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{debug, warn};

// OpenTelemetry to Jaeger Transformation
// <https://opentelemetry.io/docs/reference/specification/trace/sdk_exporters/jaeger/>

const TRACE_INDEX_ID: &str = "otel-trace";

pub struct JaegerService {
    search_service: Arc<dyn SearchService>,
}

impl JaegerService {
    pub fn new(search_service: Arc<dyn SearchService>) -> Self {
        Self { search_service }
    }
}

type SpanStream = ReceiverStream<Result<SpansResponseChunk, Status>>;

#[async_trait]
impl SpanReaderPlugin for JaegerService {
    type GetTraceStream = SpanStream;

    type FindTracesStream = SpanStream;

    async fn get_services(
        &self,
        request: Request<GetServicesRequest>,
    ) -> Result<Response<GetServicesResponse>, Status> {
        let request = request.into_inner();
        debug!(request=?request, "`get_services` request");

        let search_request = request.try_into_search_req()?;
        let search_response = self.search_service.root_search(search_request).await?;
        let response = search_response.into_jaeger_resp();
        debug!(response=?response, "`get_services` response");
        Ok(Response::new(response))
    }

    async fn get_operations(
        &self,
        request: Request<GetOperationsRequest>,
    ) -> Result<Response<GetOperationsResponse>, Status> {
        let request = request.into_inner();
        debug!(request=?request, "`get_operations` request");

        let search_request = request.try_into_search_req()?;
        let search_response = self.search_service.root_search(search_request).await?;
        let response = search_response.into_jaeger_resp();
        debug!(response=?response, "`get_operations` response");
        Ok(Response::new(response))
    }

    async fn find_traces(
        &self,
        request: Request<FindTracesRequest>,
    ) -> Result<Response<Self::FindTracesStream>, Status> {
        let request = request.into_inner();
        debug!(request=?request, "`find_traces` request");

        let search_request = request.try_into_search_req()?;
        let search_response = self.search_service.root_search(search_request).await?;
        let spans = search_response
            .hits
            .into_iter()
            .map(|hit| qw_span_to_jaeger_span(&hit.json))
            .collect::<Result<_, _>>()?;
        debug!(spans=?spans, "`find_traces` response");
        let (tx, rx) = mpsc::channel(1);
        tx.send(Ok(SpansResponseChunk { spans }))
            .await
            .expect("The channel should be opened and empty.");
        let response = ReceiverStream::new(rx);
        Ok(Response::new(response))
    }

    async fn find_trace_i_ds(
        &self,
        request: Request<FindTraceIDsRequest>,
    ) -> Result<Response<FindTraceIDsResponse>, Status> {
        let request = request.into_inner();
        debug!(request=?request, "`find_trace_ids` request");
        let search_request = request.try_into_search_req()?;
        let search_response = self.search_service.root_search(search_request).await?;
        let response = search_response.into_jaeger_resp();
        debug!(response=?response, "`find_trace_ids` response");
        Ok(Response::new(response))
    }

    async fn get_trace(
        &self,
        request: Request<GetTraceRequest>,
    ) -> Result<Response<Self::GetTraceStream>, Status> {
        let request = request.into_inner();
        debug!(request=?request, "`get_trace` request");
        let search_request = request.try_into_search_req()?;
        let search_response = self.search_service.root_search(search_request).await?;
        let spans = search_response
            .hits
            .into_iter()
            .map(|hit| qw_span_to_jaeger_span(&hit.json))
            .collect::<Result<_, _>>()?;
        debug!(spans=?spans, "`get_trace` response");
        let (tx, rx) = mpsc::channel(1);
        tx.send(Ok(SpansResponseChunk { spans }))
            .await
            .expect("The channel should be opened and empty.");
        let response = ReceiverStream::new(rx);
        Ok(Response::new(response))
    }
}
trait IntoSearchRequest {
    fn try_into_search_req(self) -> Result<SearchRequest, Status>;
}

trait FromSearchResponse {
    fn from_search_resp(search_response: SearchResponse) -> Self;
}

trait IntoJaegerResponse<T> {
    fn into_jaeger_resp(self) -> T;
}

impl<T> IntoJaegerResponse<T> for SearchResponse
where T: FromSearchResponse
{
    fn into_jaeger_resp(self) -> T {
        T::from_search_resp(self)
    }
}

// GetServices
impl IntoSearchRequest for GetServicesRequest {
    fn try_into_search_req(self) -> Result<SearchRequest, Status> {
        let request = SearchRequest {
            index_id: TRACE_INDEX_ID.to_string(),
            query: build_query("", "", "", HashMap::new()),
            search_fields: Vec::new(),
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 1_000,
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
            aggregation_request: None,
            snippet_fields: Vec::new(),
        };
        Ok(request)
    }
}

impl FromSearchResponse for GetServicesResponse {
    fn from_search_resp(search_response: SearchResponse) -> Self {
        let services: Vec<String> = search_response
            .hits
            .into_iter()
            .map(|hit| {
                serde_json::from_str::<JsonValue>(&hit.json)
                    .expect("Failed to deserialize hit. This should never happen!")
            })
            .filter_map(extract_service_name)
            .sorted()
            .dedup()
            .collect();
        Self { services }
    }
}

fn extract_service_name(mut doc: JsonValue) -> Option<String> {
    match doc["service_name"].take() {
        JsonValue::String(service_name) => Some(service_name),
        _ => None,
    }
}

// GetOperations
impl IntoSearchRequest for GetOperationsRequest {
    fn try_into_search_req(self) -> Result<SearchRequest, Status> {
        let request = SearchRequest {
            index_id: TRACE_INDEX_ID.to_string(),
            query: build_query(&self.service, &self.span_kind, "", HashMap::new()),
            search_fields: Vec::new(),
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 1_000,
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
            aggregation_request: None,
            snippet_fields: Vec::new(),
        };
        Ok(request)
    }
}

impl FromSearchResponse for GetOperationsResponse {
    fn from_search_resp(search_response: SearchResponse) -> Self {
        let operations: Vec<Operation> = search_response
            .hits
            .into_iter()
            .map(|hit| {
                serde_json::from_str::<JsonValue>(&hit.json)
                    .expect("Failed to deserialize hit. This should never happen!")
            })
            .filter_map(extract_operation)
            .sorted()
            .dedup()
            .collect();
        Self {
            operations,
            operation_names: Vec::new(), // `operation_names` is deprecated.
        }
    }
}

fn extract_operation(mut doc: JsonValue) -> Option<Operation> {
    match (doc["span_name"].take(), doc["span_kind"].take()) {
        (JsonValue::String(span_name), JsonValue::String(span_kind)) => Some(Operation {
            name: span_name,
            span_kind,
        }),
        _ => None,
    }
}

// FindTraces
impl IntoSearchRequest for FindTracesRequest {
    fn try_into_search_req(self) -> Result<SearchRequest, Status> {
        let query = self
            .query
            .ok_or_else(|| Status::invalid_argument("Query is empty."))?;
        let start_timestamp = query.start_time_min.map(|ts| ts.seconds);
        let end_timestamp = query.start_time_max.map(|ts| ts.seconds);
        // TODO: Push span duration filter.
        let max_hits = query.num_traces as u64;
        let request = SearchRequest {
            index_id: TRACE_INDEX_ID.to_string(),
            query: build_query(&query.service_name, "", &query.operation_name, query.tags),
            search_fields: Vec::new(),
            start_timestamp,
            end_timestamp,
            max_hits,
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
            aggregation_request: None,
            snippet_fields: Vec::new(),
        };
        Ok(request)
    }
}

// FindTraceIDs
impl IntoSearchRequest for FindTraceIDsRequest {
    fn try_into_search_req(self) -> Result<SearchRequest, Status> {
        let query = self
            .query
            .ok_or_else(|| Status::invalid_argument("Query is empty."))?;
        let start_timestamp = query.start_time_min.map(|ts| ts.seconds);
        let end_timestamp = query.start_time_max.map(|ts| ts.seconds);
        // TODO: Push span duration filter.
        let max_hits = query.num_traces as u64;
        let request = SearchRequest {
            index_id: TRACE_INDEX_ID.to_string(),
            query: build_query(&query.service_name, "", &query.operation_name, query.tags),
            search_fields: Vec::new(),
            start_timestamp,
            end_timestamp,
            max_hits,
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
            aggregation_request: None,
            snippet_fields: Vec::new(),
        };
        Ok(request)
    }
}

impl FromSearchResponse for FindTraceIDsResponse {
    fn from_search_resp(search_response: SearchResponse) -> Self {
        let trace_ids: Vec<Vec<u8>> = search_response
            .hits
            .into_iter()
            .map(|hit| {
                serde_json::from_str::<JsonValue>(&hit.json)
                    .expect("Failed to deserialize hit. This should never happen.")
            })
            .filter_map(extract_trace_id)
            .sorted()
            .dedup()
            .map(|trace_id| {
                base64::decode(&trace_id)
                    .expect("Failed to decode trace ID. This should never happen!")
            })
            .collect();
        Self { trace_ids }
    }
}

fn extract_trace_id(mut doc: JsonValue) -> Option<String> {
    match doc["trace_id"].take() {
        JsonValue::String(trace_id) => Some(trace_id),
        _ => None,
    }
}

// GetTrace
impl IntoSearchRequest for GetTraceRequest {
    fn try_into_search_req(self) -> Result<SearchRequest, Status> {
        let query = format!("trace_id:{}", base64::encode(self.trace_id));
        let request = SearchRequest {
            index_id: TRACE_INDEX_ID.to_string(),
            query,
            search_fields: Vec::new(),
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 1_000,
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
            aggregation_request: None,
            snippet_fields: Vec::new(),
        };
        Ok(request)
    }
}

fn build_query(
    service_name: &str,
    span_kind: &str,
    span_name: &str,
    mut tags: HashMap<String, String>,
) -> String {
    if let Some(qw_query) = tags.remove("_qw_query") {
        return qw_query;
    }
    let mut query = String::new();

    if !service_name.is_empty() {
        query.push_str("service_name:");
        query.push_str(service_name);
    }
    if !span_kind.is_empty() {
        if !query.is_empty() {
            query.push_str(" AND ");
        }
        query.push_str("span_kind:");
        query.push_str(span_kind);
    }
    if !span_name.is_empty() {
        if !query.is_empty() {
            query.push_str(" AND ");
        }
        query.push_str("span_name:");
        query.push_str(span_name);
    }
    if !tags.is_empty() {
        if !query.is_empty() {
            query.push_str(" AND ");
        }

        #[cfg(not(any(test, feature = "testsuite")))]
        let mut tags_iter = tags.into_iter();

        // Sorts keys for deterministic tests.
        #[cfg(any(test, feature = "testsuite"))]
        let mut tags_iter = tags.into_iter().sorted();

        if let Some((key, value)) = tags_iter.next() {
            query.push_str("(span_attributes.");
            query.push_str(&key);
            query.push(':');
            query.push_str(&value);
            query.push_str("OR event_attributes.");
            query.push_str(&key);
            query.push(':');
            query.push_str(&value);
            query.push(':');
            query.push_str(&value);
            query.push(')');
        }
        for (key, value) in tags_iter {
            query.push_str(" AND ");
            query.push_str("(span_attributes.");
            query.push_str(&key);
            query.push(':');
            query.push_str(&value);
            query.push_str("OR event_attributes.");
            query.push_str(&key);
            query.push(':');
            query.push_str(&value);
            query.push(':');
            query.push_str(&value);
            query.push(')');
        }
    }
    // FIXME: We'd like the query to be `*` instead.
    if query.is_empty() {
        query.push_str("span_kind:internal");
    }
    debug!(query=%query, "Search query");
    query
}

fn qw_span_to_jaeger_span(qw_span: &str) -> Result<JaegerSpan, Status> {
    let span = serde_json::from_str::<QwSpan>(qw_span)
        .map_err(|error| Status::internal(format!("Failed to deserialize span: {error:?}")))?;
    let trace_id = base64::decode(span.trace_id)
        .map_err(|error| Status::internal(format!("Failed to decode trace ID: {error:?}")))?;
    let span_id = base64::decode(span.span_id)
        .map_err(|error| Status::internal(format!("Failed to decode span ID: {error:?}")))?;

    let start_time = Some(to_well_known_timestamp(span.span_start_timestamp_nanos));
    let duration = Some(to_well_known_duration(
        span.span_start_timestamp_nanos,
        span.span_end_timestamp_nanos,
    ));
    let process = Some(JaegerProcess {
        service_name: span.service_name,
        tags: otlp_attributes_to_jaeger_tags(span.resource_attributes)?,
    });
    let logs = span
        .events
        .into_iter()
        .map(qw_event_to_jager_logs)
        .collect::<Result<_, _>>()?;

    // From <https://opentelemetry.io/docs/reference/specification/trace/sdk_exporters/jaeger/#spankind>
    let mut tags = otlp_attributes_to_jaeger_tags(span.span_attributes)?;
    // inject_dropped_attribute_tag(&mut tags, span.span_dropped_attributes_count);
    // inject_span_kind_tag(&mut tags, span.span_kind);
    // inject_span_status_tags(&mut tags, span.span_status);

    let span = JaegerSpan {
        trace_id,
        span_id,
        operation_name: span.span_name,
        references: Vec::new(), // TODO
        flags: 0,               // TODO
        start_time,
        duration,
        tags,
        logs,
        process,
        process_id: "".to_string(), // TODO
        warnings: Vec::new(),       // TODO
    };
    Ok(span)
}

fn to_well_known_timestamp(timestamp_nanos: i64) -> WellKnownTimestamp {
    let seconds = timestamp_nanos / 1_000_000_000;
    let nanos = (timestamp_nanos % 1_000_000_000) as i32;
    WellKnownTimestamp { seconds, nanos }
}

fn to_well_known_duration(
    start_timestamp_nanos: i64,
    end_timestamp_nanos: i64,
) -> WellKnownDuration {
    let duration_nanos = end_timestamp_nanos - start_timestamp_nanos;
    let seconds = duration_nanos / 1_000_000_000;
    let nanos = (duration_nanos % 1_000_000_000) as i32;
    WellKnownDuration { seconds, nanos }
}

fn inject_dropped_attribute_tag(tags: &mut Vec<JaegerKeyValue>, dropped_attributes_count: u64) {
    if dropped_attributes_count > 0 {
        tags.push(JaegerKeyValue {
            key: "otel.dropped_attributes_count".to_string(),
            v_type: 2,
            v_str: String::new(),
            v_bool: false,
            v_int64: dropped_attributes_count as i64,
            v_float64: 0.0,
            v_binary: Vec::new(),
        });
    }
}

fn inject_span_kind_tag(tags: &mut Vec<JaegerKeyValue>, span_kind_id: u64) {
    let span_kind = match span_kind_id {
        1 => return,
        2 => "server",
        3 => "client",
        4 => "producer",
        5 => "consumer",
        _ => {
            warn!(kind_id=%span_kind_id, "Unknown span kind ID.");
            return;
        }
    };
    tags.push(JaegerKeyValue {
        key: "span.kind".to_string(),
        v_type: 0,
        v_str: span_kind.to_string(),
        v_bool: false,
        v_int64: 0,
        v_float64: 0.0,
        v_binary: Vec::new(),
    });
}

fn inject_span_status_tags(tags: &mut Vec<JaegerKeyValue>, span_status_opt: Option<OtlpStatus>) {
    if let Some(span_status) = span_status_opt {
        if !span_status.message.is_empty() {
            tags.push(JaegerKeyValue {
                key: "otel.status_description".to_string(),
                v_type: 0,
                v_str: span_status.message,
                v_bool: false,
                v_int64: 0,
                v_float64: 0.0,
                v_binary: Vec::new(),
            });
        }
        let status_code = match span_status.code {
            0 => return,
            1 => "server",
            2 => "client",
            _ => {
                warn!(status_code=%span_status.code, "Unknown span status code.");
                return;
            }
        };
        tags.push(JaegerKeyValue {
            key: "otel.status_code".to_string(),
            v_type: 0,
            v_str: status_code.to_string(),
            v_bool: false,
            v_int64: 0,
            v_float64: 0.0,
            v_binary: Vec::new(),
        });
    }
}

fn otlp_attributes_to_jaeger_tags(
    attributes: HashMap<String, JsonValue>,
) -> Result<Vec<JaegerKeyValue>, Status> {
    let mut tags = Vec::with_capacity(attributes.len());
    for (key, value) in attributes {
        let mut tag = JaegerKeyValue {
            key,
            v_type: 0,
            v_str: String::new(),
            v_bool: false,
            v_int64: 0,
            v_float64: 0.0,
            v_binary: Vec::new(),
        };
        match value {
            JsonValue::String(value) => tag.v_str = value,
            JsonValue::Bool(value) => {
                tag.v_type = 1;
                tag.v_bool = value;
            }
            JsonValue::Number(number) => {
                if let Some(value) = number.as_i64() {
                    tag.v_type = 2;
                    tag.v_int64 = value;
                } else if let Some(value) = number.as_f64() {
                    tag.v_type = 3;
                    tag.v_float64 = value
                }
            }
            _ => {
                return Err(Status::internal(format!(
                    "Failed to serialize attributes: unexpected type `{value:?}`"
                )))
            }
        };
        tags.push(tag);
    }
    Ok(tags)
}

fn to_span_kind_id(span_kind_str: &str) -> &str {
    match span_kind_str {
        "unspecified" => "0",
        "internal" => "1",
        "server" => "2",
        "client" => "3",
        "producer" => "4",
        "consumer" => "5",
        _ => "FIXME!",
    }
}

fn qw_event_to_jager_logs(event: QwEvent) -> Result<JaegerLog, Status> {
    let timestamp = to_well_known_timestamp(event.event_timestamp_nanos);
    let mut fields = otlp_attributes_to_jaeger_tags(event.event_attributes)?;
    // inject_dropped_attribute_tag(&mut fields, event.event_dropped_attributes_count);
    let log = JaegerLog {
        timestamp: Some(timestamp),
        fields,
    };
    Ok(log)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_query() {
        {
            let service_name = "quickwit";
            let span_kind = "";
            let span_name = "";
            let tags = HashMap::new();
            assert_eq!(
                build_query(service_name, span_kind, span_name, tags),
                "service_name:quickwit"
            );
        }
        {
            let service_name = "quickwit";
            let span_kind = "";
            let span_name = "";
            let tags = HashMap::from_iter([("_qw_query".to_string(), "query".to_string())]);
            assert_eq!(
                build_query(service_name, span_kind, span_name, tags),
                "query"
            );
        }
        {
            let service_name = "";
            let span_kind = "client";
            let span_name = "";
            let tags = HashMap::new();
            assert_eq!(
                build_query(service_name, span_kind, span_name, tags),
                "span_kind:client"
            );
        }
        {
            let service_name = "";
            let span_kind = "";
            let span_name = "leaf_search";
            let tags = HashMap::new();
            assert_eq!(
                build_query(service_name, span_kind, span_name, tags),
                "span_name:leaf_search"
            );
        }
        {
            let service_name = "";
            let span_kind = "";
            let span_name = "";
            let tags = HashMap::from_iter([("foo".to_string(), "bar".to_string())]);
            assert_eq!(
                build_query(service_name, span_kind, span_name, tags),
                "span_attributes.foo:bar"
            );
        }
        {
            let service_name = "";
            let span_kind = "";
            let span_name = "";
            let tags = HashMap::from_iter([
                ("baz".to_string(), "qux".to_string()),
                ("foo".to_string(), "bar".to_string()),
            ]);
            assert_eq!(
                build_query(service_name, span_kind, span_name, tags),
                "span_attributes.baz:qux AND span_attributes.foo:bar"
            );
        }
        {
            let service_name = "quickwit";
            let span_kind = "";
            let span_name = "";
            let tags = HashMap::from_iter([
                ("baz".to_string(), "qux".to_string()),
                ("foo".to_string(), "bar".to_string()),
            ]);
            assert_eq!(
                build_query(service_name, span_kind, span_name, tags),
                "service_name:quickwit AND span_attributes.baz:qux AND span_attributes.foo:bar"
            );
        }
        {
            let service_name = "quickwit";
            let span_kind = "client";
            let span_name = "";
            let tags = HashMap::from_iter([
                ("baz".to_string(), "qux".to_string()),
                ("foo".to_string(), "bar".to_string()),
            ]);
            assert_eq!(
                build_query(service_name, span_kind, span_name, tags),
                "service_name:quickwit AND span_kind:client AND span_attributes.baz:qux AND \
                 span_attributes.foo:bar"
            );
        }
        {
            let service_name = "quickwit";
            let span_kind = "client";
            let span_name = "leaf_search";
            let tags = HashMap::from_iter([
                ("baz".to_string(), "qux".to_string()),
                ("foo".to_string(), "bar".to_string()),
            ]);
            assert_eq!(
                build_query(service_name, span_kind, span_name, tags),
                "service_name:quickwit AND span_kind:client AND span_name:leaf_search AND \
                 span_attributes.baz:qux AND span_attributes.foo:bar"
            );
        }
    }
}
