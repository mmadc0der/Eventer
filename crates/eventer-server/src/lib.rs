//! HTTP front end for the event store.
//!
//! `POST /events` appends one JSON object, or a JSON array of objects, and waits
//! until that write is fsynced. An array is parsed in full before any row is
//! queued, then queued together so the batch shares one fsync.
//! `GET /events?from=&to=` returns a JSON array of events in that inclusive
//! millisecond range. Repeat `eq=field=value` to AND equality filters into the scan.
//! Repeat `gt`, `ge`, `lt`, and `le` as `field=value` to AND numeric ranges.
//! `gt` and `lt` are exclusive. `ge` and `le` are inclusive.
//! `limit` keeps the first matching rows and `offset` skips matches before that.
//! Omitting both returns the full match set.
//! `GET /events/count` and `GET /events/histogram` use that same range and those
//! same filters. `limit` and `offset` are rejected on both.
//! `GET /events/histogram?from=&to=&bucket_ms=N` returns one count per epoch-aligned
//! bucket that intersects that inclusive range.
//! `GET /events/stats` returns `sum`, `min`, and `max` for numeric columns over
//! that same range and the same filters, without the matching rows. `by=field`
//! splits that result into one group per value of a single column.
//! `bucket_ms` splits it into the same epoch-aligned buckets as the histogram.
//! `POST /events/drop` calls [`Store::drop_blocks_before`](eventer::Store::drop_blocks_before)
//! with the caller's `before_ms`. The request `Content-Type` must be
//! `application/json`, with an optional `charset=utf-8`. A block is removed
//! only when its maximum timestamp is strictly less than that cutoff.

use std::sync::Arc;

use axum::body::Body;
use axum::body::Bytes;
use axum::extract::{Query, RawQuery, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use eventer::Predicate;
use serde::Deserialize;
use serde_json::{json, Map, Number, Value};

#[derive(Clone)]
struct AppState {
    store: Arc<eventer::Store>,
}

pub fn router(store: Arc<eventer::Store>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/events/count", get(count_events))
        .route("/events/histogram", get(histogram_events))
        .route("/events/stats", get(stats_events))
        .route("/events", get(get_events).post(post_event))
        .route("/events/drop", post(post_drop))
        .with_state(AppState { store })
}

pub async fn serve(store: Arc<eventer::Store>, bind: &str) -> std::io::Result<()> {
    let app = router(store);
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
}

async fn health() -> Json<Value> {
    Json(json!({"status": "ok"}))
}

enum Ingest {
    One,
    Batch(usize),
}

async fn post_event(State(state): State<AppState>, body: Bytes) -> Response {
    let store = Arc::clone(&state.store);
    let joined = tokio::task::spawn_blocking(move || ingest(&store, &body)).await;
    match joined {
        Ok(Ok(Ingest::One)) => (StatusCode::CREATED, Json(json!({"ok": true}))).into_response(),
        Ok(Ok(Ingest::Batch(count))) => (
            StatusCode::CREATED,
            Json(json!({"ok": true, "count": count})),
        )
            .into_response(),
        Ok(Err(err)) => error_response(&err),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "append task failed"})),
        )
            .into_response(),
    }
}

fn ingest(store: &eventer::Store, body: &[u8]) -> eventer::Result<Ingest> {
    if first_byte(body) == Some(b'[') {
        let values: Vec<Box<serde_json::value::RawValue>> = serde_json::from_slice(body)?;
        if values.is_empty() {
            return Err(eventer::Error::event("event batch must not be empty"));
        }
        let events: Vec<&[u8]> = values.iter().map(|value| value.get().as_bytes()).collect();
        let count = events.len();
        store.append_json_batch_durable(&events)?;
        Ok(Ingest::Batch(count))
    } else {
        store.append_json_durable(body)?;
        Ok(Ingest::One)
    }
}

async fn post_drop(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
    body: Bytes,
) -> Response {
    if let Err(response) = reject_bucket_param(raw.as_deref().unwrap_or(""), "/events/drop") {
        return *response;
    }
    if let Err(response) = require_json_content_type(headers.get(CONTENT_TYPE)) {
        return *response;
    }
    let cutoff = match parse_before_ms(&body) {
        Ok(cutoff) => cutoff,
        Err(response) => return *response,
    };
    let store = Arc::clone(&state.store);
    let joined = tokio::task::spawn_blocking(move || store.drop_blocks_before(cutoff)).await;
    match joined {
        Ok(Ok(())) => (StatusCode::OK, Json(json!({"ok": true}))).into_response(),
        Ok(Err(err)) => error_response(&err),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "drop task failed"})),
        )
            .into_response(),
    }
}

/// The media type itself must be `application/json`. `charset=utf-8` is the
/// only optional parameter. A missing header or any other type, including
/// `text/plain` and `application/jsonp`, is rejected before the body is parsed.
fn require_json_content_type(
    header: Option<&HeaderValue>,
) -> std::result::Result<(), Box<Response>> {
    let Some(value) = header else {
        return Err(bad_request("Content-Type must be application/json"));
    };
    let Ok(text) = value.to_str() else {
        return Err(bad_request("Content-Type must be application/json"));
    };
    let mut parts = text.split(';');
    let media = parts.next().unwrap_or("").trim();
    if !media.eq_ignore_ascii_case("application/json") {
        return Err(bad_request("Content-Type must be application/json"));
    }
    for param in parts {
        let param = param.trim();
        if param.is_empty() {
            continue;
        }
        let Some((name, raw_value)) = param.split_once('=') else {
            return Err(bad_request("Content-Type must be application/json"));
        };
        let charset = raw_value.trim().trim_matches('"');
        if !name.trim().eq_ignore_ascii_case("charset") || !charset.eq_ignore_ascii_case("utf-8") {
            return Err(bad_request("Content-Type must be application/json"));
        }
    }
    Ok(())
}

/// `before_ms` is the caller's cutoff, a signed integer in the `i64` range.
/// A missing field, a non-integer, a JSON array, or any value that is not an
/// object is rejected here so the store is not touched.
fn parse_before_ms(body: &[u8]) -> std::result::Result<i64, Box<Response>> {
    let value: Value = serde_json::from_slice(body).map_err(|err| bad_request(&err.to_string()))?;
    let Some(object) = value.as_object() else {
        return Err(bad_request("drop body must be a JSON object"));
    };
    if object.contains_key("bucket_ms") {
        return Err(bad_request(
            "`bucket_ms` is not a parameter of /events/drop",
        ));
    }
    let Some(before) = object.get("before_ms") else {
        return Err(bad_request("missing `before_ms`"));
    };
    before
        .as_i64()
        .ok_or_else(|| bad_request("`before_ms` must be a signed integer"))
}

fn first_byte(body: &[u8]) -> Option<u8> {
    body.iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
}

#[derive(Debug, Deserialize)]
struct RangeParams {
    from: Option<String>,
    to: Option<String>,
}

async fn get_events(
    State(state): State<AppState>,
    Query(params): Query<RangeParams>,
    RawQuery(raw): RawQuery,
) -> Response {
    let from = match parse_bound(params.from, "from") {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let to = match parse_bound(params.to, "to") {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let raw_query = raw.as_deref().unwrap_or("");
    if let Err(response) = reject_bucket_param(raw_query, "/events") {
        return *response;
    }
    let (offset, limit) = match page_params(raw_query) {
        Ok(page) => page,
        Err(response) => return *response,
    };
    let predicates = match parse_filters(state.store.schema(), raw_query) {
        Ok(predicates) => predicates,
        Err(response) => return *response,
    };
    let store = Arc::clone(&state.store);
    let joined = tokio::task::spawn_blocking(move || {
        store.query_json_window(from, to, &predicates, offset, limit)
    })
    .await;
    match joined {
        Ok(Ok(bytes)) => (
            StatusCode::OK,
            [(CONTENT_TYPE, "application/json")],
            Body::from(bytes),
        )
            .into_response(),
        Ok(Err(err)) => error_response(&err),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "query task failed"})),
        )
            .into_response(),
    }
}

async fn count_events(
    State(state): State<AppState>,
    Query(params): Query<RangeParams>,
    RawQuery(raw): RawQuery,
) -> Response {
    let raw = raw.as_deref().unwrap_or("");
    if let Err(response) = reject_page_params(raw, "/events/count") {
        return *response;
    }
    if let Err(response) = reject_bucket_param(raw, "/events/count") {
        return *response;
    }
    let from = match parse_bound(params.from, "from") {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let to = match parse_bound(params.to, "to") {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let predicates = match parse_filters(state.store.schema(), raw) {
        Ok(predicates) => predicates,
        Err(response) => return *response,
    };
    let store = Arc::clone(&state.store);
    let joined =
        tokio::task::spawn_blocking(move || store.count_with_filter(from, to, &predicates)).await;
    match joined {
        Ok(Ok(count)) => (StatusCode::OK, Json(json!({"count": count}))).into_response(),
        Ok(Err(err)) => error_response(&err),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "count task failed"})),
        )
            .into_response(),
    }
}

async fn histogram_events(
    State(state): State<AppState>,
    Query(params): Query<RangeParams>,
    RawQuery(raw): RawQuery,
) -> Response {
    let raw = raw.as_deref().unwrap_or("");
    if let Err(response) = reject_page_params(raw, "/events/histogram") {
        return *response;
    }
    let bucket_ms = match bucket_ms_param(raw) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let from = match parse_bound(params.from, "from") {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let to = match parse_bound(params.to, "to") {
        Ok(value) => value,
        Err(response) => return *response,
    };
    if let Err(response) = reject_wide_histogram(from, to, bucket_ms) {
        return *response;
    }
    let predicates = match parse_filters(state.store.schema(), raw) {
        Ok(predicates) => predicates,
        Err(response) => return *response,
    };
    let store = Arc::clone(&state.store);
    let joined = tokio::task::spawn_blocking(move || {
        store.histogram_with_filter(from, to, bucket_ms, &predicates)
    })
    .await;
    match joined {
        Ok(Ok(buckets)) => {
            let body = serde_json::json!({
                "buckets": buckets
                    .into_iter()
                    .map(|bucket| serde_json::json!({
                        "start_ms": bucket.start_ms,
                        "count": bucket.count,
                    }))
                    .collect::<Vec<_>>()
            });
            (StatusCode::OK, Json(body)).into_response()
        }
        Ok(Err(err)) => error_response(&err),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "histogram task failed"})),
        )
            .into_response(),
    }
}

async fn stats_events(
    State(state): State<AppState>,
    Query(params): Query<RangeParams>,
    RawQuery(raw): RawQuery,
) -> Response {
    let raw = raw.as_deref().unwrap_or("");
    if let Err(response) = reject_stats_params(raw) {
        return *response;
    }
    let bucket_ms = match optional_bucket_ms(raw) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let from = match parse_bound(params.from, "from") {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let to = match parse_bound(params.to, "to") {
        Ok(value) => value,
        Err(response) => return *response,
    };
    if let Some(bucket_ms) = bucket_ms {
        if let Err(response) = reject_wide_histogram(from, to, bucket_ms) {
            return *response;
        }
    }
    let by = match group_field(raw) {
        Ok(by) => by,
        Err(response) => return *response,
    };
    if by.is_some() && bucket_ms.is_some() {
        return *bad_request("`by` cannot be combined with `bucket_ms`");
    }
    if let Some(field) = &by {
        if let Err(response) = reject_group_field(state.store.schema(), field) {
            return *response;
        }
    }
    let stats = match stat_requests(raw) {
        Ok(stats) => stats,
        Err(response) => return *response,
    };
    if let Err(response) = reject_stat_fields(state.store.schema(), &stats) {
        return *response;
    }
    let predicates = match parse_filters(state.store.schema(), raw) {
        Ok(predicates) => predicates,
        Err(response) => return *response,
    };
    let store = Arc::clone(&state.store);
    let grouped_by = by.clone();
    let joined = if let Some(by) = by {
        tokio::task::spawn_blocking(move || {
            store
                .aggregate_grouped(from, to, &predicates, &stats, &by)
                .map(StatsOutcome::Groups)
        })
        .await
    } else if let Some(bucket_ms) = bucket_ms {
        tokio::task::spawn_blocking(move || {
            store
                .aggregate_buckets(from, to, bucket_ms, &predicates, &stats)
                .map(StatsOutcome::Buckets)
        })
        .await
    } else {
        tokio::task::spawn_blocking(move || {
            store
                .aggregate_with_filter(from, to, &predicates, &stats)
                .map(StatsOutcome::Columns)
        })
        .await
    };
    match joined {
        Ok(Ok(StatsOutcome::Columns(columns))) => {
            let body = stats_body(state.store.schema(), &columns);
            (StatusCode::OK, Json(body)).into_response()
        }
        Ok(Ok(StatsOutcome::Buckets(buckets))) => {
            let body = stat_buckets_body(state.store.schema(), &buckets);
            (StatusCode::OK, Json(body)).into_response()
        }
        Ok(Ok(StatsOutcome::Groups(groups))) => {
            let body = groups_body(
                state.store.schema(),
                grouped_by.as_deref().unwrap_or(""),
                &groups,
            );
            (StatusCode::OK, Json(body)).into_response()
        }
        Ok(Err(err)) => error_response(&err),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "stats task failed"})),
        )
            .into_response(),
    }
}

enum StatsOutcome {
    Columns(Vec<eventer::ColumnStats>),
    Buckets(Vec<eventer::StatBucket>),
    Groups(Vec<eventer::StatGroup>),
}

fn reject_stats_params(query: &str) -> std::result::Result<(), Box<Response>> {
    if query.is_empty() {
        return Ok(());
    }
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let key = pair.split_once('=').map(|(key, _)| key).unwrap_or(pair);
        let key = percent_decode(key, "query")?;
        if key == "limit" || key == "offset" {
            return Err(bad_request(&format!(
                "`{key}` is not a parameter of /events/stats"
            )));
        }
    }
    Ok(())
}

fn stat_requests(query: &str) -> std::result::Result<Vec<eventer::StatRequest>, Box<Response>> {
    let mut requests: Vec<eventer::StatRequest> = Vec::new();
    let mut saw = false;
    if !query.is_empty() {
        for pair in query.split('&') {
            if pair.is_empty() {
                continue;
            }
            let (key, value) = match pair.split_once('=') {
                Some((key, value)) => (key, value),
                None => {
                    let key = percent_decode(pair, "query")?;
                    if matches!(key.as_str(), "sum" | "min" | "max") {
                        return Err(bad_request(&format!("`{key}` is missing a field name")));
                    }
                    continue;
                }
            };
            let key = percent_decode(key, "query")?;
            if !matches!(key.as_str(), "sum" | "min" | "max") {
                continue;
            }
            saw = true;
            let field = percent_decode(value, &key)?;
            if field.is_empty() {
                return Err(bad_request(&format!("`{key}` is missing a field name")));
            }
            if let Some(existing) = requests.iter_mut().find(|request| request.field == field) {
                match key.as_str() {
                    "sum" => existing.sum = true,
                    "min" => existing.min = true,
                    "max" => existing.max = true,
                    _ => {}
                }
            } else {
                requests.push(eventer::StatRequest {
                    field,
                    sum: key == "sum",
                    min: key == "min",
                    max: key == "max",
                });
            }
        }
    }
    if !saw {
        return Err(bad_request(
            "missing `sum`, `min`, or `max` query parameter",
        ));
    }
    Ok(requests)
}

fn group_field(query: &str) -> std::result::Result<Option<String>, Box<Response>> {
    let mut found = None;
    if query.is_empty() {
        return Ok(None);
    }
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = match pair.split_once('=') {
            Some(pair) => pair,
            None => {
                let key = percent_decode(pair, "query")?;
                if key == "by" {
                    return Err(bad_request("`by` is missing a field name"));
                }
                continue;
            }
        };
        let key = percent_decode(key, "query")?;
        if key != "by" {
            continue;
        }
        if found.is_some() {
            return Err(bad_request("`by` was given more than once"));
        }
        let field = percent_decode(value, "by")?;
        if field.is_empty() {
            return Err(bad_request("`by` is missing a field name"));
        }
        found = Some(field);
    }
    Ok(found)
}

fn reject_group_field(
    schema: &eventer::Schema,
    name: &str,
) -> std::result::Result<(), Box<Response>> {
    let Some(field) = schema.fields.iter().find(|field| field.name == name) else {
        return Err(bad_request(&format!("unknown group field `{name}`")));
    };
    if matches!(
        field.ty,
        eventer::FieldType::Float | eventer::FieldType::Json
    ) {
        return Err(bad_request(&format!(
            "`by` does not apply to {} column `{name}`",
            field.ty.name()
        )));
    }
    Ok(())
}

fn reject_stat_fields(
    schema: &eventer::Schema,
    stats: &[eventer::StatRequest],
) -> std::result::Result<(), Box<Response>> {
    for stat in stats {
        let Some(field) = schema.fields.iter().find(|field| field.name == stat.field) else {
            return Err(bad_request(&format!("unknown stat field `{}`", stat.field)));
        };
        if matches!(
            field.ty,
            eventer::FieldType::Bool
                | eventer::FieldType::String
                | eventer::FieldType::Text
                | eventer::FieldType::Json
        ) {
            let op = if stat.sum {
                "sum"
            } else if stat.min {
                "min"
            } else {
                "max"
            };
            return Err(bad_request(&format!(
                "`{op}` does not apply to {} column `{}`",
                field.ty.name(),
                field.name
            )));
        }
    }
    Ok(())
}

fn stats_body(schema: &eventer::Schema, columns: &[eventer::ColumnStats]) -> Value {
    Value::Object(Map::from_iter([(
        "columns".to_string(),
        Value::Object(columns_object(schema, columns)),
    )]))
}

fn stat_buckets_body(schema: &eventer::Schema, buckets: &[eventer::StatBucket]) -> Value {
    let list = buckets
        .iter()
        .map(|bucket| {
            json!({
                "start_ms": bucket.start_ms,
                "columns": columns_object(schema, &bucket.columns),
            })
        })
        .collect::<Vec<_>>();
    json!({"buckets": list})
}

fn groups_body(schema: &eventer::Schema, by: &str, groups: &[eventer::StatGroup]) -> Value {
    let ty = schema
        .fields
        .iter()
        .find(|field| field.name == by)
        .map(|field| field.ty)
        .unwrap_or(eventer::FieldType::String);
    let list = groups
        .iter()
        .map(|group| {
            json!({
                "key": group_key_json(ty, &group.key),
                "columns": columns_object(schema, &group.columns),
            })
        })
        .collect::<Vec<_>>();
    json!({"groups": list})
}

fn columns_object(
    schema: &eventer::Schema,
    columns: &[eventer::ColumnStats],
) -> Map<String, Value> {
    let mut out = Map::new();
    for column in columns {
        let Some(field) = schema
            .fields
            .iter()
            .find(|field| field.name == column.field)
        else {
            continue;
        };
        let mut body = Map::new();
        if let Some(sum) = column.sum {
            body.insert("sum".to_string(), sum_json(field.ty, sum));
        }
        if let Some(min) = &column.min {
            body.insert("min".to_string(), extreme_json(field.ty, min));
        }
        if let Some(max) = &column.max {
            body.insert("max".to_string(), extreme_json(field.ty, max));
        }
        out.insert(column.field.clone(), Value::Object(body));
    }
    out
}

fn group_key_json(ty: eventer::FieldType, value: &eventer::Scalar) -> Value {
    match (ty, value) {
        (_, eventer::Scalar::Null) => Value::Null,
        (eventer::FieldType::Int, eventer::Scalar::Int(number)) => json!(number),
        (eventer::FieldType::Timestamp, eventer::Scalar::Timestamp(number)) => json!(number),
        (eventer::FieldType::Decimal { scale }, eventer::Scalar::Decimal(number)) => {
            Value::String(eventer::format_decimal(*number, scale))
        }
        (eventer::FieldType::Bool, eventer::Scalar::Bool(flag)) => json!(flag),
        (eventer::FieldType::String | eventer::FieldType::Text, eventer::Scalar::Str(text)) => {
            Value::String(text.clone())
        }
        _ => Value::Null,
    }
}

fn sum_json(ty: eventer::FieldType, sum: eventer::StatSum) -> Value {
    match sum {
        eventer::StatSum::Float { total, contributed } => float_sum_json(total, contributed),
        eventer::StatSum::Exact { total, contributed } => {
            let text = match ty {
                eventer::FieldType::Decimal { scale } if contributed => {
                    eventer::format_decimal(total, scale)
                }
                _ => total.to_string(),
            };
            Value::String(text)
        }
    }
}

fn extreme_json(ty: eventer::FieldType, value: &Option<eventer::Scalar>) -> Value {
    let Some(value) = value else {
        return Value::Null;
    };
    match (ty, value) {
        (eventer::FieldType::Int, eventer::Scalar::Int(number)) => json!(number),
        (eventer::FieldType::Timestamp, eventer::Scalar::Timestamp(number)) => json!(number),
        (eventer::FieldType::Float, eventer::Scalar::Float(number)) => float_extreme_json(*number),
        (eventer::FieldType::Decimal { scale }, eventer::Scalar::Decimal(number)) => {
            Value::String(eventer::format_decimal(*number, scale))
        }
        _ => Value::Null,
    }
}

/// An empty float sum is the JSON integer `0`. A sum that had contributing
/// values keeps its IEEE form, including positive zero as `0.0`. The store
/// rejects a non-finite accumulator before this runs.
fn float_sum_json(total: f64, contributed: bool) -> Value {
    if !contributed {
        return Value::Number(0.into());
    }
    Value::Number(Number::from_f64(total).expect("float sum is finite"))
}

/// `min` and `max` use the same JSON number as `GET /events`, including `0.0`.
fn float_extreme_json(value: f64) -> Value {
    Value::Number(Number::from_f64(value).expect("float extreme is finite"))
}

/// Refuse a span that would emit more than [`eventer::MAX_HISTOGRAM_BUCKETS`]
/// before the store is asked to read anything.
fn reject_wide_histogram(
    from: i64,
    to: i64,
    bucket_ms: i64,
) -> std::result::Result<(), Box<Response>> {
    if from > to {
        return Ok(());
    }
    let width = i128::from(bucket_ms);
    let first = i128::from(from).div_euclid(width) * width;
    let last = i128::from(to).div_euclid(width) * width;
    let buckets = (last - first) / width + 1;
    if buckets > i128::from(eventer::MAX_HISTOGRAM_BUCKETS as u32) {
        return Err(bad_request("histogram would emit more than 4096 buckets"));
    }
    if i64::try_from(first).is_err() {
        return Err(bad_request("histogram bucket start is outside i64"));
    }
    Ok(())
}

fn bucket_ms_param(query: &str) -> std::result::Result<i64, Box<Response>> {
    optional_bucket_ms(query)?.ok_or_else(|| bad_request("missing `bucket_ms` query parameter"))
}

/// `None` when `bucket_ms` is absent. A present value that is not a positive
/// integer is an error, including zero, a negative, and a non-integer.
fn optional_bucket_ms(query: &str) -> std::result::Result<Option<i64>, Box<Response>> {
    let mut found = None;
    if !query.is_empty() {
        for pair in query.split('&') {
            if pair.is_empty() {
                continue;
            }
            let Some((key, value)) = pair.split_once('=') else {
                if percent_decode(pair, "bucket_ms")? == "bucket_ms" {
                    return Err(bad_request("`bucket_ms` must be a positive integer"));
                }
                continue;
            };
            if percent_decode(key, "bucket_ms")? != "bucket_ms" {
                continue;
            }
            if found.is_some() {
                return Err(bad_request("`bucket_ms` was given more than once"));
            }
            let value = percent_decode(value, "bucket_ms")?;
            let parsed = value
                .parse::<i64>()
                .map_err(|_| bad_request("`bucket_ms` must be a positive integer"))?;
            if parsed <= 0 {
                return Err(bad_request("`bucket_ms` must be a positive integer"));
            }
            found = Some(parsed);
        }
    }
    Ok(found)
}

fn reject_bucket_param(query: &str, route: &str) -> std::result::Result<(), Box<Response>> {
    if query.is_empty() {
        return Ok(());
    }
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let key = pair.split_once('=').map(|(key, _)| key).unwrap_or(pair);
        let key = percent_decode(key, "query")?;
        if key == "bucket_ms" {
            return Err(bad_request(&format!(
                "`bucket_ms` is not a parameter of {route}"
            )));
        }
    }
    Ok(())
}

fn reject_page_params(query: &str, route: &str) -> std::result::Result<(), Box<Response>> {
    if query.is_empty() {
        return Ok(());
    }
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let key = pair.split_once('=').map(|(key, _)| key).unwrap_or(pair);
        let key = percent_decode(key, "query")?;
        if key == "limit" || key == "offset" {
            return Err(bad_request(&format!(
                "`{key}` is not a parameter of {route}"
            )));
        }
    }
    Ok(())
}

fn eq_specs(query: &str) -> std::result::Result<Vec<String>, Box<Response>> {
    let mut specs = Vec::new();
    if query.is_empty() {
        return Ok(specs);
    }
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let Some((key, value)) = pair.split_once('=') else {
            if percent_decode(pair, "eq")? == "eq" {
                return Err(bad_request("`eq` must be field=value"));
            }
            continue;
        };
        if percent_decode(key, "eq")? == "eq" {
            specs.push(percent_decode(value, "eq")?);
        }
    }
    Ok(specs)
}

fn page_params(query: &str) -> std::result::Result<(u64, Option<u64>), Box<Response>> {
    let mut limit = None;
    let mut offset = None;
    if query.is_empty() {
        return Ok((0, None));
    }
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let Some((key, value)) = pair.split_once('=') else {
            let key = percent_decode(pair, "query")?;
            if key == "limit" || key == "offset" {
                return Err(bad_request(&format!("`{key}` must be an integer")));
            }
            continue;
        };
        let key = percent_decode(key, "query")?;
        if key != "limit" && key != "offset" {
            continue;
        }
        if (key == "limit" && limit.is_some()) || (key == "offset" && offset.is_some()) {
            return Err(bad_request(&format!("`{key}` was given more than once")));
        }
        let value = percent_decode(value, &key)?;
        let parsed = parse_u64_digits(&value).ok_or_else(|| {
            bad_request(&format!(
                "`{key}` must be a {} integer",
                if key == "limit" {
                    "positive"
                } else {
                    "non-negative"
                }
            ))
        })?;
        if key == "limit" {
            if parsed == 0 {
                return Err(bad_request("`limit` must be a positive integer"));
            }
            limit = Some(parsed);
        } else {
            offset = Some(parsed);
        }
    }
    Ok((offset.unwrap_or(0), limit))
}

fn parse_u64_digits(value: &str) -> Option<u64> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

fn percent_decode(input: &str, what: &str) -> std::result::Result<String, Box<Response>> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                out.push(b' ');
                index += 1;
            }
            b'%' => {
                if index + 2 >= bytes.len() {
                    return Err(bad_request(&format!(
                        "`{what}` has an invalid percent-encoding"
                    )));
                }
                let hex = &input[index + 1..index + 3];
                let byte = u8::from_str_radix(hex, 16).map_err(|_| {
                    bad_request(&format!("`{what}` has an invalid percent-encoding"))
                })?;
                out.push(byte);
                index += 3;
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(out).map_err(|_| bad_request(&format!("`{what}` is not valid UTF-8")))
}

fn bad_request(message: &str) -> Box<Response> {
    Box::new((StatusCode::BAD_REQUEST, Json(json!({"error": message}))).into_response())
}

fn parse_filters(
    schema: &eventer::Schema,
    query: &str,
) -> std::result::Result<Vec<Predicate>, Box<Response>> {
    let mut predicates = Vec::new();
    for spec in eq_specs(query)? {
        predicates.push(parse_eq(schema, &spec)?);
    }
    for (op, spec) in cmp_specs(query)? {
        predicates.push(parse_cmp(schema, op, &spec)?);
    }
    Ok(predicates)
}

fn cmp_specs(query: &str) -> std::result::Result<Vec<(eventer::CmpOp, String)>, Box<Response>> {
    let mut specs = Vec::new();
    if query.is_empty() {
        return Ok(specs);
    }
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let Some((key, value)) = pair.split_once('=') else {
            let key = percent_decode(pair, "query")?;
            if cmp_op(&key).is_some() {
                return Err(bad_request(&format!("`{key}` must be field=value")));
            }
            continue;
        };
        let key = percent_decode(key, "query")?;
        let Some(op) = cmp_op(&key) else {
            continue;
        };
        specs.push((op, percent_decode(value, &key)?));
    }
    Ok(specs)
}

fn cmp_op(key: &str) -> Option<eventer::CmpOp> {
    match key {
        "gt" => Some(eventer::CmpOp::Gt),
        "ge" => Some(eventer::CmpOp::Ge),
        "lt" => Some(eventer::CmpOp::Lt),
        "le" => Some(eventer::CmpOp::Le),
        _ => None,
    }
}

fn parse_cmp(
    schema: &eventer::Schema,
    op: eventer::CmpOp,
    spec: &str,
) -> std::result::Result<Predicate, Box<Response>> {
    let op_name = match op {
        eventer::CmpOp::Gt => "gt",
        eventer::CmpOp::Ge => "ge",
        eventer::CmpOp::Lt => "lt",
        eventer::CmpOp::Le => "le",
    };
    let Some((name, literal)) = spec.split_once('=') else {
        return Err(bad_request(&format!("`{op_name}` must be field=value")));
    };
    if name.is_empty() {
        return Err(bad_request(&format!("`{op_name}` is missing a field name")));
    }
    if literal.is_empty() {
        return Err(bad_request(&format!("`{op_name}` is missing a value")));
    }
    let Some(field) = schema.fields.iter().find(|field| field.name == name) else {
        return Err(bad_request(&format!("unknown filter field `{name}`")));
    };
    if matches!(
        field.ty,
        eventer::FieldType::Bool
            | eventer::FieldType::String
            | eventer::FieldType::Text
            | eventer::FieldType::Json
    ) {
        return Err(bad_request(&format!(
            "`{op_name}` does not apply to {} column `{name}`",
            field.ty.name()
        )));
    }
    match eventer::scalar_from_literal(field.ty, literal) {
        Ok(scalar) => Ok(Predicate::Cmp(name.to_string(), op, scalar)),
        Err(err) => Err(Box::new(error_response(&err))),
    }
}

fn parse_eq(schema: &eventer::Schema, spec: &str) -> std::result::Result<Predicate, Box<Response>> {
    let Some((name, literal)) = spec.split_once('=') else {
        return Err(Box::new(
            (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "`eq` must be field=value"})),
            )
                .into_response(),
        ));
    };
    if name.is_empty() {
        return Err(Box::new(
            (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "`eq` is missing a field name"})),
            )
                .into_response(),
        ));
    }
    let Some(field) = schema.fields.iter().find(|field| field.name == name) else {
        return Err(Box::new(
            (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("unknown filter field `{name}`")})),
            )
                .into_response(),
        ));
    };
    match eventer::scalar_from_literal(field.ty, literal) {
        Ok(scalar) => Ok(Predicate::Eq(name.to_string(), scalar)),
        Err(err) => Err(Box::new(error_response(&err))),
    }
}

fn parse_bound(raw: Option<String>, name: &str) -> std::result::Result<i64, Box<Response>> {
    let Some(raw) = raw else {
        return Err(Box::new(
            (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("missing `{name}` query parameter")})),
            )
                .into_response(),
        ));
    };
    raw.parse::<i64>().map_err(|_| {
        Box::new(
            (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("`{name}` must be a unix millisecond integer")})),
            )
                .into_response(),
        )
    })
}

fn error_response(err: &eventer::Error) -> Response {
    let status = match err {
        eventer::Error::Event(_) | eventer::Error::Json(_) | eventer::Error::Schema(_) => {
            StatusCode::BAD_REQUEST
        }
        eventer::Error::Closed => StatusCode::SERVICE_UNAVAILABLE,
        eventer::Error::Io(_) | eventer::Error::Corrupt(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, Json(json!({"error": err.to_string()}))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use std::fs;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tower::ServiceExt;

    #[tokio::test]
    async fn post_then_get_filters_by_time() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("eventer-http-{}-{}", std::process::id(), nanos));
        fs::create_dir_all(&root).unwrap();
        let schema = root.join("schema.json");
        fs::write(
            &schema,
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "action", "type": "string"},
                    {"name": "amount", "type": "decimal", "scale": 2}
                ]
            }"#,
        )
        .unwrap();
        let options = eventer::StoreOptions {
            linger: Duration::from_millis(1),
            parser_threads: 1,
            compress_threads: 1,
            ..eventer::StoreOptions::default()
        };
        let store =
            Arc::new(eventer::Store::open_with(root.join("data"), &schema, options).unwrap());
        let app = router(Arc::clone(&store));

        let created = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/events")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"ts":1000,"action":"click","amount":"1.25"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);

        let rejected = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/events")
                    .body(Body::from(r#"{"action":"click"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);

        let listed = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/events?from=1000&to=1000")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(listed.status(), StatusCode::OK);
        let bytes = listed.into_body().collect().await.unwrap().to_bytes();
        let rows: Vec<Value> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["action"], "click");
        assert_eq!(rows[0]["amount"], "1.25");

        let second = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/events")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"ts":1001,"action":"view","amount":"2.00"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::CREATED);

        let filtered = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/events?from=1000&to=2000&eq=action=click")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(filtered.status(), StatusCode::OK);
        let filtered_bytes = filtered.into_body().collect().await.unwrap().to_bytes();
        let filtered_rows: Vec<Value> = serde_json::from_slice(&filtered_bytes).unwrap();
        assert_eq!(filtered_rows.len(), 1);
        assert_eq!(filtered_rows[0]["action"], "click");

        let missing = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/events?from=5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::BAD_REQUEST);

        let encoded = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/events?from=1000&to=2000&eq=action%3Dclick")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(encoded.status(), StatusCode::OK);

        let bare = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/events?from=1000&to=2000&eq")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(bare.status(), StatusCode::BAD_REQUEST);

        let bad_pct = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/events?from=1000&to=2000&eq=action%3D%80")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(bad_pct.status(), StatusCode::BAD_REQUEST);

        store.close().unwrap();
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn get_events_limits_and_skips_in_ingest_order() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "eventer-http-page-{}-{}",
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&root).unwrap();
        let schema = root.join("schema.json");
        fs::write(
            &schema,
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "action", "type": "string"}
                ]
            }"#,
        )
        .unwrap();
        let options = eventer::StoreOptions {
            block_rows: 2,
            linger: Duration::from_millis(1),
            parser_threads: 1,
            compress_threads: 1,
            ..eventer::StoreOptions::default()
        };
        let data = root.join("data");
        let store = Arc::new(eventer::Store::open_with(&data, &schema, options.clone()).unwrap());
        let app = router(Arc::clone(&store));

        for (ts, action) in [
            (10, "click"),
            (20, "view"),
            (30, "click"),
            (40, "view"),
            (50, "click"),
        ] {
            let created = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/events")
                        .header("content-type", "application/json")
                        .body(Body::from(format!(r#"{{"ts":{ts},"action":"{action}"}}"#)))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(created.status(), StatusCode::CREATED);
        }

        async fn timestamps(app: Router, uri: &str) -> (StatusCode, Value) {
            let response = app
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            let status = response.status();
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            let body: Value = serde_json::from_slice(&bytes).unwrap();
            (status, body)
        }

        let (status, body) = timestamps(app.clone(), "/events?from=10&to=50").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body.as_array()
                .unwrap()
                .iter()
                .map(|row| row["ts"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![10, 20, 30, 40, 50]
        );

        let (status, body) = timestamps(app.clone(), "/events?from=10&to=50&limit=2").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body.as_array()
                .unwrap()
                .iter()
                .map(|row| row["ts"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![10, 20]
        );

        let (status, body) =
            timestamps(app.clone(), "/events?from=10&to=50&offset=2&limit=2").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body.as_array()
                .unwrap()
                .iter()
                .map(|row| row["ts"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![30, 40]
        );

        let (status, body) = timestamps(app.clone(), "/events?from=10&to=50&offset=4").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body.as_array()
                .unwrap()
                .iter()
                .map(|row| row["ts"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![50]
        );

        let (status, body) = timestamps(app.clone(), "/events?from=20&to=40&limit=2").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body.as_array()
                .unwrap()
                .iter()
                .map(|row| row["ts"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![20, 30]
        );

        let (status, body) =
            timestamps(app.clone(), "/events?from=10&to=50&eq=action=click&limit=2").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body.as_array()
                .unwrap()
                .iter()
                .map(|row| row["ts"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![10, 30]
        );

        let (status, body) = timestamps(app.clone(), "/events?from=10&to=50&offset=9").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.as_array().unwrap().len(), 0);

        for uri in [
            "/events?from=10&to=50&limit=0",
            "/events?from=10&to=50&limit=-1",
            "/events?from=10&to=50&offset=-1",
            "/events?from=10&to=50&limit=1.5",
            "/events?from=10&to=50&limit=nope",
        ] {
            let (status, body) = timestamps(app.clone(), uri).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
            assert!(body.is_object(), "{uri} body {body}");
            assert!(body.get("error").is_some(), "{uri}");
        }

        store.close().unwrap();
        let reopened = Arc::new(eventer::Store::open_with(&data, &schema, options).unwrap());
        let app = router(Arc::clone(&reopened));
        let (status, body) = timestamps(app, "/events?from=10&to=50&limit=2").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body.as_array()
                .unwrap()
                .iter()
                .map(|row| row["ts"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![10, 20]
        );
        reopened.close().unwrap();
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn post_batch_is_durable_and_atomic() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "eventer-http-batch-{}-{}",
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&root).unwrap();
        let schema = root.join("schema.json");
        fs::write(
            &schema,
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "action", "type": "string"},
                    {"name": "amount", "type": "decimal", "scale": 2}
                ]
            }"#,
        )
        .unwrap();
        let options = eventer::StoreOptions {
            linger: Duration::from_millis(1),
            parser_threads: 1,
            compress_threads: 1,
            ..eventer::StoreOptions::default()
        };
        let data = root.join("data");
        let store = Arc::new(eventer::Store::open_with(&data, &schema, options.clone()).unwrap());
        let app = router(Arc::clone(&store));

        let one = post(&app, r#"{"ts":1000,"action":"click","amount":"1.25"}"#).await;
        assert_eq!(one.0, StatusCode::CREATED);
        assert_eq!(one.1, json!({"ok": true}));

        let batch = post(
            &app,
            r#"[{"ts":2000,"action":"view","amount":"2.00"},{"ts":2001,"action":"buy","amount":"3.50"}]"#,
        )
        .await;
        assert_eq!(batch.0, StatusCode::CREATED);
        assert_eq!(batch.1, json!({"ok": true, "count": 2}));

        store.close().unwrap();
        let store = Arc::new(eventer::Store::open_with(&data, &schema, options.clone()).unwrap());
        let app = router(Arc::clone(&store));
        let rows = query(&app, "/events?from=1000&to=3000").await;
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[1]["action"], "view");
        assert_eq!(rows[2]["action"], "buy");

        let rejected = post(
            &app,
            r#"[{"ts":4000,"action":"nope","amount":"1.00"},{"action":"missing","amount":"1.00"}]"#,
        )
        .await;
        assert_eq!(rejected.0, StatusCode::BAD_REQUEST);
        let after_reject = query(&app, "/events?from=0&to=9000").await;
        assert_eq!(after_reject.len(), 3);
        assert!(after_reject.iter().all(|row| row["action"] != "nope"));

        let empty = post(&app, "[]").await;
        assert_eq!(empty.0, StatusCode::BAD_REQUEST);
        let nested = post(
            &app,
            r#"[{"ts":4100,"action":"still-no","amount":"1.00"},[]]"#,
        )
        .await;
        assert_eq!(nested.0, StatusCode::BAD_REQUEST);
        let typed = post(&app, r#"[{"ts":4200,"action":"bad-amount","amount":1.25}]"#).await;
        assert_eq!(typed.0, StatusCode::BAD_REQUEST);

        let followed = post(&app, r#"{"ts":5000,"action":"kept","amount":"4.00"}"#).await;
        assert_eq!(followed.0, StatusCode::CREATED);
        assert_eq!(followed.1, json!({"ok": true}));

        store.close().unwrap();
        let store = Arc::new(eventer::Store::open_with(&data, &schema, options).unwrap());
        let app = router(Arc::clone(&store));
        let rows = query(&app, "/events?from=0&to=9000").await;
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[3]["action"], "kept");
        assert_eq!(rows[3]["ts"], 5000);

        store.close().unwrap();
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn post_drop_removes_blocks_older_than_the_caller_cutoff() {
        let (root, schema, data) = temp_store("drop-blocks");
        let options = store_options(1, eventer::StoreOptions::default().segment_bytes);
        let store = Arc::new(eventer::Store::open_with(&data, &schema, options.clone()).unwrap());
        let app = router(Arc::clone(&store));

        for ts in [1000, 2000, 3000] {
            let (status, body) = post_json(
                &app,
                "/events",
                &format!(r#"{{"ts":{ts},"action":"click","amount":"1.00"}}"#),
            )
            .await;
            assert_eq!(status, StatusCode::CREATED, "{body}");
        }

        let (status, body) = post_json(&app, "/events/drop", r#"{"before_ms":1000}"#).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, json!({"ok": true}));
        assert_eq!(timestamps_of(&app, 0, 9000).await, vec![1000, 2000, 3000]);

        let (status, body) = post_json(&app, "/events/drop", r#"{"before_ms":1001}"#).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, json!({"ok": true}));
        assert_eq!(timestamps_of(&app, 0, 9000).await, vec![2000, 3000]);

        store.close().unwrap();
        let reopened = Arc::new(eventer::Store::open_with(&data, &schema, options).unwrap());
        let app = router(Arc::clone(&reopened));
        assert_eq!(timestamps_of(&app, 0, 9000).await, vec![2000, 3000]);
        reopened.close().unwrap();
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn post_drop_keeps_an_older_row_that_shares_a_live_block() {
        let (root, schema, data) = temp_store("drop-shared");
        let options = store_options(2, eventer::StoreOptions::default().segment_bytes);
        let store = Arc::new(eventer::Store::open_with(&data, &schema, options).unwrap());
        let app = router(Arc::clone(&store));

        let (status, body) = post_json(
            &app,
            "/events",
            r#"[{"ts":1000,"action":"click","amount":"1.00"},{"ts":3000,"action":"view","amount":"2.00"}]"#,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");

        let (status, body) = post_json(&app, "/events/drop", r#"{"before_ms":2000}"#).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, json!({"ok": true}));
        assert_eq!(timestamps_of(&app, 0, 9000).await, vec![1000, 3000]);

        store.close().unwrap();
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn post_drop_deletes_files_when_every_block_in_a_segment_is_expired() {
        let (root, schema, data) = temp_store("drop-segment");
        let options = store_options(1, 1);
        let store = Arc::new(eventer::Store::open_with(&data, &schema, options).unwrap());
        let app = router(Arc::clone(&store));

        for ts in [1000, 2000] {
            let (status, body) = post_json(
                &app,
                "/events",
                &format!(r#"{{"ts":{ts},"action":"click","amount":"1.00"}}"#),
            )
            .await;
            assert_eq!(status, StatusCode::CREATED, "{body}");
        }

        let expired = ["seg-000001.dat", "seg-000001.idx", "seg-000001.zon"];
        for name in expired {
            assert!(
                data.join(name).is_file(),
                "segment 1 should have {name} before the drop"
            );
        }
        let dict = data.join("seg-000001.dict");

        let (status, body) = post_json(&app, "/events/drop", r#"{"before_ms":1001}"#).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, json!({"ok": true}));
        for name in expired {
            assert!(
                !data.join(name).exists(),
                "expired segment file still present: {name}"
            );
        }
        assert!(!dict.exists(), "expired segment dictionary still present");
        assert!(data.join("seg-000002.dat").is_file());
        assert_eq!(timestamps_of(&app, 0, 9000).await, vec![2000]);

        store.close().unwrap();
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn post_drop_rejects_a_bad_body_and_leaves_rows_in_place() {
        let (root, schema, data) = temp_store("drop-reject");
        let options = store_options(1, eventer::StoreOptions::default().segment_bytes);
        let store = Arc::new(eventer::Store::open_with(&data, &schema, options).unwrap());
        let app = router(Arc::clone(&store));

        let (status, body) = post_json(
            &app,
            "/events",
            r#"{"ts":1000,"action":"click","amount":"1.00"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let (status, body) = post_json(
            &app,
            "/events",
            r#"{"ts":2000,"action":"view","amount":"2.00"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");

        let rejected = [
            "{}",
            r#"{"before_ms":1.5}"#,
            r#"{"before_ms":"1000"}"#,
            r#"{"before_ms":null}"#,
            r#"{"before_ms":true}"#,
            r#"{"before_ms":9223372036854775808}"#,
            "[]",
            r#"[{"before_ms":1000}]"#,
            "1000",
            "null",
            "true",
            "not json",
        ];
        for payload in rejected {
            let (status, body) = post_json(&app, "/events/drop", payload).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{payload} -> {body}");
            assert!(body.is_object(), "{payload} body {body}");
            assert!(body.get("error").is_some(), "{payload}");
            assert_eq!(timestamps_of(&app, 0, 9000).await, vec![1000, 2000]);
        }

        store.close().unwrap();
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn post_drop_rejects_a_non_json_content_type_and_leaves_rows_in_place() {
        let (root, schema, data) = temp_store("drop-content-type");
        let options = store_options(1, eventer::StoreOptions::default().segment_bytes);
        let store = Arc::new(eventer::Store::open_with(&data, &schema, options).unwrap());
        let app = router(Arc::clone(&store));

        for ts in [1000, 2000] {
            let (status, body) = post_json(
                &app,
                "/events",
                &format!(r#"{{"ts":{ts},"action":"click","amount":"1.00"}}"#),
            )
            .await;
            assert_eq!(status, StatusCode::CREATED, "{body}");
        }

        let cutoff = r#"{"before_ms":9223372036854775807}"#;
        for content_type in [None, Some("text/plain"), Some("application/jsonp")] {
            let mut builder = Request::builder().method("POST").uri("/events/drop");
            if let Some(content_type) = content_type {
                builder = builder.header("content-type", content_type);
            }
            let response = app
                .clone()
                .oneshot(builder.body(Body::from(cutoff.to_string())).unwrap())
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::BAD_REQUEST,
                "{content_type:?}"
            );
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            let body: Value = serde_json::from_slice(&bytes).unwrap();
            assert!(body.get("error").is_some(), "{content_type:?} body {body}");
            assert_eq!(timestamps_of(&app, 0, 9000).await, vec![1000, 2000]);
        }

        let (status, body) = post_with_type(
            &app,
            "/events/drop",
            "application/json; charset=utf-8",
            r#"{"before_ms":1000}"#,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(timestamps_of(&app, 0, 9000).await, vec![1000, 2000]);

        store.close().unwrap();
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn count_matches_unpaged_get_and_rejects_page_params() {
        let (root, schema, data) = temp_store("count");
        let options = store_options(
            eventer::StoreOptions::default().block_rows,
            eventer::StoreOptions::default().segment_bytes,
        );
        let store = Arc::new(eventer::Store::open_with(&data, &schema, options).unwrap());
        let app = router(Arc::clone(&store));

        for (ts, action) in [
            (1000, "click"),
            (2000, "view"),
            (3000, "click"),
            (4000, "buy"),
            (5000, "view"),
        ] {
            let body = format!(r#"{{"ts":{ts},"action":"{action}","amount":"1.00"}}"#);
            let (status, _) = post(&app, &body).await;
            assert_eq!(status, StatusCode::CREATED);
        }

        let all_rows = query(&app, "/events?from=1000&to=5000").await;
        assert_eq!(all_rows.len(), 5);
        let all = get_json(&app, "/events/count?from=1000&to=5000").await;
        assert_eq!(all.0, StatusCode::OK);
        assert_eq!(all.1, json!({"count": 5}));

        let ranged_rows = query(&app, "/events?from=2000&to=3000").await;
        assert_eq!(ranged_rows.len(), 2);
        let ranged = get_json(&app, "/events/count?from=2000&to=3000").await;
        assert_eq!(ranged.0, StatusCode::OK);
        assert_eq!(ranged.1["count"], json!(ranged_rows.len()));

        let buy_rows = query(&app, "/events?from=1000&to=5000&eq=action=buy").await;
        assert_eq!(buy_rows.len(), 1);
        let buy = get_json(&app, "/events/count?from=1000&to=5000&eq=action=buy").await;
        assert_eq!(buy.0, StatusCode::OK);
        assert_eq!(buy.1["count"], json!(buy_rows.len()));

        let empty = get_json(&app, "/events/count?from=9000&to=9000").await;
        assert_eq!(empty.0, StatusCode::OK);
        assert_eq!(empty.1, json!({"count": 0}));

        let limited = get_json(&app, "/events/count?from=1000&to=5000&limit=1").await;
        assert_eq!(limited.0, StatusCode::BAD_REQUEST);
        assert!(limited.1.get("count").is_none());
        assert!(limited.1["error"].is_string());

        let skipped = get_json(&app, "/events/count?from=1000&to=5000&offset=0").await;
        assert_eq!(skipped.0, StatusCode::BAD_REQUEST);
        assert!(skipped.1.get("count").is_none());

        let missing = get_json(&app, "/events/count?from=1000").await;
        assert_eq!(missing.0, StatusCode::BAD_REQUEST);
        let bad_bound = get_json(&app, "/events/count?from=1000&to=nope").await;
        assert_eq!(bad_bound.0, StatusCode::BAD_REQUEST);
        let bad_eq = get_json(&app, "/events/count?from=1000&to=5000&eq=action").await;
        assert_eq!(bad_eq.0, StatusCode::BAD_REQUEST);

        let still = query(&app, "/events?from=1000&to=5000").await;
        assert_eq!(still.len(), 5);

        store.close().unwrap();
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn histogram_buckets_match_count_and_reject_a_wide_span() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "eventer-http-histogram-{}-{}",
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&root).unwrap();
        let schema = root.join("schema.json");
        fs::write(
            &schema,
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "action", "type": "string"}
                ]
            }"#,
        )
        .unwrap();
        let options = eventer::StoreOptions {
            block_rows: 2,
            linger: Duration::from_millis(1),
            parser_threads: 1,
            compress_threads: 1,
            ..eventer::StoreOptions::default()
        };
        let store =
            Arc::new(eventer::Store::open_with(root.join("data"), &schema, options).unwrap());
        let app = router(Arc::clone(&store));

        for (ts, action) in [(1000, "click"), (2000, "view"), (3000, "click")] {
            let (status, _) = post(&app, &format!(r#"{{"ts":{ts},"action":"{action}"}}"#)).await;
            assert_eq!(status, StatusCode::CREATED);
        }

        let fine = get_json(&app, "/events/histogram?from=1000&to=3000&bucket_ms=1000").await;
        assert_eq!(fine.0, StatusCode::OK);
        assert_eq!(
            fine.1,
            json!({
                "buckets": [
                    {"start_ms": 1000, "count": 1},
                    {"start_ms": 2000, "count": 1},
                    {"start_ms": 3000, "count": 1}
                ]
            })
        );

        let wide = get_json(&app, "/events/histogram?from=1000&to=3000&bucket_ms=2000").await;
        assert_eq!(wide.0, StatusCode::OK);
        assert_eq!(
            wide.1,
            json!({
                "buckets": [
                    {"start_ms": 0, "count": 1},
                    {"start_ms": 2000, "count": 2}
                ]
            })
        );

        let gapped = get_json(&app, "/events/histogram?from=1000&to=5000&bucket_ms=1000").await;
        assert_eq!(gapped.0, StatusCode::OK);
        let buckets = gapped.1["buckets"].as_array().unwrap();
        assert!(buckets.iter().any(|bucket| bucket["count"] == 0));
        let sum: i64 = buckets
            .iter()
            .map(|bucket| bucket["count"].as_i64().unwrap())
            .sum();
        let counted = get_json(&app, "/events/count?from=1000&to=5000").await;
        assert_eq!(sum, counted.1["count"].as_i64().unwrap());

        let filtered = get_json(
            &app,
            "/events/histogram?from=1000&to=3000&bucket_ms=1000&eq=action=missing",
        )
        .await;
        assert_eq!(filtered.0, StatusCode::OK);
        let filtered_sum: i64 = filtered.1["buckets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|bucket| bucket["count"].as_i64().unwrap())
            .sum();
        let filtered_count =
            get_json(&app, "/events/count?from=1000&to=3000&eq=action=missing").await;
        assert_eq!(filtered_sum, 0);
        assert_eq!(filtered_sum, filtered_count.1["count"].as_i64().unwrap());
        assert_eq!(filtered.1["buckets"].as_array().unwrap().len(), 3);

        let clicks = get_json(
            &app,
            "/events/histogram?from=1000&to=3000&bucket_ms=1000&eq=action=click",
        )
        .await;
        assert_eq!(clicks.0, StatusCode::OK);
        assert_eq!(
            clicks.1,
            json!({
                "buckets": [
                    {"start_ms": 1000, "count": 1},
                    {"start_ms": 2000, "count": 0},
                    {"start_ms": 3000, "count": 1}
                ]
            })
        );

        for uri in [
            "/events/histogram?from=1000&to=3000",
            "/events/histogram?from=1000&to=3000&bucket_ms=0",
            "/events/histogram?from=1000&to=3000&bucket_ms=-1",
            "/events/histogram?from=1000&to=3000&bucket_ms=1.5",
            "/events/histogram?from=1000&to=3000&bucket_ms=nope",
            "/events/histogram?from=0&to=4096&bucket_ms=1",
            &format!(
                "/events/histogram?from={}&to={}&bucket_ms=1000",
                i64::MIN,
                i64::MIN
            ),
            "/events/histogram?from=1000&to=3000&bucket_ms=1000&limit=1",
            "/events/histogram?from=1000&to=3000&bucket_ms=1000&offset=0",
        ] {
            let (status, body) = get_json(&app, uri).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
            assert!(body.get("buckets").is_none(), "{uri}");
            assert!(body["error"].is_string(), "{uri}");
        }

        let below = get_json(
            &app,
            &format!(
                "/events/histogram?from={}&to={}&bucket_ms=1000",
                i64::MIN,
                i64::MIN
            ),
        )
        .await;
        assert_eq!(below.0, StatusCode::BAD_REQUEST);
        assert_eq!(
            below.1["error"],
            json!("histogram bucket start is outside i64")
        );

        let after = get_json(&app, "/events/count?from=1000&to=3000").await;
        assert_eq!(after.0, StatusCode::OK);
        assert_eq!(after.1, json!({"count": 3}));

        store.close().unwrap();
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn range_filters_agree_across_routes_and_reject_bad_bounds() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "eventer-http-range-{}-{}",
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&root).unwrap();
        let schema = root.join("schema.json");
        fs::write(
            &schema,
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "user_id", "type": "int"},
                    {"name": "score", "type": "float"},
                    {"name": "ok", "type": "bool"},
                    {"name": "action", "type": "string"},
                    {"name": "note", "type": "text"},
                    {"name": "amount", "type": "decimal", "scale": 2},
                    {"name": "props", "type": "json"}
                ]
            }"#,
        )
        .unwrap();
        let options = eventer::StoreOptions {
            block_rows: 8,
            linger: Duration::from_millis(1),
            parser_threads: 1,
            compress_threads: 1,
            ..eventer::StoreOptions::default()
        };
        let store =
            Arc::new(eventer::Store::open_with(root.join("data"), &schema, options).unwrap());
        let app = router(Arc::clone(&store));
        for (ts, user, amount, score) in [
            (1000i64, Some(1i64), "1.00", 1.0),
            (2000, Some(5), "5.00", 5.0),
            (3000, Some(9), "9.00", 9.0),
            (4000, None, "5.00", 0.0),
        ] {
            let user_field = match user {
                Some(user) => format!(r#""user_id":{user},"#),
                None => String::new(),
            };
            let body = format!(
                r#"{{"ts":{ts},{user_field}"score":{score},"ok":{},"action":"click","amount":"{amount}","props":{{"n":1}}}}"#,
                user.is_some()
            );
            let (status, _) = post(&app, &body).await;
            assert_eq!(status, StatusCode::CREATED, "{body}");
        }

        let user_ids = |rows: Vec<Value>| {
            rows.into_iter()
                .map(|row| row["user_id"].as_i64())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            user_ids(query(&app, "/events?from=1000&to=4000&ge=user_id=5").await),
            vec![Some(5), Some(9)]
        );
        assert_eq!(
            user_ids(query(&app, "/events?from=1000&to=4000&gt=user_id=5").await),
            vec![Some(9)]
        );
        assert_eq!(
            user_ids(query(&app, "/events?from=1000&to=4000&le=user_id=5").await),
            vec![Some(1), Some(5)]
        );
        assert_eq!(
            user_ids(query(&app, "/events?from=1000&to=4000&lt=user_id=5").await),
            vec![Some(1)]
        );

        let amounts = query(&app, "/events?from=1000&to=4000&lt=amount=5.00").await;
        assert!(amounts.iter().all(|row| row["amount"] == "1.00"));
        assert_eq!(amounts.len(), 1);
        let scores = query(&app, "/events?from=1000&to=4000&gt=score=5").await;
        assert_eq!(scores.len(), 1);
        assert_eq!(scores[0]["score"].as_f64(), Some(9.0));

        let rows = query(
            &app,
            "/events?from=1000&to=4000&ge=user_id=5&eq=action=click",
        )
        .await;
        let counted = get_json(
            &app,
            "/events/count?from=1000&to=4000&ge=user_id=5&eq=action=click",
        )
        .await;
        assert_eq!(counted.0, StatusCode::OK);
        assert_eq!(counted.1["count"], json!(rows.len()));
        let buckets = get_json(
            &app,
            "/events/histogram?from=1000&to=4000&bucket_ms=1000&ge=user_id=5&eq=action=click",
        )
        .await;
        assert_eq!(buckets.0, StatusCode::OK);
        let sum: i64 = buckets.1["buckets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|bucket| bucket["count"].as_i64().unwrap())
            .sum();
        assert_eq!(sum, rows.len() as i64);

        let page = query(
            &app,
            "/events?from=1000&to=4000&ge=user_id=5&limit=1&offset=1",
        )
        .await;
        assert_eq!(page.len(), 1);
        assert_eq!(page[0]["user_id"], 9);

        for uri in [
            "/events?from=1000&to=4000&gt=ok=true",
            "/events?from=1000&to=4000&gt=action=a",
            "/events?from=1000&to=4000&gt=note=a",
            "/events?from=1000&to=4000&gt=props=1",
            "/events?from=1000&to=4000&gt=missing=1",
            "/events?from=1000&to=4000&gt=user_id",
            "/events?from=1000&to=4000&gt=user_id=",
            "/events?from=1000&to=4000&gt=user_id=nope",
            "/events?from=1000&to=4000&gt=score=inf",
            "/events?from=1000&to=4000&lt=amount=5.000",
            "/events/count?from=1000&to=4000&ge=user_id=5&limit=1",
            "/events/histogram?from=1000&to=4000&bucket_ms=1000&ge=user_id=5&offset=0",
        ] {
            let (status, body) = get_json(&app, uri).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri} {body}");
            assert!(body.get("error").is_some(), "{uri}");
        }
        assert_eq!(
            query(&app, "/events?from=1000&to=4000").await.len(),
            4,
            "a rejected range does not change the stored rows"
        );

        store.close().unwrap();
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn stats_aggregate_the_filtered_window() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "eventer-http-stats-{}-{}",
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&root).unwrap();
        let schema = root.join("schema.json");
        fs::write(
            &schema,
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "user_id", "type": "int"},
                    {"name": "score", "type": "float"},
                    {"name": "ok", "type": "bool"},
                    {"name": "action", "type": "string"},
                    {"name": "amount", "type": "decimal", "scale": 2}
                ]
            }"#,
        )
        .unwrap();
        let options = store_options(8, eventer::StoreOptions::default().segment_bytes);
        let store =
            Arc::new(eventer::Store::open_with(root.join("data"), &schema, options).unwrap());
        let app = router(Arc::clone(&store));
        let rows = [
            r#"{"ts":1000,"user_id":1,"score":0.5,"ok":true,"action":"buy","amount":"1.50"}"#,
            r#"{"ts":2000,"user_id":5,"score":0.25,"ok":true,"action":"buy","amount":"2.25"}"#,
            r#"{"ts":3000,"score":1.0,"ok":false,"action":"other"}"#,
        ];
        for row in rows {
            let (status, body) = post(&app, row).await;
            assert_eq!(status, StatusCode::CREATED, "{body}");
        }

        let (status, body) = get_json(
            &app,
            "/events/stats?from=1000&to=3000&sum=user_id&min=user_id&max=user_id",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["columns"]["user_id"]["sum"], json!("6"));
        assert_eq!(body["columns"]["user_id"]["min"], json!(1));
        assert_eq!(body["columns"]["user_id"]["max"], json!(5));

        let (status, body) = get_json(
            &app,
            "/events/stats?from=1000&to=3000&sum=amount&min=amount&max=amount",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["columns"]["amount"]["sum"], json!("3.75"));
        assert_eq!(body["columns"]["amount"]["min"], json!("1.50"));
        assert_eq!(body["columns"]["amount"]["max"], json!("2.25"));

        let (status, body) = get_json(
            &app,
            "/events/stats?from=1000&to=3000&lt=amount=2.00&sum=amount&min=amount&max=amount",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["columns"]["amount"]["sum"], json!("1.50"));
        assert_eq!(body["columns"]["amount"]["min"], json!("1.50"));
        assert_eq!(body["columns"]["amount"]["max"], json!("1.50"));

        let (status, counted) =
            get_json(&app, "/events/count?from=1000&to=3000&eq=action=buy").await;
        assert_eq!(status, StatusCode::OK, "{counted}");
        assert_eq!(counted["count"], json!(2));
        let (status, body) = get_json(
            &app,
            "/events/stats?from=1000&to=3000&eq=action=buy&sum=user_id",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["columns"]["user_id"]["sum"], json!("6"));
        assert!(body["columns"]["user_id"].get("min").is_none());

        let (status, body) = get_json(&app, "/events/stats?from=1000&to=2000&sum=score").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["columns"]["score"]["sum"], json!(0.75));

        let (status, body) = get_json(&app, "/events/stats?from=1000&to=2000&sum=ts").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["columns"]["ts"]["sum"], json!("3000"));

        let (status, body) = get_json(
            &app,
            "/events/stats?from=9000&to=9000&sum=user_id&min=user_id&max=user_id&sum=amount",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["columns"]["user_id"]["sum"], json!("0"));
        assert_eq!(body["columns"]["user_id"]["min"], Value::Null);
        assert_eq!(body["columns"]["user_id"]["max"], Value::Null);
        assert_eq!(body["columns"]["amount"]["sum"], json!("0"));

        let (status, only_null) = get_json(
            &app,
            "/events/stats?from=3000&to=3000&sum=user_id&min=user_id&max=user_id",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{only_null}");
        assert_eq!(only_null["columns"]["user_id"]["sum"], json!("0"));
        assert_eq!(only_null["columns"]["user_id"]["min"], Value::Null);
        assert_eq!(only_null["columns"]["user_id"]["max"], Value::Null);

        for uri in [
            "/events/stats?from=1000&to=3000",
            "/events/stats?from=1000&to=3000&sum=",
            "/events/stats?from=1000&to=3000&sum",
            "/events/stats?from=1000&to=3000&sum=missing",
            "/events/stats?from=1000&to=3000&sum=action",
            "/events/stats?from=1000&to=3000&min=ok",
            "/events/stats?from=1000&to=3000&sum=user_id&limit=1",
            "/events/stats?from=1000&to=3000&sum=user_id&offset=0",
            "/events/stats?from=1000&to=3000&sum=user_id&bucket_ms=0",
            "/events/stats?from=1000&to=3000&sum=user_id&bucket_ms=-1",
            "/events/stats?from=1000&to=3000&sum=user_id&bucket_ms=1.5",
            "/events/stats?from=1000&to=3000&sum=user_id&bucket_ms=nope",
            "/events/stats?from=1000&to=3000&sum=user_id&by=action&bucket_ms=1000",
        ] {
            let (status, body) = get_json(&app, uri).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri} -> {body}");
            assert!(body.get("columns").is_none(), "{uri} -> {body}");
            assert!(body.get("buckets").is_none(), "{uri} -> {body}");
        }
        let (status, body) = get_json(
            &app,
            "/events/stats?from=1000&to=3000&max=user_id&min=score",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body["columns"]["user_id"]
                .as_object()
                .unwrap()
                .keys()
                .collect::<Vec<_>>(),
            vec!["max"]
        );
        assert_eq!(body["columns"]["user_id"]["max"], json!(5));
        assert_eq!(body["columns"]["score"]["min"], json!(0.25));

        let (status, body) = post(
            &app,
            r#"{"ts":4000,"user_id":1,"score":0.0,"ok":true,"action":"buy","amount":"1.00"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let (status, listed) = get_json(&app, "/events?from=4000&to=4000").await;
        assert_eq!(status, StatusCode::OK, "{listed}");
        let (status, body) = get_json(
            &app,
            "/events/stats?from=4000&to=4000&sum=score&min=score&max=score",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["columns"]["score"]["sum"], listed[0]["score"]);
        assert_eq!(body["columns"]["score"]["min"], listed[0]["score"]);
        assert_eq!(body["columns"]["score"]["max"], listed[0]["score"]);
        let (status, empty) = get_json(
            &app,
            "/events/stats?from=9000&to=9000&sum=score&min=score&max=score",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{empty}");
        assert_eq!(empty["columns"]["score"]["sum"], json!(0));
        assert_eq!(empty["columns"]["score"]["min"], Value::Null);
        assert_eq!(empty["columns"]["score"]["max"], Value::Null);

        let (status, body) = post(
            &app,
            r#"{"ts":5000,"user_id":1,"score":1e308,"ok":true,"action":"buy","amount":"1.00"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let (status, body) = post(
            &app,
            r#"{"ts":5001,"user_id":2,"score":1e308,"ok":true,"action":"buy","amount":"1.00"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let (status, body) = get_json(
            &app,
            "/events/stats?from=5000&to=5001&sum=score&min=score&max=user_id",
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(body.get("columns").is_none(), "{body}");

        store.close().unwrap();
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn stats_bucket_numeric_columns_and_keep_empty_buckets() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "eventer-http-stats-bucket-{}-{}",
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&root).unwrap();
        let schema = root.join("schema.json");
        fs::write(
            &schema,
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "user_id", "type": "int"},
                    {"name": "score", "type": "float"},
                    {"name": "ok", "type": "bool"},
                    {"name": "action", "type": "string"},
                    {"name": "amount", "type": "decimal", "scale": 2}
                ]
            }"#,
        )
        .unwrap();
        let options = store_options(2, eventer::StoreOptions::default().segment_bytes);
        let store =
            Arc::new(eventer::Store::open_with(root.join("data"), &schema, options).unwrap());
        let app = router(Arc::clone(&store));
        for row in [
            r#"{"ts":1500,"user_id":1,"score":1e16,"ok":true,"action":"buy","amount":"1.50"}"#,
            r#"{"ts":2500,"user_id":1,"score":1.0,"ok":true,"action":"buy","amount":"2.25"}"#,
            r#"{"ts":3000,"score":-1e16,"ok":false,"action":"other","amount":"9.00"}"#,
            r#"{"ts":10000,"user_id":9,"score":1.0,"ok":true,"action":"later","amount":"4.00"}"#,
        ] {
            let (status, body) = post(&app, row).await;
            assert_eq!(status, StatusCode::CREATED, "{body}");
        }

        let (status, counted) =
            get_json(&app, "/events/count?from=1500&to=3000&eq=action=buy").await;
        assert_eq!(status, StatusCode::OK, "{counted}");
        let (status, whole) = get_json(
            &app,
            "/events/stats?from=1500&to=3000&eq=action=buy&sum=user_id&min=user_id&max=user_id",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{whole}");
        assert!(whole.get("buckets").is_none(), "{whole}");
        let (status, body) = get_json(
            &app,
            "/events/stats?from=1500&to=3000&bucket_ms=1000&eq=action=buy&sum=user_id&min=user_id&max=user_id&sum=amount",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(body.get("columns").is_none(), "{body}");
        let buckets = body["buckets"].as_array().unwrap();
        assert_eq!(buckets.len(), 3);
        assert_eq!(buckets[0]["start_ms"], json!(1000));
        assert_eq!(buckets[0]["columns"]["user_id"]["sum"], json!("1"));
        assert_eq!(buckets[0]["columns"]["user_id"]["min"], json!(1));
        assert_eq!(buckets[0]["columns"]["user_id"]["max"], json!(1));
        assert_eq!(buckets[0]["columns"]["amount"]["sum"], json!("1.50"));
        assert_eq!(buckets[1]["start_ms"], json!(2000));
        assert_eq!(buckets[1]["columns"]["user_id"]["sum"], json!("1"));
        assert_eq!(buckets[1]["columns"]["amount"]["sum"], json!("2.25"));
        assert_eq!(buckets[2]["start_ms"], json!(3000));
        assert_eq!(buckets[2]["columns"]["user_id"]["sum"], json!("0"));
        assert_eq!(buckets[2]["columns"]["user_id"]["min"], Value::Null);
        assert_eq!(buckets[2]["columns"]["user_id"]["max"], Value::Null);
        assert_eq!(buckets[2]["columns"]["amount"]["sum"], json!("0"));
        let bucket_sum: i64 = buckets
            .iter()
            .map(|bucket| {
                bucket["columns"]["user_id"]["sum"]
                    .as_str()
                    .unwrap()
                    .parse::<i64>()
                    .unwrap()
            })
            .sum();
        assert_eq!(bucket_sum, 2);
        assert_eq!(whole["columns"]["user_id"]["sum"], json!("2"));
        assert_eq!(json!(bucket_sum), counted["count"]);

        let (status, with_null) = get_json(
            &app,
            "/events/stats?from=1500&to=3000&bucket_ms=1000&sum=user_id",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{with_null}");
        let null_sum: i64 = with_null["buckets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|bucket| {
                bucket["columns"]["user_id"]["sum"]
                    .as_str()
                    .unwrap()
                    .parse::<i64>()
                    .unwrap()
            })
            .sum();
        let (status, unbucketed) =
            get_json(&app, "/events/stats?from=1500&to=3000&sum=user_id").await;
        assert_eq!(status, StatusCode::OK, "{unbucketed}");
        assert_eq!(
            unbucketed["columns"]["user_id"]["sum"],
            json!(null_sum.to_string())
        );
        let (status, all_count) = get_json(&app, "/events/count?from=1500&to=3000").await;
        assert_eq!(status, StatusCode::OK, "{all_count}");
        assert_ne!(json!(null_sum), all_count["count"]);

        let (status, scores) = get_json(
            &app,
            "/events/stats?from=2500&to=3000&bucket_ms=1000&sum=score",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{scores}");
        assert_eq!(scores["buckets"][0]["start_ms"], json!(2000));
        assert_eq!(scores["buckets"][0]["columns"]["score"]["sum"], json!(1.0));
        assert_eq!(scores["buckets"][1]["start_ms"], json!(3000));
        assert_eq!(
            scores["buckets"][1]["columns"]["score"]["sum"],
            json!(-1e16)
        );

        let (status, empty_float) = get_json(
            &app,
            "/events/stats?from=4000&to=4000&bucket_ms=1000&sum=score&min=score&max=score",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{empty_float}");
        assert_eq!(empty_float["buckets"][0]["start_ms"], json!(4000));
        assert_eq!(
            empty_float["buckets"][0]["columns"]["score"]["sum"],
            json!(0)
        );
        assert_eq!(
            empty_float["buckets"][0]["columns"]["score"]["min"],
            Value::Null
        );
        assert_eq!(
            empty_float["buckets"][0]["columns"]["score"]["max"],
            Value::Null
        );

        let (status, histogram) =
            get_json(&app, "/events/histogram?from=1500&to=3000&bucket_ms=1000").await;
        assert_eq!(status, StatusCode::OK, "{histogram}");
        assert_eq!(histogram["buckets"][0]["count"], json!(1));
        assert!(histogram["buckets"][0].get("columns").is_none());

        for uri in [
            "/events?from=1500&to=3000&bucket_ms=1000",
            "/events/count?from=1500&to=3000&bucket_ms=1000",
        ] {
            let (status, body) = get_json(&app, uri).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri} -> {body}");
        }
        let (status, dropped) =
            post_json(&app, "/events/drop?bucket_ms=1000", r#"{"before_ms":1}"#).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{dropped}");
        let (status, dropped) =
            post_json(&app, "/events/drop", r#"{"before_ms":1,"bucket_ms":1000}"#).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{dropped}");
        let (status, still) = get_json(&app, "/events/count?from=1500&to=10000").await;
        assert_eq!(status, StatusCode::OK, "{still}");
        assert_eq!(still["count"], json!(4));

        let data = root.join("data");
        let dat = fs::read_dir(&data)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.extension().and_then(|ext| ext.to_str()) == Some("dat"))
            .unwrap();
        let mut bytes = fs::read(&dat).unwrap();
        bytes[16] ^= 0xff;
        fs::write(&dat, &bytes).unwrap();
        for uri in [
            "/events/stats?from=0&to=4096&bucket_ms=1&sum=user_id",
            "/events/stats?from=1500&to=3000&sum=user_id&by=action&bucket_ms=1000",
            "/events/stats?from=1500&to=3000&sum=user_id&bucket_ms=0",
            "/events/stats?from=1500&to=3000&sum=user_id&limit=1&bucket_ms=1000",
        ] {
            let (status, body) = get_json(&app, uri).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri} -> {body}");
            assert!(body.get("buckets").is_none(), "{uri} -> {body}");
            let message = body["error"].as_str().unwrap_or("");
            assert!(!message.contains("corrupt"), "{uri} read the store: {body}");
        }

        store.close().unwrap();
        let _ = fs::remove_dir_all(&root);

        let root = std::env::temp_dir().join(format!(
            "eventer-http-stats-bucket-overflow-{}-{}",
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&root).unwrap();
        let schema = root.join("schema.json");
        fs::write(
            &schema,
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "user_id", "type": "int"},
                    {"name": "score", "type": "float"},
                    {"name": "amount", "type": "decimal", "scale": 0}
                ]
            }"#,
        )
        .unwrap();
        let options = store_options(8, eventer::StoreOptions::default().segment_bytes);
        let store =
            Arc::new(eventer::Store::open_with(root.join("data"), &schema, options).unwrap());
        let app = router(Arc::clone(&store));
        let huge = i128::MAX.to_string();
        for (ts, amount) in [(1000i64, huge.as_str()), (2000, huge.as_str())] {
            let (status, body) = post(
                &app,
                &format!(r#"{{"ts":{ts},"user_id":1,"score":1e308,"amount":"{amount}"}}"#),
            )
            .await;
            assert_eq!(status, StatusCode::CREATED, "{body}");
        }
        let (status, split) = get_json(
            &app,
            "/events/stats?from=1000&to=2000&bucket_ms=1000&sum=amount&sum=score",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{split}");
        assert_eq!(split["buckets"][0]["columns"]["amount"]["sum"], json!(huge));
        assert_eq!(split["buckets"][1]["columns"]["amount"]["sum"], json!(huge));
        assert_eq!(split["buckets"][0]["columns"]["score"]["sum"], json!(1e308));
        assert_eq!(split["buckets"][1]["columns"]["score"]["sum"], json!(1e308));
        let (status, body) = post(
            &app,
            r#"{"ts":1001,"user_id":1,"score":1e308,"amount":"1"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let (status, overflow) = get_json(
            &app,
            "/events/stats?from=1000&to=1001&bucket_ms=1000&sum=amount&min=amount",
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{overflow}");
        assert!(overflow.get("buckets").is_none(), "{overflow}");
        let (status, flooded) = get_json(
            &app,
            "/events/stats?from=1000&to=1001&bucket_ms=1000&sum=score",
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{flooded}");
        assert!(flooded.get("buckets").is_none(), "{flooded}");

        store.close().unwrap();
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn stats_group_by_one_column() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "eventer-http-stats-by-{}-{}",
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&root).unwrap();
        let schema = root.join("schema.json");
        fs::write(
            &schema,
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "user_id", "type": "int"},
                    {"name": "score", "type": "float"},
                    {"name": "ok", "type": "bool"},
                    {"name": "action", "type": "string"},
                    {"name": "amount", "type": "decimal", "scale": 2},
                    {"name": "props", "type": "json"}
                ]
            }"#,
        )
        .unwrap();
        let options = store_options(8, eventer::StoreOptions::default().segment_bytes);
        let store =
            Arc::new(eventer::Store::open_with(root.join("data"), &schema, options).unwrap());
        let app = router(Arc::clone(&store));
        for row in [
            r#"{"ts":1000,"user_id":1,"score":0.5,"ok":true,"action":"click","amount":"1.00","props":{"n":1}}"#,
            r#"{"ts":2000,"user_id":5,"score":0.25,"ok":true,"action":"click","amount":"2.00","props":{"n":2}}"#,
            r#"{"ts":3000,"user_id":9,"score":1.0,"ok":false,"action":"view","amount":"4.00","props":{"n":3}}"#,
        ] {
            let (status, body) = post(&app, row).await;
            assert_eq!(status, StatusCode::CREATED, "{body}");
        }

        let (status, body) = get_json(
            &app,
            "/events/stats?from=1000&to=3000&by=action&sum=amount&min=amount&max=amount",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let groups = body["groups"].as_array().unwrap();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0]["key"], json!("click"));
        assert_eq!(groups[0]["columns"]["amount"]["sum"], json!("3.00"));
        assert_eq!(groups[0]["columns"]["amount"]["min"], json!("1.00"));
        assert_eq!(groups[0]["columns"]["amount"]["max"], json!("2.00"));
        assert_eq!(groups[1]["key"], json!("view"));
        assert_eq!(groups[1]["columns"]["amount"]["sum"], json!("4.00"));
        assert_eq!(groups[1]["columns"]["amount"]["min"], json!("4.00"));
        assert_eq!(groups[1]["columns"]["amount"]["max"], json!("4.00"));

        let (status, plain) = get_json(
            &app,
            "/events/stats?from=1000&to=3000&sum=amount&min=amount&max=amount",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{plain}");
        assert_eq!(plain["columns"]["amount"]["sum"], json!("7.00"));
        assert!(plain.get("groups").is_none(), "{plain}");

        let (status, empty) = get_json(
            &app,
            "/events/stats?from=1000&to=3000&eq=action=missing&by=action&sum=amount",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{empty}");
        assert_eq!(empty["groups"], json!([]));
        assert!(empty.get("columns").is_none(), "{empty}");

        let (status, same) = get_json(
            &app,
            "/events/stats?from=1000&to=3000&by=user_id&sum=user_id&max=amount",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{same}");
        assert_eq!(same["groups"][0]["key"], json!(1));
        assert_eq!(same["groups"][0]["columns"]["user_id"]["sum"], json!("1"));
        assert_eq!(same["groups"][0]["columns"]["amount"]["max"], json!("1.00"));

        for uri in [
            "/events/stats?from=1000&to=3000&by=score&sum=user_id",
            "/events/stats?from=1000&to=3000&by=props&sum=user_id",
            "/events/stats?from=1000&to=3000&by=&sum=amount",
            "/events/stats?from=1000&to=3000&by&sum=amount",
            "/events/stats?from=1000&to=3000&by=missing&sum=amount",
            "/events/stats?from=1000&to=3000&by=action&by=ok&sum=amount",
            "/events/stats?from=1000&to=3000&by=action&sum=amount&limit=1",
        ] {
            let (status, body) = get_json(&app, uri).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri} -> {body}");
            assert!(body.get("groups").is_none(), "{uri} -> {body}");
            assert!(body.get("columns").is_none(), "{uri} -> {body}");
        }

        let data = root.join("data");
        let dat = fs::read_dir(&data)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.extension().and_then(|ext| ext.to_str()) == Some("dat"))
            .unwrap();
        let mut bytes = fs::read(&dat).unwrap();
        bytes[16] ^= 0xff;
        fs::write(&dat, &bytes).unwrap();
        let (status, unread) =
            get_json(&app, "/events/stats?from=1000&to=3000&by=score&sum=user_id").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{unread}");
        assert!(unread.get("groups").is_none(), "{unread}");
        let message = unread["error"].as_str().unwrap_or("");
        assert!(
            !message.contains("corrupt"),
            "float group read the store: {unread}"
        );
        let (status, read) = get_json(&app, "/events/stats?from=1000&to=3000&sum=user_id").await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{read}");
        store.close().unwrap();
        let _ = fs::remove_dir_all(&root);

        let root = std::env::temp_dir().join(format!(
            "eventer-http-stats-by-cap-{}-{}",
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&root).unwrap();
        let schema = root.join("schema.json");
        fs::write(
            &schema,
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "user_id", "type": "int"},
                    {"name": "amount", "type": "decimal", "scale": 2}
                ]
            }"#,
        )
        .unwrap();
        let options = store_options(8192, eventer::StoreOptions::default().segment_bytes);
        let store =
            Arc::new(eventer::Store::open_with(root.join("data"), &schema, options).unwrap());
        let app = router(Arc::clone(&store));
        for id in 0..4097i64 {
            store
                .append_json(format!(r#"{{"ts":{id},"user_id":{id},"amount":"1.00"}}"#).as_bytes())
                .unwrap();
        }
        store.flush().unwrap();
        let (status, body) =
            get_json(&app, "/events/stats?from=0&to=4096&by=user_id&sum=amount").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(body.get("groups").is_none(), "{body}");
        store.close().unwrap();
        let _ = fs::remove_dir_all(&root);
    }

    fn temp_store(label: &str) -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "eventer-http-{label}-{}-{}",
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&root).unwrap();
        let schema = root.join("schema.json");
        fs::write(
            &schema,
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "action", "type": "string"},
                    {"name": "amount", "type": "decimal", "scale": 2}
                ]
            }"#,
        )
        .unwrap();
        let data = root.join("data");
        (root, schema, data)
    }

    fn store_options(block_rows: usize, segment_bytes: u64) -> eventer::StoreOptions {
        eventer::StoreOptions {
            block_rows,
            segment_bytes,
            linger: Duration::from_millis(1),
            parser_threads: 1,
            compress_threads: 1,
            ..eventer::StoreOptions::default()
        }
    }

    async fn timestamps_of(app: &Router, from: i64, to: i64) -> Vec<i64> {
        query(app, &format!("/events?from={from}&to={to}"))
            .await
            .iter()
            .map(|row| row["ts"].as_i64().unwrap())
            .collect()
    }

    async fn post(app: &Router, body: &str) -> (StatusCode, Value) {
        post_json(app, "/events", body).await
    }

    async fn post_json(app: &Router, uri: &str, body: &str) -> (StatusCode, Value) {
        post_with_type(app, uri, "application/json", body).await
    }

    async fn post_with_type(
        app: &Router,
        uri: &str,
        content_type: &str,
        body: &str,
    ) -> (StatusCode, Value) {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(uri)
                    .header("content-type", content_type)
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        (status, value)
    }

    async fn query(app: &Router, uri: &str) -> Vec<Value> {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    async fn get_json(app: &Router, uri: &str) -> (StatusCode, Value) {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        (status, value)
    }
}
