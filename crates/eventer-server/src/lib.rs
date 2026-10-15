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
//! Repeat `select=field` to return only those fields, in schema order.
//! `GET /events/count` and `GET /events/histogram` use that same range and those
//! same filters. `limit` and `offset` are rejected on both.
//! `GET /events/histogram?from=&to=&bucket_ms=N` returns one count per epoch-aligned
//! bucket that intersects that inclusive range.
//! `GET /events/stats` returns `sum`, `min`, and `max` for numeric columns over
//! that same range and the same filters, without the matching rows. `by=field`
//! splits that result into one group per value of a single column.
//! `GET /events/quantile?field=&p=` returns one exact rank of a numeric column
//! over that same range and the same filters. The body is that value, or JSON
//! `null` when no non-null value matches.
//! `GET /events/latest?by=field` returns the newest matching row for each value
//! of that column, in the same key order.
//! `GET /events/distinct?field=` returns how many distinct non-null values that
//! column takes in the same filtered window. One int, timestamp, or decimal
//! with no row predicate counts a fully contained, null-free constant or unit
//! stride from the column body.
//! `GET /events/topk?field=&k=` returns the values that occur most often in
//! that column, including null, ordered by exact count.
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
        .route("/events/distinct", get(distinct_events))
        .route("/events/topk", get(topk_events))
        .route("/events/histogram", get(histogram_events))
        .route("/events/stats", get(stats_events))
        .route("/events/quantile", get(quantile_events))
        .route("/events/latest", get(latest_events))
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
    RawQuery(raw): RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let raw_query = raw.as_deref().unwrap_or("");
    if let Err(response) = reject_named_params(
        raw_query,
        "/events/drop",
        &["by", "latest", "distinct", "topk"],
    ) {
        return *response;
    }
    if let Err(response) = reject_select(raw_query, "/events/drop") {
        return *response;
    }
    if let Err(response) = reject_quantile_param(raw.as_deref().unwrap_or(""), "/events/drop") {
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
    if let Err(response) = reject_quantile_param(raw_query, "/events") {
        return *response;
    }
    if let Err(response) =
        reject_named_params(raw_query, "/events", &["by", "latest", "distinct", "topk"])
    {
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
    let columns = match projection(state.store.schema(), raw_query, "/events", &[]) {
        Ok(columns) => columns,
        Err(response) => return *response,
    };
    let store = Arc::clone(&state.store);
    let joined = tokio::task::spawn_blocking(move || match columns {
        None => store.query_json_window(from, to, &predicates, offset, limit),
        Some(columns) => store.query_json_columns(from, to, &predicates, offset, limit, &columns),
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
    if let Err(response) =
        reject_named_params(raw, "/events/count", &["by", "latest", "distinct", "topk"])
    {
        return *response;
    }
    if let Err(response) = reject_page_params(raw, "/events/count") {
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

async fn distinct_events(
    State(state): State<AppState>,
    Query(params): Query<RangeParams>,
    RawQuery(raw): RawQuery,
) -> Response {
    let raw = raw.as_deref().unwrap_or("");
    if let Err(response) = reject_distinct_params(raw) {
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
    let field = match distinct_field(raw) {
        Ok(field) => field,
        Err(response) => return *response,
    };
    if let Err(response) = reject_distinct_field(state.store.schema(), &field) {
        return *response;
    }
    let predicates = match parse_filters(state.store.schema(), raw) {
        Ok(predicates) => predicates,
        Err(response) => return *response,
    };
    let store = Arc::clone(&state.store);
    let joined =
        tokio::task::spawn_blocking(move || store.distinct_count(from, to, &predicates, &field))
            .await;
    match joined {
        Ok(Ok(count)) => (StatusCode::OK, Json(json!({"count": count}))).into_response(),
        Ok(Err(err)) => error_response(&err),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "distinct task failed"})),
        )
            .into_response(),
    }
}

async fn topk_events(
    State(state): State<AppState>,
    Query(params): Query<RangeParams>,
    RawQuery(raw): RawQuery,
) -> Response {
    let raw = raw.as_deref().unwrap_or("");
    let (field, k) = match topk_params(raw) {
        Ok(parsed) => parsed,
        Err(response) => return *response,
    };
    if let Err(response) = reject_topk_field(state.store.schema(), &field) {
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
    let field_name = field.clone();
    let joined =
        tokio::task::spawn_blocking(move || store.topk(from, to, &predicates, &field_name, k))
            .await;
    match joined {
        Ok(Ok(values)) => {
            let body = topk_body(state.store.schema(), &field, &values);
            (StatusCode::OK, Json(body)).into_response()
        }
        Ok(Err(err)) => error_response(&err),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "topk task failed"})),
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
    if let Err(response) = reject_named_params(
        raw,
        "/events/histogram",
        &["by", "latest", "distinct", "topk"],
    ) {
        return *response;
    }
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
    if let Err(response) =
        reject_named_params(raw, "/events/stats", &["latest", "distinct", "topk"])
    {
        return *response;
    }
    if let Err(response) = reject_stats_params(raw) {
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
    let by = match group_field(raw) {
        Ok(by) => by,
        Err(response) => return *response,
    };
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

async fn quantile_events(
    State(state): State<AppState>,
    Query(params): Query<RangeParams>,
    RawQuery(raw): RawQuery,
) -> Response {
    let raw = raw.as_deref().unwrap_or("");
    let (field, level) = match quantile_params(raw) {
        Ok(parsed) => parsed,
        Err(response) => return *response,
    };
    if let Err(response) = reject_quantile_field(state.store.schema(), &field) {
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
    let field_name = field.clone();
    let joined = tokio::task::spawn_blocking(move || {
        store.quantile_with_filter(from, to, &predicates, &field_name, level)
    })
    .await;
    match joined {
        Ok(Ok(value)) => {
            let body = quantile_body(state.store.schema(), &field, value.as_ref());
            (StatusCode::OK, Json(body)).into_response()
        }
        Ok(Err(err)) => error_response(&err),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "quantile task failed"})),
        )
            .into_response(),
    }
}

async fn latest_events(
    State(state): State<AppState>,
    Query(params): Query<RangeParams>,
    RawQuery(raw): RawQuery,
) -> Response {
    let raw = raw.as_deref().unwrap_or("");
    if let Err(response) = reject_latest_params(raw) {
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
    let by = match group_field(raw) {
        Ok(Some(by)) => by,
        Ok(None) => return *bad_request("missing `by` query parameter"),
        Err(response) => return *response,
    };
    if let Err(response) = reject_group_field(state.store.schema(), &by) {
        return *response;
    }
    let predicates = match parse_filters(state.store.schema(), raw) {
        Ok(predicates) => predicates,
        Err(response) => return *response,
    };
    let columns = match projection(state.store.schema(), raw, "/events/latest", &["by"]) {
        Ok(columns) => columns,
        Err(response) => return *response,
    };
    let store = Arc::clone(&state.store);
    let joined = tokio::task::spawn_blocking(move || match columns {
        None => store.latest_json(from, to, &predicates, &by, None),
        Some(columns) => store.latest_json(from, to, &predicates, &by, Some(&columns)),
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
            Json(json!({"error": "latest task failed"})),
        )
            .into_response(),
    }
}

enum StatsOutcome {
    Columns(Vec<eventer::ColumnStats>),
    Groups(Vec<eventer::StatGroup>),
}

fn quantile_params(
    query: &str,
) -> std::result::Result<(String, eventer::QuantileP), Box<Response>> {
    let mut field = None;
    let mut level = None;
    if !query.is_empty() {
        for pair in query.split('&') {
            if pair.is_empty() {
                continue;
            }
            let (key, value) = match pair.split_once('=') {
                Some(pair) => pair,
                None => {
                    let key = percent_decode(pair, "query")?;
                    if key == "field" {
                        return Err(bad_request("`field` is missing a field name"));
                    }
                    if key == "p" {
                        return Err(bad_request("`p` must be a decimal in 0..=1"));
                    }
                    if !quantile_query_key(&key) {
                        return Err(bad_request(&format!(
                            "`{key}` is not a parameter of /events/quantile"
                        )));
                    }
                    continue;
                }
            };
            let key = percent_decode(key, "query")?;
            match key.as_str() {
                "field" => {
                    if field.is_some() {
                        return Err(bad_request("`field` was given more than once"));
                    }
                    let name = percent_decode(value, "field")?;
                    if name.is_empty() {
                        return Err(bad_request("`field` is missing a field name"));
                    }
                    field = Some(name);
                }
                "p" => {
                    if level.is_some() {
                        return Err(bad_request("`p` was given more than once"));
                    }
                    let text = percent_decode(value, "p")?;
                    level = Some(
                        eventer::parse_quantile_p(&text)
                            .map_err(|_| bad_request("`p` must be a decimal in 0..=1"))?,
                    );
                }
                "from" | "to" | "eq" | "gt" | "ge" | "lt" | "le" => {}
                _ => {
                    return Err(bad_request(&format!(
                        "`{key}` is not a parameter of /events/quantile"
                    )));
                }
            }
        }
    }
    let field = field.ok_or_else(|| bad_request("missing `field` query parameter"))?;
    let level = level.ok_or_else(|| bad_request("missing `p` query parameter"))?;
    Ok((field, level))
}

fn quantile_query_key(key: &str) -> bool {
    matches!(
        key,
        "from" | "to" | "eq" | "gt" | "ge" | "lt" | "le" | "field" | "p"
    )
}

fn reject_quantile_field(
    schema: &eventer::Schema,
    name: &str,
) -> std::result::Result<(), Box<Response>> {
    if name.is_empty() {
        return Err(bad_request("`field` is missing a field name"));
    }
    let Some(field) = schema.fields.iter().find(|field| field.name == name) else {
        return Err(bad_request(&format!("unknown field `{name}`")));
    };
    if matches!(
        field.ty,
        eventer::FieldType::Bool
            | eventer::FieldType::String
            | eventer::FieldType::Text
            | eventer::FieldType::Json
    ) {
        return Err(bad_request(&format!(
            "quantile does not apply to {} column `{name}`",
            field.ty.name()
        )));
    }
    Ok(())
}

fn quantile_body(schema: &eventer::Schema, field: &str, value: Option<&eventer::Scalar>) -> Value {
    let Some(value) = value else {
        return Value::Null;
    };
    let Some(column) = schema
        .fields
        .iter()
        .find(|candidate| candidate.name == field)
    else {
        return Value::Null;
    };
    match (column.ty, value) {
        (eventer::FieldType::Int, eventer::Scalar::Int(number)) => json!(number),
        (eventer::FieldType::Timestamp, eventer::Scalar::Timestamp(number)) => json!(number),
        (eventer::FieldType::Float, eventer::Scalar::Float(number)) => {
            Value::Number(Number::from_f64(*number).expect("quantile float is finite"))
        }
        (eventer::FieldType::Decimal { scale }, eventer::Scalar::Decimal(number)) => {
            Value::String(eventer::format_decimal(*number, scale))
        }
        _ => Value::Null,
    }
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
        if key == "limit"
            || key == "offset"
            || key == "bucket_ms"
            || key == "select"
            || key == "quantile"
        {
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
    found.ok_or_else(|| bad_request("missing `bucket_ms` query parameter"))
}

fn reject_distinct_params(query: &str) -> std::result::Result<(), Box<Response>> {
    if query.is_empty() {
        return Ok(());
    }
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let key = pair.split_once('=').map(|(key, _)| key).unwrap_or(pair);
        let key = percent_decode(key, "query")?;
        if !matches!(
            key.as_str(),
            "from" | "to" | "eq" | "gt" | "ge" | "lt" | "le" | "field"
        ) {
            return Err(bad_request(&format!(
                "`{key}` is not a parameter of /events/distinct"
            )));
        }
    }
    Ok(())
}

fn distinct_field(query: &str) -> std::result::Result<String, Box<Response>> {
    let mut found = None;
    if !query.is_empty() {
        for pair in query.split('&') {
            if pair.is_empty() {
                continue;
            }
            let (key, value) = match pair.split_once('=') {
                Some(pair) => pair,
                None => {
                    let key = percent_decode(pair, "query")?;
                    if key == "field" {
                        return Err(bad_request("`field` is missing a field name"));
                    }
                    continue;
                }
            };
            let key = percent_decode(key, "query")?;
            if key != "field" {
                continue;
            }
            if found.is_some() {
                return Err(bad_request("`field` was given more than once"));
            }
            let field = percent_decode(value, "field")?;
            if field.is_empty() {
                return Err(bad_request("`field` is missing a field name"));
            }
            found = Some(field);
        }
    }
    found.ok_or_else(|| bad_request("missing `field` query parameter"))
}

fn topk_params(query: &str) -> std::result::Result<(String, usize), Box<Response>> {
    let mut field = None;
    let mut k = None;
    if !query.is_empty() {
        for pair in query.split('&') {
            if pair.is_empty() {
                continue;
            }
            let (key, value) = match pair.split_once('=') {
                Some(pair) => pair,
                None => {
                    let key = percent_decode(pair, "query")?;
                    if key == "field" {
                        return Err(bad_request("`field` is missing a field name"));
                    }
                    if key == "k" {
                        return Err(bad_request("`k` must be an integer in 1..=4096"));
                    }
                    if !topk_query_key(&key) {
                        return Err(bad_request(&format!(
                            "`{key}` is not a parameter of /events/topk"
                        )));
                    }
                    continue;
                }
            };
            let key = percent_decode(key, "query")?;
            match key.as_str() {
                "field" => {
                    if field.is_some() {
                        return Err(bad_request("`field` was given more than once"));
                    }
                    let name = percent_decode(value, "field")?;
                    if name.is_empty() {
                        return Err(bad_request("`field` is missing a field name"));
                    }
                    field = Some(name);
                }
                "k" => {
                    if k.is_some() {
                        return Err(bad_request("`k` was given more than once"));
                    }
                    let text = percent_decode(value, "k")?;
                    k = Some(parse_topk(&text)?);
                }
                "from" | "to" | "eq" | "gt" | "ge" | "lt" | "le" => {}
                _ => {
                    return Err(bad_request(&format!(
                        "`{key}` is not a parameter of /events/topk"
                    )));
                }
            }
        }
    }
    let field = field.ok_or_else(|| bad_request("missing `field` query parameter"))?;
    let k = k.ok_or_else(|| bad_request("missing `k` query parameter"))?;
    Ok((field, k))
}

fn topk_query_key(key: &str) -> bool {
    matches!(
        key,
        "from" | "to" | "eq" | "gt" | "ge" | "lt" | "le" | "field" | "k"
    )
}

fn parse_topk(text: &str) -> std::result::Result<usize, Box<Response>> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(bad_request("`k` must be an integer in 1..=4096"));
    }
    let value = text
        .parse::<u32>()
        .map_err(|_| bad_request("`k` must be an integer in 1..=4096"))?;
    if !(1..=4096).contains(&value) {
        return Err(bad_request("`k` must be an integer in 1..=4096"));
    }
    Ok(value as usize)
}

fn reject_topk_field(
    schema: &eventer::Schema,
    name: &str,
) -> std::result::Result<(), Box<Response>> {
    if name.is_empty() {
        return Err(bad_request("`field` is missing a field name"));
    }
    let Some(field) = schema.fields.iter().find(|field| field.name == name) else {
        return Err(bad_request(&format!("unknown field `{name}`")));
    };
    if matches!(field.ty, eventer::FieldType::Json) {
        return Err(bad_request(&format!(
            "`field` does not apply to json column `{name}`"
        )));
    }
    Ok(())
}

fn topk_body(schema: &eventer::Schema, field: &str, values: &[eventer::TopValue]) -> Value {
    let ty = schema
        .fields
        .iter()
        .find(|candidate| candidate.name == field)
        .map(|candidate| candidate.ty);
    let list = values
        .iter()
        .map(|hit| {
            json!({
                "value": ty.map(|ty| top_value_json(ty, &hit.value)).unwrap_or(Value::Null),
                "count": hit.count,
            })
        })
        .collect::<Vec<_>>();
    json!({ "values": list })
}

fn top_value_json(ty: eventer::FieldType, value: &eventer::Scalar) -> Value {
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
        (eventer::FieldType::Float, eventer::Scalar::Float(number)) => {
            Value::Number(Number::from_f64(*number).expect("topk float is finite"))
        }
        _ => Value::Null,
    }
}

fn reject_distinct_field(
    schema: &eventer::Schema,
    name: &str,
) -> std::result::Result<(), Box<Response>> {
    let Some(field) = schema.fields.iter().find(|field| field.name == name) else {
        return Err(bad_request(&format!("unknown field `{name}`")));
    };
    if matches!(field.ty, eventer::FieldType::Json) {
        return Err(bad_request(&format!(
            "`field` does not apply to json column `{name}`"
        )));
    }
    Ok(())
}

fn reject_latest_params(query: &str) -> std::result::Result<(), Box<Response>> {
    if query.is_empty() {
        return Ok(());
    }
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let key = pair.split_once('=').map(|(key, _)| key).unwrap_or(pair);
        let key = percent_decode(key, "query")?;
        if !matches!(
            key.as_str(),
            "from" | "to" | "eq" | "gt" | "ge" | "lt" | "le" | "select" | "by"
        ) {
            return Err(bad_request(&format!(
                "`{key}` is not a parameter of /events/latest"
            )));
        }
    }
    Ok(())
}

fn reject_named_params(
    query: &str,
    route: &str,
    names: &[&str],
) -> std::result::Result<(), Box<Response>> {
    if query.is_empty() {
        return Ok(());
    }
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let key = pair.split_once('=').map(|(key, _)| key).unwrap_or(pair);
        let key = percent_decode(key, "query")?;
        if names.contains(&key.as_str()) {
            return Err(bad_request(&format!(
                "`{key}` is not a parameter of {route}"
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
        if key == "limit" || key == "offset" || key == "select" || key == "quantile" {
            return Err(bad_request(&format!(
                "`{key}` is not a parameter of {route}"
            )));
        }
    }
    Ok(())
}

fn reject_quantile_param(query: &str, route: &str) -> std::result::Result<(), Box<Response>> {
    if query.is_empty() {
        return Ok(());
    }
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let key = pair.split_once('=').map(|(key, _)| key).unwrap_or(pair);
        let key = percent_decode(key, "query")?;
        if key == "quantile" {
            return Err(bad_request(&format!(
                "`quantile` is not a parameter of {route}"
            )));
        }
    }
    Ok(())
}

fn reject_select(query: &str, route: &str) -> std::result::Result<(), Box<Response>> {
    if query.is_empty() {
        return Ok(());
    }
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let key = pair.split_once('=').map(|(key, _)| key).unwrap_or(pair);
        let key = percent_decode(key, "query")?;
        if key == "select" {
            return Err(bad_request(&format!(
                "`select` is not a parameter of {route}"
            )));
        }
    }
    Ok(())
}

/// `None` keeps every column. `Some` is schema indexes, duplicates removed.
///
/// An empty name, an unknown field, or a query parameter other than the
/// existing filters and page bounds is rejected before the store is read.
fn projection(
    schema: &eventer::Schema,
    query: &str,
    route: &str,
    also_allowed: &[&str],
) -> std::result::Result<Option<Vec<usize>>, Box<Response>> {
    if query.is_empty() {
        return Ok(None);
    }
    let mut saw = false;
    let mut indexes = Vec::new();
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = match pair.split_once('=') {
            Some((key, value)) => (key, Some(value)),
            None => (pair, None),
        };
        let key = percent_decode(key, "query")?;
        if key != "select" {
            continue;
        }
        saw = true;
        let Some(value) = value else {
            return Err(bad_request("`select` is missing a field name"));
        };
        let name = percent_decode(value, "select")?;
        if name.is_empty() {
            return Err(bad_request("`select` is missing a field name"));
        }
        let Some(index) = schema.fields.iter().position(|field| field.name == name) else {
            return Err(bad_request(&format!("unknown field `{name}`")));
        };
        if !indexes.contains(&index) {
            indexes.push(index);
        }
    }
    if !saw {
        return Ok(None);
    }
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let key = pair.split_once('=').map(|(key, _)| key).unwrap_or(pair);
        let key = percent_decode(key, "query")?;
        if !matches!(
            key.as_str(),
            "from" | "to" | "limit" | "offset" | "eq" | "gt" | "ge" | "lt" | "le" | "select"
        ) && !also_allowed.contains(&key.as_str())
        {
            return Err(bad_request(&format!(
                "`{key}` is not a parameter of {route}"
            )));
        }
    }
    indexes.sort_unstable();
    Ok(Some(indexes))
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
            "/events/stats?from=1000&to=3000&sum=user_id&bucket_ms=1",
        ] {
            let (status, body) = get_json(&app, uri).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri} -> {body}");
            assert!(body.get("columns").is_none(), "{uri} -> {body}");
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

    #[tokio::test]
    async fn select_returns_only_the_named_fields() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "eventer-http-select-{}-{}",
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
                    {"name": "note", "type": "text"}
                ]
            }"#,
        )
        .unwrap();
        let options = store_options(8, eventer::StoreOptions::default().segment_bytes);
        let store =
            Arc::new(eventer::Store::open_with(root.join("data"), &schema, options).unwrap());
        let app = router(Arc::clone(&store));
        for _ in 0..3 {
            let (status, body) =
                post(&app, r#"{"ts":1000,"action":"click","note":"landing"}"#).await;
            assert_eq!(status, StatusCode::CREATED, "{body}");
        }
        let (status, missing_note) = post(&app, r#"{"ts":1000,"action":"click"}"#).await;
        assert_eq!(status, StatusCode::CREATED, "{missing_note}");

        let (status, projected) = get_json(&app, "/events?from=1000&to=1000&select=action").await;
        assert_eq!(status, StatusCode::OK, "{projected}");
        assert_eq!(
            projected,
            json!([
                {"action": "click"},
                {"action": "click"},
                {"action": "click"},
                {"action": "click"}
            ])
        );
        let (status, full) = get_json(&app, "/events?from=1000&to=1000").await;
        assert_eq!(status, StatusCode::OK, "{full}");
        assert_eq!(full[0]["note"], json!("landing"));
        assert_eq!(full[0]["action"], json!("click"));

        let (status, ordered) = get_json(
            &app,
            "/events?from=1000&to=1000&select=note&select=action&limit=1",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{ordered}");
        assert_eq!(
            ordered[0].as_object().unwrap().keys().collect::<Vec<_>>(),
            vec!["action", "note"]
        );
        let (status, twice) = get_json(
            &app,
            "/events?from=1000&to=1000&select=action&select=action&limit=1",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{twice}");
        assert_eq!(twice, json!([{"action": "click"}]));

        let (status, null_note) =
            get_json(&app, "/events?from=1000&to=1000&select=note&offset=3").await;
        assert_eq!(status, StatusCode::OK, "{null_note}");
        assert_eq!(null_note, json!([{"note": null}]));

        let (status, filtered) = get_json(
            &app,
            "/events?from=1000&to=1000&eq=note=landing&select=action",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{filtered}");
        assert_eq!(filtered.as_array().unwrap().len(), 3);
        assert!(filtered[0].get("note").is_none(), "{filtered}");

        let (status, all) = get_json(
            &app,
            "/events?from=1000&to=1000&select=note&select=ts&select=action&limit=1",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{all}");
        assert_eq!(
            all[0].as_object().unwrap().keys().collect::<Vec<_>>(),
            full[0].as_object().unwrap().keys().collect::<Vec<_>>()
        );

        let (status, note) =
            get_json(&app, "/events?from=1000&to=1000&select=action&note=landing").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{note}");
        assert_eq!(note["error"], "`note` is not a parameter of /events");

        for uri in [
            "/events?from=1000&to=1000&select=missing",
            "/events?from=1000&to=1000&select=",
            "/events?from=1000&to=1000&select",
            "/events?from=1000&to=1000&select=action&bogus=1",
            "/events/count?from=1000&to=1000&select=action",
            "/events/histogram?from=1000&to=1000&bucket_ms=1000&select=action",
            "/events/stats?from=1000&to=1000&sum=ts&select=action",
        ] {
            let (status, body) = get_json(&app, uri).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri} -> {body}");
        }
        let (status, dropped) =
            post_json(&app, "/events/drop?select=action", r#"{"before_ms":1}"#).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{dropped}");

        let data = root.join("data");
        let dat = fs::read_dir(&data)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.extension().and_then(|ext| ext.to_str()) == Some("dat"))
            .unwrap();
        let mut bytes = fs::read(&dat).unwrap();
        bytes[16] ^= 0xff;
        fs::write(&dat, &bytes).unwrap();
        let (status, unread) = get_json(&app, "/events?from=1000&to=1000&select=missing").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{unread}");
        let message = unread["error"].as_str().unwrap_or("");
        assert!(
            !message.contains("corrupt"),
            "unknown select read the store: {unread}"
        );
        let (status, count) = get_json(&app, "/events/count?from=1000&to=1000&select=action").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{count}");
        assert!(
            !count["error"].as_str().unwrap_or("").contains("corrupt"),
            "select on count read the store: {count}"
        );

        store.close().unwrap();
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn quantile_returns_one_exact_rank() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "eventer-http-quantile-{}-{}",
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
        let options = store_options(8, eventer::StoreOptions::default().segment_bytes);
        let store =
            Arc::new(eventer::Store::open_with(root.join("data"), &schema, options).unwrap());
        let app = router(Arc::clone(&store));
        for row in [
            r#"{"ts":1000,"user_id":1,"score":5.0,"ok":true,"action":"buy","amount":"1.50","props":{"a":1}}"#,
            r#"{"ts":2000,"user_id":5,"score":0.0,"ok":true,"action":"buy","amount":"9.00"}"#,
            r#"{"ts":2500,"user_id":4,"ok":false,"action":"other","note":"x","amount":"4.00","props":{}}"#,
            r#"{"ts":3000,"user_id":9,"score":6.0,"ok":true,"action":"view","amount":"2.25","props":{"a":2}}"#,
        ] {
            let (status, body) = post(&app, row).await;
            assert_eq!(status, StatusCode::CREATED, "{body}");
        }

        let (status, listed) = get_json(&app, "/events?from=1000&to=1000").await;
        assert_eq!(status, StatusCode::OK, "{listed}");
        let (status, rank) =
            get_json(&app, "/events/quantile?from=1000&to=1000&field=score&p=1").await;
        assert_eq!(status, StatusCode::OK, "{rank}");
        assert_eq!(rank, listed[0]["score"]);
        let (status, amount) =
            get_json(&app, "/events/quantile?from=1000&to=1000&field=amount&p=0").await;
        assert_eq!(status, StatusCode::OK, "{amount}");
        assert_eq!(amount, listed[0]["amount"]);
        let (status, ts) = get_json(&app, "/events/quantile?from=1000&to=2000&field=ts&p=1").await;
        assert_eq!(status, StatusCode::OK, "{ts}");
        assert_eq!(ts, json!(2000));
        let (status, user) =
            get_json(&app, "/events/quantile?from=1000&to=3000&field=user_id&p=0").await;
        assert_eq!(status, StatusCode::OK, "{user}");
        assert_eq!(user, json!(1));

        let (status, low) =
            get_json(&app, "/events/quantile?from=1000&to=3000&field=score&p=0").await;
        assert_eq!(status, StatusCode::OK, "{low}");
        assert_eq!(low, json!(0.0));
        let (status, mid) =
            get_json(&app, "/events/quantile?from=1000&to=3000&field=score&p=0.5").await;
        assert_eq!(status, StatusCode::OK, "{mid}");
        assert_eq!(mid, json!(5.0));
        let (status, high) =
            get_json(&app, "/events/quantile?from=1000&to=3000&field=score&p=1").await;
        assert_eq!(status, StatusCode::OK, "{high}");
        assert_eq!(high, json!(6.0));
        let (status, only_null) =
            get_json(&app, "/events/quantile?from=2500&to=2500&field=score&p=1").await;
        assert_eq!(status, StatusCode::OK, "{only_null}");
        assert_eq!(only_null, Value::Null);
        let (status, empty) =
            get_json(&app, "/events/quantile?from=9000&to=9000&field=score&p=0.5").await;
        assert_eq!(status, StatusCode::OK, "{empty}");
        assert_eq!(empty, Value::Null);
        let (status, cut) = get_json(
            &app,
            "/events/quantile?from=1000&to=3000&field=amount&p=1&lt=amount=3.00",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{cut}");
        assert_eq!(cut, json!("2.25"));

        for uri in [
            "/events/quantile?from=1000&to=3000&p=0.5",
            "/events/quantile?from=1000&to=3000&field=score",
            "/events/quantile?from=1000&to=3000&field=&p=0.5",
            "/events/quantile?from=1000&to=3000&field=score&p=1.1",
            "/events/quantile?from=1000&to=3000&field=score&p=-0.1",
            "/events/quantile?from=1000&to=3000&field=score&p=nope",
            "/events/quantile?from=1000&to=3000&field=missing&p=0.5",
            "/events/quantile?from=1000&to=3000&field=ok&p=0.5",
            "/events/quantile?from=1000&to=3000&field=action&p=0.5",
            "/events/quantile?from=1000&to=3000&field=note&p=0.5",
            "/events/quantile?from=1000&to=3000&field=props&p=0.5",
            "/events/quantile?from=1000&to=3000&field=score&p=0.5&by=action",
            "/events/quantile?from=1000&to=3000&field=score&p=0.5&select=score",
            "/events/quantile?from=1000&to=3000&field=score&p=0.5&limit=1",
            "/events/quantile?from=1000&to=3000&field=score&p=0.5&offset=0",
            "/events/quantile?from=1000&to=3000&field=score&p=0.5&bucket_ms=1",
            "/events/quantile?from=1000&to=3000&field=score&p=0.5&sum=score",
            "/events/quantile?from=1000&to=3000&field=score&p=0.5&min=score",
            "/events/quantile?from=1000&to=3000&field=score&p=0.5&max=score",
            "/events?from=1000&to=3000&quantile=score",
            "/events/count?from=1000&to=3000&quantile=1",
            "/events/histogram?from=1000&to=3000&bucket_ms=1000&quantile=1",
            "/events/stats?from=1000&to=3000&sum=user_id&quantile=1",
        ] {
            let (status, body) = get_json(&app, uri).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri} -> {body}");
            assert!(body.get("error").is_some(), "{uri} -> {body}");
        }
        let (status, dropped) =
            post_json(&app, "/events/drop?quantile=1", r#"{"before_ms":1}"#).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{dropped}");

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
            get_json(&app, "/events/quantile?from=1000&to=3000&field=ok&p=0.5").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{unread}");
        assert!(
            !unread["error"].as_str().unwrap_or("").contains("corrupt"),
            "bad quantile read the store: {unread}"
        );
        let (status, missing_p) =
            get_json(&app, "/events/quantile?from=1000&to=3000&field=score").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{missing_p}");
        assert!(
            !missing_p["error"]
                .as_str()
                .unwrap_or("")
                .contains("corrupt"),
            "missing p read the store: {missing_p}"
        );

        store.close().unwrap();
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn latest_returns_one_row_per_key_in_group_order() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "eventer-http-latest-{}-{}",
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
        let options = store_options(2, eventer::StoreOptions::default().segment_bytes);
        let store =
            Arc::new(eventer::Store::open_with(root.join("data"), &schema, options).unwrap());
        let app = router(Arc::clone(&store));
        let rows = [
            r#"{"ts":1000,"user_id":1,"score":1.0,"ok":true,"action":"click","note":"b","amount":"1.00"}"#,
            r#"{"ts":5000,"user_id":9,"score":1.0,"ok":true,"action":"click","note":"b","amount":"9.00"}"#,
            r#"{"ts":4000,"user_id":3,"score":1.0,"ok":false,"action":"click","note":"b","amount":"8.00"}"#,
            r#"{"ts":5000,"user_id":2,"score":2.5,"ok":true,"action":"click","note":"Z","amount":"3.00"}"#,
            r#"{"ts":2000,"score":1.0,"ok":false,"amount":"4.00"}"#,
            r#"{"ts":1500,"user_id":4,"score":1.0,"ok":true,"action":"view","note":"a","amount":"2.00"}"#,
            r#"{"ts":1100,"user_id":4,"score":1.0,"ok":false,"action":"view","note":"A","amount":"7.00"}"#,
        ];
        for row in rows {
            let (status, body) = post(&app, row).await;
            assert_eq!(status, StatusCode::CREATED, "{body}");
        }

        let (status, body) = get_json(&app, "/events/latest?from=1000&to=5000&by=action").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let listed = body.as_array().unwrap();
        assert_eq!(listed.len(), 3);
        assert!(listed[0]["action"].is_null());
        assert_eq!(listed[0]["ts"], 2000);
        assert!(listed[0]["user_id"].is_null());
        assert_eq!(listed[1]["action"], "click");
        assert_eq!(listed[1]["ts"], 5000);
        assert_eq!(listed[1]["user_id"], 2);
        assert_eq!(listed[2]["action"], "view");
        assert_eq!(listed[2]["ts"], 1500);
        assert_eq!(listed[2]["user_id"], 4);

        let (status, groups) = get_json(
            &app,
            "/events/stats?from=1000&to=5000&by=action&sum=user_id",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{groups}");
        let group_keys: Vec<_> = groups["groups"]
            .as_array()
            .unwrap()
            .iter()
            .map(|group| group["key"].clone())
            .collect();
        let latest_keys: Vec<_> = listed.iter().map(|row| row["action"].clone()).collect();
        assert_eq!(latest_keys, group_keys);

        let (status, at_tie) = get_json(&app, "/events?from=5000&to=5000").await;
        assert_eq!(status, StatusCode::OK, "{at_tie}");
        let tied = at_tie
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["user_id"] == 2)
            .unwrap();
        assert_eq!(&listed[1], tied);

        let (status, projected) = get_json(
            &app,
            "/events/latest?from=1000&to=5000&by=user_id&eq=note=b&select=action&select=ts",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{projected}");
        assert_eq!(
            projected,
            json!([
                {"ts": 1000, "action": "click"},
                {"ts": 4000, "action": "click"},
                {"ts": 5000, "action": "click"}
            ])
        );
        let raw = get_raw(
            &app,
            "/events/latest?from=1000&to=5000&by=user_id&eq=note=b&select=action&select=ts",
        )
        .await;
        assert_eq!(
            raw,
            br#"[{"ts":1000,"action":"click"},{"ts":4000,"action":"click"},{"ts":5000,"action":"click"}]"#
        );

        let (status, empty) = get_json(
            &app,
            "/events/latest?from=1000&to=5000&by=action&eq=action=missing",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{empty}");
        assert_eq!(empty, json!([]));

        for uri in [
            "/events/latest?from=1000&to=5000",
            "/events/latest?from=1000&to=5000&by=",
            "/events/latest?from=1000&to=5000&by",
            "/events/latest?from=1000&to=5000&by=missing",
            "/events/latest?from=1000&to=5000&by=score",
            "/events/latest?from=1000&to=5000&by=props",
            "/events/latest?from=1000&to=5000&by=action&by=ok",
            "/events/latest?from=1000&to=5000&by=action&limit=1",
            "/events/latest?from=1000&to=5000&by=action&offset=0",
            "/events/latest?from=1000&to=5000&by=action&bucket_ms=1",
            "/events?from=1000&to=5000&by=action",
            "/events?from=1000&to=5000&latest=1",
            "/events/count?from=1000&to=5000&by=action",
            "/events/count?from=1000&to=5000&latest=1",
            "/events/histogram?from=1000&to=5000&bucket_ms=1000&by=action",
            "/events/histogram?from=1000&to=5000&bucket_ms=1000&latest=1",
            "/events/stats?from=1000&to=5000&sum=ts&latest=1",
        ] {
            let (status, body) = get_json(&app, uri).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri} {body}");
            assert!(body.get("error").is_some(), "{uri}");
            assert!(!body.is_array(), "{uri} {body}");
        }
        let (status, dropped) =
            post_json(&app, "/events/drop?by=action", r#"{"before_ms":1}"#).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{dropped}");
        let (status, dropped) =
            post_json(&app, "/events/drop?latest=1", r#"{"before_ms":1}"#).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{dropped}");
        let (status, still) = get_json(&app, "/events/count?from=1000&to=5000").await;
        assert_eq!(status, StatusCode::OK, "{still}");
        assert_eq!(still["count"], 7);

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
            "/events/latest?from=1000&to=5000&by=score",
            "/events/latest?from=1000&to=5000&by=action&limit=1",
            "/events/latest?from=1000&to=5000&by=action&by=ok",
            "/events?from=1000&to=5000&by=action",
        ] {
            let (status, body) = get_json(&app, uri).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri} {body}");
            let message = body["error"].as_str().unwrap_or("");
            assert!(!message.contains("corrupt"), "{uri} read the store: {body}");
        }

        store.close().unwrap();
        let _ = fs::remove_dir_all(&root);

        let root = std::env::temp_dir().join(format!(
            "eventer-http-latest-cap-{}-{}",
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
        for id in 0..4097i64 {
            store
                .append_json(format!(r#"{{"ts":{id},"user_id":{id},"amount":"1.00"}}"#).as_bytes())
                .unwrap();
        }
        store.flush().unwrap();
        let app = router(Arc::clone(&store));
        let (status, capped) = get_json(&app, "/events/latest?from=0&to=4095&by=user_id").await;
        assert_eq!(status, StatusCode::OK, "{capped}");
        assert_eq!(capped.as_array().unwrap().len(), 4096);
        assert_eq!(capped[0]["user_id"], 0);
        assert_eq!(capped[4095]["user_id"], 4095);
        let (status, overflow) = get_json(&app, "/events/latest?from=0&to=4096&by=user_id").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{overflow}");
        assert!(!overflow.is_array(), "{overflow}");
        assert!(overflow["error"].as_str().unwrap_or("").contains("4096"));
        store.close().unwrap();
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn distinct_counts_non_null_values() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "eventer-http-distinct-{}-{}",
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
        let options = store_options(2, eventer::StoreOptions::default().segment_bytes);
        let store =
            Arc::new(eventer::Store::open_with(root.join("data"), &schema, options).unwrap());
        let app = router(Arc::clone(&store));
        for body in [
            r#"{"ts":1000,"user_id":1,"score":0.0,"ok":true,"action":"click","note":"A","amount":"1.00","props":{"n":1}}"#,
            r#"{"ts":2000,"score":-0.0,"ok":true,"action":"click","note":"a","amount":"1.00","props":{"n":2}}"#,
            r#"{"ts":3000,"user_id":2,"score":1.5,"ok":false,"action":"view","amount":"2.00","props":[1]}"#,
        ] {
            let (status, created) = post(&app, body).await;
            assert_eq!(status, StatusCode::CREATED, "{created}");
        }

        let (status, users) =
            get_json(&app, "/events/distinct?from=1000&to=3000&field=user_id").await;
        assert_eq!(status, StatusCode::OK, "{users}");
        assert_eq!(users, json!({"count": 2}));
        let (status, early) =
            get_json(&app, "/events/distinct?from=1000&to=2000&field=user_id").await;
        assert_eq!(status, StatusCode::OK, "{early}");
        assert_eq!(early["count"], 1);
        let (status, actions) =
            get_json(&app, "/events/distinct?from=1000&to=3000&field=action").await;
        assert_eq!(status, StatusCode::OK, "{actions}");
        assert_eq!(actions["count"], 2);
        let (status, scores) =
            get_json(&app, "/events/distinct?from=1000&to=3000&field=score").await;
        assert_eq!(status, StatusCode::OK, "{scores}");
        assert_eq!(scores["count"], 3);
        let (status, notes) = get_json(&app, "/events/distinct?from=1000&to=3000&field=note").await;
        assert_eq!(status, StatusCode::OK, "{notes}");
        assert_eq!(notes["count"], 2);
        let (status, amounts) = get_json(
            &app,
            "/events/distinct?from=1000&to=3000&field=amount&eq=action=click",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{amounts}");
        assert_eq!(amounts["count"], 1);
        let (status, none) =
            get_json(&app, "/events/distinct?from=9000&to=9000&field=action").await;
        assert_eq!(status, StatusCode::OK, "{none}");
        assert_eq!(none, json!({"count": 0}));
        let (status, ranged) = get_json(
            &app,
            "/events/distinct?from=1000&to=3000&field=action&gt=user_id=1",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{ranged}");
        assert_eq!(ranged["count"], 1);

        for uri in [
            "/events/distinct?from=1000&to=3000",
            "/events/distinct?from=1000&to=3000&field=",
            "/events/distinct?from=1000&to=3000&field",
            "/events/distinct?from=1000&to=3000&field=missing",
            "/events/distinct?from=1000&to=3000&field=props",
            "/events/distinct?from=1000&to=3000&field=action&field=ok",
            "/events/distinct?from=1000&to=3000&field=action&by=ok",
            "/events/distinct?from=1000&to=3000&field=action&select=action",
            "/events/distinct?from=1000&to=3000&field=action&limit=1",
            "/events/distinct?from=1000&to=3000&field=action&offset=0",
            "/events/distinct?from=1000&to=3000&field=action&bucket_ms=1",
            "/events/distinct?from=1000&to=3000&field=action&sum=user_id",
            "/events/distinct?from=1000&to=3000&field=action&min=user_id",
            "/events/distinct?from=1000&to=3000&field=action&max=user_id",
            "/events/distinct?from=1000&to=3000&field=action&quantile=user_id",
            "/events?from=1000&to=3000&distinct=action",
            "/events/count?from=1000&to=3000&distinct=action",
            "/events/histogram?from=1000&to=3000&bucket_ms=1000&distinct=action",
            "/events/stats?from=1000&to=3000&sum=user_id&distinct=action",
            "/events/latest?from=1000&to=3000&by=action&distinct=1",
            "/events/quantile?from=1000&to=3000&field=score&p=0.5&distinct=action",
        ] {
            let (status, body) = get_json(&app, uri).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri} {body}");
            assert!(body.get("count").is_none(), "{uri} {body}");
            assert!(body.get("error").is_some(), "{uri}");
        }
        let (status, dropped) =
            post_json(&app, "/events/drop?distinct=action", r#"{"before_ms":1}"#).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{dropped}");
        assert!(dropped.get("ok").is_none(), "{dropped}");
        let (status, still) = get_json(&app, "/events/count?from=1000&to=3000").await;
        assert_eq!(status, StatusCode::OK, "{still}");
        assert_eq!(still["count"], 3);

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
            "/events/distinct?from=1000&to=3000&field=props",
            "/events/distinct?from=1000&to=3000&field=",
            "/events/distinct?from=1000&to=3000&field=missing",
            "/events/distinct?from=1000&to=3000&field=action&limit=1",
            "/events?from=1000&to=3000&distinct=action",
            "/events/quantile?from=1000&to=3000&field=score&p=0.5&distinct=action",
        ] {
            let (status, body) = get_json(&app, uri).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri} {body}");
            let message = body["error"].as_str().unwrap_or("");
            assert!(!message.contains("corrupt"), "{uri} read the store: {body}");
            assert!(body.get("count").is_none(), "{uri} {body}");
        }
        store.close().unwrap();
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn topk_returns_the_most_frequent_values() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "eventer-http-topk-{}-{}",
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
        let options = store_options(2, eventer::StoreOptions::default().segment_bytes);
        let store =
            Arc::new(eventer::Store::open_with(root.join("data"), &schema, options).unwrap());
        let app = router(Arc::clone(&store));
        for body in [
            r#"{"ts":1000,"user_id":1,"score":0.0,"ok":true,"action":"click","note":"b","amount":"2.00","props":{"n":1}}"#,
            r#"{"ts":2000,"score":-0.0,"ok":false,"action":"click","note":"a","amount":"1.00","props":{"n":2}}"#,
            r#"{"ts":3000,"user_id":2,"score":0.0,"ok":true,"action":"view","amount":"1.00","props":[1]}"#,
            r#"{"ts":4000,"user_id":0,"score":1.5,"ok":true,"action":"buy","note":"a","amount":"1.00","props":1}"#,
        ] {
            let (status, created) = post(&app, body).await;
            assert_eq!(status, StatusCode::CREATED, "{created}");
        }

        let (status, actions) =
            get_json(&app, "/events/topk?from=1000&to=3000&field=action&k=1").await;
        assert_eq!(status, StatusCode::OK, "{actions}");
        assert_eq!(actions, json!({"values": [{"value": "click", "count": 2}]}));
        let (status, tied) =
            get_json(&app, "/events/topk?from=1000&to=4000&field=action&k=4").await;
        assert_eq!(status, StatusCode::OK, "{tied}");
        assert_eq!(
            tied["values"]
                .as_array()
                .unwrap()
                .iter()
                .map(|hit| hit["value"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["click", "buy", "view"]
        );
        assert_eq!(tied["values"][0]["count"], 2);
        assert_eq!(tied["values"][1]["count"], 1);
        assert_eq!(tied["values"][1]["value"], "buy");
        let (status, users) =
            get_json(&app, "/events/topk?from=1000&to=4000&field=user_id&k=4").await;
        assert_eq!(status, StatusCode::OK, "{users}");
        assert!(users["values"][0]["value"].is_null(), "{users}");
        assert_eq!(users["values"][0]["count"], 1);
        assert_eq!(users["values"][1]["value"], 0);
        assert!(
            users["values"]
                .as_array()
                .unwrap()
                .iter()
                .any(|hit| hit["value"].is_null() && hit["count"] == 1),
            "{users}"
        );
        let null_at = users["values"]
            .as_array()
            .unwrap()
            .iter()
            .position(|hit| hit["value"].is_null())
            .unwrap();
        let zero_at = users["values"]
            .as_array()
            .unwrap()
            .iter()
            .position(|hit| hit["value"] == 0)
            .unwrap();
        assert!(null_at < zero_at, "{users}");
        let (status, early) =
            get_json(&app, "/events/topk?from=1000&to=1000&field=user_id&k=4").await;
        assert_eq!(status, StatusCode::OK, "{early}");
        assert_eq!(early, json!({"values": [{"value": 1, "count": 1}]}));
        let (status, none) =
            get_json(&app, "/events/topk?from=9000&to=9000&field=action&k=1").await;
        assert_eq!(status, StatusCode::OK, "{none}");
        assert_eq!(none, json!({"values": []}));
        let (status, ranged) = get_json(
            &app,
            "/events/topk?from=1000&to=4000&field=action&k=2&gt=user_id=1",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{ranged}");
        assert_eq!(ranged, json!({"values": [{"value": "view", "count": 1}]}));
        let (status, amounts) =
            get_json(&app, "/events/topk?from=1000&to=3000&field=amount&k=2").await;
        assert_eq!(status, StatusCode::OK, "{amounts}");
        assert_eq!(amounts["values"][0]["value"], "1.00");
        assert_eq!(amounts["values"][0]["count"], 2);
        assert_eq!(amounts["values"][1]["value"], "2.00");
        let scores = get_raw(&app, "/events/topk?from=1000&to=4000&field=score&k=3").await;
        let scores = String::from_utf8(scores).unwrap();
        assert!(scores.contains("-0.0"), "{scores}");
        assert!(scores.contains("0.0"), "{scores}");
        let row = get_raw(&app, "/events?from=2000&to=2000&select=score").await;
        let row = String::from_utf8(row).unwrap();
        assert!(row.contains("-0.0"), "{row}");

        for uri in [
            "/events/topk?from=1000&to=3000&field=action",
            "/events/topk?from=1000&to=3000&k=1",
            "/events/topk?from=1000&to=3000&field=&k=1",
            "/events/topk?from=1000&to=3000&field=action&k",
            "/events/topk?from=1000&to=3000&field=action&k=0",
            "/events/topk?from=1000&to=3000&field=action&k=4097",
            "/events/topk?from=1000&to=3000&field=action&k=-1",
            "/events/topk?from=1000&to=3000&field=missing&k=1",
            "/events/topk?from=1000&to=3000&field=props&k=1",
            "/events/topk?from=1000&to=3000&field=action&field=ok&k=1",
            "/events/topk?from=1000&to=3000&field=action&k=1&k=2",
            "/events/topk?from=1000&to=3000&field=action&k=1&by=ok",
            "/events/topk?from=1000&to=3000&field=action&k=1&select=action",
            "/events/topk?from=1000&to=3000&field=action&k=1&limit=1",
            "/events/topk?from=1000&to=3000&field=action&k=1&offset=0",
            "/events/topk?from=1000&to=3000&field=action&k=1&bucket_ms=1",
            "/events/topk?from=1000&to=3000&field=action&k=1&sum=user_id",
            "/events/topk?from=1000&to=3000&field=action&k=1&min=user_id",
            "/events/topk?from=1000&to=3000&field=action&k=1&max=user_id",
            "/events/topk?from=1000&to=3000&field=action&k=1&quantile=user_id",
            "/events/topk?from=1000&to=3000&field=action&k=1&distinct=action",
            "/events?from=1000&to=3000&topk=action",
            "/events/count?from=1000&to=3000&topk=action",
            "/events/histogram?from=1000&to=3000&bucket_ms=1000&topk=action",
            "/events/stats?from=1000&to=3000&sum=user_id&topk=action",
            "/events/latest?from=1000&to=3000&by=action&topk=1",
            "/events/quantile?from=1000&to=3000&field=score&p=0.5&topk=1",
            "/events/distinct?from=1000&to=3000&field=action&topk=1",
        ] {
            let (status, body) = get_json(&app, uri).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri} {body}");
            assert!(body.get("values").is_none(), "{uri} {body}");
            assert!(body.get("error").is_some(), "{uri}");
        }
        let (status, wide) =
            get_json(&app, "/events/topk?from=1000&to=3000&field=action&k=4096").await;
        assert_eq!(status, StatusCode::OK, "{wide}");
        assert_eq!(wide["values"].as_array().unwrap().len(), 2);
        let (status, dropped) =
            post_json(&app, "/events/drop?topk=action", r#"{"before_ms":1}"#).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{dropped}");
        assert!(dropped.get("ok").is_none(), "{dropped}");
        let (status, still) = get_json(&app, "/events/count?from=1000&to=4000").await;
        assert_eq!(status, StatusCode::OK, "{still}");
        assert_eq!(still["count"], 4);

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
            "/events/topk?from=1000&to=3000&field=props&k=1",
            "/events/topk?from=1000&to=3000&field=&k=1",
            "/events/topk?from=1000&to=3000&field=missing&k=1",
            "/events/topk?from=1000&to=3000&field=action&k=0",
            "/events/topk?from=1000&to=3000&field=action&k=1&limit=1",
            "/events?from=1000&to=3000&topk=action",
            "/events/quantile?from=1000&to=3000&field=score&p=0.5&topk=1",
            "/events/distinct?from=1000&to=3000&field=action&topk=1",
        ] {
            let (status, body) = get_json(&app, uri).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri} {body}");
            let message = body["error"].as_str().unwrap_or("");
            assert!(!message.contains("corrupt"), "{uri} read the store: {body}");
            assert!(body.get("values").is_none(), "{uri} {body}");
        }
        store.close().unwrap();
        let _ = fs::remove_dir_all(&root);
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

    async fn get_raw(app: &Router, uri: &str) -> Vec<u8> {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec()
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
