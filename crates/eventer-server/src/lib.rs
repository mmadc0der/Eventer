//! HTTP front end for the event store.
//!
//! `POST /events` appends one JSON object, or a JSON array of objects, and waits
//! until that write is fsynced. An array is parsed in full before any row is
//! queued, then queued together so the batch shares one fsync.
//! `GET /events?from=&to=` returns a JSON array of events in that inclusive
//! millisecond range. Repeat `eq=field=value` to AND equality filters into the scan.
//! `limit` keeps the first matching rows and `offset` skips matches before that.
//! Omitting both returns the full match set.
//! `GET /events/count` returns `{"count":N}` for that same range and filters.
//! `GET /events/histogram?from=&to=&bucket_ms=N` returns one count per epoch-aligned
//! bucket that intersects that inclusive range.
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
use serde_json::{json, Value};

#[derive(Clone)]
struct AppState {
    store: Arc<eventer::Store>,
}

pub fn router(store: Arc<eventer::Store>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/events/count", get(count_events))
        .route("/events/histogram", get(histogram_events))
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
    body: Bytes,
) -> Response {
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
        if !name.trim().eq_ignore_ascii_case("charset") || !charset.eq_ignore_ascii_case("utf-8")
        {
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
    let specs = match eq_specs(raw_query) {
        Ok(specs) => specs,
        Err(response) => return *response,
    };
    let (offset, limit) = match page_params(raw_query) {
        Ok(page) => page,
        Err(response) => return *response,
    };
    let mut predicates = Vec::with_capacity(specs.len());
    for spec in &specs {
        match parse_eq(state.store.schema(), spec) {
            Ok(predicate) => predicates.push(predicate),
            Err(response) => return *response,
        }
    }
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
    let from = match parse_bound(params.from, "from") {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let to = match parse_bound(params.to, "to") {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let specs = match eq_specs(raw) {
        Ok(specs) => specs,
        Err(response) => return *response,
    };
    let mut predicates = Vec::with_capacity(specs.len());
    for spec in &specs {
        match parse_eq(state.store.schema(), spec) {
            Ok(predicate) => predicates.push(predicate),
            Err(response) => return *response,
        }
    }
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
    let specs = match eq_specs(raw) {
        Ok(specs) => specs,
        Err(response) => return *response,
    };
    let mut predicates = Vec::with_capacity(specs.len());
    for spec in &specs {
        match parse_eq(state.store.schema(), spec) {
            Ok(predicate) => predicates.push(predicate),
            Err(response) => return *response,
        }
    }
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
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{content_type:?}");
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
            "/events/histogram?from=1000&to=3000&bucket_ms=1000&limit=1",
            "/events/histogram?from=1000&to=3000&bucket_ms=1000&offset=0",
        ] {
            let (status, body) = get_json(&app, uri).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
            assert!(body.get("buckets").is_none(), "{uri}");
            assert!(body["error"].is_string(), "{uri}");
        }

        let after = get_json(&app, "/events/count?from=1000&to=3000").await;
        assert_eq!(after.0, StatusCode::OK);
        assert_eq!(after.1, json!({"count": 3}));

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
