//! HTTP front end for the event store.
//!
//! `POST /events` appends one JSON object, or a JSON array of objects, and waits
//! until that write is fsynced. An array is parsed in full before any row is
//! queued, then queued together so the batch shares one fsync.
//! `GET /events?from=&to=` returns a JSON array of events in that inclusive
//! millisecond range. Repeat `eq=field=value` to AND equality filters into the scan.
//! `limit` keeps the first matching rows and `offset` skips matches before that.
//! Omitting both returns the full match set.

use std::sync::Arc;

use axum::body::Body;
use axum::body::Bytes;
use axum::extract::{Query, RawQuery, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
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
        .route("/events", get(get_events).post(post_event))
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

    async fn post(app: &Router, body: &str) -> (StatusCode, Value) {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/events")
                    .header("content-type", "application/json")
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
}
