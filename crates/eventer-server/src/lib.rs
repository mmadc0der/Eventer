//! HTTP front end for the event store.
//!
//! `POST /events` appends one JSON object and waits until it is fsynced.
//! `GET /events?from=&to=` returns a JSON array of events in that inclusive
//! millisecond range. Repeat `eq=field=value` to AND equality filters into the scan.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Query, RawQuery, State};
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

async fn post_event(State(state): State<AppState>, body: Bytes) -> Response {
    let store = Arc::clone(&state.store);
    let joined = tokio::task::spawn_blocking(move || store.append_json_durable(&body)).await;
    match joined {
        Ok(Ok(())) => (StatusCode::CREATED, Json(json!({"ok": true}))).into_response(),
        Ok(Err(err)) => error_response(&err),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "append task failed"})),
        )
            .into_response(),
    }
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
    let specs = match eq_specs(raw.as_deref().unwrap_or("")) {
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
        tokio::task::spawn_blocking(move || store.query_with_filter(from, to, &predicates)).await;
    match joined {
        Ok(Ok(rows)) => Json(rows).into_response(),
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
            if percent_decode(pair)? == "eq" {
                return Err(bad_request("`eq` must be field=value"));
            }
            continue;
        };
        if percent_decode(key)? == "eq" {
            specs.push(percent_decode(value)?);
        }
    }
    Ok(specs)
}

fn percent_decode(input: &str) -> std::result::Result<String, Box<Response>> {
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
                    return Err(bad_request("`eq` has an invalid percent-encoding"));
                }
                let hex = &input[index + 1..index + 3];
                let byte = u8::from_str_radix(hex, 16)
                    .map_err(|_| bad_request("`eq` has an invalid percent-encoding"))?;
                out.push(byte);
                index += 3;
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(out).map_err(|_| bad_request("`eq` is not valid UTF-8"))
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
}
