use std::{io, sync::Arc, time::Instant};

use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use nebula_protocol::FrameLimits;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::net::TcpListener;
use tracing::info;

use crate::engine::CacheStore;

const SESSION_TTL_SECS: u64 = 8 * 60 * 60;
const SESSION_COOKIE: &str = "nebula_admin_session";

#[derive(Clone)]
struct ConsoleState {
    store: Arc<CacheStore>,
    limits: FrameLimits,
    password: Arc<str>,
    sessions: Arc<dashmap::DashMap<String, Instant>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LoginRequest {
    password: String,
}

#[derive(Deserialize)]
struct SetEntryRequest {
    key: String,
    value: String,
    #[serde(default)]
    ttl_secs: u64,
}

#[derive(Serialize)]
struct EntryView {
    key: String,
    value: String,
    ttl_secs: Option<u64>,
}

#[derive(Deserialize)]
#[serde(default)]
struct ListEntriesQuery {
    page: usize,
    page_size: usize,
    search: String,
}

impl Default for ListEntriesQuery {
    fn default() -> Self {
        Self {
            page: 1,
            page_size: 50,
            search: String::new(),
        }
    }
}

#[derive(Serialize)]
struct EntriesPage {
    entries: Vec<EntryView>,
    total: usize,
    matching: usize,
    page: usize,
    page_size: usize,
}

pub async fn run(
    listener: TcpListener,
    store: Arc<CacheStore>,
    limits: FrameLimits,
    password: String,
) -> io::Result<()> {
    info!(address = %listener.local_addr()?, "NebulaCache admin console listening");
    axum::serve(listener, router(store, limits, password)).await
}

fn router(store: Arc<CacheStore>, limits: FrameLimits, password: String) -> Router {
    let body_limit = limits.max_value_len.saturating_mul(2).saturating_add(4096);
    let state = Arc::new(ConsoleState {
        store,
        limits,
        password: password.into(),
        sessions: Arc::new(dashmap::DashMap::new()),
    });

    Router::new()
        .route("/", get(index))
        .route("/api/login", post(login))
        .route("/api/logout", post(logout))
        .route(
            "/api/entries",
            get(list_entries).put(set_entry).delete(clear_entries),
        )
        .route("/api/entries/{key}", delete(delete_entry))
        .layer(DefaultBodyLimit::max(body_limit))
        .with_state(state)
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn login(
    State(state): State<Arc<ConsoleState>>,
    Json(credentials): Json<LoginRequest>,
) -> Response {
    if !constant_time_eq(&credentials.password, &state.password) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "Invalid password"})),
        )
            .into_response();
    }

    let mut bytes = [0_u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    let token = encode_hex(&bytes);
    state.sessions.insert(
        token.clone(),
        Instant::now() + std::time::Duration::from_secs(SESSION_TTL_SECS),
    );

    (
        StatusCode::OK,
        [(header::SET_COOKIE, format!("{SESSION_COOKIE}={token}; HttpOnly; SameSite=Strict; Path=/; Max-Age={SESSION_TTL_SECS}"))],
        Json(json!({"ok": true})),
    )
        .into_response()
}

async fn logout(State(state): State<Arc<ConsoleState>>, headers: HeaderMap) -> Response {
    if let Some(token) = session_token(&headers) {
        state.sessions.remove(&token);
    }
    (
        StatusCode::OK,
        [(
            header::SET_COOKIE,
            format!("{SESSION_COOKIE}=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0"),
        )],
        Json(json!({"ok": true})),
    )
        .into_response()
}

async fn list_entries(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Query(query): Query<ListEntriesQuery>,
) -> Response {
    if !authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    let search = query.search.trim().to_lowercase();
    let mut entries = state.store.snapshot();
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    let total = entries.len();
    entries.retain(|(key, value, _)| {
        search.is_empty()
            || String::from_utf8_lossy(key)
                .to_lowercase()
                .contains(&search)
            || String::from_utf8_lossy(value)
                .to_lowercase()
                .contains(&search)
    });

    let matching = entries.len();
    let page_size = query.page_size.clamp(1, 100);
    let page_count = matching.div_ceil(page_size).max(1);
    let page = query.page.max(1).min(page_count);
    let offset = (page - 1) * page_size;
    let entries = entries
        .into_iter()
        .skip(offset)
        .take(page_size)
        .map(|(key, value, ttl_secs)| EntryView {
            key: encode_hex(&key),
            value: encode_hex(&value),
            ttl_secs,
        })
        .collect();

    Json(EntriesPage {
        entries,
        total,
        matching,
        page,
        page_size,
    })
    .into_response()
}

async fn clear_entries(State(state): State<Arc<ConsoleState>>, headers: HeaderMap) -> Response {
    if !authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    state.store.clear();
    StatusCode::NO_CONTENT.into_response()
}

async fn set_entry(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    let Ok(request) = serde_json::from_slice::<SetEntryRequest>(&body) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let (Ok(key), Ok(value)) = (decode_hex(&request.key), decode_hex(&request.value)) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if key.is_empty()
        || key.len() > state.limits.max_key_len
        || value.len() > state.limits.max_value_len
    {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    }

    state.store.set(key, value, request.ttl_secs);
    StatusCode::NO_CONTENT.into_response()
}

async fn delete_entry(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path(key): Path<String>,
) -> Response {
    if !authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    let Ok(key) = decode_hex(&key) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if state.store.delete(&key) {
        StatusCode::NO_CONTENT.into_response()
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}

fn authorized(state: &ConsoleState, headers: &HeaderMap) -> bool {
    let Some(token) = session_token(headers) else {
        return false;
    };
    let Some(expires_at) = state.sessions.get(&token) else {
        return false;
    };
    if Instant::now() >= *expires_at {
        drop(expires_at);
        state.sessions.remove(&token);
        return false;
    }
    true
}

fn session_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .filter_map(|part| part.trim().split_once('='))
        .find_map(|(name, value)| (name == SESSION_COOKIE).then(|| value.to_owned()))
}

fn constant_time_eq(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();
    let mut difference = left.len() ^ right.len();
    for index in 0..left.len().max(right.len()) {
        difference |= usize::from(
            left.get(index).copied().unwrap_or_default()
                ^ right.get(index).copied().unwrap_or_default(),
        );
    }
    difference == 0
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn decode_hex(input: &str) -> Result<Vec<u8>, ()> {
    if !input.len().is_multiple_of(2) {
        return Err(());
    }
    input
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| Ok((hex_digit(pair[0])? << 4) | hex_digit(pair[1])?))
        .collect()
}

fn hex_digit(byte: u8) -> Result<u8, ()> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(()),
    }
}

const INDEX_HTML: &str = include_str!("../templates/admin.html");

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        clear_entries, constant_time_eq, decode_hex, delete_entry, encode_hex, list_entries, login,
        set_entry, ConsoleState, ListEntriesQuery, LoginRequest,
    };
    use crate::engine::CacheStore;
    use axum::{
        body::{to_bytes, Bytes},
        extract::{Path, Query, State},
        http::{header, HeaderMap, HeaderValue, StatusCode},
        Json,
    };
    use nebula_protocol::FrameLimits;

    #[tokio::test]
    async fn login_session_can_manage_entries_and_rejects_anonymous_access() {
        let state = Arc::new(ConsoleState {
            store: Arc::new(CacheStore::new()),
            limits: FrameLimits::default(),
            password: Arc::from("secret"),
            sessions: Arc::new(dashmap::DashMap::new()),
        });

        let anonymous = list_entries(
            State(Arc::clone(&state)),
            HeaderMap::new(),
            Query(ListEntriesQuery::default()),
        )
        .await;
        assert_eq!(anonymous.status(), StatusCode::UNAUTHORIZED);
        let anonymous_clear = clear_entries(State(Arc::clone(&state)), HeaderMap::new()).await;
        assert_eq!(anonymous_clear.status(), StatusCode::UNAUTHORIZED);

        let invalid_login = login(
            State(Arc::clone(&state)),
            Json(LoginRequest {
                password: "wrong".to_owned(),
            }),
        )
        .await;
        assert_eq!(invalid_login.status(), StatusCode::UNAUTHORIZED);

        let authenticated = login(
            State(Arc::clone(&state)),
            Json(LoginRequest {
                password: "secret".to_owned(),
            }),
        )
        .await;
        assert_eq!(authenticated.status(), StatusCode::OK);
        let cookie = authenticated
            .headers()
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(header::COOKIE, HeaderValue::from_str(cookie).unwrap());

        let saved = set_entry(
            State(Arc::clone(&state)),
            headers.clone(),
            Bytes::from(r#"{"key":"6b","value":"6869","ttl_secs":0}"#),
        )
        .await;
        assert_eq!(saved.status(), StatusCode::NO_CONTENT);

        state
            .store
            .set(b"a".to_vec(), b"searchable value".to_vec(), 0);
        state.store.set(b"b".to_vec(), b"another value".to_vec(), 0);

        let filtered = list_entries(
            State(Arc::clone(&state)),
            headers.clone(),
            Query(ListEntriesQuery {
                page: 1,
                page_size: 1,
                search: "SEARCH".to_owned(),
            }),
        )
        .await;
        assert_eq!(filtered.status(), StatusCode::OK);
        let body = to_bytes(filtered.into_body(), 1024).await.unwrap();
        let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(result["total"], 3);
        assert_eq!(result["matching"], 1);
        assert_eq!(result["entries"][0]["key"], "61");

        let second_page = list_entries(
            State(Arc::clone(&state)),
            headers.clone(),
            Query(ListEntriesQuery {
                page: 2,
                page_size: 1,
                search: String::new(),
            }),
        )
        .await;
        let body = to_bytes(second_page.into_body(), 1024).await.unwrap();
        let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(result["page"], 2);
        assert_eq!(result["entries"][0]["key"], "62");

        let deleted = delete_entry(
            State(Arc::clone(&state)),
            headers.clone(),
            Path("6b".to_owned()),
        )
        .await;
        assert_eq!(deleted.status(), StatusCode::NO_CONTENT);

        let cleared = clear_entries(State(Arc::clone(&state)), headers).await;
        assert_eq!(cleared.status(), StatusCode::NO_CONTENT);
        assert!(state.store.is_empty());
    }

    #[test]
    fn hex_encoding_round_trips_arbitrary_bytes() {
        let bytes = [0, 10, 127, 128, 255];
        assert_eq!(decode_hex(&encode_hex(&bytes)).unwrap(), bytes);
    }

    #[test]
    fn rejects_invalid_hex() {
        assert!(decode_hex("0").is_err());
        assert!(decode_hex("0g").is_err());
    }

    #[test]
    fn password_comparison_checks_all_bytes_and_length() {
        assert!(constant_time_eq("secret", "secret"));
        assert!(!constant_time_eq("secret", "secreu"));
        assert!(!constant_time_eq("secret", "secret1"));
    }
}
