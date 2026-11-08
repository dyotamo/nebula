use std::{
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, Request, State},
    http::{header, HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{delete, get, post, put},
    Json, Router,
};
use nebula_protocol::FrameLimits;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{oneshot, watch},
    time::{interval, MissedTickBehavior},
};

use nebula_auth::{AuthStore, Role};
use nebula_health::{lifecycle::ApplicationState, HealthManager};
use nebula_watchdog::WorkerHeartbeat;
use tokio_rustls::{server::TlsStream, TlsAcceptor};
use tracing::info;

use nebula_core::CacheStore;

use crate::metrics::Metrics;

const SESSION_TTL_SECS: u64 = 8 * 60 * 60;
const SESSION_COOKIE: &str = "nebula_admin_session";

#[derive(Clone)]
struct ConsoleSession {
    expires_at: Instant,
    role: Role,
    csrf_token: String,
}

#[derive(Clone)]
struct ConsoleState {
    store: Arc<CacheStore>,
    limits: FrameLimits,
    password: Arc<str>,
    auth_store: Option<Arc<AuthStore>>,
    sessions: Arc<dashmap::DashMap<String, ConsoleSession>>,
    metrics: Arc<Metrics>,
    health: HealthManager,
    lifecycle: watch::Receiver<ApplicationState>,
    dashboard_enabled: bool,
    secure_cookies: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LoginRequest {
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    token: Option<String>,
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateUserRequest {
    username: String,
    role: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChangeRoleRequest {
    role: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChangeEnabledRequest {
    enabled: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateTokenRequest {
    expires_at: Option<i64>,
}

#[derive(Serialize)]
struct UserView {
    id: String,
    username: String,
    role: &'static str,
    enabled: bool,
    created_at: i64,
}

#[derive(Serialize)]
struct TokenView {
    id: String,
    created_at: i64,
    expires_at: Option<i64>,
    revoked: bool,
}

struct TlsListener {
    listener: TcpListener,
    acceptor: TlsAcceptor,
    handshakes: tokio::task::JoinSet<(
        Result<TlsStream<TcpStream>, io::Error>,
        std::net::SocketAddr,
    )>,
}

impl axum::serve::Listener for TlsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            tokio::select! {
                accepted = self.listener.accept() => match accepted {
                    Ok((stream, address)) => {
                        let acceptor = self.acceptor.clone();
                        self.handshakes.spawn(async move {
                            let result = match tokio::time::timeout(
                                Duration::from_secs(10),
                                acceptor.accept(stream),
                            ).await {
                                Ok(result) => result,
                                Err(_) => Err(io::Error::new(io::ErrorKind::TimedOut, "TLS handshake timed out")),
                            };
                            (result, address)
                        });
                    }
                    Err(error) => {
                        tracing::warn!(error = %error, "admin TLS listener accept failed");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                },
                completed = self.handshakes.join_next(), if !self.handshakes.is_empty() => {
                    match completed {
                        Some(Ok((Ok(stream), address))) => return (stream, address),
                        Some(Ok((Err(error), address))) => tracing::warn!(%address, error = %error, "admin TLS handshake failed"),
                        Some(Err(error)) => tracing::warn!(error = %error, "admin TLS handshake task failed"),
                        None => {}
                    }
                }
            }
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.listener.local_addr()
    }
}

pub(crate) async fn run(
    listener: TcpListener,
    store: Arc<CacheStore>,
    limits: FrameLimits,
    password: String,
    auth_store: Option<Arc<AuthStore>>,
    tls_acceptor: Option<TlsAcceptor>,
    lifecycle: watch::Receiver<ApplicationState>,
    heartbeat: WorkerHeartbeat,
    metrics: Arc<Metrics>,
    health: HealthManager,
    dashboard_enabled: bool,
    started: oneshot::Sender<()>,
) -> io::Result<()> {
    info!(address = %listener.local_addr()?, "NebulaCache admin console listening");
    let secure_cookies = tls_acceptor.is_some();
    let app = router(
        store,
        limits,
        password,
        auth_store,
        secure_cookies,
        lifecycle.clone(),
        metrics,
        health,
        dashboard_enabled,
    );
    let shutdown = wait_for_shutdown(lifecycle);
    let mut server: Pin<Box<dyn Future<Output = io::Result<()>> + Send>> =
        if let Some(acceptor) = tls_acceptor {
            Box::pin(async move {
                axum::serve(
                    TlsListener {
                        listener,
                        acceptor,
                        handshakes: tokio::task::JoinSet::new(),
                    },
                    app,
                )
                .with_graceful_shutdown(shutdown)
                .await
            })
        } else {
            Box::pin(async move {
                axum::serve(listener, app)
                    .with_graceful_shutdown(shutdown)
                    .await
            })
        };
    let _ = started.send(());
    let mut heartbeat_interval = interval(Duration::from_secs(1));
    heartbeat_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            result = &mut server => return result,
            _ = heartbeat_interval.tick() => heartbeat.heartbeat(),
        }
    }
}

fn router(
    store: Arc<CacheStore>,
    limits: FrameLimits,
    password: String,
    auth_store: Option<Arc<AuthStore>>,
    secure_cookies: bool,
    lifecycle: watch::Receiver<ApplicationState>,
    metrics: Arc<Metrics>,
    health: HealthManager,
    dashboard_enabled: bool,
) -> Router {
    let body_limit = limits.max_value_len.saturating_mul(2).saturating_add(4096);
    let state = Arc::new(ConsoleState {
        store,
        limits,
        password: password.into(),
        auth_store,
        sessions: Arc::new(dashmap::DashMap::new()),
        metrics,
        health,
        lifecycle: lifecycle.clone(),
        dashboard_enabled,
        secure_cookies,
    });

    Router::new()
        .route("/", get(index))
        .route("/api/metrics", get(dashboard_metrics))
        .route("/api/login", post(login))
        .route("/api/logout", post(logout))
        .route("/api/users", get(list_users).post(create_user))
        .route("/api/users/{username}", delete(delete_user))
        .route("/api/users/{username}/role", put(change_user_role))
        .route("/api/users/{username}/enabled", put(change_user_enabled))
        .route(
            "/api/users/{username}/tokens",
            get(list_tokens).post(create_token),
        )
        .route("/api/tokens/{token_id}", delete(revoke_token))
        .route(
            "/api/entries",
            get(list_entries).put(set_entry).delete(clear_entries),
        )
        .route("/api/entries/{key}", delete(delete_entry))
        .layer(DefaultBodyLimit::max(body_limit))
        .layer(middleware::from_fn_with_state(lifecycle, require_ready))
        .with_state(state)
}

async fn dashboard_metrics(State(state): State<Arc<ConsoleState>>, headers: HeaderMap) -> Response {
    if !authorized(&state, &headers, false) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if !state.dashboard_enabled {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"available": false})),
        )
            .into_response();
    }
    Json(
        state
            .metrics
            .dashboard_snapshot(&state.store, *state.lifecycle.borrow(), &state.health),
    )
    .into_response()
}

async fn require_ready(
    State(lifecycle): State<watch::Receiver<ApplicationState>>,
    request: Request,
    next: Next,
) -> Response {
    if *lifecycle.borrow() != ApplicationState::Ready {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    next.run(request).await
}

async fn wait_for_shutdown(mut lifecycle: watch::Receiver<ApplicationState>) {
    loop {
        if matches!(
            *lifecycle.borrow(),
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed
        ) {
            return;
        }
        if lifecycle.changed().await.is_err() {
            return;
        }
    }
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn login(
    State(state): State<Arc<ConsoleState>>,
    Json(credentials): Json<LoginRequest>,
) -> Response {
    let authentication_started = Instant::now();
    let role = if let Some(auth_store) = state.auth_store.as_ref() {
        match credentials
            .token
            .as_deref()
            .map(|token| auth_store.authenticate(token))
        {
            Some(Ok(Some(identity))) => Some(identity.role),
            Some(Ok(None)) | None => None,
            Some(Err(error)) => {
                tracing::warn!(error = %error, "admin console authentication lookup failed");
                None
            }
        }
    } else if credentials
        .password
        .as_deref()
        .is_some_and(|password| constant_time_eq(password, &state.password))
    {
        Some(Role::Admin)
    } else {
        None
    };
    state.metrics.record_authentication(
        role.is_some(),
        authentication_started.elapsed().as_nanos() as u64,
    );
    let Some(role) = role else {
        tracing::warn!("admin console authentication failed");
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "Invalid credentials"})),
        )
            .into_response();
    };

    let mut session_bytes = [0_u8; 32];
    let mut csrf_bytes = [0_u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut session_bytes);
    rand::rngs::OsRng.fill_bytes(&mut csrf_bytes);
    let token = encode_hex(&session_bytes);
    let csrf_token = encode_hex(&csrf_bytes);
    state.sessions.insert(
        token.clone(),
        ConsoleSession {
            expires_at: Instant::now() + Duration::from_secs(SESSION_TTL_SECS),
            role,
            csrf_token: csrf_token.clone(),
        },
    );

    let secure_flag = if state.secure_cookies { "; Secure" } else { "" };
    (
        StatusCode::OK,
        [(header::SET_COOKIE, format!("{SESSION_COOKIE}={token}; HttpOnly; SameSite=Strict; Path=/; Max-Age={SESSION_TTL_SECS}{secure_flag}"))],
        Json(json!({"ok": true, "csrf_token": csrf_token, "role": role.as_str()})),
    )
        .into_response()
}

async fn logout(State(state): State<Arc<ConsoleState>>, headers: HeaderMap) -> Response {
    let Some(token) = session_token(&headers) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let csrf_valid = state.sessions.get(&token).is_some_and(|session| {
        headers
            .get("x-nebula-csrf")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| constant_time_eq(value, &session.csrf_token))
    });
    if !csrf_valid {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    state.sessions.remove(&token);
    let secure_flag = if state.secure_cookies { "; Secure" } else { "" };
    (
        StatusCode::OK,
        [(
            header::SET_COOKIE,
            format!("{SESSION_COOKIE}=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0{secure_flag}"),
        )],
        Json(json!({"ok": true})),
    )
        .into_response()
}

async fn list_users(State(state): State<Arc<ConsoleState>>, headers: HeaderMap) -> Response {
    if !admin_authorized(&state, &headers, false) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(auth_store) = state.auth_store.as_deref() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match auth_store.list_users() {
        Ok(users) => Json(
            users
                .into_iter()
                .map(|user| UserView {
                    id: user.id,
                    username: user.username,
                    role: user.role.as_str(),
                    enabled: user.enabled,
                    created_at: user.created_at,
                })
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn create_user(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Json(request): Json<CreateUserRequest>,
) -> Response {
    if !admin_authorized(&state, &headers, true) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(auth_store) = state.auth_store.as_deref() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let role = match Role::try_from(request.role.as_str()) {
        Ok(role) => role,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    match auth_store.create_user(&request.username, role) {
        Ok(user) => {
            tracing::info!(action = "user_created", username = %user.username);
            Json(UserView {
                id: user.id,
                username: user.username,
                role: user.role.as_str(),
                enabled: user.enabled,
                created_at: user.created_at,
            })
            .into_response()
        }
        Err(nebula_auth::AuthError::UsernameExists) => StatusCode::CONFLICT.into_response(),
        Err(nebula_auth::AuthError::InvalidUsername | nebula_auth::AuthError::InvalidRole) => {
            StatusCode::BAD_REQUEST.into_response()
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn change_user_role(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path(username): Path<String>,
    Json(request): Json<ChangeRoleRequest>,
) -> Response {
    if !admin_authorized(&state, &headers, true) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(auth_store) = state.auth_store.as_deref() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let role = match Role::try_from(request.role.as_str()) {
        Ok(role) => role,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    match auth_store.set_role(&username, role) {
        Ok(()) => {
            tracing::info!(action = "user_role_changed", username = %username, role = role.as_str());
            StatusCode::NO_CONTENT.into_response()
        }
        Err(nebula_auth::AuthError::UserNotFound) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn change_user_enabled(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path(username): Path<String>,
    Json(request): Json<ChangeEnabledRequest>,
) -> Response {
    if !admin_authorized(&state, &headers, true) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(auth_store) = state.auth_store.as_deref() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match auth_store.set_enabled(&username, request.enabled) {
        Ok(()) => {
            tracing::info!(action = "user_enabled_changed", username = %username, enabled = request.enabled);
            StatusCode::NO_CONTENT.into_response()
        }
        Err(nebula_auth::AuthError::UserNotFound) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn delete_user(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path(username): Path<String>,
) -> Response {
    if !admin_authorized(&state, &headers, true) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(auth_store) = state.auth_store.as_deref() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match auth_store.delete_user(&username) {
        Ok(()) => {
            tracing::info!(action = "user_deleted", username = %username);
            StatusCode::NO_CONTENT.into_response()
        }
        Err(nebula_auth::AuthError::UserNotFound) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn list_tokens(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path(username): Path<String>,
) -> Response {
    if !admin_authorized(&state, &headers, false) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(auth_store) = state.auth_store.as_deref() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match auth_store.list_tokens(&username) {
        Ok(tokens) => Json(
            tokens
                .into_iter()
                .map(|token| TokenView {
                    id: token.id,
                    created_at: token.created_at,
                    expires_at: token.expires_at,
                    revoked: token.revoked,
                })
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn create_token(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path(username): Path<String>,
    Json(request): Json<CreateTokenRequest>,
) -> Response {
    if !admin_authorized(&state, &headers, true) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(auth_store) = state.auth_store.as_deref() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match auth_store.create_token(&username, request.expires_at) {
        Ok(token) => {
            tracing::info!(action = "token_created", username = %username, token_id = %token.metadata.id);
            Json(json!({"id": token.metadata.id, "token": token.token, "expires_at": token.metadata.expires_at})).into_response()
        }
        Err(nebula_auth::AuthError::UserNotFound) => StatusCode::NOT_FOUND.into_response(),
        Err(nebula_auth::AuthError::InvalidExpiration) => StatusCode::BAD_REQUEST.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn revoke_token(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path(token_id): Path<String>,
) -> Response {
    if !admin_authorized(&state, &headers, true) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(auth_store) = state.auth_store.as_deref() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match auth_store.revoke_token(&token_id) {
        Ok(()) => {
            state.metrics.record_revocation();
            tracing::info!(action = "token_revoked", token_id = %token_id);
            StatusCode::NO_CONTENT.into_response()
        }
        Err(nebula_auth::AuthError::TokenNotFound) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn list_entries(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Query(query): Query<ListEntriesQuery>,
) -> Response {
    if !authorized(&state, &headers, false) {
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
    if !authorized(&state, &headers, true) {
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
    if !authorized(&state, &headers, true) {
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
    if !authorized(&state, &headers, true) {
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

fn authorized(state: &ConsoleState, headers: &HeaderMap, write: bool) -> bool {
    let Some(token) = session_token(headers) else {
        return false;
    };
    let Some(session) = state.sessions.get(&token) else {
        return false;
    };
    if Instant::now() >= session.expires_at {
        drop(session);
        state.sessions.remove(&token);
        return false;
    }
    let role_allowed = if write {
        session.role.can_write()
    } else {
        session.role.can_read()
    };
    let csrf_valid = !write
        || headers
            .get("x-nebula-csrf")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| constant_time_eq(value, &session.csrf_token));
    role_allowed && csrf_valid
}

fn admin_authorized(state: &ConsoleState, headers: &HeaderMap, write: bool) -> bool {
    let Some(token) = session_token(headers) else {
        return false;
    };
    let Some(session) = state.sessions.get(&token) else {
        return false;
    };
    if Instant::now() >= session.expires_at || !session.role.is_admin() {
        return false;
    }
    !write
        || headers
            .get("x-nebula-csrf")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| constant_time_eq(value, &session.csrf_token))
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
        clear_entries, constant_time_eq, create_token, create_user, dashboard_metrics, decode_hex,
        delete_entry, encode_hex, list_entries, list_tokens, login, revoke_token, router,
        set_entry, wait_for_shutdown, ConsoleState, CreateTokenRequest, CreateUserRequest,
        ListEntriesQuery, LoginRequest,
    };
    use crate::metrics::Metrics;
    use axum::{
        body::{to_bytes, Bytes},
        extract::{Path, Query, State},
        http::{header, HeaderMap, HeaderValue, StatusCode},
        Json,
    };
    use nebula_auth::{AuthStore, Role};
    use nebula_core::CacheStore;
    use nebula_health::HealthManager;
    use nebula_protocol::FrameLimits;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    use nebula_health::lifecycle::{ApplicationState, Lifecycle};

    #[tokio::test]
    async fn rejects_admin_requests_before_application_is_ready() {
        let lifecycle = Lifecycle::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let receiver = lifecycle.subscribe();
        let health_lifecycle = lifecycle.clone();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                router(
                    Arc::new(CacheStore::new()),
                    FrameLimits::default(),
                    "secret".to_owned(),
                    None,
                    false,
                    receiver.clone(),
                    Metrics::new(),
                    HealthManager::new(health_lifecycle, false),
                    true,
                ),
            )
            .with_graceful_shutdown(wait_for_shutdown(receiver))
            .await
        });

        let mut stream = TcpStream::connect(address).await.unwrap();
        stream
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut status = [0; 12];
        stream.read_exact(&mut status).await.unwrap();
        assert_eq!(&status, b"HTTP/1.1 503");
        drop(stream);

        lifecycle.transition(ApplicationState::Failed).unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn login_session_can_manage_entries_and_rejects_anonymous_access() {
        let lifecycle = Lifecycle::new();
        let state = Arc::new(ConsoleState {
            store: Arc::new(CacheStore::new()),
            limits: FrameLimits::default(),
            password: Arc::from("secret"),
            auth_store: None,
            sessions: Arc::new(dashmap::DashMap::new()),
            metrics: Metrics::new(),
            health: HealthManager::new(lifecycle.clone(), false),
            lifecycle: lifecycle.subscribe(),
            dashboard_enabled: true,
            secure_cookies: false,
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
                password: Some("wrong".to_owned()),
                token: None,
            }),
        )
        .await;
        assert_eq!(invalid_login.status(), StatusCode::UNAUTHORIZED);

        let authenticated = login(
            State(Arc::clone(&state)),
            Json(LoginRequest {
                password: Some("secret".to_owned()),
                token: None,
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
            .unwrap()
            .to_owned();
        let body = to_bytes(authenticated.into_body(), 1024).await.unwrap();
        let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(header::COOKIE, HeaderValue::from_str(&cookie).unwrap());
        headers.insert(
            "x-nebula-csrf",
            HeaderValue::from_str(response["csrf_token"].as_str().unwrap()).unwrap(),
        );

        state.metrics.record_request("set", true, 1_000_000);
        let dashboard = dashboard_metrics(State(Arc::clone(&state)), headers.clone()).await;
        assert_eq!(dashboard.status(), StatusCode::OK);
        let body = to_bytes(dashboard.into_body(), 4096).await.unwrap();
        let snapshot: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(snapshot["available"], true);
        assert_eq!(snapshot["requests_total"], 1);

        let mut unavailable = (*state).clone();
        unavailable.dashboard_enabled = false;
        let unavailable = dashboard_metrics(State(Arc::new(unavailable)), headers.clone()).await;
        assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);

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

    #[tokio::test]
    async fn auth_store_login_shares_roles_and_requires_csrf_for_writes() {
        let auth_store = Arc::new(AuthStore::open(":memory:").unwrap());
        auth_store.create_user("viewer", Role::ReadOnly).unwrap();
        let api_token = auth_store.create_token("viewer", None).unwrap();
        let lifecycle = Lifecycle::new();
        let state = Arc::new(ConsoleState {
            store: Arc::new(CacheStore::new()),
            limits: FrameLimits::default(),
            password: Arc::from("unused"),
            auth_store: Some(auth_store),
            sessions: Arc::new(dashmap::DashMap::new()),
            metrics: Metrics::new(),
            health: HealthManager::new(lifecycle.clone(), false),
            lifecycle: lifecycle.subscribe(),
            dashboard_enabled: true,
            secure_cookies: true,
        });

        let authenticated = login(
            State(Arc::clone(&state)),
            Json(LoginRequest {
                password: None,
                token: Some(api_token.token),
            }),
        )
        .await;
        assert_eq!(authenticated.status(), StatusCode::OK);
        assert!(authenticated.headers()[header::SET_COOKIE]
            .to_str()
            .unwrap()
            .contains("; Secure"));
        let cookie = authenticated.headers()[header::SET_COOKIE]
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_owned();
        let response = to_bytes(authenticated.into_body(), 1024).await.unwrap();
        let response: serde_json::Value = serde_json::from_slice(&response).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(header::COOKIE, HeaderValue::from_str(&cookie).unwrap());
        headers.insert(
            "x-nebula-csrf",
            HeaderValue::from_str(response["csrf_token"].as_str().unwrap()).unwrap(),
        );

        let visible = list_entries(
            State(Arc::clone(&state)),
            headers.clone(),
            Query(ListEntriesQuery::default()),
        )
        .await;
        assert_eq!(visible.status(), StatusCode::OK);
        let denied_write = clear_entries(State(state), headers).await;
        assert_eq!(denied_write.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn admin_console_manages_users_and_token_revocation() {
        let auth_store = Arc::new(AuthStore::open(":memory:").unwrap());
        auth_store.create_user("root", Role::Admin).unwrap();
        let admin_token = auth_store.create_token("root", None).unwrap();
        let lifecycle = Lifecycle::new();
        let state = Arc::new(ConsoleState {
            store: Arc::new(CacheStore::new()),
            limits: FrameLimits::default(),
            password: Arc::from("unused"),
            auth_store: Some(Arc::clone(&auth_store)),
            sessions: Arc::new(dashmap::DashMap::new()),
            metrics: Metrics::new(),
            health: HealthManager::new(lifecycle.clone(), false),
            lifecycle: lifecycle.subscribe(),
            dashboard_enabled: true,
            secure_cookies: true,
        });
        let authenticated = login(
            State(Arc::clone(&state)),
            Json(LoginRequest {
                password: None,
                token: Some(admin_token.token),
            }),
        )
        .await;
        let cookie = authenticated.headers()[header::SET_COOKIE]
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_owned();
        let body = to_bytes(authenticated.into_body(), 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(header::COOKIE, HeaderValue::from_str(&cookie).unwrap());
        headers.insert(
            "x-nebula-csrf",
            HeaderValue::from_str(body["csrf_token"].as_str().unwrap()).unwrap(),
        );

        let created_user = create_user(
            State(Arc::clone(&state)),
            headers.clone(),
            Json(CreateUserRequest {
                username: "service".to_owned(),
                role: "readwrite".to_owned(),
            }),
        )
        .await;
        assert_eq!(created_user.status(), StatusCode::OK);

        let created_token = create_token(
            State(Arc::clone(&state)),
            headers.clone(),
            Path("service".to_owned()),
            Json(CreateTokenRequest { expires_at: None }),
        )
        .await;
        assert_eq!(created_token.status(), StatusCode::OK);
        let created_token = to_bytes(created_token.into_body(), 1024).await.unwrap();
        let created_token: serde_json::Value = serde_json::from_slice(&created_token).unwrap();
        let plaintext = created_token["token"].as_str().unwrap().to_owned();
        let token_id = created_token["id"].as_str().unwrap().to_owned();

        let listed_tokens = list_tokens(
            State(Arc::clone(&state)),
            headers.clone(),
            Path("service".to_owned()),
        )
        .await;
        let listed_tokens = to_bytes(listed_tokens.into_body(), 1024).await.unwrap();
        let listed_tokens: serde_json::Value = serde_json::from_slice(&listed_tokens).unwrap();
        assert!(listed_tokens[0].get("token").is_none());

        let revoked = revoke_token(State(state.clone()), headers, Path(token_id)).await;
        assert_eq!(revoked.status(), StatusCode::NO_CONTENT);
        assert_eq!(auth_store.authenticate(&plaintext).unwrap(), None);
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
