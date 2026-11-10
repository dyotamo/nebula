//! Admin console HTML and JSON API for cache, user, token, and dashboard management.
//!
//! All routes require lifecycle `Ready`; otherwise middleware returns HTTP 503.
//! The HTML page, login, and bootstrap routes do not require an existing session.
//! Protected reads require an unexpired admin session cookie; cache, user, and token
//! mutations also require `x-nebula-csrf`. API bearer tokens are accepted only by login.
//! Bootstrap instead delegates credential validation to the authentication store.
//!
//! Sessions are process-local, expire eight hours after login without renewal, and
//! retain the login-time role without rechecking user or API-token changes. The
//! `nebula_admin_session` cookie is HttpOnly, SameSite=Strict, and scoped to `/`,
//! with no Secure attribute. Login and session lookup return the separate CSRF token.
use std::{
    io,
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
    net::TcpListener,
    sync::{oneshot, watch},
    time::{interval, MissedTickBehavior},
};

use nebula_auth::{AuthStore, Role};
use nebula_health::{lifecycle::ApplicationState, HealthManager};
use nebula_watchdog::WorkerHeartbeat;
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

#[derive(Clone, Serialize)]
pub(crate) struct DashboardConfig {
    pub(crate) persistence_mode: &'static str,
    pub(crate) persistence_backend: &'static str,
    pub(crate) auth_backend: &'static str,
    pub(crate) authentication_enabled: bool,
    pub(crate) max_cache_size_bytes: usize,
}

#[derive(Clone)]
pub(crate) struct DashboardOptions {
    pub(crate) enabled: bool,
    pub(crate) config: DashboardConfig,
}

#[derive(Clone)]
struct ConsoleState {
    store: Arc<CacheStore>,
    limits: FrameLimits,
    auth_store: Option<Arc<AuthStore>>,
    sessions: Arc<dashmap::DashMap<String, ConsoleSession>>,
    metrics: Arc<Metrics>,
    health: HealthManager,
    lifecycle: watch::Receiver<ApplicationState>,
    dashboard_enabled: bool,
    dashboard_config: DashboardConfig,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LoginRequest {
    token: String,
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
struct BootstrapAdminRequest {
    credential: String,
    username: String,
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

/// Serves the admin console on a bound listener with one-second watchdog heartbeats.
/// Signals startup before polling the server and gracefully stops on terminal lifecycle
/// states or closure of the lifecycle channel.
pub(crate) async fn run(
    listener: TcpListener,
    store: Arc<CacheStore>,
    limits: FrameLimits,
    auth_store: Option<Arc<AuthStore>>,
    lifecycle: watch::Receiver<ApplicationState>,
    heartbeat: WorkerHeartbeat,
    metrics: Arc<Metrics>,
    health: HealthManager,
    dashboard: DashboardOptions,
    started: oneshot::Sender<()>,
) -> io::Result<()> {
    info!(address = %listener.local_addr()?, "NebulaCache admin console listening");
    let app = router(
        store,
        limits,
        auth_store,
        lifecycle.clone(),
        metrics,
        health,
        dashboard,
    );
    let shutdown = wait_for_shutdown(lifecycle);
    let mut server = Box::pin(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown)
            .await
    });
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
    auth_store: Option<Arc<AuthStore>>,
    lifecycle: watch::Receiver<ApplicationState>,
    metrics: Arc<Metrics>,
    health: HealthManager,
    dashboard: DashboardOptions,
) -> Router {
    // Allow hex expansion of the value plus fixed JSON/key overhead.
    let body_limit = limits.max_value_len.saturating_mul(2).saturating_add(4096);
    let state = Arc::new(ConsoleState {
        store,
        limits,
        auth_store,
        sessions: Arc::new(dashmap::DashMap::new()),
        metrics,
        health,
        lifecycle: lifecycle.clone(),
        dashboard_enabled: dashboard.enabled,
        dashboard_config: dashboard.config,
    });

    // Entries support list/set/clear and per-key delete; users and tokens expose
    // admin management. Each protected handler applies its own read/write check.
    Router::new()
        .route("/", get(index))
        .route("/api/metrics", get(dashboard_metrics))
        .route("/api/config", get(dashboard_configuration))
        .route("/api/login", post(login))
        .route("/api/session", get(session))
        .route("/api/logout", post(logout))
        .route("/api/bootstrap-admin", post(bootstrap_admin))
        .route("/api/users", get(list_users).post(create_user))
        .route("/api/users/{username}", get(show_user).delete(delete_user))
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
    if !admin_authorized(&state, &headers, false) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    // Authenticate first; disabling the dashboard does not disable other routes.
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

async fn dashboard_configuration(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
) -> Response {
    if !admin_authorized(&state, &headers, false) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    Json(&state.dashboard_config).into_response()
}

async fn require_ready(
    State(lifecycle): State<watch::Receiver<ApplicationState>>,
    request: Request,
    next: Next,
) -> Response {
    // This gate includes the HTML, login, and bootstrap routes, even when degraded.
    if *lifecycle.borrow() != ApplicationState::Ready {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    next.run(request).await
}

// Degraded state blocks requests but does not stop the listener.
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
    let role = match state.auth_store.as_ref() {
        Some(auth_store) => match auth_store.authenticate(&credentials.token) {
            Ok(Some(identity)) => Some(identity.role),
            Ok(None) => None,
            Err(error) => {
                tracing::warn!(error = %error, "admin console authentication lookup failed");
                None
            }
        },
        None => None,
    };
    // Valid non-admin credentials count as a failed console authentication.
    let admin_role = role.as_ref().is_some_and(|role| role.is_admin());
    state.metrics.record_authentication(
        admin_role,
        authentication_started.elapsed().as_nanos() as u64,
    );
    let Some(role) = role.filter(|role| role.is_admin()) else {
        tracing::warn!("admin console authentication failed");
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "Invalid credentials"})),
        )
            .into_response();
    };

    // Generate independent 256-bit session and CSRF secrets, encoded as hex.
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

    (
        StatusCode::OK,
        [(header::SET_COOKIE, format!("{SESSION_COOKIE}={token}; HttpOnly; SameSite=Strict; Path=/; Max-Age={SESSION_TTL_SECS}"))],
        Json(json!({"ok": true, "csrf_token": csrf_token, "role": role.as_str()})),
    )
        .into_response()
}

async fn session(State(state): State<Arc<ConsoleState>>, headers: HeaderMap) -> Response {
    let Some(token) = session_token(&headers) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let Some(session) = state.sessions.get(&token) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    if Instant::now() >= session.expires_at || !session.role.is_admin() {
        // Release the DashMap read guard before removing from the same map.
        drop(session);
        state.sessions.remove(&token);
        return StatusCode::UNAUTHORIZED.into_response();
    }
    Json(json!({
        "ok": true,
        "csrf_token": session.csrf_token,
        "role": session.role.as_str()
    }))
    .into_response()
}

async fn logout(State(state): State<Arc<ConsoleState>>, headers: HeaderMap) -> Response {
    let Some(token) = session_token(&headers) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    // Logout checks the stored CSRF token, not expiry or role, so an expired
    // session can still be removed and its cookie cleared.
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

async fn bootstrap_admin(
    State(state): State<Arc<ConsoleState>>,
    Json(request): Json<BootstrapAdminRequest>,
) -> Response {
    let Some(auth_store) = state.auth_store.as_deref() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    // Bootstrap needs no session or CSRF token and returns an API token, not a cookie.
    match auth_store.bootstrap_admin(&request.credential, &request.username) {
        Ok((user, token)) => {
            tracing::info!(action = "admin_bootstrapped", username = %user.username);
            Json(json!({
                "user": {
                    "id": user.id,
                    "username": user.username,
                    "role": user.role.as_str(),
                    "enabled": user.enabled,
                    "created_at": user.created_at
                },
                "token": {
                    "id": token.metadata.id,
                    "token": token.token,
                    "expires_at": token.metadata.expires_at
                }
            }))
            .into_response()
        }
        Err(nebula_auth::AuthError::InvalidBootstrapCredential)
        | Err(nebula_auth::AuthError::BootstrapUnavailable) => {
            StatusCode::UNAUTHORIZED.into_response()
        }
        Err(nebula_auth::AuthError::UsernameExists | nebula_auth::AuthError::InvalidUsername) => {
            StatusCode::BAD_REQUEST.into_response()
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
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

async fn show_user(
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
    match auth_store.get_user(&username) {
        Ok(Some(user)) => Json(UserView {
            id: user.id,
            username: user.username,
            role: user.role.as_str(),
            enabled: user.enabled,
            created_at: user.created_at,
        })
        .into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
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
    // Creation returns the plaintext secret; listing exposes only token metadata.
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
    if !admin_authorized(&state, &headers, false) {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    // Sort raw keys before paging; search uses case-insensitive, lossy UTF-8
    // key/value text, while response keys and values remain lossless hex strings.
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
    // Clamp page size and page number, retaining page 1 even for no matches.
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
    if !admin_authorized(&state, &headers, true) {
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
    if !admin_authorized(&state, &headers, true) {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    let Ok(request) = serde_json::from_slice::<SetEntryRequest>(&body) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    // JSON carries hex bytes; decoded sizes are checked against protocol limits.
    let (Ok(key), Ok(value)) = (decode_hex(&request.key), decode_hex(&request.value)) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if key.is_empty()
        || key.len() > state.limits.max_key_len
        || value.len() > state.limits.max_value_len
    {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    }

    // A missing TTL defaults to zero, which the cache treats as no expiration.
    state.store.set(key, value, request.ttl_secs);
    StatusCode::NO_CONTENT.into_response()
}

async fn delete_entry(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path(key): Path<String>,
) -> Response {
    if !admin_authorized(&state, &headers, true) {
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

// Authorize from the cookie and cached login-time role, not the auth store.
// Failed checks leave sessions in place; session lookup removes expired entries.
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
    // Reads need no CSRF header; protected mutations require the session token.
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
    // Accumulate length and byte differences without an early mismatch return.
    // The iteration count still depends on the input lengths.
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
        bootstrap_admin, clear_entries, constant_time_eq, create_token, create_user,
        dashboard_metrics, decode_hex, delete_entry, encode_hex, list_entries, list_tokens, login,
        revoke_token, router, session, set_entry, show_user, wait_for_shutdown,
        BootstrapAdminRequest, ConsoleSession, ConsoleState, CreateTokenRequest, CreateUserRequest,
        DashboardConfig, DashboardOptions, ListEntriesQuery, LoginRequest, SESSION_COOKIE,
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
                    None,
                    receiver.clone(),
                    Metrics::new(),
                    HealthManager::new(health_lifecycle, false),
                    DashboardOptions {
                        enabled: true,
                        config: DashboardConfig {
                            persistence_mode: "volatile",
                            persistence_backend: "filesystem",
                            auth_backend: "sqlite",
                            authentication_enabled: true,
                            max_cache_size_bytes: 64 * 1024 * 1024,
                        },
                    },
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
        let auth_store = Arc::new(AuthStore::open(":memory:").unwrap());
        auth_store.create_user("root", Role::Admin).unwrap();
        let api_token = auth_store.create_token("root", None).unwrap();
        let lifecycle = Lifecycle::new();
        let state = Arc::new(ConsoleState {
            store: Arc::new(CacheStore::new()),
            limits: FrameLimits::default(),
            auth_store: Some(auth_store),
            sessions: Arc::new(dashmap::DashMap::new()),
            metrics: Metrics::new(),
            health: HealthManager::new(lifecycle.clone(), false),
            lifecycle: lifecycle.subscribe(),
            dashboard_enabled: true,
            dashboard_config: DashboardConfig {
                persistence_mode: "volatile",
                persistence_backend: "filesystem",
                auth_backend: "sqlite",
                authentication_enabled: true,
                max_cache_size_bytes: 64 * 1024 * 1024,
            },
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
                token: "invalid-token".to_owned(),
            }),
        )
        .await;
        assert_eq!(invalid_login.status(), StatusCode::UNAUTHORIZED);

        let authenticated = login(
            State(Arc::clone(&state)),
            Json(LoginRequest {
                token: api_token.token,
            }),
        )
        .await;
        assert_eq!(authenticated.status(), StatusCode::OK);
        let set_cookie = authenticated
            .headers()
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(!set_cookie.contains("Secure"));
        let cookie = set_cookie.split(';').next().unwrap().to_owned();
        let body = to_bytes(authenticated.into_body(), 1024).await.unwrap();
        let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(header::COOKIE, HeaderValue::from_str(&cookie).unwrap());
        headers.insert(
            "x-nebula-csrf",
            HeaderValue::from_str(response["csrf_token"].as_str().unwrap()).unwrap(),
        );

        let restored_session = session(State(Arc::clone(&state)), headers.clone()).await;
        assert_eq!(restored_session.status(), StatusCode::OK);
        let body = to_bytes(restored_session.into_body(), 1024).await.unwrap();
        let restored: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(restored["csrf_token"], response["csrf_token"]);
        assert_eq!(restored["role"], "admin");

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
    async fn non_admin_roles_cannot_access_admin_console() {
        let auth_store = Arc::new(AuthStore::open(":memory:").unwrap());
        auth_store.create_user("viewer", Role::ReadOnly).unwrap();
        let api_token = auth_store.create_token("viewer", None).unwrap();
        let lifecycle = Lifecycle::new();
        let state = Arc::new(ConsoleState {
            store: Arc::new(CacheStore::new()),
            limits: FrameLimits::default(),
            auth_store: Some(auth_store),
            sessions: Arc::new(dashmap::DashMap::new()),
            metrics: Metrics::new(),
            health: HealthManager::new(lifecycle.clone(), false),
            lifecycle: lifecycle.subscribe(),
            dashboard_enabled: true,
            dashboard_config: DashboardConfig {
                persistence_mode: "volatile",
                persistence_backend: "filesystem",
                auth_backend: "sqlite",
                authentication_enabled: true,
                max_cache_size_bytes: 64 * 1024 * 1024,
            },
        });

        let denied_login = login(
            State(Arc::clone(&state)),
            Json(LoginRequest {
                token: api_token.token,
            }),
        )
        .await;
        assert_eq!(denied_login.status(), StatusCode::UNAUTHORIZED);
        assert!(state.sessions.is_empty());

        state.sessions.insert(
            "read-only-session".to_owned(),
            ConsoleSession {
                expires_at: std::time::Instant::now() + std::time::Duration::from_secs(60),
                role: Role::ReadOnly,
                csrf_token: "csrf".to_owned(),
            },
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            header::COOKIE,
            HeaderValue::from_str(&format!("{SESSION_COOKIE}=read-only-session")).unwrap(),
        );
        headers.insert("x-nebula-csrf", HeaderValue::from_static("csrf"));

        let denied_read = list_entries(
            State(Arc::clone(&state)),
            headers.clone(),
            Query(ListEntriesQuery::default()),
        )
        .await;
        assert_eq!(denied_read.status(), StatusCode::UNAUTHORIZED);

        let denied_dashboard = dashboard_metrics(State(Arc::clone(&state)), headers.clone()).await;
        assert_eq!(denied_dashboard.status(), StatusCode::UNAUTHORIZED);

        let denied_write = clear_entries(State(Arc::clone(&state)), headers.clone()).await;
        assert_eq!(denied_write.status(), StatusCode::UNAUTHORIZED);
        assert!(state.store.is_empty());

        let denied_session = session(State(Arc::clone(&state)), headers).await;
        assert_eq!(denied_session.status(), StatusCode::UNAUTHORIZED);
        assert!(state.sessions.is_empty());
    }

    #[tokio::test]
    async fn bootstrap_admin_api_creates_the_first_admin_and_token() {
        let auth_store = Arc::new(AuthStore::open(":memory:").unwrap());
        let credential = auth_store.ensure_bootstrap_credential().unwrap().unwrap();
        let lifecycle = Lifecycle::new();
        let state = Arc::new(ConsoleState {
            store: Arc::new(CacheStore::new()),
            limits: FrameLimits::default(),
            auth_store: Some(Arc::clone(&auth_store)),
            sessions: Arc::new(dashmap::DashMap::new()),
            metrics: Metrics::new(),
            health: HealthManager::new(lifecycle.clone(), false),
            lifecycle: lifecycle.subscribe(),
            dashboard_enabled: true,
            dashboard_config: DashboardConfig {
                persistence_mode: "volatile",
                persistence_backend: "filesystem",
                auth_backend: "sqlite",
                authentication_enabled: true,
                max_cache_size_bytes: 64 * 1024 * 1024,
            },
        });

        let response = bootstrap_admin(
            State(Arc::clone(&state)),
            Json(BootstrapAdminRequest {
                credential: credential.clone(),
                username: "root".to_owned(),
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 2048).await.unwrap();
        let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(result["user"]["username"], "root");
        assert_eq!(result["user"]["role"], "admin");
        assert!(auth_store
            .authenticate(result["token"]["token"].as_str().unwrap())
            .unwrap()
            .is_some_and(|identity| identity.role.is_admin()));

        let unavailable = bootstrap_admin(
            State(state),
            Json(BootstrapAdminRequest {
                credential,
                username: "another-root".to_owned(),
            }),
        )
        .await;
        assert_eq!(unavailable.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn admin_manages_users_and_token_revocation() {
        let auth_store = Arc::new(AuthStore::open(":memory:").unwrap());
        auth_store.create_user("root", Role::Admin).unwrap();
        let admin_token = auth_store.create_token("root", None).unwrap();
        let lifecycle = Lifecycle::new();
        let state = Arc::new(ConsoleState {
            store: Arc::new(CacheStore::new()),
            limits: FrameLimits::default(),
            auth_store: Some(Arc::clone(&auth_store)),
            sessions: Arc::new(dashmap::DashMap::new()),
            metrics: Metrics::new(),
            health: HealthManager::new(lifecycle.clone(), false),
            lifecycle: lifecycle.subscribe(),
            dashboard_enabled: true,
            dashboard_config: DashboardConfig {
                persistence_mode: "volatile",
                persistence_backend: "filesystem",
                auth_backend: "sqlite",
                authentication_enabled: true,
                max_cache_size_bytes: 64 * 1024 * 1024,
            },
        });
        let authenticated = login(
            State(Arc::clone(&state)),
            Json(LoginRequest {
                token: admin_token.token,
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

        let shown_user = show_user(
            State(Arc::clone(&state)),
            headers.clone(),
            Path("service".to_owned()),
        )
        .await;
        assert_eq!(shown_user.status(), StatusCode::OK);
        let shown_user = to_bytes(shown_user.into_body(), 1024).await.unwrap();
        let shown_user: serde_json::Value = serde_json::from_slice(&shown_user).unwrap();
        assert_eq!(shown_user["username"], "service");
        assert_eq!(shown_user["role"], "readwrite");

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
