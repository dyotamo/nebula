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

use nebula_core::{CacheEntryMetadata, CacheStore};

use crate::metrics::Metrics;

const SESSION_TTL_SECS: u64 = 8 * 60 * 60;
const SESSION_COOKIE: &str = "nebula_admin_session";

/// Server-side session data keyed by the opaque cookie secret.
/// Expiry and role are fixed at login; the CSRF secret is returned separately to the client.
#[derive(Clone)]
struct ConsoleSession {
    expires_at: Instant,
    role: Role,
    csrf_token: String,
}

/// Non-secret startup configuration exposed by `/api/config`.
/// Backend names reflect configuration even when the corresponding feature is disabled.
#[derive(Clone, Serialize)]
pub(crate) struct DashboardConfig {
    pub(crate) persistence_mode: &'static str,
    pub(crate) persistence_backend: &'static str,
    pub(crate) auth_backend: &'static str,
    pub(crate) authentication_enabled: bool,
    pub(crate) max_cache_size_bytes: usize,
}

/// Dashboard settings captured when the admin listener starts.
#[derive(Clone)]
pub(crate) struct DashboardOptions {
    /// Gates `/api/metrics` only; `/api/config` and other admin routes remain available.
    pub(crate) enabled: bool,
    pub(crate) config: DashboardConfig,
}

/// Shared handler dependencies and process-local sessions for one router instance.
/// An absent authentication store disables login and identity management, not route creation.
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

/// JSON representation of binary cache data; key and value strings contain hex bytes.
#[derive(Deserialize)]
struct SetEntryRequest {
    key: String,
    value: String,
    #[serde(default)]
    ttl_secs: u64,
}

/// Value-free cache metadata with a hex key, Unix-millisecond timestamps, and TTL in seconds.
#[derive(Clone, Serialize)]
struct EntryView {
    key: String,
    size: usize,
    created_at: u64,
    expires_at: Option<u64>,
    last_accessed: u64,
    ttl_secs: Option<u64>,
}

/// Single-entry response that adds a hex value to the same flat metadata used by listings.
#[derive(Serialize)]
struct EntryDetails {
    #[serde(flatten)]
    metadata: EntryView,
    value: String,
}

#[derive(Clone, Copy, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
enum EntrySort {
    #[default]
    Key,
    CreatedAt,
    Size,
}

#[derive(Clone, Copy, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
enum SortOrder {
    #[default]
    Asc,
    Desc,
}

/// Optional filters and ordering applied before pagination of the metadata snapshot.
/// Exact keys and prefixes use hex; search matches a lossy UTF-8 rendering of each key.
#[derive(Deserialize)]
#[serde(default)]
struct ListEntriesQuery {
    page: usize,
    #[serde(alias = "size")]
    page_size: usize,
    search: String,
    exact_key: Option<String>,
    prefix: Option<String>,
    expired: Option<bool>,
    #[serde(alias = "sort")]
    sort_by: EntrySort,
    order: SortOrder,
}

impl Default for ListEntriesQuery {
    /// Selects the first 50 live entries in ascending bytewise key order without key filters.
    fn default() -> Self {
        Self {
            page: 1,
            page_size: 50,
            search: String::new(),
            exact_key: None,
            prefix: None,
            expired: None,
            sort_by: EntrySort::Key,
            order: SortOrder::Asc,
        }
    }
}

/// Publishes both the structured pagination API and legacy top-level listing fields.
#[derive(Serialize)]
struct EntriesPage {
    data: Vec<EntryView>,
    pagination: Pagination,
    // Kept for existing clients of the original admin response.
    entries: Vec<EntryView>,
    // Live entries before filtering; `matching` and `pagination.total` count filtered rows.
    total: usize,
    matching: usize,
    page: usize,
    page_size: usize,
}

/// Filtered result counts and normalized request bounds; an empty result has zero pages.
/// A requested page beyond the last page is retained rather than clamped to an existing page.
#[derive(Serialize)]
struct Pagination {
    page: usize,
    size: usize,
    total: usize,
    total_pages: usize,
    has_next: bool,
    has_previous: bool,
}

/// Snapshot counts split live and expired rows; memory includes both until they are removed.
#[derive(Serialize)]
struct CacheStats {
    entries: usize,
    expired_entries: usize,
    memory_bytes: usize,
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

/// Token metadata safe for repeated listing, with no plaintext credential in the response.
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
    // Listener setup is complete, but this notification does not transition lifecycle readiness.
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

/// Creates the route table, a fresh session map, and the shared readiness/body-size gates.
/// Authorization remains in handlers so login, bootstrap, and embedded assets can be sessionless.
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
        .route("/assets/nebulacache-logo.svg", get(logo))
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
        .route("/api/entries/{key}", get(get_entry).delete(delete_entry))
        .route("/api/cache/stats", get(cache_stats))
        .layer(DefaultBodyLimit::max(body_limit))
        .layer(middleware::from_fn_with_state(lifecycle, require_ready))
        .with_state(state)
}

/// Returns a live dashboard snapshot to an admin session, or 503 when metrics are disabled.
/// The dashboard feature flag is checked only after session authorization succeeds.
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

/// Exposes the startup configuration to an admin session regardless of dashboard enablement.
async fn dashboard_configuration(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
) -> Response {
    if !admin_authorized(&state, &headers, false) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    Json(&state.dashboard_config).into_response()
}

/// Rejects requests outside `Ready` before forwarding them to their route handlers.
/// This is an admission check, not cancellation of requests already being handled.
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

/// Resolves graceful shutdown on a terminal lifecycle state or loss of all lifecycle senders.
/// Checks the current state before awaiting changes so an already-terminal listener can stop.
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

/// Serves the embedded console shell without requiring login; its API calls authorize separately.
async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

/// Serves the compile-time SVG asset with an explicit image content type and no session check.
async fn logo() -> Response {
    ([(header::CONTENT_TYPE, "image/svg+xml")], LOGO_SVG).into_response()
}

/// Exchanges an admin API credential in the JSON body for a fixed-lifetime browser session.
/// Missing stores, lookup errors, invalid tokens, and non-admin roles all fail authentication.
async fn login(
    State(state): State<Arc<ConsoleState>>,
    Json(credentials): Json<LoginRequest>,
) -> Response {
    let authentication_started = Instant::now();
    // Authenticate only during login; subsequent requests trust the stored session role.
    // Store failures are logged locally but collapse into the same client-visible denial.
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
    // The monotonic server deadline is authoritative even if the browser retains its cookie.
    state.sessions.insert(
        token.clone(),
        ConsoleSession {
            expires_at: Instant::now() + Duration::from_secs(SESSION_TTL_SECS),
            role,
            csrf_token: csrf_token.clone(),
        },
    );

    // Keep the session secret HttpOnly while returning the separate CSRF secret in JSON.
    (
        StatusCode::OK,
        [(header::SET_COOKIE, format!("{SESSION_COOKIE}={token}; HttpOnly; SameSite=Strict; Path=/; Max-Age={SESSION_TTL_SECS}"))],
        Json(json!({"ok": true, "csrf_token": csrf_token, "role": role.as_str()})),
    )
        .into_response()
}

/// Restores the role and CSRF token for a live admin cookie without extending its expiration.
/// Invalid stored sessions are removed here; ordinary authorization checks only reject them.
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

/// Removes a stored session only when its CSRF secret matches, then expires the browser cookie.
/// A missing cookie, unknown session, or missing/invalid CSRF header leaves the cookie untouched.
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

/// Delegates first-admin provisioning to the auth store using the supplied bootstrap credential.
/// Success returns user metadata and a plaintext API token for a later login, not a session.
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
        // Invalid credentials and unavailable bootstrap share one response without backend details.
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

// User and token management checks session/CSRF authorization before backend availability.
// These store mutations do not traverse or invalidate the separate console session map.
/// Lists user metadata for an admin reader, returning 503 if no auth store is configured.
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

/// Looks up a username for an admin reader, distinguishing a missing user from store failure.
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

/// Creates a user after admin/CSRF checks and role parsing; duplicate usernames return 409.
/// Username validation remains with the auth store, and success contains no API token.
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

/// Changes the persisted role after admin/CSRF authorization and rejects unknown role strings.
/// Existing console sessions keep their login-time role even when their user is changed here.
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

/// Updates a user's enabled flag after admin/CSRF authorization, with 404 for a missing user.
/// Disabling a user here does not remove console sessions already issued to that user.
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

/// Deletes a username through the auth store after admin/CSRF checks; absent users return 404.
/// The handler does not invalidate any previously issued console sessions.
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

/// Lists a user's token metadata for an admin reader without exposing credential material.
/// All store errors use the generic 500 response rather than a separate missing-user mapping.
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

/// Issues a user API token after admin/CSRF checks, delegating expiration validation to the store.
/// Missing users return 404 and invalid expirations return 400; success includes the secret.
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

/// Revokes an API token after admin/CSRF checks and records the revocation only on store success.
/// This affects the API credential, not console sessions established using it before revocation.
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

// Cache reads expose metadata snapshots for browsing and direct store reads for value inspection.
/// Filters, orders, and paginates cache metadata after authorizing an admin reader.
/// Live rows are selected by default; `expired=true` selects expired snapshot rows instead.
/// `total` counts live rows before filtering, while matching counts describe the selected subset.
async fn list_entries(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Query(query): Query<ListEntriesQuery>,
) -> Response {
    if !admin_authorized(&state, &headers, false) {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    // Binary filters must decode successfully even when the snapshot would have no matches.
    // Text search is separate: trim/lowercase the query rather than interpreting it as hex.
    let exact_key = match query.exact_key.as_deref().map(decode_hex).transpose() {
        Ok(key) => key,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    let prefix = match query.prefix.as_deref().map(decode_hex).transpose() {
        Ok(prefix) => prefix,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    let search = query.search.trim().to_lowercase();
    let snapshot = state.store.admin_snapshot();
    // Use one monotonic instant for expiration filtering, counts, and every row's remaining TTL.
    let instant_now = Instant::now();
    let total = snapshot
        .iter()
        .filter(|entry| !entry.is_expired_at(instant_now))
        .count();
    // Bound page work and normalize zero, but keep large requested page numbers intact.
    // Saturating offset arithmetic makes extreme page requests produce empty slices safely.
    let page_size = query.page_size.clamp(1, 100);
    let page = query.page.max(1);
    let offset = page.saturating_sub(1).saturating_mul(page_size);
    let active_key_order = exact_key.is_none()
        && prefix.is_none()
        && search.is_empty()
        && query.expired != Some(true)
        && matches!(query.sort_by, EntrySort::Key)
        && matches!(query.order, SortOrder::Asc);
    // The shared snapshot is already key-ordered, so default reads need no filtered list.
    // Expired rows must still be excluded before applying pagination offsets.
    let (matching, data): (usize, Vec<_>) = if active_key_order {
        let data = if total == snapshot.len() {
            // With no expired rows, snapshot indices coincide with visible-row page indices.
            let end = offset.saturating_add(page_size).min(snapshot.len());
            snapshot
                .get(offset..end)
                .into_iter()
                .flatten()
                .map(|entry| metadata_view(entry, instant_now))
                .collect()
        } else {
            snapshot
                .iter()
                .filter(|entry| !entry.is_expired_at(instant_now))
                .skip(offset)
                .take(page_size)
                .map(|entry| metadata_view(entry, instant_now))
                .collect()
        };
        (total, data)
    } else {
        // Combine all filters before slicing. Expiration selects one class, never both classes.
        // Exact/prefix filters compare raw bytes; search uses case-insensitive lossy key text.
        let mut entries: Vec<_> = snapshot
            .iter()
            .filter(|entry| {
                let expired = entry.is_expired_at(instant_now);
                query.expired.unwrap_or(false) == expired
                    && exact_key
                        .as_deref()
                        .is_none_or(|key| entry.key.as_slice() == key)
                    && prefix
                        .as_deref()
                        .is_none_or(|prefix| entry.key.starts_with(prefix))
                    && (search.is_empty()
                        || String::from_utf8_lossy(&entry.key)
                            .to_lowercase()
                            .contains(&search))
            })
            .collect();

        // Filtering preserves snapshot key order, so only alternate sorts need another sort pass.
        if !matches!(query.sort_by, EntrySort::Key) || matches!(query.order, SortOrder::Desc) {
            entries.sort_by(|left, right| {
                let ordering = match query.sort_by {
                    EntrySort::Key => left.key.cmp(&right.key),
                    EntrySort::CreatedAt => left.created_at.cmp(&right.created_at),
                    EntrySort::Size => left.size.cmp(&right.size),
                };
                let ordering = match query.order {
                    SortOrder::Asc => ordering,
                    SortOrder::Desc => ordering.reverse(),
                };
                // Equal sort values always use ascending keys for deterministic page boundaries.
                ordering.then_with(|| left.key.cmp(&right.key))
            });
        }

        // Count the complete filtered set before taking a page; out-of-range pages stay empty.
        let matching = entries.len();
        let end = offset.saturating_add(page_size).min(matching);
        let data = entries
            .get(offset..end)
            .unwrap_or_default()
            .iter()
            .map(|entry| metadata_view(entry, instant_now))
            .collect();
        (matching, data)
    };
    let total_pages = matching.div_ceil(page_size);

    // Both response layouts carry the same rows; navigation flags reflect the requested page.
    Json(EntriesPage {
        data: data.clone(),
        pagination: Pagination {
            page,
            size: page_size,
            total: matching,
            total_pages,
            has_next: page < total_pages,
            has_previous: page > 1,
        },
        entries: data,
        total,
        matching,
        page,
        page_size,
    })
    .into_response()
}

/// Reads one hex-encoded key directly from the cache and returns hex value plus fresh metadata.
/// Malformed hex returns 400; a store miss returns 404 rather than an empty value.
async fn get_entry(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path(key): Path<String>,
) -> Response {
    if !admin_authorized(&state, &headers, false) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Ok(key) = decode_hex(&key) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    match state.store.get_with_metadata(&key) {
        Some((value, metadata)) => Json(EntryDetails {
            metadata: metadata_view(&metadata, Instant::now()),
            value: encode_hex(&value),
        })
        .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// Counts live/expired snapshot rows and sums their recorded sizes for an authorized admin reader.
/// The byte sum includes expired rows and is not a measurement of total process or allocator memory.
async fn cache_stats(State(state): State<Arc<ConsoleState>>, headers: HeaderMap) -> Response {
    if !admin_authorized(&state, &headers, false) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let instant_now = Instant::now();
    let snapshot = state.store.admin_snapshot();
    let expired_entries = snapshot
        .iter()
        .filter(|entry| entry.is_expired_at(instant_now))
        .count();
    Json(CacheStats {
        entries: snapshot.len().saturating_sub(expired_entries),
        expired_entries,
        memory_bytes: snapshot
            .iter()
            .fold(0_usize, |total, entry| total.saturating_add(entry.size)),
    })
    .into_response()
}

/// Converts store metadata into JSON-safe hex/timestamps without loading a cached value.
/// The caller supplies the monotonic instant so a page shares one remaining-TTL reference time.
fn metadata_view(entry: &CacheEntryMetadata, instant_now: Instant) -> EntryView {
    EntryView {
        key: encode_hex(&entry.key),
        size: entry.size,
        created_at: timestamp_millis(entry.created_at),
        expires_at: entry.expires_at.map(timestamp_millis),
        last_accessed: timestamp_millis(entry.last_accessed),
        ttl_secs: entry.remaining_ttl_secs_at(instant_now),
    }
}

/// Converts wall-clock time to Unix milliseconds, mapping pre-epoch times to zero and capping overflow.
fn timestamp_millis(time: std::time::SystemTime) -> u64 {
    time.duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

// Cache mutations require the cookie session and its separate CSRF header before changing data.
/// Clears the cache after admin/CSRF authorization and returns an empty 204 response.
async fn clear_entries(State(state): State<Arc<ConsoleState>>, headers: HeaderMap) -> Response {
    if !admin_authorized(&state, &headers, true) {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    state.store.clear();
    StatusCode::NO_CONTENT.into_response()
}

/// Parses a hex key/value JSON body and writes it only after admin/CSRF and decoded-size checks.
/// Bad JSON or hex returns 400; empty/oversized keys and oversized values all return 413.
async fn set_entry(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !admin_authorized(&state, &headers, true) {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    // Parse the already-bounded raw body here so malformed JSON uses this handler's 400 response.
    let Ok(request) = serde_json::from_slice::<SetEntryRequest>(&body) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    // JSON carries hex bytes; decoded sizes are checked against protocol limits.
    let (Ok(key), Ok(value)) = (decode_hex(&request.key), decode_hex(&request.value)) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    // Reject empty keys along with limit violations; an empty value remains a valid byte sequence.
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

/// Deletes a hex-addressed key after admin/CSRF checks, distinguishing malformed keys from misses.
/// The store's deletion result determines whether the response is 204 or 404.
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

// Session and byte-representation helpers are shared by the protected handlers above.
/// Checks a live admin cookie and, for writes, the matching session-specific CSRF header.
/// Does not accept API credentials directly, renew sessions, or remove rejected sessions.
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
    // Here the header must match the stored CSRF secret, not the opaque cookie secret.
    !write
        || headers
            .get("x-nebula-csrf")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| constant_time_eq(value, &session.csrf_token))
}

/// Extracts the first matching cookie value from the selected Cookie header's semicolon list.
/// Malformed header text or an absent named cookie yields no token; values are not decoded.
fn session_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .filter_map(|part| part.trim().split_once('='))
        .find_map(|(name, value)| (name == SESSION_COOKIE).then(|| value.to_owned()))
}

/// Compares byte contents and lengths without returning early on a mismatching byte.
/// Work still depends on input length; this is not a guarantee about compiler-generated timing.
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

/// Encodes arbitrary bytes as lowercase hex for binary cache data and randomly generated secrets.
fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

/// Decodes contiguous ASCII hex pairs, accepting either letter case but no prefixes or whitespace.
/// Empty input decodes successfully; callers decide whether an empty key or value is allowed.
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

/// Converts one ASCII hex digit to its nibble value, rejecting all non-hex and non-ASCII bytes.
fn hex_digit(byte: u8) -> Result<u8, ()> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(()),
    }
}

const INDEX_HTML: &str = include_str!("../templates/admin.html");
const LOGO_SVG: &str = include_str!("../../../assets/nebulacache-logo.svg");

// Handler scenarios call functions directly; only the listener scenario exercises route middleware.
#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use super::{
        bootstrap_admin, cache_stats, clear_entries, constant_time_eq, create_token, create_user,
        dashboard_metrics, decode_hex, delete_entry, encode_hex, get_entry, list_entries,
        list_tokens, login, logo, revoke_token, router, session, set_entry, show_user,
        wait_for_shutdown, BootstrapAdminRequest, ConsoleSession, ConsoleState, CreateTokenRequest,
        CreateUserRequest, DashboardConfig, DashboardOptions, ListEntriesQuery, LoginRequest,
        LOGO_SVG, SESSION_COOKIE,
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

    /// Pins the SVG content type and body to the bundled asset rather than an external resource.
    #[tokio::test]
    async fn serves_embedded_nebulacache_logo_as_svg() {
        let response = logo().await;
        assert_eq!(response.headers()[header::CONTENT_TYPE], "image/svg+xml");
        assert_eq!(
            to_bytes(response.into_body(), usize::MAX).await.unwrap(),
            LOGO_SVG.as_bytes()
        );
    }

    /// Uses real HTTP to prove even the sessionless root route is gated before readiness.
    /// Transitioning to `Failed` then verifies the listener's graceful-shutdown signal completes.
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

    /// Exercises the cookie/CSRF login flow and cache handlers without HTTP routing/extraction.
    /// Covers metadata-only filtering, pagination bounds, value inspection, and expired-row browsing.
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

        // Both metadata reads and destructive mutations must reject a missing session.
        let anonymous = list_entries(
            State(Arc::clone(&state)),
            HeaderMap::new(),
            Query(ListEntriesQuery::default()),
        )
        .await;
        assert_eq!(anonymous.status(), StatusCode::UNAUTHORIZED);
        let anonymous_clear = clear_entries(State(Arc::clone(&state)), HeaderMap::new()).await;
        assert_eq!(anonymous_clear.status(), StatusCode::UNAUTHORIZED);

        // A bad API credential must not establish a console login before the successful flow.
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
        // Pin the currently emitted non-Secure cookie contract, then reuse cookie and CSRF separately.
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

        // Session lookup must return the original CSRF secret and the cached admin role.
        let restored_session = session(State(Arc::clone(&state)), headers.clone()).await;
        assert_eq!(restored_session.status(), StatusCode::OK);
        let body = to_bytes(restored_session.into_body(), 1024).await.unwrap();
        let restored: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(restored["csrf_token"], response["csrf_token"]);
        assert_eq!(restored["role"], "admin");

        // Dashboard observations include recorded requests; its feature switch produces 503.
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
            .set(b"searchable".to_vec(), b"private value".to_vec(), 0);
        state.store.set(b"b".to_vec(), b"another value".to_vec(), 0);

        // Search is case-insensitive over keys, retains the live total, and never lists values.
        let filtered = list_entries(
            State(Arc::clone(&state)),
            headers.clone(),
            Query(ListEntriesQuery {
                page: 1,
                page_size: 1,
                search: "SEARCH".to_owned(),
                ..ListEntriesQuery::default()
            }),
        )
        .await;
        assert_eq!(filtered.status(), StatusCode::OK);
        let body = to_bytes(filtered.into_body(), 1024).await.unwrap();
        let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(result["total"], 3);
        assert_eq!(result["matching"], 1);
        assert_eq!(result["entries"][0]["key"], "73656172636861626c65");
        assert!(result["entries"][0].get("value").is_none());
        assert_eq!(result["pagination"]["total_pages"], 1);
        assert!(!result["pagination"]["has_next"].as_bool().unwrap());

        // The default ascending key order determines which row appears at the next page offset.
        let second_page = list_entries(
            State(Arc::clone(&state)),
            headers.clone(),
            Query(ListEntriesQuery {
                page: 2,
                page_size: 1,
                search: String::new(),
                ..ListEntriesQuery::default()
            }),
        )
        .await;
        let body = to_bytes(second_page.into_body(), 1024).await.unwrap();
        let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(result["page"], 2);
        assert_eq!(result["entries"][0]["key"], "6b");
        assert_eq!(result["pagination"]["has_previous"], true);

        // An extreme page index returns an empty page rather than panicking on a slice boundary.
        let out_of_range = list_entries(
            State(Arc::clone(&state)),
            headers.clone(),
            Query(ListEntriesQuery {
                page: usize::MAX,
                page_size: 1,
                ..ListEntriesQuery::default()
            }),
        )
        .await;
        let body = to_bytes(out_of_range.into_body(), 1024).await.unwrap();
        let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(result["data"].as_array().unwrap().is_empty());

        // Value inspection after replacement must return the current value and its updated size.
        state
            .store
            .set(b"searchable".to_vec(), b"fresh primary value".to_vec(), 0);
        let inspected = get_entry(
            State(Arc::clone(&state)),
            headers.clone(),
            Path("73656172636861626c65".to_owned()),
        )
        .await;
        let body = to_bytes(inspected.into_body(), 1024).await.unwrap();
        let inspected: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            inspected["size"],
            "searchable".len() + "fresh primary value".len()
        );
        assert_eq!(inspected["value"], encode_hex(b"fresh primary value"));

        let stats = cache_stats(State(Arc::clone(&state)), headers.clone()).await;
        let body = to_bytes(stats.into_body(), 1024).await.unwrap();
        let stats: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(stats["entries"], 3);

        // Non-default size ordering must place the largest key/value entry first when descending.
        let sorted = list_entries(
            State(Arc::clone(&state)),
            headers.clone(),
            Query(ListEntriesQuery {
                sort_by: super::EntrySort::Size,
                order: super::SortOrder::Desc,
                ..ListEntriesQuery::default()
            }),
        )
        .await;
        let body = to_bytes(sorted.into_body(), 2048).await.unwrap();
        let sorted: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(sorted["data"][0]["key"], "73656172636861626c65", "{sorted}");

        // Prefixes address raw key bytes through hex, unlike the text search scenario above.
        let prefix = list_entries(
            State(Arc::clone(&state)),
            headers.clone(),
            Query(ListEntriesQuery {
                prefix: Some("73".to_owned()),
                ..ListEntriesQuery::default()
            }),
        )
        .await;
        let body = to_bytes(prefix.into_body(), 2048).await.unwrap();
        let prefix: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(prefix["pagination"]["total"], 1);

        // An exact-key miss has zero total pages, not a synthetic empty first page in the count.
        let no_matches = list_entries(
            State(Arc::clone(&state)),
            headers.clone(),
            Query(ListEntriesQuery {
                exact_key: Some("ff".to_owned()),
                ..ListEntriesQuery::default()
            }),
        )
        .await;
        let body = to_bytes(no_matches.into_body(), 2048).await.unwrap();
        let no_matches: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(no_matches["pagination"]["total_pages"], 0);
        assert!(no_matches["data"].as_array().unwrap().is_empty());

        // The advertised page size must reflect the server cap rather than the oversized request.
        let capped = list_entries(
            State(Arc::clone(&state)),
            headers.clone(),
            Query(ListEntriesQuery {
                page_size: 500,
                ..ListEntriesQuery::default()
            }),
        )
        .await;
        let body = to_bytes(capped.into_body(), 2048).await.unwrap();
        let capped: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(capped["pagination"]["size"], 100);

        // Let a stored row expire without removing it, then select the expired-only snapshot view.
        state.store.set(b"expired".to_vec(), b"value".to_vec(), 1);
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        let expired = list_entries(
            State(Arc::clone(&state)),
            headers.clone(),
            Query(ListEntriesQuery {
                expired: Some(true),
                ..ListEntriesQuery::default()
            }),
        )
        .await;
        let body = to_bytes(expired.into_body(), 2048).await.unwrap();
        let expired: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(expired["pagination"]["total"], 1);
        assert_eq!(expired["data"][0]["key"], "65787069726564");

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

    /// Valid read-only API credentials cannot log in, and a manually seeded non-admin session
    /// still fails read/write/dashboard checks before session lookup removes the invalid role.
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

        // Bypass login to test defense in depth: even correct CSRF cannot elevate a cached role.
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

    /// Bootstrap needs no existing session, returns a usable admin API credential, and cannot repeat.
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

        // Reusing the initial bootstrap credential must not provision a second administrator.
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

    /// An authenticated admin can create/read a user and issue a token whose secret is omitted
    /// from listings; revocation then makes the previously returned API credential unusable.
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

        // Keep the creation-only secret for authentication checks, but never expose it on listing.
        let listed_tokens = list_tokens(
            State(Arc::clone(&state)),
            headers.clone(),
            Path("service".to_owned()),
        )
        .await;
        let listed_tokens = to_bytes(listed_tokens.into_body(), 1024).await.unwrap();
        let listed_tokens: serde_json::Value = serde_json::from_slice(&listed_tokens).unwrap();
        assert!(listed_tokens[0].get("token").is_none());

        // Confirm revocation at the authentication store, not just the handler's success status.
        let revoked = revoke_token(State(state.clone()), headers, Path(token_id)).await;
        assert_eq!(revoked.status(), StatusCode::NO_CONTENT);
        assert_eq!(auth_store.authenticate(&plaintext).unwrap(), None);
    }

    /// Binary cache representations must round-trip zero and high-bit bytes without UTF-8 assumptions.
    #[test]
    fn hex_encoding_round_trips_arbitrary_bytes() {
        let bytes = [0, 10, 127, 128, 255];
        assert_eq!(decode_hex(&encode_hex(&bytes)).unwrap(), bytes);
    }

    /// Odd byte counts and non-hex digits are separate malformed-input cases for the decoder.
    #[test]
    fn rejects_invalid_hex() {
        assert!(decode_hex("0").is_err());
        assert!(decode_hex("0g").is_err());
    }

    /// Checks equality outcomes for a trailing-byte mismatch and unequal lengths, not timing behavior.
    #[test]
    fn password_comparison_checks_all_bytes_and_length() {
        assert!(constant_time_eq("secret", "secret"));
        assert!(!constant_time_eq("secret", "secreu"));
        assert!(!constant_time_eq("secret", "secret1"));
    }
}
