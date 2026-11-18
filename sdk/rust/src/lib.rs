//! Asynchronous NebulaCache client with a shared, bounded connection pool.
//!
//! Cloned clients share configuration, connections, and shutdown state. Connections
//! are opened on demand, optionally secured with TLS, and authenticated before use
//! when a token is configured. Keys are UTF-8 strings; values remain arbitrary bytes.
//!
//! Only GET requests retry transient failures. Mutations are not replayed because
//! a lost response does not establish whether the server applied the operation.

use std::{
    fs::File,
    io::BufReader,
    net::IpAddr,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use nebula_protocol::{read_response, write_request, Opcode, ProtocolError, Request, Status};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    sync::{watch, Mutex, OwnedSemaphorePermit, Semaphore},
    time::timeout,
};
use tokio_rustls::{
    rustls::{pki_types::ServerName, ClientConfig, RootCertStore},
    TlsConnector,
};

// Erase the transport type so pooled connections can use either TCP or TLS.
type BoxedStream = Pin<Box<dyn AsyncStream + Send>>;

// Combine the read/write bounds into one trait that can be used as a trait object.
trait AsyncStream: AsyncRead + AsyncWrite + Unpin {}

impl<T> AsyncStream for T where T: AsyncRead + AsyncWrite + Unpin {}

/// Failures from configuration, transport setup, protocol exchange, or the server.
#[derive(Debug, Error)]
pub enum NebulaCacheError {
    /// Connection setup, TLS, or timeout failure, with a diagnostic message.
    #[error("connection failure: {0}")]
    Connection(String),
    /// A framing or I/O failure reported by the binary protocol implementation.
    #[error("protocol failure: {0}")]
    Protocol(#[from] ProtocolError),
    /// A decoded server response whose status is not accepted by the operation.
    ///
    /// `status` preserves the server status for programmatic handling; `message`
    /// is the response payload decoded as UTF-8 with invalid sequences replaced.
    #[error("NebulaCache returned status {status:?}: {message}")]
    Response { status: Status, message: String },
    /// The shared client has been explicitly closed.
    #[error("NebulaCache client is closed")]
    Closed,
    /// Configuration or request data failed local validation.
    #[error("invalid input: {0}")]
    InvalidInput(String),
}

/// Connection, pooling, timeout, and authentication settings for a client.
///
/// Constructors validate settings but open connections only when a request needs
/// one. All timeout and lifetime durations must be nonzero.
#[derive(Clone, Debug)]
pub struct Config {
    /// Server endpoint as `host:port` or `[IPv6]:port`; defaults to `127.0.0.1:17171`.
    pub address: String,
    /// Maximum number of simultaneous connection leases; defaults to 10.
    /// Must be greater than zero.
    pub max_pool_size: usize,
    /// Deadline for TCP setup, any TLS handshake, and authentication together.
    /// Defaults to five seconds.
    pub connect_timeout: Duration,
    /// Deadline for a request/response exchange, also applied to authentication.
    /// Defaults to five seconds and applies separately to each retry attempt.
    pub request_timeout: Duration,
    /// Maximum wait for a pool permit, excluding connection setup.
    /// Defaults to five seconds.
    pub acquire_timeout: Duration,
    /// Idle age after which a connection is discarded when considered for reuse.
    /// Defaults to 60 seconds; there is no background eviction task.
    pub idle_timeout: Duration,
    /// Maximum connection age before it is retired on acquisition or release.
    /// Defaults to 300 seconds and does not interrupt an active exchange.
    pub max_lifetime: Duration,
    /// Additional attempts for GET after transient failures; defaults to two.
    /// Other operations are sent at most once.
    pub max_retries: usize,
    /// PEM CA bundle used to verify the server when TLS is enabled.
    /// Defaults to `None` for plaintext; only certificates in this file are trusted.
    pub tls_ca_path: Option<PathBuf>,
    /// Optional API token sent once on each new connection before other requests.
    /// Must contain 1 to 1024 UTF-8 bytes. Defaults to `None`; does not enable TLS.
    pub token: Option<String>,
}

/// Uses a local plaintext endpoint, bounded pooling, and retries only for GET.
impl Default for Config {
    fn default() -> Self {
        Self {
            address: "127.0.0.1:17171".to_owned(),
            max_pool_size: 10,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            acquire_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(60),
            max_lifetime: Duration::from_secs(300),
            max_retries: 2,
            tls_ca_path: None,
            token: None,
        }
    }
}

// Keep absolute age separate from idle age so reuse cannot extend max_lifetime.
struct Connection {
    stream: BoxedStream,
    created_at: Instant,
    idle_since: Instant,
}

// All cloned handles share this state; the idle-list lock never spans network I/O.
struct Inner {
    config: Config,
    host: String,
    tls_connector: Option<TlsConnector>,
    idle: Mutex<Vec<Connection>>,
    // A permit covers both connection setup and the subsequent request exchange.
    permits: Arc<Semaphore>,
    // The atomic flag rejects new work; the watch channel wakes asynchronous waits.
    closed: AtomicBool,
    close_signal: watch::Sender<bool>,
}

/// Asynchronous client whose clones share a bounded pool and shutdown state.
///
/// Construction is lazy: the first operation opens a connection. Concurrent
/// operations lease separate connections rather than multiplexing one stream.
/// Calling [`Self::close`] affects every clone.
#[derive(Clone)]
pub struct NebulaCache {
    inner: Arc<Inner>,
}

// Owning the permit makes cancellation release capacity automatically. An
// unreleased connection is dropped, not pooled with a potentially partial frame.
struct Lease {
    connection: Option<Connection>,
    _permit: OwnedSemaphorePermit,
}

impl Config {
    /// Sets the endpoint and retains all other [`Config::default`] settings.
    ///
    /// Validation is deferred until [`NebulaCache::with_config`] is called.
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            ..Self::default()
        }
    }
}

impl NebulaCache {
    /// Creates a lazy client for an endpoint using the default configuration.
    ///
    /// # Errors
    ///
    /// Returns [`NebulaCacheError::InvalidInput`] if the endpoint is malformed.
    /// Reachability is checked only when an operation opens a connection.
    pub fn new(address: impl Into<String>) -> Result<Self, NebulaCacheError> {
        Self::with_config(Config::new(address))
    }

    /// Validates configuration and prepares a pool without contacting the server.
    ///
    /// A configured CA bundle is loaded immediately, while TCP, TLS negotiation,
    /// and token authentication are deferred until connections are needed.
    ///
    /// # Errors
    ///
    /// Returns [`NebulaCacheError::InvalidInput`] for invalid settings or CA data,
    /// or [`NebulaCacheError::Connection`] when the CA file cannot be read or parsed.
    pub fn with_config(config: Config) -> Result<Self, NebulaCacheError> {
        let (host, _) = parse_address(&config.address)?;
        // Reject settings that would prevent progress before building shared state.
        if config.max_pool_size == 0 {
            return Err(NebulaCacheError::InvalidInput(
                "max_pool_size must be greater than zero".to_owned(),
            ));
        }
        for (name, duration) in [
            ("connect_timeout", config.connect_timeout),
            ("request_timeout", config.request_timeout),
            ("acquire_timeout", config.acquire_timeout),
            ("idle_timeout", config.idle_timeout),
            ("max_lifetime", config.max_lifetime),
        ] {
            if duration.is_zero() {
                return Err(NebulaCacheError::InvalidInput(format!(
                    "{name} must be greater than zero"
                )));
            }
        }
        // Token limits are measured in wire bytes, not Unicode character count.
        if let Some(token) = &config.token {
            if token.is_empty() || token.len() > 1024 {
                return Err(NebulaCacheError::InvalidInput(
                    "token must be non-empty and no longer than 1024 UTF-8 bytes".to_owned(),
                ));
            }
        }
        // TLS is opt-in through a CA file; authentication alone leaves TCP plaintext.
        let tls_connector = config
            .tls_ca_path
            .as_ref()
            .map(|path| load_tls_connector(path))
            .transpose()?;
        // Start with an empty pool; all handles observe the same shutdown channel.
        let (close_signal, _) = watch::channel(false);
        Ok(Self {
            inner: Arc::new(Inner {
                permits: Arc::new(Semaphore::new(config.max_pool_size)),
                config,
                host,
                tls_connector,
                idle: Mutex::new(Vec::new()),
                closed: AtomicBool::new(false),
                close_signal,
            }),
        })
    }

    /// Checks server responsiveness with a single PING exchange, without retries.
    ///
    /// # Errors
    ///
    /// Returns transport, server, or closure errors, or a protocol error if a
    /// successful response does not contain the expected `PONG` payload.
    pub async fn ping(&self) -> Result<(), NebulaCacheError> {
        let response = self.request(Request::ping(), false).await?;
        ensure_ok(response.status, &response.value)?;
        // A success status alone does not establish the command-specific reply.
        if response.value != b"PONG" {
            return Err(NebulaCacheError::Protocol(ProtocolError::InvalidFrame(
                "PING response did not contain PONG",
            )));
        }
        Ok(())
    }

    /// Stores or replaces a binary value without expiration.
    ///
    /// # Errors
    ///
    /// Returns the same validation and exchange errors as [`Self::set_with_ttl`].
    pub async fn set(&self, key: &str, value: impl AsRef<[u8]>) -> Result<(), NebulaCacheError> {
        self.set_with_ttl(key, value, 0).await
    }

    /// Stores or replaces a binary value with a TTL in whole seconds.
    ///
    /// A zero TTL disables expiration. Keys must be nonempty and fit within
    /// [`nebula_protocol::MAX_KEY_LEN`] UTF-8 bytes; values may be empty but must
    /// fit within [`nebula_protocol::MAX_VALUE_LEN`] bytes. SET is never retried:
    /// an exchange failure can leave it unknown whether the write took effect.
    ///
    /// # Errors
    ///
    /// Returns [`NebulaCacheError::InvalidInput`] for invalid keys or oversized
    /// values, or an error from connection acquisition, exchange, or server status.
    pub async fn set_with_ttl(
        &self,
        key: &str,
        value: impl AsRef<[u8]>,
        ttl_secs: u64,
    ) -> Result<(), NebulaCacheError> {
        let key = encode_key(key)?;
        let value = value.as_ref();
        // Validate the wire limit before copying data or acquiring a connection.
        if value.len() > nebula_protocol::MAX_VALUE_LEN {
            return Err(NebulaCacheError::InvalidInput(format!(
                "value exceeds the {}-byte protocol limit",
                nebula_protocol::MAX_VALUE_LEN
            )));
        }
        // Keep raw bytes and TTL in the request; the protocol layer encodes framing.
        let response = self
            .request(
                Request {
                    opcode: Opcode::Set,
                    key,
                    value: value.to_vec(),
                    ttl_secs,
                },
                false,
            )
            .await?;
        ensure_ok(response.status, &response.value)
    }

    /// Stores UTF-8 text as its bytes, without expiration or extra serialization.
    ///
    /// # Errors
    ///
    /// Returns the same validation and exchange errors as [`Self::set`].
    pub async fn set_string(&self, key: &str, value: &str) -> Result<(), NebulaCacheError> {
        self.set(key, value.as_bytes()).await
    }

    /// Stores UTF-8 text as its bytes with a TTL; zero disables expiration.
    ///
    /// # Errors
    ///
    /// Returns the same validation and exchange errors as [`Self::set_with_ttl`].
    pub async fn set_string_with_ttl(
        &self,
        key: &str,
        value: &str,
        ttl_secs: u64,
    ) -> Result<(), NebulaCacheError> {
        self.set_with_ttl(key, value.as_bytes(), ttl_secs).await
    }

    /// Fetches raw bytes, returning `None` when the key is absent or expired.
    ///
    /// An empty stored value is `Some(Vec::new())`, not a cache miss. Transient
    /// failures may be retried up to [`Config::max_retries`] additional times.
    ///
    /// # Errors
    ///
    /// Returns [`NebulaCacheError::InvalidInput`] for an empty or oversized key,
    /// or an acquisition, exchange, or server error after applicable retries.
    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, NebulaCacheError> {
        // GET carries only a key; the unused value and TTL fields remain empty/zero.
        let response = self
            .request(
                Request {
                    opcode: Opcode::Get,
                    key: encode_key(key)?,
                    value: Vec::new(),
                    ttl_secs: 0,
                },
                true,
            )
            .await?;
        // A miss is an expected result, while other non-OK statuses remain errors.
        match response.status {
            Status::Ok => Ok(Some(response.value)),
            Status::NotFound => Ok(None),
            status => Err(response_error(status, &response.value)),
        }
    }

    /// Removes a key, returning whether the server reported an existing entry.
    ///
    /// A missing key returns `false`. DELETE is not retried because replay could
    /// change the reported result or remove a value written by another client.
    ///
    /// # Errors
    ///
    /// Returns [`NebulaCacheError::InvalidInput`] for an empty or oversized key,
    /// or an error from acquisition, exchange, or an unexpected server status.
    pub async fn delete(&self, key: &str) -> Result<bool, NebulaCacheError> {
        let response = self
            .request(
                Request {
                    opcode: Opcode::Delete,
                    key: encode_key(key)?,
                    value: Vec::new(),
                    ttl_secs: 0,
                },
                false,
            )
            .await?;
        // Preserve the distinction between deletion and an already-missing key.
        match response.status {
            Status::Ok => Ok(true),
            Status::NotFound => Ok(false),
            status => Err(response_error(status, &response.value)),
        }
    }

    /// Closes the shared client, drops idle connections, and signals active waits.
    ///
    /// This operation is idempotent and affects all clones. It does not wait for
    /// active tasks to finish; subsequent valid requests fail with
    /// [`NebulaCacheError::Closed`]. A cancelled write may still reach the server.
    pub async fn close(&self) {
        // Only the first caller broadcasts shutdown and drains the idle pool.
        if !self.inner.closed.swap(true, Ordering::AcqRel) {
            let _ = self.inner.close_signal.send(true);
            self.inner.idle.lock().await.clear();
        }
    }

    // Retry policy belongs here rather than in the transport so callers decide
    // whether replay is safe. A decoded server error response is never retried.
    async fn request(
        &self,
        request: Request,
        retry: bool,
    ) -> Result<nebula_protocol::Response, NebulaCacheError> {
        // Count the first send as well as retries without overflowing user input.
        let attempts = if retry {
            self.inner.config.max_retries.saturating_add(1)
        } else {
            1
        };
        for attempt in 0..attempts {
            match self.send_once(&request).await {
                Ok(response) => return Ok(response),
                Err(error) if retry && attempt + 1 < attempts && is_transient(&error) => {
                    // Exponential backoff caps at one second; jitter spreads retries
                    // across the upper half of the current delay window.
                    let max_ms = 50u64.saturating_mul(1u64 << attempt.min(10)).min(1000);
                    let delay = Duration::from_millis(max_ms / 2 + jitter(max_ms / 2));
                    let mut close_signal = self.inner.close_signal.subscribe();
                    // Closing the client can interrupt backoff rather than wait it out.
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {},
                        _ = close_signal.wait_for(|closed| *closed) => return Err(NebulaCacheError::Closed),
                    }
                }
                Err(error) => return Err(error),
            }
        }
        // There is always at least one attempt, and the final attempt returns.
        unreachable!()
    }

    // A lease exclusively owns its stream until one response frame is consumed.
    async fn send_once(
        &self,
        request: &Request,
    ) -> Result<nebula_protocol::Response, NebulaCacheError> {
        let mut lease = self.acquire().await?;
        let connection = lease
            .connection
            .as_mut()
            .expect("new lease has a connection");
        let mut close_signal = self.inner.close_signal.subscribe();
        // One deadline covers writing and reading. Protocol helpers encode and
        // decode complete binary frames rather than relying on TCP read boundaries.
        let exchange = timeout(self.inner.config.request_timeout, async {
            write_request(&mut connection.stream, request).await?;
            read_response(&mut connection.stream).await
        });
        // Keep framing/I/O errors distinct from the SDK's exchange deadline.
        let response = tokio::select! {
            _ = close_signal.wait_for(|closed| *closed) => Err(NebulaCacheError::Closed),
            result = exchange => match result {
                Ok(Ok(response)) => Ok(response),
                Ok(Err(error)) => Err(NebulaCacheError::Protocol(error)),
                Err(_) => Err(NebulaCacheError::Connection("request timed out".to_owned())),
            }
        };
        match response {
            Ok(response) => {
                // A fully decoded frame permits reuse even if its status is an error.
                self.release(lease, true).await;
                Ok(response)
            }
            Err(error) => {
                // A partial write/read or cancellation can leave frame boundaries
                // uncertain, so the connection must not return to the idle pool.
                self.release(lease, false).await;
                Err(error)
            }
        }
    }

    // Reserve capacity before reusing or creating a connection, so concurrent
    // connection attempts cannot exceed the same bound as active requests.
    async fn acquire(&self) -> Result<Lease, NebulaCacheError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(NebulaCacheError::Closed);
        }
        let mut close_signal = self.inner.close_signal.subscribe();
        // Pool saturation has its own deadline, independent of network deadlines.
        let permit = timeout(self.inner.config.acquire_timeout, async {
            tokio::select! {
                _ = close_signal.wait_for(|closed| *closed) => Err(NebulaCacheError::Closed),
                result = self.inner.permits.clone().acquire_owned() => {
                    result.map_err(|_| NebulaCacheError::Closed)
                }
            }
        })
        .await
        .map_err(|_| {
            NebulaCacheError::Connection("timed out acquiring a connection".to_owned())
        })??;
        // Shutdown may have raced with a permit becoming available.
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(NebulaCacheError::Closed);
        }
        let now = Instant::now();
        let connection = {
            let mut idle = self.inner.idle.lock().await;
            // Pop the most recently returned connection first and lazily discard
            // candidates that exceed either age limit without probing the network.
            loop {
                match idle.pop() {
                    Some(connection)
                        if now.duration_since(connection.idle_since)
                            < self.inner.config.idle_timeout
                            && now.duration_since(connection.created_at)
                                < self.inner.config.max_lifetime =>
                    {
                        break Some(connection)
                    }
                    Some(_) => continue,
                    None => break None,
                }
            }
        };
        // Drop the idle-list lock before any TCP, TLS, or authentication awaits.
        let connection = match connection {
            Some(connection) => connection,
            None => self.connect().await?,
        };
        Ok(Lease {
            connection: Some(connection),
            _permit: permit,
        })
    }

    // A connection becomes pool-eligible only after transport setup and any AUTH
    // exchange succeed; failures drop the partially initialized stream.
    async fn connect(&self) -> Result<Connection, NebulaCacheError> {
        let mut close_signal = self.inner.close_signal.subscribe();
        // The outer deadline bounds the entire setup, including nested AUTH work.
        let connect = timeout(self.inner.config.connect_timeout, async {
            let socket = TcpStream::connect(&self.inner.config.address)
                .await
                .map_err(|error| NebulaCacheError::Connection(error.to_string()))?;
            // Small request/response exchanges should not wait on Nagle buffering.
            socket
                .set_nodelay(true)
                .map_err(|error| NebulaCacheError::Connection(error.to_string()))?;
            let mut stream: BoxedStream = if let Some(connector) = &self.inner.tls_connector {
                // Verify the certificate against the endpoint's DNS name or IP,
                // not the address string containing its port or IPv6 brackets.
                let server_name = server_name(&self.inner.host)?;
                Box::pin(
                    connector
                        .connect(server_name, socket)
                        .await
                        .map_err(|error| NebulaCacheError::Connection(error.to_string()))?,
                )
            } else {
                // No CA bundle means plaintext, even when a token is configured.
                Box::pin(socket)
            };
            if let Some(token) = &self.inner.config.token {
                // Authentication is per connection and precedes application frames.
                // The token is encrypted in transit only if TLS was enabled above.
                let auth = Request::auth(token);
                let response = timeout(self.inner.config.request_timeout, async {
                    write_request(&mut stream, &auth).await?;
                    read_response(&mut stream).await
                })
                .await
                .map_err(|_| {
                    NebulaCacheError::Connection("authentication timed out".to_owned())
                })??;
                ensure_auth(response)?;
            }
            Ok::<_, NebulaCacheError>(stream)
        });
        // Shutdown can abort setup instead of waiting for the connection deadline.
        let stream = tokio::select! {
            _ = close_signal.wait_for(|closed| *closed) => return Err(NebulaCacheError::Closed),
            result = connect => result
                .map_err(|_| NebulaCacheError::Connection("connection timed out".to_owned()))??,
        };
        // Lifetime accounting starts once the connection is ready for requests.
        let now = Instant::now();
        Ok(Connection {
            stream,
            created_at: now,
            idle_since: now,
        })
    }

    // Only reusable streams return to the pool; all others close when dropped.
    async fn release(&self, mut lease: Lease, healthy: bool) {
        let Some(mut connection) = lease.connection.take() else {
            return;
        };
        if healthy
            && !self.inner.closed.load(Ordering::Acquire)
            && connection.created_at.elapsed() < self.inner.config.max_lifetime
        {
            connection.idle_since = Instant::now();
            let mut idle = self.inner.idle.lock().await;
            // Recheck under the lock so shutdown cannot drain the pool and then
            // have this task put a connection back into it.
            if !self.inner.closed.load(Ordering::Acquire) {
                idle.push(connection);
            }
        }
        // Dropping the lease releases capacity after the pool decision is complete.
    }
}

fn parse_address(address: &str) -> Result<(String, u16), NebulaCacheError> {
    let (host, port) = if let Some(rest) = address.strip_prefix('[') {
        let (host, rest) = rest.split_once(']').ok_or_else(|| {
            NebulaCacheError::InvalidInput("address must be a host:port string".to_owned())
        })?;
        let port = rest.strip_prefix(':').ok_or_else(|| {
            NebulaCacheError::InvalidInput("address must be a host:port string".to_owned())
        })?;
        (host, port)
    } else {
        address.rsplit_once(':').ok_or_else(|| {
            NebulaCacheError::InvalidInput("address must be a host:port string".to_owned())
        })?
    };
    let port = port.parse::<u16>().map_err(|_| {
        NebulaCacheError::InvalidInput("address contains an invalid port".to_owned())
    })?;
    if host.is_empty() || port == 0 {
        return Err(NebulaCacheError::InvalidInput(
            "address must contain a host and a port in 1..=65535".to_owned(),
        ));
    }
    Ok((host.to_owned(), port))
}

fn encode_key(key: &str) -> Result<Vec<u8>, NebulaCacheError> {
    if key.is_empty() {
        return Err(NebulaCacheError::InvalidInput(
            "key must not be empty".to_owned(),
        ));
    }
    if key.len() > nebula_protocol::MAX_KEY_LEN {
        return Err(NebulaCacheError::InvalidInput(format!(
            "key exceeds the {}-byte protocol limit",
            nebula_protocol::MAX_KEY_LEN
        )));
    }
    Ok(key.as_bytes().to_vec())
}

fn ensure_ok(status: Status, value: &[u8]) -> Result<(), NebulaCacheError> {
    match status {
        Status::Ok => Ok(()),
        status => Err(response_error(status, value)),
    }
}

fn ensure_auth(response: nebula_protocol::Response) -> Result<(), NebulaCacheError> {
    ensure_ok(response.status, &response.value)?;
    if response.value != b"AUTH_RESULT" {
        return Err(NebulaCacheError::Protocol(ProtocolError::InvalidFrame(
            "invalid AUTH_RESULT response",
        )));
    }
    Ok(())
}

fn response_error(status: Status, value: &[u8]) -> NebulaCacheError {
    NebulaCacheError::Response {
        status,
        message: String::from_utf8_lossy(value).into_owned(),
    }
}

fn is_transient(error: &NebulaCacheError) -> bool {
    matches!(
        error,
        NebulaCacheError::Connection(_) | NebulaCacheError::Protocol(ProtocolError::Io(_))
    )
}

fn jitter(upper: u64) -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.subsec_nanos() as u64);
    nanos.wrapping_mul(6364136223846793005).wrapping_add(1) % upper.saturating_add(1)
}

fn server_name(host: &str) -> Result<ServerName<'static>, NebulaCacheError> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(ServerName::IpAddress(ip.into()));
    }
    ServerName::try_from(host.to_owned()).map_err(|error| {
        NebulaCacheError::InvalidInput(format!("invalid TLS server name: {error}"))
    })
}

fn load_tls_connector(path: &Path) -> Result<TlsConnector, NebulaCacheError> {
    let certificates = rustls_pemfile::certs(&mut BufReader::new(
        File::open(path).map_err(|error| NebulaCacheError::Connection(error.to_string()))?,
    ))
    .collect::<Result<Vec<_>, _>>()
    .map_err(|error| NebulaCacheError::Connection(error.to_string()))?;
    if certificates.is_empty() {
        return Err(NebulaCacheError::InvalidInput(
            "TLS CA file contains no certificates".to_owned(),
        ));
    }
    let mut roots = RootCertStore::empty();
    for certificate in certificates {
        roots.add(certificate).map_err(|error| {
            NebulaCacheError::InvalidInput(format!("invalid TLS CA certificate: {error}"))
        })?;
    }
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(TlsConnector::from(Arc::new(config)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nebula_protocol::{read_request, write_response, Response};
    use tokio::{net::TcpListener, task::JoinHandle};

    async fn test_server() -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let values = Arc::new(Mutex::new(
                std::collections::HashMap::<Vec<u8>, Vec<u8>>::new(),
            ));
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let values = Arc::clone(&values);
                tokio::spawn(async move {
                    loop {
                        let request = match read_request(&mut stream).await {
                            Ok(request) => request,
                            Err(_) => break,
                        };
                        let response = match request.opcode {
                            Opcode::Ping => Response::ok(b"PONG".to_vec()),
                            Opcode::Set => {
                                values.lock().await.insert(request.key, request.value);
                                Response::empty_ok()
                            }
                            Opcode::Get => values
                                .lock()
                                .await
                                .get(&request.key)
                                .cloned()
                                .map(Response::ok)
                                .unwrap_or_else(Response::not_found),
                            Opcode::Delete => values
                                .lock()
                                .await
                                .remove(&request.key)
                                .map(|_| Response::empty_ok())
                                .unwrap_or_else(Response::not_found),
                            Opcode::Auth => Response::ok(b"AUTH_RESULT".to_vec()),
                        };
                        if write_response(&mut stream, &response).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });
        (address, server)
    }

    #[tokio::test]
    async fn supports_ping_and_key_lifecycle() {
        let (address, server) = test_server().await;
        let client = NebulaCache::new(address).unwrap();
        assert_eq!(client.get("missing").await.unwrap(), None);
        client.set_string("greeting", "hello").await.unwrap();
        assert_eq!(
            client.get("greeting").await.unwrap(),
            Some(b"hello".to_vec())
        );
        assert!(client.delete("greeting").await.unwrap());
        assert!(!client.delete("greeting").await.unwrap());
        client.ping().await.unwrap();
        client.close().await;
        assert!(matches!(client.ping().await, Err(NebulaCacheError::Closed)));
        server.abort();
    }

    #[tokio::test]
    async fn validates_keys_and_configuration() {
        let client = NebulaCache::new("127.0.0.1:17171").unwrap();
        assert!(matches!(
            client.get("").await,
            Err(NebulaCacheError::InvalidInput(_))
        ));
        assert!(matches!(
            client
                .set("key", vec![0; nebula_protocol::MAX_VALUE_LEN + 1])
                .await,
            Err(NebulaCacheError::InvalidInput(_))
        ));
        let config = Config {
            max_pool_size: 0,
            ..Config::default()
        };
        assert!(matches!(
            NebulaCache::with_config(config),
            Err(NebulaCacheError::InvalidInput(_))
        ));
        let token_client = NebulaCache::with_config(Config {
            token: Some("token".to_owned()),
            ..Config::default()
        })
        .unwrap();
        token_client.close().await;
        client.close().await;
    }
}
