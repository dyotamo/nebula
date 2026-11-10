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

type BoxedStream = Pin<Box<dyn AsyncStream + Send>>;

trait AsyncStream: AsyncRead + AsyncWrite + Unpin {}

impl<T> AsyncStream for T where T: AsyncRead + AsyncWrite + Unpin {}

#[derive(Debug, Error)]
pub enum NebulaCacheError {
    #[error("connection failure: {0}")]
    Connection(String),
    #[error("protocol failure: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("NebulaCache returned status {status:?}: {message}")]
    Response { status: Status, message: String },
    #[error("NebulaCache client is closed")]
    Closed,
    #[error("invalid input: {0}")]
    InvalidInput(String),
}

#[derive(Clone, Debug)]
pub struct Config {
    pub address: String,
    pub max_pool_size: usize,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub acquire_timeout: Duration,
    pub idle_timeout: Duration,
    pub max_lifetime: Duration,
    pub max_retries: usize,
    pub tls_ca_path: Option<PathBuf>,
    pub token: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            address: "127.0.0.1:7001".to_owned(),
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

struct Connection {
    stream: BoxedStream,
    created_at: Instant,
    idle_since: Instant,
}

struct Inner {
    config: Config,
    host: String,
    tls_connector: Option<TlsConnector>,
    idle: Mutex<Vec<Connection>>,
    permits: Arc<Semaphore>,
    closed: AtomicBool,
    close_signal: watch::Sender<bool>,
}

#[derive(Clone)]
pub struct NebulaCache {
    inner: Arc<Inner>,
}

struct Lease {
    connection: Option<Connection>,
    _permit: OwnedSemaphorePermit,
}

impl Config {
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            ..Self::default()
        }
    }
}

impl NebulaCache {
    pub fn new(address: impl Into<String>) -> Result<Self, NebulaCacheError> {
        Self::with_config(Config::new(address))
    }

    pub fn with_config(config: Config) -> Result<Self, NebulaCacheError> {
        let (host, _) = parse_address(&config.address)?;
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
        if let Some(token) = &config.token {
            if token.is_empty() || token.len() > 1024 {
                return Err(NebulaCacheError::InvalidInput(
                    "token must be non-empty and no longer than 1024 UTF-8 bytes".to_owned(),
                ));
            }
            if config.tls_ca_path.is_none() {
                return Err(NebulaCacheError::InvalidInput(
                    "token authentication requires TLS with a trusted CA".to_owned(),
                ));
            }
        }
        let tls_connector = config
            .tls_ca_path
            .as_ref()
            .map(|path| load_tls_connector(path))
            .transpose()?;
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

    pub async fn ping(&self) -> Result<(), NebulaCacheError> {
        let response = self.request(Request::ping(), false).await?;
        ensure_ok(response.status, &response.value)?;
        if response.value != b"PONG" {
            return Err(NebulaCacheError::Protocol(ProtocolError::InvalidFrame(
                "PING response did not contain PONG",
            )));
        }
        Ok(())
    }

    pub async fn set(&self, key: &str, value: impl AsRef<[u8]>) -> Result<(), NebulaCacheError> {
        self.set_with_ttl(key, value, 0).await
    }

    pub async fn set_with_ttl(
        &self,
        key: &str,
        value: impl AsRef<[u8]>,
        ttl_secs: u64,
    ) -> Result<(), NebulaCacheError> {
        let key = encode_key(key)?;
        let value = value.as_ref();
        if value.len() > nebula_protocol::MAX_VALUE_LEN {
            return Err(NebulaCacheError::InvalidInput(format!(
                "value exceeds the {}-byte protocol limit",
                nebula_protocol::MAX_VALUE_LEN
            )));
        }
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

    pub async fn set_string(&self, key: &str, value: &str) -> Result<(), NebulaCacheError> {
        self.set(key, value.as_bytes()).await
    }

    pub async fn set_string_with_ttl(
        &self,
        key: &str,
        value: &str,
        ttl_secs: u64,
    ) -> Result<(), NebulaCacheError> {
        self.set_with_ttl(key, value.as_bytes(), ttl_secs).await
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, NebulaCacheError> {
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
        match response.status {
            Status::Ok => Ok(Some(response.value)),
            Status::NotFound => Ok(None),
            status => Err(response_error(status, &response.value)),
        }
    }

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
        match response.status {
            Status::Ok => Ok(true),
            Status::NotFound => Ok(false),
            status => Err(response_error(status, &response.value)),
        }
    }

    pub async fn close(&self) {
        if !self.inner.closed.swap(true, Ordering::AcqRel) {
            let _ = self.inner.close_signal.send(true);
            self.inner.idle.lock().await.clear();
        }
    }

    async fn request(
        &self,
        request: Request,
        retry: bool,
    ) -> Result<nebula_protocol::Response, NebulaCacheError> {
        let attempts = if retry {
            self.inner.config.max_retries.saturating_add(1)
        } else {
            1
        };
        for attempt in 0..attempts {
            match self.send_once(&request).await {
                Ok(response) => return Ok(response),
                Err(error) if retry && attempt + 1 < attempts && is_transient(&error) => {
                    let max_ms = 50u64.saturating_mul(1u64 << attempt.min(10)).min(1000);
                    let delay = Duration::from_millis(max_ms / 2 + jitter(max_ms / 2));
                    let mut close_signal = self.inner.close_signal.subscribe();
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {},
                        _ = close_signal.wait_for(|closed| *closed) => return Err(NebulaCacheError::Closed),
                    }
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!()
    }

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
        let exchange = timeout(self.inner.config.request_timeout, async {
            write_request(&mut connection.stream, request).await?;
            read_response(&mut connection.stream).await
        });
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
                self.release(lease, true).await;
                Ok(response)
            }
            Err(error) => {
                self.release(lease, false).await;
                Err(error)
            }
        }
    }

    async fn acquire(&self) -> Result<Lease, NebulaCacheError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(NebulaCacheError::Closed);
        }
        let mut close_signal = self.inner.close_signal.subscribe();
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
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(NebulaCacheError::Closed);
        }
        let now = Instant::now();
        let connection = {
            let mut idle = self.inner.idle.lock().await;
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
        let connection = match connection {
            Some(connection) => connection,
            None => self.connect().await?,
        };
        Ok(Lease {
            connection: Some(connection),
            _permit: permit,
        })
    }

    async fn connect(&self) -> Result<Connection, NebulaCacheError> {
        let mut close_signal = self.inner.close_signal.subscribe();
        let connect = timeout(self.inner.config.connect_timeout, async {
            let socket = TcpStream::connect(&self.inner.config.address)
                .await
                .map_err(|error| NebulaCacheError::Connection(error.to_string()))?;
            socket
                .set_nodelay(true)
                .map_err(|error| NebulaCacheError::Connection(error.to_string()))?;
            let mut stream: BoxedStream = if let Some(connector) = &self.inner.tls_connector {
                let server_name = server_name(&self.inner.host)?;
                Box::pin(
                    connector
                        .connect(server_name, socket)
                        .await
                        .map_err(|error| NebulaCacheError::Connection(error.to_string()))?,
                )
            } else {
                Box::pin(socket)
            };
            if let Some(token) = &self.inner.config.token {
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
        let stream = tokio::select! {
            _ = close_signal.wait_for(|closed| *closed) => return Err(NebulaCacheError::Closed),
            result = connect => result
                .map_err(|_| NebulaCacheError::Connection("connection timed out".to_owned()))??,
        };
        let now = Instant::now();
        Ok(Connection {
            stream,
            created_at: now,
            idle_since: now,
        })
    }

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
            if !self.inner.closed.load(Ordering::Acquire) {
                idle.push(connection);
            }
        }
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
        let client = NebulaCache::new("127.0.0.1:7001").unwrap();
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
        client.close().await;
    }
}
