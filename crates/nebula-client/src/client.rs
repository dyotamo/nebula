use std::{
    fs::File,
    io::{self, BufReader},
    path::Path,
    sync::Arc,
};

use nebula_protocol::{read_response, write_request, Opcode, ProtocolError, Request, Status};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};
use tokio_rustls::{
    rustls::{pki_types::ServerName, ClientConfig, RootCertStore},
    TlsConnector,
};

#[derive(Debug, Error)]
pub(super) enum ClientError {
    #[error("connection failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("server returned {status:?}: {message}")]
    Server { status: Status, message: String },
}

pub(super) struct Client {
    address: String,
    server_name: String,
    tls_connector: Option<TlsConnector>,
    token: Option<String>,
}

impl Client {
    pub(super) fn new(
        address: impl Into<String>,
        tls_ca: Option<impl AsRef<Path>>,
        token: Option<String>,
    ) -> Result<Self, ClientError> {
        let address = address.into();
        let server_name = address
            .rsplit_once(':')
            .map(|(host, _)| host.trim_start_matches('[').trim_end_matches(']'))
            .unwrap_or(&address)
            .to_owned();
        if token.is_some() && tls_ca.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a trusted TLS CA is required when token authentication is enabled",
            )
            .into());
        }
        let tls_connector = tls_ca.map(load_tls_connector).transpose()?;

        Ok(Self {
            address,
            server_name,
            tls_connector,
            token,
        })
    }

    pub(super) async fn ping(&self) -> Result<(), ClientError> {
        let response = self.send(Request::ping()).await?;
        self.ensure_ok(response.status, response.value)
    }

    pub(super) async fn set(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        ttl_secs: u64,
    ) -> Result<(), ClientError> {
        let response = self
            .send(Request {
                opcode: Opcode::Set,
                key: key.as_ref().to_vec(),
                value: value.as_ref().to_vec(),
                ttl_secs,
            })
            .await?;
        self.ensure_ok(response.status, response.value)
    }

    pub(super) async fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, ClientError> {
        let response = self
            .send(Request {
                opcode: Opcode::Get,
                key: key.as_ref().to_vec(),
                value: Vec::new(),
                ttl_secs: 0,
            })
            .await?;
        match response.status {
            Status::Ok => Ok(Some(response.value)),
            Status::NotFound => Ok(None),
            status => Err(Self::server_error(status, response.value)),
        }
    }

    pub(super) async fn delete(&self, key: impl AsRef<[u8]>) -> Result<bool, ClientError> {
        let response = self
            .send(Request {
                opcode: Opcode::Delete,
                key: key.as_ref().to_vec(),
                value: Vec::new(),
                ttl_secs: 0,
            })
            .await?;
        match response.status {
            Status::Ok => Ok(true),
            Status::NotFound => Ok(false),
            status => Err(Self::server_error(status, response.value)),
        }
    }

    async fn send(&self, request: Request) -> Result<nebula_protocol::Response, ClientError> {
        let stream = TcpStream::connect(&self.address).await?;
        if let Some(connector) = &self.tls_connector {
            let server_name = ServerName::try_from(self.server_name.clone())
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
            let stream = connector.connect(server_name, stream).await?;
            Self::send_on(stream, &request, self.token.as_deref()).await
        } else {
            Self::send_on(stream, &request, self.token.as_deref()).await
        }
    }

    async fn send_on<S>(
        mut stream: S,
        request: &Request,
        token: Option<&str>,
    ) -> Result<nebula_protocol::Response, ClientError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        if let Some(token) = token {
            write_request(&mut stream, &Request::auth(token)).await?;
            let response = read_response(&mut stream).await?;
            if response.status != Status::Ok {
                return Err(Self::server_error(response.status, response.value));
            }
            if response.value != b"AUTH_RESULT" {
                return Err(ClientError::Protocol(ProtocolError::InvalidFrame(
                    "invalid AUTH_RESULT response",
                )));
            }
        }
        write_request(&mut stream, request).await?;
        Ok(read_response(&mut stream).await?)
    }

    fn ensure_ok(&self, status: Status, value: Vec<u8>) -> Result<(), ClientError> {
        match status {
            Status::Ok => Ok(()),
            status => Err(Self::server_error(status, value)),
        }
    }

    fn server_error(status: Status, value: Vec<u8>) -> ClientError {
        ClientError::Server {
            status,
            message: String::from_utf8_lossy(&value).into_owned(),
        }
    }
}

fn load_tls_connector(path: impl AsRef<Path>) -> Result<TlsConnector, ClientError> {
    let certificates = rustls_pemfile::certs(&mut BufReader::new(File::open(path)?))
        .collect::<Result<Vec<_>, _>>()?;
    if certificates.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "TLS CA file contains no certificates",
        )
        .into());
    }

    let mut roots = RootCertStore::empty();
    for certificate in certificates {
        roots
            .add(certificate)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    }
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(TlsConnector::from(Arc::new(config)))
}
