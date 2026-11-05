use nebula_protocol::{read_response, write_request, Opcode, ProtocolError, Request, Status};
use thiserror::Error;
use tokio::net::TcpStream;

#[derive(Debug, Error)]
pub(super) enum ClientError {
    #[error("connection failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("server returned {status:?}: {message}")]
    Server { status: Status, message: String },
}

#[derive(Debug)]
pub(super) struct Client {
    address: String,
}

impl Client {
    pub(super) fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
        }
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
        let mut stream = TcpStream::connect(&self.address).await?;
        write_request(&mut stream, &request).await?;
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
