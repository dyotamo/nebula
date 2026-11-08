use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{Opcode, ProtocolError, Request, Response, Status};

pub const MAGIC: [u8; 4] = *b"NEBU";
pub const VERSION: u8 = 1;
pub const MAX_KEY_LEN: usize = 1_024;
pub const MAX_VALUE_LEN: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrameLimits {
    pub max_key_len: usize,
    pub max_value_len: usize,
}

impl Default for FrameLimits {
    fn default() -> Self {
        Self {
            max_key_len: MAX_KEY_LEN,
            max_value_len: MAX_VALUE_LEN,
        }
    }
}

const REQUEST_HEADER_LEN: usize = 22;
const RESPONSE_HEADER_LEN: usize = 10;

/// Reads one complete request. `UnexpectedEof` before any header byte is a clean disconnect.
pub async fn read_request<R>(reader: &mut R) -> Result<Request, ProtocolError>
where
    R: AsyncRead + Unpin,
{
    read_request_with_limits(reader, FrameLimits::default()).await
}

pub async fn read_request_with_limits<R>(
    reader: &mut R,
    limits: FrameLimits,
) -> Result<Request, ProtocolError>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0u8; REQUEST_HEADER_LEN];
    reader.read_exact(&mut header).await?;

    if header[0..4] != MAGIC {
        return Err(ProtocolError::InvalidMagic);
    }

    if header[4] != VERSION {
        return Err(ProtocolError::UnsupportedVersion(header[4]));
    }

    let opcode = Opcode::try_from(header[5])?;
    let key_len = u32::from_be_bytes(header[6..10].try_into().unwrap()) as usize;
    let value_len = u32::from_be_bytes(header[10..14].try_into().unwrap()) as usize;
    let ttl_secs = u64::from_be_bytes(header[14..22].try_into().unwrap());

    if key_len > limits.max_key_len || value_len > limits.max_value_len {
        return Err(ProtocolError::FrameTooLarge);
    }

    if opcode == Opcode::Ping && (key_len != 0 || value_len != 0) {
        return Err(ProtocolError::InvalidFrame(
            "PING must not contain key or value",
        ));
    }

    if opcode == Opcode::Auth
        && (key_len != 0 || value_len == 0 || value_len > 1_024 || ttl_secs != 0)
    {
        return Err(ProtocolError::InvalidFrame(
            "AUTH requires a token and no key or TTL",
        ));
    }

    if matches!(opcode, Opcode::Get | Opcode::Delete) && value_len != 0 {
        return Err(ProtocolError::InvalidFrame(
            "GET/DELETE must not contain a value",
        ));
    }

    if opcode == Opcode::Set && key_len == 0 {
        return Err(ProtocolError::InvalidFrame("SET requires a key"));
    }

    if matches!(opcode, Opcode::Get | Opcode::Delete) && key_len == 0 {
        return Err(ProtocolError::InvalidFrame("GET/DELETE require a key"));
    }

    let mut key = vec![0; key_len];
    let mut value = vec![0; value_len];

    reader.read_exact(&mut key).await?;
    reader.read_exact(&mut value).await?;

    Ok(Request {
        opcode,
        key,
        value,
        ttl_secs,
    })
}

pub async fn write_request<W>(writer: &mut W, request: &Request) -> Result<(), ProtocolError>
where
    W: AsyncWrite + Unpin,
{
    write_request_with_limits(writer, request, FrameLimits::default()).await
}

pub async fn write_request_with_limits<W>(
    writer: &mut W,
    request: &Request,
    limits: FrameLimits,
) -> Result<(), ProtocolError>
where
    W: AsyncWrite + Unpin,
{
    if request.key.len() > limits.max_key_len || request.value.len() > limits.max_value_len {
        return Err(ProtocolError::FrameTooLarge);
    }

    if matches!(request.opcode, Opcode::Set | Opcode::Get | Opcode::Delete)
        && request.key.is_empty()
    {
        return Err(ProtocolError::InvalidFrame("operation requires a key"));
    }

    if request.opcode == Opcode::Auth
        && (!request.key.is_empty()
            || request.value.is_empty()
            || request.value.len() > 1_024
            || request.ttl_secs != 0)
    {
        return Err(ProtocolError::InvalidFrame(
            "AUTH requires a token and no key or TTL",
        ));
    }

    if matches!(request.opcode, Opcode::Get | Opcode::Delete | Opcode::Ping)
        && !request.value.is_empty()
    {
        return Err(ProtocolError::InvalidFrame(
            "operation must not contain a value",
        ));
    }

    if request.opcode == Opcode::Ping && !request.key.is_empty() {
        return Err(ProtocolError::InvalidFrame("PING must not contain a key"));
    }

    let key_len = u32::try_from(request.key.len()).map_err(|_| ProtocolError::FrameTooLarge)?;
    let value_len = u32::try_from(request.value.len()).map_err(|_| ProtocolError::FrameTooLarge)?;
    let mut header = [0u8; REQUEST_HEADER_LEN];

    header[0..4].copy_from_slice(&MAGIC);
    header[4] = VERSION;
    header[5] = request.opcode as u8;
    header[6..10].copy_from_slice(&key_len.to_be_bytes());
    header[10..14].copy_from_slice(&value_len.to_be_bytes());
    header[14..22].copy_from_slice(&request.ttl_secs.to_be_bytes());

    writer.write_all(&header).await?;
    writer.write_all(&request.key).await?;
    writer.write_all(&request.value).await?;
    writer.flush().await?;
    Ok(())
}

pub async fn read_response<R>(reader: &mut R) -> Result<Response, ProtocolError>
where
    R: AsyncRead + Unpin,
{
    read_response_with_limits(reader, FrameLimits::default()).await
}

pub async fn read_response_with_limits<R>(
    reader: &mut R,
    limits: FrameLimits,
) -> Result<Response, ProtocolError>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0u8; RESPONSE_HEADER_LEN];
    reader.read_exact(&mut header).await?;

    if header[0..4] != MAGIC {
        return Err(ProtocolError::InvalidMagic);
    }

    if header[4] != VERSION {
        return Err(ProtocolError::UnsupportedVersion(header[4]));
    }

    let status = Status::try_from(header[5])?;
    let value_len = u32::from_be_bytes(header[6..10].try_into().unwrap()) as usize;
    if value_len > limits.max_value_len.max(b"PONG".len()) {
        return Err(ProtocolError::FrameTooLarge);
    }

    let mut value = vec![0; value_len];
    reader.read_exact(&mut value).await?;
    Ok(Response { status, value })
}

pub async fn write_response<W>(writer: &mut W, response: &Response) -> Result<(), ProtocolError>
where
    W: AsyncWrite + Unpin,
{
    write_response_with_limits(writer, response, FrameLimits::default()).await
}

pub async fn write_response_with_limits<W>(
    writer: &mut W,
    response: &Response,
    limits: FrameLimits,
) -> Result<(), ProtocolError>
where
    W: AsyncWrite + Unpin,
{
    if response.value.len() > limits.max_value_len.max(b"PONG".len()) {
        return Err(ProtocolError::FrameTooLarge);
    }

    let value_len =
        u32::try_from(response.value.len()).map_err(|_| ProtocolError::FrameTooLarge)?;
    let mut header = [0u8; RESPONSE_HEADER_LEN];

    header[0..4].copy_from_slice(&MAGIC);
    header[4] = VERSION;
    header[5] = response.status as u8;
    header[6..10].copy_from_slice(&value_len.to_be_bytes());

    writer.write_all(&header).await?;
    writer.write_all(&response.value).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn rejects_bad_magic() {
        let (mut tx, mut rx) = duplex(64);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"NOPE");
        bytes.push(VERSION);
        bytes.push(Opcode::Ping as u8);
        bytes.extend_from_slice(&0u32.to_be_bytes());
        bytes.extend_from_slice(&0u32.to_be_bytes());
        bytes.extend_from_slice(&0u64.to_be_bytes());
        tx.write_all(&bytes).await.unwrap();
        drop(tx);
        assert!(matches!(
            read_request(&mut rx).await,
            Err(ProtocolError::InvalidMagic)
        ));
    }

    #[tokio::test]
    async fn decodes_ping() {
        let (mut tx, mut rx) = duplex(64);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&MAGIC);
        bytes.push(VERSION);
        bytes.push(Opcode::Ping as u8);
        bytes.extend_from_slice(&0u32.to_be_bytes());
        bytes.extend_from_slice(&0u32.to_be_bytes());
        bytes.extend_from_slice(&0u64.to_be_bytes());
        tx.write_all(&bytes).await.unwrap();
        let req = read_request(&mut rx).await.unwrap();
        assert_eq!(req.opcode, Opcode::Ping);
    }

    #[tokio::test]
    async fn rejects_request_exceeding_custom_limits() {
        let (mut tx, mut rx) = duplex(64);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&MAGIC);
        bytes.push(VERSION);
        bytes.push(Opcode::Set as u8);
        bytes.extend_from_slice(&2u32.to_be_bytes());
        bytes.extend_from_slice(&0u32.to_be_bytes());
        bytes.extend_from_slice(&0u64.to_be_bytes());
        tx.write_all(&bytes).await.unwrap();

        let limits = FrameLimits {
            max_key_len: 1,
            max_value_len: MAX_VALUE_LEN,
        };
        assert!(matches!(
            read_request_with_limits(&mut rx, limits).await,
            Err(ProtocolError::FrameTooLarge)
        ));
    }

    #[tokio::test]
    async fn control_response_fits_below_value_limit() {
        let (mut tx, mut rx) = duplex(64);
        let limits = FrameLimits {
            max_key_len: MAX_KEY_LEN,
            max_value_len: 0,
        };
        let response = Response::ok(b"PONG".to_vec());

        write_response_with_limits(&mut tx, &response, limits)
            .await
            .unwrap();
        assert_eq!(
            read_response_with_limits(&mut rx, limits).await.unwrap(),
            response
        );
    }

    #[test]
    fn status_values_are_stable() {
        assert_eq!(Status::Ok as u8, 0);
        assert_eq!(Status::NotFound as u8, 1);
    }
}
