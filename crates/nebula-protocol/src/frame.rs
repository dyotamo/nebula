use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{Opcode, ProtocolError, Request, Response, Status};

/// Prefix identifying both request and response frames.
///
/// Readers compare these four bytes before interpreting the remaining header; writers always
/// emit them first. The prefix is not a checksum and does not validate the payload contents.
pub const MAGIC: [u8; 4] = *b"NEBU";
/// Wire version emitted by writers and accepted by readers.
///
/// Readers reject any other value rather than attempting compatibility fallback or resynchronization.
pub const VERSION: u8 = 1;
/// Default key-length limit in bytes; custom [`FrameLimits`] can override it.
///
/// This bound applies to request keys; response frames have no key field.
pub const MAX_KEY_LEN: usize = 1_024;
/// Default value-length limit in bytes (8 MiB).
///
/// This is the default for both request values and response payloads, except that response
/// codecs retain a four-byte minimum to allow the `PONG` control payload.
pub const MAX_VALUE_LEN: usize = 8 * 1024 * 1024;

/// Per-frame payload limits in bytes, checked before allocation or writing.
/// Responses allow at least four bytes for PONG; AUTH tokens also have a fixed 1 KiB cap.
///
/// The limits are independent of wire-format length-field capacity: a frame must satisfy both
/// its configured bound and the `u32` length representation. Request keys and values are
/// bounded separately; the value limit also applies to response payloads with the PONG exception.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrameLimits {
    /// Maximum request key length; response frames have no key field.
    pub max_key_len: usize,
    /// Maximum request value length; responses use this limit or four bytes, whichever is larger.
    pub max_value_len: usize,
}

impl Default for FrameLimits {
    /// Uses the crate's standard request-key and payload bounds.
    fn default() -> Self {
        Self {
            max_key_len: MAX_KEY_LEN,
            max_value_len: MAX_VALUE_LEN,
        }
    }
}

// Integers are big-endian; payload lengths exclude the header.
// Request: magic(4), version(1), opcode(1), key_len(4), value_len(4), TTL(8).
// Response: magic(4), version(1), status(1), value_len(4).
const REQUEST_HEADER_LEN: usize = 22;
const RESPONSE_HEADER_LEN: usize = 10;

/// Reads one request with default limits; EOF, including truncation, is an I/O error.
///
/// This convenience entry point has the same framing and stream-consumption behavior as
/// [`read_request_with_limits`], using [`FrameLimits::default`].
pub async fn read_request<R>(reader: &mut R) -> Result<Request, ProtocolError>
where
    R: AsyncRead + Unpin,
{
    read_request_with_limits(reader, FrameLimits::default()).await
}

/// Reads one request, validating its header before allocating payload buffers.
/// On rejection the payload is not drained, so the stream is not ready for another frame.
///
/// The fixed-size header is read first, then magic, version, opcode, declared lengths, and
/// operation-specific shape are checked in that order. Only an accepted pair of lengths is used
/// to allocate the owned key and value buffers; short headers or payloads surface as I/O errors.
/// Any error after bytes have been consumed leaves the caller responsible for discarding or
/// otherwise recovering the connection, since this codec does not scan for a later frame boundary.
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

    // Reject declared payload sizes before shape checks or allocation so malformed frames cannot
    // request buffers larger than the caller's configured budget.
    if key_len > limits.max_key_len || value_len > limits.max_value_len {
        return Err(ProtocolError::FrameTooLarge);
    }

    // Control operations have explicit payload shapes; TTL is retained for SET and must be
    // zero for AUTH, whose token has a protocol-level cap independent of custom value limits.
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

/// Validates, writes, and flushes one request using default limits.
///
/// Shape or size errors are returned before output begins. Stream I/O errors can still occur
/// after a prefix of the header or payload has been written, so the stream may then be unusable.
pub async fn write_request<W>(writer: &mut W, request: &Request) -> Result<(), ProtocolError>
where
    W: AsyncWrite + Unpin,
{
    write_request_with_limits(writer, request, FrameLimits::default()).await
}

/// Validates request shape and lengths before writing any bytes, then flushes the frame.
/// An I/O failure may leave a partial frame on the stream.
///
/// Validation first enforces caller limits, then operation-specific key/value/token rules, and
/// finally verifies that payload lengths fit the `u32` fields. The encoder writes the fixed header,
/// key, and value in that order; the TTL field is serialized for every opcode, though only SET
/// assigns it cache-expiration meaning. The final flush sends buffered bytes but does not make
/// a failed or partially written stream safe to reuse.
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

/// Reads one response using default limits; non-OK statuses are returned as data.
///
/// A decoded status indicates the application result and is not itself a codec error. This
/// convenience function delegates to [`read_response_with_limits`] with default bounds.
pub async fn read_response<R>(reader: &mut R) -> Result<Response, ProtocolError>
where
    R: AsyncRead + Unpin,
{
    read_response_with_limits(reader, FrameLimits::default()).await
}

/// Reads one response with the value limit described by [`FrameLimits`].
/// A rejected header leaves its payload unread; no frame resynchronization is attempted.
///
/// The reader consumes and checks the fixed header before allocating the declared payload. The
/// response limit is raised to at least four bytes so a `PONG` can be received even with a zero or
/// smaller configured value limit; all other payloads remain bounded by the configured limit.
/// Truncated headers or values are I/O errors, and after an error the connection should not be
/// assumed to begin at a frame boundary.
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
    // Keep liveness replies possible even when cache values are disabled or tiny.
    if value_len > limits.max_value_len.max(b"PONG".len()) {
        return Err(ProtocolError::FrameTooLarge);
    }

    let mut value = vec![0; value_len];
    reader.read_exact(&mut value).await?;
    Ok(Response { status, value })
}

/// Writes and flushes one response using default limits.
///
/// The response status and value are serialized as supplied; application-level status/value
/// combinations are not interpreted by this framing layer.
pub async fn write_response<W>(writer: &mut W, response: &Response) -> Result<(), ProtocolError>
where
    W: AsyncWrite + Unpin,
{
    write_response_with_limits(writer, response, FrameLimits::default()).await
}

/// Checks the response value limit before writing, then flushes the frame.
/// As on reads, a four-byte minimum limit permits PONG; I/O errors may leave a partial frame.
///
/// The writer rejects an oversized value before emitting any bytes, encodes the status and value
/// length in the fixed response header, and then writes the value. As with request writes, a
/// stream error after output starts can leave the peer with an incomplete frame.
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
