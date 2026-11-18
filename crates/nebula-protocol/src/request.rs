/// Request operation codes; numeric discriminants are part of the wire format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Opcode {
    Set = 1,
    Get = 2,
    Delete = 3,
    Ping = 4,
    Auth = 5,
}

impl TryFrom<u8> for Opcode {
    type Error = crate::ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Set),
            2 => Ok(Self::Get),
            3 => Ok(Self::Delete),
            4 => Ok(Self::Ping),
            5 => Ok(Self::Auth),
            other => Err(crate::ProtocolError::UnknownOpcode(other)),
        }
    }
}

/// Decoded request with owned byte payloads; frame codecs enforce operation-specific shape.
/// AUTH payloads contain bearer secrets, including in this type's derived `Debug` output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub opcode: Opcode,
    pub key: Vec<u8>,
    /// Cache bytes for SET, bearer-token bytes for AUTH, otherwise empty.
    pub value: Vec<u8>,
    /// TTL in seconds for SET; zero means no expiration. AUTH requires zero.
    pub ttl_secs: u64,
}

impl Request {
    /// Builds a payload-free liveness request.
    pub fn ping() -> Self {
        Self {
            opcode: Opcode::Ping,
            key: Vec::new(),
            value: Vec::new(),
            ttl_secs: 0,
        }
    }

    /// Copies a bearer token into the value; codecs reject empty tokens or tokens over 1 KiB.
    pub fn auth(token: impl AsRef<[u8]>) -> Self {
        Self {
            opcode: Opcode::Auth,
            key: Vec::new(),
            value: token.as_ref().to_vec(),
            ttl_secs: 0,
        }
    }
}
