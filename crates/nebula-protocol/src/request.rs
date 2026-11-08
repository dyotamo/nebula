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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub opcode: Opcode,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    /// TTL in seconds. Zero means no expiration.
    pub ttl_secs: u64,
}

impl Request {
    pub fn ping() -> Self {
        Self {
            opcode: Opcode::Ping,
            key: Vec::new(),
            value: Vec::new(),
            ttl_secs: 0,
        }
    }

    pub fn auth(token: impl AsRef<[u8]>) -> Self {
        Self {
            opcode: Opcode::Auth,
            key: Vec::new(),
            value: token.as_ref().to_vec(),
            ttl_secs: 0,
        }
    }
}
