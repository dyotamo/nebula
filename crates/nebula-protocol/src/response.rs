/// Response status codes; numeric discriminants are part of the wire format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Status {
    Ok = 0,
    NotFound = 1,
    InvalidArgument = 2,
    InternalError = 3,
    /// Authentication is missing or invalid.
    Unauthorized = 4,
    /// The authenticated identity lacks permission for the operation.
    Forbidden = 5,
}

impl TryFrom<u8> for Status {
    type Error = crate::ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Ok),
            1 => Ok(Self::NotFound),
            2 => Ok(Self::InvalidArgument),
            3 => Ok(Self::InternalError),
            4 => Ok(Self::Unauthorized),
            5 => Ok(Self::Forbidden),
            other => Err(crate::ProtocolError::UnknownStatus(other)),
        }
    }
}

/// Application result carried inside a successfully decoded protocol frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub status: Status,
    /// Opaque result bytes or an error message, interpreted according to the status.
    pub value: Vec<u8>,
}

impl Response {
    /// Wraps result bytes in an OK response; frame writers enforce payload limits separately.
    pub fn ok(value: Vec<u8>) -> Self {
        Self {
            status: Status::Ok,
            value,
        }
    }
    /// Builds an empty acknowledgement, as used by successful SET and DELETE operations.
    pub fn empty_ok() -> Self {
        Self::ok(Vec::new())
    }
    /// Builds a cache miss response with no value payload.
    pub fn not_found() -> Self {
        Self {
            status: Status::NotFound,
            value: Vec::new(),
        }
    }
    /// Rejects an operation with a fixed, generic error message.
    pub fn invalid_argument() -> Self {
        Self {
            status: Status::InvalidArgument,
            value: b"invalid argument".to_vec(),
        }
    }
    /// Copies a message verbatim for transmission; callers must exclude sensitive details.
    pub fn internal_error(message: &[u8]) -> Self {
        Self {
            status: Status::InternalError,
            value: message.to_vec(),
        }
    }
    /// Builds an authentication failure without revealing why credentials were rejected.
    pub fn unauthorized() -> Self {
        Self {
            status: Status::Unauthorized,
            value: b"authentication failed".to_vec(),
        }
    }
    /// Builds an authorization failure, distinct from rejected authentication.
    pub fn forbidden() -> Self {
        Self {
            status: Status::Forbidden,
            value: b"operation is not permitted".to_vec(),
        }
    }
}
