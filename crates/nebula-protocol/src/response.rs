#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Status {
    Ok = 0,
    NotFound = 1,
    InvalidArgument = 2,
    InternalError = 3,
}

impl TryFrom<u8> for Status {
    type Error = crate::ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Ok),
            1 => Ok(Self::NotFound),
            2 => Ok(Self::InvalidArgument),
            3 => Ok(Self::InternalError),
            other => Err(crate::ProtocolError::UnknownStatus(other)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub status: Status,
    pub value: Vec<u8>,
}

impl Response {
    pub fn ok(value: Vec<u8>) -> Self {
        Self {
            status: Status::Ok,
            value,
        }
    }
    pub fn empty_ok() -> Self {
        Self::ok(Vec::new())
    }
    pub fn not_found() -> Self {
        Self {
            status: Status::NotFound,
            value: Vec::new(),
        }
    }
    pub fn invalid_argument() -> Self {
        Self {
            status: Status::InvalidArgument,
            value: b"invalid argument".to_vec(),
        }
    }
    pub fn internal_error(message: &[u8]) -> Self {
        Self {
            status: Status::InternalError,
            value: message.to_vec(),
        }
    }
}
