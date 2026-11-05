#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Status {
    Ok = 0,
    NotFound = 1,
    InvalidArgument = 2,
    InternalError = 3,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub status: Status,
    pub value: Vec<u8>,
}

impl Response {
    pub fn ok(value: Vec<u8>) -> Self { Self { status: Status::Ok, value } }
    pub fn empty_ok() -> Self { Self::ok(Vec::new()) }
    pub fn not_found() -> Self { Self { status: Status::NotFound, value: Vec::new() } }
    pub fn invalid_argument() -> Self {
        Self { status: Status::InvalidArgument, value: b"invalid argument".to_vec() }
    }
}
