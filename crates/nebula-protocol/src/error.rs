use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid protocol magic")]
    InvalidMagic,

    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u8),

    #[error("unknown opcode: {0}")]
    UnknownOpcode(u8),

    #[error("frame exceeds configured limits")]
    FrameTooLarge,

    #[error("invalid frame: {0}")]
    InvalidFrame(&'static str),

    #[error("unknown response status: {0}")]
    UnknownStatus(u8),
}
