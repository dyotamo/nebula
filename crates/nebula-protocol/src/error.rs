use thiserror::Error;

/// Transport failures or rejected wire frames, distinct from application [`crate::Status`] values.
///
/// These errors describe whether a frame could be read or written according to the wire
/// contract. A successfully decoded response may still carry a non-OK application status;
/// that status is data in [`crate::Response`], not a [`ProtocolError`].
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// Reading or writing the stream failed, including when `read_exact` encounters EOF
    /// before the complete header or payload has arrived.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The leading four bytes did not match [`crate::MAGIC`].
    #[error("invalid protocol magic")]
    InvalidMagic,

    /// The frame's version byte differs from the version this codec implements.
    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u8),

    /// The request opcode byte does not identify a supported operation.
    #[error("unknown opcode: {0}")]
    UnknownOpcode(u8),

    /// A payload exceeds the configured limit or cannot fit its wire-format length field.
    ///
    /// Readers check declared lengths before allocating payload buffers; writers reject
    /// oversized lengths before emitting the frame header.
    #[error("frame exceeds configured limits")]
    FrameTooLarge,

    /// A known operation or status was encoded with an invalid frame shape or control payload.
    /// The static detail is intended to identify the violated protocol rule, not carry payload data.
    #[error("invalid frame: {0}")]
    InvalidFrame(&'static str),

    /// The response status byte does not identify a supported application status.
    #[error("unknown response status: {0}")]
    UnknownStatus(u8),
}
