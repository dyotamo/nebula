//! Versioned binary request/response framing over Tokio byte streams.
//!
//! Each frame has a fixed, versioned header followed by the payload lengths declared in that
//! header. The codecs read or write one frame at a time; request and response lengths use the
//! configured [`FrameLimits`] plus the limits of their wire-format fields.
//!
//! Payloads are opaque bytes; frame codecs validate lengths and request shape,
//! not authentication or cache semantics. Application response statuses are values carried by
//! successfully decoded frames, while malformed or unsupported wire data is reported as
//! [`ProtocolError`]. Invalid frames are not resynchronized, so a rejected or truncated frame
//! should not be followed by another read as though the stream were still aligned.
//!
//! Frame reads and writes are not cancellation-safe: dropping a future can leave a frame
//! partially consumed or emitted, so restarting the operation may corrupt stream alignment.
//! For the same reason, callers should not assume a stream remains reusable after an I/O error
//! during frame processing unless the surrounding transport protocol provides its own recovery.

mod error;
mod frame;
mod request;
mod response;

pub use error::ProtocolError;
pub use frame::{
    read_request, read_request_with_limits, read_response, read_response_with_limits,
    write_request, write_request_with_limits, write_response, write_response_with_limits,
    FrameLimits, MAGIC, MAX_KEY_LEN, MAX_VALUE_LEN, VERSION,
};
pub use request::{Opcode, Request};
pub use response::{Response, Status};
