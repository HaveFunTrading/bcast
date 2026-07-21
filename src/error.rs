//! Error and result types returned by readers.
//!
//! Writers publish infallibly once input sizes satisfy their documented
//! preconditions. Reader operations can fail when the caller-provided buffer is
//! too small, when a reader has fallen behind the writer's retained window, or
//! when shared storage contains an invalid frame.

use std::fmt;

/// Crate result type.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors returned by reader operations.
#[derive(Debug, Eq, PartialEq)]
pub enum Error {
    /// Consumer is unable to keep up with the producer.
    Overrun(usize),
    /// The buffer used to read the message is too small.
    InsufficientBufferSize(usize, usize),
    /// The frame header read from storage describes bytes outside the ring.
    CorruptFrame {
        /// Absolute stream position of the corrupt frame header.
        stream_position: usize,
        /// Physical payload index within the ring data section.
        payload_index: usize,
        /// Payload length encoded in the frame header.
        payload_len: usize,
        /// Ring data-section capacity.
        capacity: usize,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Overrun(position) => {
                write!(f, "overran by the producer, reader position: {position}")
            }
            Error::InsufficientBufferSize(provided, required) => {
                write!(f, "provided buffer is of insufficient size, provided: {provided}, required: {required}")
            }
            Error::CorruptFrame {
                stream_position,
                payload_index,
                payload_len,
                capacity,
            } => {
                write!(
                    f,
                    "corrupt frame, stream position: {stream_position}, payload index: {payload_index}, payload length: {payload_len}, capacity: {capacity}",
                )
            }
        }
    }
}

impl std::error::Error for Error {}

impl Error {
    /// Construct an [`Error::InsufficientBufferSize`] value.
    #[inline]
    pub const fn insufficient_buffer_size(provided: usize, required: usize) -> Error {
        Error::InsufficientBufferSize(provided, required)
    }

    /// Construct an [`Error::Overrun`] value.
    #[inline]
    pub const fn overrun(position: usize) -> Error {
        Error::Overrun(position)
    }

    /// Construct an [`Error::CorruptFrame`] value.
    #[inline]
    pub const fn corrupt_frame(
        stream_position: usize,
        payload_index: usize,
        payload_len: usize,
        capacity: usize,
    ) -> Error {
        Error::CorruptFrame {
            stream_position,
            payload_index,
            payload_len,
            capacity,
        }
    }
}
