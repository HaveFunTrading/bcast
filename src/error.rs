//! Defines error types.
use thiserror::Error;

/// Crate result type (re-exported),
pub type Result<T> = std::result::Result<T, Error>;

/// Error types.
#[derive(Error, Debug, Eq, PartialEq)]
pub enum Error {
    /// Consumer is unable to keep up with the producer.
    #[error("overran by the producer, reader position: {0}")]
    Overrun(usize),
    /// The buffer used to read the message is too small.
    #[error("provided buffer is of insufficient size, provided: {0}, required: {1}")]
    InsufficientBufferSize(usize, usize),
}

impl Error {
    #[inline]
    pub const fn insufficient_buffer_size(provided: usize, required: usize) -> Error {
        Error::InsufficientBufferSize(provided, required)
    }

    #[inline]
    pub const fn overrun(position: usize) -> Error {
        Error::Overrun(position)
    }
}
