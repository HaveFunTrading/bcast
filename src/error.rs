//! Error and result types returned by readers.
//!
//! Writers publish infallibly once input sizes satisfy their documented
//! preconditions. Reader operations can fail when the caller-provided buffer is
//! too small or when a reader has fallen behind the writer's retained window.

use thiserror::Error;

/// Crate result type.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors returned by reader operations.
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
}
