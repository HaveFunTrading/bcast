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
    /// Attempting to publish a message that exceeds MTU.
    #[error("mtu limit exceeded, requested: {0}, mtu: {1}")]
    MtuLimitExceeded(usize, usize),
}

#[cold]
#[inline(never)]
pub(crate) const fn overrun(position: usize) -> Error {
    Error::Overrun(position)
}

#[cold]
#[inline(never)]
pub(crate) const fn insufficient_buffer_size(provided: usize, required: usize) -> Error {
    Error::InsufficientBufferSize(provided, required)
}

#[cold]
#[inline(never)]
pub(crate) const fn mtu_limit_exceeded(requested: usize, mtu: usize) -> Error {
    Error::MtuLimitExceeded(requested, mtu)
}
