use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("overran by the producer at position {0}")]
    Overrun(usize),
    #[error("provided buffer is of insufficient size, provided: {0}, required: {1}")]
    InsufficientBufferSize(usize, usize),
}