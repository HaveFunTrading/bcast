//! Low latency, single producer & many consumer (SPMC) ring buffer that works with shared memory.
//! Natively supports variable message sizes.
//!
//! ## Platform scope
//!
//! `bcast` is optimized for and supported on 64-bit x86 Linux with ordinary cache-coherent,
//! write-back memory. The single-writer publication and overrun protocol relies on aligned
//! 64-bit loads and stores, x86 cache coherence, and x86 memory ordering.
//!
//! A reader samples the writer's claimed overwrite frontier before and after accessing a
//! frame. If the writer laps the reader during that access, the result is discarded and
//! [`Error::Overrun`] is returned. Callers must either propagate that error or call
//! [`Reader::reset`] to discard missed data and resume at the producer's current committed
//! position.
//!
//! This is a deliberate x86-64 hardware-level protocol, not a portability guarantee for
//! overlapping non-atomic payload access under Rust's abstract memory model. Other operating
//! systems and architectures, especially weakly ordered architectures, are unsupported until
//! separately validated.
//!
//! ## Examples
//! Create `Writer` and use `claim` to publish a message.
//! ```no_run
//! use bcast::{LocalStorage, StorageExt};
//!
//! let storage = LocalStorage::with_capacity(1024);
//! let mut writer = storage.into_writer();
//!
//! // publish first message
//! let mut claim = writer.claim(5, true);
//! claim.get_buffer_mut().copy_from_slice(b"hello");
//! claim.commit();
//!
//! // publish second message
//! let mut claim = writer.claim(5, true);
//! claim.get_buffer_mut().copy_from_slice(b"world");
//! claim.commit();
//! ```
//! Create `Reader` and use `read_batch` to receive messages.
//! ```no_run
//! use bcast::{LocalStorage, StorageExt};
//!
//! let storage = LocalStorage::with_capacity(1024);
//! let mut reader = storage.into_reader();
//! let mut batch = reader.read_batch().unwrap();
//! let mut payload = [0u8; 1024];
//!
//! // read first message
//! let msg = batch.receive_next(&mut payload).unwrap().unwrap();
//! assert_eq!(b"hello", msg.payload);
//!
//! // read second message
//! let msg = batch.receive_next(&mut payload).unwrap().unwrap();
//! assert_eq!(b"world", msg.payload);
//!
//! // no more messages
//! assert!(batch.receive_next(&mut payload).is_none())
//! ```

pub mod error;

mod reader;
mod ring;
mod storage;
mod writer;

#[cfg(feature = "mmap")]
mod mmap;

pub use error::{Error, Result};
pub use reader::{Batch, Bulk, BulkIter, FilteredBatch, Message, Reader};
pub use storage::{LocalStorage, SharedStorage, Storage, StorageExt, WriteStorage};
pub use writer::{Claim, Writer, WriterConfig};

#[cfg(feature = "mmap")]
pub use mmap::{MappedReader, MappedWriter, MmapMutStorage, MmapStorage, OpenedMmap};

/// Ring buffer header size in bytes.
pub const HEADER_SIZE: usize = std::mem::size_of::<ring::Header>();
/// Metadata buffer size in bytes.
pub const METADATA_BUFFER_SIZE: usize = 1024;
/// Null value for `user_defined` field.
pub const USER_DEFINED_NULL_VALUE: u32 = 0;
