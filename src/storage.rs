//! Storage backends for bcast rings.
//!
//! Readers and writers are generic over storage so the same channel logic can
//! run on owned in-process memory, memory mapped files, or user-provided
//! shared-memory adapters. Storage handles own or otherwise keep the underlying
//! bytes alive while the ring view is in use.

use crate::reader::Reader;
use crate::ring::Header;
use crate::writer::{Writer, WriterConfig};
use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::mem::{align_of, size_of};
use std::ptr::NonNull;
use std::sync::Arc;

/// Backing storage for a ring buffer.
///
/// # Safety
/// Implementors must guarantee that the memory returned by `ptr` remains valid for `len` bytes
/// while any `Reader` or `Writer` owns the storage handle. The memory must be aligned for the ring
/// header and must not be accessed outside the ring protocol while attached to bcast handles.
pub unsafe trait Storage {
    /// Return a non-null pointer to the first byte of the storage region.
    fn ptr(&self) -> NonNull<u8>;

    /// Return the storage region length in bytes.
    fn len(&self) -> usize;

    /// Return whether the storage region has zero bytes.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Backing storage that can be used by a writer.
///
/// # Safety
/// Implementors must guarantee that bytes behind `ptr` may be mutated through the ring protocol.
/// Correctness still requires at most one active writer for a channel.
pub unsafe trait WriteStorage: Storage {}

/// Owned in-process storage for a ring buffer.
pub struct LocalStorage {
    ptr: NonNull<u8>,
    len: usize,
    layout: Layout,
}

impl LocalStorage {
    /// Allocate zeroed storage of `size` bytes.
    pub fn new(size: usize) -> Self {
        assert!(size > 0, "storage size must be greater than zero");
        let layout = Layout::from_size_align(size, align_of::<Header>()).unwrap();
        let ptr = unsafe { alloc_zeroed(layout) };
        let ptr = NonNull::new(ptr).unwrap_or_else(|| std::alloc::handle_alloc_error(layout));
        Self { ptr, len: size, layout }
    }

    /// Allocate zeroed storage for a ring with `capacity` data bytes.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::new(size_of::<Header>() + capacity)
    }
}

impl Drop for LocalStorage {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.ptr.as_ptr(), self.layout);
        }
    }
}

unsafe impl Send for LocalStorage {}
unsafe impl Sync for LocalStorage {}

unsafe impl Storage for LocalStorage {
    #[inline]
    fn ptr(&self) -> NonNull<u8> {
        self.ptr
    }

    #[inline]
    fn len(&self) -> usize {
        self.len
    }
}

unsafe impl WriteStorage for LocalStorage {}

/// Reference-counted storage adapter for sharing one storage handle within a process.
pub struct SharedStorage<S> {
    inner: Arc<S>,
}

impl<S> Clone for SharedStorage<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<S> SharedStorage<S> {
    /// Wrap storage in a reference-counted handle.
    pub fn new(storage: S) -> Self {
        Self {
            inner: Arc::new(storage),
        }
    }
}

unsafe impl<S: Storage> Storage for SharedStorage<S> {
    #[inline]
    fn ptr(&self) -> NonNull<u8> {
        self.inner.ptr()
    }

    #[inline]
    fn len(&self) -> usize {
        self.inner.len()
    }
}

unsafe impl<S: WriteStorage> WriteStorage for SharedStorage<S> {}

/// Extension trait for fluent storage conversions.
pub trait StorageExt: Storage + Sized {
    /// Convert this storage value into a cloneable [`SharedStorage`] handle.
    fn into_shared(self) -> SharedStorage<Self> {
        SharedStorage::new(self)
    }

    /// Convert this storage value into a reader starting at the producer's
    /// current position.
    fn into_reader(self) -> Reader<Self> {
        Reader::new(self)
    }

    /// Convert this storage value into a reader starting at `position`.
    fn into_reader_at(self, position: usize) -> Reader<Self> {
        Reader::new(self).with_initial_position(position)
    }

    /// Convert this storage value into a reader starting at the most recent
    /// retained lap when possible.
    fn into_reader_at_last_lap(self) -> Reader<Self> {
        Reader::new_at_last_lap(self)
    }

    /// Convert this writable storage value into a new writer.
    ///
    /// This initializes the ring header and overwrites any existing channel
    /// state in the storage.
    fn into_writer(self) -> Writer<Self>
    where
        Self: WriteStorage,
    {
        Writer::new(self)
    }

    /// Convert this writable storage value into a new writer with custom
    /// configuration.
    fn into_writer_with_cfg<F>(self, config: F) -> Writer<Self>
    where
        Self: WriteStorage,
        F: FnOnce(WriterConfig) -> WriterConfig,
    {
        Writer::new_with_cfg(self, config)
    }

    /// Convert this writable storage value into a writer joined to an existing
    /// channel.
    fn join_writer(self) -> Writer<Self>
    where
        Self: WriteStorage,
    {
        Writer::join(self)
    }

    /// Convert this writable storage value into a writer joined to an existing
    /// channel with custom configuration.
    fn join_writer_with_cfg<F>(self, config: F) -> Writer<Self>
    where
        Self: WriteStorage,
        F: FnOnce(WriterConfig) -> WriterConfig,
    {
        Writer::join_with_cfg(self, config)
    }

    /// Convert this writable storage value into a writer joined at `position`.
    fn join_writer_at(self, position: usize) -> Writer<Self>
    where
        Self: WriteStorage,
    {
        Writer::join_at(self, position)
    }

    /// Convert this writable storage value into a writer joined at `position`
    /// with custom configuration.
    fn join_writer_at_with_cfg<F>(self, position: usize, config: F) -> Writer<Self>
    where
        Self: WriteStorage,
        F: FnOnce(WriterConfig) -> WriterConfig,
    {
        Writer::join_at_with_cfg(self, position, config)
    }
}

impl<S: Storage> StorageExt for S {}
