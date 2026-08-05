//! Writer-side channel API.
//!
//! This module owns producer state for a storage-backed bcast ring. It supports
//! zero-copy claims, closure-based publishing into a claimed ring slice, and
//! copy-based sends from caller-owned payload buffers.

use crate::USER_DEFINED_NULL_VALUE;
use crate::ring::{
    FrameHeader, RingBuffer, claim_reserve_bytes, get_aligned_size, is_position_after, is_position_at_or_after,
    pack_fields,
};
use crate::storage::WriteStorage;
use std::mem::{ManuallyDrop, size_of};
use std::sync::atomic::Ordering;

const MAX_CLAIM_RESERVE_RATIO: f64 = 0.5;
const DEFAULT_CLAIM_RESERVE_RATIO: f64 = 0.01;

/// Configuration used to initialize a writer-backed channel.
#[derive(Debug, Clone, Copy)]
pub struct WriterConfig {
    metadata: fn(&mut [u8]),
    claim_reserve_ratio: f64,
}

impl Default for WriterConfig {
    fn default() -> Self {
        const fn noop_metadata(_: &mut [u8]) {}
        Self {
            metadata: noop_metadata,
            claim_reserve_ratio: DEFAULT_CLAIM_RESERVE_RATIO,
        }
    }
}

impl WriterConfig {
    /// Set the function used to populate the channel metadata buffer during
    /// writer initialization.
    ///
    /// The callback receives the full fixed-size metadata buffer and may write
    /// any application-specific bytes into it before readers observe the channel
    /// as ready.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let _writer = storage.clone().into_writer_with_cfg(|config| {
    ///     config.metadata(|metadata| metadata[..4].copy_from_slice(b"meta"))
    /// });
    ///
    /// let reader = storage.into_reader();
    /// assert_eq!(b"meta", &reader.metadata()[..4]);
    /// ```
    pub fn metadata(mut self, metadata: fn(&mut [u8])) -> Self {
        self.metadata = metadata;
        self
    }

    /// Set how far ahead the writer may reserve the claimed-position cursor, as
    /// a ratio of ring capacity.
    ///
    /// For example, `0.01` reserves 1% and `0.5` reserves 50%. The computed byte
    /// reservation is rounded up to the next power of two and every non-zero
    /// reservation is at least the 8-byte frame alignment.
    ///
    /// A non-zero value reduces the reader's effective retained window by up to
    /// the reserved amount, but lets the writer avoid updating the shared
    /// claimed-position cursor on every claim. The default is `0.01` (1%). Set
    /// the ratio to `0.0` to disable reservation and retain the full ring
    /// capacity.
    ///
    /// # Panics
    ///
    /// Panics if `ratio` is outside `0.0..=0.5`.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024);
    /// let mut writer = storage.into_writer_with_cfg(|config| {
    ///     config.claim_reserve_ratio(0.25)
    /// });
    ///
    /// writer.send(b"hello", true);
    /// ```
    pub fn claim_reserve_ratio(mut self, ratio: f64) -> Self {
        assert!((0.0..=MAX_CLAIM_RESERVE_RATIO).contains(&ratio), "claim reserve ratio must be in 0.0..=0.5");
        self.claim_reserve_ratio = ratio;
        self
    }
}

/// Publishes messages into a single-producer ring.
///
/// A channel must have exactly one active writer. Multiple readers can observe
/// the same storage, but concurrent writers over the same storage are not
/// supported.
///
/// Writing requires `&mut self`. This lets Rust enforce that a writer can have
/// at most one open [`Claim`] at a time, which prevents accidental overlapping
/// writes.
///
/// # Example
///
/// ```
/// use bcast::{LocalStorage, StorageExt};
///
/// let storage = LocalStorage::with_capacity(1024).into_shared();
/// let mut writer = storage.clone().into_writer();
/// let mut reader = storage.into_reader();
///
/// writer.send(b"hello", true);
///
/// let mut payload = [0u8; 16];
/// let msg = reader.receive_next(&mut payload).unwrap().unwrap();
/// assert_eq!(b"hello", msg.payload);
/// ```
#[derive(Debug)]
pub struct Writer<S> {
    _storage: S,
    ring: RingBuffer,
    claim_reserve: usize,
    claimed_limit: usize,
    position: usize, // local producer position
    lap_count: usize,
}

impl<S: WriteStorage> Writer<S> {
    /// Create a new writer and initialize the ring header at position zero.
    ///
    /// Existing ring contents in `storage` are overwritten. Use [`Writer::join`]
    /// to continue writing to an already initialized channel.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt, Writer};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let mut writer = Writer::new(storage.clone());
    /// let mut reader = storage.into_reader();
    ///
    /// writer.send(b"hello", true);
    ///
    /// let mut payload = [0u8; 16];
    /// assert_eq!(b"hello", reader.receive_next(&mut payload).unwrap().unwrap().payload);
    /// ```
    pub fn new(storage: S) -> Self {
        Self::new_with_cfg(storage, |config| config)
    }

    /// Create a new writer with custom configuration and initialize the ring
    /// header at position zero.
    ///
    /// Existing ring contents in `storage` are overwritten.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt, Writer};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let _writer = Writer::new_with_cfg(storage.clone(), |config| {
    ///     config.metadata(|metadata| metadata[..4].copy_from_slice(b"meta"))
    /// });
    ///
    /// let reader = storage.into_reader();
    /// assert_eq!(b"meta", &reader.metadata()[..4]);
    /// ```
    pub fn new_with_cfg<F: FnOnce(WriterConfig) -> WriterConfig>(storage: S, config: F) -> Self {
        let config = config(WriterConfig::default());
        let mut ring = RingBuffer::from_storage(&storage);
        ring.init_header(0, config.metadata);
        Self::from_position(storage, ring, 0, config)
    }

    /// Join an initialized ring and continue writing from the current producer
    /// position.
    ///
    /// This waits until the ring header has been initialized by the original
    /// writer.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt, Writer};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// {
    ///     let mut writer = storage.clone().into_writer();
    ///     writer.send(b"one", true);
    /// }
    ///
    /// let mut writer = Writer::join(storage.clone());
    /// let mut reader = storage.into_reader_at(0);
    /// writer.send(b"two", true);
    ///
    /// let mut payload = [0u8; 16];
    /// assert_eq!(b"one", reader.receive_next(&mut payload).unwrap().unwrap().payload);
    /// assert_eq!(b"two", reader.receive_next(&mut payload).unwrap().unwrap().payload);
    /// ```
    pub fn join(storage: S) -> Self {
        Self::join_with_cfg(storage, |config| config)
    }

    /// Join an initialized ring with custom writer configuration and continue
    /// writing from the current producer position.
    ///
    /// This waits until the ring header has been initialized by the original
    /// writer.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt, Writer};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// {
    ///     let mut writer = storage.clone().into_writer();
    ///     writer.send(b"one", true);
    /// }
    ///
    /// let mut writer = Writer::join_with_cfg(storage.clone(), |config| {
    ///     config.claim_reserve_ratio(0.25)
    /// });
    /// let mut reader = storage.into_reader_at(0);
    /// writer.send(b"two", true);
    ///
    /// let mut payload = [0u8; 16];
    /// assert_eq!(b"one", reader.receive_next(&mut payload).unwrap().unwrap().payload);
    /// assert_eq!(b"two", reader.receive_next(&mut payload).unwrap().unwrap().payload);
    /// ```
    pub fn join_with_cfg<F: FnOnce(WriterConfig) -> WriterConfig>(storage: S, config: F) -> Self {
        let ring = RingBuffer::from_storage(&storage);
        ring.wait_until_ready();
        let config = config(WriterConfig::default());
        let position = ring.header().producer_position.load(Ordering::SeqCst);
        Self::from_position(storage, ring, position, config)
    }

    /// Join an initialized ring and continue writing from `position`.
    ///
    /// Use this when the caller has externally persisted a writer position and
    /// wants to resume from that exact point.
    ///
    /// # Panics
    ///
    /// Panics if `position` is not aligned to the frame alignment.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt, Writer};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// {
    ///     let _writer = storage.clone().into_writer();
    /// }
    ///
    /// let mut writer = Writer::join_at(storage.clone(), 0);
    /// let mut reader = storage.into_reader_at(0);
    /// writer.send(b"hello", true);
    ///
    /// let mut payload = [0u8; 16];
    /// assert_eq!(b"hello", reader.receive_next(&mut payload).unwrap().unwrap().payload);
    /// ```
    pub fn join_at(storage: S, position: usize) -> Self {
        Self::join_at_with_cfg(storage, position, |config| config)
    }

    /// Join an initialized ring with custom writer configuration and continue
    /// writing from `position`.
    ///
    /// # Panics
    ///
    /// Panics if `position` is not aligned to the frame alignment.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt, Writer};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// {
    ///     let _writer = storage.clone().into_writer();
    /// }
    ///
    /// let mut writer = Writer::join_at_with_cfg(storage.clone(), 0, |config| {
    ///     config.claim_reserve_ratio(0.25)
    /// });
    /// let mut reader = storage.into_reader_at(0);
    /// writer.send(b"hello", true);
    ///
    /// let mut payload = [0u8; 16];
    /// assert_eq!(b"hello", reader.receive_next(&mut payload).unwrap().unwrap().payload);
    /// ```
    pub fn join_at_with_cfg<F: FnOnce(WriterConfig) -> WriterConfig>(storage: S, position: usize, config: F) -> Self {
        let ring = RingBuffer::from_storage(&storage);
        ring.wait_until_ready();
        let config = config(WriterConfig::default());
        assert_eq!(get_aligned_size(position), position, "position must be aligned");
        Self::from_position(storage, ring, position, config)
    }

    fn from_position(storage: S, ring: RingBuffer, position: usize, config: WriterConfig) -> Self {
        let lap_count = ring.header().lap_count.load(Ordering::Relaxed);
        let header_claimed_position = ring.header().claimed_position.load(Ordering::Acquire);
        let claimed_limit = if is_position_at_or_after(header_claimed_position, position) {
            header_claimed_position
        } else {
            position
        };
        Self {
            _storage: storage,
            claim_reserve: claim_reserve_bytes(ring.capacity, config.claim_reserve_ratio),
            claimed_limit,
            ring,
            position,
            lap_count,
        }
    }
}

impl<S> Writer<S> {
    /// Claim a payload buffer for zero-copy publication.
    ///
    /// The returned [`Claim`] borrows the writer mutably until it is committed,
    /// aborted, or dropped. The `fin` flag marks this frame as the final
    /// fragment of a logical message.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let mut writer = storage.clone().into_writer();
    /// let mut reader = storage.into_reader();
    ///
    /// let mut claim = writer.claim(5, true);
    /// claim.get_buffer_mut().copy_from_slice(b"hello");
    /// claim.commit();
    ///
    /// let mut payload = [0u8; 16];
    /// assert_eq!(b"hello", reader.receive_next(&mut payload).unwrap().unwrap().payload);
    /// ```
    ///
    /// ## Panics
    ///
    /// Panics when the aligned payload length is greater than [`Writer::mtu`].
    #[inline]
    pub fn claim(&mut self, len: usize, fin: bool) -> Claim<'_, S> {
        self.claim_with_user_defined(len, fin, USER_DEFINED_NULL_VALUE)
    }

    /// Claim a payload buffer for zero-copy publication with a user-defined
    /// frame value.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let mut writer = storage.clone().into_writer();
    /// let mut reader = storage.into_reader();
    ///
    /// let mut claim = writer.claim_with_user_defined(5, true, 123);
    /// claim.get_buffer_mut().copy_from_slice(b"hello");
    /// claim.commit();
    ///
    /// let mut payload = [0u8; 16];
    /// let msg = reader.receive_next(&mut payload).unwrap().unwrap();
    /// assert_eq!(b"hello", msg.payload);
    /// assert_eq!(123, msg.user_defined);
    /// ```
    ///
    /// ## Panics
    ///
    /// Panics when the aligned payload length is greater than [`Writer::mtu`].
    #[inline]
    pub fn claim_with_user_defined(&mut self, len: usize, fin: bool, user_defined: u32) -> Claim<'_, S> {
        let aligned_len = get_aligned_size(len);
        assert!(aligned_len <= self.mtu(), "mtu exceeded");
        Claim::new(self, aligned_len, len, user_defined, fin, false, false)
    }

    /// Claim, write, and commit one frame.
    ///
    /// The `write` closure receives the claimed payload buffer and must fill it
    /// before returning.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let mut writer = storage.clone().into_writer();
    /// let mut reader = storage.into_reader();
    ///
    /// writer.publish(5, true, |payload| payload.copy_from_slice(b"hello"));
    ///
    /// let mut payload = [0u8; 16];
    /// assert_eq!(b"hello", reader.receive_next(&mut payload).unwrap().unwrap().payload);
    /// ```
    ///
    /// ## Panics
    ///
    /// Panics when the aligned payload length is greater than [`Writer::mtu`].
    #[inline]
    pub fn publish<F>(&mut self, len: usize, fin: bool, write: F)
    where
        F: FnOnce(&mut [u8]),
    {
        self.publish_with_user_defined(len, fin, USER_DEFINED_NULL_VALUE, write);
    }

    /// Claim, write, and commit one frame with a user-defined frame value.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let mut writer = storage.clone().into_writer();
    /// let mut reader = storage.into_reader();
    ///
    /// writer.publish_with_user_defined(5, true, 123, |payload| {
    ///     payload.copy_from_slice(b"hello");
    /// });
    ///
    /// let mut payload = [0u8; 16];
    /// let msg = reader.receive_next(&mut payload).unwrap().unwrap();
    /// assert_eq!(b"hello", msg.payload);
    /// assert_eq!(123, msg.user_defined);
    /// ```
    ///
    /// ## Panics
    ///
    /// Panics when the aligned payload length is greater than [`Writer::mtu`].
    #[inline]
    pub fn publish_with_user_defined<F>(&mut self, len: usize, fin: bool, user_defined: u32, write: F)
    where
        F: FnOnce(&mut [u8]),
    {
        let mut claim = self.claim_with_user_defined(len, fin, user_defined);
        write(claim.get_buffer_mut());
        claim.commit();
    }

    /// Copy one payload into the ring and commit it.
    ///
    /// This is the simplest write API when the payload already exists in caller
    /// memory. Use [`Writer::claim`] or [`Writer::publish`] to avoid preparing a
    /// separate payload buffer.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let mut writer = storage.clone().into_writer();
    /// let mut reader = storage.into_reader();
    ///
    /// writer.send(b"hello", true);
    ///
    /// let mut payload = [0u8; 16];
    /// assert_eq!(b"hello", reader.receive_next(&mut payload).unwrap().unwrap().payload);
    /// ```
    ///
    /// ## Panics
    ///
    /// Panics when the aligned payload length is greater than [`Writer::mtu`].
    #[inline]
    pub fn send(&mut self, payload: &[u8], fin: bool) {
        self.send_with_user_defined(payload, fin, USER_DEFINED_NULL_VALUE);
    }

    /// Copy one payload into the ring and commit it with a user-defined frame
    /// value.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let mut writer = storage.clone().into_writer();
    /// let mut reader = storage.into_reader();
    ///
    /// writer.send_with_user_defined(b"hello", true, 123);
    ///
    /// let mut payload = [0u8; 16];
    /// let msg = reader.receive_next(&mut payload).unwrap().unwrap();
    /// assert_eq!(b"hello", msg.payload);
    /// assert_eq!(123, msg.user_defined);
    /// ```
    ///
    /// ## Panics
    ///
    /// Panics when the aligned payload length is greater than [`Writer::mtu`].
    #[inline]
    pub fn send_with_user_defined(&mut self, payload: &[u8], fin: bool, user_defined: u32) {
        self.publish_with_user_defined(payload.len(), fin, user_defined, |buffer| {
            buffer.copy_from_slice(payload);
        });
    }

    /// Claim a continuation frame.
    ///
    /// Use continuation frames to fragment a logical message across multiple
    /// frames. Set `fin` to true on the final fragment.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let mut writer = storage.clone().into_writer();
    /// let mut reader = storage.into_reader();
    ///
    /// writer.claim(5, false).commit();
    /// let mut continuation = writer.continuation(5, true);
    /// continuation.get_buffer_mut().copy_from_slice(b"world");
    /// continuation.commit();
    ///
    /// let mut payload = [0u8; 16];
    /// let first = reader.receive_next(&mut payload).unwrap().unwrap();
    /// assert!(!first.is_continuation);
    /// assert!(!first.is_fin);
    ///
    /// let second = reader.receive_next(&mut payload).unwrap().unwrap();
    /// assert_eq!(b"world", second.payload);
    /// assert!(second.is_continuation);
    /// assert!(second.is_fin);
    /// ```
    ///
    /// ## Panics
    ///
    /// Panics when the aligned payload length is greater than [`Writer::mtu`].
    #[inline]
    pub fn continuation(&mut self, len: usize, fin: bool) -> Claim<'_, S> {
        let aligned_len = get_aligned_size(len);
        assert!(aligned_len <= self.mtu(), "mtu exceeded");
        Claim::new(self, aligned_len, len, USER_DEFINED_NULL_VALUE, fin, true, false)
    }

    /// Claim a heartbeat frame with no payload and no user-defined value.
    ///
    /// Heartbeat frames are visible to readers as messages with
    /// [`crate::Message::is_heartbeat`] set.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let mut writer = storage.clone().into_writer();
    /// let mut reader = storage.into_reader();
    ///
    /// writer.heartbeat().commit();
    ///
    /// let mut payload = [0u8; 16];
    /// let msg = reader.receive_next(&mut payload).unwrap().unwrap();
    /// assert!(msg.payload.is_empty());
    /// assert!(msg.is_heartbeat);
    /// ```
    #[inline]
    pub fn heartbeat(&mut self) -> Claim<'_, S> {
        Claim::new(self, 0, 0, USER_DEFINED_NULL_VALUE, true, false, true)
    }

    /// Claim a heartbeat frame with a user-defined value and no payload.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let mut writer = storage.clone().into_writer();
    /// let mut reader = storage.into_reader();
    ///
    /// writer.heartbeat_with_user_defined(123).commit();
    ///
    /// let mut payload = [0u8; 16];
    /// let msg = reader.receive_next(&mut payload).unwrap().unwrap();
    /// assert!(msg.is_heartbeat);
    /// assert_eq!(123, msg.user_defined);
    /// ```
    #[inline]
    pub fn heartbeat_with_user_defined(&mut self, user_defined: u32) -> Claim<'_, S> {
        Claim::new(self, 0, 0, user_defined, true, false, true)
    }

    /// Claim a heartbeat frame with payload and no user-defined value.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let mut writer = storage.clone().into_writer();
    /// let mut reader = storage.into_reader();
    ///
    /// let mut heartbeat = writer.heartbeat_with_payload(5);
    /// heartbeat.get_buffer_mut().copy_from_slice(b"hello");
    /// heartbeat.commit();
    ///
    /// let mut payload = [0u8; 16];
    /// let msg = reader.receive_next(&mut payload).unwrap().unwrap();
    /// assert_eq!(b"hello", msg.payload);
    /// assert!(msg.is_heartbeat);
    /// ```
    ///
    /// ## Panics
    ///
    /// Panics when the aligned payload length is greater than [`Writer::mtu`].
    #[inline]
    pub fn heartbeat_with_payload(&mut self, len: usize) -> Claim<'_, S> {
        let aligned_len = get_aligned_size(len);
        assert!(aligned_len <= self.mtu(), "mtu exceeded");
        Claim::new(self, aligned_len, len, USER_DEFINED_NULL_VALUE, true, false, true)
    }

    /// Claim a heartbeat frame with payload and a user-defined value.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let mut writer = storage.clone().into_writer();
    /// let mut reader = storage.into_reader();
    ///
    /// let mut heartbeat = writer.heartbeat_with_payload_and_user_defined(5, 123);
    /// heartbeat.get_buffer_mut().copy_from_slice(b"hello");
    /// heartbeat.commit();
    ///
    /// let mut payload = [0u8; 16];
    /// let msg = reader.receive_next(&mut payload).unwrap().unwrap();
    /// assert_eq!(b"hello", msg.payload);
    /// assert!(msg.is_heartbeat);
    /// assert_eq!(123, msg.user_defined);
    /// ```
    ///
    /// ## Panics
    ///
    /// Panics when the aligned payload length is greater than [`Writer::mtu`].
    #[inline]
    pub fn heartbeat_with_payload_and_user_defined(&mut self, len: usize, user_defined: u32) -> Claim<'_, S> {
        let aligned_len = get_aligned_size(len);
        assert!(aligned_len <= self.mtu(), "mtu exceeded");
        Claim::new(self, aligned_len, len, user_defined, true, false, true)
    }

    /// Return the maximum unaligned payload length accepted by one frame.
    ///
    /// It is calculated as `min(capacity / 2 - size_of::<FrameHeader>(), MAX_PAYLOAD_LEN)`.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt};
    ///
    /// let writer = LocalStorage::with_capacity(1024).into_writer();
    ///
    /// assert!(writer.mtu() >= 5);
    /// ```
    #[inline]
    pub const fn mtu(&self) -> usize {
        self.ring.mtu
    }

    /// Buffer index at which next write will happen.
    #[inline]
    const fn index(&self) -> usize {
        self.position & (self.ring.capacity - 1)
    }

    /// Number of bytes remaining in the buffer before it will wrap around.
    #[inline]
    const fn remaining(&self) -> usize {
        self.ring.capacity - self.index()
    }

    /// Get reference to the next (unpublished) message frame header;
    #[inline]
    const fn frame_header(&self) -> &FrameHeader {
        unsafe {
            let ptr = self.ring.header().data_ptr();
            &*(ptr.add(self.index()) as *const FrameHeader)
        }
    }

    /// Get mutable reference to the next (unpublished) message frame header;
    #[inline]
    const fn frame_header_mut(&mut self) -> &mut FrameHeader {
        unsafe {
            let ptr = self.ring.header().data_ptr();
            &mut *(ptr.add(self.index()) as *mut FrameHeader)
        }
    }

    #[inline]
    const fn write_padding_frame(&mut self, padding_len: usize) {
        let fields = pack_fields(true, false, true, false, padding_len as u32);
        let header = self.frame_header_mut();
        header.set(fields, USER_DEFINED_NULL_VALUE);
    }

    #[inline]
    fn reserve_claimed_position(&mut self, claim_end: usize) {
        if !is_position_after(claim_end, self.claimed_limit) {
            return;
        }

        let claimed_limit = claim_end.wrapping_add(self.claim_reserve);
        self.claimed_limit = claimed_limit;
        self.ring
            .header()
            .claimed_position
            .store(claimed_limit, Ordering::Release);
    }

    #[inline]
    fn update_lap_count(&mut self, frame_start: usize) {
        if frame_start & (self.ring.capacity - 1) != 0 {
            return;
        }

        let lap_count = frame_start / self.ring.capacity;
        if lap_count != self.lap_count {
            self.lap_count = lap_count;
            self.ring.header().lap_count.store(lap_count, Ordering::Relaxed);
        }
    }
}

/// Claimed region of the ring that can be published as one frame.
///
/// A claim is committed automatically when dropped. Call [`Claim::abort`] to
/// publish the reserved region as padding instead, causing readers to skip it.
///
/// Because a claim holds `&mut Writer`, the type system prevents multiple open
/// claims from the same writer.
///
/// # Example
///
/// ```
/// use bcast::{LocalStorage, StorageExt};
///
/// let storage = LocalStorage::with_capacity(1024).into_shared();
/// let mut writer = storage.clone().into_writer();
/// let mut reader = storage.into_reader();
///
/// let mut claim = writer.claim(5, true);
/// claim.get_buffer_mut().copy_from_slice(b"hello");
/// claim.commit();
///
/// let mut payload = [0u8; 16];
/// assert_eq!(b"hello", reader.receive_next(&mut payload).unwrap().unwrap().payload);
/// ```
#[derive(Debug)]
pub struct Claim<'a, S> {
    writer: &'a mut Writer<S>, // underlying writer
    len: usize,                // frame header aligned payload length
    limit: usize,              // actual payload length
    user_defined: u32,         // user defined field
    fin: bool,                 // final message fragment
    continuation: bool,        // continuation frame
    heartbeat: bool,           // heartbeat frame
}

impl<'a, S> Claim<'a, S> {
    /// Create new claim.
    #[inline]
    fn new(
        writer: &'a mut Writer<S>,
        len: usize,
        limit: usize,
        user_defined: u32,
        fin: bool,
        continuation: bool,
        heartbeat: bool,
    ) -> Self {
        #[cold]
        fn insert_padding_frame<S>(writer: &mut Writer<S>, remaining: usize) {
            let padding_len = remaining - size_of::<FrameHeader>();
            writer.write_padding_frame(padding_len);
            writer.position = writer.position.wrapping_add(padding_len + size_of::<FrameHeader>());
        }

        let position = writer.position;
        let frame_len = len + size_of::<FrameHeader>();

        // insert padding frame if required
        let remaining = writer.remaining();
        let claim_end = if frame_len > remaining {
            position.wrapping_add(remaining).wrapping_add(frame_len)
        } else {
            position.wrapping_add(frame_len)
        };

        // Publish the overwrite frontier before touching padding bytes or exposing payload memory.
        writer.reserve_claimed_position(claim_end);

        if frame_len > remaining {
            insert_padding_frame(writer, remaining);
        };

        Self {
            writer,
            len,
            limit,
            user_defined,
            fin,
            continuation,
            heartbeat,
        }
    }

    /// Return the claimed payload buffer.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt};
    ///
    /// let mut writer = LocalStorage::with_capacity(1024).into_writer();
    ///
    /// let mut claim = writer.claim(5, true);
    /// claim.get_buffer_mut().copy_from_slice(b"hello");
    /// assert_eq!(b"hello", claim.get_buffer());
    /// ```
    #[inline]
    pub const fn get_buffer(&self) -> &[u8] {
        let ptr = self.writer.frame_header().get_payload_ptr();
        unsafe { std::slice::from_raw_parts(ptr as *const u8, self.limit) }
    }

    /// Return the claimed payload buffer mutably.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let mut writer = storage.clone().into_writer();
    /// let mut reader = storage.into_reader();
    ///
    /// let mut claim = writer.claim(5, true);
    /// claim.get_buffer_mut().copy_from_slice(b"hello");
    /// claim.commit();
    ///
    /// let mut payload = [0u8; 16];
    /// assert_eq!(b"hello", reader.receive_next(&mut payload).unwrap().unwrap().payload);
    /// ```
    #[inline]
    pub const fn get_buffer_mut(&mut self) -> &mut [u8] {
        let ptr = self.writer.frame_header_mut().get_payload_ptr_mut();
        unsafe { std::slice::from_raw_parts_mut(ptr as *mut u8, self.limit) }
    }

    /// Abort the claim by publishing the reserved frame as padding.
    ///
    /// This consumes the claimed stream position; readers skip the resulting
    /// padding frame.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let mut writer = storage.clone().into_writer();
    /// let mut reader = storage.into_reader();
    ///
    /// writer.claim(5, true).abort();
    ///
    /// let mut payload = [0u8; 16];
    /// assert!(reader.receive_next(&mut payload).is_none());
    /// ```
    #[inline]
    pub fn abort(self) {
        let mut claim = ManuallyDrop::new(self);
        claim.abort_impl();
    }

    #[inline]
    fn abort_impl(&mut self) {
        let frame_start = self.writer.position;
        self.writer.write_padding_frame(self.len);

        self.writer.position = self.writer.position.wrapping_add(self.len + size_of::<FrameHeader>());

        self.writer.update_lap_count(frame_start);

        self.writer
            .ring
            .header()
            .producer_position
            .store(self.writer.position, Ordering::Release);
    }

    /// Commit the frame and make it visible to readers.
    ///
    /// If this method is not called, the claim is committed automatically when
    /// dropped.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let mut writer = storage.clone().into_writer();
    /// let mut reader = storage.into_reader();
    ///
    /// writer.claim(0, true).commit();
    ///
    /// let mut payload = [0u8; 16];
    /// let msg = reader.receive_next(&mut payload).unwrap().unwrap();
    /// assert!(msg.payload.is_empty());
    /// ```
    #[inline]
    pub fn commit(self) {
        // we need to ensure the destructor will not be called in this case
        let mut claim = ManuallyDrop::new(self);
        claim.commit_impl();
    }

    #[inline]
    fn commit_impl(&mut self) {
        let frame_start = self.writer.position;

        // update frame header
        let header = self.writer.frame_header_mut();
        let fields = pack_fields(self.fin, self.continuation, false, self.heartbeat, self.limit as u32);
        header.set(fields, self.user_defined);

        // advance writer position
        self.writer.position = self.writer.position.wrapping_add(self.len + size_of::<FrameHeader>());

        // update last lap count if required
        self.writer.update_lap_count(frame_start);

        // signal updated producer position
        self.writer
            .ring
            .header()
            .producer_position
            .store(self.writer.position, Ordering::Release);
    }
}

impl<S> Drop for Claim<'_, S> {
    fn drop(&mut self) {
        self.commit_impl();
    }
}
