//! Reader-side channel API.
//!
//! This module implements independent read cursors over a storage-backed bcast
//! ring. It provides single-message reads, bounded batch reads, and raw bulk
//! copies for consumers that want to parse copied frame bytes themselves.

use crate::error::{Error, Result};
use crate::ring::{
    FrameHeader, RingBuffer, get_aligned_size, is_overrun, is_position_after, is_position_at_or_after, unpack_fields,
    unpack_header,
};
use crate::storage::Storage;
use std::cmp::min;
use std::mem::size_of;
use std::ptr::{copy_nonoverlapping, read_unaligned};
use std::sync::atomic::Ordering;

/// Receives messages from a single-producer ring.
///
/// Multiple readers may observe the same channel at the same time. Readers are
/// independent cursors: they do not participate in congestion control and do
/// not slow the writer down. A reader that falls behind the writer's retained
/// window receives [`Error::Overrun`] and can recover with [`Reader::reset`].
///
/// Most read methods return `Option<Result<_>>`:
///
/// - `None` means there is currently no committed message to read.
/// - `Some(Ok(_))` means a message, batch, or bulk window was read.
/// - `Some(Err(_))` means the reader cursor hit an error, usually overrun.
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
/// assert!(reader.receive_next(&mut payload).is_none());
/// ```
pub struct Reader<S> {
    _storage: S,
    ring: RingBuffer,
    position: usize,          // next stream position this reader will consume
    producer_position: usize, // cached committed/readable limit observed from producer
    claimed_position: usize,  // cached overwrite frontier observed from producer
}

impl<S: Storage> Reader<S> {
    /// Create a reader over initialized storage.
    ///
    /// The reader starts at the producer's current committed position, so it
    /// observes messages published after it attaches. To start from a known
    /// earlier position, call [`Reader::with_initial_position`] on the returned
    /// reader.
    ///
    /// This waits until the ring header has been initialized by a writer.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, Reader, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let mut writer = storage.clone().into_writer();
    ///
    /// writer.send(b"before-reader", true);
    ///
    /// let mut reader = Reader::new(storage);
    /// let mut payload = [0u8; 32];
    /// assert!(reader.receive_next(&mut payload).is_none());
    ///
    /// writer.send(b"after-reader", true);
    /// assert_eq!(b"after-reader", reader.receive_next(&mut payload).unwrap().unwrap().payload);
    /// ```
    pub fn new(storage: S) -> Self {
        let ring = RingBuffer::from_storage(&storage);
        Self::from_ring_at_producer_position(storage, ring)
    }

    /// Create a reader at the start of the most recent physical ring lap when
    /// that position is still retained.
    ///
    /// If the lap start has already been overwritten, the reader starts at the
    /// producer's current committed position instead. This is useful for late
    /// readers that want as much recent data as can still be safely read.
    ///
    /// This waits until the ring header has been initialized by a writer.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, Reader, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let mut writer = storage.clone().into_writer();
    /// writer.send(b"retained", true);
    ///
    /// let mut reader = Reader::new_at_last_lap(storage);
    /// let mut payload = [0u8; 16];
    /// assert_eq!(b"retained", reader.receive_next(&mut payload).unwrap().unwrap().payload);
    /// ```
    pub fn new_at_last_lap(storage: S) -> Self {
        let ring = RingBuffer::from_storage(&storage);
        ring.wait_until_ready();
        let producer_position = ring.header().producer_position.load(Ordering::Acquire);
        let claimed_position = ring.header().claimed_position.load(Ordering::Acquire);
        let lap_count = ring.header().lap_count.load(Ordering::Relaxed);
        let lap_position = lap_count.wrapping_mul(ring.capacity);
        let position = if producer_position.wrapping_sub(lap_position) <= ring.capacity {
            lap_position
        } else {
            producer_position
        };
        Self {
            _storage: storage,
            ring,
            position,
            producer_position,
            claimed_position,
        }
    }

    fn from_ring_at_producer_position(storage: S, ring: RingBuffer) -> Self {
        ring.wait_until_ready();
        let producer_position = ring.header().producer_position.load(Ordering::SeqCst);
        let claimed_position = ring.header().claimed_position.load(Ordering::SeqCst);
        Self {
            _storage: storage,
            ring,
            position: producer_position,
            producer_position,
            claimed_position,
        }
    }
}

impl<S> Reader<S> {
    /// Return the channel metadata buffer written during writer initialization.
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
    pub fn metadata(&self) -> &[u8] {
        self.ring.header().metadata()
    }

    /// Set the reader's initial stream position.
    ///
    /// The position is an absolute stream position, not a physical ring index.
    /// It must point at a frame boundary. If the writer has already overwritten
    /// that position, the next read returns [`Error::Overrun`].
    ///
    /// # Panics
    ///
    /// Panics if `position` is not aligned to the frame alignment.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, Reader, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let mut writer = storage.clone().into_writer();
    /// writer.send(b"hello", true);
    ///
    /// let mut reader = Reader::new(storage).with_initial_position(0);
    /// let mut payload = [0u8; 16];
    /// assert_eq!(b"hello", reader.receive_next(&mut payload).unwrap().unwrap().payload);
    /// ```
    pub fn with_initial_position(self, position: usize) -> Self {
        assert_eq!(get_aligned_size(position), position, "position must be aligned");
        let cached_producer_position = self.producer_position;
        let producer_position = if is_position_at_or_after(cached_producer_position, position) {
            cached_producer_position
        } else {
            position
        };
        let cached_claimed_position = self.claimed_position;
        let claimed_position = if is_position_at_or_after(cached_claimed_position, position) {
            cached_claimed_position
        } else {
            position
        };
        Self {
            _storage: self._storage,
            ring: self.ring,
            position,
            producer_position,
            claimed_position,
        }
    }

    /// Obtain reference to the (unpublished) message frame header.
    #[inline]
    const fn as_frame_header(&self) -> &FrameHeader {
        unsafe { &*(self.ring.header().data_ptr().add(self.index()) as *const FrameHeader) }
    }

    /// Buffer index at which read will happen.
    #[inline]
    const fn index(&self) -> usize {
        self.position & (self.ring.capacity - 1)
    }

    /// Reset this reader to the producer's current committed position.
    ///
    /// This is the intended recovery path after [`Error::Overrun`]. Messages
    /// between the old reader position and the current producer position are
    /// skipped.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use bcast::{Error, Reader};
    ///
    /// fn poll<S>(reader: &mut Reader<S>, payload: &mut [u8]) -> Result<(), Error> {
    ///     match reader.receive_next(payload) {
    ///         Some(Ok(msg)) => {
    ///             let _ = msg.payload;
    ///         }
    ///         Some(Err(Error::Overrun(_))) => {
    ///             reader.reset();
    ///         }
    ///         Some(Err(err)) => return Err(err),
    ///         None => {}
    ///     }
    ///     Ok(())
    /// }
    /// ```
    #[cold]
    #[inline(never)]
    pub fn reset(&mut self) {
        let producer_position = self.ring.header().producer_position.load(Ordering::Acquire);
        let claimed_position = self.ring.header().claimed_position.load(Ordering::Acquire);
        self.position = producer_position;
        self.producer_position = producer_position;
        self.claimed_position = claimed_position;
    }

    #[inline]
    fn refresh_producer_position(&mut self) -> usize {
        let producer_position = self.ring.header().producer_position.load(Ordering::Acquire);
        self.producer_position = producer_position;
        producer_position
    }

    #[inline]
    fn readable_limit(&mut self) -> usize {
        let reader_position = self.position;
        let producer_position = self.producer_position;
        if is_position_after(producer_position, reader_position) {
            return producer_position;
        }

        let producer_position = self.refresh_producer_position();
        if is_position_at_or_after(producer_position, reader_position) {
            producer_position
        } else {
            self.producer_position = reader_position;
            reader_position
        }
    }

    #[inline]
    fn refresh_claimed_position(&mut self) -> usize {
        let claimed_position = self.ring.header().claimed_position.load(Ordering::Acquire);
        self.claimed_position = claimed_position;
        claimed_position
    }

    /// Open a bounded batch from this reader's current position to the last
    /// observed producer position.
    ///
    /// The batch limit is fixed when this method is called. Messages published
    /// after the batch is created are not part of that batch. Returns `None`
    /// when there is no committed data to read.
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
    /// writer.send(b"one", true);
    /// writer.send(b"two", true);
    ///
    /// let mut payload = [0u8; 16];
    /// let mut batch = reader.read_batch().unwrap();
    /// assert_eq!(b"one", batch.receive_next(&mut payload).unwrap().unwrap().payload);
    /// assert_eq!(b"two", batch.receive_next(&mut payload).unwrap().unwrap().payload);
    /// assert!(batch.receive_next(&mut payload).is_none());
    /// ```
    #[inline]
    pub fn read_batch(&mut self) -> Option<Batch<'_, S>> {
        let producer_position = self.readable_limit();
        let limit = producer_position.wrapping_sub(self.position);
        if limit == 0 {
            return None;
        }
        Some(Batch {
            reader: self,
            end_position: producer_position,
        })
    }

    /// Open a bounded raw byte window from this reader's current position to the
    /// last observed producer position.
    ///
    /// The returned [`Bulk`] includes frame headers and payload bytes exactly as
    /// they appear in the ring data section. Use this when copying a larger
    /// chunk out of the ring is cheaper than receiving one message at a time.
    /// Returns `None` when there is no committed data to read.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let mut writer = storage.clone().into_writer();
    /// let mut reader = storage.into_reader_at(0);
    ///
    /// writer.send(b"hello", true);
    ///
    /// let bulk = reader.read_bulk().unwrap().unwrap();
    /// assert!(bulk.len() >= b"hello".len());
    /// ```
    #[inline]
    pub fn read_bulk(&mut self) -> Option<Result<Bulk<'_, S>>> {
        let start_position = self.position;
        let end_position = self.readable_limit();
        let len = end_position.wrapping_sub(start_position);
        if len == 0 {
            return None;
        }
        let claimed_position = self.refresh_claimed_position();
        if len > self.ring.capacity || is_overrun(start_position, claimed_position, self.ring.capacity) {
            return Some(Err(Error::overrun(start_position)));
        }
        Some(Ok(Bulk {
            reader: self,
            start_position,
            end_position,
            len,
        }))
    }

    #[inline]
    fn read_frame(&mut self, reader_position: usize) -> Result<Frame> {
        let claimed_position_before = self.claimed_position;
        if is_overrun(reader_position, claimed_position_before, self.ring.capacity) {
            return Err(Error::overrun(reader_position));
        }

        // extract frame header fields
        let frame_header = self.as_frame_header();
        let (is_fin, is_continuation, is_padding, is_heartbeat, length) = frame_header.unpack_fields();
        let user_defined = frame_header.user_defined();
        let claimed_position_after = self.refresh_claimed_position();

        // ensure we have not been overrun by the writer
        // so the frame header is not overwritten and can be trusted
        if is_overrun(reader_position, claimed_position_after, self.ring.capacity) {
            return Err(Error::overrun(reader_position));
        }

        let payload_len = length as usize;
        Ok(Frame {
            payload_len,
            frame_len: get_aligned_size(payload_len) + size_of::<FrameHeader>(),
            user_defined,
            is_fin,
            is_continuation,
            is_padding,
            is_heartbeat,
        })
    }

    #[inline]
    const fn advance_position(&mut self, frame_len: usize) {
        self.position = self.position.wrapping_add(frame_len);
    }

    #[inline]
    const fn skip_frame(&mut self, frame: Frame) {
        self.advance_position(frame.frame_len);
    }

    #[inline]
    fn copy_frame_into<'a>(&mut self, reader_position: usize, frame: Frame, dst: &'a mut [u8]) -> Result<Message<'a>> {
        if frame.payload_len > dst.len() {
            return Err(Error::insufficient_buffer_size(dst.len(), frame.payload_len));
        }

        let payload_start = reader_position.wrapping_add(size_of::<FrameHeader>());
        let payload_index = payload_start & (self.ring.capacity - 1);
        if payload_index + frame.payload_len > self.ring.capacity {
            return Err(Error::corrupt_frame(reader_position, payload_index, frame.payload_len, self.ring.capacity));
        }

        unsafe {
            copy_nonoverlapping(self.ring.header().data_ptr().add(payload_index), dst.as_mut_ptr(), frame.payload_len);
        }

        let claimed_position_after = self.refresh_claimed_position();
        if is_overrun(payload_start, claimed_position_after, self.ring.capacity) {
            return Err(Error::overrun(payload_start));
        }

        self.advance_position(frame.frame_len);
        Ok(Message {
            stream_position: reader_position,
            user_defined: frame.user_defined,
            is_fin: frame.is_fin,
            is_continuation: frame.is_continuation,
            is_heartbeat: frame.is_heartbeat,
            payload: &dst[..frame.payload_len],
        })
    }

    /// Receive the next pending non-padding message, copying its payload into
    /// `dst`.
    ///
    /// Returns `None` when there is no committed message available. Padding
    /// frames are skipped internally.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InsufficientBufferSize`] if `dst` is smaller than the
    /// next payload. Returns [`Error::Overrun`] if the writer has overwritten
    /// the frame before it could be read safely. Returns [`Error::CorruptFrame`]
    /// if the frame header describes payload bytes outside the ring.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let mut writer = storage.clone().into_writer();
    /// let mut reader = storage.into_reader_at(0);
    ///
    /// writer.send(b"hello", true);
    ///
    /// let mut payload = [0u8; 16];
    /// let msg = reader.receive_next(&mut payload).unwrap().unwrap();
    /// assert_eq!(b"hello", msg.payload);
    /// assert!(reader.receive_next(&mut payload).is_none());
    /// ```
    #[inline]
    pub fn receive_next<'a>(&mut self, dst: &'a mut [u8]) -> Option<Result<Message<'a>>> {
        let end_position = self.readable_limit();
        self.receive_next_until(end_position, true, dst)
    }

    #[inline]
    fn receive_next_until<'a>(
        &mut self,
        mut end_position: usize,
        refresh_after_padding: bool,
        dst: &'a mut [u8],
    ) -> Option<Result<Message<'a>>> {
        let mut skipped_padding = false;

        loop {
            let reader_position = self.position;
            if is_position_at_or_after(reader_position, end_position) {
                if refresh_after_padding && skipped_padding {
                    end_position = self.readable_limit();
                    skipped_padding = false;
                    if !is_position_at_or_after(reader_position, end_position) {
                        continue;
                    }
                }
                return None;
            }

            let frame = match self.read_frame(reader_position) {
                Ok(frame) => frame,
                Err(err) => return Some(Err(err)),
            };

            if frame.is_padding {
                self.advance_position(frame.frame_len);
                skipped_padding = true;
                continue;
            }

            return Some(self.copy_frame_into(reader_position, frame, dst));
        }
    }

    /// Skip the next pending non-padding message.
    ///
    /// Returns `None` when there is no committed message available. Padding
    /// frames are skipped internally.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let mut writer = storage.clone().into_writer();
    /// let mut reader = storage.into_reader_at(0);
    ///
    /// writer.send(b"skip", true);
    /// writer.send(b"read", true);
    ///
    /// assert_eq!(Some(Ok(())), reader.skip_next());
    ///
    /// let mut payload = [0u8; 16];
    /// assert_eq!(b"read", reader.receive_next(&mut payload).unwrap().unwrap().payload);
    /// ```
    #[inline]
    pub fn skip_next(&mut self) -> Option<Result<()>> {
        loop {
            let producer_position_before = self.readable_limit();
            // no new messages
            if producer_position_before.wrapping_sub(self.position) == 0 {
                return None;
            }

            let frame = match self.read_frame(self.position) {
                Ok(frame) => frame,
                Err(err) => return Some(Err(err)),
            };

            let is_padding = frame.is_padding;
            self.skip_frame(frame);

            if is_padding {
                continue;
            }

            return Some(Ok(()));
        }
    }
}

/// Message payload and frame metadata.
///
/// The payload slice always points into caller-owned memory:
///
/// - for [`Reader::receive_next`] and [`Batch::receive_next`], it points into
///   the destination buffer passed by the caller.
/// - for [`BulkIter`], it points into the copied bulk buffer being iterated.
#[derive(Debug, Clone, Copy)]
pub struct Message<'a> {
    /// Absolute stream position of the frame header.
    pub stream_position: usize,
    /// User-defined frame value supplied by the writer.
    pub user_defined: u32,
    /// Whether this frame is the final fragment of a logical message.
    pub is_fin: bool,
    /// Whether this frame is a continuation fragment.
    pub is_continuation: bool,
    /// Whether this frame is a heartbeat.
    pub is_heartbeat: bool,
    /// Message payload copied into the caller-provided buffer.
    pub payload: &'a [u8],
}

/// Bounded batch of messages available to a [`Reader`].
///
/// A batch captures the producer position observed by [`Reader::read_batch`].
/// It can reduce repeated producer cursor loads when draining several messages
/// together.
pub struct Batch<'a, S> {
    reader: &'a mut Reader<S>,
    end_position: usize,
}

impl<S> Batch<'_, S> {
    /// Return the number of raw frame bytes remaining in this batch.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let mut writer = storage.clone().into_writer();
    /// let mut reader = storage.into_reader_at(0);
    /// writer.send(b"hello", true);
    ///
    /// let batch = reader.read_batch().unwrap();
    /// assert!(batch.remaining() >= b"hello".len());
    /// ```
    #[inline]
    pub const fn remaining(&self) -> usize {
        let reader_position = self.reader.position;
        if is_position_at_or_after(reader_position, self.end_position) {
            0
        } else {
            self.end_position.wrapping_sub(reader_position)
        }
    }

    /// Receive the next non-padding message from this batch, copying its
    /// payload into `dst`.
    ///
    /// Returns `None` when the batch has been fully consumed.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InsufficientBufferSize`] if `dst` is smaller than the
    /// next payload. Returns [`Error::Overrun`] if the writer has overwritten
    /// the frame before it could be read safely. Returns [`Error::CorruptFrame`]
    /// if the frame header describes payload bytes outside the ring.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let mut writer = storage.clone().into_writer();
    /// let mut reader = storage.into_reader_at(0);
    ///
    /// writer.send(b"hello", true);
    ///
    /// let mut payload = [0u8; 16];
    /// let mut batch = reader.read_batch().unwrap();
    /// assert_eq!(b"hello", batch.receive_next(&mut payload).unwrap().unwrap().payload);
    /// assert!(batch.receive_next(&mut payload).is_none());
    /// ```
    #[inline]
    pub fn receive_next<'a>(&mut self, dst: &'a mut [u8]) -> Option<Result<Message<'a>>> {
        self.reader.receive_next_until(self.end_position, false, dst)
    }

    /// Consume this batch and reset the underlying reader to the producer's
    /// current committed position.
    ///
    /// This is the intended recovery path when [`Batch::receive_next`] returns
    /// [`Error::Overrun`]. Consuming the batch releases its exclusive borrow of
    /// the reader so the caller can open another batch.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use bcast::{Error, Reader};
    ///
    /// fn poll<S>(reader: &mut Reader<S>, payload: &mut [u8]) -> Result<(), Error> {
    ///     let Some(mut batch) = reader.read_batch() else {
    ///         return Ok(());
    ///     };
    ///
    ///     while let Some(result) = batch.receive_next(payload) {
    ///         match result {
    ///             Ok(message) => {
    ///                 let _ = message.payload;
    ///             }
    ///             Err(Error::Overrun(_)) => {
    ///                 batch.reset();
    ///                 return Ok(());
    ///             }
    ///             Err(error) => return Err(error),
    ///         }
    ///     }
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub fn reset(self) {
        self.reader.reset();
    }

    /// Skip all remaining frames in this batch.
    ///
    /// On success the underlying reader advances to the end of the batch.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let mut writer = storage.clone().into_writer();
    /// let mut reader = storage.into_reader_at(0);
    ///
    /// writer.send(b"old", true);
    /// reader.read_batch().unwrap().skip_remaining().unwrap();
    ///
    /// writer.send(b"new", true);
    /// let mut payload = [0u8; 16];
    /// assert_eq!(b"new", reader.receive_next(&mut payload).unwrap().unwrap().payload);
    /// ```
    #[inline]
    pub fn skip_remaining(self) -> Result<()> {
        let remaining = self.remaining();
        if remaining == 0 {
            return Ok(());
        }

        let start_position = self.reader.position;
        let claimed_position = self.reader.refresh_claimed_position();
        if remaining > self.reader.ring.capacity
            || is_overrun(start_position, claimed_position, self.reader.ring.capacity)
        {
            return Err(Error::overrun(start_position));
        }

        self.reader.position = self.end_position;
        Ok(())
    }
}

/// Bounded raw byte window available to a [`Reader`].
///
/// Unlike [`Batch`], bulk reading copies the underlying frame bytes directly and
/// can parse them afterwards with [`BulkIter`]. This is useful when consumers
/// want to amortize ring access and overrun checks across many frames.
pub struct Bulk<'a, S> {
    reader: &'a mut Reader<S>,
    start_position: usize,
    end_position: usize,
    len: usize,
}

#[allow(clippy::len_without_is_empty)]
impl<S> Bulk<'_, S> {
    /// Return the number of raw frame bytes available in this bulk window.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let mut writer = storage.clone().into_writer();
    /// let mut reader = storage.into_reader_at(0);
    /// writer.send(b"hello", true);
    ///
    /// let bulk = reader.read_bulk().unwrap().unwrap();
    /// assert!(bulk.len() >= b"hello".len());
    /// ```
    #[inline]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Absolute stream position at which this bulk window starts.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let mut writer = storage.clone().into_writer();
    /// let mut reader = storage.into_reader_at(0);
    /// writer.send(b"hello", true);
    ///
    /// let bulk = reader.read_bulk().unwrap().unwrap();
    /// assert_eq!(0, bulk.start_position());
    /// ```
    #[inline]
    pub const fn start_position(&self) -> usize {
        self.start_position
    }

    /// Absolute stream position immediately after this bulk window.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let mut writer = storage.clone().into_writer();
    /// let mut reader = storage.into_reader_at(0);
    /// writer.send(b"hello", true);
    ///
    /// let bulk = reader.read_bulk().unwrap().unwrap();
    /// assert_eq!(bulk.start_position() + bulk.len(), bulk.end_position());
    /// ```
    #[inline]
    pub const fn end_position(&self) -> usize {
        self.end_position
    }

    /// Copy this bulk window into `dst`.
    ///
    /// This performs at most two raw copies if the window wraps around the ring
    /// buffer.
    ///
    /// On success reader position advances to `end_position`. On error reader position is left
    /// unchanged so the caller can decide how to recover.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InsufficientBufferSize`] if `dst` is smaller than
    /// [`Bulk::len`]. Returns [`Error::Overrun`] if the writer has overwritten
    /// any part of the window before it could be copied safely.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{LocalStorage, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let mut writer = storage.clone().into_writer();
    /// let mut reader = storage.into_reader_at(0);
    /// writer.send(b"hello", true);
    ///
    /// let bulk = reader.read_bulk().unwrap().unwrap();
    /// let mut bytes = vec![0u8; bulk.len()];
    /// assert_eq!(bytes.len(), bulk.copy_into(&mut bytes).unwrap());
    /// ```
    #[inline]
    pub fn copy_into(self, dst: &mut [u8]) -> Result<usize> {
        if dst.len() < self.len {
            return Err(Error::insufficient_buffer_size(dst.len(), self.len));
        }

        let start_index = self.start_position & (self.reader.ring.capacity - 1);
        let first_len = min(self.len, self.reader.ring.capacity - start_index);
        let data_ptr = self.reader.ring.header().data_ptr();
        let claimed_position_before = self.reader.claimed_position;

        if is_overrun(self.start_position, claimed_position_before, self.reader.ring.capacity) {
            return Err(Error::overrun(self.start_position));
        }

        unsafe {
            copy_nonoverlapping(data_ptr.add(start_index), dst.as_mut_ptr(), first_len);
            if self.len > first_len {
                copy_nonoverlapping(data_ptr, dst.as_mut_ptr().add(first_len), self.len - first_len);
            }
        }

        let claimed_position_after = self.reader.refresh_claimed_position();
        if is_overrun(self.start_position, claimed_position_after, self.reader.ring.capacity) {
            return Err(Error::overrun(self.start_position));
        }

        self.reader.position = self.end_position;
        Ok(self.len)
    }

    /// Copy this bulk window into `dst` and return an iterator over the copied
    /// data.
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
    /// let bulk = reader.read_bulk().unwrap().unwrap();
    /// let mut bytes = vec![0u8; bulk.len()];
    /// let mut messages = bulk.into_iter(&mut bytes).unwrap();
    ///
    /// assert_eq!(b"hello", messages.next().unwrap().payload);
    /// assert!(messages.next().is_none());
    /// ```
    #[inline]
    pub fn into_iter(self, dst: &mut [u8]) -> Result<BulkIter<'_>> {
        let start_position = self.start_position;
        let len = self.copy_into(dst)?;
        Ok(BulkIter::new(&dst[..len], start_position))
    }
}

/// Iterator over messages contained in raw bulk bytes.
///
/// Padding frames are skipped automatically. Use [`Bulk::into_iter`] for the
/// common path where the bytes are copied out of the ring immediately before
/// iteration.
pub struct BulkIter<'a> {
    bytes: &'a [u8],
    start_position: usize,
    index: usize,
}

impl<'a> BulkIter<'a> {
    /// Construct an iterator over raw bulk bytes.
    ///
    /// `start_position` must be the absolute stream position of the first byte
    /// in `bytes`.
    ///
    /// # Example
    ///
    /// ```
    /// use bcast::{BulkIter, LocalStorage, StorageExt};
    ///
    /// let storage = LocalStorage::with_capacity(1024).into_shared();
    /// let mut writer = storage.clone().into_writer();
    /// let mut reader = storage.into_reader_at(0);
    /// writer.send(b"hello", true);
    ///
    /// let bulk = reader.read_bulk().unwrap().unwrap();
    /// let start_position = bulk.start_position();
    /// let mut bytes = vec![0u8; bulk.len()];
    /// let len = bulk.copy_into(&mut bytes).unwrap();
    ///
    /// let mut messages = BulkIter::new(&bytes[..len], start_position);
    /// assert_eq!(b"hello", messages.next().unwrap().payload);
    /// ```
    #[inline]
    pub const fn new(bytes: &'a [u8], start_position: usize) -> Self {
        Self {
            bytes,
            start_position,
            index: 0,
        }
    }

    fn next_impl(&mut self) -> Option<Message<'a>> {
        while self.index < self.bytes.len() {
            let header_end = self.index + size_of::<FrameHeader>();
            if header_end > self.bytes.len() {
                return None;
            }

            let header_ptr = unsafe { self.bytes.as_ptr().add(self.index) };
            let (fields, user_defined) = unsafe { read_bulk_header(header_ptr) };
            let (is_fin, is_continuation, is_padding, is_heartbeat, payload_len) = unpack_fields(fields);
            let payload_len = payload_len as usize;
            let aligned_payload_len = get_aligned_size(payload_len);
            let frame_len = size_of::<FrameHeader>() + aligned_payload_len;
            let payload_start = self.index + size_of::<FrameHeader>();
            let payload_end = payload_start + payload_len;
            let stream_position = self.start_position + self.index;

            if payload_end > self.bytes.len() || self.index + frame_len > self.bytes.len() {
                return None;
            }

            self.index += frame_len;

            if is_padding {
                continue;
            }

            return Some(Message {
                stream_position,
                user_defined,
                is_fin,
                is_continuation,
                is_heartbeat,
                payload: &self.bytes[payload_start..payload_end],
            });
        }

        None
    }
}

#[inline]
const unsafe fn read_bulk_header(ptr: *const u8) -> (u32, u32) {
    let header = unsafe { read_unaligned(ptr as *const u64) };
    unpack_header(header)
}

impl<'a> Iterator for BulkIter<'a> {
    type Item = Message<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_impl()
    }
}

#[derive(Clone, Copy)]
struct Frame {
    payload_len: usize,
    frame_len: usize,
    user_defined: u32,
    is_fin: bool,
    is_continuation: bool,
    is_padding: bool,
    is_heartbeat: bool,
}
