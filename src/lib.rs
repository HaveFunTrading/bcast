//! Low latency, single producer & many consumer (SPMC) ring buffer that works with shared memory.
//! Natively supports variable message sizes.
//!
//! ## Examples
//! Create `Writer` and use `claim` to publish a message.
//! ```no_run
//! use bcast::RingBuffer;
//!
//! // create writer
//! let bytes = [0u8; 1024];
//! let mut writer = RingBuffer::new(&bytes).into_writer();
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
//! Create `Reader` and use `batch_iter` tp receive messages.
//! ```no_run
//! use bcast::RingBuffer;
//!
//! // create reader
//! let bytes = [0u8; 1024];
//! let mut reader = RingBuffer::new(&bytes).into_reader();
//! let mut iter = reader.read_batch().unwrap().into_iter();
//! let mut payload = [0u8; 1024];
//!
//! // read first message
//! let msg = iter.next().unwrap().unwrap();
//! let len = msg.read(&mut payload).unwrap();
//! assert_eq!(b"hello", &payload[..len]);
//!
//! // read second message
//! let msg = iter.next().unwrap().unwrap();
//! let len = msg.read(&mut payload).unwrap();
//! assert_eq!(b"world", &payload[..len]);
//!
//! // no more messages
//! assert!(iter.next().is_none())
//! ```

pub mod error;

#[cfg(feature = "mmap")]
pub mod mmap;
pub mod util;

use crossbeam_utils::CachePadded;
use std::cell::Cell;
use std::cmp::min;
use std::hint;
use std::mem::ManuallyDrop;
use std::ptr::{NonNull, copy_nonoverlapping};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::error::Error;
// re-export
pub use error::Result;
use std::mem::align_of;
use std::mem::size_of;

/// Ring buffer header size in bytes.
pub const HEADER_SIZE: usize = size_of::<Header>();
/// Metadata buffer size in bytes.
pub const METADATA_BUFFER_SIZE: usize = 1024;
/// Null value for `user_defined` field.
pub const USER_DEFINED_NULL_VALUE: u32 = 0;

// mask to obtain message length from frame header
const FRAME_HEADER_MSG_LEN_MASK: u32 = 0x0FFFFFFF;
// represents the max value we can encode on the frame header for the payload length
const MAX_PAYLOAD_LEN: usize = (1 << 28) - 1;

/// Ring buffer header that contains producer position. The position is expressed in bytes and
/// will always increment.
#[derive(Debug)]
#[repr(C)]
struct Header {
    producer_position: CachePadded<AtomicUsize>, // will always increase
    ready: CachePadded<AtomicBool>,              // indicates channel readiness
    metadata: CachePadded<[u8; 1024]>,           // metadata buffer
}

impl Header {
    /// Get pointer to the data section of the ring buffer.
    #[inline]
    const fn data_ptr(&self) -> *const u8 {
        let header_ptr: *const Header = self;
        unsafe { header_ptr.add(1) as *const u8 }
    }

    /// Check readiness status.
    #[inline]
    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }

    /// Get metadata buffer as slice.
    #[inline]
    fn metadata(&self) -> &[u8] {
        &*self.metadata
    }

    /// Get metadata buffer as mutable slice.
    #[inline]
    fn metadata_mut(&mut self) -> &mut [u8] {
        &mut *self.metadata
    }
}

/// Message frame header that contains packed `fields` (fin, continuation, padding, length)
/// as well as `user_defined` field.
#[repr(C, align(8))]
struct FrameHeader {
    fields: Cell<u32>,       // contains padding flag and payload length
    user_defined: Cell<u32>, // user defined field
}

impl FrameHeader {
    #[inline]
    #[cfg(test)]
    const fn new(
        payload_len: u32,
        user_defined: u32,
        fin: bool,
        continuation: bool,
        padding: bool,
        heartbeat: bool,
    ) -> Self {
        let fields = pack_fields(fin, continuation, padding, heartbeat, payload_len);
        FrameHeader {
            fields: Cell::new(fields),
            user_defined: Cell::new(user_defined),
        }
    }

    #[inline]
    #[cfg(test)]
    const fn new_padding() -> Self {
        Self::new(0, 0, true, false, true, false)
    }

    #[inline]
    #[cfg(test)]
    fn is_heartbeat(&self) -> bool {
        ((self.fields.get() >> 28) & 1) == 1
    }

    #[inline]
    #[cfg(test)]
    fn is_padding(&self) -> bool {
        ((self.fields.get() >> 29) & 1) == 1
    }

    #[inline]
    #[cfg(test)]
    fn is_continuation(&self) -> bool {
        ((self.fields.get() >> 30) & 1) == 1
    }

    #[inline]
    #[cfg(test)]
    fn is_fin(&self) -> bool {
        ((self.fields.get() >> 31) & 1) == 1
    }

    #[inline]
    #[cfg(test)]
    fn payload_len(&self) -> u32 {
        self.fields.get() & FRAME_HEADER_MSG_LEN_MASK
    }

    /// Extract `(fin, continuation, padding, heartbeat, length)` fields from the message header.
    #[inline]
    fn unpack_fields(&self) -> (bool, bool, bool, bool, u32) {
        unpack_fields(self.fields.get())
    }

    /// Get pointer to the message payload.
    #[inline]
    const fn get_payload_ptr(&self) -> *const FrameHeader {
        let message_header_ptr: *const FrameHeader = self;
        unsafe { message_header_ptr.add(1) }
    }

    /// Get mutable pointer to the message payload.
    #[inline]
    const fn get_payload_ptr_mut(&self) -> *mut FrameHeader {
        let message_header_ptr = self as *const FrameHeader as *mut FrameHeader;
        unsafe { message_header_ptr.add(1) }
    }
}

/// Packs the `FrameHeader` fields into a single u32 according to the following encoding:
/// - Bit 31: fin flag
/// - Bit 30: continuation flag
/// - Bit 29: padding flag
/// - Bit 28: reserved (always 0)
/// - Bits 0-27: message length
#[inline]
const fn pack_fields(fin: bool, continuation: bool, padding: bool, heartheat: bool, length: u32) -> u32 {
    unsafe { pack_fields_unchecked(fin, continuation, padding, heartheat, length & FRAME_HEADER_MSG_LEN_MASK) }
}

#[inline]
const unsafe fn pack_fields_unchecked(
    fin: bool,
    continuation: bool,
    padding: bool,
    heartbeat: bool,
    length: u32,
) -> u32 {
    length
        | ((heartbeat as u32) << 28)
        | ((padding as u32) << 29)
        | ((continuation as u32) << 30)
        | ((fin as u32) << 31)
}

/// Unpacks `u32` field into a tuple: (fin, continuation, padding, heartbeat, length).
#[inline]
const fn unpack_fields(fields: u32) -> (bool, bool, bool, bool, u32) {
    let fin = (fields >> 31) & 1 == 1;
    let continuation = (fields >> 30) & 1 == 1;
    let padding = (fields >> 29) & 1 == 1;
    let heartbeat = (fields >> 28) & 1 == 1;
    let length = fields & FRAME_HEADER_MSG_LEN_MASK;
    (fin, continuation, padding, heartbeat, length)
}

/// Calculate the number of bytes needed to ensure frame header alignment of the payload.
#[inline]
const fn get_aligned_size(payload_length: usize) -> usize {
    const ALIGNMENT_MASK: usize = align_of::<FrameHeader>() - 1;
    (payload_length + ALIGNMENT_MASK) & !ALIGNMENT_MASK
}

/// Single producer, many consumer (SPMC) ring buffer backed by shared memory.
#[derive(Debug, Clone)]
pub struct RingBuffer {
    ptr: NonNull<Header>,
    capacity: usize,
    mtu: usize,
}

impl RingBuffer {
    /// Create new `RingBuffer` by wrapping provided `bytes`. It is necessary to call `into_writer()`
    /// or `into_reader()` following the buffer construction to start using it.
    pub fn new(bytes: &[u8]) -> Self {
        assert!(bytes.len() > size_of::<Header>(), "insufficient size for the header");
        assert!((bytes.len() - size_of::<Header>()).is_power_of_two(), "buffer len must be power of two");

        let header = bytes.as_ptr() as *mut Header;
        let capacity = bytes.len() - size_of::<Header>();
        Self {
            ptr: NonNull::new(header).unwrap(),
            capacity,
            mtu: min(capacity / 2 - size_of::<FrameHeader>(), MAX_PAYLOAD_LEN),
        }
    }

    /// Get mutable reference to ring buffer header.
    #[inline]
    const fn header(&self) -> &'static mut Header {
        unsafe { &mut *self.ptr.as_ptr() }
    }

    /// Will consume `self` and return instance of writer backed by this ring buffer. The writer
    /// will write from the beginning of the ring buffer. If you need to continue writing to the
    /// previous buffer use `RingBuffer::join_writer` instead.
    pub fn into_writer(self) -> Writer {
        // will write from the start of the buffer
        self.header().producer_position.store(0, Ordering::SeqCst);
        // mark as initialised
        self.header().ready.store(true, Ordering::SeqCst);
        Writer {
            ring: self,
            position: Cell::new(0),
        }
    }

    /// Will consume `self` and return instance of writer backed by this ring buffer. The writer
    /// will not reset current `producer_position` and will continue writing from that point.
    pub fn join_writer(self) -> Writer {
        assert!(self.header().ready.load(Ordering::SeqCst), "unable to join buffer not marked as ready");
        // read current position
        let position = self.header().producer_position.load(Ordering::SeqCst);
        Writer {
            ring: self,
            position: Cell::new(position),
        }
    }

    /// Will consume `self` and return instance of writer backed by this ring buffer, with the
    /// initial position set to the provided value.
    #[cfg(test)]
    pub fn into_writer_at(self, position: usize) -> Writer {
        assert_eq!(get_aligned_size(position), position, "position must be aligned");
        self.header().producer_position.store(position, Ordering::SeqCst);
        // mark as initialised after setting the position
        self.header().ready.store(true, Ordering::SeqCst);
        Writer {
            ring: self,
            position: Cell::new(position),
        }
    }

    /// Will consume `self` and return instance of writer backed by this ring buffer. This
    /// method also accepts closure to populate `metadata` buffer.
    pub fn into_writer_with_metadata<F: FnOnce(&mut [u8])>(self, metadata: F) -> Writer {
        // populate metadata
        metadata(self.header().metadata_mut());
        self.into_writer()
    }

    /// Will consume `self` and return instance of reader backed by this ring buffer. The reader
    /// position will be set to producer most up-to-date position.
    pub fn into_reader(self) -> Reader {
        // wait until buffer has been initialised
        while !self.header().is_ready() {
            hint::spin_loop();
        }
        let producer_position = self.header().producer_position.load(Ordering::SeqCst);
        Reader {
            ring: self,
            position: Cell::new(producer_position),
        }
    }
}

/// Wraps `RingBuffer` and allows to publish messages. Only single writer should be present at any time.
#[derive(Debug)]
pub struct Writer {
    ring: RingBuffer,
    position: Cell<usize>, // local producer position
}

impl From<RingBuffer> for Writer {
    fn from(ring: RingBuffer) -> Self {
        ring.into_writer()
    }
}

impl Writer {
    /// Claim part of the underlying `RingBuffer` for publication. The `fin` flag is used to indicate
    /// final message fragment.
    ///
    /// ## Panics
    /// When aligned message length is greater than the `MTU`.
    #[inline]
    pub fn claim(&self, len: usize, fin: bool) -> Claim<'_> {
        self.claim_with_user_defined(len, fin, USER_DEFINED_NULL_VALUE)
    }

    /// Claim part of the underlying `RingBuffer` for publication. The `fin` flag is used to indicate
    /// final message fragment. This method also accepts `user_defined` value
    /// that will be attached to the message frame header.
    ///
    /// ## Panics
    /// When aligned message length is greater than the `MTU`.
    #[inline]
    pub fn claim_with_user_defined(&self, len: usize, fin: bool, user_defined: u32) -> Claim<'_> {
        let aligned_len = get_aligned_size(len);
        debug_assert!(aligned_len <= self.mtu(), "mtu exceeded");
        Claim::new(self, aligned_len, len, user_defined, fin, false, false)
    }

    /// Claim part of the underlying `RingBuffer` for continuation frame publication also passing
    /// `fin` value to indicate final message fragment.
    ///
    /// ## Panics
    /// When aligned message length is greater than the `MTU`.
    #[inline]
    pub fn continuation(&self, len: usize, fin: bool) -> Claim<'_> {
        let aligned_len = get_aligned_size(len);
        debug_assert!(aligned_len <= self.mtu(), "mtu exceeded");
        Claim::new(self, aligned_len, len, USER_DEFINED_NULL_VALUE, fin, true, false)
    }

    /// Claim part of the underlying `RingBuffer` for heartbeat frame publication (zero payload,
    /// no user defined field and no fragmentation). This operation will always succeed.
    #[inline]
    pub fn heartbeat(&self) -> Claim<'_> {
        Claim::new(self, 0, 0, USER_DEFINED_NULL_VALUE, true, false, true)
    }

    /// Claim part of the underlying `RingBuffer` for heartbeat frame publication with user defined
    /// field (zero payload and no fragmentation). This operation will always succeed.
    #[inline]
    pub fn heartbeat_with_user_defined(&self, user_defined: u32) -> Claim<'_> {
        Claim::new(self, 0, 0, user_defined, true, false, true)
    }

    /// Claim part of the underlying `RingBuffer` for heartbeat frame publication with payload (no
    /// user defined field and no fragmentation). This operation will always succeed.
    #[inline]
    pub fn heartbeat_with_payload(&self, len: usize) -> Claim<'_> {
        let aligned_len = get_aligned_size(len);
        debug_assert!(aligned_len <= self.mtu(), "mtu exceeded");
        Claim::new(self, aligned_len, len, USER_DEFINED_NULL_VALUE, true, false, true)
    }

    /// Claim part of the underlying `RingBuffer` for heartbeat frame publication with payload and
    /// user defined field (no fragmentation). This operation will always succeed.
    #[inline]
    pub fn heartbeat_with_payload_and_user_defined(&self, len: usize, user_defined: u32) -> Claim<'_> {
        let aligned_len = get_aligned_size(len);
        debug_assert!(aligned_len <= self.mtu(), "mtu exceeded");
        Claim::new(self, aligned_len, len, user_defined, true, false, true)
    }

    /// Get maximum permissible unaligned payload length that can be accepted by the buffer.
    /// It is calculated as `min(capacity / 2 - size_of::<FrameHeader>(), MAX_PAYLOAD_LEN)` where
    /// `MAX_PAYLOAD_LEN` is `(1 << 31) - 1`.
    #[inline]
    pub const fn mtu(&self) -> usize {
        self.ring.mtu
    }

    /// Buffer index at which next write will happen.
    #[inline]
    const fn index(&self) -> usize {
        self.position.get() & (self.ring.capacity - 1)
    }

    /// Number of bytes remaining in the buffer before it will wrap around.
    #[inline]
    fn remaining(&self) -> usize {
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
}

/// Represents region of the `RingBuffer` we can publish message to.
#[derive(Debug)]
pub struct Claim<'a> {
    writer: &'a Writer,       // underlying writer
    position_snapshot: usize, // writer initial position
    len: usize,               // frame header aligned payload length
    limit: usize,             // actual payload length
    user_defined: u32,        // user defined field
    fin: bool,                // final message fragment
    continuation: bool,       // continuation frame
    heartbeat: bool,          // heartbeat frame
}

impl<'a> Claim<'a> {
    /// Create new claim.
    #[inline]
    fn new(
        writer: &'a Writer,
        len: usize,
        limit: usize,
        user_defined: u32,
        fin: bool,
        continuation: bool,
        heartbeat: bool,
    ) -> Self {
        #[cold]
        fn insert_padding_frame(writer: &Writer, remaining: usize) {
            let padding_len = remaining - size_of::<FrameHeader>();
            let fields = pack_fields(true, false, true, false, padding_len as u32);
            let header = writer.frame_header();
            header.fields.set(fields);
            header.user_defined.set(USER_DEFINED_NULL_VALUE);
            writer.position.set(
                writer
                    .position
                    .get()
                    .wrapping_add(padding_len + size_of::<FrameHeader>()),
            );
        }

        let position_snapshot = writer.position.get();

        // insert padding frame if required
        let remaining = writer.remaining();
        if len + size_of::<FrameHeader>() > remaining {
            insert_padding_frame(writer, remaining);
        };

        Self {
            writer,
            position_snapshot,
            len,
            limit,
            user_defined,
            fin,
            continuation,
            heartbeat,
        }
    }

    /// Get next message payload as byte slice.
    #[inline]
    pub fn get_buffer(&self) -> &[u8] {
        let ptr = self.writer.frame_header().get_payload_ptr();
        unsafe { std::slice::from_raw_parts(ptr as *const u8, self.limit) }
    }

    /// Get next message payload as mutable byte slice.
    #[inline]
    pub fn get_buffer_mut(&mut self) -> &mut [u8] {
        let ptr = self.writer.frame_header().get_payload_ptr_mut();
        unsafe { std::slice::from_raw_parts_mut(ptr as *mut u8, self.limit) }
    }

    /// Abort the publication.
    #[inline]
    pub fn abort(self) {
        // rollback to the initial position (in case padding frame was inserted)
        self.writer.position.set(self.position_snapshot);
        let _ = ManuallyDrop::new(self);
    }

    /// Commit the message thus making it visible to other consumers. If this operation is not
    /// invoked, the commit will happen automatically when `Claim` is dropped.
    #[inline]
    pub fn commit(self) {
        // we need to ensure the destructor will not be called in this case
        ManuallyDrop::new(self).commit_impl();
    }

    #[inline]
    fn commit_impl(&self) {
        // update frame header
        let header = self.writer.frame_header();
        let fields = pack_fields(self.fin, self.continuation, false, self.heartbeat, self.limit as u32);
        header.fields.set(fields);
        header.user_defined.set(self.user_defined);

        // advance writer position
        self.writer.position.set(
            self.writer
                .position
                .get()
                .wrapping_add(self.len + size_of::<FrameHeader>()),
        );

        // signal updated producer position
        self.writer
            .ring
            .header()
            .producer_position
            .store(self.writer.position.get(), Ordering::Release);
    }
}

impl Drop for Claim<'_> {
    fn drop(&mut self) {
        self.commit_impl();
    }
}

/// Wraps `RingBuffer` and allows to receive messages. Multiple readers can be present at any time,
/// they operate independently and are not part of any congestion control flow. As a result, each reader
/// can be overrun by the producer if it's unable to keep up.
pub struct Reader {
    ring: RingBuffer,
    position: Cell<usize>, // local position that will always increase
}

impl Reader {
    /// Get metadata buffer associated with the underlying ring buffer.
    pub fn metadata(&self) -> &'static [u8] {
        self.ring.header().metadata()
    }

    /// Set reader initial position (the default is producer current position).
    pub fn with_initial_position(self, position: usize) -> Self {
        assert_eq!(get_aligned_size(position), position, "position must be aligned");
        Self {
            ring: self.ring,
            position: Cell::new(position),
        }
    }

    /// Obtain reference to the (unpublished) message frame header.
    #[inline]
    fn as_frame_header(&self) -> &FrameHeader {
        unsafe { &*(self.ring.header().data_ptr().add(self.index()) as *const FrameHeader) }
    }

    /// Buffer index at which read will happen.
    #[inline]
    fn index(&self) -> usize {
        self.position.get() & (self.ring.capacity - 1)
    }

    /// Reset reader position to current producer position, for recovering from overrun.
    #[cold]
    pub fn reset(&self) {
        self.position
            .set(self.ring.header().producer_position.load(Ordering::Acquire));
    }

    /// Construct `Batch` object that can efficiently read multiple messages in a batch between
    /// `Reader` current position and prevailing producer position. Returns `None` if there is
    /// no new data to read.
    #[inline]
    pub fn read_batch(&self) -> Option<Batch<'_>> {
        let producer_position = self.ring.header().producer_position.load(Ordering::Acquire);
        let limit = producer_position.wrapping_sub(self.position.get());
        if limit == 0 {
            return None;
        }
        let position_snapshot = self.position.get();
        Some(Batch {
            reader: self,
            remaining: limit,
            position_snapshot,
        })
    }

    /// Receive next pending message from the ring buffer.
    #[inline]
    pub fn receive_next(&self) -> Option<Result<Message>> {
        let producer_position_before = self.ring.header().producer_position.load(Ordering::Acquire);
        // no new messages
        if producer_position_before.wrapping_sub(self.position.get()) == 0 {
            return None;
        }
        // attempt to receive next frame
        // if the frame is padding will skip it and attempt to return next frame
        match self.receive_next_impl(self.position.get()) {
            Some(msg) => match msg {
                Ok(msg) if !msg.is_padding => Some(Ok(msg)),
                Ok(_) => self.receive_next_impl(self.position.get()),
                Err(err) => Some(Err(err)),
            },
            None => None,
        }
    }

    #[inline]
    fn receive_next_impl(&self, reader_position: usize) -> Option<Result<Message>> {
        // extract frame header fields
        let frame_header = self.as_frame_header();
        let (is_fin, is_continuation, is_padding, is_heartbeat, length) = frame_header.unpack_fields();
        let user_defined = frame_header.user_defined.get();
        let producer_position_after = self.ring.header().producer_position.load(Ordering::Acquire);

        // construct the massage
        let message = Message {
            header: self.ring.header(),
            position: self.position.get().wrapping_add(size_of::<FrameHeader>()),
            payload_len: length as usize,
            capacity: self.ring.capacity,
            is_fin,
            is_continuation,
            is_padding,
            is_heartbeat,
            user_defined,
        };

        // ensure we have not been overrun by the writer
        // so the frame header is not overwritten and can be trusted
        if producer_position_after.wrapping_sub(reader_position) > self.ring.capacity {
            return Some(Err(Error::overrun(reader_position)));
        }

        // update reader position
        let aligned_payload_len = get_aligned_size(message.payload_len);
        let position = self.position.get();
        self.position
            .set(position.wrapping_add(aligned_payload_len + size_of::<FrameHeader>()));
        Some(Ok(message))
    }
}

/// Contains coordinates to some payload at particular point in time. Messages are consumer in a
/// 'lazily' way that's why it's safe to `clone()` and pass them around. When message is read
/// (consumed) it can result in overrun error if the producer has lapped around.
#[derive(Debug, Clone)]
pub struct Message {
    header: &'static Header, // ring buffer header
    position: usize,         // marks beginning of message payload
    capacity: usize,         // ring buffer capacity
    /// Message length.
    pub payload_len: usize,
    /// User defined field.
    pub user_defined: u32,
    /// Indicates final message fragment.
    pub is_fin: bool,
    /// Indicates continuation frame
    pub is_continuation: bool,
    /// Indicates padding frame.
    pub is_padding: bool,
    /// Indicates heartbeat frame.
    pub is_heartbeat: bool,
}

impl Message {
    /// Buffer index at which read will happen.
    #[inline]
    const fn index(&self) -> usize {
        self.position & (self.capacity - 1)
    }

    /// Read the message into specified buffer. It will return error if the provided buffer
    /// is too small. Will also return error if at any point the producer has overrun this consumer.
    /// On success, it will return the number of bytes written to the buffer.
    /// ## Examples
    /// ```no_run
    /// use bcast::Message;
    ///
    /// fn consume_message(msg: &Message) {
    ///     let mut payload = [0u8; 1024];
    ///     // read into provided buffer (error means overrun)
    ///     let len = msg.read(&mut payload).unwrap();
    ///     // process payload
    ///     // ...
    /// }
    /// ```
    #[inline]
    pub fn read(&self, buf: &mut [u8]) -> Result<usize> {
        debug_assert!(
            self.payload_len <= min(self.capacity / 2 - size_of::<FrameHeader>(), MAX_PAYLOAD_LEN),
            "payload size is greater than mtu"
        );
        debug_assert!(self.index() + self.payload_len <= self.capacity, "payload over shots ring buffer");
        // ensure destination buffer is of sufficient size
        if self.payload_len > buf.len() {
            return Err(Error::insufficient_buffer_size(buf.len(), self.payload_len));
        }

        // attempt to copy message data into provided buffer
        let producer_position_before = self.position;
        unsafe {
            copy_nonoverlapping(self.header.data_ptr().add(self.index()), buf.as_mut_ptr(), self.payload_len);
        }
        let producer_position_after = self.header.producer_position.load(Ordering::Acquire);

        // ensure we have not been overrun by the producer
        if producer_position_after.wrapping_sub(producer_position_before) > self.capacity {
            return Err(Error::overrun(self.position));
        }

        Ok(self.payload_len)
    }
}

/// Represents pending batch of messages between last observed producer position and the reader current position.
/// Should be used in conjunction with `BatchIter` to allow iteration.
pub struct Batch<'a> {
    reader: &'a Reader,
    remaining: usize,         // remaining bytes to consume
    position_snapshot: usize, // reader position snapshot
}

impl<'a> IntoIterator for Batch<'a> {
    type Item = <BatchIter<'a> as Iterator>::Item;
    type IntoIter = BatchIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        BatchIter { batch: self }
    }
}

impl Batch<'_> {
    /// Return the number of remaining bytes to be consumed by this instance of batch.
    #[inline]
    pub const fn remaining(&self) -> usize {
        self.remaining
    }

    /// Receive next message from the current batch or `None` if end of batch. This is a low level
    /// method that will also return padding frames. Use `into_iter()` to work with more user-friendly
    /// `BatchIter`.
    #[inline]
    pub fn receive_next(&mut self) -> Option<Result<Message>> {
        // we reached end of batch
        if self.remaining == 0 {
            return None;
        }
        // update iterator with the number of bytes consumed
        match self.reader.receive_next_impl(self.position_snapshot) {
            None => None,
            Some(Ok(msg)) => {
                self.remaining -= get_aligned_size(msg.payload_len) + size_of::<FrameHeader>();
                Some(Ok(msg))
            }
            Some(Err(err)) => Some(Err(err)),
        }
    }
}

/// Iterator that allows to process pending messages in a batch. This is more efficient than iterating
/// over the messages using `receive_next()` on the `Reader` directly.
pub struct BatchIter<'a> {
    batch: Batch<'a>,
}

impl Iterator for BatchIter<'_> {
    type Item = Result<Message>;

    fn next(&mut self) -> Option<Self::Item> {
        // attempt to receive next frame
        // if the frame is padding will skip it and attempt to return next frame
        match self.batch.receive_next() {
            None => None,
            Some(msg) => match msg {
                Ok(msg) if !msg.is_padding => Some(Ok(msg)),
                Ok(_) => self.batch.receive_next(),
                Err(err) => Some(Err(err)),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::util::AlignedBytes;
    use aligned_vec::avec;
    use rand::{Rng, thread_rng};
    use std::ptr::addr_of;
    use std::sync::atomic::Ordering::SeqCst;

    #[test]
    fn should_read_messages_in_batch() {
        let bytes = AlignedBytes::<{ HEADER_SIZE + 64 }>::new();
        let writer = RingBuffer::new(&bytes).into_writer();
        let reader = RingBuffer::new(&bytes).into_reader();

        let mut claim = writer.claim(5, true);
        claim.get_buffer_mut().copy_from_slice(b"hello");
        claim.commit();

        let mut claim = writer.claim(5, true);
        claim.get_buffer_mut().copy_from_slice(b"world");
        claim.commit();

        let mut iter = reader.read_batch().unwrap().into_iter();

        let msg = iter.next().unwrap().unwrap();
        let mut payload = [0u8; 1024];
        msg.read(&mut payload).unwrap();
        let payload = &payload[..msg.payload_len];
        assert_eq!(payload, b"hello");

        let msg = iter.next().unwrap().unwrap();
        let mut payload = [0u8; 1024];
        msg.read(&mut payload).unwrap();
        let payload = &payload[..msg.payload_len];
        assert_eq!(payload, b"world");

        assert_eq!(32, writer.index());
        assert_eq!(32, writer.remaining());
        assert_eq!(32, iter.batch.reader.index());

        assert!(iter.next().is_none());

        assert_eq!(32, iter.batch.reader.index());
        assert_eq!(32, iter.batch.reader.position.get());
        assert_eq!(32, writer.position.get());
        assert_eq!(32, writer.remaining());

        let claim = writer.claim(15, true);
        claim.commit();

        assert_eq!(56, writer.index());
        assert_eq!(56, writer.position.get());

        let mut claim = writer.claim(4, true);
        claim.get_buffer_mut().copy_from_slice(b"test");
        claim.commit();

        let mut iter = reader.read_batch().unwrap().into_iter();

        // skip big message
        let _ = iter.next().unwrap().unwrap();
        let msg = iter.next().unwrap().unwrap();

        let mut payload = [0u8; 1024];
        msg.read(&mut payload).unwrap();
        let payload = &payload[..msg.payload_len];
        assert_eq!(payload, b"test");

        assert!(iter.next().is_none());

        assert_eq!(reader.index(), writer.index());
        assert_eq!(reader.position.get(), writer.position.get());
    }

    #[test]
    fn should_read_in_batch_with_limit() {
        let bytes = AlignedBytes::<{ HEADER_SIZE + 64 }>::new();
        let writer = RingBuffer::new(&bytes).into_writer();
        let reader = RingBuffer::new(&bytes).into_reader();

        let mut claim = writer.claim(1, true);
        claim.get_buffer_mut().copy_from_slice(b"a");
        claim.commit();

        let mut claim = writer.claim(1, true);
        claim.get_buffer_mut().copy_from_slice(b"b");
        claim.commit();

        let mut claim = writer.claim(1, true);
        claim.get_buffer_mut().copy_from_slice(b"c");
        claim.commit();

        let mut iter = reader.read_batch().unwrap().into_iter().take(2);

        let msg = iter.next().unwrap().unwrap();
        let mut payload = [0u8; 1];
        msg.read(&mut payload).unwrap();
        assert_eq!(b"a", &payload);

        let msg = iter.next().unwrap().unwrap();
        let mut payload = [0u8; 1];
        msg.read(&mut payload).unwrap();
        assert_eq!(b"b", &payload);

        assert!(iter.next().is_none());

        let mut iter = reader.read_batch().unwrap().into_iter();

        let msg = iter.next().unwrap().unwrap();
        let mut payload = [0u8; 1];
        msg.read(&mut payload).unwrap();
        assert_eq!(b"c", &payload);

        assert!(iter.next().is_none());
    }

    #[test]
    fn should_resume_batch_if_previous_not_consumed() {
        let bytes = AlignedBytes::<{ HEADER_SIZE + 64 }>::new();
        let writer = RingBuffer::new(&bytes).into_writer();
        let reader = RingBuffer::new(&bytes).into_reader();

        writer.claim_with_user_defined(0, true, 100).commit();
        writer.claim_with_user_defined(0, true, 200).commit();
        writer.claim_with_user_defined(0, true, 300).commit();
        writer.claim_with_user_defined(0, true, 400).commit();

        let mut iter = reader.read_batch().unwrap().into_iter();

        assert_eq!(100, iter.next().unwrap().unwrap().user_defined);
        assert_eq!(200, iter.next().unwrap().unwrap().user_defined);
        assert_eq!(300, iter.next().unwrap().unwrap().user_defined);

        let mut iter = reader.read_batch().unwrap().into_iter();
        assert_eq!(400, iter.next().unwrap().unwrap().user_defined);
        assert!(iter.next().is_none());
    }

    #[test]
    fn should_read_next_message_if_batch_not_consumed() {
        let bytes = AlignedBytes::<{ HEADER_SIZE + 64 }>::new();
        let writer = RingBuffer::new(&bytes).into_writer();
        let reader = RingBuffer::new(&bytes).into_reader();

        writer.claim_with_user_defined(0, true, 100).commit();
        writer.claim_with_user_defined(0, true, 200).commit();
        writer.claim_with_user_defined(0, true, 300).commit();
        writer.claim_with_user_defined(0, true, 400).commit();

        let mut iter = reader.read_batch().unwrap().into_iter();

        assert_eq!(100, iter.next().unwrap().unwrap().user_defined);
        assert_eq!(200, iter.next().unwrap().unwrap().user_defined);
        assert_eq!(300, iter.next().unwrap().unwrap().user_defined);

        assert_eq!(400, reader.receive_next().unwrap().unwrap().user_defined);
        assert!(reader.receive_next().is_none());
    }

    #[test]
    fn should_read_next_message() {
        let bytes = AlignedBytes::<{ HEADER_SIZE + 64 }>::new();
        let writer = RingBuffer::new(&bytes).into_writer();
        let reader = RingBuffer::new(&bytes).into_reader();

        let mut claim = writer.claim(1, true);
        claim.get_buffer_mut().copy_from_slice(b"a");
        claim.commit();

        let mut claim = writer.claim(1, true);
        claim.get_buffer_mut().copy_from_slice(b"b");
        claim.commit();

        let mut claim = writer.claim(1, true);
        claim.get_buffer_mut().copy_from_slice(b"c");
        claim.commit();

        let msg = reader.receive_next().unwrap().unwrap();
        let mut payload = [0u8; 1];
        msg.read(&mut payload).unwrap();
        assert_eq!(b"a", &payload);

        let msg = reader.receive_next().unwrap().unwrap();
        let mut payload = [0u8; 1];
        msg.read(&mut payload).unwrap();
        assert_eq!(b"b", &payload);

        let msg = reader.receive_next().unwrap().unwrap();
        let mut payload = [0u8; 1];
        msg.read(&mut payload).unwrap();
        assert_eq!(b"c", &payload);

        assert!(reader.receive_next().is_none());
    }

    #[test]
    fn should_overrun_reader() {
        let bytes = AlignedBytes::<{ HEADER_SIZE + 64 }>::new();
        let writer = RingBuffer::new(&bytes).into_writer();
        let reader = RingBuffer::new(&bytes).into_reader();

        writer.claim(16, true).commit();
        writer.claim(16, true).commit();
        writer.claim(16, true).commit();
        writer.claim(16, true).commit();

        let msg = reader.receive_next().unwrap();
        assert!(matches!(msg.unwrap_err(), Error::Overrun(_)));
    }

    #[test]
    fn should_overrun_read_batch() {
        let bytes = AlignedBytes::<{ HEADER_SIZE + 64 }>::new();
        let writer = RingBuffer::new(&bytes).into_writer();
        let reader = RingBuffer::new(&bytes).into_reader();

        writer.claim(16, true).commit();
        writer.claim(16, true).commit();
        writer.claim(16, true).commit();
        writer.claim(16, true).commit();

        let mut iter = reader.read_batch().unwrap().into_iter();
        let msg = iter.next().unwrap();
        assert!(matches!(msg.unwrap_err(), Error::Overrun(_)));
    }

    #[test]
    #[should_panic(expected = "mtu exceeded")]
    fn should_error_if_mtu_exceeded() {
        let bytes = AlignedBytes::<{ HEADER_SIZE + 64 }>::new();
        let writer = RingBuffer::new(&bytes).into_writer();
        assert_eq!(24, writer.mtu());
        let _ = writer.claim(32, true);
    }

    #[test]
    fn should_start_read_from_last_producer_position() {
        let bytes = AlignedBytes::<{ HEADER_SIZE + 64 }>::new();
        let writer = RingBuffer::new(&bytes).into_writer();

        writer.claim(16, true).commit();

        assert_eq!(24, writer.position.get());
        assert_eq!(24, writer.index());

        let reader = RingBuffer::new(&bytes).into_reader();

        assert_eq!(reader.position.get(), writer.position.get());
        assert_eq!(reader.index(), writer.index());
    }

    #[test]
    fn should_pack_and_unpack_fields() {
        assert_eq!((true, true, true, true, 123), unpack_fields(pack_fields(true, true, true, true, 123)));
        assert_eq!((true, true, true, false, 123), unpack_fields(pack_fields(true, true, true, false, 123)));
        assert_eq!((true, true, false, false, 123), unpack_fields(pack_fields(true, true, false, false, 123)));
        assert_eq!((true, false, false, false, 123), unpack_fields(pack_fields(true, false, false, false, 123)));
        assert_eq!((false, false, false, false, 123), unpack_fields(pack_fields(false, false, false, false, 123)));
        assert_eq!((false, false, true, false, 123), unpack_fields(pack_fields(false, false, true, false, 123)));
        assert_eq!((false, true, false, true, 123), unpack_fields(pack_fields(false, true, false, true, 123)));
    }

    #[test]
    fn should_encode_and_decode_max_payload_len() {
        let frame = FrameHeader::new(MAX_PAYLOAD_LEN as u32, 0, true, false, false, false);
        assert!(frame.is_fin());
        assert!(!frame.is_continuation());
        assert!(!frame.is_padding());
        assert!(!frame.is_heartbeat());
        assert_eq!(MAX_PAYLOAD_LEN as u32, frame.payload_len());
        assert_eq!(268435455, MAX_PAYLOAD_LEN);
        assert_eq!(
            (true, true, true, true, MAX_PAYLOAD_LEN as u32),
            unpack_fields(pack_fields(true, true, true, true, MAX_PAYLOAD_LEN as u32))
        );
    }

    #[test]
    fn should_align_frame_header() {
        assert_eq!(8, align_of::<FrameHeader>());
        assert_eq!(8, size_of::<FrameHeader>());

        let frame = FrameHeader::new(10, 0, true, false, false, true);
        assert!(frame.is_fin());
        assert!(!frame.is_continuation());
        assert!(!frame.is_padding());
        assert!(frame.is_heartbeat());
        assert_eq!(10, frame.payload_len());

        let frame = FrameHeader::new(10, 0, true, false, true, false);
        assert!(frame.is_fin());
        assert!(!frame.is_continuation());
        assert!(frame.is_padding());
        assert!(!frame.is_heartbeat());
        assert_eq!(10, frame.payload_len());

        let frame = FrameHeader::new(0, 0, true, false, false, true);
        assert!(frame.is_fin());
        assert!(!frame.is_continuation());
        assert!(!frame.is_padding());
        assert!(frame.is_heartbeat());
        assert_eq!(0, frame.payload_len());

        let frame = FrameHeader::new(0, 0, true, false, true, false);
        assert!(frame.is_fin());
        assert!(!frame.is_continuation());
        assert!(frame.is_padding());
        assert!(!frame.is_heartbeat());
        assert_eq!(0, frame.payload_len());

        let frame = FrameHeader::new_padding();
        assert!(frame.is_fin());
        assert!(!frame.is_continuation());
        assert!(frame.is_padding());
        assert!(!frame.is_heartbeat());
        assert_eq!(0, frame.payload_len());
    }

    #[test]
    fn should_insert_padding() {
        let bytes = AlignedBytes::<{ HEADER_SIZE + 64 }>::new();
        let writer = RingBuffer::new(&bytes).into_writer();

        assert_eq!(0, writer.index());

        let claim = writer.claim(10, true);
        assert_eq!(10, claim.get_buffer().len());
        claim.commit();

        assert_eq!(24, writer.index());
        assert_eq!(40, writer.remaining());

        let claim = writer.claim(5, true);
        assert_eq!(5, claim.get_buffer().len());
        claim.commit();

        assert_eq!(40, writer.index());
        assert_eq!(24, writer.remaining());

        let claim = writer.claim(17, true);
        assert_eq!(17, claim.get_buffer().len());
        claim.commit();

        assert_eq!(32, writer.index());
        assert_eq!(32, writer.remaining());
    }

    #[test]
    fn should_ensure_frame_alignment() {
        let bytes = AlignedBytes::<{ HEADER_SIZE + 1024 }>::new();
        let writer = RingBuffer::new(&bytes).into_writer();

        let producer_position_addr = addr_of!(writer.ring.header().producer_position) as usize;
        let ready_addr = addr_of!(writer.ring.header().ready) as usize;
        let metadata_addr = addr_of!(writer.ring.header().metadata) as usize;
        let data_addr = writer.ring.header().data_ptr() as usize;

        assert_eq!(METADATA_BUFFER_SIZE, data_addr - metadata_addr);
        // 128 for x86_64, 64 for x86
        assert_eq!(align_of::<CachePadded<()>>(), metadata_addr - ready_addr);
        assert_eq!(align_of::<CachePadded<()>>(), ready_addr - producer_position_addr);

        let header_ptr = writer.ring.header() as *const Header;
        let data_ptr = writer.ring.header().data_ptr();

        let claim = writer.claim(16, true);
        let buf_ptr_0 = claim.get_buffer().as_ptr();
        let frame_ptr_0 = claim.writer.frame_header() as *const FrameHeader;

        assert_eq!(size_of::<Header>() + size_of::<FrameHeader>(), buf_ptr_0 as usize - header_ptr as usize);
        assert_eq!(size_of::<FrameHeader>(), buf_ptr_0 as usize - data_ptr as usize);
        assert_eq!(16, claim.get_buffer().len());
        #[cfg(target_arch = "x86_64")]
        assert_eq!(1280, size_of::<Header>());
        #[cfg(target_arch = "x86")]
        assert_eq!(1152, size_of::<Header>());
        assert_eq!(8, size_of::<FrameHeader>());
        assert_eq!(8, align_of::<FrameHeader>());
        assert_eq!(size_of::<Header>(), frame_ptr_0 as usize - bytes.as_ptr() as usize);

        claim.commit();

        let claim = writer.claim(13, true);
        let buf_ptr_1 = claim.get_buffer().as_ptr();
        let frame_ptr_1 = claim.writer.frame_header() as *const FrameHeader;

        assert_eq!(size_of::<Header>() + 24, frame_ptr_1 as usize - bytes.as_ptr() as usize);
        assert_eq!(16 + size_of::<FrameHeader>(), buf_ptr_1 as usize - buf_ptr_0 as usize);
        assert_eq!(16 + size_of::<FrameHeader>(), frame_ptr_1 as usize - frame_ptr_0 as usize);

        claim.commit();
    }

    #[test]
    fn should_construct_ring_buffer() {
        let bytes = AlignedBytes::<{ HEADER_SIZE + 64 }>::new();
        let rb = RingBuffer::new(&bytes);
        assert_eq!(0, rb.header().producer_position.load(SeqCst));
        assert_eq!(64, rb.capacity);
    }

    #[test]
    fn should_construct_ring_buffer_from_vec() {
        let bytes = avec![[128] | 0u8; HEADER_SIZE + (1024 * 1024 * 2)];
        let rb = RingBuffer::new(&bytes);
        assert_eq!(0, rb.header().producer_position.load(SeqCst));
        assert_eq!(2097152, rb.capacity);
        assert_eq!(1048568, rb.mtu);
    }

    #[test]
    fn should_read_message_into_vec() {
        let bytes = avec![[128] | 0u8; HEADER_SIZE + 1024];
        let writer = RingBuffer::new(&bytes).into_writer();
        let reader = RingBuffer::new(&bytes).into_reader();

        let mut claim = writer.claim(11, true);
        claim.get_buffer_mut().copy_from_slice(b"hello world");
        claim.commit();

        let mut iter = reader.read_batch().unwrap().into_iter();
        let msg = iter.next().unwrap().unwrap();

        let mut payload = vec![0u8; 1024];
        unsafe { payload.set_len(msg.payload_len) };
        msg.read(&mut payload).unwrap();

        assert_eq!(payload, b"hello world");
    }

    #[test]
    fn should_send_zero_size_message() {
        let bytes = AlignedBytes::<{ HEADER_SIZE + 64 }>::new();
        let writer = RingBuffer::new(&bytes).into_writer();
        let reader = RingBuffer::new(&bytes).into_reader();

        let claim = writer.claim(0, true);
        claim.commit();

        let msg = reader.receive_next().unwrap().unwrap();
        assert_eq!(0, msg.payload_len);
    }

    #[test]
    fn should_send_heartbeat() {
        let bytes = AlignedBytes::<{ HEADER_SIZE + 64 }>::new();
        let writer = RingBuffer::new(&bytes).into_writer();
        let reader = RingBuffer::new(&bytes).into_reader();

        let claim = writer.heartbeat();
        claim.commit();

        let msg = reader.receive_next().unwrap().unwrap();
        assert_eq!(0, msg.payload_len);
        assert!(msg.is_fin);
        assert!(!msg.is_continuation);
        assert!(!msg.is_padding);
    }

    #[test]
    fn should_abort_publication() {
        let bytes = AlignedBytes::<{ HEADER_SIZE + 64 }>::new();
        let writer = RingBuffer::new(&bytes).into_writer();
        assert_eq!(0, writer.position.get());

        let claim = writer.claim(16, true);
        claim.abort();
        assert_eq!(0, writer.position.get());

        let claim = writer.claim(24, true);
        claim.commit();
        assert_eq!(32, writer.position.get());

        let claim = writer.claim(8, true);
        claim.commit();
        assert_eq!(48, writer.position.get());

        let claim = writer.claim(16, true);
        claim.abort();
        assert_eq!(48, writer.position.get()); // wll rollback padding frame
    }

    #[test]
    fn should_attach_metadata() {
        let bytes = AlignedBytes::<{ HEADER_SIZE + 64 }>::new();
        let _ = RingBuffer::new(&bytes).into_writer_with_metadata(|metadata| {
            assert_eq!(METADATA_BUFFER_SIZE, metadata.len());
            metadata[0..11].copy_from_slice(b"hello world");
        });
        let reader = RingBuffer::new(&bytes).into_reader();
        assert_eq!(b"hello world", &reader.metadata()[..11]);
    }

    #[test]
    fn should_skip_padding_frame() {
        let bytes = AlignedBytes::<{ HEADER_SIZE + 64 }>::new();
        let mut buffer = [0u8; 1024];
        let writer = RingBuffer::new(&bytes).into_writer();
        let reader = RingBuffer::new(&bytes).into_reader();

        let claim = writer.claim_with_user_defined(24, true, 123);
        claim.commit();
        let msg = reader.receive_next().unwrap().unwrap();
        msg.read(&mut buffer).unwrap();
        assert!(!msg.is_padding);

        let claim = writer.claim(8, true);
        claim.commit();
        let msg = reader.receive_next().unwrap().unwrap();
        msg.read(&mut buffer).unwrap();
        assert!(!msg.is_padding);

        let claim = writer.claim(24, true);
        claim.commit();
        let msg = reader.receive_next().unwrap().unwrap();
        msg.read(&mut buffer).unwrap();
        assert!(!msg.is_padding);

        let msg = reader.receive_next();
        assert!(msg.is_none())
    }

    #[test]
    fn should_fragment_message() {
        let bytes = AlignedBytes::<{ HEADER_SIZE + 64 }>::new();
        let writer = RingBuffer::new(&bytes).into_writer();
        let reader = RingBuffer::new(&bytes).into_reader();

        let claim = writer.claim_with_user_defined(24, false, 123);
        claim.commit();
        let msg = reader.receive_next().unwrap().unwrap();
        assert!(!msg.is_fin);
        assert!(!msg.is_continuation);
        assert!(!msg.is_padding);
        assert_eq!(123, msg.user_defined); // only attached to the first frame

        let claim = writer.continuation(8, false);
        claim.commit();
        let msg = reader.receive_next().unwrap().unwrap();
        assert!(!msg.is_fin);
        assert!(msg.is_continuation);
        assert!(!msg.is_padding);
        assert_eq!(USER_DEFINED_NULL_VALUE, msg.user_defined);

        let claim = writer.continuation(24, true);
        claim.commit();
        let msg = reader.receive_next().unwrap().unwrap();
        assert!(msg.is_fin);
        assert!(msg.is_continuation);
        assert!(!msg.is_padding);
        assert_eq!(USER_DEFINED_NULL_VALUE, msg.user_defined);

        let msg = reader.receive_next();
        assert!(msg.is_none())
    }

    #[test]
    fn should_join_writer() {
        let bytes = AlignedBytes::<{ HEADER_SIZE + 1024 }>::new();

        // first writer will write from the beginning
        {
            let writer = RingBuffer::new(&bytes).into_writer();
            writer.claim_with_user_defined(16, true, 100).commit();
            writer.claim_with_user_defined(16, true, 101).commit();
            writer.claim_with_user_defined(16, true, 102).commit();
        }

        // second writer will pick up from the current position
        {
            let writer = RingBuffer::new(&bytes).join_writer();
            writer.claim_with_user_defined(16, true, 103).commit();
            writer.claim_with_user_defined(16, true, 104).commit();
            writer.claim_with_user_defined(16, true, 105).commit();
        }

        // verify we got all the messages
        let reader = RingBuffer::new(&bytes).into_reader().with_initial_position(0);
        assert_eq!(100, reader.receive_next().unwrap().unwrap().user_defined);
        assert_eq!(101, reader.receive_next().unwrap().unwrap().user_defined);
        assert_eq!(102, reader.receive_next().unwrap().unwrap().user_defined);
        assert_eq!(103, reader.receive_next().unwrap().unwrap().user_defined);
        assert_eq!(104, reader.receive_next().unwrap().unwrap().user_defined);
        assert_eq!(105, reader.receive_next().unwrap().unwrap().user_defined);
    }

    #[test]
    fn should_handle_position_wrap_around_if_no_overrun() {
        let bytes = AlignedBytes::<{ HEADER_SIZE + 2048 }>::new();
        let writer = RingBuffer::new(&bytes).into_writer_at(usize::MAX - 1023);
        // last claim before wrap around
        writer.claim_with_user_defined(1000, true, 100).commit();
        assert_eq!(usize::MAX - 15, writer.position.get());
        // first claim after wrap around, will insert padding frame and
        // continue from position zero
        writer.claim_with_user_defined(128, true, 101).commit();
        assert_eq!(136, writer.position.get());
        // a normal claim after wrap around
        writer.claim_with_user_defined(16, true, 102).commit();
        assert_eq!(160, writer.position.get());
        // verify we got all the messages
        let reader = RingBuffer::new(&bytes)
            .into_reader()
            .with_initial_position(usize::MAX - 1023);
        assert_eq!(100, reader.receive_next().unwrap().unwrap().user_defined);
        assert_eq!(101, reader.receive_next().unwrap().unwrap().user_defined);
        assert_eq!(102, reader.receive_next().unwrap().unwrap().user_defined);
        // and are still in sync
        assert_eq!(160, reader.position.get());
    }

    #[test]
    fn should_allow_reader_to_recover_from_overrun_when_position_wrapped_around() {
        let bytes = AlignedBytes::<{ HEADER_SIZE + 2048 }>::new();
        let writer = RingBuffer::new(&bytes).into_writer_at(usize::MAX - 2047);
        let reader = RingBuffer::new(&bytes).into_reader();

        // First claim and read
        writer.claim_with_user_defined(1000, true, 100).commit();
        assert_eq!(100, reader.receive_next().unwrap().unwrap().user_defined);

        // Last claim before wrap around
        writer.claim_with_user_defined(1000, true, 101).commit();

        // First claim after wrap around
        writer.claim_with_user_defined(512, true, 102).commit();

        // Overrun the reader and overwrite the header frame the reader will read
        let mut claim = writer.claim_with_user_defined(1000, true, 103);
        thread_rng().fill(claim.get_buffer_mut());
        claim.commit();

        assert!(matches!(reader.receive_next().unwrap().unwrap_err(), Error::Overrun(_)));
        // Reset the reader and start over
        reader.reset();
        assert!(reader.receive_next().is_none());
        // Continue writing and reading
        assert_eq!(reader.position.get(), writer.position.get());

        writer.claim_with_user_defined(1000, true, 104).commit();

        assert_eq!(104, reader.receive_next().unwrap().unwrap().user_defined);
    }
}
