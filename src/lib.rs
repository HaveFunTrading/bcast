//! Low latency, single producer & many consumer (SPMC) ring buffer that works with in-process
//! and interprocess memory. Natively supports variable message sizes.
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
//! let mut claim = writer.claim(5).unwrap();
//! claim.get_buffer_mut().copy_from_slice(b"hello");
//! claim.commit();
//!
//! // publish second message
//! let mut claim = writer.claim(5).unwrap();
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
//! let mut iter = reader.batch_iter();
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

use crossbeam_utils::CachePadded;
use std::cmp::min;
use std::mem::ManuallyDrop;
use std::ptr::{copy_nonoverlapping, NonNull};
use std::sync::atomic::{AtomicUsize, Ordering};

use std::mem::align_of;
use std::mem::size_of;

// re-export
pub use error::Result;

/// Ring buffer header size.
pub const HEADER_SIZE: usize = size_of::<Header>();
/// Null value for `user_defined` field.
pub const USER_DEFINED_NULL_VALUE: u32 = u32::MAX;
const FRAME_HEADER_PADDING_MASK: u32 = 0x80000000;
const FRAME_HEADER_MSG_LEN_MASK: u32 = 0x7FFFFFFF;

/// Ring buffer header that contains producer position. The position is expressed in bytes and
/// will always increment.
#[derive(Debug)]
#[repr(C)]
struct Header {
    producer_position: CachePadded<AtomicUsize>, // will always increase
}

impl Header {
    /// Get pointer to the data section of the ring buffer.
    #[inline]
    const fn data_ptr(&self) -> *const u8 {
        let header_ptr: *const Header = self;
        unsafe { header_ptr.add(1) as *const u8 }
    }

    /// Get mutable pointer to the data section of the ring buffer.
    #[inline]
    const fn data_ptr_mut(&self) -> *mut u8 {
        let header_ptr = self as *const Header as *mut Header;
        unsafe { header_ptr.add(1) as *mut u8 }
    }
}

/// Message frame header that contains `fields` (packed `padding_flag` and `payload_length`)
/// and `user_defined` field.
#[repr(C, align(8))]
struct FrameHeader {
    fields: u32,       // contains padding flag and payload length
    user_defined: u32, // user defined field
}

impl FrameHeader {
    #[inline]
    #[cfg(test)]
    const fn new(payload_len: u32, user_defined: u32, is_padding: bool) -> Self {
        let fields = (payload_len & FRAME_HEADER_MSG_LEN_MASK) | ((is_padding as u32) << 31);
        FrameHeader { fields, user_defined }
    }

    #[inline]
    #[cfg(test)]
    const fn new_padding() -> Self {
        Self::new(0, 0, true)
    }

    #[inline]
    #[cfg(test)]
    const fn is_padding(&self) -> bool {
        (self.fields & FRAME_HEADER_PADDING_MASK) != 0
    }

    #[inline]
    #[cfg(test)]
    const fn payload_len(&self) -> u32 {
        self.fields & FRAME_HEADER_MSG_LEN_MASK
    }

    /// Extract `payload_length`, `is_padding` and `user_defined` fields from the message header.
    #[inline]
    const fn unpack_fields(&self) -> (u32, bool, u32) {
        let fields = self.fields;
        (fields & FRAME_HEADER_MSG_LEN_MASK, (fields & FRAME_HEADER_PADDING_MASK) != 0, self.user_defined)
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

    /// Get message payload as byte slice.
    #[inline]
    const fn payload(&self, size: usize) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.get_payload_ptr() as *const u8, size) }
    }

    /// Get message payload as mutable byte slice.
    #[inline]
    const fn payload_mut(&mut self, size: usize) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.get_payload_ptr_mut() as *mut u8, size) }
    }
}

/// Calculate the number of bytes needed to ensure frame header alignment of the payload.
#[inline]
const fn get_aligned_size(payload_length: usize) -> usize {
    const ALIGNMENT_MASK: usize = align_of::<FrameHeader>() - 1;
    (payload_length + ALIGNMENT_MASK) & !ALIGNMENT_MASK
}

/// Single producer, many consumer (SPMC) ring buffer backed by some 'shared' memory.
#[derive(Debug, Clone)]
pub struct RingBuffer {
    ptr: NonNull<Header>,
    capacity: usize,
    mtu: usize,
}

impl RingBuffer {
    /// Create new ``RingBuffer` by wrapping provided `bytes`. It is necessary to call `into_writer()`
    /// or `into_reader()` following the buffer construction to start using it.
    pub fn new(bytes: &[u8]) -> Self {
        assert!(bytes.len() > size_of::<Header>(), "insufficient size for the header");
        assert!((bytes.len() - size_of::<Header>()).is_power_of_two(), "buffer len must be power of two");

        // represents the max value we can encode on the frame header for the payload length
        const MAX_PAYLOAD_LEN: usize = (1 << 31) - 1;

        let header = bytes.as_ptr() as *mut Header;
        let capacity = bytes.len() - size_of::<Header>();
        Self {
            ptr: NonNull::new(header).unwrap(),
            capacity,
            mtu: min(capacity / 2 - size_of::<FrameHeader>(), MAX_PAYLOAD_LEN),
        }
    }

    /// Get reference to ring buffer header.
    #[inline]
    const fn header(&self) -> &'static Header {
        unsafe { self.ptr.as_ref() }
    }

    /// Get mutable reference to ring buffer header.
    #[inline]
    const fn header_mut(&mut self) -> &'static mut Header {
        unsafe { self.ptr.as_mut() }
    }

    /// Will consume `self` and return instance of writer backed by this ring buffer.
    pub fn into_writer(self) -> Writer {
        Writer {
            ring: self,
            position: 0,
        }
    }

    /// Will consume `self` and return instance of reader backed by this ring buffer. The reader
    /// position will be set to producer most up-to-date position.
    pub fn into_reader(self) -> Reader {
        let producer_position = self.header().producer_position.load(Ordering::SeqCst);
        Reader {
            ring: self,
            position: producer_position,
        }
    }
}

/// Wraps`RingBuffer` and allows to publish messages. Only single writer should be present at any time.
#[derive(Debug)]
pub struct Writer {
    ring: RingBuffer,
    position: usize, // local producer position
}

impl From<RingBuffer> for Writer {
    fn from(ring: RingBuffer) -> Self {
        ring.into_writer()
    }
}

impl Writer {
    /// Claim part of the underlying `RingBuffer` for publication. This operation will always succeed (since
    /// producer can overrun consumers) except for the case when the message `mtu` limit has been exceeded.
    #[inline]
    pub fn claim(&mut self, len: usize) -> Result<Claim> {
        self.claim_with_user_defined(len, USER_DEFINED_NULL_VALUE)
    }

    /// Claim part of the underlying `RingBuffer` for publication also passing `user_defined` value
    /// that will be attached to the message. This operation will always succeed (since
    /// producer can overrun consumers) except for the case when the message `mtu` limit has been exceeded.
    #[inline]
    pub fn claim_with_user_defined(&mut self, len: usize, user_defined: u32) -> Result<Claim> {
        let aligned_len = get_aligned_size(len);
        if aligned_len > self.ring.mtu {
            return Err(error::Error::MtuLimitExceeded(len, self.ring.mtu));
        }
        Ok(Claim::new(self, aligned_len, len, user_defined))
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
            let ptr = self.ring.header_mut().data_ptr_mut();
            &mut *(ptr.add(self.index()) as *mut FrameHeader)
        }
    }
}

/// Represents region of the RingBuffer` we can publish message to.
#[derive(Debug)]
pub struct Claim<'a> {
    writer: &'a mut Writer, // underlying writer
    len: usize,             // frame header aligned payload length
    limit: usize,           // actual payload length
    user_defined: u32,      // user defined field
}

impl<'a> Claim<'a> {
    /// Create new claim.
    #[inline]
    const fn new(writer: &'a mut Writer, len: usize, limit: usize, user_defined: u32) -> Self {
        // insert padding frame if required
        let remaining = writer.remaining();
        if len + size_of::<FrameHeader>() > remaining {
            let padding_len = remaining - size_of::<FrameHeader>();
            let fields = (padding_len as u32 & FRAME_HEADER_MSG_LEN_MASK) | ((true as u32) << 31);

            let header = writer.frame_header_mut();
            header.fields = fields;
            header.user_defined = USER_DEFINED_NULL_VALUE;
            writer.position += padding_len + size_of::<FrameHeader>();
        };

        Self {
            writer,
            len,
            limit,
            user_defined,
        }
    }

    /// Get next message payload as byte slice.
    #[inline]
    pub const fn get_buffer(&self) -> &[u8] {
        self.writer.frame_header().payload(self.limit)
    }

    /// Get next message payload as mutable byte slice.
    #[inline]
    pub const fn get_buffer_mut(&mut self) -> &mut [u8] {
        self.writer.frame_header_mut().payload_mut(self.limit)
    }

    /// Commit the message thus making it visible to other consumers. If this operation is not
    /// invoked the commit will happen when `Claim` is dropped.
    #[inline]
    pub fn commit(self) {
        // we need to ensure the destructor will not be called in this case
        ManuallyDrop::new(self).commit_impl();
    }

    #[inline]
    fn commit_impl(&mut self) {
        // update frame header
        let header = self.writer.frame_header_mut();
        let fields = (self.limit as u32 & FRAME_HEADER_MSG_LEN_MASK) | ((false as u32) << 31);
        header.fields = fields;
        header.user_defined = self.user_defined;

        // advance writer position
        self.writer.position += self.len + size_of::<FrameHeader>();

        // signal updated producer position
        self.writer
            .ring
            .header()
            .producer_position
            .store(self.writer.position, Ordering::Release);
    }
}

impl Drop for Claim<'_> {
    fn drop(&mut self) {
        self.commit_impl();
    }
}

/// Wraps`RingBuffer` and allows to receive messages. Multiple readers can be present at any time,
/// that operate independently and are not part of any congestion control flow. As a result, each reader
/// can be overrun by the producer if it's unable to keep up.
pub struct Reader {
    ring: RingBuffer,
    position: usize, // local position that will always increase
}

impl Reader {
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

    /// Construct iterator object that can efficiently read multiple messages in a batch between
    /// `Reader` current position and prevailing producer position.
    #[inline]
    pub fn batch_iter(&mut self) -> BatchIter {
        let producer_position = self.ring.header().producer_position.load(Ordering::Acquire);
        let limit = producer_position - self.position;
        let position_snapshot = self.position;
        BatchIter {
            reader: self,
            remaining: limit,
            position_snapshot,
        }
    }

    /// Receive next pending message from the ring buffer.
    #[inline]
    pub fn receive_next(&mut self) -> Option<Result<Message>> {
        let producer_position_before = self.ring.header().producer_position.load(Ordering::Acquire);
        // no new messages
        if producer_position_before - self.position == 0 {
            return None;
        }
        self.receive_next_impl(producer_position_before)
    }

    #[inline]
    fn receive_next_impl(&mut self, producer_position_before: usize) -> Option<Result<Message>> {
        // extract frame header fields
        let frame_header = self.as_frame_header();
        let (payload_len, is_padding, user_defined) = frame_header.unpack_fields();
        let producer_position_after = self.ring.header().producer_position.load(Ordering::Acquire);

        // ensure we have not been overrun
        if producer_position_after > producer_position_before + self.ring.capacity {
            return Some(Err(error::Error::Overrun(self.position)));
        }

        // construct the massage
        let message = Message {
            header: self.ring.header(),
            position: self.position + size_of::<FrameHeader>(),
            payload_len: payload_len as usize,
            capacity: self.ring.capacity,
            is_padding,
            user_defined,
        };

        // update reader position
        let aligned_payload_len = get_aligned_size(message.payload_len);
        self.position += aligned_payload_len + size_of::<FrameHeader>();

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
    /// Indicates padding frame.
    pub is_padding: bool,
    /// User defined field.
    pub user_defined: u32,
}

impl Message {
    /// Buffer index at which read will happen.
    #[inline]
    const fn index(&self) -> usize {
        self.position & (self.capacity - 1)
    }

    /// Read the message into specified buffer. It will return error if the provided buffer
    /// is too small. Will also return error if at any point the producer has overrun the reader.
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
        if self.payload_len > buf.len() {
            return Err(error::Error::InsufficientBufferSize(buf.len(), self.payload_len));
        }

        // attempt to copy message data into provided buffer
        let producer_position_before = self.position;
        unsafe {
            copy_nonoverlapping(self.header.data_ptr().add(self.index()), buf.as_mut_ptr(), self.payload_len);
        }
        let producer_position_after = self.header.producer_position.load(Ordering::Acquire);

        // ensure we have not been overrun by the producer
        match producer_position_after > producer_position_before + self.capacity {
            true => Err(error::Error::Overrun(self.position)),
            false => Ok(self.payload_len),
        }
    }
}

/// Iterator that allows to process pending messages in a batch. This is more efficient than iterating
/// over the messages than using `receive_next()` on the Reader` directly.
pub struct BatchIter<'a> {
    reader: &'a mut Reader,
    remaining: usize,         // remaining bytes to consume
    position_snapshot: usize, // reader position snapshot
}

impl Iterator for BatchIter<'_> {
    type Item = Result<Message>;

    fn next(&mut self) -> Option<Self::Item> {
        // attempt to receive next frame
        // if the frame is padding will skip it and attempt to return next frame
        match self.receive_next() {
            None => None,
            Some(msg) => match msg {
                Ok(msg) if !msg.is_padding => Some(Ok(msg)),
                Ok(_) => self.receive_next(),
                Err(err) => Some(Err(err)),
            },
        }
    }
}

impl BatchIter<'_> {
    #[inline]
    fn receive_next(&mut self) -> Option<Result<Message>> {
        // we reached the batch limit
        if self.remaining == 0 {
            return None;
        }
        // update iterator with the number of bytes received
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use std::sync::atomic::Ordering::SeqCst;

    #[test]
    fn should_read_messages_in_batch() {
        let bytes = [0u8; HEADER_SIZE + 64];
        let mut writer = RingBuffer::new(&bytes).into_writer();
        let mut reader = RingBuffer::new(&bytes).into_reader();

        let mut claim = writer.claim(5).unwrap();
        claim.get_buffer_mut().copy_from_slice(b"hello");
        claim.commit();

        let mut claim = writer.claim(5).unwrap();
        claim.get_buffer_mut().copy_from_slice(b"world");
        claim.commit();

        let mut iter = reader.batch_iter();

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
        assert_eq!(32, iter.reader.index());

        assert!(iter.next().is_none());

        assert_eq!(32, iter.reader.index());
        assert_eq!(32, iter.reader.position);
        assert_eq!(32, writer.position);
        assert_eq!(32, writer.remaining());

        let claim = writer.claim(15).unwrap();
        claim.commit();

        assert_eq!(56, writer.index());
        assert_eq!(56, writer.position);

        let mut claim = writer.claim(4).unwrap();
        claim.get_buffer_mut().copy_from_slice(b"test");
        claim.commit();

        let mut iter = reader.batch_iter();

        // skip big message
        let _ = iter.next().unwrap().unwrap();
        let msg = iter.next().unwrap().unwrap();

        let mut payload = [0u8; 1024];
        msg.read(&mut payload).unwrap();
        let payload = &payload[..msg.payload_len];
        assert_eq!(payload, b"test");

        assert!(iter.next().is_none());

        assert_eq!(reader.index(), writer.index());
        assert_eq!(reader.position, writer.position);
    }

    #[test]
    fn should_read_in_batch_with_limit() {
        let bytes = [0u8; HEADER_SIZE + 64];
        let mut writer = RingBuffer::new(&bytes).into_writer();
        let mut reader = RingBuffer::new(&bytes).into_reader();

        let mut claim = writer.claim(1).unwrap();
        claim.get_buffer_mut().copy_from_slice(b"a");
        claim.commit();

        let mut claim = writer.claim(1).unwrap();
        claim.get_buffer_mut().copy_from_slice(b"b");
        claim.commit();

        let mut claim = writer.claim(1).unwrap();
        claim.get_buffer_mut().copy_from_slice(b"c");
        claim.commit();

        let mut iter = reader.batch_iter().take(2);

        let msg = iter.next().unwrap().unwrap();
        let mut payload = [0u8; 1];
        msg.read(&mut payload).unwrap();
        assert_eq!(b"a", &payload);

        let msg = iter.next().unwrap().unwrap();
        let mut payload = [0u8; 1];
        msg.read(&mut payload).unwrap();
        assert_eq!(b"b", &payload);

        assert!(iter.next().is_none());

        let mut iter = reader.batch_iter();

        let msg = iter.next().unwrap().unwrap();
        let mut payload = [0u8; 1];
        msg.read(&mut payload).unwrap();
        assert_eq!(b"c", &payload);

        assert!(iter.next().is_none());
    }

    #[test]
    fn should_read_next_message() {
        let bytes = [0u8; HEADER_SIZE + 64];
        let mut writer = RingBuffer::new(&bytes).into_writer();
        let mut reader = RingBuffer::new(&bytes).into_reader();

        let mut claim = writer.claim(1).unwrap();
        claim.get_buffer_mut().copy_from_slice(b"a");
        claim.commit();

        let mut claim = writer.claim(1).unwrap();
        claim.get_buffer_mut().copy_from_slice(b"b");
        claim.commit();

        let mut claim = writer.claim(1).unwrap();
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
        let bytes = [0u8; HEADER_SIZE + 64];
        let mut writer = RingBuffer::new(&bytes).into_writer();
        let mut reader = RingBuffer::new(&bytes).into_reader();

        writer.claim(16).unwrap().commit();
        writer.claim(16).unwrap().commit();
        writer.claim(16).unwrap().commit();
        writer.claim(16).unwrap().commit();

        let mut iter = reader.batch_iter();
        let msg = iter.next().unwrap();
        assert!(matches!(msg.unwrap_err(), Error::Overrun(_)));
    }

    #[test]
    fn should_error_if_mtu_exceeded() {
        let bytes = [0u8; HEADER_SIZE + 64];
        let mut writer = RingBuffer::new(&bytes).into_writer();
        assert_eq!(24, writer.mtu());
        let claim = writer.claim(32);
        assert!(claim.is_err());
        assert_eq!(Error::MtuLimitExceeded(32, 24), claim.unwrap_err());
    }

    #[test]
    fn should_start_read_from_last_producer_position() {
        let bytes = [0u8; HEADER_SIZE + 64];
        let mut writer = RingBuffer::new(&bytes).into_writer();

        writer.claim(16).unwrap().commit();

        assert_eq!(24, writer.position);
        assert_eq!(24, writer.index());

        let reader = RingBuffer::new(&bytes).into_reader();

        assert_eq!(reader.position, writer.position);
        assert_eq!(reader.index(), writer.index());
    }

    #[test]
    fn should_align_frame_header() {
        assert_eq!(8, align_of::<FrameHeader>());
        assert_eq!(8, size_of::<FrameHeader>());

        let frame = FrameHeader::new(10, 0, false);
        assert!(!frame.is_padding());
        assert_eq!(10, frame.payload_len());

        let frame = FrameHeader::new(10, 0, true);
        assert!(frame.is_padding());
        assert_eq!(10, frame.payload_len());

        let frame = FrameHeader::new(0, 0, false);
        assert!(!frame.is_padding());
        assert_eq!(0, frame.payload_len());

        let frame = FrameHeader::new(0, 0, true);
        assert!(frame.is_padding());
        assert_eq!(0, frame.payload_len());

        let frame = FrameHeader::new_padding();
        assert!(frame.is_padding());
        assert_eq!(0, frame.payload_len());
    }

    #[test]
    fn should_insert_padding() {
        let bytes = [0u8; HEADER_SIZE + 64];
        let mut writer = RingBuffer::new(&bytes).into_writer();

        assert_eq!(0, writer.index());

        let claim = writer.claim(10).unwrap();
        assert_eq!(10, claim.get_buffer().len());
        claim.commit();

        assert_eq!(24, writer.index());
        assert_eq!(40, writer.remaining());

        let claim = writer.claim(5).unwrap();
        assert_eq!(5, claim.get_buffer().len());
        claim.commit();

        assert_eq!(40, writer.index());
        assert_eq!(24, writer.remaining());

        let claim = writer.claim(17).unwrap();
        assert_eq!(17, claim.get_buffer().len());
        claim.commit();

        assert_eq!(32, writer.index());
        assert_eq!(32, writer.remaining());
    }

    #[test]
    fn should_ensure_frame_alignment() {
        let bytes = [0u8; HEADER_SIZE + 1024];
        let mut writer = RingBuffer::new(&bytes).into_writer();

        let header_ptr = writer.ring.header() as *const Header;
        let data_ptr = writer.ring.header().data_ptr();

        let claim = writer.claim(16).unwrap();
        let buf_ptr_0 = claim.get_buffer().as_ptr();
        let frame_ptr_0 = claim.writer.frame_header() as *const FrameHeader;

        assert_eq!(size_of::<Header>() + size_of::<FrameHeader>(), buf_ptr_0 as usize - header_ptr as usize);
        assert_eq!(size_of::<FrameHeader>(), buf_ptr_0 as usize - data_ptr as usize);
        assert_eq!(16, claim.get_buffer().len());
        assert_eq!(128, size_of::<Header>());
        assert_eq!(8, size_of::<FrameHeader>());
        assert_eq!(8, align_of::<FrameHeader>());
        assert_eq!(size_of::<Header>(), frame_ptr_0 as usize - bytes.as_ptr() as usize);

        claim.commit();

        let claim = writer.claim(13).unwrap();
        let buf_ptr_1 = claim.get_buffer().as_ptr();
        let frame_ptr_1 = claim.writer.frame_header() as *const FrameHeader;

        assert_eq!(size_of::<Header>() + 24, frame_ptr_1 as usize - bytes.as_ptr() as usize);
        assert_eq!(16 + size_of::<FrameHeader>(), buf_ptr_1 as usize - buf_ptr_0 as usize);
        assert_eq!(16 + size_of::<FrameHeader>(), frame_ptr_1 as usize - frame_ptr_0 as usize);

        claim.commit();
    }

    #[test]
    fn should_construct_ring_buffer() {
        let bytes = [0u8; HEADER_SIZE + 64];
        let rb = RingBuffer::new(&bytes);
        assert_eq!(0, rb.header().producer_position.load(SeqCst));
        assert_eq!(64, rb.capacity);
    }

    #[test]
    fn should_construct_ring_buffer_from_vec() {
        let bytes = vec![0u8; HEADER_SIZE + 64];
        let rb = RingBuffer::new(&bytes);
        assert_eq!(0, rb.header().producer_position.load(SeqCst));
        assert_eq!(64, rb.capacity);
    }

    #[test]
    fn should_read_message_into_vec() {
        let bytes = vec![0u8; HEADER_SIZE + 1024];
        let mut writer = RingBuffer::new(&bytes).into_writer();
        let mut reader = RingBuffer::new(&bytes).into_reader();

        let mut claim = writer.claim(11).unwrap();
        claim.get_buffer_mut().copy_from_slice(b"hello world");
        claim.commit();

        let mut iter = reader.batch_iter();
        let msg = iter.next().unwrap().unwrap();

        let mut payload = vec![0u8; 1024];
        unsafe { payload.set_len(msg.payload_len) };
        msg.read(&mut payload).unwrap();

        assert_eq!(payload, b"hello world");
    }
}
