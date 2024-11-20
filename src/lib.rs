mod error;

use crossbeam_utils::CachePadded;
use std::io::Read;
use std::mem::ManuallyDrop;
use std::ptr::{copy_nonoverlapping, slice_from_raw_parts, slice_from_raw_parts_mut, NonNull};
use std::sync::atomic::{AtomicUsize, Ordering};

// re-export
pub use error::Result;

pub const HEADER_SIZE: usize = size_of::<Header>();
const FRAME_HEADER_PADDING_MASK: u32 = 0x80000000;
const FRAME_HEADER_MSG_LEN_MASK: u32 = 0x7FFFFFFF;

#[derive(Debug)]
#[repr(C)]
struct Header {
    producer_position: CachePadded<AtomicUsize>, // will always increase
}

impl Header {
    #[inline]
    const fn data_ptr(&self) -> *const u8 {
        let header_ptr: *const Header = self;
        unsafe { header_ptr.add(1) as *const u8 }
    }

    #[inline]
    const fn data_ptr_mut(&self) -> *mut u8 {
        let header_ptr = self as *const Header as *mut Header;
        unsafe { header_ptr.add(1) as *mut u8 }
    }
}

#[repr(C, align(8))]
struct FrameHeader {
    fields: u32,       // contains padding flag and payload length
    user_defined: u32, // user defined field
}

impl FrameHeader {
    #[inline]
    const fn new(payload_len: u32, user_defined: u32, is_padding: bool) -> Self {
        let fields = (payload_len & FRAME_HEADER_MSG_LEN_MASK) | ((is_padding as u32) << 31);
        FrameHeader { fields, user_defined }
    }

    #[inline]
    const fn new_padding() -> Self {
        Self::new(0, 0, true)
    }

    #[inline]
    const fn is_padding(&self) -> bool {
        (self.fields & FRAME_HEADER_PADDING_MASK) != 0
    }

    #[inline]
    const fn payload_len(&self) -> u32 {
        self.fields & FRAME_HEADER_MSG_LEN_MASK
    }

    #[inline]
    const fn unpack_fields(&self) -> (u32, bool, u32) {
        let fields = self.fields;
        (fields & FRAME_HEADER_MSG_LEN_MASK, (fields & FRAME_HEADER_PADDING_MASK) != 0, self.user_defined)
    }

    // #[inline]
    // const fn get_payload_ptr(&self) -> *const FrameHeader {
    //     let message_header_ptr: *const FrameHeader = self;
    //     unsafe { message_header_ptr.add(1) }
    // }
    //
    // #[inline]
    // const fn payload(&self, size: usize) -> &[u8] {
    //     unsafe { std::slice::from_raw_parts(self.get_payload_ptr() as *mut u8, size) }
    // }
    //
    // #[inline]
    // const fn get_payload_ptr_mut(&self) -> *mut FrameHeader {
    //     let message_header_ptr = self as *const FrameHeader as *mut FrameHeader;
    //     unsafe { message_header_ptr.add(1) }
    // }
    //
    // #[inline]
    // fn payload_mut(&self, size: usize) -> &mut [u8] {
    //     unsafe { std::slice::from_raw_parts_mut(self.get_payload_ptr_mut() as *mut u8, size) }
    // }
}

#[inline]
const fn get_aligned_size(payload_length: u64) -> u64 {
    // Calculate the number of bytes needed to ensure frame header alignment
    const ALIGNMENT_MASK: u64 = align_of::<FrameHeader>() as u64 - 1;
    (payload_length + ALIGNMENT_MASK) & !ALIGNMENT_MASK
}

#[derive(Clone)]
struct RingBuffer {
    ptr: NonNull<Header>,
    capacity: usize,
    mtu: usize,
}

impl RingBuffer {
    fn new(bytes: &[u8]) -> Self {
        assert!(bytes.len() > size_of::<Header>(), "insufficient size for the header");
        assert!((bytes.len() - size_of::<Header>()).is_power_of_two(), "buffer len must be power of two");

        let header = bytes.as_ptr() as *mut Header;
        let capacity = bytes.len() - size_of::<Header>();
        Self {
            ptr: NonNull::new(header).unwrap(),
            capacity,
            mtu: capacity / 2 - size_of::<FrameHeader>(),
        }
    }

    #[inline]
    const fn header(&self) -> &'static Header {
        unsafe { self.ptr.as_ref() }
    }

    #[inline]
    fn header_mut(&mut self) -> &'static mut Header {
        unsafe { self.ptr.as_mut() }
    }

    fn into_writer(self) -> Writer {
        Writer {
            ring: self,
            position: 0,
        }
    }

    fn into_reader(self) -> Reader {
        let producer_position = self.header().producer_position.load(Ordering::SeqCst);
        Reader {
            ring: self,
            position: producer_position,
        }
    }
}

struct Writer {
    ring: RingBuffer,
    position: usize,
}

impl From<RingBuffer> for Writer {
    fn from(ring: RingBuffer) -> Self {
        ring.into_writer()
    }
}

struct Claim<'a> {
    writer: &'a mut Writer,
    len: usize,
    limit: usize,
    is_padding: bool,
}

impl<'a> Claim<'a> {
    #[inline]
    const fn get_buffer(&self) -> &[u8] {
        unsafe {
            let ptr = self.writer.ring.header().data_ptr();
            &*slice_from_raw_parts(ptr.add(self.writer.index() + size_of::<FrameHeader>()), self.limit)
        }
    }

    #[inline]
    fn get_buffer_mut(&mut self) -> &mut [u8] {
        unsafe {
            let ptr = self.writer.ring.header_mut().data_ptr_mut();
            &mut *slice_from_raw_parts_mut(ptr.add(self.writer.index() + size_of::<FrameHeader>()), self.limit)
        }
    }

    #[inline]
    const fn frame_header(&self) -> &FrameHeader {
        unsafe {
            let ptr = self.writer.ring.header().data_ptr();
            &*(ptr.add(self.writer.index()) as *const FrameHeader)
        }
    }

    #[inline]
    fn frame_header_mut(&mut self) -> &mut FrameHeader {
        unsafe {
            let ptr = self.writer.ring.header_mut().data_ptr_mut();
            &mut *(ptr.add(self.writer.index()) as *mut FrameHeader)
        }
    }

    #[inline]
    fn commit(self) {
        // we need to ensure the destructor will not be called in this case
        ManuallyDrop::new(self).commit_impl();
    }

    #[inline]
    fn commit_impl(&mut self) {
        // update frame header
        let fields = (self.limit as u32 & FRAME_HEADER_MSG_LEN_MASK) | ((self.is_padding as u32) << 31);
        let header = self.frame_header_mut();
        header.fields = fields;

        // update header
        let previous_position = self
            .writer
            .ring
            .header()
            .producer_position
            .fetch_add(self.len + size_of::<FrameHeader>(), Ordering::Release);
        self.writer.position = previous_position + self.len + size_of::<FrameHeader>();
    }
}

impl<'a> Drop for Claim<'a> {
    fn drop(&mut self) {
        self.commit_impl();
    }
}

impl Writer {
    #[inline]
    pub fn claim(&mut self, len: usize) -> Result<Claim> {
        if len > self.ring.mtu {
            return Err(error::Error::MtuLimitExceeded(len, self.ring.mtu));
        }

        let remaining = self.remaining();
        let aligned_len = get_aligned_size(len as u64);

        // insert padding frame if required
        if aligned_len as usize + size_of::<FrameHeader>() > remaining {
            let _ = Claim {
                writer: self,
                len: remaining - size_of::<FrameHeader>(),
                limit: 0,
                is_padding: true,
            };
        }

        Ok(Claim {
            writer: self,
            len: aligned_len as usize,
            limit: len,
            is_padding: false,
        })
    }

    #[inline]
    const fn index(&self) -> usize {
        self.position & (self.ring.capacity - 1)
    }

    #[inline]
    const fn remaining(&self) -> usize {
        self.ring.capacity - self.index()
    }
}

struct Reader {
    ring: RingBuffer,
    position: usize, // local position
}

#[derive(Debug, Clone)]
struct Message {
    header: &'static Header,
    position: usize,    // marks beginning of message payload
    capacity: usize,    // ring buffer capacity
    payload_len: usize, // message length
    is_padding: bool,   // indicates padding frame
}

impl Message {
    #[inline]
    pub const fn len(&self) -> usize {
        self.payload_len
    }

    #[inline]
    pub const fn is_padding(&self) -> bool {
        self.is_padding
    }

    #[inline]
    const fn index(&self) -> usize {
        self.position & (self.capacity - 1)
    }

    /// Read the message into specified buffer. It will return error if the provided buffer
    /// is too small. Will also return error if at any point the producer has overrun the reader.
    /// On success, it will return the number of bytes written to the buffer.
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

struct BatchIter<'a> {
    reader: &'a mut Reader,
    remaining: usize, // remaining bytes to consume
}

// msg_type: u32
// payload_length: u32

// packed as u64?
// use u32::MAX as PADDING_FRAME -> check for reserved (cold)
// use 0 as not UNSPECIFIED

impl<'a> Iterator for BatchIter<'a> {
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

impl<'a> BatchIter<'a> {
    fn receive_next(&mut self) -> Option<Result<Message>> {
        // we reached the batch limit
        if self.remaining == 0 {
            return None;
        }

        // extract frame header fields
        let producer_position_before = self.reader.position;
        let frame_header = self.reader.as_frame_header();
        let (payload_len, is_padding, _) = frame_header.unpack_fields();
        let producer_position_after = self.reader.ring.header().producer_position.load(Ordering::Acquire);

        // ensure we have not been overrun
        if producer_position_after > producer_position_before + self.reader.ring.capacity {
            return Some(Err(error::Error::Overrun(self.reader.position)));
        }

        let message = Message {
            header: self.reader.ring.header(),
            position: self.reader.position + size_of::<FrameHeader>(),
            payload_len: payload_len as usize,
            capacity: self.reader.ring.capacity,
            is_padding,
        };

        let aligned_payload_len = get_aligned_size(message.len() as u64) as usize;
        self.remaining -= aligned_payload_len + size_of::<FrameHeader>();
        self.reader.position += aligned_payload_len + size_of::<FrameHeader>();

        Some(Ok(message))
    }
}

impl Reader {
    #[inline]
    const fn as_frame_header(&self) -> &FrameHeader {
        unsafe { &*(self.ring.header().data_ptr().add(self.index()) as *const FrameHeader) }
    }

    #[inline]
    const fn index(&self) -> usize {
        self.position & (self.ring.capacity - 1)
    }

    #[inline]
    pub fn batch_iter(&mut self) -> BatchIter {
        let producer_position = self.ring.header().producer_position.load(Ordering::Acquire);
        let limit = producer_position - self.position;
        BatchIter {
            reader: self,
            remaining: limit,
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
        let bytes = CachePadded::new([0u8; size_of::<Header>() + 64]);
        let mut writer = RingBuffer::new(&*bytes).into_writer();
        let mut reader = RingBuffer::new(&*bytes).into_reader();

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
        let payload = &payload[..msg.len()];
        assert_eq!(payload, b"hello");

        let msg = iter.next().unwrap().unwrap();
        let mut payload = [0u8; 1024];
        msg.read(&mut payload).unwrap();
        let payload = &payload[..msg.len()];
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
        let payload = &payload[..msg.len()];
        assert_eq!(payload, b"test");

        assert!(iter.next().is_none());

        assert_eq!(reader.index(), writer.index());
        assert_eq!(reader.position, writer.position);
    }

    #[test]
    fn should_overrun_reader() {
        let bytes = CachePadded::new([0u8; HEADER_SIZE + 64]);
        let mut writer = RingBuffer::new(&*bytes).into_writer();
        let mut reader = RingBuffer::new(&*bytes).into_reader();

        writer.claim(16).unwrap().commit();
        writer.claim(16).unwrap().commit();
        writer.claim(16).unwrap().commit();
        writer.claim(16).unwrap().commit();

        let mut iter = reader.batch_iter();
        let msg = iter.next().unwrap();
        assert!(matches!(msg.unwrap_err(), Error::Overrun(_)));
    }

    #[test]
    fn should_start_read_from_last_producer_position() {
        let bytes = CachePadded::new([0u8; HEADER_SIZE + 64]);
        let mut writer = RingBuffer::new(&*bytes).into_writer();

        writer.claim(16).unwrap().commit();

        assert_eq!(24, writer.position);
        assert_eq!(24, writer.index());

        let reader = RingBuffer::new(&*bytes).into_reader();

        assert_eq!(reader.position, writer.position);
        assert_eq!(reader.index(), writer.index());
    }

    #[test]
    fn frame_header() {
        assert_eq!(8, align_of::<FrameHeader>());

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

        assert_eq!(0, get_aligned_size(0));
        assert_eq!(8, get_aligned_size(3));
        assert_eq!(8, get_aligned_size(8));
        assert_eq!(16, get_aligned_size(13));
        assert_eq!(16, get_aligned_size(16));
    }

    #[test]
    fn should_insert_padding() {
        let bytes = CachePadded::new([0u8; HEADER_SIZE + 64]);
        let mut writer = RingBuffer::new(&*bytes).into_writer();

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
        let bytes = CachePadded::new([0u8; size_of::<Header>() + 1024]);
        let mut writer = RingBuffer::new(&*bytes).into_writer();

        let header_ptr = writer.ring.header() as *const Header;
        let data_ptr = writer.ring.header().data_ptr();

        let claim = writer.claim(16).unwrap();
        let buf_ptr_0 = claim.get_buffer().as_ptr();
        let frame_ptr_0 = claim.frame_header() as *const FrameHeader;

        println!("buffer address: {:p}", buf_ptr_0);
        println!("buffer address: {:#x}", buf_ptr_0 as usize);

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
        let frame_ptr_1 = claim.frame_header() as *const FrameHeader;

        assert_eq!(size_of::<Header>() + 24, frame_ptr_1 as usize - bytes.as_ptr() as usize);
        assert_eq!(16 + size_of::<FrameHeader>(), buf_ptr_1 as usize - buf_ptr_0 as usize);
        assert_eq!(16 + size_of::<FrameHeader>(), frame_ptr_1 as usize - frame_ptr_0 as usize);

        claim.commit();
    }

    #[test]
    fn should_construct_ring_buffer() {
        let data = CachePadded::new([0u8; HEADER_SIZE + 1024]);
        let rb = RingBuffer::new(&*data);
        assert_eq!(0, rb.header().producer_position.load(SeqCst));
        assert_eq!(1024, rb.capacity);
    }
}
