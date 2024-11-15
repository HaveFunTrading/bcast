use crossbeam_utils::CachePadded;
use std::io::Write;
use std::mem::ManuallyDrop;
use std::ptr::{slice_from_raw_parts, slice_from_raw_parts_mut, NonNull};
use std::sync::atomic::{AtomicUsize, Ordering};

const FRAME_HEADER_PADDING_MASK: u32 = 0x80000000;
const FRAME_HEADER_MSG_LEN_MASK: u32 = 0x7FFFFFFF;
const FRAME_HEADER_ALIGNMENT: usize = align_of::<FrameHeader>();

#[repr(C)]
struct Header {
    producer_position: CachePadded<AtomicUsize>, // will always increase
    capacity: CachePadded<AtomicUsize>,
}

impl Header {
    fn data_ptr(&self) -> *const u8 {
        let header_ptr: *const Header = self;
        unsafe { header_ptr.add(1) as *const u8 }
    }

    fn data_ptr_mut(&mut self) -> *mut u8 {
        let header_ptr: *mut Header = self;
        unsafe { header_ptr.add(1) as *mut u8 }
    }
}

#[repr(C, align(8))]
struct FrameHeader {
    fields: u32, // contains padding flag and payload length
}

impl FrameHeader {
    fn new(payload_len: u32, is_padding: bool) -> Self {
        let fields = (payload_len & FRAME_HEADER_MSG_LEN_MASK) | ((is_padding as u32) << 31);
        FrameHeader { fields }
    }

    fn new_padding() -> Self {
        let fields = FRAME_HEADER_PADDING_MASK;
        FrameHeader { fields }
    }

    fn is_padding(&self) -> bool {
        (self.fields & FRAME_HEADER_PADDING_MASK) != 0
    }

    fn payload_len(&self) -> u32 {
        self.fields & FRAME_HEADER_MSG_LEN_MASK
    }

    #[inline]
    fn get_payload_ptr(&self) -> *const FrameHeader {
        let message_header_ptr: *const FrameHeader = self;
        unsafe { message_header_ptr.add(1) }
    }

    #[inline]
    fn payload(&mut self, size: usize) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.get_payload_ptr() as *mut u8, size) }
    }

    #[inline]
    fn get_payload_ptr_mut(&mut self) -> *mut FrameHeader {
        let message_header_ptr: *mut FrameHeader = self;
        unsafe { message_header_ptr.add(1) }
    }

    #[inline]
    fn payload_mut(&mut self, size: usize) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.get_payload_ptr_mut() as *mut u8, size) }
    }
}

#[inline]
const fn get_aligned_size(payload_length: u64) -> u64 {
    // Calculate the number of bytes needed to ensure frame header alignment
    const ALIGNMENT_MASK: u64 = FRAME_HEADER_ALIGNMENT as u64 - 1;
    (payload_length + ALIGNMENT_MASK) & !ALIGNMENT_MASK
}

#[derive(Clone)]
struct RingBuffer {
    ptr: NonNull<Header>,
}

impl RingBuffer {
    fn new(bytes: &[u8]) -> Self {
        assert!(bytes.len() > size_of::<Header>(), "insufficient size for the header");
        assert!((bytes.len() - size_of::<Header>()).is_power_of_two(), "buffer len must be power of two");

        unsafe {
            let header = bytes.as_ptr() as *mut Header;
            (*header).producer_position = CachePadded::new(AtomicUsize::new(0));
            (*header).capacity = CachePadded::new(AtomicUsize::new(bytes.len() - size_of::<Header>()));
            Self {
                ptr: NonNull::new(header).unwrap(),
            }
        }
    }

    #[inline]
    const fn header(&self) -> &Header {
        unsafe { self.ptr.as_ref() }
    }

    #[inline]
    fn header_mut(&mut self) -> &mut Header {
        unsafe { self.ptr.as_mut() }
    }

    fn into_writer(self) -> Writer {
        let capacity = self.header().capacity.load(Ordering::SeqCst);
        Writer {
            ring: self,
            position: 0,
            capacity,
        }
    }
}

struct Writer {
    ring: RingBuffer,
    position: usize,
    capacity: usize,
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
    fn get_buffer(&self) -> &[u8] {
        unsafe {
            let ptr = self.writer.ring.header().data_ptr();
            &*slice_from_raw_parts(ptr.add(self.writer.index() + size_of::<FrameHeader>()), self.limit)
        }
    }

    fn get_buffer_mut(&mut self) -> &mut [u8] {
        unsafe {
            let ptr = self.writer.ring.header_mut().data_ptr_mut();
            &mut *slice_from_raw_parts_mut(ptr.add(self.writer.index() + size_of::<FrameHeader>()), self.limit)
        }
    }

    fn frame_header(&self) -> &FrameHeader {
        unsafe {
            let ptr = self.writer.ring.header().data_ptr();
            &*(ptr.add(self.writer.index()) as *const FrameHeader)
        }
    }

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
        println!("[{}] index before commit: {}", self.is_padding, self.writer.index());
        println!("[{}] position before commit: {}", self.is_padding, self.writer.position);
        println!("[{}] position before commit: {}", self.is_padding, self.writer.ring.header().producer_position.load(Ordering::SeqCst));

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

        println!("[{}] index after commit: {}", self.is_padding, self.writer.index());
        println!("[{}] position after commit: {}", self.is_padding, self.writer.position);
        println!("[{}] position after commit: {}", self.is_padding, self.writer.ring.header().producer_position.load(Ordering::SeqCst));
    }
}

impl<'a> Drop for Claim<'a> {
    fn drop(&mut self) {
        self.commit_impl();
    }
}

impl Writer {
    #[inline]
    fn claim(&mut self, len: u32) -> Claim {
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

        Claim {
            writer: self,
            len: aligned_len as usize,
            limit: len as usize,
            is_padding: false,
        }
    }

    #[inline]
    const fn index(&self) -> usize {
        self.position & (self.capacity - 1)
    }

    #[inline]
    const fn remaining(&self) -> usize {
        self.capacity - self.index()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering::SeqCst;

    #[test]
    fn frame_header() {
        assert_eq!(8, FRAME_HEADER_ALIGNMENT);

        let frame = FrameHeader::new(10, false);
        assert!(!frame.is_padding());
        assert_eq!(10, frame.payload_len());

        let frame = FrameHeader::new(10, true);
        assert!(frame.is_padding());
        assert_eq!(10, frame.payload_len());

        let frame = FrameHeader::new(0, false);
        assert!(!frame.is_padding());
        assert_eq!(0, frame.payload_len());

        let frame = FrameHeader::new(0, true);
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
        let bytes = CachePadded::new([0u8; size_of::<Header>() + 32]);
        let mut writer = RingBuffer::new(&*bytes).into_writer();

        assert_eq!(0, writer.index());

        let claim = writer.claim(10);
        assert_eq!(10, claim.get_buffer().len());
        claim.commit();

        assert_eq!(24, writer.index());
        assert_eq!(8, writer.remaining());

        let claim = writer.claim(5);
        assert_eq!(5, claim.get_buffer().len());
        claim.commit();

        assert_eq!(16, writer.index());
        assert_eq!(16, writer.remaining());

        let claim = writer.claim(8);
        assert_eq!(8, claim.get_buffer().len());
        claim.commit();

        assert_eq!(0, writer.index());
        assert_eq!(32, writer.remaining());
    }

    #[test]
    fn should_ensure_frame_alignment() {
        let bytes = CachePadded::new([0u8; 1024]);
        let mut writer = RingBuffer::new(&*bytes).into_writer();

        let header_ptr = writer.ring.header() as *const Header;
        let data_ptr = writer.ring.header().data_ptr();

        let claim = writer.claim(16);
        let buf_ptr_0 = claim.get_buffer().as_ptr();
        let frame_ptr_0 = claim.frame_header() as *const FrameHeader;

        println!("buffer address: {:p}", buf_ptr_0);
        println!("buffer address: {:#x}", buf_ptr_0 as usize);

        assert_eq!(size_of::<Header>() + size_of::<FrameHeader>(), buf_ptr_0 as usize - header_ptr as usize);
        assert_eq!(size_of::<FrameHeader>(), buf_ptr_0 as usize - data_ptr as usize);
        assert_eq!(16, claim.get_buffer().len());
        assert_eq!(256, size_of::<Header>());
        assert_eq!(8, size_of::<FrameHeader>());
        assert_eq!(8, align_of::<FrameHeader>());
        assert_eq!(size_of::<Header>(), frame_ptr_0 as usize - bytes.as_ptr() as usize);

        claim.commit();

        let claim = writer.claim(13);
        let buf_ptr_1 = claim.get_buffer().as_ptr();
        let frame_ptr_1 = claim.frame_header() as *const FrameHeader;

        assert_eq!(256 + 24, frame_ptr_1 as usize - bytes.as_ptr() as usize);
        assert_eq!(16 + size_of::<FrameHeader>(), buf_ptr_1 as usize - buf_ptr_0 as usize);
        assert_eq!(16 + size_of::<FrameHeader>(), frame_ptr_1 as usize - frame_ptr_0 as usize);

        claim.commit();
    }

    #[test]
    fn it_works() {
        let data = CachePadded::new([0u8; 1024]);

        let rb = RingBuffer::new(&*data);

        assert_eq!(0, rb.header().producer_position.load(SeqCst));
        assert_eq!(1024, rb.header().capacity.load(SeqCst));
    }
}
