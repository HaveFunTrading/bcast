//! Internal ring layout and position arithmetic.
//!
//! This module defines the shared-memory header format, packed frame header
//! representation, and low-level helpers used by readers and writers. It is
//! private to the crate and intentionally not part of the downstream API.

use crate::METADATA_BUFFER_SIZE;
use crate::storage::Storage;
use crossbeam_utils::CachePadded;
use std::cmp::min;
use std::hint;
use std::mem::{align_of, size_of};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Magic bytes used to identify an initialized bcast ring header.
pub const HEADER_MAGIC: u32 = u32::from_le_bytes(*b"BCST");
/// Shared-memory format version understood by this crate.
pub const HEADER_VERSION: u16 = 1;
/// Maximum payload length encodable in the 28-bit frame length field.
pub const MAX_PAYLOAD_LEN: usize = (1 << 28) - 1;
/// Smallest data-section capacity that can produce a valid frame MTU.
const MIN_CAPACITY: usize = 2 * size_of::<FrameHeader>();

// mask to obtain message length from frame header
const FRAME_HEADER_MSG_LEN_MASK: u32 = 0x0FFFFFFF;

/// Fixed header preamble used to locate and validate the shared-memory format.
#[derive(Debug)]
#[repr(C)]
pub struct HeaderPreamble {
    /// Format marker used to reject non-bcast memory regions.
    pub magic: u32,
    /// On-disk/shared-memory format version.
    pub version: u16,
    /// Reserved for future format flags.
    pub _flags: u16, // reserved
}

/// Shared header placed at the start of every ring storage region.
///
/// Stream positions are monotonically increasing byte offsets. Physical ring
/// indexes are derived from positions by masking with `capacity - 1`.
#[derive(Debug)]
#[repr(C)]
pub struct Header {
    /// Fixed format bootstrap data.
    pub preamble: CachePadded<HeaderPreamble>,
    /// Last committed producer position visible to readers.
    pub producer_position: CachePadded<AtomicUsize>,
    /// Furthest position the producer may have reserved or touched.
    pub claimed_position: CachePadded<AtomicUsize>,
    /// Set after the header has been initialized.
    pub ready: CachePadded<AtomicBool>,
    /// Current physical ring lap, used by late readers.
    pub lap_count: CachePadded<AtomicUsize>,
    /// User metadata copied into the ring during writer initialization.
    pub metadata: CachePadded<[u8; METADATA_BUFFER_SIZE]>, // metadata buffer
}

impl Header {
    /// Return a pointer to the first byte of the ring data section.
    #[inline]
    pub const fn data_ptr(&self) -> *const u8 {
        let header_ptr: *const Header = self;
        unsafe { header_ptr.add(1) as *const u8 }
    }

    /// Return whether writer initialization has completed.
    #[inline]
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }

    /// Return the immutable metadata buffer.
    #[inline]
    pub fn metadata(&self) -> &[u8] {
        &*self.metadata
    }

    /// Return the mutable metadata buffer.
    #[inline]
    pub fn metadata_mut(&mut self) -> &mut [u8] {
        &mut *self.metadata
    }
}

/// Per-frame header stored immediately before each payload.
///
/// The low 32 bits contain packed frame flags and payload length. The high 32
/// bits contain the writer-supplied `user_defined` value.
#[repr(C, align(8))]
pub struct FrameHeader(u64);

impl FrameHeader {
    #[inline]
    #[cfg(test)]
    /// Build a frame header for unit tests.
    pub const fn new(
        payload_len: u32,
        user_defined: u32,
        fin: bool,
        continuation: bool,
        padding: bool,
        heartbeat: bool,
    ) -> Self {
        let fields = pack_fields(fin, continuation, padding, heartbeat, payload_len);
        FrameHeader(pack_header(fields, user_defined))
    }

    #[inline]
    #[cfg(test)]
    /// Build a padding frame header for unit tests.
    pub const fn new_padding() -> Self {
        Self::new(0, 0, true, false, true, false)
    }

    #[inline]
    #[cfg(test)]
    /// Return whether this test header has the heartbeat flag set.
    pub const fn is_heartbeat(&self) -> bool {
        ((self.fields() >> 28) & 1) == 1
    }

    #[inline]
    #[cfg(test)]
    /// Return whether this test header has the padding flag set.
    pub const fn is_padding(&self) -> bool {
        ((self.fields() >> 29) & 1) == 1
    }

    #[inline]
    #[cfg(test)]
    /// Return whether this test header has the continuation flag set.
    pub const fn is_continuation(&self) -> bool {
        ((self.fields() >> 30) & 1) == 1
    }

    #[inline]
    #[cfg(test)]
    /// Return whether this test header has the final-fragment flag set.
    pub const fn is_fin(&self) -> bool {
        ((self.fields() >> 31) & 1) == 1
    }

    #[inline]
    #[cfg(test)]
    /// Return this test header's payload length.
    pub const fn payload_len(&self) -> u32 {
        self.fields() & FRAME_HEADER_MSG_LEN_MASK
    }

    /// Extract `(fin, continuation, padding, heartbeat, length)` from this frame.
    #[inline]
    pub const fn unpack_fields(&self) -> (bool, bool, bool, bool, u32) {
        unpack_fields(self.fields())
    }

    /// Return the packed frame flags and payload length.
    #[inline]
    pub const fn fields(&self) -> u32 {
        unpack_header(self.0).0
    }

    /// Return the writer-supplied user-defined frame value.
    #[inline]
    pub const fn user_defined(&self) -> u32 {
        unpack_header(self.0).1
    }

    /// Replace the packed frame fields and user-defined value.
    #[inline]
    pub const fn set(&mut self, fields: u32, user_defined: u32) {
        self.0 = pack_header(fields, user_defined);
    }

    /// Return a pointer to the payload bytes following this frame header.
    #[inline]
    pub const fn get_payload_ptr(&self) -> *const FrameHeader {
        let message_header_ptr: *const FrameHeader = self;
        unsafe { message_header_ptr.add(1) }
    }

    /// Return a mutable pointer to the payload bytes following this frame header.
    #[inline]
    pub const fn get_payload_ptr_mut(&mut self) -> *mut FrameHeader {
        let message_header_ptr = self as *mut FrameHeader;
        unsafe { message_header_ptr.add(1) }
    }
}

/// Pack frame flags and payload length into the low 32-bit frame word.
///
/// Encoding:
///
/// - Bit 31: fin flag
/// - Bit 30: continuation flag
/// - Bit 29: padding flag
/// - Bit 28: heartbeat flag
/// - Bits 0-27: message length
///
/// Length is masked to the encodable 28-bit range.
#[inline]
pub const fn pack_fields(fin: bool, continuation: bool, padding: bool, heartbeat: bool, length: u32) -> u32 {
    unsafe { pack_fields_unchecked(fin, continuation, padding, heartbeat, length & FRAME_HEADER_MSG_LEN_MASK) }
}

/// Pack pre-masked frame fields without applying the length mask again.
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

/// Pack the low 32-bit frame fields and high 32-bit user value into one word.
#[inline]
pub const fn pack_header(fields: u32, user_defined: u32) -> u64 {
    fields as u64 | ((user_defined as u64) << u32::BITS)
}

/// Split a packed frame header word into `(fields, user_defined)`.
#[inline]
pub const fn unpack_header(header: u64) -> (u32, u32) {
    (header as u32, (header >> u32::BITS) as u32)
}

/// Unpack a low 32-bit frame word into `(fin, continuation, padding, heartbeat, length)`.
#[inline]
pub const fn unpack_fields(fields: u32) -> (bool, bool, bool, bool, u32) {
    let fin = (fields >> 31) & 1 == 1;
    let continuation = (fields >> 30) & 1 == 1;
    let padding = (fields >> 29) & 1 == 1;
    let heartbeat = (fields >> 28) & 1 == 1;
    let length = fields & FRAME_HEADER_MSG_LEN_MASK;
    (fin, continuation, padding, heartbeat, length)
}

/// Round a payload length up to the frame-header alignment.
#[inline]
pub const fn get_aligned_size(payload_length: usize) -> usize {
    const ALIGNMENT_MASK: usize = align_of::<FrameHeader>() - 1;
    (payload_length + ALIGNMENT_MASK) & !ALIGNMENT_MASK
}

/// Return whether `cursor` has advanced far enough to make bytes at `position` unsafe to read.
#[inline]
pub const fn is_overrun(position: usize, cursor: usize, capacity: usize) -> bool {
    cursor.wrapping_sub(position) > capacity
}

/// Return whether `position` is strictly after `base` under wrapping stream arithmetic.
#[inline]
pub const fn is_position_after(position: usize, base: usize) -> bool {
    let distance = position.wrapping_sub(base);
    distance != 0 && distance < usize::MAX / 2
}

/// Return whether `position` is equal to or after `base` under wrapping stream arithmetic.
#[inline]
pub const fn is_position_at_or_after(position: usize, base: usize) -> bool {
    position == base || is_position_after(position, base)
}

/// Convert a claim-reserve ratio into aligned, rounded byte capacity.
///
/// Every non-zero reservation is at least one frame-header alignment so adding
/// it to an aligned claim end keeps the shared claimed position aligned.
#[inline]
pub const fn claim_reserve_bytes(capacity: usize, ratio: f64) -> usize {
    if ratio <= 0.0 {
        return 0;
    }

    let bytes = (capacity as f64 * ratio).ceil() as usize;
    let rounded = bytes.next_power_of_two();
    if rounded < align_of::<FrameHeader>() {
        align_of::<FrameHeader>()
    } else {
        rounded
    }
}

/// Non-owning view of a storage-backed ring buffer.
///
/// `RingBuffer` validates the storage layout and keeps derived values used by
/// readers and writers. The storage object itself is owned by `Reader` or
/// `Writer`; this type only stores the header pointer and computed dimensions.
#[derive(Debug)]
pub struct RingBuffer {
    /// Pointer to the shared ring header.
    pub ptr: NonNull<Header>,
    /// Byte capacity of the ring data section.
    pub capacity: usize,
    /// Maximum single-frame payload length.
    pub mtu: usize,
}

impl RingBuffer {
    /// Build a ring view over storage.
    ///
    /// The storage pointer must be aligned for [`Header`]. The data section
    /// length, excluding [`Header`], must be a power of two and at least large
    /// enough to hold two frame headers.
    ///
    /// # Panics
    ///
    /// Panics if storage is misaligned, too small, or has a non-power-of-two
    /// data section.
    pub fn from_storage<S: Storage>(storage: &S) -> Self {
        let ptr = storage.ptr();
        let len = storage.len();
        assert_eq!(ptr.as_ptr() as usize % align_of::<Header>(), 0, "buffer must be header aligned");
        assert!(len > size_of::<Header>(), "insufficient size for the header");
        let capacity = len - size_of::<Header>();
        assert!(capacity.is_power_of_two(), "buffer len must be power of two");
        assert!(capacity >= MIN_CAPACITY, "buffer capacity must be at least two frame headers");

        let header = ptr.as_ptr() as *mut Header;
        Self {
            ptr: NonNull::new(header).unwrap(),
            capacity,
            mtu: min(capacity / 2 - size_of::<FrameHeader>(), MAX_PAYLOAD_LEN),
        }
    }

    /// Return the shared ring header.
    #[inline]
    pub const fn header(&self) -> &Header {
        unsafe { &*self.ptr.as_ptr() }
    }

    /// Initialize the shared header for a newly created writer.
    ///
    /// The metadata callback runs while `ready` is false. Readers spin until
    /// `ready` becomes true and then validate the preamble.
    #[inline]
    pub fn init_header<F: FnOnce(&mut [u8])>(&mut self, position: usize, metadata: F) {
        let header = unsafe { &mut *self.ptr.as_ptr() };
        header.ready.store(false, Ordering::SeqCst);
        metadata(header.metadata_mut());
        header.preamble.magic = HEADER_MAGIC;
        header.preamble.version = HEADER_VERSION;
        header.preamble._flags = 0;
        header.producer_position.store(position, Ordering::SeqCst);
        header.claimed_position.store(position, Ordering::SeqCst);
        header.lap_count.store(0, Ordering::SeqCst);
        header.ready.store(true, Ordering::SeqCst);
    }

    /// Wait until the header is ready and validate the shared-memory preamble.
    #[inline]
    pub fn wait_until_ready(&self) {
        while !self.header().is_ready() {
            hint::spin_loop();
        }
        let preamble = &*self.header().preamble;
        assert_eq!(HEADER_MAGIC, preamble.magic, "invalid ring buffer header magic");
        assert_eq!(HEADER_VERSION, preamble.version, "unsupported ring buffer header version");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::LocalStorage;

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
    fn should_pack_and_unpack_header() {
        let fields = pack_fields(true, false, false, true, 123);
        let user_defined = 456;

        let header = pack_header(fields, user_defined);

        assert_eq!((fields, user_defined), unpack_header(header));
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
    fn should_align_non_zero_claim_reservations_to_frame_headers() {
        assert_eq!(0, claim_reserve_bytes(64, 0.0));
        assert_eq!(8, claim_reserve_bytes(64, 0.01));
        assert_eq!(8, claim_reserve_bytes(1024, 0.0001));
        assert_eq!(16, claim_reserve_bytes(1024, 0.01));
    }

    #[test]
    #[should_panic(expected = "buffer capacity must be at least two frame headers")]
    fn should_reject_capacity_too_small_for_mtu() {
        let storage = LocalStorage::with_capacity(size_of::<FrameHeader>());
        let _ = RingBuffer::from_storage(&storage);
    }
}
