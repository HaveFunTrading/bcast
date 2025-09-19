use std::ops::Deref;
use std::ptr::slice_from_raw_parts;

/// Wrapper over array of bytes that provides 128-byte alignment.
/// ## Example
/// ```no_run
/// use bcast::util::AlignedBytes;
///
/// let bytes = AlignedBytes::<1024>::new();
/// // we can now work with bytes as if it was &[u8].
/// ```
#[repr(align(128))]
pub struct AlignedBytes<const N: usize>([u8; N]);

impl<const N: usize> AlignedBytes<N> {
    /// Construct new wrapper with specific size `N`.
    #[inline]
    pub const fn new() -> AlignedBytes<N> {
        Self([0; N])
    }
}

impl<const N: usize> Default for AlignedBytes<N> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> Deref for AlignedBytes<N> {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { &*slice_from_raw_parts(self.0.as_ptr(), N) }
    }
}
