use std::alloc::{alloc, Layout};
use std::slice::{from_raw_parts, from_raw_parts_mut};

pub const CACHE_LINE_SIZE: usize = 64;

#[must_use]
#[inline]
pub fn alloc_aligned(size: usize, alignment: usize) -> *mut u8 {
    let layout = Layout::from_size_align(size, alignment).expect("invalid layout");
    unsafe { alloc(layout) }
}

#[inline]
pub fn new_slice_aligned(size: usize, alignment: usize) -> &'static [u8] {
    let ptr = alloc_aligned(size, alignment);
    unsafe { from_raw_parts(ptr, size) }
}

#[inline]
pub fn new_slice_aligned_mut(size: usize, alignment: usize) -> &'static mut [u8] {
    let ptr = alloc_aligned(size, alignment);
    unsafe { from_raw_parts_mut(ptr, size) }
}
