use std::alloc::{alloc, Layout};

pub const CACHE_LINE_SIZE: usize = 64;

pub fn allocate_aligned(size: usize, alignment: usize) -> *const u8 {
    let layout = Layout::from_size_align(size, alignment).expect("Invalid layout");
    unsafe { alloc(layout) }
}
