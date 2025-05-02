use std::sync::atomic::{AtomicU32, AtomicU64};
use crossbeam_utils::CachePadded;

struct Block {
    version: AtomicU32,
    size: AtomicU32,
    data: CachePadded<[u8; 64]>,
}

struct Header {
    block_counter: CachePadded<AtomicU64>,
}

fn main() {
    println!("{}", size_of::<Block>());
    println!("{}", size_of::<Header>());
}