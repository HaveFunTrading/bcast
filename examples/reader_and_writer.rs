use crate::common::{reader, writer};
use bcast::HEADER_SIZE;
use std::slice::from_raw_parts;

mod common;

// This example will demonstrate the use shared buffer in order to achieve in-process communication
// between reader and writer that are running on separate threads.

const RING_BUFFER_SIZE: usize = HEADER_SIZE + 1024;

fn main() -> anyhow::Result<()> {
    let bytes = [0u8; RING_BUFFER_SIZE];
    let ptr = bytes.as_ptr();
    let addr = ptr as usize;

    let writer_task = std::thread::spawn(move || {
        let bytes = unsafe { from_raw_parts(addr as *const u8, RING_BUFFER_SIZE) };
        writer(bytes).unwrap();
    });

    let reader_task = std::thread::spawn(move || {
        // delay for a bit so that we are not joining from position 0
        std::thread::sleep(std::time::Duration::from_secs(1));
        let bytes = unsafe { from_raw_parts(addr as *const u8, RING_BUFFER_SIZE) };
        reader(bytes).unwrap();
    });

    writer_task.join().unwrap();
    reader_task.join().unwrap();

    Ok(())
}
