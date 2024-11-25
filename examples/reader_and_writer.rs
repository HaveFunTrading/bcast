use bcast::mem::{alloc_aligned, CACHE_LINE_SIZE};
use bcast::{RingBuffer, HEADER_SIZE};
use rand::{thread_rng, Rng};
use std::slice::from_raw_parts;

const RING_BUFFER_SIZE: usize = HEADER_SIZE + 1024;

fn main() -> anyhow::Result<()> {
    let ptr = alloc_aligned(RING_BUFFER_SIZE, CACHE_LINE_SIZE);
    let addr = ptr as usize;
    
    let writer_task = std::thread::spawn(move || {
        let bytes = unsafe { from_raw_parts(addr as *const u8, RING_BUFFER_SIZE) };
        let mut writer = RingBuffer::new(bytes).into_writer();
        loop {
            let symbol = thread_rng().gen_range(b'A'..=b'Z');
            let msg_len = thread_rng().gen_range(1..20);
            let random_bytes: Vec<u8> = (0..msg_len).map(|_| symbol).collect();
            let mut claim = writer.claim_with_user_defined(msg_len, symbol as u32).unwrap();
            claim.get_buffer_mut().copy_from_slice(&random_bytes);
            claim.commit();
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    });

    let reader_task = std::thread::spawn(move || {
        // delay for a bit so that we are not joining from position 0
        std::thread::sleep(std::time::Duration::from_secs(1));
        let bytes = unsafe { from_raw_parts(addr as *const u8, RING_BUFFER_SIZE) };
        let mut reader = RingBuffer::new(bytes).into_reader();
        loop {
            let mut count = 0;
            for msg in reader.batch_iter() {
                count += 1;
                let msg = msg.unwrap();
                let mut payload = [0u8; 1024];
                msg.read(&mut payload).unwrap();
                let payload = &payload[..msg.payload_len];
                assert!(payload.iter().all(|b| *b == msg.user_defined as u8));
                println!("{}", std::str::from_utf8(payload).unwrap());
            }
            if count > 0 {
                println!("batch_size: {}", count);
            }
            // adding delay here to simulate impact of batching
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    });

    writer_task.join().unwrap();
    reader_task.join().unwrap();

    Ok(())
}
