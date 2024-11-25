use bcast::{RingBuffer};
use memmap2::MmapOptions;
use std::fs::OpenOptions;

fn main() -> anyhow::Result<()> {
    let file = OpenOptions::new().read(true).open("test.dat")?;

    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let bytes = mmap.as_ref();
    
    let mut reader = RingBuffer::new(bytes).into_reader();
    loop {
        let mut count = 0;
        for msg in reader.batch_iter() {
            count += 1;
            let msg = msg?;
            let mut payload = [0u8; 1024];
            msg.read(&mut payload)?;
            let payload = &payload[..msg.payload_len];
            assert!(payload.iter().all(|b| *b == msg.user_defined as u8));
            println!("{}", std::str::from_utf8(payload)?);
        }
        if count > 0 {
            println!("batch_size: {}", count);
        }
        // adding delay here to simulate impact of batching
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}
