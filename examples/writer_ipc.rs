use bcast::{RingBuffer, HEADER_SIZE};
use memmap2::MmapOptions;
use rand::{thread_rng, Rng};
use std::fs::OpenOptions;

fn main() -> anyhow::Result<()> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open("test.dat")?;
    file.set_len((HEADER_SIZE + 1024) as u64)?;

    let mmap = unsafe { MmapOptions::new().map_mut(&file)? };
    let bytes = mmap.as_ref();
    
    let mut writer = RingBuffer::new(bytes).into_writer();
    loop {
        let symbol = thread_rng().gen_range(b'A'..=b'Z');
        let msg_len = thread_rng().gen_range(1..20);
        let random_bytes: Vec<u8> = (0..msg_len).map(|_| symbol).collect();
        let mut claim = writer.claim_with_user_defined(msg_len, symbol as u32)?;
        claim.get_buffer_mut().copy_from_slice(&random_bytes);
        claim.commit();
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}
