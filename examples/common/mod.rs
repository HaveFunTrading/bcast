use bcast::RingBuffer;
use rand::{thread_rng, Rng};

/// Generate random message every 1 millisecond.
#[allow(dead_code)]
pub fn writer(bytes: &[u8]) -> anyhow::Result<()> {
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

/// Consume messages produced by the writer. It will also perform payload validation (using user
/// defined field set by the writer) and sleep for 10 milliseconds in order to process messages
/// in a batch.
#[allow(dead_code)]
pub fn reader(bytes: &[u8]) -> anyhow::Result<()> {
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
