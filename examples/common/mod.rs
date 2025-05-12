use anyhow::anyhow;
use bcast::{error::Error, RingBuffer};
use rand::{thread_rng, Rng};
use std::mem::MaybeUninit;

/// Generate random message every 1 millisecond.
#[allow(dead_code)]
pub fn writer(bytes: &[u8]) {
    let mut writer = RingBuffer::new(bytes).into_writer();
    loop {
        let symbol = thread_rng().gen_range(b'A'..=b'Z');
        let msg_len = thread_rng().gen_range(1..20);
        let random_bytes: Vec<u8> = (0..msg_len).map(|_| symbol).collect();
        let mut claim = writer.claim_with_user_defined(msg_len, true, symbol as u32);
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
    let reader = RingBuffer::new(bytes).into_reader();
    loop {
        #[cfg(debug_assertions)]
        let mut count = 0;
        if let Some(batch) = reader.read_batch() {
            for msg in batch {
                let msg = match msg {
                    Ok(msg) => msg,
                    Err(Error::Overrun(position)) => {
                        println!("overrun for {} bytes, resetting reader", position);
                        reader.reset();
                        break;
                    }
                    Err(e) => {
                        return Err(anyhow!(e));
                    }
                };
                let mut payload = unsafe { MaybeUninit::new([0u8; 1024]).assume_init() };
                msg.read(&mut payload)?;
                #[cfg(debug_assertions)]
                {
                    count += 1;
                    let payload = &payload[..msg.payload_len];
                    assert!(payload.iter().all(|b| *b == msg.user_defined as u8));
                    println!("{}", String::from_utf8_lossy(payload));
                }
            }
        }
        #[cfg(debug_assertions)]
        {
            if count > 0 {
                println!("batch_size: {}", count);
            }
            // adding delay here to simulate impact of batching
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}
