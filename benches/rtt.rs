use bcast::{HEADER_SIZE, RingBuffer, Writer};
use std::slice::from_raw_parts;
use std::time::{SystemTime, UNIX_EPOCH};

// Will measure round trip time (RTT). There are 2 shared buffers, one for outgoing messages whose
// payload contains the current timestamp in nanoseconds. The other buffer is used to echo back
// the original message. Once the original messages is received the round trip time will be calculated
// as current time in nanoseconds minus the timestamp from the message.

const RING_BUFFER_SIZE: usize = HEADER_SIZE + 1024 * 1024 * 32;
const NUM_MESSAGES: usize = 1_000_000;

fn main() -> anyhow::Result<()> {
    let bytes_tx = vec![0u8; RING_BUFFER_SIZE];
    let bytes_rx = vec![0u8; RING_BUFFER_SIZE];
    let addr_tx = bytes_tx.as_ptr() as usize;
    let addr_rx = bytes_rx.as_ptr() as usize;

    let receiver = std::thread::spawn(move || {
        let bytes_tx = unsafe { from_raw_parts(addr_rx as *const u8, RING_BUFFER_SIZE) };
        let bytes_rx = unsafe { from_raw_parts(addr_tx as *const u8, RING_BUFFER_SIZE) };
        let tx = RingBuffer::new(bytes_tx).into_writer();
        let rx = RingBuffer::new(bytes_rx).into_reader().with_initial_position(0);

        'outer: loop {
            if let Some(batch) = rx.read_batch() {
                for msg in batch.into_iter().flatten() {
                    let mut claim = tx.claim(8, true);
                    if msg.read(claim.get_buffer_mut()).is_ok() {
                        let time = u64::from_le_bytes(claim.get_buffer().try_into().unwrap());

                        #[cold]
                        #[inline(never)]
                        fn poison() {}

                        if time == 0 {
                            poison();
                            break 'outer;
                        }

                        claim.commit();
                    }
                }
            }
        }
    });

    let sender = std::thread::spawn(move || {
        let bytes_tx = unsafe { from_raw_parts(addr_tx as *const u8, RING_BUFFER_SIZE) };
        let bytes_rx = unsafe { from_raw_parts(addr_rx as *const u8, RING_BUFFER_SIZE) };
        let mut tx = RingBuffer::new(bytes_tx).into_writer();
        let rx = RingBuffer::new(bytes_rx).into_reader().with_initial_position(0);

        let mut payload = [0u8; 8];
        let mut msg_count: usize = 0;

        let mut latencies = hdrhistogram::Histogram::<u64>::new(3).unwrap();

        loop {
            let mut claim = tx.claim(8, true);
            let bytes = u64::to_le_bytes(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64);
            claim.get_buffer_mut().copy_from_slice(&bytes);
            claim.commit();
            msg_count += 1;

            if let Some(batch) = rx.read_batch() {
                for msg in batch.into_iter().flatten() {
                    if let Ok(len) = msg.read(&mut payload) {
                        let time = u64::from_le_bytes(payload[..len].try_into().unwrap());
                        let rtt = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64 - time;
                        latencies.record(rtt).unwrap();
                    }
                }
            }

            #[cold]
            #[inline(never)]
            fn send_poison(tx: &mut Writer) {
                // send POISON pill
                let mut claim = tx.claim(8, true);
                let bytes = u64::to_le_bytes(0);
                claim.get_buffer_mut().copy_from_slice(&bytes);
                claim.commit();
            }

            if msg_count >= NUM_MESSAGES {
                send_poison(&mut tx);
                break;
            }
        }

        println!("######################");
        println!("latencies");
        println!("######################");
        println!("min: {}", latencies.min());
        println!("50th: {}", latencies.value_at_percentile(0.5));
        println!("90th: {}", latencies.value_at_percentile(0.9));
        println!("99th: {}", latencies.value_at_percentile(0.99));
        println!("99.9th: {}", latencies.value_at_percentile(0.999));
        println!("99.99th: {}", latencies.value_at_percentile(0.9999));
        println!("max: {}", latencies.max());
        println!("count: {}", latencies.len());
    });

    receiver.join().unwrap();
    sender.join().unwrap();

    Ok(())
}
