use bcast::{HEADER_SIZE, RingBuffer, Writer};
use std::slice::from_raw_parts;
use std::time::{SystemTime, UNIX_EPOCH};

// Will measure receive delays between producer and consumer. The producer will attach current
// time in nanoseconds to outgoing messages. When consumer reads those it will compute receive
// delay as current time in nanoseconds minus timestamp from the message.

const RING_BUFFER_SIZE: usize = HEADER_SIZE + 1024 * 1024 * 32;
const NUM_MESSAGES: usize = 1_000_000;

fn main() -> anyhow::Result<()> {
    let bytes = vec![0u8; RING_BUFFER_SIZE];
    let addr = bytes.as_ptr() as usize;

    let receiver = std::thread::spawn(move || {
        let bytes = unsafe { from_raw_parts(addr as *const u8, RING_BUFFER_SIZE) };
        let rx = RingBuffer::new(bytes).into_reader().with_initial_position(0);

        let mut payload = [0u8; 8];

        let mut latencies = hdrhistogram::Histogram::<u64>::new(3).unwrap();
        let mut batch_size = hdrhistogram::Histogram::<u64>::new(3).unwrap();

        'outer: loop {
            let mut msg_count = 0;
            if let Some(batch) = rx.read_batch() {
                for msg in batch.into_iter().flatten() {
                    if msg.read(&mut payload).is_ok() {
                        let time = u64::from_le_bytes(payload);

                        #[cold]
                        #[inline(never)]
                        fn poison() {
                            println!("poisson");
                        }

                        if time == 0 {
                            poison();
                            break 'outer;
                        }

                        latencies
                            .record(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64 - time)
                            .unwrap();
                        msg_count += 1;
                    }
                }
            }

            if msg_count > 0 {
                batch_size.record(msg_count).unwrap();
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

        println!("######################");
        println!("batch size");
        println!("######################");
        println!("min: {}", batch_size.min());
        println!("50th: {}", batch_size.value_at_percentile(0.5));
        println!("90th: {}", batch_size.value_at_percentile(0.9));
        println!("99th: {}", batch_size.value_at_percentile(0.99));
        println!("99.9th: {}", batch_size.value_at_percentile(0.999));
        println!("99.99th: {}", batch_size.value_at_percentile(0.9999));
        println!("max: {}", batch_size.max());
        println!("count: {}", batch_size.len());
    });

    let sender = std::thread::spawn(move || {
        let bytes = unsafe { from_raw_parts(addr as *const u8, RING_BUFFER_SIZE) };
        let mut tx = RingBuffer::new(bytes).into_writer();
        let mut msg_count: usize = 0;

        loop {
            let mut claim = tx.claim(8, true);
            let bytes = u64::to_le_bytes(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64);
            claim.get_buffer_mut().copy_from_slice(&bytes);
            claim.commit();
            msg_count += 1;

            #[cold]
            #[inline(never)]
            fn send_poison(tx: &mut Writer) {
                // send POISON pill
                let mut claim = tx.claim(8, true);
                let bytes = u64::to_le_bytes(0);
                claim.get_buffer_mut().copy_from_slice(&bytes);
                claim.commit();
            }

            if msg_count == NUM_MESSAGES {
                send_poison(&mut tx);
                break;
            }

            std::thread::sleep(std::time::Duration::from_nanos(1));
        }
    });

    receiver.join().unwrap();
    sender.join().unwrap();

    Ok(())
}
