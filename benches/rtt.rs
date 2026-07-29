use bcast::{HEADER_SIZE, LocalStorage, StorageExt, Writer};
use std::time::{SystemTime, UNIX_EPOCH};

// Will measure round trip time (RTT). There are 2 shared buffers, one for outgoing messages whose
// payload contains the current timestamp in nanoseconds. The other buffer is used to echo back
// the original message. Once the original messages is received the round trip time will be calculated
// as current time in nanoseconds minus the timestamp from the message.

const RING_BUFFER_SIZE: usize = HEADER_SIZE + 1024 * 1024 * 32;
const NUM_MESSAGES: usize = 1_000_000;

fn main() -> anyhow::Result<()> {
    let outbound = LocalStorage::new(RING_BUFFER_SIZE).into_shared();
    let inbound = LocalStorage::new(RING_BUFFER_SIZE).into_shared();

    let outbound_for_receiver = outbound.clone();
    let inbound_for_receiver = inbound.clone();
    let receiver = std::thread::spawn(move || {
        let mut tx = inbound_for_receiver.into_writer();
        let mut rx = outbound_for_receiver.into_reader_at(0);
        let mut payload = [0u8; 8];

        'outer: loop {
            if let Some(mut batch) = rx.read_batch() {
                while let Some(msg) = batch.receive_next(&mut payload) {
                    if let Ok(msg) = msg {
                        let time = u64::from_le_bytes(msg.payload.try_into().unwrap());

                        #[cold]
                        #[inline(never)]
                        fn poison() {}

                        if time == 0 {
                            poison();
                            break 'outer;
                        }

                        let mut claim = tx.claim(msg.payload.len(), true);
                        claim.get_buffer_mut().copy_from_slice(msg.payload);
                        claim.commit();
                    }
                }
            }
        }
    });

    let sender = std::thread::spawn(move || {
        let mut tx = outbound.into_writer();
        let mut rx = inbound.into_reader_at(0);

        let mut payload = [0u8; 8];
        let mut msg_count: usize = 0;

        let mut latencies = hdrhistogram::Histogram::<u64>::new(3).unwrap();

        loop {
            let mut claim = tx.claim(8, true);
            let bytes = u64::to_le_bytes(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64);
            claim.get_buffer_mut().copy_from_slice(&bytes);
            claim.commit();
            msg_count += 1;

            if let Some(mut batch) = rx.read_batch() {
                while let Some(msg) = batch.receive_next(&mut payload) {
                    if let Ok(msg) = msg {
                        let time = u64::from_le_bytes(msg.payload.try_into().unwrap());
                        let rtt = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64 - time;
                        latencies.record(rtt).unwrap();
                    }
                }
            }

            #[cold]
            #[inline(never)]
            fn send_poison<S>(tx: &mut Writer<S>) {
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
