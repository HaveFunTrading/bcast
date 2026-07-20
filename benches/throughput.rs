use bcast::{HEADER_SIZE, RingBuffer};
use std::hint::black_box;
use std::slice::from_raw_parts;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const CAPACITY: usize = 8 * 1024 * 1024;
const RING_BUFFER_SIZE: usize = HEADER_SIZE + CAPACITY;
const MESSAGE_SIZE: usize = 73;
const NUM_MESSAGES: usize = 10_000_000;
const RESERVATION_PERCENTAGES: [u8; 3] = [0, 1, 5];
const READER_COUNTS: [usize; 3] = [2, 4, 8];

struct CaseResult {
    elapsed: Duration,
    reader_messages: usize,
    reader_overruns: usize,
}

fn main() {
    println!("throughput benchmark");
    println!("ring capacity: {} bytes", CAPACITY);
    println!("message payload: {} bytes", MESSAGE_SIZE);
    println!("messages: {}", NUM_MESSAGES);
    println!();
    println!(
        "{:>8} {:>8} {:>14} {:>14} {:>14} {:>14} {:>16} {:>16}",
        "readers", "reserve", "elapsed_ms", "ns/msg", "msg/s", "reserve_bytes", "reader_msgs", "reader_overruns"
    );

    for reader_count in READER_COUNTS {
        for reserve_percent in RESERVATION_PERCENTAGES {
            let result = run_case(reserve_percent, reader_count);
            let elapsed_secs = result.elapsed.as_secs_f64();
            let ns_per_msg = elapsed_secs * 1_000_000_000.0 / NUM_MESSAGES as f64;
            let messages_per_sec = NUM_MESSAGES as f64 / elapsed_secs;

            println!(
                "{:>8} {:>7}% {:>14.3} {:>14.3} {:>14.0} {:>14} {:>16} {:>16}",
                reader_count,
                reserve_percent,
                elapsed_secs * 1_000.0,
                ns_per_msg,
                messages_per_sec,
                claim_reserve_bytes(CAPACITY, reserve_percent),
                result.reader_messages,
                result.reader_overruns,
            );
        }
    }
}

fn run_case(reserve_percent: u8, reader_count: usize) -> CaseResult {
    let bytes = vec![0u8; RING_BUFFER_SIZE];
    let addr = bytes.as_ptr() as usize;
    let writer = RingBuffer::new(&bytes).into_writer_with_cfg(|config| config.claim_reserve_percent(reserve_percent));
    let payload = [0xAB; MESSAGE_SIZE];
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(reader_count + 1));

    let readers = (0..reader_count)
        .map(|_| {
            let stop = Arc::clone(&stop);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || reader_loop(addr, stop, barrier))
        })
        .collect::<Vec<_>>();

    barrier.wait();

    let start = Instant::now();
    for _ in 0..NUM_MESSAGES {
        let mut claim = writer.claim(MESSAGE_SIZE, true);
        claim.get_buffer_mut().copy_from_slice(black_box(&payload));
        claim.commit();
    }
    let elapsed = start.elapsed();

    stop.store(true, Ordering::Release);
    let (reader_messages, reader_overruns) = readers
        .into_iter()
        .map(|reader| reader.join().unwrap())
        .fold((0, 0), |(messages, overruns), stats| (messages + stats.messages, overruns + stats.overruns));

    black_box(writer);
    CaseResult {
        elapsed,
        reader_messages,
        reader_overruns,
    }
}

struct ReaderStats {
    messages: usize,
    overruns: usize,
}

fn reader_loop(addr: usize, stop: Arc<AtomicBool>, barrier: Arc<Barrier>) -> ReaderStats {
    let bytes = unsafe { from_raw_parts(addr as *const u8, RING_BUFFER_SIZE) };
    let reader = RingBuffer::new(bytes).into_reader().with_initial_position(0);
    let mut payload = [0u8; MESSAGE_SIZE];
    let mut messages = 0;
    let mut overruns = 0;

    barrier.wait();

    while !stop.load(Ordering::Acquire) {
        let Some(mut batch) = reader.read_batch() else {
            std::hint::spin_loop();
            continue;
        };

        while let Some(msg) = batch.receive_next(&mut payload) {
            match msg {
                Ok(msg) => {
                    messages += 1;
                    black_box(msg.payload[0]);
                }
                Err(_) => {
                    overruns += 1;
                    reader.reset();
                    break;
                }
            }
        }
    }

    ReaderStats { messages, overruns }
}

fn claim_reserve_bytes(capacity: usize, percent: u8) -> usize {
    if percent == 0 {
        return 0;
    }

    let percent = percent as usize;
    let bytes = (capacity / 100) * percent + ((capacity % 100) * percent).div_ceil(100);
    bytes.next_power_of_two()
}
