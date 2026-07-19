use bcast::{HEADER_SIZE, RingBuffer};
use std::hint::black_box;
use std::time::{Duration, Instant};

const CAPACITY: usize = 8 * 1024 * 1024;
const RING_BUFFER_SIZE: usize = HEADER_SIZE + CAPACITY;
const MESSAGE_SIZE: usize = 73;
const ALIGNED_MESSAGE_SIZE: usize = 80;
const FRAME_HEADER_SIZE: usize = 8;
const FRAME_SIZE: usize = FRAME_HEADER_SIZE + ALIGNED_MESSAGE_SIZE;
const MESSAGES_PER_PASS: usize = CAPACITY / FRAME_SIZE;
const PASSES: usize = 256;

struct CaseResult {
    elapsed: Duration,
    checksum: usize,
}

fn main() {
    let bytes = prepare_ring();
    let messages = MESSAGES_PER_PASS * PASSES;

    println!("reader api benchmark");
    println!("ring capacity: {} bytes", CAPACITY);
    println!("message payload: {} bytes", MESSAGE_SIZE);
    println!("messages/pass: {}", MESSAGES_PER_PASS);
    println!("passes: {}", PASSES);
    println!();
    println!("{:>16} {:>14} {:>14} {:>14} {:>14}", "api", "elapsed_ms", "ns/msg", "msg/s", "messages");

    let message_result = run_message_api(&bytes);
    print_result("Message::read", message_result, messages);

    let receive_into_result = run_receive_into_api(&bytes);
    print_result("receive_into", receive_into_result, messages);
}

fn prepare_ring() -> Vec<u8> {
    let bytes = vec![0u8; RING_BUFFER_SIZE];
    let writer = RingBuffer::new(&bytes).into_writer();
    let payload = [0xAB; MESSAGE_SIZE];

    for _ in 0..MESSAGES_PER_PASS {
        let mut claim = writer.claim(MESSAGE_SIZE, true);
        claim.get_buffer_mut().copy_from_slice(black_box(&payload));
        claim.commit();
    }

    bytes
}

fn run_message_api(bytes: &[u8]) -> CaseResult {
    let mut payload = [0u8; MESSAGE_SIZE];
    let mut checksum = 0usize;

    let start = Instant::now();
    for _ in 0..PASSES {
        let reader = RingBuffer::new(bytes).into_reader().with_initial_position(0);
        let batch = reader.read_batch().unwrap();

        for msg in batch {
            let msg = msg.unwrap();
            let len = msg.read(&mut payload).unwrap();
            checksum = checksum.wrapping_add(black_box(payload[..len][0] as usize));
        }
    }
    let elapsed = start.elapsed();

    CaseResult { elapsed, checksum }
}

fn run_receive_into_api(bytes: &[u8]) -> CaseResult {
    let mut payload = [0u8; MESSAGE_SIZE];
    let mut checksum = 0usize;

    let start = Instant::now();
    for _ in 0..PASSES {
        let reader = RingBuffer::new(bytes).into_reader().with_initial_position(0);
        let mut batch = reader.read_batch().unwrap();

        while let Some(msg) = batch.receive_next_into(&mut payload) {
            let msg = msg.unwrap();
            checksum = checksum.wrapping_add(black_box(msg.payload[0] as usize));
        }
    }
    let elapsed = start.elapsed();

    CaseResult { elapsed, checksum }
}

fn print_result(api: &str, result: CaseResult, messages: usize) {
    let elapsed_secs = result.elapsed.as_secs_f64();
    let ns_per_msg = elapsed_secs * 1_000_000_000.0 / messages as f64;
    let messages_per_sec = messages as f64 / elapsed_secs;

    println!(
        "{:>16} {:>14.3} {:>14.3} {:>14.0} {:>14}",
        api,
        elapsed_secs * 1_000.0,
        ns_per_msg,
        messages_per_sec,
        messages,
    );

    black_box(result.checksum);
}
