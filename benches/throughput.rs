use bcast::{HEADER_SIZE, LocalStorage, Reader, SharedStorage, StorageExt};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const CAPACITY: usize = 8 * 1024 * 1024;
const RING_BUFFER_SIZE: usize = HEADER_SIZE + CAPACITY;
const MESSAGE_SIZE: usize = 73;
const NUM_MESSAGES: usize = 10_000_000;
const RESERVATION_RATIOS: [f64; 4] = [0.0, 0.005, 0.01, 0.05];
const READER_COUNTS: [usize; 4] = [1, 2, 4, 8];
const READER_APIS: [ReaderApi; 3] = [ReaderApi::ReceiveNext, ReaderApi::ReadBatch, ReaderApi::ReadBulk];

#[derive(Clone, Copy)]
enum ReaderApi {
    ReceiveNext,
    ReadBatch,
    ReadBulk,
}

impl ReaderApi {
    const fn name(self) -> &'static str {
        match self {
            Self::ReceiveNext => "receive_next",
            Self::ReadBatch => "read_batch",
            Self::ReadBulk => "read_bulk",
        }
    }
}

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
        "{:>12} {:>8} {:>8} {:>14} {:>14} {:>14} {:>14} {:>16} {:>16}",
        "api", "readers", "reserve", "elapsed_ms", "ns/msg", "msg/s", "reserve_bytes", "reader_msgs", "reader_overruns"
    );

    for api in READER_APIS {
        for reader_count in READER_COUNTS {
            for reserve_ratio in RESERVATION_RATIOS {
                let result = run_case(api, reserve_ratio, reader_count);
                let elapsed_secs = result.elapsed.as_secs_f64();
                let ns_per_msg = elapsed_secs * 1_000_000_000.0 / NUM_MESSAGES as f64;
                let messages_per_sec = NUM_MESSAGES as f64 / elapsed_secs;

                println!(
                    "{:>12} {:>8} {:>8.3} {:>14.3} {:>14.3} {:>14.0} {:>14} {:>16} {:>16}",
                    api.name(),
                    reader_count,
                    reserve_ratio,
                    elapsed_secs * 1_000.0,
                    ns_per_msg,
                    messages_per_sec,
                    claim_reserve_bytes(CAPACITY, reserve_ratio),
                    result.reader_messages,
                    result.reader_overruns,
                );
            }
        }
    }
}

fn run_case(api: ReaderApi, reserve_ratio: f64, reader_count: usize) -> CaseResult {
    let storage = LocalStorage::new(RING_BUFFER_SIZE).into_shared();
    let mut writer = storage
        .clone()
        .into_writer_with_cfg(|config| config.claim_reserve_ratio(reserve_ratio));
    let payload = [0xAB; MESSAGE_SIZE];
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(reader_count + 1));

    let readers = (0..reader_count)
        .map(|_| {
            let stop = Arc::clone(&stop);
            let barrier = Arc::clone(&barrier);
            let storage = storage.clone();
            thread::spawn(move || reader_loop(storage, api, stop, barrier))
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

fn reader_loop(
    storage: SharedStorage<LocalStorage>,
    api: ReaderApi,
    stop: Arc<AtomicBool>,
    barrier: Arc<Barrier>,
) -> ReaderStats {
    let reader = storage.into_reader_at(0);

    match api {
        ReaderApi::ReceiveNext => receive_next_loop(reader, stop, barrier),
        ReaderApi::ReadBatch => read_batch_loop(reader, stop, barrier),
        ReaderApi::ReadBulk => read_bulk_loop(reader, stop, barrier),
    }
}

fn receive_next_loop<S>(reader: Reader<S>, stop: Arc<AtomicBool>, barrier: Arc<Barrier>) -> ReaderStats {
    let mut reader = reader;
    let mut payload = [0u8; MESSAGE_SIZE];
    let mut messages = 0;
    let mut overruns = 0;

    barrier.wait();

    while !stop.load(Ordering::Acquire) {
        receive_next(&mut reader, &mut payload, &mut messages, &mut overruns);
    }

    ReaderStats { messages, overruns }
}

fn read_batch_loop<S>(reader: Reader<S>, stop: Arc<AtomicBool>, barrier: Arc<Barrier>) -> ReaderStats {
    let mut reader = reader;
    let mut payload = [0u8; MESSAGE_SIZE];
    let mut messages = 0;
    let mut overruns = 0;

    barrier.wait();

    while !stop.load(Ordering::Acquire) {
        read_batch(&mut reader, &mut payload, &mut messages, &mut overruns);
    }

    ReaderStats { messages, overruns }
}

fn read_bulk_loop<S>(reader: Reader<S>, stop: Arc<AtomicBool>, barrier: Arc<Barrier>) -> ReaderStats {
    let mut reader = reader;
    let mut bulk = vec![0u8; CAPACITY];
    let mut messages = 0;
    let mut overruns = 0;

    barrier.wait();

    while !stop.load(Ordering::Acquire) {
        read_bulk(&mut reader, &mut bulk, &mut messages, &mut overruns);
    }

    ReaderStats { messages, overruns }
}

fn receive_next<S>(reader: &mut Reader<S>, payload: &mut [u8], messages: &mut usize, overruns: &mut usize) {
    match reader.receive_next(payload) {
        Some(Ok(msg)) => {
            *messages += 1;
            black_box(msg.payload[0]);
        }
        Some(Err(_)) => {
            *overruns += 1;
            reader.reset();
        }
        None => std::hint::spin_loop(),
    }
}

fn read_batch<S>(reader: &mut Reader<S>, payload: &mut [u8], messages: &mut usize, overruns: &mut usize) {
    let Some(mut batch) = reader.read_batch() else {
        std::hint::spin_loop();
        return;
    };

    while let Some(msg) = batch.receive_next(payload) {
        match msg {
            Ok(msg) => {
                *messages += 1;
                black_box(msg.payload[0]);
            }
            Err(_) => {
                *overruns += 1;
                batch.reset();
                break;
            }
        }
    }
}

fn read_bulk<S>(reader: &mut Reader<S>, bulk_bytes: &mut [u8], messages: &mut usize, overruns: &mut usize) {
    let Some(bulk) = reader.read_bulk() else {
        std::hint::spin_loop();
        return;
    };

    let bulk = match bulk {
        Ok(bulk) => bulk,
        Err(_) => {
            *overruns += 1;
            reader.reset();
            return;
        }
    };

    let iter = match bulk.into_iter(bulk_bytes) {
        Ok(iter) => iter,
        Err(_) => {
            *overruns += 1;
            reader.reset();
            return;
        }
    };

    for msg in iter {
        *messages += 1;
        black_box(msg.payload[0]);
    }
}

fn claim_reserve_bytes(capacity: usize, ratio: f64) -> usize {
    if ratio <= 0.0 {
        return 0;
    }

    let bytes = (capacity as f64 * ratio).ceil() as usize;
    bytes.next_power_of_two()
}
