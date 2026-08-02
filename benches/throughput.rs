mod common;

use anyhow::{Context, Result, anyhow};
use bcast::{HEADER_SIZE, MmapMutStorage, MmapStorage, Reader, StorageExt, error::Error};
use common::CpuAffinity;
use std::hint::{black_box, spin_loop};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const CAPACITY: usize = 8 * 1024 * 1024;
const MESSAGE_SIZE: usize = 73;
const NUM_MESSAGES: usize = 10_000_000;
const SHM_DIRECTORY: &str = "/dev/shm";
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
    publisher_elapsed: Duration,
    end_to_end_elapsed: Duration,
    reader_messages: usize,
    expected_reader_messages: usize,
    reader_overruns: usize,
}

fn main() -> Result<()> {
    let max_readers = *READER_COUNTS.iter().max().expect("at least one reader count");
    let affinity = CpuAffinity::from_env(max_readers + 1)?;

    println!("publisher and reader-settle throughput benchmark");
    println!("ring capacity: {CAPACITY} bytes");
    println!("message payload: {MESSAGE_SIZE} bytes");
    println!("messages published per case: {NUM_MESSAGES}");
    println!("mmap directory: {SHM_DIRECTORY}");
    println!("writer and reader mappings are populated and locked before each measured case");
    println!("settle time ends when every reader has drained or reset after an overrun");
    affinity.print();
    println!();
    println!(
        "{:>12} {:>8} {:>8} {:>12} {:>12} {:>14} {:>14} {:>12} {:>12}",
        "api",
        "readers",
        "reserve",
        "pub_ns/msg",
        "settle_ns/msg",
        "published/s",
        "delivery_%",
        "reader_msgs",
        "overruns"
    );

    for api in READER_APIS {
        for reader_count in READER_COUNTS {
            for reserve_ratio in RESERVATION_RATIOS {
                let result = run_case(api, reserve_ratio, reader_count, &affinity)?;
                let publisher_ns = nanos_per_message(result.publisher_elapsed, NUM_MESSAGES);
                let settle_ns = nanos_per_message(result.end_to_end_elapsed, NUM_MESSAGES);
                let published_per_second = NUM_MESSAGES as f64 / result.publisher_elapsed.as_secs_f64();
                let delivery_percent = result.reader_messages as f64 * 100.0 / result.expected_reader_messages as f64;

                println!(
                    "{:>12} {:>8} {:>8.3} {:>12.3} {:>12.3} {:>14.0} {:>14.3} {:>12} {:>12}",
                    api.name(),
                    reader_count,
                    reserve_ratio,
                    publisher_ns,
                    settle_ns,
                    published_per_second,
                    delivery_percent,
                    result.reader_messages,
                    result.reader_overruns,
                );
            }
        }
    }

    Ok(())
}

fn run_case(api: ReaderApi, reserve_ratio: f64, reader_count: usize, affinity: &CpuAffinity) -> Result<CaseResult> {
    affinity.pin_current(0, "throughput publisher thread")?;
    let directory = tempfile::tempdir_in(SHM_DIRECTORY).context("create throughput benchmark directory in /dev/shm")?;
    let path = directory.path().join("throughput.bcast");
    let writer_storage =
        MmapMutStorage::new(&path, HEADER_SIZE + CAPACITY).context("create throughput benchmark mmap storage")?;
    let mut writer = writer_storage.into_writer_with_cfg(|config| config.claim_reserve_ratio(reserve_ratio));
    let published = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(reader_count + 1));

    let readers = (0..reader_count)
        .map(|reader_index| {
            let published = Arc::clone(&published);
            let barrier = Arc::clone(&barrier);
            let path = path.clone();
            let affinity = affinity.clone();
            thread::Builder::new()
                .name(format!("throughput-reader-{}", reader_index + 1))
                .spawn(move || {
                    affinity.pin_current(reader_index + 1, "throughput reader thread")?;
                    reader_loop(path, api, published, barrier)
                })
        })
        .collect::<std::io::Result<Vec<_>>>()?;

    barrier.wait();
    let started_at = Instant::now();
    for _ in 0..NUM_MESSAGES {
        writer.publish(MESSAGE_SIZE, true, |payload| payload.fill(black_box(0xAB)));
    }
    let publisher_elapsed = started_at.elapsed();
    published.store(true, Ordering::Release);

    let (reader_messages, reader_overruns) = readers
        .into_iter()
        .map(|reader| {
            reader
                .join()
                .map_err(|_| anyhow!("throughput reader thread panicked"))?
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .fold((0, 0), |(messages, overruns), stats| (messages + stats.messages, overruns + stats.overruns));
    let end_to_end_elapsed = started_at.elapsed();

    black_box(writer);
    black_box(directory);
    Ok(CaseResult {
        publisher_elapsed,
        end_to_end_elapsed,
        reader_messages,
        expected_reader_messages: NUM_MESSAGES * reader_count,
        reader_overruns,
    })
}

struct ReaderStats {
    messages: usize,
    overruns: usize,
}

fn reader_loop(
    path: PathBuf,
    api: ReaderApi,
    published: Arc<AtomicBool>,
    barrier: Arc<Barrier>,
) -> Result<ReaderStats> {
    let reader = MmapStorage::attach(&path)
        .with_context(|| format!("attach throughput reader to {}", path.display()))?
        .into_reader_at(0);

    match api {
        ReaderApi::ReceiveNext => receive_next_loop(reader, published, barrier),
        ReaderApi::ReadBatch => read_batch_loop(reader, published, barrier),
        ReaderApi::ReadBulk => read_bulk_loop(reader, published, barrier),
    }
}

fn receive_next_loop<S>(
    mut reader: Reader<S>,
    published: Arc<AtomicBool>,
    barrier: Arc<Barrier>,
) -> Result<ReaderStats> {
    let mut payload = [0u8; MESSAGE_SIZE];
    let mut stats = ReaderStats {
        messages: 0,
        overruns: 0,
    };
    barrier.wait();

    loop {
        match reader.receive_next(&mut payload) {
            Some(Ok(message)) => {
                stats.messages += 1;
                black_box(message.payload[0]);
            }
            Some(Err(Error::Overrun(_))) => {
                stats.overruns += 1;
                reader.reset();
            }
            Some(Err(error)) => return Err(error.into()),
            None if published.load(Ordering::Acquire) => break,
            None => spin_loop(),
        }
    }

    Ok(stats)
}

fn read_batch_loop<S>(mut reader: Reader<S>, published: Arc<AtomicBool>, barrier: Arc<Barrier>) -> Result<ReaderStats> {
    let mut payload = [0u8; MESSAGE_SIZE];
    let mut stats = ReaderStats {
        messages: 0,
        overruns: 0,
    };
    barrier.wait();

    loop {
        let Some(mut batch) = reader.read_batch() else {
            if published.load(Ordering::Acquire) {
                break;
            }
            spin_loop();
            continue;
        };

        while let Some(result) = batch.receive_next(&mut payload) {
            match result {
                Ok(message) => {
                    stats.messages += 1;
                    black_box(message.payload[0]);
                }
                Err(Error::Overrun(_)) => {
                    stats.overruns += 1;
                    batch.reset();
                    break;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    Ok(stats)
}

fn read_bulk_loop<S>(mut reader: Reader<S>, published: Arc<AtomicBool>, barrier: Arc<Barrier>) -> Result<ReaderStats> {
    let mut bulk_bytes = vec![0u8; CAPACITY];
    let mut stats = ReaderStats {
        messages: 0,
        overruns: 0,
    };
    barrier.wait();

    loop {
        let Some(bulk) = reader.read_bulk() else {
            if published.load(Ordering::Acquire) {
                break;
            }
            spin_loop();
            continue;
        };

        let bulk = match bulk {
            Ok(bulk) => bulk,
            Err(Error::Overrun(_)) => {
                stats.overruns += 1;
                reader.reset();
                continue;
            }
            Err(error) => return Err(error.into()),
        };

        match bulk.into_iter(&mut bulk_bytes) {
            Ok(messages) => {
                for message in messages {
                    stats.messages += 1;
                    black_box(message.payload[0]);
                }
            }
            Err(Error::Overrun(_)) => {
                stats.overruns += 1;
                reader.reset();
            }
            Err(error) => return Err(error.into()),
        }
    }

    Ok(stats)
}

fn nanos_per_message(elapsed: Duration, messages: usize) -> f64 {
    elapsed.as_secs_f64() * 1_000_000_000.0 / messages as f64
}
