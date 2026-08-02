mod common;

use anyhow::{Context, Result, anyhow, ensure};
use bcast::{HEADER_SIZE, MmapMutStorage, MmapStorage, StorageExt};
use common::{BenchClock, CpuAffinity, WARMUP_ENV, env_usize, print_histogram};
use hdrhistogram::Histogram;
use std::hint::spin_loop;
use std::sync::{Arc, Barrier};

const CAPACITY: usize = 1024 * 1024;
const DEFAULT_MESSAGES: usize = 1_000_000;
const DEFAULT_WARMUP_MESSAGES: usize = 100_000;
const DEFAULT_INTERVAL_NS: usize = 1_000;
const DEFAULT_PAYLOAD_SIZE: usize = 64;
const DEFAULT_BURST_SIZE: usize = 1;
const TIMESTAMP_SIZE: usize = size_of::<u64>();
const SEQUENCE_SIZE: usize = size_of::<u64>();
const MIN_PAYLOAD_SIZE: usize = TIMESTAMP_SIZE + SEQUENCE_SIZE;
const SHM_DIRECTORY: &str = "/dev/shm";
const MESSAGES_ENV: &str = "BCAST_RX_MESSAGES";
const INTERVAL_ENV: &str = "BCAST_RX_INTERVAL_NS";
const PAYLOAD_SIZE_ENV: &str = "BCAST_RX_PAYLOAD_SIZE";
const BURST_SIZE_ENV: &str = "BCAST_RX_BURST_SIZE";

struct ReceiverStats {
    latencies: Histogram<u64>,
    batch_sizes: Histogram<u64>,
}

fn main() -> Result<()> {
    let measured_messages = env_usize(MESSAGES_ENV, DEFAULT_MESSAGES)?;
    let warmup_messages = env_usize(WARMUP_ENV, DEFAULT_WARMUP_MESSAGES)?;
    let interval_ns = u64::try_from(env_usize(INTERVAL_ENV, DEFAULT_INTERVAL_NS)?)
        .context("RX offered interval does not fit in u64")?;
    let payload_size = env_usize(PAYLOAD_SIZE_ENV, DEFAULT_PAYLOAD_SIZE)?;
    let burst_size = env_usize(BURST_SIZE_ENV, DEFAULT_BURST_SIZE)?;
    ensure!(measured_messages != 0, "{MESSAGES_ENV} must be greater than zero");
    ensure!(payload_size >= MIN_PAYLOAD_SIZE, "{PAYLOAD_SIZE_ENV} must be at least {MIN_PAYLOAD_SIZE}");
    ensure!(burst_size != 0, "{BURST_SIZE_ENV} must be greater than zero");
    let total_messages = warmup_messages
        .checked_add(measured_messages)
        .context("total RX message count overflowed usize")?;
    let affinity = CpuAffinity::from_env(2)?;
    let clock = BenchClock::new();
    let barrier = Arc::new(Barrier::new(2));
    let directory = tempfile::tempdir_in(SHM_DIRECTORY).context("create RX benchmark directory in /dev/shm")?;
    let path = directory.path().join("rx.bcast");
    let writer_storage =
        MmapMutStorage::new(&path, HEADER_SIZE + CAPACITY).context("create RX benchmark writable mmap storage")?;
    let mut writer = writer_storage.into_writer();
    ensure!(payload_size <= writer.mtu(), "{PAYLOAD_SIZE_ENV} exceeds writer MTU {}", writer.mtu());
    let reader_storage = MmapStorage::attach(&path).context("attach RX benchmark read-only mmap storage")?;

    println!("mmap paced one-way receive-latency benchmark");
    println!("payload: {payload_size} bytes (8-byte timestamp, 8-byte sequence, remaining bytes fixed)");
    println!("warm-up messages: {warmup_messages}");
    println!("measured messages: {measured_messages}");
    println!("burst size: {burst_size}");
    println!("offered interval between burst starts: {interval_ns}ns (set {INTERVAL_ENV}=0 for saturation)");
    println!("writer API: Writer::publish");
    println!("reader API: read_batch + Batch::receive_next");
    println!("mmap directory: {SHM_DIRECTORY}");
    affinity.print();
    println!();

    let receiver_affinity = affinity.clone();
    let receiver_barrier = Arc::clone(&barrier);
    let receiver = std::thread::Builder::new()
        .name("rx-consumer".into())
        .spawn(move || -> Result<ReceiverStats> {
            receiver_affinity.pin_current(1, "RX consumer thread")?;
            let mut reader = reader_storage.into_reader_at(0);
            let mut payload = vec![0u8; payload_size];
            let mut latencies = Histogram::<u64>::new(3).context("create RX latency histogram")?;
            let mut batch_sizes = Histogram::<u64>::new(3).context("create RX batch-size histogram")?;
            let mut received = 0_usize;
            receiver_barrier.wait();

            while received < total_messages {
                let Some(mut batch) = reader.read_batch() else {
                    spin_loop();
                    continue;
                };
                let mut measured_in_batch = 0;

                while let Some(result) = batch.receive_next(&mut payload) {
                    let message = result?;
                    let received_at = clock.now_nanos();
                    let sent_at = u64::from_le_bytes(
                        message.payload[..TIMESTAMP_SIZE]
                            .try_into()
                            .expect("RX timestamp has eight bytes"),
                    );
                    let sequence = u64::from_le_bytes(
                        message.payload[TIMESTAMP_SIZE..MIN_PAYLOAD_SIZE]
                            .try_into()
                            .expect("RX sequence has eight bytes"),
                    );
                    ensure!(
                        sequence == received as u64,
                        "RX sequence mismatch: expected {received}, received {sequence}"
                    );
                    received += 1;
                    if received > warmup_messages {
                        latencies.record(received_at.saturating_sub(sent_at))?;
                        measured_in_batch += 1;
                    }
                }

                if measured_in_batch != 0 {
                    batch_sizes.record(measured_in_batch)?;
                }
            }

            Ok(ReceiverStats { latencies, batch_sizes })
        })
        .context("spawn RX consumer thread")?;

    affinity.pin_current(0, "RX producer thread")?;
    barrier.wait();
    let pacing_epoch = clock.now_nanos();

    for sequence in 0..total_messages {
        if interval_ns != 0 && sequence.is_multiple_of(burst_size) {
            let burst = u64::try_from(sequence / burst_size).context("RX burst number does not fit in u64")?;
            let deadline = pacing_epoch.saturating_add(burst.saturating_mul(interval_ns));
            while clock.now_nanos() < deadline {
                spin_loop();
            }
        }
        let sent_at = clock.now_nanos();
        writer.publish(payload_size, true, |payload| {
            payload.fill(0xA5);
            payload[..TIMESTAMP_SIZE].copy_from_slice(&sent_at.to_le_bytes());
            payload[TIMESTAMP_SIZE..MIN_PAYLOAD_SIZE].copy_from_slice(&(sequence as u64).to_le_bytes());
        });
    }

    let stats = receiver.join().map_err(|_| anyhow!("RX consumer thread panicked"))??;
    ensure!(stats.latencies.len() == measured_messages as u64, "RX benchmark lost samples");
    print_histogram("one-way receive latency", &stats.latencies, "ns");
    println!();
    print_histogram("measured messages per observed batch", &stats.batch_sizes, "");
    Ok(())
}
