mod common;

use anyhow::{Context, Result, anyhow, ensure};
use bcast::{LocalStorage, StorageExt};
use common::{BenchClock, CpuAffinity, WARMUP_ENV, env_usize, print_histogram};
use hdrhistogram::Histogram;
use std::hint::spin_loop;
use std::sync::{Arc, Barrier};

const CAPACITY: usize = 1024 * 1024;
const NUM_MESSAGES: usize = 1_000_000;
const DEFAULT_WARMUP_MESSAGES: usize = 100_000;
const DEFAULT_INTERVAL_NS: usize = 1_000;
const INTERVAL_ENV: &str = "BCAST_RX_INTERVAL_NS";

struct ReceiverStats {
    latencies: Histogram<u64>,
    batch_sizes: Histogram<u64>,
}

fn main() -> Result<()> {
    let warmup_messages = env_usize(WARMUP_ENV, DEFAULT_WARMUP_MESSAGES)?;
    let interval_ns = env_usize(INTERVAL_ENV, DEFAULT_INTERVAL_NS)? as u64;
    let total_messages = warmup_messages
        .checked_add(NUM_MESSAGES)
        .context("total RX message count overflowed usize")?;
    let affinity = CpuAffinity::from_env(2)?;
    let clock = BenchClock::new();
    let barrier = Arc::new(Barrier::new(2));
    let storage = LocalStorage::with_capacity(CAPACITY).into_shared();

    println!("paced one-way receive-latency benchmark");
    println!("payload: 8-byte monotonic timestamp");
    println!("warm-up messages: {warmup_messages}");
    println!("measured messages: {NUM_MESSAGES}");
    println!("offered interval: {interval_ns}ns (set {INTERVAL_ENV}=0 for saturation)");
    affinity.print();
    println!();

    let receiver_storage = storage.clone();
    let receiver_affinity = affinity.clone();
    let receiver_barrier = Arc::clone(&barrier);
    let receiver = std::thread::Builder::new()
        .name("rx-consumer".into())
        .spawn(move || -> Result<ReceiverStats> {
            receiver_affinity.pin_current(1, "RX consumer thread")?;
            let mut reader = receiver_storage.into_reader_at(0);
            let mut payload = [0u8; 8];
            let mut latencies = Histogram::<u64>::new(3).context("create RX latency histogram")?;
            let mut batch_sizes = Histogram::<u64>::new(3).context("create RX batch-size histogram")?;
            let mut received = 0;
            receiver_barrier.wait();

            while received < total_messages {
                let Some(mut batch) = reader.read_batch() else {
                    spin_loop();
                    continue;
                };
                let mut measured_in_batch = 0;

                while let Some(result) = batch.receive_next(&mut payload) {
                    let message = result?;
                    let sent_at = u64::from_le_bytes(message.payload.try_into().expect("RX payload has eight bytes"));
                    received += 1;
                    if received > warmup_messages {
                        latencies.record(clock.now_nanos().saturating_sub(sent_at))?;
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
    let mut writer = storage.into_writer();
    barrier.wait();
    let pacing_epoch = clock.now_nanos();

    for sequence in 0..total_messages {
        if interval_ns != 0 {
            let deadline = pacing_epoch.saturating_add((sequence as u64).saturating_mul(interval_ns));
            while clock.now_nanos() < deadline {
                spin_loop();
            }
        }
        let sent_at = clock.now_nanos();
        writer.send(&sent_at.to_le_bytes(), true);
    }

    let stats = receiver.join().map_err(|_| anyhow!("RX consumer thread panicked"))??;
    ensure!(stats.latencies.len() == NUM_MESSAGES as u64, "RX benchmark lost samples");
    print_histogram("one-way receive latency", &stats.latencies, "ns");
    println!();
    print_histogram("measured messages per observed batch", &stats.batch_sizes, "");
    Ok(())
}
