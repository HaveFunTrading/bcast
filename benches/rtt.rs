mod common;

use anyhow::{Context, Result, anyhow, ensure};
use bcast::{LocalStorage, StorageExt};
use common::{BenchClock, CpuAffinity, WARMUP_ENV, env_usize, print_histogram};
use hdrhistogram::Histogram;
use std::hint::spin_loop;
use std::sync::{Arc, Barrier};

const CAPACITY: usize = 64 * 1024;
const NUM_MESSAGES: usize = 1_000_000;
const DEFAULT_WARMUP_MESSAGES: usize = 100_000;

fn main() -> Result<()> {
    let warmup_messages = env_usize(WARMUP_ENV, DEFAULT_WARMUP_MESSAGES)?;
    let total_messages = warmup_messages
        .checked_add(NUM_MESSAGES)
        .context("total RTT message count overflowed usize")?;
    let affinity = CpuAffinity::from_env(2)?;
    let clock = BenchClock::new();
    let barrier = Arc::new(Barrier::new(2));
    let outbound = LocalStorage::with_capacity(CAPACITY).into_shared();
    let inbound = LocalStorage::with_capacity(CAPACITY).into_shared();

    println!("strict ping-pong RTT benchmark");
    println!("payload: 8-byte monotonic timestamp");
    println!("warm-up messages: {warmup_messages}");
    println!("measured messages: {NUM_MESSAGES}");
    affinity.print();
    println!();

    let receiver_affinity = affinity.clone();
    let receiver_barrier = Arc::clone(&barrier);
    let receiver_outbound = outbound.clone();
    let receiver_inbound = inbound.clone();
    let receiver = std::thread::Builder::new()
        .name("rtt-echo".into())
        .spawn(move || -> Result<()> {
            receiver_affinity.pin_current(1, "RTT echo thread")?;
            let mut tx = receiver_inbound.into_writer();
            let mut rx = receiver_outbound.into_reader_at(0);
            let mut payload = [0u8; 8];
            receiver_barrier.wait();

            for _ in 0..total_messages {
                let message = loop {
                    match rx.receive_next(&mut payload) {
                        Some(Ok(message)) => break message,
                        Some(Err(error)) => return Err(error.into()),
                        None => spin_loop(),
                    }
                };
                tx.send(message.payload, true);
            }
            Ok(())
        })
        .context("spawn RTT echo thread")?;

    affinity.pin_current(0, "RTT sender thread")?;
    let mut tx = outbound.into_writer();
    let mut rx = inbound.into_reader_at(0);
    let mut payload = [0u8; 8];
    let mut latencies = Histogram::<u64>::new(3).context("create RTT histogram")?;
    barrier.wait();

    for sequence in 0..total_messages {
        let sent_at = clock.now_nanos();
        tx.send(&sent_at.to_le_bytes(), true);

        let echoed_at = loop {
            match rx.receive_next(&mut payload) {
                Some(Ok(message)) => {
                    let echoed = u64::from_le_bytes(message.payload.try_into().expect("RTT payload has eight bytes"));
                    ensure!(echoed == sent_at, "RTT echo payload mismatch");
                    break clock.now_nanos();
                }
                Some(Err(error)) => return Err(error.into()),
                None => spin_loop(),
            }
        };

        if sequence >= warmup_messages {
            latencies.record(echoed_at.saturating_sub(sent_at))?;
        }
    }

    receiver.join().map_err(|_| anyhow!("RTT echo thread panicked"))??;
    ensure!(latencies.len() == NUM_MESSAGES as u64, "RTT benchmark lost samples");
    print_histogram("round-trip latency", &latencies, "ns");
    Ok(())
}
