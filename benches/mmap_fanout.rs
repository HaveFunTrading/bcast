use anyhow::{Context, Result, anyhow, ensure};
use bcast::{HEADER_SIZE, MmapMutStorage, MmapStorage, StorageExt, Writer};
use hdrhistogram::Histogram;
use std::cell::Cell;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const CONSUMER_COUNT: usize = 5;
const DEFAULT_MESSAGES_PER_CONSUMER: usize = 200_000;
const CLAIM_RESERVE_RATIO: f64 = 0.01;
const PAYLOAD_SIZE: usize = 73;
const TIMESTAMP_SIZE: usize = size_of::<u64>();
const FRAME_SIZE: usize = size_of::<u64>() + PAYLOAD_SIZE.next_multiple_of(size_of::<u64>());
const SHM_DIRECTORY: &str = "/dev/shm";

#[derive(Clone, Copy)]
enum Topology {
    Shared,
    Distinct,
}

impl Topology {
    const fn name(self) -> &'static str {
        match self {
            Self::Shared => "one shared channel",
            Self::Distinct => "five distinct channels",
        }
    }
}

#[derive(Clone, Copy)]
enum ReaderApi {
    Batch,
    Direct,
}

impl ReaderApi {
    const fn name(self) -> &'static str {
        match self {
            Self::Batch => "read_batch_with_filter",
            Self::Direct => "receive_next_with_filter",
        }
    }
}

struct ProducerStats {
    started_at: Instant,
    elapsed: Duration,
}

struct ConsumerStats {
    message_type: u32,
    polled: usize,
    matched: usize,
    finished_at: Instant,
    latencies: Histogram<u64>,
}

struct CaseResult {
    topology: Topology,
    reader_api: ReaderApi,
    producer: ProducerStats,
    consumers: Vec<ConsumerStats>,
    end_to_end: Duration,
    latencies: Histogram<u64>,
}

fn main() -> Result<()> {
    let messages_per_consumer = messages_per_consumer()?;
    let total_messages = messages_per_consumer
        .checked_mul(CONSUMER_COUNT)
        .context("total message count overflowed usize")?;

    println!("mmap single-producer / five-consumer benchmark");
    println!("payload: {PAYLOAD_SIZE} bytes ({TIMESTAMP_SIZE}-byte timestamp + 65 data bytes)");
    println!("messages per consumer: {messages_per_consumer}");
    println!("total messages published per case: {total_messages}");
    println!("writer claim reserve: {:.1}%", CLAIM_RESERVE_RATIO * 100.0);
    println!("reader APIs: read_batch_with_filter, receive_next_with_filter");
    println!("mmap directory: {SHM_DIRECTORY}");
    println!();

    let batch_shared = run_shared_channel(messages_per_consumer, ReaderApi::Batch)?;
    print_result(&batch_shared, total_messages);

    let batch_distinct = run_distinct_channels(messages_per_consumer, ReaderApi::Batch)?;
    print_result(&batch_distinct, total_messages);

    print_topology_comparison(&batch_shared, &batch_distinct, total_messages);

    let direct_shared = run_shared_channel(messages_per_consumer, ReaderApi::Direct)?;
    print_result(&direct_shared, total_messages);

    let direct_distinct = run_distinct_channels(messages_per_consumer, ReaderApi::Direct)?;
    print_result(&direct_distinct, total_messages);

    print_topology_comparison(&direct_shared, &direct_distinct, total_messages);
    print_reader_api_comparison(&batch_shared, &direct_shared, total_messages);
    print_reader_api_comparison(&batch_distinct, &direct_distinct, total_messages);

    Ok(())
}

fn print_topology_comparison(shared: &CaseResult, distinct: &CaseResult, total_messages: usize) {
    println!("{}: distinct / shared", shared.reader_api.name());
    println!(
        "  producer throughput: {:.3}x",
        messages_per_second(total_messages, distinct.producer.elapsed)
            / messages_per_second(total_messages, shared.producer.elapsed)
    );
    println!(
        "  end-to-end throughput: {:.3}x",
        messages_per_second(total_messages, distinct.end_to_end)
            / messages_per_second(total_messages, shared.end_to_end)
    );
    println!(
        "  p50 latency: {:.3}x",
        ratio(distinct.latencies.value_at_percentile(50.0), shared.latencies.value_at_percentile(50.0),)
    );
    println!(
        "  p99 latency: {:.3}x",
        ratio(distinct.latencies.value_at_percentile(99.0), shared.latencies.value_at_percentile(99.0),)
    );
    println!();
}

fn print_reader_api_comparison(batch: &CaseResult, direct: &CaseResult, total_messages: usize) {
    println!("{}: direct / batch", batch.topology.name());
    println!(
        "  producer throughput: {:.3}x",
        messages_per_second(total_messages, direct.producer.elapsed)
            / messages_per_second(total_messages, batch.producer.elapsed)
    );
    println!(
        "  end-to-end throughput: {:.3}x",
        messages_per_second(total_messages, direct.end_to_end) / messages_per_second(total_messages, batch.end_to_end)
    );
    println!(
        "  p50 latency: {:.3}x",
        ratio(direct.latencies.value_at_percentile(50.0), batch.latencies.value_at_percentile(50.0),)
    );
    println!(
        "  p99 latency: {:.3}x",
        ratio(direct.latencies.value_at_percentile(99.0), batch.latencies.value_at_percentile(99.0),)
    );
    println!();
}

fn messages_per_consumer() -> Result<usize> {
    let Some(value) = std::env::args().skip(1).find(|value| !value.starts_with('-')) else {
        return Ok(DEFAULT_MESSAGES_PER_CONSUMER);
    };

    let count = value
        .parse::<usize>()
        .with_context(|| format!("invalid messages-per-consumer value: {value}"))?;
    ensure!(count > 0, "messages per consumer must be greater than zero");
    Ok(count)
}

fn run_shared_channel(messages_per_consumer: usize, reader_api: ReaderApi) -> Result<CaseResult> {
    let total_messages = messages_per_consumer
        .checked_mul(CONSUMER_COUNT)
        .context("total message count overflowed usize")?;
    let capacity = ring_capacity(total_messages)?;
    let directory =
        tempfile::tempdir_in(SHM_DIRECTORY).context("create shared-channel benchmark directory in /dev/shm")?;
    let path = directory.path().join("shared.bcast");
    let writer_storage =
        MmapMutStorage::new(&path, HEADER_SIZE + capacity).context("create shared-channel mmap storage")?;
    let barrier = Arc::new(Barrier::new(CONSUMER_COUNT + 1));
    let clock = Arc::new(Instant::now());

    let consumers = spawn_consumers(
        Topology::Shared,
        reader_api,
        std::slice::from_ref(&path),
        messages_per_consumer,
        total_messages,
        Arc::clone(&barrier),
        Arc::clone(&clock),
    );

    let producer_barrier = Arc::clone(&barrier);
    let producer_clock = Arc::clone(&clock);
    let producer = thread::Builder::new()
        .name("mmap-shared-producer".into())
        .spawn(move || {
            let mut writer =
                writer_storage.into_writer_with_cfg(|config| config.claim_reserve_ratio(CLAIM_RESERVE_RATIO));
            producer_barrier.wait();
            let started_at = Instant::now();

            for sequence in 0..total_messages {
                let message_type = message_type(sequence);
                publish(&mut writer, message_type, sequence, &producer_clock);
            }

            ProducerStats {
                started_at,
                elapsed: started_at.elapsed(),
            }
        })
        .context("spawn shared-channel producer")?;

    collect_case(Topology::Shared, reader_api, producer, consumers)
}

fn run_distinct_channels(messages_per_consumer: usize, reader_api: ReaderApi) -> Result<CaseResult> {
    let capacity = ring_capacity(messages_per_consumer)?;
    let directory =
        tempfile::tempdir_in(SHM_DIRECTORY).context("create distinct-channel benchmark directory in /dev/shm")?;
    let paths = (0..CONSUMER_COUNT)
        .map(|index| directory.path().join(format!("channel-{}.bcast", index + 1)))
        .collect::<Vec<_>>();
    let writer_storages = paths
        .iter()
        .map(|path| {
            MmapMutStorage::new(path, HEADER_SIZE + capacity)
                .with_context(|| format!("create mmap storage {}", path.display()))
        })
        .collect::<Result<Vec<_>>>()?;
    let barrier = Arc::new(Barrier::new(CONSUMER_COUNT + 1));
    let clock = Arc::new(Instant::now());

    let consumers = spawn_consumers(
        Topology::Distinct,
        reader_api,
        &paths,
        messages_per_consumer,
        messages_per_consumer,
        Arc::clone(&barrier),
        Arc::clone(&clock),
    );

    let producer_barrier = Arc::clone(&barrier);
    let producer_clock = Arc::clone(&clock);
    let producer = thread::Builder::new()
        .name("mmap-distinct-producer".into())
        .spawn(move || {
            let mut writers = writer_storages
                .into_iter()
                .map(|storage| storage.into_writer_with_cfg(|config| config.claim_reserve_ratio(CLAIM_RESERVE_RATIO)))
                .collect::<Vec<_>>();
            producer_barrier.wait();
            let started_at = Instant::now();

            for sequence in 0..messages_per_consumer {
                for (index, writer) in writers.iter_mut().enumerate() {
                    publish(writer, (index + 1) as u32, sequence, &producer_clock);
                }
            }

            ProducerStats {
                started_at,
                elapsed: started_at.elapsed(),
            }
        })
        .context("spawn distinct-channel producer")?;

    collect_case(Topology::Distinct, reader_api, producer, consumers)
}

fn spawn_consumers(
    topology: Topology,
    reader_api: ReaderApi,
    paths: &[PathBuf],
    messages_per_consumer: usize,
    frames_to_poll: usize,
    barrier: Arc<Barrier>,
    clock: Arc<Instant>,
) -> Vec<thread::JoinHandle<Result<ConsumerStats>>> {
    (0..CONSUMER_COUNT)
        .map(|index| {
            let path = match topology {
                Topology::Shared => paths[0].clone(),
                Topology::Distinct => paths[index].clone(),
            };
            let barrier = Arc::clone(&barrier);
            let clock = Arc::clone(&clock);
            let expected_type = (index + 1) as u32;

            thread::Builder::new()
                .name(format!("mmap-consumer-{}", index + 1))
                .spawn(move || {
                    consume(&path, reader_api, expected_type, messages_per_consumer, frames_to_poll, barrier, clock)
                })
                .expect("spawn consumer")
        })
        .collect()
}

fn consume(
    path: &Path,
    reader_api: ReaderApi,
    expected_type: u32,
    expected_matches: usize,
    frames_to_poll: usize,
    barrier: Arc<Barrier>,
    clock: Arc<Instant>,
) -> Result<ConsumerStats> {
    let mut reader = MmapStorage::attach(path)
        .with_context(|| format!("attach consumer to {}", path.display()))?
        .into_reader_at(0);
    let mut payload = [0u8; PAYLOAD_SIZE];
    let mut latencies = Histogram::<u64>::new(3).context("create latency histogram")?;
    let polled = Cell::new(0);
    let mut matched = 0;

    barrier.wait();

    match reader_api {
        ReaderApi::Batch => {
            while polled.get() < frames_to_poll {
                let Some(mut batch) = reader.read_batch_with_filter(|user_defined| {
                    polled.set(polled.get() + 1);
                    user_defined == expected_type
                }) else {
                    std::hint::spin_loop();
                    continue;
                };

                while polled.get() < frames_to_poll {
                    let Some(message) = batch.receive_next(&mut payload) else {
                        break;
                    };
                    let message = message.map_err(|error| {
                        anyhow!(
                            "consumer {expected_type} was overrun after {}/{frames_to_poll} frames: {error}",
                            polled.get()
                        )
                    })?;
                    record_latency(message.payload, &clock, &mut latencies)?;
                    matched += 1;
                }
            }
        }
        ReaderApi::Direct => {
            while polled.get() < frames_to_poll {
                let Some(message) = reader.receive_next_with_filter(&mut payload, |user_defined| {
                    polled.set(polled.get() + 1);
                    user_defined == expected_type
                }) else {
                    std::hint::spin_loop();
                    continue;
                };
                let message = message.map_err(|error| {
                    anyhow!(
                        "consumer {expected_type} was overrun after {}/{frames_to_poll} frames: {error}",
                        polled.get()
                    )
                })?;
                record_latency(message.payload, &clock, &mut latencies)?;
                matched += 1;
            }
        }
    }

    let polled = polled.get();
    ensure!(
        matched == expected_matches,
        "consumer {expected_type} matched {matched} messages; expected {expected_matches}"
    );

    Ok(ConsumerStats {
        message_type: expected_type,
        polled,
        matched,
        finished_at: Instant::now(),
        latencies,
    })
}

fn record_latency(payload: &[u8], clock: &Instant, latencies: &mut Histogram<u64>) -> Result<()> {
    let timestamp = u64::from_le_bytes(payload[..TIMESTAMP_SIZE].try_into().expect("timestamp has eight bytes"));
    let latency = now_nanos(clock).saturating_sub(timestamp);
    latencies
        .record(latency)
        .with_context(|| format!("record latency of {latency}ns"))?;
    black_box(payload[TIMESTAMP_SIZE]);
    Ok(())
}

fn publish<S>(writer: &mut Writer<S>, message_type: u32, sequence: usize, clock: &Instant) {
    let mut claim = writer.claim_with_user_defined(PAYLOAD_SIZE, true, message_type);
    let payload = claim.get_buffer_mut();
    payload[TIMESTAMP_SIZE..].fill(message_type as u8);
    payload[TIMESTAMP_SIZE..TIMESTAMP_SIZE + size_of::<usize>()].copy_from_slice(&sequence.to_le_bytes());
    payload[..TIMESTAMP_SIZE].copy_from_slice(&now_nanos(clock).to_le_bytes());
    claim.commit();
}

fn collect_case(
    topology: Topology,
    reader_api: ReaderApi,
    producer: thread::JoinHandle<ProducerStats>,
    consumers: Vec<thread::JoinHandle<Result<ConsumerStats>>>,
) -> Result<CaseResult> {
    let producer = producer.join().map_err(|_| anyhow!("producer thread panicked"))?;
    let consumers = consumers
        .into_iter()
        .map(|consumer| consumer.join().map_err(|_| anyhow!("consumer thread panicked"))?)
        .collect::<Result<Vec<_>>>()?;
    let finished_at = consumers
        .iter()
        .map(|consumer| consumer.finished_at)
        .max()
        .context("case has no consumers")?;
    let end_to_end = finished_at.duration_since(producer.started_at);
    let mut latencies = Histogram::<u64>::new(3).context("create aggregate latency histogram")?;
    for consumer in &consumers {
        latencies
            .add(&consumer.latencies)
            .context("merge consumer latency histogram")?;
    }

    Ok(CaseResult {
        topology,
        reader_api,
        producer,
        consumers,
        end_to_end,
        latencies,
    })
}

fn ring_capacity(messages: usize) -> Result<usize> {
    messages
        .checked_mul(FRAME_SIZE)
        .context("ring capacity overflowed usize")?
        .max(16)
        .checked_next_power_of_two()
        .context("ring capacity cannot be represented as a power of two")
}

fn message_type(sequence: usize) -> u32 {
    (sequence % CONSUMER_COUNT + 1) as u32
}

fn now_nanos(clock: &Instant) -> u64 {
    clock.elapsed().as_nanos() as u64
}

fn messages_per_second(messages: usize, elapsed: Duration) -> f64 {
    messages as f64 / elapsed.as_secs_f64()
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    numerator as f64 / denominator as f64
}

fn print_result(result: &CaseResult, total_messages: usize) {
    let polled = result.consumers.iter().map(|consumer| consumer.polled).sum::<usize>();
    let matched = result.consumers.iter().map(|consumer| consumer.matched).sum::<usize>();

    println!("{} / {}", result.topology.name(), result.reader_api.name());
    println!(
        "  producer: {:>10.3} ms, {:>12.0} published msg/s",
        result.producer.elapsed.as_secs_f64() * 1_000.0,
        messages_per_second(total_messages, result.producer.elapsed),
    );
    println!(
        "  end-to-end: {:>8.3} ms, {:>12.0} delivered msg/s",
        result.end_to_end.as_secs_f64() * 1_000.0,
        messages_per_second(matched, result.end_to_end),
    );
    println!(
        "  receiver work: {polled} frames polled, {:>12.0} polls/s",
        messages_per_second(polled, result.end_to_end),
    );
    println!(
        "  latency ns: min={} p50={} p90={} p99={} p99.9={} max={} samples={}",
        result.latencies.min(),
        result.latencies.value_at_percentile(50.0),
        result.latencies.value_at_percentile(90.0),
        result.latencies.value_at_percentile(99.0),
        result.latencies.value_at_percentile(99.9),
        result.latencies.max(),
        result.latencies.len(),
    );
    for consumer in &result.consumers {
        println!(
            "    type {}: polled={} matched={} p50={}ns p99={}ns",
            consumer.message_type,
            consumer.polled,
            consumer.matched,
            consumer.latencies.value_at_percentile(50.0),
            consumer.latencies.value_at_percentile(99.0),
        );
    }
    println!();
}
