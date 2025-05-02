use bcast::error::Error;
use bcast::{Reader, RingBuffer, Writer, HEADER_SIZE};
use num_format::{Locale, ToFormattedString};
use std::mem::MaybeUninit;
use std::slice::from_raw_parts;
use std::time::{Duration, Instant};

const MSG_LENGTH_BYTES: usize = 32;

const RING_BUFFER_SIZE: usize = HEADER_SIZE + 1024 * 1024 * 256;

const PAYLOAD: [u8; 32] = [0u8; 32];

struct Publisher {
    writer: Writer,
}

impl Publisher {
    fn new(writer: Writer) -> Self {
        Self { writer }
    }

    fn run(&mut self) -> anyhow::Result<()> {
        loop {
            let mut claim = self.writer.claim(MSG_LENGTH_BYTES, true);
            claim.get_buffer_mut().copy_from_slice(&PAYLOAD);
        }
    }
}

struct Consumer {
    reader: Reader,
    msg_count: usize,
    overrun_count: usize,
    start_time: Instant,
}

impl Consumer {
    fn new(reader: Reader) -> Consumer {
        Self {
            reader,
            msg_count: 0,
            overrun_count: 0,
            start_time: Instant::now(),
        }
    }

    fn run(&mut self) -> anyhow::Result<()> {
        loop {
            for msg in self.reader.read_batch().into_iter().take(100) {
                let msg = msg?;
                self.msg_count += 1;
                let buffer = MaybeUninit::<[u8; MSG_LENGTH_BYTES]>::uninit();
                let mut buffer = unsafe { buffer.assume_init() };
                match msg.read(&mut buffer) {
                    Ok(_) => {}
                    Err(err) => {
                        if let Error::Overrun(_) = err {
                            self.overrun_count += 1;
                        }
                    }
                }
            }

            if self.msg_count >= 1_00_000 {
                let elapsed = self.start_time.elapsed().as_nanos() as u64;
                let messages_per_sec = (self.msg_count * 1000000000) as u64 / elapsed;

                println!(
                    "{}ms {} msgs/sec messages: {} overruns: {}",
                    Duration::from_nanos(elapsed).as_millis(),
                    messages_per_sec.to_formatted_string(&Locale::en),
                    self.msg_count,
                    self.overrun_count
                );
                self.msg_count = 0;
                self.overrun_count = 0;
                self.start_time = Instant::now();
            }
        }
    }
}

fn main() {
    let bytes = vec![0u8; RING_BUFFER_SIZE];
    let addr = bytes.as_ptr() as usize;

    let publisher_task = std::thread::spawn(move || {
        let bytes = unsafe { from_raw_parts(addr as *const u8, RING_BUFFER_SIZE) };
        let writer = RingBuffer::new(bytes).into_writer();
        let mut publisher = Publisher::new(writer);
        publisher.run().unwrap()
    });

    let consumer_task = std::thread::spawn(move || {
        let bytes = unsafe { from_raw_parts(addr as *const u8, RING_BUFFER_SIZE) };
        let reader = RingBuffer::new(bytes).into_reader();
        let mut consumer = Consumer::new(reader);
        consumer.run().unwrap()
    });

    publisher_task.join().unwrap();
    consumer_task.join().unwrap();
}
