use bcast::{HEADER_SIZE, RingBuffer};
use std::mem::MaybeUninit;

const RING_CAPACITY: usize = 128;
const RING_BUFFER_SIZE: usize = HEADER_SIZE + RING_CAPACITY;

fn publish(writer: &bcast::Writer, label: &str, user_defined: u32) {
    let mut claim = writer.claim_with_user_defined(label.len(), true, user_defined);
    claim.get_buffer_mut().copy_from_slice(label.as_bytes());
    claim.commit();
}

fn main() -> anyhow::Result<()> {
    let bytes = bcast::util::AlignedBytes::<RING_BUFFER_SIZE>::new();

    {
        let writer = RingBuffer::new(&bytes).into_writer();

        // Each message occupies 48 bytes: 8 bytes frame header + 40 bytes aligned payload.
        // The third publish cannot fit in the remaining 32 bytes, so it inserts padding and
        // starts a new physical lap at position 128.
        publish(&writer, "lap-0-message-0-------------------------", 0);
        publish(&writer, "lap-0-message-1-------------------------", 1);
        publish(&writer, "lap-1-message-0-------------------------", 2);
    }

    let reader = RingBuffer::new(&bytes).into_reader_at_last_lap();
    let mut payload = unsafe { MaybeUninit::new([0u8; RING_CAPACITY]).assume_init() };

    while let Some(msg) = reader.receive_next() {
        let msg = msg?;
        let len = msg.read(&mut payload)?;
        println!("{}: {}", msg.user_defined, String::from_utf8_lossy(&payload[..len]));
    }

    Ok(())
}
