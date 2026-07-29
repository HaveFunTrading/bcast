use bcast::{LocalStorage, StorageExt};
use std::mem::MaybeUninit;

const RING_CAPACITY: usize = 128;

fn publish<S>(writer: &mut bcast::Writer<S>, label: &str, user_defined: u32) {
    let mut claim = writer.claim_with_user_defined(label.len(), true, user_defined);
    claim.get_buffer_mut().copy_from_slice(label.as_bytes());
    claim.commit();
}

fn main() -> anyhow::Result<()> {
    let storage = LocalStorage::with_capacity(RING_CAPACITY).into_shared();

    {
        let mut writer = storage.clone().into_writer();

        // Each message occupies 48 bytes: 8 bytes frame header + 40 bytes aligned payload.
        // The third publish cannot fit in the remaining 32 bytes, so it inserts padding and
        // starts a new physical lap at position 128.
        publish(&mut writer, "lap-0-message-0-------------------------", 0);
        publish(&mut writer, "lap-0-message-1-------------------------", 1);
        publish(&mut writer, "lap-1-message-0-------------------------", 2);
    }

    let mut reader = storage.into_reader_at_last_lap();
    let mut payload = unsafe { MaybeUninit::new([0u8; RING_CAPACITY]).assume_init() };

    while let Some(msg) = reader.receive_next(&mut payload) {
        let msg = msg?;
        println!("{}: {}", msg.user_defined, String::from_utf8_lossy(msg.payload));
    }

    Ok(())
}
