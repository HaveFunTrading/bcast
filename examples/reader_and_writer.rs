use crate::common::{reader, writer};
use bcast::{LocalStorage, StorageExt};

mod common;

// This example will demonstrate the use shared buffer in order to achieve in-process communication
// between reader and writer that are running on separate threads.

fn main() -> anyhow::Result<()> {
    let storage = LocalStorage::with_capacity(1024).into_shared();

    let writer_storage = storage.clone();
    let writer_task = std::thread::spawn(move || {
        let mut writer_handle = writer_storage.into_writer();
        writer(&mut writer_handle);
    });

    let reader_task = std::thread::spawn(move || {
        // delay for a bit so that we are not joining from position 0
        std::thread::sleep(std::time::Duration::from_secs(1));
        let reader_handle = storage.into_reader();
        reader(&reader_handle).unwrap();
    });

    writer_task.join().unwrap();
    reader_task.join().unwrap();

    Ok(())
}
