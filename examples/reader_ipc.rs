use crate::common::reader;
use bcast::{MmapStorage, StorageExt};
use std::fs::OpenOptions;

mod common;

/// This example assumes a memory mapped file has already been created and will
/// attach a reader to it. The memory mapped file must be initialised (it's length set) by the
/// writer before it can be used.
fn main() -> anyhow::Result<()> {
    let path = std::env::current_dir()?.join("test.dat");
    let file = OpenOptions::new().read(true).open(&path)?;

    // wait until file has been initialised
    loop {
        let len = file.metadata()?.len() as usize;
        if len > 0 {
            break;
        }
    }

    let mut reader_handle: bcast::MappedReader = MmapStorage::attach(path)?.into_reader();
    reader(&mut reader_handle)?;

    Ok(())
}
