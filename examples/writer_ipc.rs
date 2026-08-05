use crate::common::writer;
use bcast::{HEADER_SIZE, MmapMutStorage, StorageExt};
use std::path::Path;

mod common;

/// This example will create a memory mapped file and attach a writer to it.
/// If the file exists it will be removed so that any potential readers can detect message
/// loss and act accordingly.
fn main() -> anyhow::Result<()> {
    let path = std::env::current_dir()?.join(Path::new("test.dat"));
    let mut writer_handle = MmapMutStorage::new(path, HEADER_SIZE + 1024)?.into_writer();
    writer(&mut writer_handle);

    Ok(())
}
