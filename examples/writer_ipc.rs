use crate::common::writer;
use bcast::HEADER_SIZE;
use memmap2::MmapOptions;
use std::fs::{OpenOptions, remove_file};
use std::path::Path;

mod common;

/// This example will create a memory mapped file and attach a writer to it.
/// If the file exists it will be removed so that any potential readers can detect message
/// loss and act accordingly.
fn main() -> anyhow::Result<()> {
    let path = Path::new("test.dat");
    if path.exists() {
        println!("removing {}", path.display());
        remove_file(path)?;
    }

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open("test.dat")?;
    file.set_len((HEADER_SIZE + 1024) as u64)?;
    file.sync_all()?;

    let mmap = unsafe { MmapOptions::new().map_mut(&file)? };
    let bytes = mmap.as_ref();

    writer(bytes);

    Ok(())
}
