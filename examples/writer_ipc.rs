use crate::common::writer;
use bcast::HEADER_SIZE;
use std::path::Path;

mod common;

/// This example will create a memory mapped file and attach a writer to it.
/// If the file exists it will be removed so that any potential readers can detect message
/// loss and act accordingly.
fn main() -> anyhow::Result<()> {
    let path = Path::new("test.dat");
    let mut writer_handle = bcast::MappedWriter::new(path, HEADER_SIZE + 1024)?;
    writer(&mut writer_handle);

    Ok(())
}
