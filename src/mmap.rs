//! Provides wrappers for `Reader` and `Writer` to work with memory mapped files.

use crate::{Reader, RingBuffer, Writer};
use memmap2::{Mmap, MmapMut, MmapOptions};
use std::ops::{Deref, DerefMut};
use std::path::Path;

/// Writer backed by memory mapped object.
pub struct MappedWriter {
    writer: Writer,
    #[allow(dead_code)]
    mmap: MmapMut,
}

impl Deref for MappedWriter {
    type Target = Writer;

    fn deref(&self) -> &Self::Target {
        &self.writer
    }
}

impl DerefMut for MappedWriter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.writer
    }
}

impl MappedWriter {
    /// Construct writer backed by memory mapped file of certain size. If the file already
    /// exists it will be removed. If you need to continue writing to existing file use
    /// `MappedWriter::join` instead.
    pub fn new(path: impl AsRef<Path>, size: usize) -> std::io::Result<Self> {
        if path.as_ref().exists() {
            std::fs::remove_file(path.as_ref())?;
        }

        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;

        file.set_len(size as u64)?;
        file.sync_all()?;

        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };
        let bytes = mmap.as_ref();
        Ok(Self {
            writer: RingBuffer::new(bytes).into_writer(),
            mmap,
        })
    }

    /// Construct writer backed by memory mapped file and continue writing from the most
    /// recent position. It assumes the file already exists.
    pub fn join(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new().read(true).write(true).open(path)?;
        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };
        let bytes = mmap.as_ref();
        Ok(Self {
            writer: RingBuffer::new(bytes).join_writer(),
            mmap,
        })
    }
}

/// Reader backed by memory mapped object.
pub struct MappedReader {
    reader: Reader,
    #[allow(dead_code)]
    mmap: Mmap,
}

impl Deref for MappedReader {
    type Target = Reader;

    fn deref(&self) -> &Self::Target {
        &self.reader
    }
}

impl DerefMut for MappedReader {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.reader
    }
}

impl MappedReader {
    /// Construct reader backed by memory mapped file with initial position set to producer
    /// most recent position.
    pub fn new(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new().read(true).open(path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        let bytes = mmap.as_ref();
        Ok(Self {
            reader: RingBuffer::new(bytes).into_reader(),
            mmap,
        })
    }

    /// Construct reader backed by memory mapped file with specific initial position.
    pub fn new_with_position(path: impl AsRef<Path>, position: usize) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new().read(true).open(path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        let bytes = mmap.as_ref();
        Ok(Self {
            reader: RingBuffer::new(bytes).into_reader().with_initial_position(position),
            mmap,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::mmap::{MappedReader, MappedWriter};
    use crate::HEADER_SIZE;
    use tempfile::NamedTempFile;

    #[test]
    fn should_use_mapped_reader_and_writer() {
        const RING_BUFFER_SIZE: usize = HEADER_SIZE + 1024;

        let file = NamedTempFile::new().unwrap();

        let mut writer = MappedWriter::new(&file, RING_BUFFER_SIZE).unwrap();
        let reader = MappedReader::new(&file).unwrap();

        writer.claim_with_user_defined(32, true, 100).commit();
        writer.claim_with_user_defined(32, true, 101).commit();

        let mut iter = reader.read_batch().unwrap().into_iter();
        assert_eq!(100, iter.next().unwrap().unwrap().user_defined);
        assert_eq!(101, iter.next().unwrap().unwrap().user_defined);

        // attach another (late) reader
        let late_reader = MappedReader::new_with_position(&file, 0).unwrap();
        let mut iter = late_reader.read_batch().unwrap().into_iter();
        assert_eq!(100, iter.next().unwrap().unwrap().user_defined);
        assert_eq!(101, iter.next().unwrap().unwrap().user_defined);
    }

    #[test]
    fn should_use_writer_join() {
        const RING_BUFFER_SIZE: usize = HEADER_SIZE + 1024;

        let file = NamedTempFile::new().unwrap();

        {
            let mut writer = MappedWriter::new(&file, RING_BUFFER_SIZE).unwrap();
            writer.claim_with_user_defined(32, true, 100).commit();
            writer.claim_with_user_defined(32, true, 101).commit();
        }

        let mut writer = MappedWriter::join(&file).unwrap();
        writer.claim_with_user_defined(32, true, 102).commit();

        let reader = MappedReader::new_with_position(&file, 0).unwrap();
        let mut iter = reader.read_batch().unwrap().into_iter();
        assert_eq!(100, iter.next().unwrap().unwrap().user_defined);
        assert_eq!(101, iter.next().unwrap().unwrap().user_defined);
        assert_eq!(102, iter.next().unwrap().unwrap().user_defined);
        assert!(iter.next().is_none());
    }
}
