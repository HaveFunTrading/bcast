//! Memory mapped storage and convenience wrappers.
//!
//! The `mmap` feature provides two levels of API:
//!
//! - [`MmapStorage`] and [`MmapMutStorage`] are storage adapters for the generic
//!   [`Reader`] and [`Writer`] types.
//! - [`MappedReader`] and [`MappedWriter`] are convenience wrappers that open the
//!   memory map from a file path.
//!
//! The mapped file size must be `HEADER_SIZE + capacity`, where `capacity` is a
//! power of two and at least 16 bytes.
//!
//! `MappedWriter` also holds an exclusive sidecar lock at `<path>.lock` for its
//! full lifetime. Writer construction fails with [`std::io::ErrorKind::WouldBlock`]
//! if another process already owns the writer lock. Readers do not take file
//! locks.
//!
//! # Example
//!
//! ```no_run
//! use bcast::{HEADER_SIZE, MappedReader, MappedWriter};
//!
//! # fn main() -> std::io::Result<()> {
//! let path = std::env::temp_dir().join("bcast-example.mmap");
//! let size = HEADER_SIZE + 1024;
//!
//! let mut writer = MappedWriter::new(&path, size)?;
//! let mut reader = MappedReader::new(&path)?;
//!
//! writer.send(b"hello", true);
//!
//! let mut payload = [0u8; 16];
//! let msg = reader.receive_next(&mut payload).unwrap().unwrap();
//! assert_eq!(b"hello", msg.payload);
//! # let _ = std::fs::remove_file(path);
//! # Ok(())
//! # }
//! ```

use crate::{Reader, Storage, StorageExt, WriteStorage, Writer, WriterConfig};
use memmap2::{Mmap, MmapMut, MmapOptions};
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::hint;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::ptr::NonNull;

/// Read-only memory mapped ring storage.
///
/// This adapter owns a read-only [`Mmap`] and can be used with [`Reader`]. Use it
/// when you want the generic reader API rather than the [`MappedReader`]
/// convenience wrapper.
pub struct MmapStorage {
    mmap: Mmap,
}

impl MmapStorage {
    /// Open an existing file as read-only ring storage.
    ///
    /// The file must already contain an initialized bcast ring. Readers wait for
    /// the ring header through [`Reader::new`] or [`Reader::new_at_last_lap`],
    /// not here.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use bcast::{MmapStorage, StorageExt};
    ///
    /// # fn main() -> std::io::Result<()> {
    /// let storage = MmapStorage::attach("/tmp/channel.bcast")?;
    /// let reader = storage.into_reader();
    /// let _metadata = reader.metadata();
    /// # Ok(())
    /// # }
    /// ```
    pub fn attach(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new().read(true).open(&path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        Ok(Self { mmap })
    }
}

unsafe impl Storage for MmapStorage {
    #[inline]
    fn ptr(&self) -> NonNull<u8> {
        NonNull::new(self.mmap.as_ptr() as *mut u8).unwrap()
    }

    #[inline]
    fn len(&self) -> usize {
        self.mmap.len()
    }
}

/// Writable memory mapped ring storage.
///
/// This adapter owns a writable [`MmapMut`] and can be used with [`Writer`]. Use
/// it when you want the generic writer API rather than the [`MappedWriter`]
/// convenience wrapper.
///
/// This low-level storage adapter does not take a writer lock. Use
/// [`MappedWriter`] when opening by path and you want the crate to enforce
/// single-writer ownership with a sidecar `<path>.lock` file.
pub struct MmapMutStorage {
    mmap: MmapMut,
}

impl MmapMutStorage {
    /// Create a new writable mapped file of `size` bytes.
    ///
    /// If the path already exists it is removed first. Parent directories are
    /// created when needed. The size must be valid for bcast ring construction:
    /// `HEADER_SIZE + capacity`, where `capacity` is a power of two and at least
    /// 16 bytes.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use bcast::{HEADER_SIZE, MmapMutStorage, StorageExt};
    ///
    /// # fn main() -> std::io::Result<()> {
    /// let path = std::env::temp_dir().join("bcast-storage-example.mmap");
    /// let storage = MmapMutStorage::new(&path, HEADER_SIZE + 1024)?;
    /// let mut writer = storage.into_writer();
    ///
    /// writer.send(b"hello", true);
    /// # let _ = std::fs::remove_file(path);
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(path: impl AsRef<Path>, size: usize) -> std::io::Result<Self> {
        if path.as_ref().exists() {
            std::fs::remove_file(path.as_ref())?;
        }

        if let Some(parent) = path.as_ref().parent().filter(|parent| !parent.as_os_str().is_empty()) {
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
        Ok(Self { mmap })
    }

    /// Open an existing file as writable ring storage.
    ///
    /// This does not initialize the ring header. Use [`Writer::join`] or
    /// [`Writer::join_with_cfg`] with the returned storage to continue writing to
    /// an existing channel.
    ///
    /// This does not take a writer lock. Use [`MappedWriter::join`] if the
    /// writer should be protected by the standard sidecar lock.
    pub fn attach(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new().read(true).write(true).open(path)?;
        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };
        Ok(Self { mmap })
    }
}

unsafe impl Storage for MmapMutStorage {
    #[inline]
    fn ptr(&self) -> NonNull<u8> {
        NonNull::new(self.mmap.as_ptr() as *mut u8).unwrap()
    }

    #[inline]
    fn len(&self) -> usize {
        self.mmap.len()
    }
}

unsafe impl WriteStorage for MmapMutStorage {}

/// Writer backed by a writable memory mapped file.
///
/// `MappedWriter` dereferences to [`Writer<MmapMutStorage>`], so the normal
/// writer API is available directly.
///
/// The writer owns an exclusive sidecar lock at `<path>.lock` for its full
/// lifetime. Creating or joining a writer fails with
/// [`std::io::ErrorKind::WouldBlock`] if another writer already holds that lock.
///
/// # Example
///
/// ```no_run
/// use bcast::{HEADER_SIZE, MappedWriter};
///
/// # fn main() -> std::io::Result<()> {
/// let path = std::env::temp_dir().join("bcast-writer-example.mmap");
/// let mut writer = MappedWriter::new(&path, HEADER_SIZE + 1024)?;
///
/// writer.publish(5, true, |payload| payload.copy_from_slice(b"hello"));
/// writer.send_with_user_defined(b"world", true, 42);
/// # let _ = std::fs::remove_file(path);
/// # Ok(())
/// # }
/// ```
pub struct MappedWriter {
    _writer_lock: File,
    writer: Writer<MmapMutStorage>,
}

impl Deref for MappedWriter {
    type Target = Writer<MmapMutStorage>;

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
    /// Create a writer backed by a new memory mapped file of `size` bytes.
    ///
    /// If the file already exists it is removed. To continue writing to an
    /// existing file, use [`MappedWriter::join`] instead. The writer lock at
    /// `<path>.lock` is acquired before the mapped file is replaced.
    pub fn new(path: impl AsRef<Path>, size: usize) -> std::io::Result<Self> {
        Self::new_with_cfg(path, size, |config| config)
    }

    /// Create a writer backed by a new memory mapped file using custom writer
    /// configuration.
    ///
    /// If the file already exists it is removed. The writer lock at
    /// `<path>.lock` is acquired before the mapped file is replaced.
    pub fn new_with_cfg<F: FnOnce(WriterConfig) -> WriterConfig>(
        path: impl AsRef<Path>,
        size: usize,
        config: F,
    ) -> std::io::Result<Self> {
        let writer_lock = acquire_writer_lock(path.as_ref())?;
        Self::new_with_cfg_locked(path, size, config, writer_lock)
    }

    fn new_with_cfg_locked<F: FnOnce(WriterConfig) -> WriterConfig>(
        path: impl AsRef<Path>,
        size: usize,
        config: F,
        writer_lock: File,
    ) -> std::io::Result<Self> {
        Ok(Self {
            _writer_lock: writer_lock,
            writer: MmapMutStorage::new(path, size)?.into_writer_with_cfg(config),
        })
    }

    /// Open an existing mapped file and continue writing from the most recent
    /// producer position.
    ///
    /// The writer lock at `<path>.lock` is acquired before the mapped file is
    /// opened.
    pub fn join(path: impl AsRef<Path>) -> std::io::Result<Self> {
        Self::join_with_cfg(path, |config| config)
    }

    /// Open an existing mapped file with custom writer configuration and continue
    /// writing from the most recent producer position.
    ///
    /// The writer lock at `<path>.lock` is acquired before the mapped file is
    /// opened.
    pub fn join_with_cfg<F: FnOnce(WriterConfig) -> WriterConfig>(
        path: impl AsRef<Path>,
        config: F,
    ) -> std::io::Result<Self> {
        let writer_lock = acquire_writer_lock(path.as_ref())?;
        Self::join_with_cfg_locked(path, config, writer_lock)
    }

    fn join_with_cfg_locked<F: FnOnce(WriterConfig) -> WriterConfig>(
        path: impl AsRef<Path>,
        config: F,
        writer_lock: File,
    ) -> std::io::Result<Self> {
        Ok(Self {
            _writer_lock: writer_lock,
            writer: MmapMutStorage::attach(path)?.join_writer_with_cfg(config),
        })
    }

    /// Join an existing mapped file when it exists with `size` bytes, otherwise
    /// create a new mapped writer.
    ///
    /// If the path exists with a different size, the file is replaced. The
    /// writer lock at `<path>.lock` is acquired before the existing file is
    /// inspected or replaced.
    pub fn join_or_create(path: impl AsRef<Path>, size: usize) -> std::io::Result<Self> {
        Self::join_or_create_with_cfg(path, size, |config| config)
    }

    /// Join an existing mapped file with custom writer configuration when it
    /// exists with `size` bytes, otherwise create a new mapped writer.
    ///
    /// If the path exists with a different size, the file is replaced. The
    /// writer lock at `<path>.lock` is acquired before the existing file is
    /// inspected or replaced.
    pub fn join_or_create_with_cfg<F: FnOnce(WriterConfig) -> WriterConfig>(
        path: impl AsRef<Path>,
        size: usize,
        config: F,
    ) -> std::io::Result<Self> {
        let path = path.as_ref();
        let writer_lock = acquire_writer_lock(path)?;
        match path.exists() {
            true => {
                let file_len = path.metadata()?.len() as usize;
                match file_len == size {
                    true => Self::join_with_cfg_locked(path, config, writer_lock),
                    false => Self::new_with_cfg_locked(path, size, config, writer_lock),
                }
            }
            false => Self::new_with_cfg_locked(path, size, config, writer_lock),
        }
    }
}

fn acquire_writer_lock(path: &Path) -> std::io::Result<File> {
    if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }

    let lock_path = writer_lock_path(path)?;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path)?;
    lock.try_lock()?;
    Ok(lock)
}

fn writer_lock_path(path: &Path) -> std::io::Result<PathBuf> {
    let file_name = path.file_name().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "mapped writer path must include a file name")
    })?;
    let mut lock_file_name = OsString::from(file_name);
    lock_file_name.push(".lock");
    Ok(path.with_file_name(lock_file_name))
}

/// Reader backed by a read-only memory mapped file.
///
/// `MappedReader` dereferences to [`Reader<MmapStorage>`], so the normal reader
/// API is available directly.
///
/// # Example
///
/// ```no_run
/// use bcast::{HEADER_SIZE, MappedReader, MappedWriter};
///
/// # fn main() -> std::io::Result<()> {
/// let path = std::env::temp_dir().join("bcast-reader-example.mmap");
/// let mut writer = MappedWriter::new(&path, HEADER_SIZE + 1024)?;
/// writer.send(b"hello", true);
///
/// let mut reader = MappedReader::new_with_position(&path, 0)?;
/// let mut payload = [0u8; 16];
/// let msg = reader.receive_next(&mut payload).unwrap().unwrap();
/// assert_eq!(b"hello", msg.payload);
/// # let _ = std::fs::remove_file(path);
/// # Ok(())
/// # }
/// ```
pub struct MappedReader {
    reader: Reader<MmapStorage>,
}

impl Deref for MappedReader {
    type Target = Reader<MmapStorage>;

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
    /// Open a reader with its initial position set to the producer's current
    /// position.
    ///
    /// This is suitable for consumers that only want messages published after
    /// they attach. The call waits until the mapped file has non-zero length;
    /// the underlying [`Reader`] waits until the ring header is initialized.
    pub fn new(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new().read(true).open(&path)?;
        // wait until file has been initialised
        loop {
            let len = file.metadata()?.len() as usize;
            if len > 0 {
                break;
            }
            hint::spin_loop()
        }
        Ok(Self {
            reader: MmapStorage::attach(path)?.into_reader(),
        })
    }

    /// Open a reader at a specific stream position.
    ///
    /// `position` must be aligned to the frame alignment used by the ring.
    pub fn new_with_position(path: impl AsRef<Path>, position: usize) -> std::io::Result<Self> {
        Ok(Self {
            reader: MmapStorage::attach(path)?.into_reader_at(position),
        })
    }

    /// Open a reader at the start of the most recent physical ring lap when that
    /// position is still retained.
    ///
    /// If the lap start has already been overwritten, the reader starts at the
    /// producer's current position instead.
    pub fn new_at_last_lap(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new().read(true).open(&path)?;
        // wait until file has been initialised
        loop {
            let len = file.metadata()?.len() as usize;
            if len > 0 {
                break;
            }
            hint::spin_loop()
        }
        Ok(Self {
            reader: MmapStorage::attach(path)?.into_reader_at_last_lap(),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::HEADER_SIZE;
    use crate::mmap::{MappedReader, MappedWriter, writer_lock_path};
    use std::io::ErrorKind;
    use tempfile::NamedTempFile;

    #[test]
    fn should_use_mapped_reader_and_writer() {
        const RING_BUFFER_SIZE: usize = HEADER_SIZE + 1024;

        let file = NamedTempFile::new().unwrap();

        let mut writer = MappedWriter::new(&file, RING_BUFFER_SIZE).unwrap();
        let mut reader = MappedReader::new(&file).unwrap();

        writer.claim_with_user_defined(32, true, 100).commit();
        writer.claim_with_user_defined(32, true, 101).commit();

        let mut batch = reader.read_batch().unwrap();
        let mut payload = [0u8; 32];
        assert_eq!(100, batch.receive_next(&mut payload).unwrap().unwrap().user_defined);
        assert_eq!(101, batch.receive_next(&mut payload).unwrap().unwrap().user_defined);

        // attach another (late) reader
        let mut late_reader = MappedReader::new_with_position(&file, 0).unwrap();
        let mut batch = late_reader.read_batch().unwrap();
        assert_eq!(100, batch.receive_next(&mut payload).unwrap().unwrap().user_defined);
        assert_eq!(101, batch.receive_next(&mut payload).unwrap().unwrap().user_defined);
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

        let mut reader = MappedReader::new_with_position(&file, 0).unwrap();
        let mut batch = reader.read_batch().unwrap();
        let mut payload = [0u8; 32];
        assert_eq!(100, batch.receive_next(&mut payload).unwrap().unwrap().user_defined);
        assert_eq!(101, batch.receive_next(&mut payload).unwrap().unwrap().user_defined);
        assert_eq!(102, batch.receive_next(&mut payload).unwrap().unwrap().user_defined);
        assert!(batch.receive_next(&mut payload).is_none());
    }

    #[test]
    fn should_use_mapped_reader_at_last_lap() {
        const RING_BUFFER_SIZE: usize = HEADER_SIZE + 1024;

        let file = NamedTempFile::new().unwrap();

        let mut writer = MappedWriter::new(&file, RING_BUFFER_SIZE).unwrap();
        writer.claim_with_user_defined(504, true, 100).commit();
        writer.claim_with_user_defined(504, true, 101).commit();
        writer.claim_with_user_defined(16, true, 102).commit();

        let mut reader = MappedReader::new_at_last_lap(&file).unwrap();
        let mut payload = [0u8; 16];
        assert_eq!(102, reader.receive_next(&mut payload).unwrap().unwrap().user_defined);
        assert!(reader.receive_next(&mut payload).is_none());
    }

    #[test]
    fn should_use_sidecar_writer_lock_path() {
        let lock_path = writer_lock_path(std::path::Path::new("/tmp/channel.bcast")).unwrap();
        assert_eq!(std::path::Path::new("/tmp/channel.bcast.lock"), lock_path);
    }

    #[test]
    fn should_reject_second_mapped_writer_while_lock_is_held() {
        const RING_BUFFER_SIZE: usize = HEADER_SIZE + 1024;

        let file = NamedTempFile::new().unwrap();

        let _writer = MappedWriter::new(&file, RING_BUFFER_SIZE).unwrap();
        let err = match MappedWriter::join(&file) {
            Ok(_) => panic!("second mapped writer unexpectedly acquired the writer lock"),
            Err(err) => err,
        };

        assert_eq!(ErrorKind::WouldBlock, err.kind());
    }
}
