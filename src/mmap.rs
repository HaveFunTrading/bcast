//! Memory mapped storage and convenience wrappers.
//!
//! The `mmap` feature provides two levels of API:
//!
//! - [`MmapStorage`] and [`MmapMutStorage`] are storage adapters for the generic
//!   [`Reader`] and [`Writer`] types.
//! - [`MappedReader`] is a convenience wrapper for opening a reader from a file
//!   path, while [`MappedWriter`] is an alias for `Writer<MmapMutStorage>`.
//!
//! The mapped file size must be `HEADER_SIZE + capacity`, where `capacity` is a
//! power of two and at least 16 bytes.
//!
//! Mappings are populated when they are created so page-table faults happen
//! during construction rather than on the reader or writer hot path. On Unix,
//! mappings are also locked into RAM for their full lifetime; construction
//! fails if the operating system refuses to lock the complete mapping.
//!
//! Every [`MmapMutStorage`] holds an exclusive sidecar lock at `<path>.lock` for
//! its full lifetime. Writable storage construction fails with
//! [`std::io::ErrorKind::WouldBlock`] if another independently opened writable
//! mapping already owns the writer lock. Readers do not take file locks.
//!
//! # Example
//!
//! ```no_run
//! use bcast::{HEADER_SIZE, MappedReader, MappedWriter, MmapMutStorage, StorageExt};
//!
//! # fn main() -> std::io::Result<()> {
//! let path = std::env::temp_dir().join("bcast-example.mmap");
//! let size = HEADER_SIZE + 1024;
//!
//! let mut writer: MappedWriter = MmapMutStorage::new(&path, size)?.into_writer();
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

use crate::{Reader, Storage, StorageExt, WriteStorage, Writer};
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
    /// The mapping is populated during construction and, on Unix, locked into
    /// RAM for its full lifetime.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be opened or mapped, or if the
    /// operating system refuses to lock the complete mapping into RAM.
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
        let mmap = unsafe { MmapOptions::new().populate().map(&file)? };
        #[cfg(unix)]
        mmap.lock()?;
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
/// This adapter owns a writable [`MmapMut`] and an exclusive sidecar writer lock
/// at `<path>.lock`. Convert it into [`MappedWriter`] with the methods on
/// [`StorageExt`].
pub struct MmapMutStorage {
    mmap: MmapMut,
    _writer_lock: File,
}

impl MmapMutStorage {
    /// Create a new writable mapped file of `size` bytes.
    ///
    /// The writer lock at `<path>.lock` is acquired before an existing mapped
    /// file is replaced. If the path already exists it is removed first. Parent
    /// directories are created when needed. The size must be valid for bcast
    /// ring construction: `HEADER_SIZE + capacity`, where `capacity` is a power
    /// of two and at least 16 bytes.
    ///
    /// The mapping is populated during construction and, on Unix, locked into
    /// RAM for its full lifetime.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the writer lock cannot be acquired, the file
    /// cannot be created, sized or mapped, or the mapping cannot be locked into
    /// RAM.
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
        let path = path.as_ref();
        let writer_lock = acquire_writer_lock(path)?;

        if path.exists() {
            std::fs::remove_file(path)?;
        }

        if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;

        file.set_len(size as u64)?;
        file.sync_all()?;

        let mmap = unsafe { MmapOptions::new().populate().map_mut(&file)? };
        #[cfg(unix)]
        mmap.lock()?;
        Ok(Self {
            mmap,
            _writer_lock: writer_lock,
        })
    }

    /// Open an existing file as writable ring storage.
    ///
    /// This does not initialize the ring header. Use [`Writer::join`] or
    /// [`Writer::join_with_cfg`] with the returned storage to continue writing to
    /// an existing channel.
    ///
    /// The mapping is populated during construction and, on Unix, locked into
    /// RAM for its full lifetime.
    ///
    /// The writer lock at `<path>.lock` is acquired before the mapped file is
    /// opened.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the writer lock cannot be acquired, the file
    /// cannot be opened or mapped, or the operating system refuses to lock the
    /// complete mapping into RAM.
    pub fn attach(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref();
        let writer_lock = acquire_writer_lock(path)?;
        let file = std::fs::OpenOptions::new().read(true).write(true).open(path)?;
        let mmap = unsafe { MmapOptions::new().populate().map_mut(&file)? };
        #[cfg(unix)]
        mmap.lock()?;
        Ok(Self {
            mmap,
            _writer_lock: writer_lock,
        })
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
/// Construct the writable storage with [`MmapMutStorage::new`] and initialize
/// it with [`StorageExt::into_writer`], or attach it with
/// [`MmapMutStorage::attach`] and continue from the existing producer position
/// with [`StorageExt::join_writer`].
///
/// # Example
///
/// ```no_run
/// use bcast::{HEADER_SIZE, MappedWriter, MmapMutStorage, StorageExt};
///
/// # fn main() -> std::io::Result<()> {
/// let path = std::env::temp_dir().join("bcast-writer-example.mmap");
/// let storage = MmapMutStorage::new(&path, HEADER_SIZE + 1024)?;
/// let mut writer: MappedWriter = storage.into_writer();
///
/// writer.publish(5, true, |payload| payload.copy_from_slice(b"hello"));
/// writer.send_with_user_defined(b"world", true, 42);
/// # let _ = std::fs::remove_file(path);
/// # Ok(())
/// # }
/// ```
pub type MappedWriter = Writer<MmapMutStorage>;

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
/// use bcast::{HEADER_SIZE, MappedReader, MmapMutStorage, StorageExt};
///
/// # fn main() -> std::io::Result<()> {
/// let path = std::env::temp_dir().join("bcast-reader-example.mmap");
/// let mut writer = MmapMutStorage::new(&path, HEADER_SIZE + 1024)?.into_writer();
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
    use crate::mmap::{MappedReader, MappedWriter, MmapMutStorage, writer_lock_path};
    use crate::{HEADER_SIZE, StorageExt};
    use std::io::ErrorKind;
    use tempfile::NamedTempFile;

    #[test]
    fn should_use_mapped_reader_and_writer() {
        const RING_BUFFER_SIZE: usize = HEADER_SIZE + 1024;

        let file = NamedTempFile::new().unwrap();

        let mut writer: MappedWriter = MmapMutStorage::new(&file, RING_BUFFER_SIZE).unwrap().into_writer();
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
            let mut writer: MappedWriter = MmapMutStorage::new(&file, RING_BUFFER_SIZE).unwrap().into_writer();
            writer.claim_with_user_defined(32, true, 100).commit();
            writer.claim_with_user_defined(32, true, 101).commit();
        }

        let mut writer: MappedWriter = MmapMutStorage::attach(&file).unwrap().join_writer();
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

        let mut writer: MappedWriter = MmapMutStorage::new(&file, RING_BUFFER_SIZE).unwrap().into_writer();
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
    fn should_reject_second_writable_mapping_while_lock_is_held() {
        const RING_BUFFER_SIZE: usize = HEADER_SIZE + 1024;

        let file = NamedTempFile::new().unwrap();

        let _writer: MappedWriter = MmapMutStorage::new(&file, RING_BUFFER_SIZE).unwrap().into_writer();
        let err = match MmapMutStorage::attach(&file) {
            Ok(_) => panic!("second writable mapping unexpectedly acquired the writer lock"),
            Err(err) => err,
        };

        assert_eq!(ErrorKind::WouldBlock, err.kind());
    }
}
