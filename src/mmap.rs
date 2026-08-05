//! Memory mapped storage and convenience wrappers.
//!
//! The `mmap` feature provides two levels of API:
//!
//! - [`MmapStorage`] and [`MmapMutStorage`] are storage adapters for the generic
//!   [`Reader`] and [`Writer`] types.
//! - [`MappedReader`] and [`MappedWriter`] are aliases for readers and writers
//!   backed by the corresponding mapped storage types.
//!
//! The mapped file size must be `HEADER_SIZE + capacity`, where `capacity` is a
//! power of two and at least 16 bytes.
//!
//! Mapped channel paths must be absolute. Every process attached to a channel
//! must use the same path without symlink or hard-link aliases so writable
//! mappings contend on the same sidecar writer lock.
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
//! use bcast::{HEADER_SIZE, MappedReader, MappedWriter, MmapMutStorage, MmapStorage, StorageExt};
//!
//! # fn main() -> std::io::Result<()> {
//! let path = std::env::temp_dir().join("bcast-example.mmap");
//! let size = HEADER_SIZE + 1024;
//!
//! let mut writer: MappedWriter = MmapMutStorage::open_or_create(&path, size)?.into_writer();
//! let mut reader: MappedReader = MmapStorage::attach(&path)?.into_reader();
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

use crate::ring::{FrameHeader, HEADER_MAGIC, HEADER_VERSION, RingBuffer};
use crate::{HEADER_SIZE, Reader, Storage, WriteStorage, Writer, WriterConfig};
use memmap2::{Mmap, MmapMut, MmapOptions};
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::mem::{align_of, size_of};
use std::path::{Path, PathBuf};
use std::ptr::NonNull;
use std::sync::atomic::Ordering;

/// Read-only memory mapped ring storage.
///
/// This adapter owns a read-only [`Mmap`] and can be converted into a
/// [`MappedReader`] with the methods on [`crate::StorageExt`].
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
    /// operating system refuses to lock the complete mapping into RAM. Returns
    /// [`std::io::ErrorKind::InvalidInput`] if `path` is not absolute.
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
        let path = path.as_ref();
        validate_channel_path(path)?;
        let file = std::fs::OpenOptions::new().read(true).open(path)?;
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

/// Reader backed by a read-only memory mapped file.
///
/// Construct the read-only storage with [`MmapStorage::attach`] and convert it
/// with [`crate::StorageExt::into_reader`], [`crate::StorageExt::into_reader_at`],
/// or [`crate::StorageExt::into_reader_at_last_lap`].
///
/// # Example
///
/// ```no_run
/// use bcast::{HEADER_SIZE, MappedReader, MmapMutStorage, MmapStorage, StorageExt};
///
/// # fn main() -> std::io::Result<()> {
/// let path = std::env::temp_dir().join("bcast-reader-example.mmap");
/// let mut writer = MmapMutStorage::new(&path, HEADER_SIZE + 1024)?.into_writer();
/// writer.send(b"hello", true);
///
/// let mut reader: MappedReader = MmapStorage::attach(&path)?.into_reader_at(0);
/// let mut payload = [0u8; 16];
/// let msg = reader.receive_next(&mut payload).unwrap().unwrap();
/// assert_eq!(b"hello", msg.payload);
/// # let _ = std::fs::remove_file(path);
/// # Ok(())
/// # }
/// ```
pub type MappedReader = Reader<MmapStorage>;

/// Writable memory mapped ring storage.
///
/// This adapter owns a writable [`MmapMut`] and an exclusive sidecar writer lock
/// at `<path>.lock`. Convert it into [`MappedWriter`] with the methods on
/// [`crate::StorageExt`], or use [`MmapMutStorage::open_or_create`] when the
/// channel may already exist.
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
    /// RAM. Returns [`std::io::ErrorKind::InvalidInput`] if `path` is not
    /// absolute.
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

        Self::replace_locked(path, size, writer_lock)
    }

    fn replace_locked(path: &Path, size: usize, writer_lock: File) -> std::io::Result<Self> {
        if path.exists() {
            std::fs::remove_file(path)?;
        }

        Self::create_locked(path, size, writer_lock)
    }

    fn create_locked(path: &Path, size: usize, writer_lock: File) -> std::io::Result<Self> {
        if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new().read(true).write(true).create_new(true).open(path)?;

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
    /// complete mapping into RAM. Returns [`std::io::ErrorKind::InvalidInput`]
    /// if `path` is not absolute.
    pub fn attach(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref();
        let writer_lock = acquire_writer_lock(path)?;

        Self::attach_locked(path, writer_lock)
    }

    fn attach_locked(path: &Path, writer_lock: File) -> std::io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mmap = unsafe { MmapOptions::new().populate().map_mut(&file)? };
        #[cfg(unix)]
        mmap.lock()?;
        Ok(Self {
            mmap,
            _writer_lock: writer_lock,
        })
    }

    /// Open an existing mapped channel or create a new one when the path does
    /// not exist.
    ///
    /// The sidecar writer lock is acquired before the path is inspected. An
    /// existing file must have exactly `size` bytes and contain a ready bcast
    /// header with the supported format version. Existing files are never
    /// replaced by this operation.
    ///
    /// Convert the returned [`OpenedMmap`] into a writer with
    /// [`OpenedMmap::into_writer`] or [`OpenedMmap::into_writer_with_cfg`].
    ///
    /// # Errors
    ///
    /// Returns [`std::io::ErrorKind::InvalidInput`] when `path` is not absolute
    /// or `size` is not a valid bcast storage size, and
    /// [`std::io::ErrorKind::InvalidData`] when an existing file has a different
    /// size or an invalid channel header. Other file, mapping, memory-lock, and
    /// writer-lock failures are returned as I/O errors.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use bcast::{HEADER_SIZE, MmapMutStorage};
    ///
    /// # fn main() -> std::io::Result<()> {
    /// let path = std::env::temp_dir().join("bcast-open-or-create-example.mmap");
    /// let mut writer = MmapMutStorage::open_or_create(&path, HEADER_SIZE + 1024)?.into_writer();
    /// writer.send(b"hello", true);
    /// # let _ = std::fs::remove_file(path);
    /// # Ok(())
    /// # }
    /// ```
    pub fn open_or_create(path: impl AsRef<Path>, size: usize) -> std::io::Result<OpenedMmap> {
        validate_storage_size(size)?;

        let path = path.as_ref();
        let writer_lock = acquire_writer_lock(path)?;
        match path.metadata() {
            Ok(metadata) => {
                if metadata.len() != size as u64 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("mapped channel has {} bytes, expected {size}", metadata.len()),
                    ));
                }

                let storage = Self::attach_locked(path, writer_lock)?;
                validate_existing_channel(&storage)?;
                Ok(OpenedMmap::Existing(storage))
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                Self::create_locked(path, size, writer_lock).map(OpenedMmap::Created)
            }
            Err(err) => Err(err),
        }
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
/// it with [`crate::StorageExt::into_writer`], or attach it with
/// [`MmapMutStorage::attach`] and continue from the existing producer position
/// with [`crate::StorageExt::join_writer`]. When either state is acceptable,
/// [`MmapMutStorage::open_or_create`] records the correct operation in an
/// [`OpenedMmap`].
///
/// # Example
///
/// ```no_run
/// use bcast::{HEADER_SIZE, MappedWriter, MmapMutStorage};
///
/// # fn main() -> std::io::Result<()> {
/// let path = std::env::temp_dir().join("bcast-writer-example.mmap");
/// let mut writer: MappedWriter =
///     MmapMutStorage::open_or_create(&path, HEADER_SIZE + 1024)?.into_writer();
///
/// writer.publish(5, true, |payload| payload.copy_from_slice(b"hello"));
/// writer.send_with_user_defined(b"world", true, 42);
/// # let _ = std::fs::remove_file(path);
/// # Ok(())
/// # }
/// ```
pub type MappedWriter = Writer<MmapMutStorage>;

/// Result of opening or creating writable mapped storage.
///
/// The variant records whether [`OpenedMmap::into_writer`] should initialize a
/// new channel or join the producer position stored in an existing channel.
#[must_use = "convert the opened mapping into a writer to initialize or join the channel"]
pub enum OpenedMmap {
    /// Newly created, uninitialized mapped storage.
    Created(MmapMutStorage),
    /// Existing mapped storage containing a validated channel header.
    Existing(MmapMutStorage),
}

impl OpenedMmap {
    /// Convert this mapping into a writer, initializing or joining the channel
    /// according to how the mapping was opened.
    pub fn into_writer(self) -> MappedWriter {
        self.into_writer_with_cfg(|config| config)
    }

    /// Convert this mapping into a writer with custom configuration,
    /// initializing or joining the channel according to how it was opened.
    pub fn into_writer_with_cfg<F>(self, config: F) -> MappedWriter
    where
        F: FnOnce(WriterConfig) -> WriterConfig,
    {
        match self {
            Self::Created(storage) => Writer::new_with_cfg(storage, config),
            Self::Existing(storage) => Writer::join_with_cfg(storage, config),
        }
    }
}

fn validate_storage_size(size: usize) -> std::io::Result<()> {
    let Some(capacity) = size.checked_sub(HEADER_SIZE) else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "mapped channel size must include the complete bcast header",
        ));
    };

    if !capacity.is_power_of_two() || capacity < 2 * size_of::<FrameHeader>() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "mapped channel capacity must be a power of two and at least 16 bytes",
        ));
    }

    Ok(())
}

fn validate_existing_channel(storage: &MmapMutStorage) -> std::io::Result<()> {
    let ring = RingBuffer::from_storage(storage);
    let header = ring.header();
    if !header.is_ready() {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "mapped channel header is not initialized"));
    }
    if header.preamble.magic != HEADER_MAGIC {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid mapped channel header magic"));
    }
    if header.preamble.version != HEADER_VERSION {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unsupported mapped channel header version {}", header.preamble.version),
        ));
    }
    let producer_position = header.producer_position.load(Ordering::Relaxed);
    let claimed_position = header.claimed_position.load(Ordering::Relaxed);
    if !producer_position.is_multiple_of(align_of::<FrameHeader>()) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "mapped channel contains an unaligned producer position",
        ));
    }
    if !claimed_position.is_multiple_of(align_of::<FrameHeader>()) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "mapped channel contains an unaligned claimed position",
        ));
    }

    Ok(())
}

fn acquire_writer_lock(path: &Path) -> std::io::Result<File> {
    validate_channel_path(path)?;

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

fn validate_channel_path(path: &Path) -> std::io::Result<()> {
    if !path.is_absolute() {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "mapped channel path must be absolute"));
    }

    Ok(())
}

fn writer_lock_path(path: &Path) -> std::io::Result<PathBuf> {
    let file_name = path.file_name().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "mapped writer path must include a file name")
    })?;
    let mut lock_file_name = OsString::from(file_name);
    lock_file_name.push(".lock");
    Ok(path.with_file_name(lock_file_name))
}

#[cfg(test)]
mod tests {
    use crate::mmap::{MappedReader, MappedWriter, MmapMutStorage, MmapStorage, OpenedMmap, writer_lock_path};
    use crate::ring::RingBuffer;
    use crate::{HEADER_SIZE, StorageExt};
    use std::io::ErrorKind;
    use std::sync::atomic::Ordering;
    use tempfile::NamedTempFile;

    #[test]
    fn should_use_mapped_reader_and_writer() {
        const RING_BUFFER_SIZE: usize = HEADER_SIZE + 1024;

        let file = NamedTempFile::new().unwrap();

        let mut writer: MappedWriter = MmapMutStorage::new(&file, RING_BUFFER_SIZE).unwrap().into_writer();
        let mut reader: MappedReader = MmapStorage::attach(&file).unwrap().into_reader();

        writer.claim_with_user_defined(32, true, 100).commit();
        writer.claim_with_user_defined(32, true, 101).commit();

        let mut batch = reader.read_batch().unwrap();
        let mut payload = [0u8; 32];
        assert_eq!(100, batch.receive_next(&mut payload).unwrap().unwrap().user_defined);
        assert_eq!(101, batch.receive_next(&mut payload).unwrap().unwrap().user_defined);

        // attach another (late) reader
        let mut late_reader: MappedReader = MmapStorage::attach(&file).unwrap().into_reader_at(0);
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

        let mut reader: MappedReader = MmapStorage::attach(&file).unwrap().into_reader_at(0);
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

        let mut reader: MappedReader = MmapStorage::attach(&file).unwrap().into_reader_at_last_lap();
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

    #[test]
    fn should_create_missing_mapped_channel() {
        const RING_BUFFER_SIZE: usize = HEADER_SIZE + 1024;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("channel.bcast");

        let opened = MmapMutStorage::open_or_create(&path, RING_BUFFER_SIZE).unwrap();
        assert!(matches!(opened, OpenedMmap::Created(_)));
    }

    #[test]
    fn should_join_existing_mapped_channel() {
        const RING_BUFFER_SIZE: usize = HEADER_SIZE + 1024;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("channel.bcast");

        {
            let mut writer = MmapMutStorage::open_or_create(&path, RING_BUFFER_SIZE)
                .unwrap()
                .into_writer();
            writer.send_with_user_defined(b"one", true, 1);
        }

        let opened = MmapMutStorage::open_or_create(&path, RING_BUFFER_SIZE).unwrap();
        assert!(matches!(&opened, OpenedMmap::Existing(_)));
        let mut writer = opened.into_writer();
        writer.send_with_user_defined(b"two", true, 2);

        let mut reader = MmapStorage::attach(&path).unwrap().into_reader_at(0);
        let mut payload = [0; 8];
        assert_eq!(1, reader.receive_next(&mut payload).unwrap().unwrap().user_defined);
        assert_eq!(2, reader.receive_next(&mut payload).unwrap().unwrap().user_defined);
    }

    #[test]
    fn should_reject_existing_mapped_channel_with_different_size() {
        const RING_BUFFER_SIZE: usize = HEADER_SIZE + 1024;
        const OTHER_RING_BUFFER_SIZE: usize = HEADER_SIZE + 2048;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("channel.bcast");
        {
            let _writer = MmapMutStorage::open_or_create(&path, RING_BUFFER_SIZE)
                .unwrap()
                .into_writer();
        }

        let err = match MmapMutStorage::open_or_create(&path, OTHER_RING_BUFFER_SIZE) {
            Ok(_) => panic!("channel with a different size was accepted"),
            Err(err) => err,
        };
        assert_eq!(ErrorKind::InvalidData, err.kind());
        assert_eq!(RING_BUFFER_SIZE as u64, path.metadata().unwrap().len());
    }

    #[test]
    fn should_reject_existing_mapped_channel_with_invalid_header() {
        const RING_BUFFER_SIZE: usize = HEADER_SIZE + 1024;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("channel.bcast");
        let file = std::fs::File::create(&path).unwrap();
        file.set_len(RING_BUFFER_SIZE as u64).unwrap();

        let err = match MmapMutStorage::open_or_create(&path, RING_BUFFER_SIZE) {
            Ok(_) => panic!("channel with an invalid header was accepted"),
            Err(err) => err,
        };
        assert_eq!(ErrorKind::InvalidData, err.kind());
    }

    #[test]
    fn should_reject_invalid_open_or_create_size() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("channel.bcast");

        let err = match MmapMutStorage::open_or_create(&path, HEADER_SIZE + 1000) {
            Ok(_) => panic!("invalid channel size was accepted"),
            Err(err) => err,
        };
        assert_eq!(ErrorKind::InvalidInput, err.kind());
        assert!(!path.exists());
    }

    #[test]
    fn should_hold_writer_lock_after_open_or_create() {
        const RING_BUFFER_SIZE: usize = HEADER_SIZE + 1024;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("channel.bcast");
        let opened = MmapMutStorage::open_or_create(&path, RING_BUFFER_SIZE).unwrap();

        let err = match MmapMutStorage::attach(&path) {
            Ok(_) => panic!("second writable mapping unexpectedly acquired the writer lock"),
            Err(err) => err,
        };
        assert_eq!(ErrorKind::WouldBlock, err.kind());
        drop(opened);
    }

    #[test]
    fn should_align_default_claim_reservation() {
        const RING_BUFFER_SIZE: usize = HEADER_SIZE + 64;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("channel.bcast");
        {
            let mut writer = MmapMutStorage::open_or_create(&path, RING_BUFFER_SIZE)
                .unwrap()
                .into_writer();
            writer.send(b"x", true);
        }

        {
            let storage = MmapMutStorage::attach(&path).unwrap();
            let claimed_position = RingBuffer::from_storage(&storage)
                .header()
                .claimed_position
                .load(Ordering::Relaxed);
            assert!(claimed_position.is_multiple_of(8));
        }

        let opened = MmapMutStorage::open_or_create(&path, RING_BUFFER_SIZE).unwrap();
        assert!(matches!(opened, OpenedMmap::Existing(_)));
    }

    #[test]
    fn should_align_custom_claim_reservation() {
        const RING_BUFFER_SIZE: usize = HEADER_SIZE + 1024;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("channel.bcast");
        {
            let mut writer = MmapMutStorage::open_or_create(&path, RING_BUFFER_SIZE)
                .unwrap()
                .into_writer_with_cfg(|config| config.claim_reserve_ratio(0.0001));
            writer.send(b"x", true);
        }

        {
            let storage = MmapMutStorage::attach(&path).unwrap();
            let claimed_position = RingBuffer::from_storage(&storage)
                .header()
                .claimed_position
                .load(Ordering::Relaxed);
            assert!(claimed_position.is_multiple_of(8));
        }

        let opened = MmapMutStorage::open_or_create(&path, RING_BUFFER_SIZE).unwrap();
        assert!(matches!(opened, OpenedMmap::Existing(_)));
    }

    #[test]
    fn should_reject_existing_channel_with_unaligned_producer_position() {
        const RING_BUFFER_SIZE: usize = HEADER_SIZE + 1024;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("channel.bcast");
        {
            let _writer = MmapMutStorage::open_or_create(&path, RING_BUFFER_SIZE)
                .unwrap()
                .into_writer();
        }
        {
            let storage = MmapMutStorage::attach(&path).unwrap();
            RingBuffer::from_storage(&storage)
                .header()
                .producer_position
                .store(1, Ordering::Relaxed);
        }

        let err = match MmapMutStorage::open_or_create(&path, RING_BUFFER_SIZE) {
            Ok(_) => panic!("channel with an unaligned producer position was accepted"),
            Err(err) => err,
        };
        assert_eq!(ErrorKind::InvalidData, err.kind());
    }

    #[test]
    fn should_reject_existing_channel_with_unaligned_claimed_position() {
        const RING_BUFFER_SIZE: usize = HEADER_SIZE + 1024;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("channel.bcast");
        {
            let _writer = MmapMutStorage::open_or_create(&path, RING_BUFFER_SIZE)
                .unwrap()
                .into_writer();
        }
        {
            let storage = MmapMutStorage::attach(&path).unwrap();
            RingBuffer::from_storage(&storage)
                .header()
                .claimed_position
                .store(1, Ordering::Relaxed);
        }

        let err = match MmapMutStorage::open_or_create(&path, RING_BUFFER_SIZE) {
            Ok(_) => panic!("channel with an unaligned claimed position was accepted"),
            Err(err) => err,
        };
        assert_eq!(ErrorKind::InvalidData, err.kind());
    }

    #[test]
    fn should_reject_relative_mapped_channel_paths() {
        const RING_BUFFER_SIZE: usize = HEADER_SIZE + 1024;

        let reader_err = match MmapStorage::attach("channel.bcast") {
            Ok(_) => panic!("relative reader path was accepted"),
            Err(err) => err,
        };
        assert_eq!(ErrorKind::InvalidInput, reader_err.kind());

        let new_writer_err = match MmapMutStorage::new("channel.bcast", RING_BUFFER_SIZE) {
            Ok(_) => panic!("relative new-writer path was accepted"),
            Err(err) => err,
        };
        assert_eq!(ErrorKind::InvalidInput, new_writer_err.kind());

        let attached_writer_err = match MmapMutStorage::attach("channel.bcast") {
            Ok(_) => panic!("relative attached-writer path was accepted"),
            Err(err) => err,
        };
        assert_eq!(ErrorKind::InvalidInput, attached_writer_err.kind());

        let open_or_create_err = match MmapMutStorage::open_or_create("channel.bcast", RING_BUFFER_SIZE) {
            Ok(_) => panic!("relative open-or-create path was accepted"),
            Err(err) => err,
        };
        assert_eq!(ErrorKind::InvalidInput, open_or_create_err.kind());
    }
}
