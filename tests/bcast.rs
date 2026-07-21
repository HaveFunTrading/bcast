use bcast::{
    Batch, BulkIter, Error, HEADER_SIZE, LocalStorage, METADATA_BUFFER_SIZE, Reader, SharedStorage, Storage,
    StorageExt, USER_DEFINED_NULL_VALUE, Writer, WriterConfig,
};
use rand::{Rng, thread_rng};

type SharedLocalStorage = SharedStorage<LocalStorage>;

fn shared_storage<const CAPACITY: usize>() -> SharedLocalStorage {
    LocalStorage::with_capacity(CAPACITY).into_shared()
}

fn writer_and_reader<const CAPACITY: usize>() -> (Writer<SharedLocalStorage>, Reader<SharedLocalStorage>) {
    let storage = shared_storage::<CAPACITY>();
    let writer = storage.clone().into_writer();
    let reader = storage.into_reader();
    (writer, reader)
}

fn storage_writer_and_reader<const CAPACITY: usize>()
-> (SharedLocalStorage, Writer<SharedLocalStorage>, Reader<SharedLocalStorage>) {
    let storage = shared_storage::<CAPACITY>();
    let writer = storage.clone().into_writer();
    let reader = storage.clone().into_reader();
    (storage, writer, reader)
}

fn writer_and_reader_at<const CAPACITY: usize>(
    position: usize,
) -> (Writer<SharedLocalStorage>, Reader<SharedLocalStorage>) {
    let storage = shared_storage::<CAPACITY>();
    let _ = storage.clone().into_writer();
    let writer = storage.clone().join_writer_at(position);
    let reader = storage.into_reader_at(position);
    (writer, reader)
}

fn storage_bytes<S: Storage>(storage: &S) -> &[u8] {
    unsafe { std::slice::from_raw_parts(storage.ptr().as_ptr(), storage.len()) }
}

fn receive_user_defined<S>(reader: &Reader<S>) -> u32 {
    let mut payload = [0u8; 4096];
    reader.receive_next(&mut payload).unwrap().unwrap().user_defined
}

fn receive_payload_len<S>(reader: &Reader<S>) -> usize {
    let mut payload = [0u8; 4096];
    reader.receive_next(&mut payload).unwrap().unwrap().payload.len()
}

fn receive_error<S>(reader: &Reader<S>) -> Error {
    let mut payload = [0u8; 4096];
    reader.receive_next(&mut payload).unwrap().unwrap_err()
}

fn assert_no_message<S>(reader: &Reader<S>) {
    let mut payload = [0u8; 4096];
    assert!(reader.receive_next(&mut payload).is_none());
}

fn receive_batch_user_defined<S>(batch: &mut Batch<'_, S>) -> u32 {
    let mut payload = [0u8; 4096];
    batch.receive_next(&mut payload).unwrap().unwrap().user_defined
}

fn receive_batch_error<S>(batch: &mut Batch<'_, S>) -> Error {
    let mut payload = [0u8; 4096];
    batch.receive_next(&mut payload).unwrap().unwrap_err()
}

fn assert_batch_empty<S>(batch: &mut Batch<'_, S>) {
    let mut payload = [0u8; 4096];
    assert!(batch.receive_next(&mut payload).is_none());
}

#[test]
fn should_read_messages_in_batch() {
    let (mut writer, reader) = writer_and_reader::<64>();

    let mut claim = writer.claim(5, true);
    claim.get_buffer_mut().copy_from_slice(b"hello");
    claim.commit();

    let mut claim = writer.claim(5, true);
    claim.get_buffer_mut().copy_from_slice(b"world");
    claim.commit();

    let mut payload = [0u8; 1024];
    let mut batch = reader.read_batch().unwrap();

    let msg = batch.receive_next(&mut payload).unwrap().unwrap();
    assert_eq!(msg.payload, b"hello");

    let msg = batch.receive_next(&mut payload).unwrap().unwrap();
    assert_eq!(msg.payload, b"world");

    assert_batch_empty(&mut batch);

    let claim = writer.claim(15, true);
    claim.commit();

    let mut claim = writer.claim(4, true);
    claim.get_buffer_mut().copy_from_slice(b"test");
    claim.commit();

    let mut batch = reader.read_batch().unwrap();

    // skip big message
    let _ = batch.receive_next(&mut payload).unwrap().unwrap();
    let msg = batch.receive_next(&mut payload).unwrap().unwrap();

    assert_eq!(msg.payload, b"test");

    assert_batch_empty(&mut batch);
}

#[test]
fn should_read_bulk() {
    let (storage, mut writer, reader) = storage_writer_and_reader::<64>();
    let reader = reader.with_initial_position(0);

    let mut claim = writer.claim(5, true);
    claim.get_buffer_mut().copy_from_slice(b"hello");
    claim.commit();

    let mut claim = writer.claim(5, true);
    claim.get_buffer_mut().copy_from_slice(b"world");
    claim.commit();

    let bulk = reader.read_bulk().unwrap().unwrap();
    assert_eq!(32, bulk.len());
    assert_eq!(0, bulk.start_position());
    assert_eq!(32, bulk.end_position());

    let mut dst = vec![0_u8; bulk.len()];
    assert_eq!(32, bulk.copy_into(&mut dst).unwrap());
    assert_eq!(&storage_bytes(&storage)[HEADER_SIZE..HEADER_SIZE + 32], dst.as_slice());
}

#[test]
fn should_iterate_bulk_messages() {
    let (mut writer, reader) = writer_and_reader::<64>();
    let reader = reader.with_initial_position(0);

    let mut claim = writer.claim_with_user_defined(5, true, 100);
    claim.get_buffer_mut().copy_from_slice(b"hello");
    claim.commit();

    let mut claim = writer.heartbeat_with_payload_and_user_defined(5, 200);
    claim.get_buffer_mut().copy_from_slice(b"world");
    claim.commit();

    let bulk = reader.read_bulk().unwrap().unwrap();
    let start_position = bulk.start_position();
    let mut dst = vec![0_u8; bulk.len()];
    let len = bulk.copy_into(&mut dst).unwrap();

    let mut iter = BulkIter::new(&dst[..len], start_position);

    let first = iter.next().unwrap();
    assert_eq!(0, first.stream_position);
    assert_eq!(100, first.user_defined);
    assert!(first.is_fin);
    assert!(!first.is_continuation);
    assert!(!first.is_heartbeat);
    assert_eq!(b"hello", first.payload);

    let second = iter.next().unwrap();
    assert_eq!(16, second.stream_position);
    assert_eq!(200, second.user_defined);
    assert!(second.is_fin);
    assert!(!second.is_continuation);
    assert!(second.is_heartbeat);
    assert_eq!(b"world", second.payload);

    assert!(iter.next().is_none());
}

#[test]
fn should_iterate_bulk_messages_via_bulk() {
    let (mut writer, reader) = writer_and_reader::<64>();
    let reader = reader.with_initial_position(0);

    let mut claim = writer.claim_with_user_defined(5, true, 100);
    claim.get_buffer_mut().copy_from_slice(b"hello");
    claim.commit();

    let mut claim = writer.heartbeat_with_payload_and_user_defined(5, 200);
    claim.get_buffer_mut().copy_from_slice(b"world");
    claim.commit();

    let bulk = reader.read_bulk().unwrap().unwrap();
    let mut dst = vec![0_u8; bulk.len()];
    let mut iter = bulk.into_iter(&mut dst).unwrap();

    let first = iter.next().unwrap();
    assert_eq!(0, first.stream_position);
    assert_eq!(100, first.user_defined);
    assert_eq!(b"hello", first.payload);

    let second = iter.next().unwrap();
    assert_eq!(16, second.stream_position);
    assert_eq!(200, second.user_defined);
    assert_eq!(b"world", second.payload);

    assert!(iter.next().is_none());
}

#[test]
fn should_skip_padding_frames_in_bulk_iter() {
    let (mut writer, reader) = writer_and_reader_at::<64>(56);

    let mut claim = writer.claim_with_user_defined(4, true, 300);
    claim.get_buffer_mut().copy_from_slice(b"test");
    claim.commit();

    let bulk = reader.read_bulk().unwrap().unwrap();
    let start_position = bulk.start_position();
    let mut dst = vec![0_u8; bulk.len()];
    let len = bulk.copy_into(&mut dst).unwrap();

    let mut iter = BulkIter::new(&dst[..len], start_position);
    let msg = iter.next().unwrap();
    assert_eq!(64, msg.stream_position);
    assert_eq!(300, msg.user_defined);
    assert_eq!(b"test", msg.payload);
    assert!(iter.next().is_none());
}

#[test]
fn should_read_wrapped_bulk() {
    let storage = shared_storage::<64>();
    let _ = storage.clone().into_writer();
    let mut writer = storage.clone().join_writer_at(56);
    let reader = storage.clone().into_reader_at(56);

    let claim = writer.claim(0, true);
    claim.commit();

    let mut claim = writer.claim(4, true);
    claim.get_buffer_mut().copy_from_slice(b"test");
    claim.commit();

    let bulk = reader.read_bulk().unwrap().unwrap();
    assert_eq!(24, bulk.len());

    let mut dst = vec![0_u8; bulk.len()];
    assert_eq!(24, bulk.copy_into(&mut dst).unwrap());

    let mut expected = Vec::with_capacity(24);
    let bytes = storage_bytes(&storage);
    expected.extend_from_slice(&bytes[HEADER_SIZE + 56..HEADER_SIZE + 64]);
    expected.extend_from_slice(&bytes[HEADER_SIZE..HEADER_SIZE + 16]);

    assert_eq!(expected, dst);
}

#[test]
fn should_overrun_read_bulk() {
    let (mut writer, reader) = writer_and_reader::<64>();
    let reader = reader.with_initial_position(0);

    writer.claim(16, true).commit();
    writer.claim(16, true).commit();
    writer.claim(16, true).commit();
    writer.claim(16, true).commit();

    let bulk = reader.read_bulk().unwrap();
    assert!(matches!(bulk, Err(Error::Overrun(0))));
}

#[test]
fn should_allow_bulk_reader_to_recover_from_initial_overrun_after_reset() {
    let (mut writer, reader) = writer_and_reader_at::<2048>(usize::MAX - 2047);

    writer.claim_with_user_defined(1000, true, 100).commit();
    assert_eq!(100, receive_user_defined(&reader));

    writer.claim_with_user_defined(1000, true, 101).commit();
    writer.claim_with_user_defined(512, true, 102).commit();

    let mut claim = writer.claim_with_user_defined(1000, true, 103);
    thread_rng().fill(claim.get_buffer_mut());
    claim.commit();

    assert!(matches!(reader.read_bulk().unwrap(), Err(Error::Overrun(_))));

    reader.reset();
    assert!(reader.read_bulk().is_none());

    writer.claim_with_user_defined(1000, true, 104).commit();

    let bulk = reader.read_bulk().unwrap().unwrap();
    let mut dst = vec![0_u8; bulk.len()];
    assert_eq!(bulk.len(), bulk.copy_into(&mut dst).unwrap());
}

#[test]
fn should_error_if_bulk_overruns_during_copy() {
    let (mut writer, reader) = writer_and_reader::<128>();
    let reader = reader.with_initial_position(0);

    writer.claim(16, true).commit();
    writer.claim(16, true).commit();

    let bulk = reader.read_bulk().unwrap().unwrap();
    let mut dst = vec![0_u8; bulk.len()];

    writer.claim(16, true).commit();
    writer.claim(16, true).commit();
    writer.claim(16, true).commit();
    writer.claim(16, true).commit();

    assert!(matches!(bulk.copy_into(&mut dst), Err(Error::Overrun(0))));
}

#[test]
fn should_allow_bulk_reader_to_recover_from_copy_overrun_after_reset() {
    let (mut writer, reader) = writer_and_reader::<128>();
    let reader = reader.with_initial_position(0);

    writer.claim_with_user_defined(16, true, 100).commit();
    writer.claim_with_user_defined(16, true, 101).commit();

    let bulk = reader.read_bulk().unwrap().unwrap();
    let mut dst = vec![0_u8; bulk.len()];

    writer.claim_with_user_defined(16, true, 102).commit();
    writer.claim_with_user_defined(16, true, 103).commit();
    writer.claim_with_user_defined(16, true, 104).commit();
    writer.claim_with_user_defined(16, true, 105).commit();

    assert!(matches!(bulk.copy_into(&mut dst), Err(Error::Overrun(0))));

    reader.reset();
    assert!(reader.read_bulk().is_none());

    writer.claim_with_user_defined(16, true, 106).commit();

    let bulk = reader.read_bulk().unwrap().unwrap();
    assert_eq!(24, bulk.len());
    let mut dst = vec![0_u8; bulk.len()];
    assert_eq!(24, bulk.copy_into(&mut dst).unwrap());
}

#[test]
fn should_read_in_batch_with_limit() {
    let (mut writer, reader) = writer_and_reader::<64>();

    let mut claim = writer.claim(1, true);
    claim.get_buffer_mut().copy_from_slice(b"a");
    claim.commit();

    let mut claim = writer.claim(1, true);
    claim.get_buffer_mut().copy_from_slice(b"b");
    claim.commit();

    let mut claim = writer.claim(1, true);
    claim.get_buffer_mut().copy_from_slice(b"c");
    claim.commit();

    let mut payload = [0u8; 1];
    {
        let mut batch = reader.read_batch().unwrap();

        let msg = batch.receive_next(&mut payload).unwrap().unwrap();
        assert_eq!(b"a", msg.payload);

        let msg = batch.receive_next(&mut payload).unwrap().unwrap();
        assert_eq!(b"b", msg.payload);
    }

    let mut batch = reader.read_batch().unwrap();

    let msg = batch.receive_next(&mut payload).unwrap().unwrap();
    assert_eq!(b"c", msg.payload);

    assert_batch_empty(&mut batch);
}

#[test]
fn should_cache_producer_position_until_reader_catches_up() {
    let storage = shared_storage::<64>();
    let mut writer = storage.clone().into_writer();

    writer.claim_with_user_defined(0, true, 100).commit();
    writer.claim_with_user_defined(0, true, 200).commit();

    let reader = storage.into_reader_at(0);

    writer.claim_with_user_defined(0, true, 300).commit();

    assert_eq!(100, receive_user_defined(&reader));

    assert_eq!(200, receive_user_defined(&reader));

    assert_eq!(300, receive_user_defined(&reader));
}

#[test]
fn should_read_batch_from_cached_producer_position_until_reader_catches_up() {
    let storage = shared_storage::<64>();
    let mut writer = storage.clone().into_writer();

    writer.claim_with_user_defined(0, true, 100).commit();
    writer.claim_with_user_defined(0, true, 200).commit();

    let reader = storage.into_reader_at(0);

    writer.claim_with_user_defined(0, true, 300).commit();

    let mut batch = reader.read_batch().unwrap();
    assert_eq!(16, batch.remaining());
    assert_eq!(100, receive_batch_user_defined(&mut batch));
    assert_eq!(200, receive_batch_user_defined(&mut batch));
    assert_batch_empty(&mut batch);

    let mut batch = reader.read_batch().unwrap();
    assert_eq!(8, batch.remaining());
    assert_eq!(300, receive_batch_user_defined(&mut batch));
    assert_batch_empty(&mut batch);
}

#[test]
fn should_resume_batch_if_previous_not_consumed() {
    let (mut writer, reader) = writer_and_reader::<64>();

    writer.claim_with_user_defined(0, true, 100).commit();
    writer.claim_with_user_defined(0, true, 200).commit();
    writer.claim_with_user_defined(0, true, 300).commit();
    writer.claim_with_user_defined(0, true, 400).commit();

    let mut batch = reader.read_batch().unwrap();

    assert_eq!(100, receive_batch_user_defined(&mut batch));
    assert_eq!(200, receive_batch_user_defined(&mut batch));
    assert_eq!(300, receive_batch_user_defined(&mut batch));

    let mut batch = reader.read_batch().unwrap();
    assert_eq!(400, receive_batch_user_defined(&mut batch));
    assert_batch_empty(&mut batch);
}

#[test]
fn should_read_next_message_if_batch_not_consumed() {
    let (mut writer, reader) = writer_and_reader::<64>();

    writer.claim_with_user_defined(0, true, 100).commit();
    writer.claim_with_user_defined(0, true, 200).commit();
    writer.claim_with_user_defined(0, true, 300).commit();
    writer.claim_with_user_defined(0, true, 400).commit();

    let mut batch = reader.read_batch().unwrap();

    assert_eq!(100, receive_batch_user_defined(&mut batch));
    assert_eq!(200, receive_batch_user_defined(&mut batch));
    assert_eq!(300, receive_batch_user_defined(&mut batch));

    assert_eq!(400, receive_user_defined(&reader));
    assert_no_message(&reader);
}

#[test]
fn should_not_extend_batch_when_new_messages_arrive() {
    let (mut writer, reader) = writer_and_reader::<64>();

    writer.claim_with_user_defined(0, true, 100).commit();
    writer.claim_with_user_defined(0, true, 200).commit();

    let mut batch = reader.read_batch().unwrap();
    assert_eq!(100, receive_batch_user_defined(&mut batch));

    writer.claim_with_user_defined(0, true, 300).commit();
    writer.claim_with_user_defined(0, true, 400).commit();

    assert_eq!(200, receive_batch_user_defined(&mut batch));
    assert_batch_empty(&mut batch);

    let mut batch = reader.read_batch().unwrap();
    assert_eq!(300, receive_batch_user_defined(&mut batch));
    assert_eq!(400, receive_batch_user_defined(&mut batch));
    assert_batch_empty(&mut batch);
}

#[test]
fn should_read_next_message() {
    let (mut writer, reader) = writer_and_reader::<64>();

    let mut claim = writer.claim(1, true);
    claim.get_buffer_mut().copy_from_slice(b"a");
    claim.commit();

    let mut claim = writer.claim(1, true);
    claim.get_buffer_mut().copy_from_slice(b"b");
    claim.commit();

    let mut claim = writer.claim(1, true);
    claim.get_buffer_mut().copy_from_slice(b"c");
    claim.commit();

    let mut payload = [0u8; 1];
    let msg = reader.receive_next(&mut payload).unwrap().unwrap();
    assert_eq!(b"a", msg.payload);

    let msg = reader.receive_next(&mut payload).unwrap().unwrap();
    assert_eq!(b"b", msg.payload);

    let msg = reader.receive_next(&mut payload).unwrap().unwrap();
    assert_eq!(b"c", msg.payload);

    assert_no_message(&reader);
}

#[test]
fn should_skip_next_message() {
    let (mut writer, reader) = writer_and_reader::<64>();

    writer.claim_with_user_defined(1, true, 100).commit();
    writer.claim_with_user_defined(1, true, 200).commit();
    writer.claim_with_user_defined(1, true, 300).commit();

    assert_eq!(Some(Ok(())), reader.skip_next());

    let mut payload = [0u8; 1];
    let msg = reader.receive_next(&mut payload).unwrap().unwrap();
    assert_eq!(200, msg.user_defined);

    assert_eq!(Some(Ok(())), reader.skip_next());
    assert!(reader.skip_next().is_none());
    assert_no_message(&reader);
}

#[test]
fn should_skip_padding_when_skipping_next_message() {
    let (mut writer, reader) = writer_and_reader_at::<64>(56);

    let mut claim = writer.claim_with_user_defined(4, true, 123);
    claim.get_buffer_mut().copy_from_slice(b"test");
    claim.commit();

    assert_eq!(Some(Ok(())), reader.skip_next());
    assert!(reader.skip_next().is_none());
}

#[test]
fn should_receive_next_message_into_buffer() {
    let (mut writer, reader) = writer_and_reader::<64>();

    let mut claim = writer.claim_with_user_defined(5, true, 123);
    claim.get_buffer_mut().copy_from_slice(b"hello");
    claim.commit();

    let mut payload = [0u8; 16];
    {
        let msg = reader.receive_next(&mut payload).unwrap().unwrap();
        assert_eq!(0, msg.stream_position);
        assert_eq!(123, msg.user_defined);
        assert!(msg.is_fin);
        assert!(!msg.is_continuation);
        assert!(!msg.is_heartbeat);
        assert_eq!(b"hello", msg.payload);
    }
    assert!(reader.receive_next(&mut payload).is_none());
}

#[test]
fn should_return_error_if_receive_next_buffer_is_too_small() {
    let (mut writer, reader) = writer_and_reader::<64>();

    let mut claim = writer.claim(5, true);
    claim.get_buffer_mut().copy_from_slice(b"hello");
    claim.commit();

    let mut too_small = [0u8; 4];
    let err = reader.receive_next(&mut too_small).unwrap().unwrap_err();
    assert_eq!(Error::InsufficientBufferSize(4, 5), err);

    let mut payload = [0u8; 5];
    let msg = reader.receive_next(&mut payload).unwrap().unwrap();
    assert_eq!(b"hello", msg.payload);
}

#[test]
fn should_skip_padding_when_receiving_next_message_into_buffer() {
    let (mut writer, reader) = writer_and_reader_at::<64>(56);

    let mut claim = writer.claim_with_user_defined(4, true, 123);
    claim.get_buffer_mut().copy_from_slice(b"test");
    claim.commit();

    let mut payload = [0u8; 4];
    {
        let msg = reader.receive_next(&mut payload).unwrap().unwrap();
        assert_eq!(64, msg.stream_position);
        assert_eq!(123, msg.user_defined);
        assert_eq!(b"test", msg.payload);
    }
    assert!(reader.receive_next(&mut payload).is_none());
}

#[test]
fn should_receive_batch_message_into_buffer() {
    let (mut writer, reader) = writer_and_reader::<64>();

    let mut claim = writer.claim_with_user_defined(5, true, 100);
    claim.get_buffer_mut().copy_from_slice(b"hello");
    claim.commit();

    let mut claim = writer.claim_with_user_defined(5, true, 200);
    claim.get_buffer_mut().copy_from_slice(b"world");
    claim.commit();

    let mut batch = reader.read_batch().unwrap();
    let mut payload = [0u8; 16];

    {
        let msg = batch.receive_next(&mut payload).unwrap().unwrap();
        assert_eq!(100, msg.user_defined);
        assert_eq!(b"hello", msg.payload);
    }

    {
        let msg = batch.receive_next(&mut payload).unwrap().unwrap();
        assert_eq!(200, msg.user_defined);
        assert_eq!(b"world", msg.payload);
    }

    assert!(batch.receive_next(&mut payload).is_none());
}

#[test]
fn should_skip_padding_when_receiving_batch_message_into_buffer() {
    let (mut writer, reader) = writer_and_reader_at::<64>(56);

    let mut claim = writer.claim_with_user_defined(4, true, 123);
    claim.get_buffer_mut().copy_from_slice(b"test");
    claim.commit();

    let mut batch = reader.read_batch().unwrap();
    let mut payload = [0u8; 4];
    let msg = batch.receive_next(&mut payload).unwrap().unwrap();
    assert_eq!(64, msg.stream_position);
    assert_eq!(123, msg.user_defined);
    assert_eq!(b"test", msg.payload);
    assert!(batch.receive_next(&mut payload).is_none());
}

#[test]
fn should_skip_remaining_batch() {
    let (mut writer, reader) = writer_and_reader::<64>();

    writer.claim_with_user_defined(1, true, 100).commit();
    writer.claim_with_user_defined(1, true, 200).commit();
    writer.claim_with_user_defined(1, true, 300).commit();

    let batch = reader.read_batch().unwrap();
    assert_eq!(48, batch.remaining());
    batch.skip_remaining().unwrap();

    assert_no_message(&reader);
}

#[test]
fn should_skip_remaining_after_partial_batch_read() {
    let (mut writer, reader) = writer_and_reader::<64>();

    writer.claim_with_user_defined(1, true, 100).commit();
    writer.claim_with_user_defined(1, true, 200).commit();
    writer.claim_with_user_defined(1, true, 300).commit();

    let mut batch = reader.read_batch().unwrap();
    let mut payload = [0u8; 1];
    let msg = batch.receive_next(&mut payload).unwrap().unwrap();
    assert_eq!(100, msg.user_defined);
    assert_eq!(32, batch.remaining());

    batch.skip_remaining().unwrap();

    assert_no_message(&reader);
}

#[test]
fn should_return_error_if_batch_receive_next_buffer_is_too_small() {
    let (mut writer, reader) = writer_and_reader::<64>();

    let mut claim = writer.claim(5, true);
    claim.get_buffer_mut().copy_from_slice(b"hello");
    claim.commit();

    let mut batch = reader.read_batch().unwrap();
    let mut too_small = [0u8; 4];
    let err = batch.receive_next(&mut too_small).unwrap().unwrap_err();
    assert_eq!(Error::InsufficientBufferSize(4, 5), err);
    assert_eq!(16, batch.remaining());

    let mut payload = [0u8; 5];
    let msg = batch.receive_next(&mut payload).unwrap().unwrap();
    assert_eq!(b"hello", msg.payload);
    assert_eq!(0, batch.remaining());
}

#[test]
fn should_overrun_reader() {
    let (mut writer, reader) = writer_and_reader::<64>();

    writer.claim(16, true).commit();
    writer.claim(16, true).commit();
    writer.claim(16, true).commit();
    writer.claim(16, true).commit();

    let mut payload = [0u8; 16];
    let msg = reader.receive_next(&mut payload).unwrap();
    assert!(matches!(msg.unwrap_err(), Error::Overrun(_)));
}

#[test]
fn should_overrun_skip_next() {
    let (mut writer, reader) = writer_and_reader::<64>();

    writer.claim(16, true).commit();
    writer.claim(16, true).commit();
    writer.claim(16, true).commit();
    writer.claim(16, true).commit();

    let err = reader.skip_next().unwrap().unwrap_err();
    assert!(matches!(err, Error::Overrun(_)));
}

#[test]
fn should_overrun_read_batch() {
    let (mut writer, reader) = writer_and_reader::<64>();

    writer.claim(16, true).commit();
    writer.claim(16, true).commit();
    writer.claim(16, true).commit();
    writer.claim(16, true).commit();

    let mut batch = reader.read_batch().unwrap();
    let err = receive_batch_error(&mut batch);
    assert!(matches!(err, Error::Overrun(_)));
}

#[test]
fn should_overrun_batch_skip_remaining() {
    let (mut writer, reader) = writer_and_reader::<64>();

    writer.claim(16, true).commit();
    writer.claim(16, true).commit();
    writer.claim(16, true).commit();
    writer.claim(16, true).commit();

    let batch = reader.read_batch().unwrap();
    let err = batch.skip_remaining().unwrap_err();
    assert!(matches!(err, Error::Overrun(_)));
}

#[test]
#[should_panic(expected = "mtu exceeded")]
fn should_error_if_mtu_exceeded() {
    let storage = shared_storage::<64>();
    let mut writer = storage.into_writer();
    assert_eq!(24, writer.mtu());
    let _ = writer.claim(32, true);
}

#[test]
fn should_start_read_from_last_producer_position() {
    let storage = shared_storage::<64>();
    let mut writer = storage.clone().into_writer();

    writer.claim(16, true).commit();

    let reader = storage.into_reader();
    assert_no_message(&reader);
}

#[test]
fn should_not_start_reader_at_retained_window_start_unless_it_is_a_frame_boundary() {
    let storage = shared_storage::<1024>();
    let mut writer = storage.clone().into_writer();

    writer.claim(8, true).commit();

    let mut claim = writer.claim_with_user_defined(16, true, 100);
    claim.get_buffer_mut().fill(0);
    claim.commit();

    writer.claim_with_user_defined(504, true, 101).commit();
    writer.claim(464, true).commit();
    writer.claim_with_user_defined(16, true, 200).commit();

    let retained_window_start = 24;
    assert_eq!(24, retained_window_start);
    let first_valid_frame_position = 40;

    let reader = storage.clone().into_reader_at(retained_window_start);

    let mut payload = [0u8; 1024];
    let msg = reader.receive_next(&mut payload).unwrap().unwrap();
    assert_eq!(retained_window_start, msg.stream_position);
    assert_eq!(0, msg.payload.len());
    assert_eq!(0, msg.user_defined);

    let reader = storage.into_reader_at(first_valid_frame_position);

    let msg = reader.receive_next(&mut payload).unwrap().unwrap();
    assert_eq!(first_valid_frame_position, msg.stream_position);
    assert_eq!(504, msg.payload.len());
    assert_eq!(101, msg.user_defined);
}

#[test]
fn should_start_read_from_beginning_before_first_lap_completes() {
    let storage = shared_storage::<128>();
    let mut writer = storage.clone().into_writer();

    writer.claim_with_user_defined(16, true, 100).commit();
    writer.claim_with_user_defined(16, true, 101).commit();

    let reader = storage.into_reader_at_last_lap();

    assert_eq!(100, receive_user_defined(&reader));
    assert_eq!(101, receive_user_defined(&reader));
    assert_no_message(&reader);
}

#[test]
fn should_not_advance_last_lap_until_new_frame_starts_at_ring_beginning() {
    let storage = shared_storage::<128>();
    let mut writer = storage.clone().into_writer();

    writer.claim_with_user_defined(56, true, 100).commit();
    writer.claim_with_user_defined(56, true, 101).commit();

    let reader = storage.clone().into_reader_at_last_lap();

    assert_eq!(100, receive_user_defined(&reader));
    assert_eq!(101, receive_user_defined(&reader));
    assert_no_message(&reader);

    writer.claim_with_user_defined(16, true, 102).commit();

    let reader = storage.into_reader_at_last_lap();

    assert_eq!(102, receive_user_defined(&reader));
    assert_no_message(&reader);
}

#[test]
fn should_start_read_from_last_lap_after_padding_wrap() {
    let storage = shared_storage::<128>();
    let mut writer = storage.clone().into_writer();

    writer.claim_with_user_defined(40, true, 100).commit();
    writer.claim_with_user_defined(40, true, 101).commit();
    writer.claim_with_user_defined(40, true, 102).commit();

    let reader = storage.into_reader_at_last_lap();

    assert_eq!(102, receive_user_defined(&reader));
    assert_no_message(&reader);
}

#[test]
fn should_start_read_from_last_lap_without_writer_config() {
    let storage = shared_storage::<128>();
    let mut writer = storage.clone().into_writer();

    writer.claim_with_user_defined(16, true, 100).commit();

    let reader = storage.into_reader_at_last_lap();

    assert_eq!(100, receive_user_defined(&reader));
    assert_no_message(&reader);
}

#[test]
#[should_panic(expected = "claim reserve ratio must be in 0.0..=0.5")]
fn should_reject_claim_reserve_ratio_above_half_capacity() {
    let _ = WriterConfig::default().claim_reserve_ratio(0.51);
}

#[test]
fn should_allow_claim_reservation_to_reduce_readable_window() {
    let storage = shared_storage::<64>();
    let mut writer = storage
        .clone()
        .into_writer_with_cfg(|config| config.claim_reserve_ratio(0.25));
    let reader = storage.into_reader_at(0);

    writer.claim(24, true).commit();
    writer.claim(24, true).commit();

    assert!(matches!(receive_error(&reader), Error::Overrun(0)));
}

#[test]
fn should_read_message_into_vec() {
    let (mut writer, reader) = writer_and_reader::<1024>();

    let mut claim = writer.claim(11, true);
    claim.get_buffer_mut().copy_from_slice(b"hello world");
    claim.commit();

    let mut payload = vec![0u8; 1024];
    let msg = reader.receive_next(&mut payload).unwrap().unwrap();

    assert_eq!(msg.payload, b"hello world");
}

#[test]
fn should_publish_message_using_closure() {
    let (mut writer, reader) = writer_and_reader::<64>();

    writer.publish_with_user_defined(11, true, 123, |payload| {
        payload.copy_from_slice(b"hello world");
    });

    let mut payload = [0u8; 16];
    let msg = reader.receive_next(&mut payload).unwrap().unwrap();

    assert_eq!(msg.payload, b"hello world");
    assert_eq!(123, msg.user_defined);
}

#[test]
fn should_send_message_from_payload_slice() {
    let (mut writer, reader) = writer_and_reader::<64>();

    writer.send_with_user_defined(b"hello world", false, 123);

    let mut payload = [0u8; 16];
    let msg = reader.receive_next(&mut payload).unwrap().unwrap();

    assert_eq!(msg.payload, b"hello world");
    assert!(!msg.is_fin);
    assert_eq!(123, msg.user_defined);
}

#[test]
fn should_send_zero_size_message() {
    let (mut writer, reader) = writer_and_reader::<64>();

    let claim = writer.claim(0, true);
    claim.commit();

    assert_eq!(0, receive_payload_len(&reader));
}

#[test]
fn should_send_heartbeat() {
    let (mut writer, reader) = writer_and_reader::<64>();

    let claim = writer.heartbeat();
    claim.commit();

    let mut payload = [0u8; 1];
    let msg = reader.receive_next(&mut payload).unwrap().unwrap();
    assert_eq!(0, msg.payload.len());
    assert!(msg.is_fin);
    assert!(!msg.is_continuation);
}

#[test]
fn should_abort_publication() {
    let (mut writer, reader) = writer_and_reader::<64>();
    let reader = reader.with_initial_position(0);

    let claim = writer.claim(16, true);
    claim.abort();
    assert_no_message(&reader);

    let claim = writer.claim(24, true);
    claim.commit();
    assert_eq!(24, receive_payload_len(&reader));

    let claim = writer.claim(8, true);
    claim.commit();
    assert_eq!(8, receive_payload_len(&reader));

    let claim = writer.claim(16, true);
    claim.abort();
    assert_no_message(&reader);
}

#[test]
fn should_attach_metadata() {
    let storage = shared_storage::<64>();
    let _ = storage.clone().into_writer_with_cfg(|config| {
        config.metadata(|metadata| {
            assert_eq!(METADATA_BUFFER_SIZE, metadata.len());
            metadata[0..11].copy_from_slice(b"hello world");
        })
    });
    let reader = storage.into_reader();
    assert_eq!(b"hello world", &reader.metadata()[..11]);
}

#[test]
fn should_skip_padding_frame() {
    let (mut writer, reader) = writer_and_reader::<64>();
    let mut buffer = [0u8; 1024];

    let claim = writer.claim_with_user_defined(24, true, 123);
    claim.commit();
    let msg = reader.receive_next(&mut buffer).unwrap().unwrap();
    assert_eq!(123, msg.user_defined);

    let claim = writer.claim(8, true);
    claim.commit();
    let msg = reader.receive_next(&mut buffer).unwrap().unwrap();
    assert_eq!(8, msg.payload.len());

    let claim = writer.claim(24, true);
    claim.commit();
    let msg = reader.receive_next(&mut buffer).unwrap().unwrap();
    assert_eq!(24, msg.payload.len());

    assert!(reader.receive_next(&mut buffer).is_none())
}

#[test]
fn should_fragment_message() {
    let (mut writer, reader) = writer_and_reader::<64>();

    let claim = writer.claim_with_user_defined(24, false, 123);
    claim.commit();
    let mut buffer = [0u8; 1024];
    let msg = reader.receive_next(&mut buffer).unwrap().unwrap();
    assert!(!msg.is_fin);
    assert!(!msg.is_continuation);
    assert_eq!(123, msg.user_defined); // only attached to the first frame

    let claim = writer.continuation(8, false);
    claim.commit();
    let msg = reader.receive_next(&mut buffer).unwrap().unwrap();
    assert!(!msg.is_fin);
    assert!(msg.is_continuation);
    assert_eq!(USER_DEFINED_NULL_VALUE, msg.user_defined);

    let claim = writer.continuation(24, true);
    claim.commit();
    let msg = reader.receive_next(&mut buffer).unwrap().unwrap();
    assert!(msg.is_fin);
    assert!(msg.is_continuation);
    assert_eq!(USER_DEFINED_NULL_VALUE, msg.user_defined);

    assert!(reader.receive_next(&mut buffer).is_none())
}

#[test]
fn should_join_writer() {
    let storage = shared_storage::<1024>();

    // first writer will write from the beginning
    {
        let mut writer = storage.clone().into_writer();
        writer.claim_with_user_defined(16, true, 100).commit();
        writer.claim_with_user_defined(16, true, 101).commit();
        writer.claim_with_user_defined(16, true, 102).commit();
    }

    // second writer will pick up from the current position
    {
        let mut writer = storage.clone().join_writer();
        writer.claim_with_user_defined(16, true, 103).commit();
        writer.claim_with_user_defined(16, true, 104).commit();
        writer.claim_with_user_defined(16, true, 105).commit();
    }

    // verify we got all the messages
    let reader = storage.into_reader_at(0);
    assert_eq!(100, receive_user_defined(&reader));
    assert_eq!(101, receive_user_defined(&reader));
    assert_eq!(102, receive_user_defined(&reader));
    assert_eq!(103, receive_user_defined(&reader));
    assert_eq!(104, receive_user_defined(&reader));
    assert_eq!(105, receive_user_defined(&reader));
}

#[test]
fn should_handle_position_wrap_around_if_no_overrun() {
    let (mut writer, reader) = writer_and_reader_at::<2048>(usize::MAX - 1023);
    // last claim before wrap around
    writer.claim_with_user_defined(1000, true, 100).commit();
    // first claim after wrap around, will insert padding frame and
    // continue from position zero
    writer.claim_with_user_defined(128, true, 101).commit();
    // a normal claim after wrap around
    writer.claim_with_user_defined(16, true, 102).commit();
    // verify we got all the messages
    assert_eq!(100, receive_user_defined(&reader));
    assert_eq!(101, receive_user_defined(&reader));
    assert_eq!(102, receive_user_defined(&reader));
    // and are still in sync
}

#[test]
fn should_allow_reader_to_recover_from_overrun_when_position_wrapped_around() {
    let (mut writer, reader) = writer_and_reader_at::<2048>(usize::MAX - 2047);

    // First claim and read
    writer.claim_with_user_defined(1000, true, 100).commit();
    assert_eq!(100, receive_user_defined(&reader));

    // Last claim before wrap around
    writer.claim_with_user_defined(1000, true, 101).commit();

    // First claim after wrap around
    writer.claim_with_user_defined(512, true, 102).commit();

    // Overrun the reader and overwrite the header frame the reader will read
    let mut claim = writer.claim_with_user_defined(1000, true, 103);
    thread_rng().fill(claim.get_buffer_mut());
    claim.commit();

    assert!(matches!(receive_error(&reader), Error::Overrun(_)));
    // Reset the reader and start over
    reader.reset();
    assert_no_message(&reader);
    // Continue writing and reading

    writer.claim_with_user_defined(1000, true, 104).commit();

    assert_eq!(104, receive_user_defined(&reader));
}

#[test]
fn should_allow_batch_reader_to_recover_from_overrun_when_position_wrapped_around() {
    let (mut writer, reader) = writer_and_reader_at::<2048>(usize::MAX - 2047);

    // First claim and read
    writer.claim_with_user_defined(1000, true, 100).commit();
    let mut batch = reader.read_batch().unwrap();
    assert_eq!(100, receive_batch_user_defined(&mut batch));

    // Last claim before wrap around
    writer.claim_with_user_defined(1000, true, 101).commit();

    // First claim after wrap around
    writer.claim_with_user_defined(512, true, 102).commit();

    // Overrun the reader and overwrite the header frame the reader will read
    let mut claim = writer.claim_with_user_defined(1000, true, 103);
    thread_rng().fill(claim.get_buffer_mut());
    claim.commit();

    let mut batch = reader.read_batch().unwrap();
    assert!(matches!(receive_batch_error(&mut batch), Error::Overrun(_)));

    // Reset the reader and start over
    reader.reset();
    assert!(reader.read_batch().is_none());

    // Continue writing and reading through the batch API
    writer.claim_with_user_defined(1000, true, 104).commit();

    let mut batch = reader.read_batch().unwrap();
    assert_eq!(104, receive_batch_user_defined(&mut batch));
    assert_batch_empty(&mut batch);
}

#[test]
fn should_not_overrun_batch_when_reader_has_not_been_lapped() {
    let (mut writer, reader) = writer_and_reader::<128>();
    let reader = reader.with_initial_position(0);

    for i in 0..4_u32 {
        writer.claim_with_user_defined(16, true, i).commit();
    }

    let mut batch = reader.read_batch().unwrap();
    assert_eq!(0, receive_batch_user_defined(&mut batch));

    writer.claim_with_user_defined(16, true, 100).commit();
    writer.claim_with_user_defined(16, true, 101).commit();

    // Reader has advanced by one frame, so message 1 should still be readable.
    assert_eq!(1, receive_batch_user_defined(&mut batch));
}

#[test]
fn should_allow_receive_next_when_reader_has_not_been_lapped() {
    let (mut writer, reader) = writer_and_reader::<128>();
    let reader = reader.with_initial_position(0);

    for i in 0..4_u32 {
        writer.claim_with_user_defined(16, true, i).commit();
    }

    assert_eq!(0, receive_user_defined(&reader));

    writer.claim_with_user_defined(16, true, 100).commit();
    writer.claim_with_user_defined(16, true, 101).commit();

    // This is the control case for the batch repro above.
    assert_eq!(1, receive_user_defined(&reader));
}

#[test]
fn should_not_return_uncommitted_claim_as_committed_payload() {
    const CAPACITY: usize = 64;
    const PAYLOAD_LEN: usize = 24;

    let (mut writer, reader) = writer_and_reader::<CAPACITY>();

    {
        let mut claim = writer.claim(PAYLOAD_LEN, true);
        claim.get_buffer_mut().fill(0x11);
        claim.commit();
    }

    {
        let mut claim = writer.claim(PAYLOAD_LEN, true);
        claim.get_buffer_mut().fill(0x22);
        claim.commit();
    }

    let mut outstanding = writer.claim(PAYLOAD_LEN, true);
    outstanding.get_buffer_mut().fill(0xAA);

    let mut payload = [0u8; PAYLOAD_LEN];
    match reader.receive_next(&mut payload).unwrap() {
        Err(_) => {
            // Detecting an overrun before returning a message is acceptable.
        }
        Ok(msg) => {
            assert_eq!(msg.payload, &[0x11; PAYLOAD_LEN], "reader returned payload bytes from an uncommitted claim");
        }
    }

    outstanding.abort();
}
