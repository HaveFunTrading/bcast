[![Build Status](https://img.shields.io/endpoint.svg?url=https%3A%2F%2Factions-badge.atrox.dev%2Fhavefuntrading%2Fbcast%2Fbadge%3Fref%3Dmain&style=flat&label=build&logo=none)](https://actions-badge.atrox.dev/havefuntrading/bcast/goto?ref=main)
[![Crates.io](https://img.shields.io/crates/v/bcast.svg)](https://crates.io/crates/bcast)
[![Documentation](https://docs.rs/bcast/badge.svg)](https://docs.rs/bcast/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## Overview

Low latency, single producer & many consumer (SPMC) ring buffer that works with shared memory. `bcast` natively supports variable-sized byte messages,
zero-copy writes and batch aware reads.

## What's changing in 1.0

Relative to the latest non-RC release, `0.0.29`, the 1.0 line makes the reader API explicit about payload ownership and tightens overrun semantics:

- readers and writers are constructed from storage handles: `storage.into_writer()`, `storage.join_writer()`, `storage.into_reader()`, `storage.into_reader_at(position)` and `storage.into_reader_at_last_lap()`
- the lazy `Message::read(...)` API is removed; `Reader::receive_next(&mut payload)` and `Batch::receive_next(&mut payload)` now copy directly into caller-provided storage and return a `Message` view over that buffer
- `BulkIter` now yields the same `Message` type as direct receives, so message metadata has one shape across the reader APIs
- readers can discard without copying via `Reader::skip_next()` and `Batch::skip_remaining()`
- readers and writers are now generic over owned storage handles; `LocalStorage`, mmap-backed storage and `SharedStorage` cover in-process and file-backed use cases
- late readers can start from the retained most recent lap via `storage.into_reader_at_last_lap()` / `MappedReader::new_at_last_lap(...)`
- overrun detection now tracks the producer's claimed overwrite frontier separately from committed readable position, with writer-side claim reservation and reader-side producer-position caching to reduce shared cursor traffic
- writer claim reservation is configured as a capacity ratio, for example `claim_reserve_ratio(0.05)` for 5%
- writers can publish via scoped `publish(...)` closures or copy caller-owned payloads via `send(...)`, in addition to the lower-level `claim(...)` API
- writer publication APIs now require mutable writer access, so the type system prevents multiple open claims from the same writer
- cursor-advancing reader APIs require mutable reader access; batches and bulk windows borrow the reader exclusively so its cursor cannot advance independently while either is active

## Supported Platforms
The crate has been developed and tested exclusively on `x86_64-linux`. It should also work (but it's by 
no means guaranteed) on CPU architectures with weaker memory ordering semantics. If you want a particular platform
to be properly supported feel free to contribute and submit a pull request.

## Example

Create storage first, then attach a writer and one or more readers to it. `LocalStorage` owns heap-backed memory for
in-process use. Use `into_shared()` when the same storage needs to be handed to multiple handles in the same process.

```rust
use bcast::{LocalStorage, StorageExt};

let storage = LocalStorage::with_capacity(1024).into_shared();
let mut writer = storage.clone().into_writer();
let mut reader = storage.clone().into_reader_at(0);
```

The simplest write API copies a caller-owned payload into the ring and commits it as a single message:

```rust
writer.send(b"hello", true);
```

To write directly into ring memory without creating an intermediate payload buffer, use `publish`. The closure receives
the claimed payload region and the message is committed when the closure returns:

```rust
writer.publish(5, true, |payload| {
    payload.copy_from_slice(b"hello");
});
```

For lower-level zero-copy control, use `claim`. It returns a `Claim` object that exposes the underlying payload buffer:

```rust
let mut claim = writer.claim(5, true);
claim.get_buffer_mut().copy_from_slice(b"hello");
claim.commit();
```

The `commit` operation is optional: a `Claim` commits automatically when dropped. Use `claim.abort()` if the reserved
region should be published as padding and skipped by readers.

Readers own independent cursors. `into_reader()` starts at the writer's current producer position and observes only
new messages. Use `into_reader_at(position)` when resuming from a known stream position.

```rust
let mut live_reader = storage.clone().into_reader();
let mut replay_from_start = storage.clone().into_reader_at(0);
```

Late readers can also replay up to one lap of retained data from the most recent physical ring
lap. The writer updates this marker only when a new frame starts at the beginning of the ring:

```rust
let mut reader = storage.clone().into_reader_at_last_lap();
```

The `Reader` is batch aware (it knows how far behind a producer it is) and can copy pending messages into a caller-provided buffer.

```rust
let mut payload = [0u8; 1024];

if let Some(mut batch) = reader.read_batch() {
    while let Some(msg) = batch.receive_next(&mut payload) {
        let msg = msg?;
        println!("{}", String::from_utf8_lossy(msg.payload));
    }
}
```

If you want to copy a bounded raw window out of the ring first and parse it off-ring, use the bulk API:

```rust
if let Some(bulk) = reader.read_bulk() {
    let bulk = bulk?;
    let mut bytes = vec![0u8; bulk.len()];
    for msg in bulk.into_iter(&mut bytes)? {
        println!("{}", String::from_utf8_lossy(msg.payload));
    }
}
```

When the `mmap` feature is enabled, `MappedWriter` and `MappedReader` provide file-backed wrappers over the same API for IPC-style usage.
The lower-level `MmapMutStorage` and `MmapStorage` types are also available if you want to construct `Writer` and
`Reader` directly.

`MappedWriter` holds an exclusive sidecar lock at `<path>.lock` for its full lifetime and returns
`std::io::ErrorKind::WouldBlock` if another writer already owns the channel. Readers do not take locks. The lower-level
mmap storage adapters do not lock either, so use `MappedWriter` when you want the crate to enforce single-writer ownership
for a file-backed channel.

```rust
use bcast::{HEADER_SIZE, MappedReader, MappedWriter};

let path = "channel.bcast";
let size = HEADER_SIZE + 1024;

let mut writer = MappedWriter::join_or_create(path, size)?;
let mut reader = MappedReader::new(path)?;
```

## Backpressure (and the lack of it)
`bcast` design is to allow producer to process and publish messages at full line rate and deliver the same latency irrespective
of the number of consumers (in reality there is a tiny penalty associated with adding each additional consumer). Consumers can detect when they have been overrun by the producer and take appropriate action.

If a consumer wants to continue after an overrun, call `reader.reset()`.

```rust
match reader.receive_next(&mut payload) {
    Some(Ok(msg)) => { /* process msg.payload */ },
    Some(Err(Error::Overrun(_))) => {
        // skip unread data and move this reader to the producer's current position.
        reader.reset();
    },
    Some(Err(err)) => return Err(err.into()),
    None => { /* no message available */ },
}
```

If a message or batch should be discarded, use `Reader::skip_next()` or `Batch::skip_remaining()` to advance without copying payload bytes.

When a batch detects an overrun, `Batch::reset(self)` consumes the batch and
resets its underlying reader to the producer's current committed position:

```rust
if let Some(mut batch) = reader.read_batch() {
    while let Some(result) = batch.receive_next(&mut payload) {
        match result {
            Ok(message) => process(message),
            Err(Error::Overrun(_)) => {
                batch.reset();
                break;
            }
            Err(error) => return Err(error.into()),
        }
    }
}
```
