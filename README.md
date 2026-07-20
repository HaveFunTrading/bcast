[![Build Status](https://img.shields.io/endpoint.svg?url=https%3A%2F%2Factions-badge.atrox.dev%2Fhavefuntrading%2Fbcast%2Fbadge%3Fref%3Dmain&style=flat&label=build&logo=none)](https://actions-badge.atrox.dev/havefuntrading/bcast/goto?ref=main)
[![Crates.io](https://img.shields.io/crates/v/bcast.svg)](https://crates.io/crates/bcast)
[![Documentation](https://docs.rs/bcast/badge.svg)](https://docs.rs/bcast/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## Overview

Low latency, single producer & many consumer (SPMC) ring buffer that works with shared memory. `bcast` natively supports variable message sizes (`&[u8]`) and offers two read styles:

- message copy via `read_batch()` / `receive_next(&mut payload)`
- raw bulk copy via `read_bulk()` for lower reader-side overhead

## What's changing in 1.0

Relative to the latest non-RC release, `0.0.29`, the 1.0 line makes the reader API explicit about payload ownership and tightens overrun semantics:

- the lazy `Message::read(...)` API is removed; `Reader::receive_next(&mut payload)` and `Batch::receive_next(&mut payload)` now copy directly into caller-provided storage and return a `Message` view over that buffer
- `BulkIter` now yields the same `Message` type as direct receives, so message metadata has one shape across the reader APIs
- readers can discard without copying via `Reader::skip_next()` and `Batch::skip_remaining()`
- late readers can start from the retained most recent lap via `into_reader_at_last_lap()` / `MappedReader::new_at_last_lap(...)`
- overrun detection now tracks the producer's claimed overwrite frontier separately from committed readable position, with writer-side claim reservation and reader-side producer-position caching to reduce shared cursor traffic
- writer claim reservation is configured as a capacity ratio, for example `claim_reserve_ratio(0.05)` for 5%

## Supported Platforms
The crate has been developed and tested exclusively on `x86_64-linux`. It should also work (but it's by 
no means guaranteed) on CPU architectures with weaker memory ordering semantics. If you want a particular platform
to be properly supported feel free to contribute and submit a pull request.

## Example

Create `Writer` by attaching it to the provided byte slice. It does not matter where the underlying bytes are stored, it
could be on the heap, stack as well as a result of memory mapping of a file by the process.

```rust
let bytes: &[u8] = ...;
let writer = RingBuffer::new(bytes).into_writer();
```

Writing takes place via `claim` operation that returns `Claim` object. We then have access to the underlying buffer to which
we can write our variable length message.

```rust
let mut claim = writer.claim(5, true);
claim.get_buffer_mut().copy_from_slice(b"hello");
claim.commit();
```

The `commit` operation is optional as the new producer position (as a result of us writing to the buffer) will be made
visible to other processes (threads) the moment the `Claim` is dropped. The `Reader` is constructed in similar way by attaching it to some 'shared' memory.

```rust
let bytes: &[u8] = ...;
let reader = RingBuffer::new(bytes).into_reader();
```

Late readers can also replay up to one lap of retained data from the most recent physical ring
lap. The writer updates this marker only when a new frame starts at the beginning of the ring:

```rust
let reader = RingBuffer::new(bytes).into_reader_at_last_lap();
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

## Backpressure (and the lack of it)
`bcast` design is to allow producer to process and publish messages at full line rate and deliver the same latency irrespective
of the number of consumers (in reality there is a tiny penalty associated with adding each additional consumer). Consumers can detect when they have been overrun by the producer and take appropriate action, such as resetting or crashing the application.

```rust
match reader.receive_next(&mut payload) {
    Some(Ok(msg)) => { /* process msg.payload */ },
    Some(Err(Error::Overrun(_))) => { /* handle overrun */ },
    Some(Err(err)) => return Err(err.into()),
    None => { /* no message available */ },
}
```

If a message or batch should be discarded, use `Reader::skip_next()` or `Batch::skip_remaining()` to advance without copying payload bytes.
