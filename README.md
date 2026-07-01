[![Build Status](https://img.shields.io/endpoint.svg?url=https%3A%2F%2Factions-badge.atrox.dev%2Fhavefuntrading%2Fbcast%2Fbadge%3Fref%3Dmain&style=flat&label=build&logo=none)](https://actions-badge.atrox.dev/havefuntrading/bcast/goto?ref=main)
[![Crates.io](https://img.shields.io/crates/v/bcast.svg)](https://crates.io/crates/bcast)
[![Documentation](https://docs.rs/bcast/badge.svg)](https://docs.rs/bcast/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## Overview

Low latency, single producer & many consumer (SPMC) ring buffer that works with shared memory. `bcast` natively supports variable message sizes (`&[u8]`) and offers two read styles:

- lazy message access via `read_batch()` / `receive_next()`
- raw bulk copy via `read_bulk()` for lower reader-side overhead

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

The `Reader` is batch aware (it knows how far behind a producer it is) and provides an iterator over pending messages.

```rust
if let Some(batch) = reader.read_batch() {
    for msg in batch {
        let mut payload = [0u8; 1024];
        let len = msg?.read(&mut payload)?;
        println!("{}", String::from_utf8_lossy(&payload[..len]));
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
of the number of consumers (in reality there is a tiny penalty associated with adding each additional consumer). With the `Message`
API, consumer can detect when it has been overrun by the producer and take appropriate action (such as crashing
the application).

```rust
match msg.read(&mut payload) {
    Ok(_) => { /* read succeeded */},
    Err(err) => if let Error::Overrun(_) = err { /* handle overrun */ },
}
```

The message API is intentionally lazy: payload bytes are only copied when `Message::read(...)` is called, and `Message`
can be cloned if you need to defer consumption. If you prefer eager copying with a single overrun check at the end of
the copy, use `read_bulk()` instead.
