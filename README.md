[![Build Status](https://img.shields.io/endpoint.svg?url=https%3A%2F%2Factions-badge.atrox.dev%2Fhavefuntrading%2Fbcast%2Fbadge%3Fref%3Dmain&style=flat&label=build&logo=none)](https://actions-badge.atrox.dev/havefuntrading/bcast/goto?ref=main)
[![Crates.io](https://img.shields.io/crates/v/bcast.svg)](https://crates.io/crates/bcast)
[![Documentation](https://docs.rs/bcast/badge.svg)](https://docs.rs/bcast/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## Overview

Low latency, single producer & many consumer (SPMC) ring buffer that works with in-process 
and interprocess memory. One of the key features of `bcast` is that it natively supports variable message sizes
(the payload is just `&[u8]`) which in turns works very well in any ipc-based distributed system (such as market data
feed handler). 

## Supported Platforms
The crate has been developed and tested exclusively on `x86_64-linux`. It should also work (but it's by 
no means guaranteed) on CPU architectures with weaker memory ordering semantics. If you want a particular platform
to be properly supported feel free to contribute and raise a pull request.

## Example

Create `Writer` by attaching it to the provided byte slice. It does not matter where the underlying bytes are stored, it
could be on the heap, stack as well as a result of memory mapping of a file by the process.

```rust
let bytes: &[u8] = ...;
let mut writer = RingBuffer::new(bytes).into_writer();
```

Writing takes place via `claim` operation that returns `Claim` object. We then have access to the underlying buffer to which
we can write our variable length message.

```rust
let mut claim = writer.claim(5)?;
claim.get_buffer_mut().copy_from_slice(b"hello");
claim.commit();
```

The `commit` operation is optional as the new producer position (as a result of us writing to the buffer) will be made
visible to other processes (threads) the moment the `Claim` is dropped.

The `Reader` is constructed in similar way by attaching it to some 'shared' memory.

```rust
let bytes: &[u8] = ...;
let mut reader = RingBuffer::new(bytes).into_reader();
```

The reader is batch aware (it knows how far behind a producer it is) and provides elegant way to process pending
messages in form of an iterator.

```rust
for msg in reader.batch_iter() {
    let mut payload = [0u8; 1024];
    let len = msg?.read(&mut payload)?;
    assert_eq!(b"hello", &payload[..len]);
}
```

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

This also means that messages are consumed in a 'lazy' way with the `read` operation delayed until it is required. As a
result it is possible to `clone` each `Message`. This approach is particularly useful if we want to delay the
actual consumption of messages (e.g. when we want to combine and expose data from various sources to the application in
a single step).
