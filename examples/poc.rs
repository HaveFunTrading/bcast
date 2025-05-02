use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A single slot in the ring buffer
struct Slot<T> {
    /// The sequence number that also acts as a guard.
    /// For a given slot, the valid sequence is:
    ///   - before writing: seq == index * 2 (even)
    ///   - after writing: seq == index * 2 + 1 (odd)
    seq: AtomicUsize,
    /// The storage for the message
    data: UnsafeCell<MaybeUninit<T>>,
}

unsafe impl<T: Send> Send for Slot<T> {}
unsafe impl<T: Send> Sync for Slot<T> {}

/// A one-producer, many-consumer ring buffer
/// (for simplicity, each consumer maintains its own read index)
pub struct RingBuffer<T> {
    slots: Vec<Slot<T>>,
    /// Capacity must be a power of two.
    capacity: usize,
    mask: usize,
    /// The producer’s position is a continuously increasing counter.
    producer_pos: AtomicUsize,
}

impl<T> RingBuffer<T> {
    /// Create a new ring buffer with the given capacity (power of 2).
    pub fn new(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two(), "Capacity must be a power of 2");
        let slots = (0..capacity)
            .map(|i| Slot {
                // Initialize the sequence so that the slot is ready to be written.
                // Here we use: seq = i * 2 (even) initially.
                seq: AtomicUsize::new(i * 2),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect();
        Self {
            slots,
            capacity,
            mask: capacity - 1,
            producer_pos: AtomicUsize::new(0),
        }
    }

    /// Called by the producer to write a message.
    pub fn produce(&self, msg: T) {
        // Reserve the next slot by taking the producer position.
        let pos = self.producer_pos.load(Ordering::Relaxed);
        let index = pos & self.mask;
        let slot = &self.slots[index];

        // The expected sequence value for a writable slot is pos * 2.
        let expected_seq = pos * 2;
        // Wait (or spin) until the slot’s sequence matches what we expect.
        // (In a production system you might want a better strategy than a busy loop.)
        while slot.seq.load(Ordering::Acquire) != expected_seq {
            std::hint::spin_loop();
        }

        // Write the data (this copy is not atomic, but the double-check later will detect any overwrite).
        unsafe {
            (*slot.data.get()).write(msg);
        }
        // Publish the write by updating the slot’s sequence number.
        // We set it to (pos * 2 + 1) to signal that the data is valid.
        slot.seq.store(pos * 2 + 1, Ordering::Release);

        // Advance the producer’s position.
        self.producer_pos.store(pos + 1, Ordering::Release);
    }

    /// Called by a consumer to try reading the next message.
    /// Each consumer must maintain its own `consumer_pos`, which is a continuously increasing counter.
    pub fn consume(&self, consumer_pos: &mut usize) -> Option<T> {
        let index = *consumer_pos & self.mask;
        let slot = &self.slots[index];

        // The expected sequence value when a slot is ready to be read is (consumer_pos * 2 + 1).
        let expected_seq = *consumer_pos * 2 + 1;

        // First read: check if the slot appears to be ready.
        let seq_before = slot.seq.load(Ordering::Acquire);
        if seq_before != expected_seq {
            // Either the producer hasn't written this message yet,
            // or (if seq_before is less than expected_seq) it has been overrun.
            return None;
        }

        // Copy the message.
        let msg = unsafe { (*slot.data.get()).assume_init_read() };

        // Second read: check the sequence again to detect if an overrun occurred during the copy.
        let seq_after = slot.seq.load(Ordering::Acquire);
        if seq_before != seq_after {
            // The slot's content was overwritten while we were copying it.
            // In this case, we consider the consumer as having been overrun.
            return None;
        }

        // Successfully consumed the message; advance the consumer position.
        *consumer_pos += 1;
        Some(msg)
    }
}

fn main() {}
