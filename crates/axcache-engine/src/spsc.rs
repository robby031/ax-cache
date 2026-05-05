// crates/axcache-engine/src/spsc.rs

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Ring Buffer SPSC (Single-Producer Single-Consumer) yang Wait-Free.
/// Menjamin latensi O(1) konstan tanpa Mutex atau Spinlock.
pub struct WaitFreeSpscQueue<T> {
    buffer: Box<[UnsafeCell<MaybeUninit<T>>]>,
    capacity: usize,
    head: AtomicUsize,
    tail: AtomicUsize,
}

unsafe impl<T: Send> Sync for WaitFreeSpscQueue<T> {}
unsafe impl<T: Send> Send for WaitFreeSpscQueue<T> {}

impl<T> WaitFreeSpscQueue<T> {
    pub fn new(capacity: usize) -> Self {
        let mut buffer = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buffer.push(UnsafeCell::new(MaybeUninit::uninit()));
        }
        Self {
            buffer: buffer.into_boxed_slice(),
            capacity,
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
        }
    }

    /// Push oleh Producer (Tidak pernah memblokir thread)
    pub fn push(&self, value: T) -> Result<(), T> {
        let head = self.head.load(Ordering::Relaxed);
        let next_head = (head + 1) % self.capacity;

        if next_head == self.tail.load(Ordering::Acquire) {
            return Err(value); // Antrian Penuh
        }

        unsafe {
            (*self.buffer[head].get()).write(value);
        }

        self.head.store(next_head, Ordering::Release);
        Ok(())
    }

    /// Pop oleh Consumer
    pub fn pop(&self) -> Option<T> {
        let tail = self.tail.load(Ordering::Relaxed);

        if tail == self.head.load(Ordering::Acquire) {
            return None; // Antrian Kosong
        }

        let value = unsafe { (*self.buffer[tail].get()).assume_init_read() };

        self.tail
            .store((tail + 1) % self.capacity, Ordering::Release);
        Some(value)
    }
}
