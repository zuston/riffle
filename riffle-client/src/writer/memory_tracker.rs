use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::{ensure, Result};
use tokio::sync::Notify;

pub enum AcquireResult {
    Acquired(MemoryReservation),
    ExceededThreshold,
}

#[derive(Clone)]
pub struct MemoryTracker {
    inner: Arc<Inner>,
}

struct Inner {
    threshold: usize,
    used: AtomicUsize,
    released: Notify,
}

pub struct MemoryReservation {
    tracker: MemoryTracker,
    size: usize,
}

impl MemoryTracker {
    pub fn new(threshold: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                threshold,
                used: AtomicUsize::new(0),
                released: Notify::new(),
            }),
        }
    }

    pub fn try_acquire(&self, size: usize) -> Result<AcquireResult> {
        ensure!(
            size <= self.inner.threshold,
            "allocation exceeds writer memory capacity"
        );
        let mut used = self.inner.used.load(Ordering::Relaxed);
        loop {
            let Some(total) = used
                .checked_add(size)
                .filter(|&total| total <= self.inner.threshold)
            else {
                return Ok(AcquireResult::ExceededThreshold);
            };
            match self.inner.used.compare_exchange_weak(
                used,
                total,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Ok(AcquireResult::Acquired(MemoryReservation {
                        tracker: self.clone(),
                        size,
                    }))
                }
                Err(current) => used = current,
            }
        }
    }

    pub fn release(&self, size: usize) {
        let mut used = self.inner.used.load(Ordering::Relaxed);
        loop {
            let remaining = used
                .checked_sub(size)
                .expect("cannot release more memory than reserved");
            match self.inner.used.compare_exchange_weak(
                used,
                remaining,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => used = current,
            }
        }
        self.inner.released.notify_waiters();
    }

    pub fn released(&self) -> impl Future<Output = ()> + '_ {
        self.inner.released.notified()
    }
}

impl MemoryReservation {
    pub fn size(&self) -> usize {
        self.size
    }

    pub fn try_grow(&mut self, capacity: usize) -> Result<bool> {
        if capacity <= self.size {
            return Ok(true);
        }
        match self.tracker.try_acquire(capacity - self.size)? {
            AcquireResult::Acquired(memory) => {
                self.merge(memory);
                Ok(true)
            }
            AcquireResult::ExceededThreshold => Ok(false),
        }
    }

    fn merge(&mut self, mut other: Self) {
        assert!(Arc::ptr_eq(&self.tracker.inner, &other.tracker.inner));
        self.size += other.size;
        other.size = 0;
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        if self.size > 0 {
            self.tracker.release(self.size);
        }
    }
}
