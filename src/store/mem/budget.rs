use std::sync::Arc;
use crate::metric::{GAUGE_MEMORY_ALLOCATED, GAUGE_MEMORY_CAPACITY, GAUGE_MEMORY_USED};
use crate::store::mem::capacity::CapacitySnapshot;
use anyhow::Result;
use std::sync::atomic::{AtomicI64, Ordering};

#[derive(Clone)]
pub struct MemoryBudget {
    capacity: i64,
    allocated: Arc<AtomicI64>,
    used: Arc<AtomicI64>,
    allocation_incr_id: Arc<AtomicI64>,
}

impl MemoryBudget {
    pub(crate) fn new(capacity: i64) -> MemoryBudget {
        GAUGE_MEMORY_CAPACITY.set(capacity);
        MemoryBudget {
            capacity,
            allocated: Default::default(),
            used: Default::default(),
            allocation_incr_id: Default::default(),
        }
    }

    pub fn snapshot(&self) -> CapacitySnapshot {
        (self.capacity, self.allocated.load(Ordering::Relaxed), self.used.load(Ordering::Relaxed)).into()
    }

    pub(crate) fn pre_allocate(&self, size: i64) -> Result<(bool, i64)> {
        let capacity = self.capacity;
        let allocated = self.allocated.load(Ordering::SeqCst);
        let used = self.used.load(Ordering::SeqCst);

        let free_space = capacity - allocated - used;
        if free_space < size {
            Ok((false, -1))
        } else {
            self.allocated.fetch_add(size, Ordering::SeqCst);
            GAUGE_MEMORY_ALLOCATED.set(allocated + size);
            let id = self.allocation_incr_id.fetch_add(1, Ordering::SeqCst);
            Ok((true, id))
        }
    }

    pub(crate) fn allocated_to_used(&self, size: i64) -> Result<bool> {
        let mut allocated = self.allocated.load(Ordering::SeqCst);
        if allocated < size {
            self.allocated.store(0, Ordering::SeqCst);
            allocated = 0;
        } else {
            self.allocated.fetch_add(-size, Ordering::SeqCst);
            allocated -= size;
        }
        let previous = self.used.fetch_add(size, Ordering::SeqCst);
        GAUGE_MEMORY_ALLOCATED.set(allocated);
        GAUGE_MEMORY_USED.set(previous + size);
        Ok(true)
    }

    pub(crate) fn free_used(&self, size: i64) -> Result<bool> {
        let mut used = self.used.load(Ordering::SeqCst);
        if used < size {
            self.used.store(0, Ordering::SeqCst);
            used = 0;
        } else {
            self.used.fetch_add(-size, Ordering::SeqCst);
            used -= size;
        }
        GAUGE_MEMORY_USED.set(used);
        Ok(true)
    }

    pub fn free_allocated(&self, size: i64) -> Result<bool> {
        let mut allocated = self.allocated.load(Ordering::SeqCst);
        if allocated < size {
            self.allocated.store(0, Ordering::SeqCst);
            allocated = 0;
        } else {
            self.allocated.fetch_add(-size, Ordering::SeqCst);
            allocated -= size;
        }
        GAUGE_MEMORY_ALLOCATED.set(allocated);
        Ok(true)
    }
}
