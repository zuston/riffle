use crate::metric::{GAUGE_MEMORY_ALLOCATED, GAUGE_MEMORY_CAPACITY, GAUGE_MEMORY_USED};
use crate::store::mem::capacity::CapacitySnapshot;
use anyhow::Result;
use std::sync::Arc;

#[derive(Clone)]
pub struct MemoryBudget {
    inner: Arc<std::sync::Mutex<MemoryBudgetInner>>,
}

struct MemoryBudgetInner {
    capacity: i64,
    allocated: i64,
    used: i64,
    allocation_incr_id: i64,
}

impl MemoryBudget {
    fn new(capacity: i64) -> MemoryBudget {
        GAUGE_MEMORY_CAPACITY.set(capacity);
        MemoryBudget {
            inner: Arc::new(std::sync::Mutex::new(MemoryBudgetInner {
                capacity,
                allocated: 0,
                used: 0,
                allocation_incr_id: 0,
            })),
        }
    }

    pub fn snapshot(&self) -> CapacitySnapshot {
        let inner = self.inner.lock().unwrap();
        (inner.capacity, inner.allocated, inner.used).into()
    }

    fn pre_allocate(&self, size: i64) -> Result<(bool, i64)> {
        let mut inner = self.inner.lock().unwrap();
        let free_space = inner.capacity - inner.allocated - inner.used;
        if free_space < size {
            Ok((false, -1))
        } else {
            inner.allocated += size;
            let now = inner.allocation_incr_id;
            inner.allocation_incr_id += 1;
            GAUGE_MEMORY_ALLOCATED.set(inner.allocated);
            Ok((true, now))
        }
    }

    fn allocated_to_used(&self, size: i64) -> Result<bool> {
        let mut inner = self.inner.lock().unwrap();
        if inner.allocated < size {
            inner.allocated = 0;
        } else {
            inner.allocated -= size;
        }
        inner.used += size;
        GAUGE_MEMORY_ALLOCATED.set(inner.allocated);
        GAUGE_MEMORY_USED.set(inner.used);
        Ok(true)
    }

    fn free_used(&self, size: i64) -> Result<bool> {
        let mut inner = self.inner.lock().unwrap();
        if inner.used < size {
            inner.used = 0;
            // todo: metric
        } else {
            inner.used -= size;
        }
        GAUGE_MEMORY_USED.set(inner.used);
        Ok(true)
    }

    fn free_allocated(&self, size: i64) -> Result<bool> {
        let mut inner = self.inner.lock().unwrap();
        if inner.allocated < size {
            inner.allocated = 0;
        } else {
            inner.allocated -= size;
        }
        GAUGE_MEMORY_ALLOCATED.set(inner.allocated);
        Ok(true)
    }
}
