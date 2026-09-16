use crate::metric::{GAUGE_MEMORY_ALLOCATED, GAUGE_MEMORY_CAPACITY, GAUGE_MEMORY_USED};
use crate::store::mem::capacity::CapacitySnapshot;
use anyhow::Result;
use fastrace::trace;
use log::warn;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct MemoryBudget {
    capacity: i64,
    high_watermark_duration: Option<Duration>,
    allocated_high_watermark: i64,
    inner: Arc<parking_lot::Mutex<BudgetInner>>,
}

#[derive(Default)]
struct BudgetInner {
    allocated: i64,
    used: i64,
    allocation_inc_counter: i64,
    allocated_high_since: Option<Instant>,
    unhealthy: bool,
}

impl MemoryBudget {
    pub(crate) fn new(
        capacity: i64,
        high_watermark_duration_sec: Option<u64>,
        high_watermark_ratio: f64,
    ) -> MemoryBudget {
        if let Some(duration) = high_watermark_duration_sec {
            assert!(
                duration > 0,
                "allocated_buffer_high_watermark_duration_sec must be positive"
            );
            assert!(
                capacity > 0 && high_watermark_ratio > 0.0 && high_watermark_ratio < 1.0,
                "Allocated buffer health checking requires positive capacity and allocated_buffer_high_watermark_ratio in (0, 1)"
            );
        }
        GAUGE_MEMORY_CAPACITY.set(capacity);
        MemoryBudget {
            capacity,
            high_watermark_duration: high_watermark_duration_sec.map(Duration::from_secs),
            allocated_high_watermark: (capacity as f64 * high_watermark_ratio).floor() as i64,
            inner: Default::default(),
        }
    }

    fn update_allocated_high_since(&self, inner: &mut BudgetInner) {
        if self.high_watermark_duration.is_none() {
            return;
        }
        // Update under the budget lock so even a dip between health checks resets the timer.
        if inner.allocated > self.allocated_high_watermark {
            inner.allocated_high_since.get_or_insert_with(Instant::now);
        } else {
            inner.allocated_high_since = None;
        }
    }

    pub fn is_healthy(&self) -> bool {
        self.check_health(Instant::now())
    }

    fn check_health(&self, now: Instant) -> bool {
        let Some(duration) = self.high_watermark_duration else {
            return true;
        };
        let mut inner = self.inner.lock();
        if !inner.unhealthy
            && inner
                .allocated_high_since
                .is_some_and(|since| now.saturating_duration_since(since) > duration)
        {
            inner.unhealthy = true;
            warn!(
                "Mark the service unhealthy: allocated buffer {} bytes has stayed above {} bytes for more than {} seconds",
                inner.allocated, self.allocated_high_watermark, duration.as_secs()
            );
        }
        !inner.unhealthy
    }

    #[trace]
    pub fn snapshot(&self) -> CapacitySnapshot {
        let capacity = self.capacity;
        let inner = self.inner.lock();
        let allocated = inner.allocated;
        let used = inner.used;
        drop(inner);
        (capacity, allocated, used).into()
    }

    #[trace]
    pub fn require_allocated(&self, size: i64) -> Result<(bool, i64)> {
        let capacity = self.capacity;

        let mut inner = self.inner.lock();
        let allocated = inner.allocated;
        let used = inner.used;

        let remaining = capacity - allocated - used;
        if remaining < size {
            Ok((false, -1))
        } else {
            inner.allocated += size;
            self.update_allocated_high_since(&mut inner);
            GAUGE_MEMORY_ALLOCATED.set(inner.allocated);
            inner.allocation_inc_counter += 1;
            Ok((true, inner.allocation_inc_counter))
        }
    }

    #[trace]
    pub fn move_allocated_to_used(&self, size: i64) -> Result<bool> {
        let mut inner = self.inner.lock();
        let allocated = inner.allocated;

        let mut desc = size;
        if allocated < size {
            desc = allocated;
            inner.allocated = 0;
        } else {
            inner.allocated -= size;
        }
        inner.used += desc;
        self.update_allocated_high_since(&mut inner);
        GAUGE_MEMORY_ALLOCATED.set(inner.allocated);
        GAUGE_MEMORY_USED.set(inner.used);
        Ok(true)
    }

    pub fn inc_used(&self, size: i64) -> Result<bool> {
        let mut inner = self.inner.lock();
        inner.used += size;
        GAUGE_MEMORY_USED.set(inner.used);
        Ok(true)
    }

    #[trace]
    pub fn dec_used(&self, size: i64) -> Result<bool> {
        let mut inner = self.inner.lock();
        if inner.used < size {
            inner.used = 0;
        } else {
            inner.used -= size;
        }
        GAUGE_MEMORY_USED.set(inner.used);
        Ok(true)
    }

    #[trace]
    pub fn dec_allocated(&self, size: i64) -> Result<bool> {
        let mut inner = self.inner.lock();
        if inner.allocated < size {
            inner.allocated = 0;
        } else {
            inner.allocated -= size;
        }
        self.update_allocated_high_since(&mut inner);
        GAUGE_MEMORY_ALLOCATED.set(inner.allocated);
        Ok(true)
    }
}

#[cfg(test)]
mod test {
    use crate::config::MemoryStoreConfig;
    use crate::metric::{GAUGE_MEMORY_ALLOCATED, GAUGE_MEMORY_USED};
    use crate::store::mem::budget::MemoryBudget;
    use std::time::{Duration, Instant};

    #[test]
    fn allocated_buffer_high_watermark_requires_continuous_duration() -> anyhow::Result<()> {
        let budget = MemoryBudget::new(100, Some(60), 0.5);
        budget.require_allocated(50)?;
        assert!(budget.inner.lock().allocated_high_since.is_none());
        assert!(budget.check_health(Instant::now() + Duration::from_secs(600)));

        budget.require_allocated(1)?;
        let since = Instant::now() - Duration::from_secs(59);
        budget.inner.lock().allocated_high_since = Some(since);
        assert!(budget.check_health(since + Duration::from_secs(60)));

        // Allocations and successful writes above the threshold keep the original timer.
        budget.require_allocated(20)?;
        budget.move_allocated_to_used(10)?;
        assert_eq!(61, budget.snapshot().allocated());
        assert_eq!(Some(since), budget.inner.lock().allocated_high_since);

        // A dip to exactly the threshold resets the timer even between health checks.
        budget.dec_allocated(11)?;
        assert!(budget.inner.lock().allocated_high_since.is_none());
        budget.require_allocated(1)?;
        let restarted = budget.inner.lock().allocated_high_since.unwrap();
        assert!(restarted > since);
        assert!(budget.check_health(restarted + Duration::from_secs(59)));

        // Moving allocated bytes to used must also reset the timer on a downward crossing.
        budget.move_allocated_to_used(1)?;
        assert!(budget.inner.lock().allocated_high_since.is_none());
        budget.require_allocated(1)?;
        let restarted = budget.inner.lock().allocated_high_since.unwrap();
        assert!(budget.check_health(restarted + Duration::from_secs(60)));
        assert!(!budget.check_health(restarted + Duration::from_secs(60) + Duration::from_nanos(1)));
        budget.dec_allocated(51)?;
        assert!(!budget.is_healthy());
        assert!(!budget.clone().is_healthy());

        let conf: MemoryStoreConfig = toml::from_str("capacity = \"100B\"")?;
        assert_eq!(None, conf.allocated_buffer_high_watermark_duration_sec);
        assert_eq!(0.5, conf.allocated_buffer_high_watermark_ratio);
        let disabled = MemoryBudget::new(100, None, 0.5);
        disabled.require_allocated(100)?;
        assert!(disabled.check_health(Instant::now() + Duration::from_secs(600)));
        Ok(())
    }

    #[test]
    fn invalid_allocated_buffer_health_config_is_rejected() {
        for (duration, ratio) in [(0, 0.5), (60, 0.0), (60, 1.0), (60, 1.1), (60, f64::NAN)] {
            assert!(
                std::panic::catch_unwind(|| MemoryBudget::new(100, Some(duration), ratio)).is_err()
            );
        }
        // High watermarks can persist through ticket turnover, regardless of ticket timeout.
        assert!(MemoryBudget::new(100, Some(600), 0.5).is_healthy());
    }

    #[test]
    fn basic() -> anyhow::Result<()> {
        let memory_budget = MemoryBudget::new(100, None, 0.5);

        // case1: reject the overflow allocation size
        let (succeed, id) = memory_budget.require_allocated(120)?;
        assert!(!succeed);

        // case2: pass the legal allocation size
        let (succeed, _) = memory_budget.require_allocated(50)?;
        assert!(succeed);

        let snapshot = memory_budget.snapshot();
        assert_eq!(100, snapshot.capacity());
        assert_eq!(0, snapshot.used());
        assert_eq!(50, snapshot.allocated());

        // case3: allocation to used
        memory_budget.move_allocated_to_used(50)?;

        let snapshot = memory_budget.snapshot();
        assert_eq!(100, snapshot.capacity());
        assert_eq!(50, snapshot.used());
        assert_eq!(0, snapshot.allocated());

        // case4: release the used
        memory_budget.dec_used(50)?;
        let snapshot = memory_budget.snapshot();
        assert_eq!(100, snapshot.capacity());
        assert_eq!(0, snapshot.used());
        assert_eq!(0, snapshot.allocated());

        // case5: check the metrics
        assert_eq!(0, GAUGE_MEMORY_ALLOCATED.get());
        assert_eq!(0, GAUGE_MEMORY_USED.get());

        Ok(())
    }
}
