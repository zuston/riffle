use crate::error::WorkerError;
use crate::runtime::manager::RuntimeManager;
use crate::runtime::RuntimeRef;
use crate::store::local::layers::{Handler, Layer};
use crate::store::local::{FileStat, LocalIO};
use crate::store::BytesWrapper;
use async_trait::async_trait;
use await_tree::InstrumentAwait;
use bytes::Bytes;
use log::warn;
use parking_lot::Mutex;
use std::cmp::min;
use std::num::NonZeroU32;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use governor::{Quota, RateLimiter};
use governor::clock::{Clock, DefaultClock};
use governor::state::{InMemoryState, NotKeyed};
use nonzero_ext::nonzero;
use tokio::time::{self, Duration, Instant};

#[derive(Clone)]
pub struct TokenBucketLimiter {
    limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
    clock: DefaultClock,
}

impl TokenBucketLimiter {
    pub fn new(
        rt: &RuntimeRef,
        capacity: usize,
        fill_rate: usize,
        refill_interval: Duration,
    ) -> Self {
        let clock = DefaultClock::default();
        let capacity = NonZeroU32::new(capacity as u32).unwrap();
        let limiter = Arc::new(RateLimiter::direct_with_clock(Quota::per_second(
            capacity,
        ), clock.clone()));
        TokenBucketLimiter {
            limiter,
            clock,
        }
    }

    pub async fn acquire(&self, throughput: usize) {
        if throughput <= 0 {
            return;
        }
        let throughput = NonZeroU32::new(throughput as u32).unwrap();
        loop {
            match self.limiter.check_n(throughput) {
                Ok(Ok(())) => {
                    return;
                },
                Ok(Err(wait)) => {
                    // Not enough capacity right now, but could be allowed later
                    let wait_duration = wait.wait_time_from(self.clock.now());
                    // Wait and try again...
                    tokio::time::sleep(wait_duration)
                        .instrument_await("Throttle limited. waiting...")
                        .await;
                },
                Err(insufficient) => {
                    // Will never be allowed (requested more than maximum capacity)
                    println!("Maximum capacity is {}", insufficient.0);
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use governor::clock;
    use governor::clock::{Clock, DefaultClock};
    use nonzero_ext::nonzero;
    use super::*;
    use tokio::time::Instant;
    use crate::runtime::manager::create_runtime;

    #[tokio::test]
    async fn test_limiter() {
        let capacity = nonzero!((1024 * 1024 * 1024) as u32);
        let clock = DefaultClock::default();
        let rate_limiter = Arc::new(RateLimiter::direct_with_clock(
            Quota::per_second(capacity).allow_burst(capacity), clock.clone()
        ));
        loop {
            match rate_limiter.check_n(nonzero!(1u32)) {
                Ok(Ok(())) => {
                    println!("Ok");
                    return;
                },
                Ok(Err(wait)) => {
                    // Not enough capacity right now, but could be allowed later
                    let wait_duration = wait.wait_time_from(clock.now());
                    // Wait and try again...
                    tokio::time::sleep(wait_duration).await;
                },
                Err(insufficient) => {
                    // Will never be allowed (requested more than maximum capacity)
                    println!("Maximum capacity is {}", insufficient.0);
                    return;
                }
            }
        }
    }

    #[tokio::test]
    async fn test_token_bucket_basic() {
        let rt = create_runtime(10, "");
        let capacity = 10;
        let fill_rate = 10;
        let refill_interval = Duration::from_secs(1);
        let limiter = TokenBucketLimiter::new(&rt, capacity, fill_rate, refill_interval);

        // First acquire should not wait
        let start = Instant::now();
        limiter.acquire(5).await;
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(10),
            "First acquire should not wait"
        );

        // Acquire more than capacity should wait
        let start = Instant::now();
        limiter.acquire(20).await;
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(100),
            "Should wait due to rate limiting"
        );
    }
}

pub struct ThrottleLayer {
    runtime: RuntimeRef,
    capacity: usize,
    fill_rate: usize,
    refill_interval: Duration,
}

impl ThrottleLayer {
    pub fn new(
        rt: &RuntimeRef,
        capacity: usize,
        fill_rate: usize,
        refill_interval: Duration,
    ) -> Self {
        Self {
            runtime: rt.clone(),
            capacity,
            fill_rate,
            refill_interval,
        }
    }
}

impl Layer for ThrottleLayer {
    fn wrap(&self, handler: Handler) -> Handler {
        Arc::new(Box::new(ThrottleLayerWrapper {
            limiter: TokenBucketLimiter::new(
                &self.runtime,
                self.capacity,
                self.fill_rate,
                self.refill_interval,
            ),
            handler,
        }))
    }
}

struct ThrottleLayerWrapper {
    limiter: TokenBucketLimiter,
    handler: Handler,
}

unsafe impl Send for ThrottleLayerWrapper {}
unsafe impl Sync for ThrottleLayerWrapper {}

#[async_trait]
impl LocalIO for ThrottleLayerWrapper {
    async fn create_dir(&self, dir: &str) -> anyhow::Result<(), WorkerError> {
        self.handler.create_dir(dir).await
    }

    async fn append(&self, path: &str, data: BytesWrapper) -> anyhow::Result<(), WorkerError> {
        self.handler.append(path, data).await
    }

    async fn read(
        &self,
        path: &str,
        offset: i64,
        length: Option<i64>,
    ) -> anyhow::Result<Bytes, WorkerError> {
        self.handler.read(path, offset, length).await
    }

    async fn delete(&self, path: &str) -> anyhow::Result<(), WorkerError> {
        self.handler.delete(path).await
    }

    async fn write(&self, path: &str, data: Bytes) -> anyhow::Result<(), WorkerError> {
        self.handler.write(path, data).await
    }

    async fn file_stat(&self, path: &str) -> anyhow::Result<FileStat, WorkerError> {
        self.handler.file_stat(path).await
    }

    async fn direct_append(
        &self,
        path: &str,
        written_bytes: usize,
        data: BytesWrapper,
    ) -> anyhow::Result<(), WorkerError> {
        self.limiter
            .acquire(written_bytes)
            .instrument_await(format!("Getting IO limiter permits: {}", written_bytes))
            .await;

        self.handler.direct_append(path, written_bytes, data).await
    }

    async fn direct_read(
        &self,
        path: &str,
        offset: i64,
        length: i64,
    ) -> anyhow::Result<Bytes, WorkerError> {
        let len = 14 * 1024 * 1024;
        self.limiter
            .acquire(len as usize)
            .instrument_await(format!("Getting IO limiter permits: {}", len))
            .await;

        self.handler.direct_read(path, offset, length).await
    }
}
