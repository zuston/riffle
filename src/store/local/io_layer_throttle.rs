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
use std::cmp::min;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{self, Duration, Instant};

#[derive(Clone)]
pub struct TokenBucketLimiter {
    inner: Arc<Mutex<Inner>>,
    throughput: f64,
}

struct Inner {
    throughput_quota: f64,
    last: Instant,
}

impl TokenBucketLimiter {
    pub fn new(
        rt: &RuntimeRef,
        capacity: usize,
        fill_rate: usize,
        refill_interval: Duration,
    ) -> Self {
        TokenBucketLimiter {
            inner: Arc::new(Mutex::new(Inner {
                throughput_quota: 0f64,
                last: Instant::now(),
            })),
            throughput: capacity as f64,
        }
    }

    pub async fn acquire(&self, throughput: usize) {
        let throughput = throughput as f64;

        loop {
            let mut inner = self.inner.lock().await;
            let now = Instant::now();
            let dur = now.duration_since(inner.last).as_secs_f64();
            let throughput_refill = dur * self.throughput;
            inner.last = now;
            inner.throughput_quota =
                f64::min(inner.throughput_quota + throughput_refill, self.throughput);

            let throughput_refill_duration =
                if self.throughput == 0.0 || inner.throughput_quota >= 0.0 {
                    Duration::ZERO
                } else {
                    Duration::from_secs_f64(-inner.throughput_quota / self.throughput)
                };
            let wait = throughput_refill_duration;
            if wait.is_zero() {
                if self.throughput > 0.0 {
                    inner.throughput_quota -= throughput;
                    return;
                }
            }

            drop(inner);
            tokio::time::sleep(wait).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Instant;

    #[tokio::test]
    async fn test_token_bucket_basic() {
        let rt = RuntimeRef::default();
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
