use crate::error::WorkerError;
use crate::runtime::manager::RuntimeManager;
use crate::runtime::RuntimeRef;
use crate::store::local::layers::{Handler, Layer};
use crate::store::local::{FileStat, LocalIO};
use crate::store::BytesWrapper;
use async_trait::async_trait;
use await_tree::InstrumentAwait;
use bytes::Bytes;
use std::cmp::min;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use tokio::time::{self, Duration, Instant};

#[derive(Clone)]
pub struct TokenBucketLimiter {
    inner: Arc<Mutex<Inner>>,
    notify: Arc<Notify>,
}

struct Inner {
    capacity: usize,
    tokens: usize,
    fill_rate: usize,
    last_refill: Instant,
}

impl TokenBucketLimiter {
    pub fn new(
        rt: &RuntimeRef,
        capacity: usize,
        fill_rate: usize,
        refill_interval: Duration,
    ) -> Self {
        let limiter = TokenBucketLimiter {
            inner: Arc::new(Mutex::new(Inner {
                capacity,
                tokens: capacity,
                fill_rate,
                last_refill: Instant::now(),
            })),
            notify: Arc::new(Default::default()),
        };

        let l_c = limiter.clone();
        rt.clone()
            .spawn_with_await_tree("TokenBucketLimiter periodical refill", async move {
                l_c.refill_periodically(refill_interval).await;
            });

        limiter
    }

    // todo: if the acquire amount > capacity, this will hang!
    // blocking acquire
    pub async fn acquire(&self, amount: usize) {
        let mut inner = self
            .inner
            .lock()
            .instrument_await("waiting the limiter lock...")
            .await;
        loop {
            let tokens = &mut inner.tokens;
            if *tokens >= amount {
                *tokens -= amount;
                return;
            } else {
                drop(inner);
                self.notify
                    .notified()
                    .instrument_await("waiting the notify")
                    .await;
                inner = self
                    .inner
                    .lock()
                    .instrument_await("waiting the inner lock...")
                    .await;
            }
        }
    }

    async fn refill(&self) {
        let inner = &mut self
            .inner
            .lock()
            .instrument_await("waiting the limiter lock...")
            .await;
        if inner.tokens >= inner.capacity {
            return;
        }

        let now = Instant::now();
        let elapsed = now.duration_since(inner.last_refill);

        let new_tokens = (elapsed.as_secs_f64() * inner.fill_rate as f64) as usize;
        if new_tokens > 0 {
            inner.tokens = min(inner.tokens + new_tokens, inner.capacity);
            inner.last_refill = now;
            self.notify.notify_waiters();
        }
    }

    async fn refill_periodically(&self, period: Duration) {
        loop {
            tokio::time::sleep(period)
                .instrument_await("sleeping...")
                .await;
            self.refill().instrument_await("refilling...").await;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::runtime::manager::RuntimeManager;
    use crate::store::local::io_layer_throttle::TokenBucketLimiter;
    use std::sync::atomic::Ordering::SeqCst;
    use std::time::Duration;
    use tokio::time::Instant;

    #[test]
    fn test_token_bucket() {
        let rc: RuntimeManager = Default::default();
        let limiter =
            TokenBucketLimiter::new(&rc.localfile_write_runtime, 4, 1, Duration::from_secs(1));

        let rt = rc.default_runtime.clone();

        // case1
        rt.block_on(limiter.acquire(4));
        let l_c = limiter.clone();
        assert_eq!(0, rt.block_on(async move { l_c.inner.lock().await.tokens }));

        // case2
        let start_time = Instant::now();
        rt.block_on(limiter.acquire(2));
        assert!(start_time.elapsed() >= Duration::from_secs(2));

        // case3
        awaitility::at_most(Duration::from_secs(5)).until(|| {
            let l_c = limiter.clone();
            rt.block_on(async move { l_c.inner.lock().await.tokens }) == 4
        });
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
