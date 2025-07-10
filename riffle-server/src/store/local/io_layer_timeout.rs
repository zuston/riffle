use crate::error::WorkerError;
use crate::store::local::layers::{Handler, Layer};
use crate::store::local::options::{CreateOptions, ReadOptions, WriteOptions};
use crate::store::local::{FileStat, LocalIO};
use crate::store::BytesWrapper;
use anyhow::Context;
use async_trait::async_trait;
use await_tree::InstrumentAwait;
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

pub struct TimeoutLayer {
    threshold_seconds: usize,
}

impl TimeoutLayer {
    pub fn new(threshold_seconds: usize) -> TimeoutLayer {
        Self { threshold_seconds }
    }
}

impl Layer for TimeoutLayer {
    fn wrap(&self, handler: Handler) -> Handler {
        let layer = TimeoutLayerWrapper {
            handler,
            threshold_seconds: self.threshold_seconds as u64,
        };
        Arc::new(Box::new(layer))
    }
}

#[derive(Clone)]
struct TimeoutLayerWrapper {
    handler: Handler,
    threshold_seconds: u64,
}

unsafe impl Send for TimeoutLayerWrapper {}
unsafe impl Sync for TimeoutLayerWrapper {}

#[async_trait]
impl LocalIO for TimeoutLayerWrapper {
    async fn create(&self, path: &str, options: CreateOptions) -> anyhow::Result<(), WorkerError> {
        let f = self.handler.create(path, options);
        timeout(Duration::from_secs(self.threshold_seconds), f).await??;
        Ok(())
    }

    async fn read(
        &self,
        path: &str,
        options: ReadOptions,
    ) -> anyhow::Result<BytesWrapper, WorkerError> {
        let f = self.handler.read(path, options);
        let bytes = timeout(Duration::from_secs(self.threshold_seconds), f).await??;
        Ok(bytes)
    }

    async fn delete(&self, path: &str) -> anyhow::Result<(), WorkerError> {
        let f = self.handler.delete(path);
        timeout(Duration::from_secs(self.threshold_seconds), f).await??;
        Ok(())
    }

    async fn write(&self, path: &str, options: WriteOptions) -> anyhow::Result<(), WorkerError> {
        let f = self.handler.write(path, options);
        timeout(Duration::from_secs(self.threshold_seconds), f).await??;
        Ok(())
    }

    async fn file_stat(&self, path: &str) -> anyhow::Result<FileStat, WorkerError> {
        let f = self.handler.file_stat(path);
        let stat = timeout(Duration::from_secs(self.threshold_seconds), f).await??;
        Ok(stat)
    }
}
