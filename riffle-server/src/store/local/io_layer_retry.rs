use crate::error::WorkerError;
use crate::store::local::layers::{Handler, Layer};
use crate::store::local::options::{CreateOptions, ReadOptions, WriteOptions};
use crate::store::local::{FileStat, LocalIO};
use crate::store::BytesWrapper;
use async_trait::async_trait;
use bytes::Bytes;
use clap::builder::Str;
use log::error;
use std::sync::Arc;

pub const RETRY_MAX_TIMES: usize = 3;

macro_rules! retry {
    ($f:expr, $count:expr, $interval:expr, $operation:expr) => {{
        let mut retries = 0;
        let result = loop {
            let result = $f;
            if result.is_ok() {
                break result;
            } else if retries > $count {
                break result;
            } else {
                error!("Errors on {}. err: {:?}", $operation, result.err());
                retries += 1;
                tokio::time::sleep(std::time::Duration::from_millis($interval)).await;
            }
        };
        result
    }};
    ($f:expr, $operation:expr) => {
        retry!($f, 3, 100, $operation)
    };
}

pub struct IoLayerRetry {
    root: String,
    max_times: usize,
}

impl IoLayerRetry {
    pub fn new(max_times: usize, root: &str) -> Self {
        Self {
            root: root.to_string(),
            max_times,
        }
    }
}

impl Layer for IoLayerRetry {
    fn wrap(&self, handler: Handler) -> Handler {
        Arc::new(Box::new(IoLayerRetryImpl {
            root: self.root.to_string(),
            max_times: self.max_times,
            handler,
        }))
    }
}

struct IoLayerRetryImpl {
    root: String,
    max_times: usize,
    handler: Handler,
}

unsafe impl Send for IoLayerRetryImpl {}
unsafe impl Sync for IoLayerRetryImpl {}

#[async_trait]
impl LocalIO for IoLayerRetryImpl {
    async fn create(&self, dir: &str, options: CreateOptions) -> anyhow::Result<(), WorkerError> {
        self.handler.create(dir, options).await
    }

    async fn write(&self, path: &str, options: WriteOptions) -> anyhow::Result<(), WorkerError> {
        self.handler.write(path, options).await
    }

    async fn read(
        &self,
        path: &str,
        options: ReadOptions,
    ) -> anyhow::Result<BytesWrapper, WorkerError> {
        self.handler.read(path, options).await
    }

    async fn delete(&self, path: &str) -> anyhow::Result<(), WorkerError> {
        retry! {
            async {
                self.handler.delete(path).await
            }.await,
            RETRY_MAX_TIMES,
            100,
            format!("deleting {}/{}", &self.root, path)
        }
    }

    async fn file_stat(&self, path: &str) -> anyhow::Result<FileStat, WorkerError> {
        self.handler.file_stat(path).await
    }
}
