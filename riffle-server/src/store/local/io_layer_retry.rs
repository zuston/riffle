use crate::error::WorkerError;
use crate::store::local::layers::{Handler, Layer};
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
        retry! {
            async {
                self.handler.delete(path).await
            }.await,
            RETRY_MAX_TIMES,
            100,
            format!("deleting {}/{}", &self.root, path)
        }
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
        self.handler.direct_append(path, written_bytes, data).await
    }

    async fn direct_read(
        &self,
        path: &str,
        offset: i64,
        length: i64,
    ) -> anyhow::Result<Bytes, WorkerError> {
        self.handler.direct_read(path, offset, length).await
    }
}
