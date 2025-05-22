use crate::error::WorkerError;
use crate::store::local::layers::{Handler, Layer};
use crate::store::local::{FileStat, LocalIO};
use crate::store::BytesWrapper;
use anyhow::Context;
use async_trait::async_trait;
use await_tree::InstrumentAwait;
use bytes::Bytes;
use dashmap::{DashMap, DashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

pub struct ReadPrefetchLayer;

impl ReadPrefetchLayer {
    pub fn new() -> ReadPrefetchLayer {
        Self
    }
}

impl Layer for ReadPrefetchLayer {
    fn wrap(&self, handler: Handler) -> Handler {
        let layer = ReadPrefetchLayerWrapper {
            handler,
            fetched: Default::default(),
        };
        Arc::new(Box::new(layer))
    }
}

#[derive(Clone)]
struct ReadPrefetchLayerWrapper {
    handler: Handler,
    fetched: DashSet<String>,
}

unsafe impl Send for ReadPrefetchLayerWrapper {}
unsafe impl Sync for ReadPrefetchLayerWrapper {}

#[async_trait]
impl LocalIO for ReadPrefetchLayerWrapper {
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
        #[cfg(all(target_family = "unix", not(target_os = "macos")))]
        {
            if !self.fetched.contains(path) {
                self.fetched.insert(path.to_string());

                use libc::{posix_fadvise, POSIX_FADV_WILLNEED};
                use std::fs::File;
                use std::os::unix::io::AsRawFd;

                if let Ok(file) = File::open(path) {
                    if let Ok(metadata) = file.metadata() {
                        let file_len = metadata.len() as i64;
                        unsafe {
                            let fd = file.as_raw_fd();
                            let ret = posix_fadvise(fd, 0, file_len, POSIX_FADV_WILLNEED);
                            if ret != 0 {
                                // ignore
                            }
                        }
                    }
                }
            }
        }
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
