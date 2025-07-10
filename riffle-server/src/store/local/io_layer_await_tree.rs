use crate::error::WorkerError;
use crate::store::local::layers::{Handler, Layer};
use crate::store::local::options::{CreateOptions, ReadOptions, WriteOptions};
use crate::store::local::{FileStat, LocalIO};
use crate::store::DataBytes;
use anyhow::Context;
use async_trait::async_trait;
use await_tree::InstrumentAwait;
use bytes::Bytes;
use std::sync::Arc;
use tracing_subscriber::fmt::format;

pub struct AwaitTreeLayer {
    root: String,
}

impl AwaitTreeLayer {
    pub fn new(root: &str) -> Self {
        Self {
            root: root.to_string(),
        }
    }
}

impl Layer for AwaitTreeLayer {
    fn wrap(&self, handler: Handler) -> Handler {
        Arc::new(Box::new(AwaitTreeLayerWrapper {
            handler,
            root: self.root.to_owned(),
        }))
    }
}

#[derive(Clone)]
struct AwaitTreeLayerWrapper {
    handler: Handler,
    root: String,
}

unsafe impl Send for AwaitTreeLayerWrapper {}
unsafe impl Sync for AwaitTreeLayerWrapper {}

#[async_trait]
impl LocalIO for AwaitTreeLayerWrapper {
    async fn create(&self, path: &str, options: CreateOptions) -> anyhow::Result<(), WorkerError> {
        self.handler
            .create(path, options)
            .instrument_await(format!("Creating {}/{}", &self.root, path))
            .await
            .with_context(|| format!("failed to create [{}]", path))?;
        Ok(())
    }

    async fn write(&self, path: &str, options: WriteOptions) -> anyhow::Result<(), WorkerError> {
        self.handler
            .write(path, options)
            .instrument_await(format!("Writing to file: {}/{}", &self.root, path))
            .await
            .with_context(|| format!("failed to write to file: {}/{}", &self.root, path))?;
        Ok(())
    }

    async fn read(
        &self,
        path: &str,
        options: ReadOptions,
    ) -> anyhow::Result<DataBytes, WorkerError> {
        let data = self
            .handler
            .read(path, options)
            .instrument_await(format!("Reading from file: {}/{}", &self.root, path))
            .await
            .with_context(|| format!("failed to read from file: {}/{}", &self.root, path))?;
        Ok(data)
    }

    async fn delete(&self, path: &str) -> anyhow::Result<(), WorkerError> {
        self.handler
            .delete(path)
            .instrument_await(format!("Deleting file: {}/{}", &self.root, path))
            .await
            .with_context(|| format!("failed to delete file: {}/{}", &self.root, path))?;
        Ok(())
    }

    async fn file_stat(&self, path: &str) -> anyhow::Result<FileStat, WorkerError> {
        let stat = self
            .handler
            .file_stat(path)
            .instrument_await(format!("Stating file: {}/{}", &self.root, path))
            .await
            .with_context(|| format!("failed to state file: {}/{}", &self.root, path))?;
        Ok(stat)
    }
}
