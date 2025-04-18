use crate::error::WorkerError;
use crate::store::local::layers::{Handler, Layer};
use crate::store::local::{FileStat, LocalIO};
use crate::store::BytesWrapper;
use anyhow::Context;
use async_trait::async_trait;
use await_tree::InstrumentAwait;
use bytes::Bytes;
use std::sync::Arc;
use tracing_subscriber::fmt::format;

pub struct AwaitTreeLayer {
    root: String,
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
    async fn create_dir(&self, dir: &str) -> anyhow::Result<(), WorkerError> {
        self.handler
            .create_dir(dir)
            .instrument_await(format!("Creating dir: {}/{}", &self.root, dir))
            .await
            .with_context(|| format!("failed to create dir: {}", dir))?;
        Ok(())
    }

    async fn append(&self, path: &str, data: BytesWrapper) -> anyhow::Result<(), WorkerError> {
        self.handler
            .append(path, data)
            .instrument_await(format!("Appending to file: {}/{}", &self.root, path))
            .await
            .with_context(|| format!("failed to append to file: {}/{}", &self.root, path))?;
        Ok(())
    }

    async fn read(
        &self,
        path: &str,
        offset: i64,
        length: Option<i64>,
    ) -> anyhow::Result<Bytes, WorkerError> {
        let data = self
            .handler
            .read(path, offset, length)
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

    async fn write(&self, path: &str, data: Bytes) -> anyhow::Result<(), WorkerError> {
        self.handler
            .write(path, data)
            .instrument_await(format!("Writing file: {}/{}", &self.root, path))
            .await
            .with_context(|| format!("failed to write file: {}/{}", &self.root, path))?;
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

    async fn direct_append(
        &self,
        path: &str,
        written_bytes: usize,
        data: BytesWrapper,
    ) -> anyhow::Result<(), WorkerError> {
        self.handler
            .direct_append(path, written_bytes, data)
            .instrument_await(format!("Direct appending file: {}/{}", &self.root, path))
            .await
            .with_context(|| format!("failed to direct append file: {}/{}", &self.root, path))?;
        Ok(())
    }

    async fn direct_read(
        &self,
        path: &str,
        offset: i64,
        length: i64,
    ) -> anyhow::Result<Bytes, WorkerError> {
        let bytes = self
            .handler
            .direct_read(path, offset, length)
            .instrument_await(format!("Direct reading file: {}/{}", &self.root, path))
            .await
            .with_context(|| format!("failed to direct read file: {}/{}", &self.root, path))?;
        Ok(bytes)
    }
}
