use crate::error::WorkerError;
use crate::store::local::layers::{Handler, Layer};
use crate::store::local::{FileStat, LocalIO};
use crate::store::BytesWrapper;
use async_trait::async_trait;
use bytes::Bytes;

pub struct AwaitTreeLayer;

impl Layer for AwaitTreeLayer {
    fn wrap(&self, handler: Handler) -> Handler {
        Box::new(AwaitTreeLayerWrapper { handler })
    }
}

#[derive(Clone)]
struct AwaitTreeLayerWrapper {
    handler: Handler,
}

#[async_trait]
impl LocalIO for AwaitTreeLayerWrapper {
    async fn create_dir(&self, dir: &str) -> anyhow::Result<(), WorkerError> {
        todo!()
    }

    async fn append(&self, path: &str, data: BytesWrapper) -> anyhow::Result<(), WorkerError> {
        todo!()
    }

    async fn read(
        &self,
        path: &str,
        offset: i64,
        length: Option<i64>,
    ) -> anyhow::Result<Bytes, WorkerError> {
        todo!()
    }

    async fn delete(&self, path: &str) -> anyhow::Result<(), WorkerError> {
        todo!()
    }

    async fn write(&self, path: &str, data: Bytes) -> anyhow::Result<(), WorkerError> {
        todo!()
    }

    async fn file_stat(&self, path: &str) -> anyhow::Result<FileStat, WorkerError> {
        todo!()
    }

    async fn direct_append(
        &self,
        path: &str,
        written_bytes: usize,
        data: BytesWrapper,
    ) -> anyhow::Result<(), WorkerError> {
        todo!()
    }

    async fn direct_read(
        &self,
        path: &str,
        offset: i64,
        length: i64,
    ) -> anyhow::Result<Bytes, WorkerError> {
        todo!()
    }
}
