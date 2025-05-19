use crate::error::WorkerError;
use crate::store::hadoop::{FileStatus, HdfsClient};
use crate::store::BytesWrapper;
use anyhow::Context;
use async_trait::async_trait;
use await_tree::InstrumentAwait;
use std::time::Duration;
use tokio::time::timeout;

pub struct HdfsClientDelegator {
    root: String,
    inner: Box<dyn HdfsClient>,
    operation_duration_secs: u64,
}

impl HdfsClientDelegator {
    pub fn new(root: &str, operation_duration_secs: u64, client: Box<dyn HdfsClient>) -> Self {
        Self {
            root: root.to_owned(),
            inner: client,
            operation_duration_secs,
        }
    }
}

#[async_trait]
impl HdfsClient for HdfsClientDelegator {
    async fn touch(&self, file_path: &str) -> anyhow::Result<()> {
        self.inner.touch(file_path).await
    }

    async fn append(&self, file_path: &str, data: BytesWrapper) -> anyhow::Result<(), WorkerError> {
        let f = self.inner.append(file_path, data);
        timeout(Duration::from_secs(self.operation_duration_secs), f)
            .await
            .with_context(|| format!("Errors on appending on hdfs: {}", &self.root))??;
        Ok(())
    }

    async fn len(&self, file_path: &str) -> anyhow::Result<u64> {
        self.inner.len(file_path).await
    }

    async fn create_dir(&self, dir: &str) -> anyhow::Result<()> {
        self.inner.create_dir(dir).await
    }

    async fn delete_dir(&self, dir: &str) -> anyhow::Result<(), WorkerError> {
        self.inner.delete_dir(dir).await
    }

    async fn delete_file(&self, file_path: &str) -> anyhow::Result<(), WorkerError> {
        self.inner.delete_file(file_path).await
    }

    async fn list_status(&self, dir: &str) -> anyhow::Result<Vec<FileStatus>, WorkerError> {
        self.inner.list_status(dir).await
    }

    fn root(&self) -> String {
        self.inner.root()
    }
}
