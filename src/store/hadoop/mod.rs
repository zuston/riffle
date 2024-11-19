#[cfg(feature = "hdfs")]
mod hdfs_native;
#[cfg(feature = "hdfs")]
use crate::store::hadoop::hdfs_native::HdfsNativeClient;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;

#[async_trait]
pub(crate) trait HdfsDelegator: Send + Sync {
    async fn touch(&self, file_path: &str) -> Result<()>;
    async fn append(&self, file_path: &str, data: Bytes) -> Result<()>;
    async fn len(&self, file_path: &str) -> Result<u64>;

    async fn create_dir(&self, dir: &str) -> Result<()>;
    async fn delete_dir(&self, dir: &str) -> Result<()>;
}

#[cfg(feature = "hdfs")]
pub fn getHdfsDelegator(
    root: &str,
    configs: HashMap<String, String>,
) -> Result<Box<dyn HdfsDelegator>> {
    let client = HdfsNativeClient::new(root.to_owned(), configs)?;
    let client: Box<dyn HdfsDelegator> = Box::new(client);
    return Ok(client);
}
