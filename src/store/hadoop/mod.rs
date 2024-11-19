#[cfg(feature = "hdfs")]
mod hdfs_native;
#[cfg(feature = "hdrs")]
mod hdrs;

#[cfg(feature = "hdfs")]
use crate::store::hadoop::hdfs_native::HdfsNativeClient;
#[cfg(feature = "hdrs")]
use crate::store::hadoop::hdrs::HdrsClient;

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
    #[cfg(not(feature = "hdrs"))]
    return Ok(Box::new(HdfsNativeClient::new(root.to_owned(), configs)?));

    #[cfg(feature = "hdrs")]
    return Ok(Box::new(HdrsClient::new(root.to_owned(), configs)?));
}
