use crate::config::LocalfileStoreConfig;
use crate::readable_size::ReadableSize;
use crate::runtime::manager::RuntimeManager;
use crate::store::local::sync_io::SyncLocalIO;
use crate::store::local::{FileStat, LocalDiskStorage, LocalIO};
use crate::store::BytesWrapper;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
struct LocalDiskDelegator {
    io_handler: SyncLocalIO,

    is_healthy: Arc<AtomicBool>,
    is_corrupted: Arc<AtomicBool>,

    high_watermark: f32,
    low_watermark: f32,

    concurrency: usize,
}

impl LocalDiskDelegator {
    pub fn new(
        runtime_manager: &RuntimeManager,
        root: &str,
        config: &LocalfileStoreConfig,
    ) -> LocalDiskDelegator {
        let high_watermark = config.disk_high_watermark;
        let low_watermark = config.disk_low_watermark;
        let concurrency = config.disk_max_concurrency as usize;
        let write_capacity = ReadableSize::from_str(&config.disk_write_buf_capacity).unwrap();
        let read_capacity = ReadableSize::from_str(&config.disk_read_buf_capacity).unwrap();

        let runtime_manager = runtime_manager.clone();
        let io = SyncLocalIO::new(
            root,
            Some(write_capacity.as_bytes() as usize),
            Some(read_capacity.as_bytes() as usize),
        );

        let runtime = runtime_manager.default_runtime.clone();
        runtime.spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                // 1. check watermark
                // 2. check write
            }
        });

        Self {
            io_handler: io,
            is_healthy: Arc::new(AtomicBool::new(true)),
            is_corrupted: Arc::new(AtomicBool::new(false)),
            high_watermark,
            low_watermark,
            concurrency,
        }
    }

    fn get_disk_used_ratio(root: &str, capacity: u64) -> Result<f64> {
        // Get the total and available space in bytes
        let available_space = fs2::available_space(root)?;
        Ok(1.0 - (available_space as f64 / capacity as f64))
    }

    fn get_disk_capacity(root: &str) -> Result<u64> {
        Ok(fs2::total_space(root)?)
    }

    fn get_disk_available(root: &str) -> Result<u64> {
        Ok(fs2::available_space(root)?)
    }
}

#[async_trait]
impl LocalIO for LocalDiskDelegator {
    async fn create_dir(&self, dir: &str) -> Result<()> {
        self.io_handler.create_dir(dir).await
    }

    async fn append(&self, path: &str, data: Bytes) -> Result<()> {
        self.io_handler.append(path, data).await
    }

    async fn read(&self, path: &str, offset: i64, length: Option<i64>) -> Result<Bytes> {
        self.io_handler.read(path, offset, length).await
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.io_handler.delete(path).await
    }

    async fn write(&self, path: &str, data: Bytes) -> Result<()> {
        self.io_handler.write(path, data).await
    }

    async fn file_stat(&self, path: &str) -> Result<FileStat> {
        self.io_handler.file_stat(path).await
    }
}

#[async_trait]
impl LocalDiskStorage for LocalDiskDelegator {
    async fn is_healthy(&self) -> Result<bool> {
        Ok(self.is_healthy.load(SeqCst))
    }

    async fn is_corrupted(&self) -> Result<bool> {
        Ok(self.is_corrupted.load(SeqCst))
    }

    async fn mark_unhealthy(&self) -> Result<()> {
        self.is_healthy.store(false, SeqCst);
        Ok(())
    }

    async fn mark_corrupted(&self) -> Result<()> {
        self.is_corrupted.store(true, SeqCst);
        Ok(())
    }
}
