use crate::await_tree::AWAIT_TREE_REGISTRY;
use crate::config::LocalfileStoreConfig;
use crate::metric::{
    GAUGE_LOCAL_DISK_IS_HEALTHY, GAUGE_LOCAL_DISK_USED, LOCALFILE_DISK_APPEND_OPERATION_DURATION,
    LOCALFILE_DISK_DELETE_OPERATION_DURATION, LOCALFILE_DISK_READ_OPERATION_DURATION,
    TOTAL_LOCAL_DISK_APPEND_OPERATION_BYTES_COUNTER, TOTAL_LOCAL_DISK_APPEND_OPERATION_COUNTER,
    TOTAL_LOCAL_DISK_READ_OPERATION_BYTES_COUNTER, TOTAL_LOCAL_DISK_READ_OPERATION_COUNTER,
};
use crate::readable_size::ReadableSize;
use crate::runtime::manager::RuntimeManager;
use crate::store::local::sync_io::SyncLocalIO;
use crate::store::local::{FileStat, LocalDiskStorage, LocalIO};
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use log::{error, warn};
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, Instrument};

#[derive(Clone)]
pub struct LocalDiskDelegator {
    inner: Arc<Inner>,
}

struct Inner {
    root: String,

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

        let io_handler = SyncLocalIO::new(
            root,
            Some(write_capacity.as_bytes() as usize),
            Some(read_capacity.as_bytes() as usize),
        );

        let delegator = Self {
            inner: Arc::new(Inner {
                root: root.to_owned(),
                io_handler,
                is_healthy: Arc::new(AtomicBool::new(true)),
                is_corrupted: Arc::new(AtomicBool::new(false)),
                high_watermark,
                low_watermark,
                concurrency,
            }),
        };

        let runtime = runtime_manager.clone().default_runtime.clone();
        let io_delegator = delegator.clone();
        let span = format!("disk[{}] checker", root);
        runtime.spawn(async move {
            let await_tree = AWAIT_TREE_REGISTRY.register(span).await;
            await_tree.instrument(async move {
                info!("starting the disk[{}] checker", &io_delegator.inner.root);
                if let Err(e) = io_delegator.schedule_check().await {
                    error!(
                        "disk[{}] checker exit. err: {:?}",
                        &io_delegator.inner.root, e
                    )
                }
            });
        });

        delegator
    }

    pub fn root(&self) -> String {
        self.inner.root.to_owned()
    }

    async fn schedule_check(&self) -> Result<()> {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
            if self.is_corrupted()? {
                continue;
            }

            if let Err(e) = self.capacity_check().await {
                error!(
                    "Errors on checking the disk:{} capacity. err: {:#?}",
                    &self.inner.root, e
                );
            }
            if let Err(e) = self.write_read_check().await {
                error!(
                    "Errors on checking the disk:{} write+read. err: {:#?}",
                    &self.inner.root, e
                );
                self.mark_corrupted()?;
            }
        }
    }

    async fn capacity_check(&self) -> Result<()> {
        let capacity = Self::get_disk_capacity(&self.inner.root)?;
        let available = Self::get_disk_available(&self.inner.root)?;
        let used = capacity - available;

        GAUGE_LOCAL_DISK_USED
            .with_label_values(&[&self.inner.root])
            .set(used as i64);

        let used_ratio = used as f64 / capacity as f64;
        let healthy_stat = self.is_healthy()?;

        if healthy_stat && used_ratio > self.inner.high_watermark as f64 {
            warn!("Disk={} has been unhealthy", &self.inner.root);
            self.mark_unhealthy()?;
            GAUGE_LOCAL_DISK_IS_HEALTHY
                .with_label_values(&[&self.inner.root])
                .set(1i64);
        }

        if !healthy_stat && used_ratio < self.inner.low_watermark as f64 {
            warn!("Disk={} has been healthy.", &self.inner.root);
            self.mark_healthy()?;
            GAUGE_LOCAL_DISK_IS_HEALTHY
                .with_label_values(&[&self.inner.root])
                .set(0i64);
        }

        Ok(())
    }

    async fn write_read_check(&self) -> Result<()> {
        let temp_path = "corruption_check.file";
        self.delete(temp_path).await?;

        let written_data = Bytes::copy_from_slice(b"hello world");
        self.write(temp_path, written_data.clone()).await?;
        let read_data = self.read(temp_path, 0, None).await?;

        if written_data != read_data {
            error!(
                "The local disk has been corrupted. path: {}. expected: {:?}, actual: {:?}",
                &self.inner.root, &written_data, &read_data
            );
            self.mark_corrupted()?;
        }

        Ok(())
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
        self.inner.io_handler.create_dir(dir).await
    }

    async fn append(&self, path: &str, data: Bytes) -> Result<()> {
        // todo: add the concurrency limitation. do we need? may be not.

        let timer = LOCALFILE_DISK_APPEND_OPERATION_DURATION
            .with_label_values(&[&self.inner.root])
            .start_timer();
        let len = data.len();

        self.inner.io_handler.append(path, data).await?;

        timer.observe_duration();
        TOTAL_LOCAL_DISK_APPEND_OPERATION_BYTES_COUNTER
            .with_label_values(&[&self.inner.root])
            .inc_by(len as u64);
        TOTAL_LOCAL_DISK_APPEND_OPERATION_COUNTER
            .with_label_values(&[&self.inner.root])
            .inc();
        Ok(())
    }

    async fn read(&self, path: &str, offset: i64, length: Option<i64>) -> Result<Bytes> {
        let timer = LOCALFILE_DISK_READ_OPERATION_DURATION
            .with_label_values(&[&self.inner.root])
            .start_timer();

        let data = self.inner.io_handler.read(path, offset, length).await?;

        timer.observe_duration();
        TOTAL_LOCAL_DISK_READ_OPERATION_BYTES_COUNTER
            .with_label_values(&[&self.inner.root])
            .inc_by(data.len() as u64);
        TOTAL_LOCAL_DISK_READ_OPERATION_COUNTER
            .with_label_values(&[&self.inner.root])
            .inc();
        Ok(data)
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let timer = LOCALFILE_DISK_DELETE_OPERATION_DURATION
            .with_label_values(&[&self.inner.root])
            .start_timer();

        self.inner.io_handler.delete(path).await?;

        timer.observe_duration();

        Ok(())
    }

    async fn write(&self, path: &str, data: Bytes) -> Result<()> {
        self.inner.io_handler.write(path, data).await
    }

    async fn file_stat(&self, path: &str) -> Result<FileStat> {
        self.inner.io_handler.file_stat(path).await
    }
}

impl LocalDiskStorage for LocalDiskDelegator {
    fn is_healthy(&self) -> Result<bool> {
        Ok(self.inner.is_healthy.load(SeqCst))
    }

    fn is_corrupted(&self) -> Result<bool> {
        Ok(self.inner.is_corrupted.load(SeqCst))
    }

    fn mark_healthy(&self) -> Result<()> {
        self.inner.is_healthy.store(true, SeqCst);
        Ok(())
    }

    fn mark_unhealthy(&self) -> Result<()> {
        self.inner.is_healthy.store(false, SeqCst);
        Ok(())
    }

    fn mark_corrupted(&self) -> Result<()> {
        self.inner.is_corrupted.store(true, SeqCst);
        Ok(())
    }
}

#[cfg(test)]
mod test {

    #[tokio::test]
    async fn test() -> anyhow::Result<()> {
        Ok(())
    }
}
