use crate::app_manager::SHUFFLE_SERVER_ID;
use crate::await_tree::AWAIT_TREE_REGISTRY;
use crate::config::LocalfileStoreConfig;
use crate::error::WorkerError;
use crate::metric::{
    GAUGE_LOCAL_DISK_CAPACITY, GAUGE_LOCAL_DISK_IS_CORRUPTED, GAUGE_LOCAL_DISK_IS_HEALTHY,
    GAUGE_LOCAL_DISK_USED, GAUGE_LOCAL_DISK_USED_RATIO, LOCALFILE_DISK_APPEND_OPERATION_DURATION,
    LOCALFILE_DISK_DELETE_OPERATION_DURATION, LOCALFILE_DISK_DIRECT_APPEND_OPERATION_DURATION,
    LOCALFILE_DISK_DIRECT_READ_OPERATION_DURATION, LOCALFILE_DISK_READ_OPERATION_DURATION,
    TOTAL_LOCAL_DISK_APPEND_OPERATION_BYTES_COUNTER, TOTAL_LOCAL_DISK_APPEND_OPERATION_COUNTER,
    TOTAL_LOCAL_DISK_READ_OPERATION_BYTES_COUNTER, TOTAL_LOCAL_DISK_READ_OPERATION_COUNTER,
};
use crate::runtime::manager::RuntimeManager;
use crate::store::local::io_layer_await_tree::AwaitTreeLayer;
use crate::store::local::io_layer_metrics::MetricsLayer;
use crate::store::local::io_layer_read_ahead::ReadAheadLayer;
use crate::store::local::io_layer_retry::{IoLayerRetry, RETRY_MAX_TIMES};
use crate::store::local::io_layer_throttle::{ThrottleLayer, ThroughputBasedRateLimiter};
use crate::store::local::io_layer_timeout::TimeoutLayer;
use crate::store::local::layers::{Handler, OperatorBuilder};
use crate::store::local::options::WriteOptions;
use crate::store::local::read_options::ReadOptions;
use crate::store::local::sync_io::SyncLocalIO;
#[cfg(all(feature = "io-uring", target_os = "linux"))]
use crate::store::local::uring_io::UringIoEngineBuilder;
use crate::store::local::{DiskStat, FileStat, LocalDiskStorage, LocalIO};
use crate::store::DataBytes;
use crate::util;
use anyhow::{Context, Result};
use async_trait::async_trait;
use await_tree::InstrumentAwait;
use bytes::Bytes;
use bytesize::ByteSize;
use clap::error::ErrorKind::Io;
use log::{error, warn};
use once_cell::sync::OnceCell;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{timeout, Instant};
use tracing::{info, Instrument};

#[derive(Clone)]
pub struct LocalDiskDelegator {
    inner: Arc<Inner>,
}

impl Deref for LocalDiskDelegator {
    type Target = Handler;

    fn deref(&self) -> &Self::Target {
        &self.inner.io_handler
    }
}

unsafe impl Send for LocalDiskDelegator {}
unsafe impl Sync for LocalDiskDelegator {}

struct Inner {
    root: String,

    io_handler: Handler,

    is_corrupted: Arc<AtomicBool>,
    is_space_enough: Arc<AtomicBool>,
    is_operation_normal: Arc<AtomicBool>,

    high_watermark: f32,
    low_watermark: f32,

    healthy_check_interval_sec: u64,

    // only for the test case
    capacity_ref: OnceCell<Arc<AtomicU64>>,
    available_ref: OnceCell<Arc<AtomicU64>>,
}

impl LocalDiskDelegator {
    pub fn new(
        runtime_manager: &RuntimeManager,
        root: &str,
        config: &LocalfileStoreConfig,
    ) -> LocalDiskDelegator {
        let high_watermark = config.disk_high_watermark;
        let low_watermark = config.disk_low_watermark;
        let write_capacity = ByteSize::from_str(&config.disk_write_buf_capacity).unwrap();
        let read_capacity = ByteSize::from_str(&config.disk_read_buf_capacity).unwrap();

        let underlying_io_handler = SyncLocalIO::new(
            &runtime_manager.read_runtime,
            &runtime_manager.localfile_write_runtime,
            root,
            Some(write_capacity.as_u64() as usize),
            Some(read_capacity.as_u64() as usize),
        );

        #[cfg(all(feature = "io-uring", target_os = "linux"))]
        info!("Binary compiled with the io-uring feature enabled.");

        #[cfg(all(feature = "io-uring", target_os = "linux"))]
        let mut operator_builder = if let Some(cfg) = &config.io_uring_options {
            info!("io-uring engine is activated!");
            OperatorBuilder::new(Arc::new(Box::new(
                UringIoEngineBuilder::new()
                    .with_threads(cfg.threads)
                    .with_io_depth(cfg.io_depth)
                    .build(underlying_io_handler)
                    .unwrap(),
            )))
        } else {
            OperatorBuilder::new(Arc::new(Box::new(underlying_io_handler)))
        };

        #[cfg(not(feature = "io-uring"))]
        let mut operator_builder = OperatorBuilder::new(Arc::new(Box::new(underlying_io_handler)));

        if let Some(conf) = config.io_limiter.as_ref() {
            operator_builder = operator_builder.layer(ThrottleLayer::new(
                &runtime_manager.localfile_write_runtime,
                &conf,
            ));
            info!(
                "IO layer of throttle is enabled for disk: {}. throughput: {}/s",
                root, conf.capacity
            );
        }
        if let Some(options) = config.read_ahead_options.as_ref() {
            operator_builder =
                operator_builder.layer(ReadAheadLayer::new(root, options, runtime_manager.clone()));
            info!("Read ahead layer is enabled for disk: {}.", root);
        }
        let io_handler = operator_builder
            .layer(TimeoutLayer::new(config.io_duration_threshold_sec))
            .layer(IoLayerRetry::new(RETRY_MAX_TIMES, root))
            .layer(AwaitTreeLayer::new(root))
            .layer(MetricsLayer::new(root))
            .build();

        let delegator = Self {
            inner: Arc::new(Inner {
                root: root.to_owned(),
                io_handler,
                is_corrupted: Arc::new(AtomicBool::new(false)),
                is_space_enough: Arc::new(AtomicBool::new(true)),
                is_operation_normal: Arc::new(AtomicBool::new(true)),
                high_watermark,
                low_watermark,
                healthy_check_interval_sec: config.disk_healthy_check_interval_sec,
                capacity_ref: Default::default(),
                available_ref: Default::default(),
            }),
        };

        // in test env, this disk detection will always make disk unhealthy status
        #[cfg(not(test))]
        {
            info!("Starting the disk:{} checker", root);
            let runtime = runtime_manager.clone().default_runtime.clone();
            let io_delegator = delegator.clone();
            let span = format!("disk[{}] checker", root);
            runtime.spawn_with_await_tree(&span, async move {
                info!("starting the disk[{}] checker", &io_delegator.inner.root);
                if let Err(e) = io_delegator.schedule_check().await {
                    error!(
                        "disk[{}] checker exit. err: {:?}",
                        &io_delegator.inner.root, e
                    )
                }
            });
        }

        delegator
    }

    pub fn with_capacity(&self, capacity_ref: Arc<AtomicU64>) {
        let _ = self.inner.capacity_ref.set(capacity_ref);
    }

    pub fn with_available(&self, available_ref: Arc<AtomicU64>) {
        let _ = self.inner.available_ref.set(available_ref);
    }

    pub fn root(&self) -> String {
        self.inner.root.to_owned()
    }

    async fn schedule_check(&self) -> Result<()> {
        loop {
            if self.is_corrupted()? {
                info!(
                    "No longer detection for [{}] due to disk corruption",
                    &self.inner.root
                );
                return Ok(());
            }

            match self
                .capacity_check()
                .instrument_await("capacity check")
                .await
            {
                Err(e) => {
                    error!(
                        "Errors on checking disk capacity: {}. err: {:#?}",
                        &self.inner.root, e
                    );
                    self.mark_space_insufficient();
                }
                Ok(is_space_enough) => {
                    if is_space_enough {
                        self.mark_space_sufficient();
                    } else {
                        self.mark_space_insufficient();
                    }
                }
            }

            match self
                .write_read_check()
                .instrument_await("write+read checking")
                .await
            {
                Err(error) => match error {
                    WorkerError::FUTURE_EXEC_TIMEOUT(e) => {
                        error!("write+read check:{} timed out: {}", &self.inner.root, e);
                        self.mark_operation_abnormal();
                    }
                    error => {
                        error!(
                            "Errors on checking the disk:{} write+read. err: {:#?}",
                            &self.inner.root, error
                        );
                        self.mark_corrupted()?;
                    }
                },
                Ok(_) => {
                    self.mark_operation_normal();
                }
            }

            tokio::time::sleep(Duration::from_secs(self.inner.healthy_check_interval_sec))
                .instrument_await("sleeping")
                .await;
        }
    }

    fn used_ratio(&self) -> Result<f64> {
        let capacity = self.get_disk_capacity()?;
        let available = self.get_disk_available()?;
        let used = capacity - available;
        let used_ratio = used as f64 / capacity as f64;
        Ok(used_ratio)
    }

    pub fn stat(&self) -> Result<DiskStat> {
        let used_ratio = self.used_ratio()?;
        Ok(DiskStat {
            root: self.root(),
            used_ratio,
        })
    }

    async fn capacity_check(&self) -> Result<bool> {
        let capacity = self.get_disk_capacity()?;
        let available = self.get_disk_available()?;
        let used = capacity - available;

        GAUGE_LOCAL_DISK_CAPACITY
            .with_label_values(&[&self.inner.root])
            .set(capacity as i64);
        GAUGE_LOCAL_DISK_USED
            .with_label_values(&[&self.inner.root])
            .set(used as i64);

        let used_ratio = used as f64 / capacity as f64;
        GAUGE_LOCAL_DISK_USED_RATIO
            .with_label_values(&[&self.inner.root])
            .set(used_ratio);

        let previous_status = self.is_space_enough()?;

        if previous_status && used_ratio > self.inner.high_watermark as f64 {
            warn!("Disk={} has been unhealthy", &self.inner.root);
            return Ok(false);
        }

        if !previous_status && used_ratio < self.inner.low_watermark as f64 {
            warn!("Disk={} has been healthy.", &self.inner.root);
            self.mark_space_sufficient()?;
            return Ok(true);
        }
        Ok(true)
    }

    async fn write_read_check(&self) -> Result<(), WorkerError> {
        // Bound the server_id to ensure unique if having another instance in the same machine
        let detection_file = "corruption_check.file";

        self.delete(&detection_file).await?;

        // 10MB data
        let written_data = Bytes::copy_from_slice(&[0u8; 1024 * 1024 * 10]);
        // slow disk if exceeding 5 seconds
        // todo: add disk speed latency metrics to report to coordinator
        let timer = Instant::now();
        let options =
            WriteOptions::with_append_of_direct_io(DataBytes::Direct(written_data.clone()), 0);
        let f = self.write(&detection_file, options);
        timeout(Duration::from_secs(60), f).await??;
        let write_time = timer.elapsed().as_millis();

        let timer = Instant::now();
        let options = ReadOptions::default().with_read_all();
        let read_data = self.read(&detection_file, options).await?;
        let read_data = read_data.freeze();
        let read_time = timer.elapsed().as_millis();

        info!(
            "[{}] Check duration of write/read: {}/{} (millis)",
            &self.inner.root, write_time, read_time
        );

        if written_data != read_data {
            error!(
                "The local disk has been corrupted. path: {}. length(expected/actual): {:?}/{:?}",
                &self.inner.root,
                &written_data.len(),
                &read_data.len()
            );
            return Err(WorkerError::INTERNAL_ERROR);
        }

        Ok(())
    }

    fn get_disk_capacity(&self) -> Result<u64> {
        if let Some(capacity) = self.inner.capacity_ref.get() {
            return Ok(capacity.load(SeqCst));
        }
        Ok(fs2::total_space(&self.inner.root)?)
    }

    fn get_disk_available(&self) -> Result<u64> {
        if let Some(available) = self.inner.available_ref.get() {
            return Ok(available.load(SeqCst));
        }
        Ok(fs2::available_space(&self.inner.root)?)
    }

    pub fn mark_space_sufficient(&self) -> Result<()> {
        GAUGE_LOCAL_DISK_IS_HEALTHY
            .with_label_values(&[&self.inner.root])
            .set(0);
        self.inner.is_space_enough.store(true, SeqCst);
        Ok(())
    }

    pub fn mark_space_insufficient(&self) -> Result<()> {
        GAUGE_LOCAL_DISK_IS_HEALTHY
            .with_label_values(&[&self.inner.root])
            .set(1);
        self.inner.is_space_enough.store(false, SeqCst);
        Ok(())
    }

    pub fn mark_corrupted(&self) -> Result<()> {
        GAUGE_LOCAL_DISK_IS_CORRUPTED
            .with_label_values(&[&self.inner.root])
            .set(1);
        self.inner.is_corrupted.store(true, SeqCst);
        Ok(())
    }

    pub fn mark_operation_abnormal(&self) -> Result<()> {
        self.inner.is_operation_normal.store(false, SeqCst);
        Ok(())
    }

    pub fn mark_operation_normal(&self) -> Result<()> {
        self.inner.is_operation_normal.store(true, SeqCst);
        Ok(())
    }

    fn is_space_enough(&self) -> Result<bool> {
        Ok(self.inner.is_space_enough.load(SeqCst))
    }
}

impl LocalDiskStorage for LocalDiskDelegator {
    fn is_healthy(&self) -> Result<bool> {
        if self.inner.is_operation_normal.load(SeqCst) && self.inner.is_space_enough.load(SeqCst) {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn is_corrupted(&self) -> Result<bool> {
        Ok(self.inner.is_corrupted.load(SeqCst))
    }
}

#[cfg(test)]
mod test {
    use crate::config::LocalfileStoreConfig;
    use crate::runtime::manager::{create_runtime, RuntimeManager};
    use crate::runtime::Runtime;
    use crate::store::local::delegator::LocalDiskDelegator;
    use crate::store::local::LocalDiskStorage;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    #[ignore]
    fn test_capacity_check() -> anyhow::Result<()> {
        let temp_dir = tempdir::TempDir::new("test_sync_io").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("created the temp file path: {}", &temp_path);

        let mut config = LocalfileStoreConfig::new(vec![temp_path.clone()]);
        config.disk_healthy_check_interval_sec = 2;

        let runtime_manager = RuntimeManager::default();
        let delegator = LocalDiskDelegator::new(&runtime_manager, &temp_path, &config);

        let capacity = Arc::new(AtomicU64::new(100));
        let available = Arc::new(AtomicU64::new(90));

        delegator.with_capacity(capacity.clone());
        delegator.with_available(available.clone());

        // case1
        assert!(delegator.is_healthy()?);

        // case2
        available.store(10, SeqCst);
        awaitility::at_most(Duration::from_secs(5))
            .until(|| delegator.is_healthy().unwrap() == false);

        // case3
        available.store(90, SeqCst);
        awaitility::at_most(Duration::from_secs(5))
            .until(|| delegator.is_healthy().unwrap() == true);

        Ok(())
    }

    #[test]
    fn test_capacity_check_behavior() -> anyhow::Result<()> {
        use crate::config::LocalfileStoreConfig;
        use crate::runtime::manager::RuntimeManager;
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;

        let temp_dir = tempdir::TempDir::new("test_capacity_check_behavior").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();

        let mut config = LocalfileStoreConfig::new(vec![temp_path.clone()]);
        config.disk_high_watermark = 0.8;
        config.disk_low_watermark = 0.5;

        let runtime = create_runtime(1, "");
        let runtime_manager = RuntimeManager::default();
        let delegator = LocalDiskDelegator::new(&runtime_manager, &temp_path, &config);

        let capacity = Arc::new(AtomicU64::new(100));
        let available = Arc::new(AtomicU64::new(30)); // 70 used, 70/100 = 0.7

        delegator.with_capacity(capacity.clone());
        delegator.with_available(available.clone());

        assert_eq!(runtime.block_on(delegator.capacity_check())?, true);

        available.store(15, Ordering::SeqCst); // 85 used, 85/100 = 0.85
        assert_eq!(runtime.block_on(delegator.capacity_check())?, false);

        available.store(60, Ordering::SeqCst); // 40 used, 40/100 = 0.4
        assert_eq!(runtime.block_on(delegator.capacity_check())?, true);

        Ok(())
    }

    #[test]
    fn test_write_read_check_behavior() -> anyhow::Result<()> {
        use crate::config::LocalfileStoreConfig;
        use crate::runtime::manager::RuntimeManager;
        use std::str::FromStr;

        let temp_dir = tempdir::TempDir::new("test_write_read_check").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();

        let mut config = LocalfileStoreConfig::new(vec![temp_path.clone()]);
        config.disk_high_watermark = 0.8;
        config.disk_low_watermark = 0.5;

        let runtime = create_runtime(1, "");
        let runtime_manager = RuntimeManager::default();
        let delegator = LocalDiskDelegator::new(&runtime_manager, &temp_path, &config);

        runtime.block_on(delegator.write_read_check())?;

        Ok(())
    }

    #[test]
    fn test_schedule_check_behavior() -> anyhow::Result<()> {
        use crate::config::LocalfileStoreConfig;
        use crate::runtime::manager::RuntimeManager;
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;
        use std::time::Duration;

        let temp_dir = tempdir::TempDir::new("test_schedule_check_behavior").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();

        let mut config = LocalfileStoreConfig::new(vec![temp_path.clone()]);
        config.disk_high_watermark = 0.8;
        config.disk_low_watermark = 0.5;
        config.disk_healthy_check_interval_sec = 1;

        let runtime = create_runtime(1, "");
        let runtime_manager = RuntimeManager::default();
        let delegator = LocalDiskDelegator::new(&runtime_manager, &temp_path, &config);

        let capacity = Arc::new(AtomicU64::new(100));
        let available = Arc::new(AtomicU64::new(90));
        delegator.with_capacity(capacity.clone());
        delegator.with_available(available.clone());

        let delegator_clone = delegator.clone();
        let handle = runtime.spawn(async move {
            let mut count = 0;
            loop {
                if count >= 3 {
                    break;
                }
                delegator_clone.schedule_check().await.ok();
                count += 1;
            }
        });

        awaitility::at_most(Duration::from_secs(3))
            .until(|| delegator.is_healthy().unwrap() == true);

        available.store(10, Ordering::SeqCst);
        awaitility::at_most(Duration::from_secs(3))
            .until(|| delegator.is_healthy().unwrap() == false);

        available.store(90, Ordering::SeqCst);
        awaitility::at_most(Duration::from_secs(3))
            .until(|| delegator.is_healthy().unwrap() == true);

        delegator.mark_operation_abnormal();
        assert!(!delegator.is_healthy().unwrap());

        delegator.mark_operation_normal();
        assert!(delegator.is_healthy().unwrap());

        drop(handle);

        Ok(())
    }
}
