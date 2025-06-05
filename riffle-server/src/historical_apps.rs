use crate::app_manager::App;
use crate::config::HistoricalAppStoreBackend;
use crate::runtime::manager::RuntimeManager;
use crate::util::{now_timestamp_as_millis, now_timestamp_as_sec};
use crate::{config, util};
use anyhow::Result;
use async_trait::async_trait;
use await_tree::InstrumentAwait;
use clap::builder::Str;
use dashmap::DashMap;
use log::{error, info};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::cmp::{max, min};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct HistoricalAppManager {
    app_store: Arc<Box<dyn HistoricalAppStore>>,
    retention_days: usize,
}

unsafe impl Sync for HistoricalAppManager {}
unsafe impl Send for HistoricalAppManager {}

#[derive(Serialize, Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct HistoricalAppInfo {
    app_id: String,

    // about bytes
    pub total_bytes: u64,

    // about partitions
    pub partition_num: u64,
    pub huge_partition_num: u64,

    pub avg_huge_partition_bytes: u64,
    pub max_huge_partition_bytes: u64,
    pub min_huge_partition_bytes: u64,

    pub start_timestamp: u128,
    pub end_timestamp: u128,
}

impl HistoricalAppManager {
    pub fn new(
        rtm: &RuntimeManager,
        conf: &config::HistoricalAppStoreConfig,
    ) -> HistoricalAppManager {
        let backend: Box<dyn HistoricalAppStore> = match conf.backend {
            HistoricalAppStoreBackend::MEM => Box::new(MemoryHistoricalAppStore {
                apps: Default::default(),
            }),
            HistoricalAppStoreBackend::SLED => {
                let path = conf.db_path.as_ref().unwrap();
                Box::new(SledHistoricalAppStore::new(path))
            }
        };
        let manager = HistoricalAppManager {
            app_store: Arc::new(backend),
            retention_days: conf.retention_days,
        };

        let app_manager = manager.clone();
        rtm.default_runtime
            .spawn_with_await_tree("Historical app statistics", async move {
                // 30 minutes
                let interval = 60 * 30;
                loop {
                    tokio::time::sleep(Duration::from_secs(interval))
                        .instrument_await(format!("sleeping for {} sec...", interval))
                        .await;
                    if let Err(err) = app_manager.purge().await {
                        error!("Errors on purging historical app. err: {}", err);
                    }
                }
            });

        manager
    }

    async fn purge(self: &Self) -> Result<()> {
        info!("Purging expired historical apps...");
        let timer = Instant::now();
        let now = util::now_timestamp_as_sec() as u128;
        let mut purge_candidates = vec![];
        for app in self.app_store.load().await? {
            if (now - app.end_timestamp) > (self.retention_days * 24 * 60 * 60) as u128 {
                purge_candidates.push(app.app_id.to_owned());
            }
        }
        for purge_candidate in purge_candidates.iter() {
            self.app_store.remove(purge_candidate).await?;
        }
        info!(
            "Purged {} historical apps in {}(ms).",
            purge_candidates.len(),
            timer.elapsed().as_millis()
        );
        Ok(())
    }

    pub async fn load(&self) -> Result<Vec<HistoricalAppInfo>> {
        self.app_store.load().await
    }

    pub async fn save(&self, app: &App) -> Result<()> {
        let start = Instant::now();

        let app_id = app.app_id.to_owned();
        let partition_num = app.partition_number() as u64;
        let huge_partition_num = app.huge_partition_number();

        let huge_partition_size_list = app.dump_all_huge_partitions_size().await?;
        let mut max_size = 0u64;
        let mut min_size = u64::MAX;
        let mut total = 0u64;
        for huge_partition_size in &huge_partition_size_list {
            let huge_partition_size = *huge_partition_size;
            if (huge_partition_size > max_size) {
                max_size = huge_partition_size;
            }
            if (huge_partition_size < min_size) {
                min_size = huge_partition_size;
            }
            total += huge_partition_size;
        }
        let avg = if huge_partition_size_list.is_empty() {
            0
        } else {
            total / huge_partition_size_list.len() as u64
        };

        let historical_app = HistoricalAppInfo {
            app_id: app_id.to_owned(),
            total_bytes: app.total_received_data_size(),
            partition_num,
            huge_partition_num,
            avg_huge_partition_bytes: avg,
            max_huge_partition_bytes: max_size,
            min_huge_partition_bytes: min_size,
            start_timestamp: app.start_timestamp,
            end_timestamp: now_timestamp_as_millis(),
        };
        info!(
            "Saved historical app: {} cost {}(ms)",
            historical_app.app_id.as_str(),
            start.elapsed().as_millis()
        );
        self.app_store.save(historical_app).await?;
        Ok(())
    }
}

#[async_trait]
trait HistoricalAppStore {
    async fn save(&self, app: HistoricalAppInfo) -> Result<()>;

    async fn load(&self) -> Result<Vec<HistoricalAppInfo>>;
    async fn remove(&self, app_id: &str) -> Result<()>;
}

struct MemoryHistoricalAppStore {
    apps: DashMap<String, HistoricalAppInfo>,
}

#[async_trait]
impl HistoricalAppStore for MemoryHistoricalAppStore {
    async fn save(&self, app: HistoricalAppInfo) -> Result<()> {
        self.apps.insert(app.app_id.to_string(), app);
        Ok(())
    }

    async fn load(&self) -> Result<Vec<HistoricalAppInfo>> {
        let apps = self.apps.clone().into_read_only();
        let apps = apps.values().map(|x| x.clone()).collect();
        Ok(apps)
    }

    async fn remove(&self, app_id: &str) -> Result<()> {
        self.apps.remove(app_id);
        Ok(())
    }
}

struct SledHistoricalAppStore {
    db: sled::Db,
}

impl SledHistoricalAppStore {
    pub fn new(path: &str) -> Self {
        // fast fail
        let db = sled::open(path).unwrap();
        Self { db }
    }
}

#[async_trait]
impl HistoricalAppStore for SledHistoricalAppStore {
    async fn save(&self, app: HistoricalAppInfo) -> Result<()> {
        let key = app.app_id.clone();
        let value = serde_json::to_vec(&app)?;
        self.db.insert(key, value)?;
        Ok(())
    }

    async fn load(&self) -> Result<Vec<HistoricalAppInfo>> {
        let mut apps = Vec::new();
        for item in self.db.iter() {
            let (_, value) = item?;
            let app: HistoricalAppInfo = serde_json::from_slice(&value)?;
            apps.push(app);
        }
        Ok(apps)
    }

    async fn remove(&self, app_id: &str) -> Result<()> {
        self.db.remove(app_id)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::historical_apps::{
        HistoricalAppInfo, HistoricalAppStore, MemoryHistoricalAppStore, SledHistoricalAppStore,
    };
    use anyhow::Result;

    async fn check_store(store: Box<dyn HistoricalAppStore>) -> Result<()> {
        let app = HistoricalAppInfo {
            app_id: "test_app".to_string(),
            total_bytes: 1024,
            partition_num: 10,
            huge_partition_num: 2,
            avg_huge_partition_bytes: 512,
            max_huge_partition_bytes: 1024,
            min_huge_partition_bytes: 256,
            start_timestamp: 1000,
            end_timestamp: 2000,
        };

        // Save
        store.save(app.clone()).await?;
        let loaded_apps = store.load().await?;
        assert_eq!(loaded_apps.len(), 1);
        assert_eq!(loaded_apps[0], app);

        // Remove
        store.remove(&app.app_id).await?;
        let loaded_apps = store.load().await?;
        assert!(loaded_apps.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_sled_store() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("test_sled_store")?;
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("created the temp file path: {}", &temp_path);
        let store = SledHistoricalAppStore::new(&temp_path);
        check_store(Box::new(store)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_mem_store() -> Result<()> {
        let store = MemoryHistoricalAppStore {
            apps: Default::default(),
        };
        check_store(Box::new(store)).await?;
        Ok(())
    }
}
