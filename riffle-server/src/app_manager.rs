// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

pub mod app;
pub mod app_configs;
pub mod application_identifier;
pub mod partition_identifier;
mod partition_meta;
pub mod purge_event;
pub mod request_context;

use crate::config::{Config, HistoricalAppStoreConfig, StorageType};
use crate::error::WorkerError;
use crate::metric::{
    BLOCK_ID_NUMBER, GAUGE_APP_NUMBER, GAUGE_HUGE_PARTITION_NUMBER, GAUGE_PARTITION_NUMBER,
    PURGE_FAILED_COUNTER, RESIDENT_BYTES, TOTAL_APP_NUMBER, TOTAL_HUGE_PARTITION_NUMBER,
    TOTAL_HUGE_PARTITION_REQUIRE_BUFFER_FAILED, TOTAL_PARTITION_NUMBER, TOTAL_READ_DATA,
    TOTAL_READ_DATA_FROM_LOCALFILE, TOTAL_READ_DATA_FROM_MEMORY, TOTAL_READ_INDEX_FROM_LOCALFILE,
    TOTAL_RECEIVED_DATA, TOTAL_REQUIRE_BUFFER_FAILED,
};

use crate::readable_size::ReadableSize;
use crate::runtime::manager::RuntimeManager;
use crate::store::hybrid::HybridStore;
use crate::store::{Block, RequireBufferResponse, ResponseData, ResponseDataIndex, Store};
use crate::util::{now_timestamp_as_millis, now_timestamp_as_sec};
use anyhow::{anyhow, Result};
use bytes::Bytes;
use croaring::{JvmLegacy, Treemap};

use dashmap::DashMap;
use log::{debug, error, info, warn};

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap, HashSet};

use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::ops::Deref;
use std::str::FromStr;

use crate::app_manager::app::App;
use crate::app_manager::app_configs::AppConfigOptions;
use crate::app_manager::application_identifier::ApplicationId;
use crate::app_manager::purge_event::{PurgeEvent, PurgeReason};
use crate::await_tree::AWAIT_TREE_REGISTRY;
use crate::block_id_manager::{get_block_id_manager, BlockIdManager};
use crate::config_reconfigure::ReconfigurableConfManager;
use crate::config_ref::{ByteString, ConfRef, ConfigOption};
use crate::constant::ALL_LABEL;
use crate::dashmap_extension::DashMapExtend;
use crate::grpc::protobuf::uniffle::{BlockIdLayout, RemoteStorage};
use crate::historical_apps::HistoricalAppManager;
use crate::id_layout::IdLayout;
use crate::storage::HybridStorage;
use crate::store::local::LocalfileStoreStat;
use crate::store::mem::capacity::CapacitySnapshot;
use crate::util;
use await_tree::InstrumentAwait;
use crossbeam::epoch::Atomic;
use fxhash::{FxBuildHasher, FxHasher};
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use prometheus::core::Collector;
use prometheus::proto::MetricType::GAUGE;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tracing::Instrument;

pub static SHUFFLE_SERVER_ID: OnceLock<String> = OnceLock::new();
pub static SHUFFLE_SERVER_IP: OnceLock<String> = OnceLock::new();

pub static APP_MANAGER_REF: OnceCell<AppManagerRef> = OnceCell::new();

pub type AppManagerRef = Arc<AppManager>;

pub struct AppManager {
    // key: app_id
    pub(crate) apps: DashMapExtend<ApplicationId, Arc<App>, BuildHasherDefault<FxHasher>>,
    receiver: async_channel::Receiver<PurgeEvent>,
    sender: async_channel::Sender<PurgeEvent>,
    store: Arc<HybridStore>,
    app_heartbeat_timeout_min: u32,
    config: Config,
    runtime_manager: RuntimeManager,
    historical_app_manager: Option<HistoricalAppManager>,
    reconf_manager: ReconfigurableConfManager,
}

impl AppManager {
    fn new(
        runtime_manager: RuntimeManager,
        config: Config,
        storage: &HybridStorage,
        reconf_manager: &ReconfigurableConfManager,
    ) -> Self {
        let (sender, receiver) = async_channel::unbounded();
        let app_heartbeat_timeout_min = config.app_config.app_heartbeat_timeout_min;

        let historical_app_manager: Option<HistoricalAppManager> =
            match &config.historical_apps_config {
                None => None,
                Some(conf) => Some(HistoricalAppManager::new(&runtime_manager, &conf)),
            };

        let manager = AppManager {
            apps: DashMapExtend::<ApplicationId, Arc<App>, BuildHasherDefault<FxHasher>>::new(),
            receiver,
            sender,
            store: storage.clone(),
            app_heartbeat_timeout_min,
            config,
            runtime_manager: runtime_manager.clone(),
            historical_app_manager,
            reconf_manager: reconf_manager.clone(),
        };
        manager
    }
}

impl AppManager {
    pub fn get_ref(
        runtime_manager: RuntimeManager,
        config: Config,
        storage: &HybridStorage,
        reconf_manager: &ReconfigurableConfManager,
    ) -> AppManagerRef {
        let app_ref = Arc::new(AppManager::new(
            runtime_manager.clone(),
            config,
            storage,
            reconf_manager,
        ));
        let app_manager_ref_cloned = app_ref.clone();

        runtime_manager.default_runtime.spawn_with_await_tree("App heartbeat checker", async move {
                info!("Starting app heartbeat checker...");
                loop {
                    // task1: find out heartbeat timeout apps
                    tokio::time::sleep(Duration::from_secs(10))
                        .instrument_await("sleeping for 10s...")
                        .await;

                    for item in app_manager_ref_cloned.apps.iter() {
                        let (key, app) = item.pair();
                        let last_time = app.get_latest_heartbeat_time();
                        let current = now_timestamp_as_sec();

                        if current - last_time
                            > (app_manager_ref_cloned.app_heartbeat_timeout_min * 60) as u64
                        {
                            info!("Detected app:{:?} heartbeat timeout. now: {:?}, latest heartbeat: {:?}. timeout threshold: {:?}(min)",
                            key, current, last_time, app_manager_ref_cloned.app_heartbeat_timeout_min);
                            if app_manager_ref_cloned
                                .sender
                                .send(PurgeEvent {
                                    reason: PurgeReason::APP_LEVEL_HEARTBEAT_TIMEOUT(key.clone()),
                                })
                                .await
                                .is_err()
                            {
                                error!(
                                "Errors on sending purge event when app: {} heartbeat timeout",
                                key
                            );
                            }
                        }
                    }
                }
        });

        let app_manager_cloned = app_ref.clone();
        runtime_manager
            .default_runtime
            .spawn_with_await_tree("App purger", async move {
                info!("Starting purge event handler...");
                while let Ok(event) = app_manager_cloned
                    .receiver
                    .recv()
                    .instrument_await("waiting events coming...")
                    .await
                {
                    let reason = event.reason;
                    info!("Purging data with reason: {}", &reason);
                    if let Err(err) = app_manager_cloned.purge_app_data(&reason).await {
                        PURGE_FAILED_COUNTER.inc();
                        error!(
                            "Errors on purging data with reason: {}. err: {:?}",
                            &reason, err
                        );
                    }
                }
            });

        app_ref
    }

    pub fn get_historical_app_manager(&self) -> Option<&HistoricalAppManager> {
        self.historical_app_manager.as_ref()
    }

    pub fn app_is_exist(&self, app_id: &ApplicationId) -> bool {
        self.apps.contains_key(app_id)
    }

    pub async fn store_is_healthy(&self) -> Result<bool> {
        self.store.is_healthy().await
    }

    pub async fn store_memory_snapshot(&self) -> Result<CapacitySnapshot> {
        self.store.mem_snapshot()
    }

    pub fn store_localfile_stat(&self) -> Result<LocalfileStoreStat> {
        self.store.localfile_stat()
    }

    pub fn store_memory_spill_event_num(&self) -> Result<u64> {
        self.store.get_spill_event_num()
    }

    async fn purge_app_data(&self, reason: &PurgeReason) -> Result<()> {
        let (app_id, shuffle_id_option) = reason.extract();
        let app = self.get_app(&app_id).ok_or(anyhow!(format!(
            "App:{} don't exist when purging data, this should not happen",
            &app_id
        )))?;
        if shuffle_id_option.is_none() {
            self.apps.remove(&app_id);
            GAUGE_APP_NUMBER.dec();

            // record into the historical app list
            if let Some(historical_manager) = self.historical_app_manager.as_ref() {
                info!(
                    "Saving timeout app into the historical list.. app_id: {}",
                    app_id.to_string()
                );
                historical_manager
                    .save(&app)
                    .instrument_await("Saving to historical app list...")
                    .await?;
            }
        }
        app.purge(reason).await?;
        Ok(())
    }

    pub fn get_app(&self, app_id: &ApplicationId) -> Option<Arc<App>> {
        self.apps.get(app_id).map(|v| v.value().clone())
    }

    pub fn get_alive_app_number(&self) -> usize {
        self.apps.len()
    }

    pub fn register(
        &self,
        app_id: String,
        shuffle_id: i32,
        app_config_options: AppConfigOptions,
    ) -> Result<()> {
        info!(
            "Accepting registry. app_id: {}, shuffle_id: {}",
            app_id.clone(),
            shuffle_id
        );
        let application_id = ApplicationId::from(app_id.as_str());
        let app_ref = self.apps.compute_if_absent(application_id, || {
            TOTAL_APP_NUMBER.inc();
            GAUGE_APP_NUMBER.inc();

            Arc::new(App::from(
                app_id,
                app_config_options,
                self.store.clone(),
                self.runtime_manager.clone(),
                &self.config,
                &self.reconf_manager,
            ))
        });
        app_ref.register_shuffle(shuffle_id)
    }

    pub async fn unregister_shuffle(&self, app_id: String, shuffle_id: i32) -> Result<()> {
        let application_id = ApplicationId::from(app_id.as_str());
        self.sender
            .send(PurgeEvent {
                reason: PurgeReason::SHUFFLE_LEVEL_EXPLICIT_UNREGISTER(application_id, shuffle_id),
            })
            .await?;
        Ok(())
    }

    pub async fn unregister_app(&self, app_id: String) -> Result<()> {
        let application_id = ApplicationId::from(app_id.as_str());
        self.sender
            .send(PurgeEvent {
                reason: PurgeReason::APP_LEVEL_EXPLICIT_UNREGISTER(application_id),
            })
            .await?;
        Ok(())
    }

    pub fn runtime_manager(&self) -> RuntimeManager {
        self.runtime_manager.clone()
    }

    pub fn get_config(&self) -> &Config {
        &self.config
    }
}

#[cfg(test)]
pub(crate) mod test {
    use crate::app_manager::app::App;
    use crate::app_manager::app_configs::{AppConfigOptions, DataDistribution};
    use crate::app_manager::application_identifier::ApplicationId;
    use crate::app_manager::partition_identifier::PartitionUId;
    use crate::app_manager::request_context::{
        GetMultiBlockIdsContext, ReadingOptions, ReadingViewContext, ReportMultiBlockIdsContext,
        RequireBufferContext, RpcType, WritingViewContext,
    };
    use crate::app_manager::{AppManager, PurgeReason};
    use crate::config::{
        Config, HybridStoreConfig, LocalfileStoreConfig, MemoryStoreConfig, ReadAheadConfig,
    };
    use crate::config_reconfigure::ReconfigurableConfManager;
    use crate::error::WorkerError;
    use crate::id_layout::{to_layout, IdLayout, DEFAULT_BLOCK_ID_LAYOUT};
    use crate::runtime::manager::RuntimeManager;
    use crate::storage::StorageService;
    use crate::store::{Block, ResponseData};
    use bytes::Bytes;
    use crc32fast::hash;
    use croaring::{JvmLegacy, Treemap};
    use dashmap::DashMap;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn test_uid_hash() {
        let app_id = ApplicationId::mock();
        let uid = PartitionUId::new(&app_id, 1, 1);
        let hash_value = PartitionUId::get_hash(&uid);
        println!("{}", hash_value);
    }

    pub fn mock_config() -> Config {
        let temp_dir = tempdir::TempDir::new("test_local_store").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("init local file path: {}", temp_path);

        let mut config = Config::default();
        config.memory_store = Some(MemoryStoreConfig::new((1024 * 1024).to_string()));
        config.localfile_store = Some(LocalfileStoreConfig::new(vec![temp_path]));
        config.hybrid_store = HybridStoreConfig::default();
        config
    }

    pub fn mock_writing_context(
        app_id: &ApplicationId,
        shuffle_id: i32,
        partition_id: i32,
        block_batch: i32,
        block_len: i32,
    ) -> WritingViewContext {
        let mut blocks = vec![];
        for idx in 0..block_batch {
            let block = Block {
                block_id: idx as i64,
                length: block_len,
                uncompress_length: 0,
                crc: 0,
                data: Bytes::copy_from_slice(&vec![0; block_len as usize]),
                task_attempt_id: 0,
            };
            blocks.push(block);
        }
        let writing_ctx = WritingViewContext::new_with_size(
            PartitionUId::new(&app_id, shuffle_id, partition_id),
            blocks,
            (block_len * block_batch) as u64,
        );
        writing_ctx
    }

    #[test]
    fn app_read_ahead_mechanism_test() {
        let app_id = ApplicationId::mock();
        let raw_app_id = app_id.to_string();
        let runtime_manager: RuntimeManager = Default::default();
        let mut config = create_config_for_partition_features();
        let reconf_manager = ReconfigurableConfManager::new(&config, None).unwrap();

        // activate read ahead layer for localfile store
        config.localfile_store.as_mut().unwrap().read_ahead_options =
            Some(ReadAheadConfig::default());

        let storage = StorageService::init(&runtime_manager, &config, &reconf_manager);
        let app_manager_ref =
            AppManager::get_ref(runtime_manager.clone(), config, &storage, &reconf_manager).clone();

        let shuffle_id = 1;
        let partition_id = 1;
        let app_options = AppConfigOptions {
            data_distribution: DataDistribution::NORMAL,
            max_concurrency_per_partition_to_write: 0,
            remote_storage_config_option: None,
            sendfile_enable: false,
            // activate read ahead
            read_ahead_enable: true,
            read_ahead_batch_number: None,
            read_ahead_batch_size: None,
            client_configs: Default::default(),
        };
        app_manager_ref
            .register(raw_app_id.to_string(), shuffle_id, app_options)
            .unwrap();

        let app = app_manager_ref.get_app(&app_id).unwrap();
        let ctx = mock_writing_context(&app_id, shuffle_id, partition_id, 2, 3);
        let f = app.insert(ctx);
        if runtime_manager.wait(f).is_err() {
            panic!()
        }

        // case1: read ahead is enabled when registering
        let uid = PartitionUId::new(&app_id, shuffle_id, partition_id);
        let context = ReadingViewContext::new(
            uid.clone(),
            ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(-1, 1000),
            RpcType::GRPC,
        );

        let f = app.select(context);
        let result = runtime_manager.wait(f).unwrap();

        let meta = app.get_partition_meta(&uid);
        assert!(meta.is_sequential_read());
    }

    #[test]
    fn app_partition_split_test() {
        let app_id = ApplicationId::mock();
        let raw_app_id = app_id.to_string();
        let runtime_manager: RuntimeManager = Default::default();

        let mut config = create_config_for_partition_features();
        let mut app_config = &mut config.app_config;
        app_config.partition_split_enable = true;
        app_config.partition_split_threshold = "5B".to_string();

        let reconf_manager = ReconfigurableConfManager::new(&config, None).unwrap();
        let storage = StorageService::init(&runtime_manager, &config, &reconf_manager);
        let app_manager_ref =
            AppManager::get_ref(runtime_manager.clone(), config, &storage, &reconf_manager).clone();
        app_manager_ref
            .register(raw_app_id.to_string(), 1, Default::default())
            .unwrap();

        // case1: exceeding the partition split, but not exceeding the partition limit.
        let app = app_manager_ref.get_app(&app_id).unwrap();
        let ctx = mock_writing_context(&app_id, 1, 0, 2, 3);
        let f = app.insert(ctx);
        if runtime_manager.wait(f).is_err() {
            panic!()
        }

        let require_buffer_ctx = RequireBufferContext {
            uid: PartitionUId::new(&app_id, 1, 0),
            size: 10,
            partition_ids: vec![0],
        };
        let f = app.require_buffer(require_buffer_ctx.clone());
        match runtime_manager.wait(f) {
            Err(_) => panic!(),
            Ok(response) => {
                let split_list = response.split_partitions;
                assert_eq!(1, split_list.len());
                assert_eq!(0, *split_list.get(0).unwrap());
            }
        }

        // case2: exceeding the partition split and also exceeding the partition limit.
        // this happens when the client is not responded to.
        // now the partition=0 data len = 6 * 2 = 12
        let ctx = mock_writing_context(&app_id, 1, 0, 2, 3);
        let f = app.insert(ctx.clone());
        if runtime_manager.wait(f).is_err() {
            panic!()
        }
        let f = app.require_buffer(require_buffer_ctx);
        match runtime_manager.wait(f) {
            Err(WorkerError::MEMORY_USAGE_LIMITED_BY_HUGE_PARTITION) => {
                // ignore
            }
            _ => panic!(),
        }
    }

    #[test]
    #[should_panic]
    fn test_partition_related_config_value_valid() {
        let app_id = "test_partition_related_config_value_valid";
        let runtime_manager: RuntimeManager = Default::default();

        // the default value of partition-limit is 10B
        let mut config = create_config_for_partition_features();
        let mut app_config = &mut config.app_config;
        app_config.partition_split_enable = true;
        app_config.partition_split_threshold = "20B".to_string();

        let reconf_manager = ReconfigurableConfManager::new(&config, None).unwrap();
        let storage = StorageService::init(&runtime_manager, &config, &reconf_manager);
        let app_manager_ref =
            AppManager::get_ref(runtime_manager.clone(), config, &storage, &reconf_manager).clone();
        app_manager_ref
            .register(app_id.clone().into(), 1, Default::default())
            .unwrap();
    }

    fn create_config_for_partition_features() -> Config {
        let mut config = mock_config();
        let _ = std::mem::replace(
            &mut config.memory_store,
            Some(MemoryStoreConfig {
                capacity: "20B".to_string(),
                buffer_ticket_timeout_sec: 1,
                buffer_ticket_check_interval_sec: 1,
            }),
        );
        let _ = std::mem::replace(
            &mut config.hybrid_store,
            HybridStoreConfig {
                memory_spill_high_watermark: 1.0,
                memory_spill_low_watermark: 0.0,
                memory_single_buffer_max_spill_size: None,
                memory_spill_to_cold_threshold_size: None,
                memory_spill_to_localfile_concurrency: None,
                memory_spill_to_hdfs_concurrency: None,
                huge_partition_memory_spill_to_hdfs_threshold_size: Some("64M".to_string()),
                huge_partition_fallback_enable: false,
                sensitive_watermark_spill_enable: false,
                async_watermark_spill_trigger_enable: false,
                async_watermark_spill_trigger_interval_ms: 0,
            },
        );
        let mut app_config = &mut config.app_config;
        app_config.partition_limit_enable = true;
        app_config.partition_limit_threshold = "10B".to_string();
        app_config.partition_limit_memory_backpressure_ratio = 0.4;

        config
    }

    #[test]
    fn app_backpressure_of_huge_partition() {
        let app_id = ApplicationId::mock();
        let raw_app_id = app_id.to_string();
        let runtime_manager: RuntimeManager = Default::default();

        let config = create_config_for_partition_features();

        let reconf_manager = ReconfigurableConfManager::new(&config, None).unwrap();
        let storage = StorageService::init(&runtime_manager, &config, &reconf_manager);
        let app_manager_ref =
            AppManager::get_ref(runtime_manager.clone(), config, &storage, &reconf_manager).clone();
        app_manager_ref
            .register(raw_app_id.to_string(), 1, Default::default())
            .unwrap();

        let app = app_manager_ref.get_app(&app_id).unwrap();
        let ctx = mock_writing_context(&app_id, 1, 0, 2, 10);
        let f = app.insert(ctx);
        if runtime_manager.wait(f).is_err() {
            panic!()
        }

        let ctx = RequireBufferContext {
            uid: PartitionUId::new(&app_id, 1, 0),
            size: 10,
            partition_ids: vec![0],
        };
        let f = app.require_buffer(ctx);
        match runtime_manager.wait(f) {
            Err(WorkerError::MEMORY_USAGE_LIMITED_BY_HUGE_PARTITION) => {}
            _ => panic!(),
        }
    }

    #[test]
    fn app_put_get_purge_test() {
        let app_id = ApplicationId::mock();
        let raw_app_id = app_id.to_string();

        let runtime_manager: RuntimeManager = Default::default();
        let config = mock_config();
        let reconf_manager = ReconfigurableConfManager::new(&config, None).unwrap();
        let storage = StorageService::init(&runtime_manager, &config, &reconf_manager);
        let app_manager_ref =
            AppManager::get_ref(runtime_manager.clone(), config, &storage, &reconf_manager).clone();
        app_manager_ref
            .register(raw_app_id.to_string(), 1, Default::default())
            .unwrap();

        if let Some(app) = app_manager_ref.get_app(&app_id) {
            let writing_ctx = mock_writing_context(&app_id, 1, 0, 2, 20);

            // case1: put
            let f = app.insert(writing_ctx);
            if runtime_manager.wait(f).is_err() {
                panic!()
            }

            let reading_ctx = ReadingViewContext::new(
                PartitionUId::new(&app_id, 1, 0),
                ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(-1, 1000000),
                RpcType::GRPC,
            );

            // case2: get
            let f = app.select(reading_ctx);
            let result = runtime_manager.wait(f);
            if result.is_err() {
                panic!("{}", result.err().unwrap());
            }

            match result.unwrap() {
                ResponseData::Mem(data) => {
                    assert_eq!(2, data.shuffle_data_block_segments.len());
                }
                _ => todo!(),
            }

            // check the data size
            assert_eq!(40, app.total_received_data_size());
            assert_eq!(40, app.total_resident_data_size());

            // case3: purge
            runtime_manager
                .wait(
                    app_manager_ref.purge_app_data(&PurgeReason::APP_LEVEL_HEARTBEAT_TIMEOUT(
                        app_id.to_owned(),
                    )),
                )
                .expect("");

            assert!(app_manager_ref.get_app(&app_id).is_none());

            // check the data size again after the data has been removed
            assert_eq!(40, app.total_received_data_size());
            assert_eq!(0, app.total_resident_data_size());
        }
    }

    #[test]
    fn app_manager_test() {
        let config = mock_config();
        let runtime_manager: RuntimeManager = Default::default();
        let reconf_manager = ReconfigurableConfManager::new(&config, None).unwrap();
        let storage = StorageService::init(&runtime_manager, &config, &reconf_manager);
        let app_manager_ref =
            AppManager::get_ref(Default::default(), config, &storage, &reconf_manager).clone();

        let app_id = ApplicationId::mock();
        app_manager_ref
            .register(app_id.to_string(), 1, Default::default())
            .unwrap();
        if let Some(app) = app_manager_ref.get_app(&app_id) {
            // ignore
        } else {
            panic!()
        }
    }

    #[test]
    fn test_get_or_put_block_ids() {
        let app_id = ApplicationId::mock();
        let raw_app_id = app_id.to_string();

        let runtime_manager: RuntimeManager = Default::default();
        let config = mock_config();
        let reconf_manager = ReconfigurableConfManager::new(&config, None).unwrap();
        let storage = StorageService::init(&runtime_manager, &config, &reconf_manager);
        let app_manager_ref =
            AppManager::get_ref(runtime_manager.clone(), config, &storage, &reconf_manager).clone();
        app_manager_ref
            .register(raw_app_id.to_string(), 1, Default::default())
            .unwrap();

        let app = app_manager_ref.get_app(&app_id).unwrap();
        let block_id_1 = DEFAULT_BLOCK_ID_LAYOUT.get_block_id(1, 10, 2);
        let block_id_2 = DEFAULT_BLOCK_ID_LAYOUT.get_block_id(2, 10, 3);
        let block_id_3 = DEFAULT_BLOCK_ID_LAYOUT.get_block_id(2, 20, 3);
        runtime_manager
            .wait(app.report_multi_block_ids(ReportMultiBlockIdsContext {
                shuffle_id: 1,
                block_ids: {
                    let mut hashmap = HashMap::new();
                    hashmap.insert(10, vec![block_id_1.clone(), block_id_2.clone()]);
                    hashmap.insert(20, vec![block_id_3.clone()]);
                    hashmap
                },
            }))
            .expect("TODO: panic message");

        // case1: fetch partition=10
        let data = runtime_manager
            .wait(app.get_multi_block_ids(GetMultiBlockIdsContext {
                shuffle_id: 1,
                partition_ids: vec![10],
                layout: to_layout(None),
            }))
            .expect("");
        let deserialized = Treemap::deserialize::<JvmLegacy>(&data);
        assert_eq!(
            deserialized,
            Treemap::from_iter(vec![block_id_1 as u64, block_id_2 as u64])
        );

        // case2: fetch partition=20
        let data = runtime_manager
            .wait(app.get_multi_block_ids(GetMultiBlockIdsContext {
                shuffle_id: 1,
                partition_ids: vec![20],
                layout: to_layout(None),
            }))
            .expect("");
        let deserialized = Treemap::deserialize::<JvmLegacy>(&data);
        assert_eq!(deserialized, Treemap::from_iter(vec![block_id_3 as u64]));
    }

    #[test]
    fn test_dashmap_values() {
        let dashmap = DashMap::new();
        dashmap.insert(1, 3);
        dashmap.insert(2, 2);
        dashmap.insert(3, 8);

        let cloned = dashmap.clone().into_read_only();
        let mut vals: Vec<_> = cloned.values().collect();
        vals.sort_by_key(|x| -(*x));
        assert_eq!(vec![&8, &3, &2], vals);

        let apps = vec![0, 1, 2, 3];
        println!("{:#?}", &apps[0..2]);
    }

    #[test]
    fn test_dashmap_internal_clone() {
        let dashmap: DashMap<i32, Arc<RwLock<i32>>> = DashMap::new();
        dashmap.insert(1, Arc::new(RwLock::new(1)));
        dashmap.insert(2, Arc::new(RwLock::new(2)));

        let entry_1 = dashmap
            .entry(3)
            .or_insert_with(|| Arc::new(RwLock::new(3)))
            .clone();
        let entry_2 = dashmap
            .entry(3)
            .or_insert_with(|| Arc::new(RwLock::new(3)))
            .clone();
        let k1 = *entry_1.read();
        // drop(entry_1);
        let k2 = *entry_2.read();
        // drop(entry_2);
        assert_eq!(k1, k2);
    }
}
