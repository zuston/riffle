use crate::app_manager::app_configs::AppConfigOptions;
use crate::app_manager::partition_identifier::PartitionUId;
use crate::app_manager::partition_meta::PartitionMeta;
use crate::app_manager::purge_event::PurgeReason;
use crate::app_manager::request_context::{
    GetMultiBlockIdsContext, PurgeDataContext, ReadingIndexViewContext, ReadingOptions,
    ReadingViewContext, RegisterAppContext, ReleaseTicketContext, ReportMultiBlockIdsContext,
    RequireBufferContext, WritingViewContext,
};
use crate::block_id_manager::{get_block_id_manager, BlockIdManager};
use crate::config::Config;
use crate::config_reconfigure::ReconfigurableConfManager;
use crate::config_ref::{ByteString, ConfigOption};
use crate::constant::ALL_LABEL;
use crate::dashmap_extension::DashMapExtend;
use crate::error::WorkerError;
use crate::metric::{
    BLOCK_ID_NUMBER, GAUGE_HUGE_PARTITION_NUMBER, GAUGE_PARTITION_NUMBER, RESIDENT_BYTES,
    TOTAL_HUGE_PARTITION_NUMBER, TOTAL_HUGE_PARTITION_REQUIRE_BUFFER_FAILED,
    TOTAL_PARTITION_NUMBER, TOTAL_READ_DATA, TOTAL_READ_DATA_FROM_LOCALFILE,
    TOTAL_READ_DATA_FROM_MEMORY, TOTAL_READ_INDEX_FROM_LOCALFILE, TOTAL_RECEIVED_DATA,
    TOTAL_REQUIRE_BUFFER_FAILED,
};
use crate::runtime::manager::RuntimeManager;
use crate::store::hybrid::HybridStore;
use crate::store::{RequireBufferResponse, ResponseData, ResponseDataIndex, Store};
use crate::util;
use crate::util::{now_timestamp_as_millis, now_timestamp_as_sec};
use anyhow::Result;
use bytes::Bytes;
use fxhash::FxHasher;
use log::{error, info, warn};
use parking_lot::RwLock;
use std::collections::HashSet;
use std::hash::BuildHasherDefault;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

pub struct App {
    pub app_id: String,
    app_config_options: AppConfigOptions,
    latest_heartbeat_time: AtomicU64,
    store: Arc<HybridStore>,

    memory_capacity: u64,

    // partition limitation
    partition_limit_enable: bool,
    partition_limit_threshold: ConfigOption<ByteString>,
    partition_limit_mem_backpressure_ratio: ConfigOption<f64>,

    total_received_data_size: AtomicU64,
    total_resident_data_size: AtomicU64,

    // when exceeding the partition-limit-threshold, it will be marked as huge partition
    huge_partition_number: AtomicU64,

    pub(crate) start_timestamp: u128,

    // key: shuffle_id, val: shuffle's all block_ids bitmap
    block_id_manager: Arc<Box<dyn BlockIdManager>>,

    // key: (shuffle_id, partition_id)
    partition_meta_infos: DashMapExtend<(i32, i32), PartitionMeta, BuildHasherDefault<FxHasher>>,

    // partition split
    partition_split_enable: bool,
    partition_split_threshold: ConfigOption<ByteString>,

    // reconfiguration manager
    reconf_manager: ReconfigurableConfManager,

    // partition split triggered tag
    partition_split_triggered: AtomicBool,
}

impl App {
    pub fn from(
        app_id: String,
        config_options: AppConfigOptions,
        store: Arc<HybridStore>,
        runtime_manager: RuntimeManager,
        config: &Config,
        reconf_manager: &ReconfigurableConfManager,
    ) -> Self {
        // todo: should throw exception if register failed.
        let copy_app_id = app_id.to_string();
        let app_options = config_options.clone();
        match store.register_app(RegisterAppContext {
            app_id: copy_app_id,
            app_config_options: app_options,
        }) {
            Err(error) => {
                error!("Errors on registering app to store: {:#?}", error,);
            }
            _ => {}
        }

        let memory_capacity = util::to_bytes(&config.memory_store.as_ref().unwrap().capacity);

        let partition_limit_enable = config.app_config.partition_limit_enable;
        let partition_limit_threshold: ConfigOption<ByteString> = reconf_manager
            .register("app_config.partition_limit_threshold")
            .unwrap();
        let partition_limit_mem_backpressure_ratio = reconf_manager
            .register("app_config.partition_limit_memory_backpressure_ratio")
            .unwrap();

        let partition_split_enable = config.app_config.partition_split_enable;
        let partition_split_threshold: ConfigOption<ByteString> = reconf_manager
            .register("app_config.partition_split_threshold")
            .unwrap();

        // pre-check partition features values.
        // the partition-split threshold should always be less than the partition-limit threshold
        if partition_limit_enable && partition_split_enable {
            if partition_split_threshold.get().as_u64() >= partition_limit_threshold.get().as_u64()
            {
                panic!("The value of partition-split threshold should always be less than the partition-limit threshold value!")
            }
        }

        let block_id_manager = get_block_id_manager(&config.app_config.block_id_manager_type);

        info!("App=[{}]. {}. block_manager_type: {}. partition_limit/threshold/ratio: {}/{}/{}. partition_split/threshold: {}/{}",
            &app_id,
            &config_options,
            &config.app_config.block_id_manager_type,
            partition_limit_enable,
            partition_limit_threshold.get(),
            partition_limit_mem_backpressure_ratio.get(),
            partition_split_enable,
            partition_split_threshold.get()
        );

        App {
            app_id,
            app_config_options: config_options,
            latest_heartbeat_time: AtomicU64::new(now_timestamp_as_sec()),
            store,
            memory_capacity,
            partition_limit_enable,
            partition_limit_threshold,
            partition_limit_mem_backpressure_ratio,
            partition_meta_infos: DashMapExtend::<
                (i32, i32),
                PartitionMeta,
                BuildHasherDefault<FxHasher>,
            >::new(),
            total_received_data_size: Default::default(),
            total_resident_data_size: Default::default(),
            huge_partition_number: Default::default(),
            start_timestamp: now_timestamp_as_millis(),
            block_id_manager,
            partition_split_enable,
            partition_split_threshold,
            reconf_manager: reconf_manager.clone(),
            partition_split_triggered: AtomicBool::new(false),
        }
    }

    pub fn reported_block_id_number(&self) -> u64 {
        self.block_id_manager.get_blocks_number().unwrap_or(0)
    }

    pub fn huge_partition_number(&self) -> u64 {
        self.huge_partition_number.load(SeqCst)
    }

    pub fn partition_number(&self) -> usize {
        self.partition_meta_infos.len()
    }

    pub fn get_latest_heartbeat_time(&self) -> u64 {
        self.latest_heartbeat_time.load(SeqCst)
    }

    pub fn heartbeat(&self) -> anyhow::Result<()> {
        let timestamp = now_timestamp_as_sec();
        self.latest_heartbeat_time
            .store(timestamp, Ordering::Relaxed);
        Ok(())
    }

    pub fn register_shuffle(&self, shuffle_id: i32) -> anyhow::Result<()> {
        self.heartbeat()?;
        Ok(())
    }

    pub fn is_partition_split_triggered(&self) -> bool {
        self.partition_split_triggered.load(SeqCst)
    }

    pub async fn insert(&self, ctx: WritingViewContext) -> anyhow::Result<i32, WorkerError> {
        let len: u64 = ctx.data_size;

        TOTAL_RECEIVED_DATA.inc_by(len);
        RESIDENT_BYTES.add(len as i64);

        self.total_received_data_size
            .fetch_add(len, Ordering::Relaxed);
        self.total_resident_data_size
            .fetch_add(len, Ordering::Relaxed);

        // add the partition size into the meta and check its constraint
        let partition_meta = self.get_partition_meta(&ctx.uid);
        let partition_size = partition_meta.inc_size(len) + len;
        if self.partition_limit_enable
            && partition_size > self.partition_limit_threshold.get().as_u64()
            && !partition_meta.is_huge_partition()
        {
            partition_meta.mark_as_huge_partition();
            self.add_huge_partition_metric();
        }
        if self.partition_split_enable
            && partition_size > self.partition_split_threshold.get().as_u64()
            && !partition_meta.is_split()
        {
            partition_meta.mark_as_split();
            self.partition_split_triggered.store(true, SeqCst);
        }

        self.store.insert(ctx).await?;
        Ok(len as i32)
    }

    pub async fn select(&self, ctx: ReadingViewContext) -> Result<ResponseData, WorkerError> {
        self.heartbeat()?;

        let ctx = if (self.app_config_options.sendfile_enable) {
            ctx.with_sendfile_enabled()
        } else {
            ctx
        };

        let ctx = if self.app_config_options.read_ahead_enable {
            // This is a workaround — we can infer sequential reads when the taskId filter bitmap is enabled.
            match ctx.reading_options {
                ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(_, _) => {
                    if ctx.task_ids_filter.is_none() {
                        let partition_meta = self.get_partition_meta(&ctx.uid);
                        partition_meta.mark_as_sequential_read();
                    }
                    ctx
                }
                _ => {
                    // for the localfile getting, tag the sequential read
                    let partition_meta = self.get_partition_meta(&ctx.uid);
                    let ctx = ctx.with_read_ahead_client_enabled(true);
                    if (partition_meta.is_sequential_read()) {
                        ctx.with_sequential(
                            self.app_config_options.read_ahead_batch_number,
                            self.app_config_options.read_ahead_batch_size,
                        )
                    } else {
                        ctx
                    }
                }
            }
        } else {
            ctx
        };

        let response = self.store.get(ctx).await;
        response.map(|data| {
            match &data {
                ResponseData::Local(local_data) => {
                    let length = local_data.data.len() as u64;
                    TOTAL_READ_DATA_FROM_LOCALFILE.inc_by(length);
                    TOTAL_READ_DATA.inc_by(length);
                }
                ResponseData::Mem(mem_data) => {
                    let length = mem_data.data.len() as u64;
                    TOTAL_READ_DATA_FROM_MEMORY.inc_by(length);
                    TOTAL_READ_DATA.inc_by(length);
                }
            };

            data
        })
    }

    pub async fn list_index(
        &self,
        ctx: ReadingIndexViewContext,
    ) -> anyhow::Result<ResponseDataIndex, WorkerError> {
        self.heartbeat()?;

        let response = self.store.get_index(ctx).await;
        response.map(|data| {
            match &data {
                ResponseDataIndex::Local(local_data) => {
                    let len = local_data.index_data.len();
                    TOTAL_READ_INDEX_FROM_LOCALFILE.inc_by(len as u64);
                    TOTAL_READ_DATA.inc_by(len as u64);
                }
                _ => {}
            };
            data
        })
    }

    // Only for test case
    pub fn mark_huge_partition(&self, uid: &PartitionUId) -> anyhow::Result<()> {
        let mut meta = self.get_partition_meta(uid);
        meta.mark_as_huge_partition();
        Ok(())
    }

    fn add_huge_partition_metric(&self) {
        self.huge_partition_number.fetch_add(1, Ordering::SeqCst);
        TOTAL_HUGE_PARTITION_NUMBER.inc();
        GAUGE_HUGE_PARTITION_NUMBER.inc();
    }

    fn sub_huge_partition_metric(&self) {
        let number = self.huge_partition_number.load(SeqCst);
        if number > 0 {
            GAUGE_HUGE_PARTITION_NUMBER.sub(number as i64);
        }
    }

    pub fn dec_allocated_from_budget(&self, size: i64) -> anyhow::Result<bool> {
        self.store.release_allocated_from_hot_store(size)
    }

    pub fn move_allocated_used_from_budget(&self, size: i64) -> anyhow::Result<bool> {
        self.store.move_allocated_to_used_from_hot_store(size)
    }

    pub fn is_huge_partition(&self, uid: &PartitionUId) -> Result<bool> {
        if self.partition_limit_enable {
            let partition_meta = self.get_partition_meta(uid);
            Ok(partition_meta.is_huge_partition())
        } else {
            Ok(false)
        }
    }

    pub async fn require_buffer(
        &self,
        ctx: RequireBufferContext,
    ) -> anyhow::Result<RequireBufferResponse, WorkerError> {
        self.heartbeat()?;

        let app_id = &ctx.uid.app_id;
        let shuffle_id = &ctx.uid.shuffle_id;

        let mut partition_split_candidates = HashSet::new();

        if self.partition_limit_enable || self.partition_split_enable {
            let partition_limit_threshold = (self.memory_capacity as f64
                * self.partition_limit_mem_backpressure_ratio.get())
                as u64;

            for partition_id in &ctx.partition_ids {
                let puid = PartitionUId::new(app_id, *shuffle_id, *partition_id);
                let partition_meta = self.get_partition_meta(&puid);

                if self.partition_split_enable && partition_meta.is_split() {
                    partition_split_candidates.insert(*partition_id);
                }

                if self.partition_limit_enable && partition_meta.is_huge_partition() {
                    let used = self.store.get_memory_buffer_size(&puid)?;
                    if used > partition_limit_threshold {
                        info!("[{}] with huge partition (used/limited: {}/{}), it has been limited of writing speed",
                            puid, used, partition_limit_threshold);
                        TOTAL_HUGE_PARTITION_REQUIRE_BUFFER_FAILED.inc();
                        return Err(WorkerError::MEMORY_USAGE_LIMITED_BY_HUGE_PARTITION);
                    }
                }
            }
        }

        let mut required = self.store.require_buffer(ctx).await.map_err(|err| {
            TOTAL_REQUIRE_BUFFER_FAILED.inc();
            err
        })?;
        required.split_partitions = partition_split_candidates
            .iter()
            .map(|data| *data)
            .collect();
        Ok(required)
    }

    pub async fn release_ticket(&self, ticket_id: i64) -> anyhow::Result<i64, WorkerError> {
        self.store
            .release_ticket(ReleaseTicketContext::from(ticket_id))
            .await
    }

    pub fn get_partition_meta(&self, uid: &PartitionUId) -> PartitionMeta {
        let shuffle_id = uid.shuffle_id;
        let partition_id = uid.partition_id;
        self.partition_meta_infos
            .compute_if_absent((shuffle_id, partition_id), || {
                TOTAL_PARTITION_NUMBER.inc();
                GAUGE_PARTITION_NUMBER.inc();
                PartitionMeta::new()
            })
    }

    pub async fn get_multi_block_ids(&self, ctx: GetMultiBlockIdsContext) -> anyhow::Result<Bytes> {
        self.heartbeat()?;
        self.block_id_manager.get_multi_block_ids(ctx).await
    }

    pub async fn report_multi_block_ids(
        &self,
        ctx: ReportMultiBlockIdsContext,
    ) -> anyhow::Result<()> {
        self.heartbeat()?;
        let number = self.block_id_manager.report_multi_block_ids(ctx).await?;
        BLOCK_ID_NUMBER.add(number as i64);
        Ok(())
    }

    pub async fn dump_all_huge_partitions_size(&self) -> anyhow::Result<Vec<u64>> {
        let mut records = vec![];
        let view = self.partition_meta_infos.clone().into_read_only();
        for entry in view.iter() {
            let val = entry.1;
            if val.is_huge_partition() {
                let size = val.get_size();
                records.push(size);
            }
        }
        Ok(records)
    }

    pub async fn purge(&self, reason: &PurgeReason) -> anyhow::Result<()> {
        let (app_id, shuffle_id) = reason.extract();
        let removed_size = self.store.purge(&PurgeDataContext::new(reason)).await?;
        self.total_resident_data_size
            .fetch_sub(removed_size as u64, SeqCst);

        RESIDENT_BYTES.sub(removed_size);

        if let Some(shuffle_id) = shuffle_id {
            // shuffle level bitmap deletion
            let purged_number = self.block_id_manager.purge_block_ids(shuffle_id).await?;
            BLOCK_ID_NUMBER.sub(purged_number as i64);

            let mut deletion_keys = vec![];
            let view = self.partition_meta_infos.clone().into_read_only();
            for entry in view.iter() {
                let key = entry.0;
                if shuffle_id == key.0 {
                    deletion_keys.push(key);
                }
            }
            GAUGE_PARTITION_NUMBER.sub(deletion_keys.len() as i64);
            let mut huge_partition_cnt = 0;
            for deletion_key in deletion_keys {
                if let Some(meta) = self.partition_meta_infos.remove(deletion_key) {
                    if meta.1.is_huge_partition() {
                        huge_partition_cnt += 1;
                    }
                }
            }
            GAUGE_HUGE_PARTITION_NUMBER.sub(huge_partition_cnt as i64);
        } else {
            // app level deletion
            GAUGE_PARTITION_NUMBER.sub(self.partition_meta_infos.len() as i64);
            self.sub_huge_partition_metric();

            BLOCK_ID_NUMBER.sub(self.block_id_manager.get_blocks_number()? as i64);
        }

        Ok(())
    }

    pub fn total_received_data_size(&self) -> u64 {
        self.total_received_data_size.load(SeqCst)
    }

    pub fn total_resident_data_size(&self) -> u64 {
        self.total_resident_data_size.load(SeqCst)
    }
}
