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

use crate::app_manager::request_context::ReadingOptions::FILE_OFFSET_AND_LEN;
use crate::app_manager::request_context::{
    PurgeDataContext, ReadingIndexViewContext, ReadingViewContext, RegisterAppContext,
    ReleaseTicketContext, RequireBufferContext, RpcType, WritingViewContext,
};
use crate::config::{LocalfileStoreConfig, StorageType};
use crate::error::WorkerError;
use crate::metric::{
    GAUGE_LOCAL_DISK_SERVICE_USED, RPC_BATCH_BYTES_OPERATION, RPC_BATCH_DATA_BYTES_HISTOGRAM,
    TOTAL_DETECTED_LOCALFILE_IN_CONSISTENCY, TOTAL_LOCALFILE_USED,
};
use crate::store::ResponseDataIndex::Local;
use crate::store::{
    Block, DataBytes, LocalDataIndex, PartitionedLocalData, Persistent, RequireBufferResponse,
    ResponseData, ResponseDataIndex, Store,
};
use std::cmp::min;
use std::fs;
use std::hash::BuildHasherDefault;
use std::ops::Deref;
use std::path::Path;
use std::str::FromStr;

use anyhow::Result;
use async_trait::async_trait;
use await_tree::InstrumentAwait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use dashmap::DashMap;

use log::{debug, error, info, warn};

use crate::app_manager::partition_identifier::PartitionUId;
use crate::await_tree::AWAIT_TREE_REGISTRY;
use crate::composed_bytes::ComposedBytes;
use crate::dashmap_extension::DashMapExtend;
use crate::readable_size::ReadableSize;
use crate::runtime::manager::RuntimeManager;
use crate::store::index_codec::{IndexCodec, INDEX_BLOCK_SIZE};
use crate::store::local::delegator::LocalDiskDelegator;
use crate::store::local::options::{CreateOptions, ReadOptions, WriteOptions};
use crate::store::local::{LocalDiskStorage, LocalIO, LocalfileStoreStat};
use crate::store::spill::SpillWritingViewContext;
use crate::util;
use crate::util::get_crc;
use dashmap::mapref::entry::Entry;
use futures::AsyncReadExt;
use fxhash::FxHasher;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::Instrument;

struct PartitionCoordinator {
    disk: LocalDiskDelegator,
    pointer: Arc<AtomicU64>,
    write_lock: Arc<Mutex<()>>,
}

impl From<LocalDiskDelegator> for PartitionCoordinator {
    fn from(value: LocalDiskDelegator) -> Self {
        Self {
            disk: value,
            pointer: Default::default(),
            write_lock: Arc::new(Default::default()),
        }
    }
}

pub struct LocalFileStore {
    local_disks: Vec<LocalDiskDelegator>,
    min_number_of_available_disks: i32,
    runtime_manager: RuntimeManager,
    partition_coordinators:
        DashMapExtend<String, Arc<PartitionCoordinator>, BuildHasherDefault<FxHasher>>,

    direct_io_enable: bool,
    direct_io_read_enable: bool,
    direct_io_append_enable: bool,

    // This is only valid in urpc mode.
    read_io_sendfile_enable: bool,

    conf: LocalfileStoreConfig,
}

impl Persistent for LocalFileStore {}

unsafe impl Send for LocalFileStore {}
unsafe impl Sync for LocalFileStore {}

impl LocalFileStore {
    // only for test cases
    pub fn new(local_disks: Vec<String>) -> Self {
        let mut local_disk_instances = vec![];
        let runtime_manager: RuntimeManager = Default::default();
        let config = LocalfileStoreConfig::new(local_disks.clone());
        for path in &local_disks {
            local_disk_instances.push(LocalDiskDelegator::new(&runtime_manager, &path, &config));
        }
        LocalFileStore {
            local_disks: local_disk_instances,
            min_number_of_available_disks: 1,
            runtime_manager,
            partition_coordinators: DashMapExtend::<
                String,
                Arc<PartitionCoordinator>,
                BuildHasherDefault<FxHasher>,
            >::new(),
            direct_io_enable: config.direct_io_enable,
            direct_io_read_enable: config.direct_io_read_enable,
            direct_io_append_enable: config.direct_io_append_enable,
            read_io_sendfile_enable: false,
            conf: Default::default(),
        }
    }

    pub fn stat(&self) -> Result<LocalfileStoreStat> {
        let mut stats = vec![];
        for local_disk in &self.local_disks {
            let stat = local_disk.stat()?;
            stats.push(stat);
        }
        Ok(LocalfileStoreStat { stats })
    }

    pub fn from(localfile_config: LocalfileStoreConfig, runtime_manager: RuntimeManager) -> Self {
        let mut local_disk_instances = vec![];
        for path in &localfile_config.data_paths {
            if localfile_config.launch_purge_enable {
                info!("Launch purging for [{}]...", path.as_str());
                if let Err(e) = LocalFileStore::remove_dir_children(path.as_str()) {
                    panic!(
                        "Errors on clear up children files of path: {:?}. err: {:#?}",
                        path.as_str(),
                        e
                    );
                }
            }
            local_disk_instances.push(LocalDiskDelegator::new(
                &runtime_manager,
                &path,
                &localfile_config,
            ));
        }

        let len = local_disk_instances.len();
        if len <= 0 {
            panic!("Must specify at least one local disk path!")
        }

        let min_number_of_available_disks = match localfile_config.min_number_of_available_disks {
            Some(value) => min(len as i32, value),
            _ => len as i32,
        };

        info!("Initializing localfile store with the disk paths: [{:?}] and min_number_of_available_disks: [{}]",
            &localfile_config.data_paths, min_number_of_available_disks);

        LocalFileStore {
            local_disks: local_disk_instances,
            min_number_of_available_disks,
            runtime_manager,
            partition_coordinators: DashMapExtend::<
                String,
                Arc<PartitionCoordinator>,
                BuildHasherDefault<FxHasher>,
            >::new(),
            direct_io_enable: localfile_config.direct_io_enable,
            direct_io_read_enable: localfile_config.direct_io_read_enable,
            direct_io_append_enable: localfile_config.direct_io_append_enable,
            read_io_sendfile_enable: localfile_config.read_io_sendfile_enable,
            conf: localfile_config.clone(),
        }
    }

    fn remove_dir_children(parent: &str) -> Result<()> {
        for entry in std::fs::read_dir(parent)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                std::fs::remove_dir_all(entry.path())?;
                continue;
            }
            if file_type.is_file() {
                std::fs::remove_file(entry.path())?;
                continue;
            }
        }
        Ok(())
    }

    fn gen_relative_path_for_app(app_id: &str) -> String {
        format!("{}", app_id)
    }

    fn gen_relative_path_for_shuffle(app_id: &str, shuffle_id: i32) -> String {
        format!("{}/{}/", app_id, shuffle_id)
    }

    fn gen_relative_path_for_partition(uid: &PartitionUId) -> (String, String) {
        (
            format!(
                "{}/{}/partition-{}.data",
                uid.app_id, uid.shuffle_id, uid.partition_id
            ),
            format!(
                "{}/{}/partition-{}.index",
                uid.app_id, uid.shuffle_id, uid.partition_id
            ),
        )
    }

    fn healthy_check(&self) -> Result<bool> {
        let mut available = 0;
        for local_disk in &self.local_disks {
            if local_disk.is_healthy()? && !local_disk.is_corrupted()? {
                available += 1;
            }
        }

        debug!(
            "disk: available={}, healthy_check_min={}",
            available, self.min_number_of_available_disks
        );
        Ok(available >= self.min_number_of_available_disks)
    }

    fn select_disk(&self, uid: &PartitionUId) -> Result<LocalDiskDelegator, WorkerError> {
        let hash_value = PartitionUId::get_hash(uid);

        let mut candidates = vec![];
        for local_disk in &self.local_disks {
            if !local_disk.is_corrupted()? && local_disk.is_healthy()? {
                candidates.push(local_disk);
            }
        }

        let len = candidates.len();
        if len == 0 {
            error!("There is no available local disk!");
            return Err(WorkerError::NO_AVAILABLE_LOCAL_DISK);
        }

        let index = (hash_value % len as u64) as usize;
        if let Some(&disk) = candidates.get(index) {
            Ok(disk.clone())
        } else {
            Err(WorkerError::INTERNAL_ERROR)
        }
    }

    async fn data_insert(&self, uid: PartitionUId, blocks: Vec<&Block>) -> Result<(), WorkerError> {
        let (data_file_path, index_file_path) =
            LocalFileStore::gen_relative_path_for_partition(&uid);

        let mut parent_dir_is_created = true;
        let partition_coordinator = match self.partition_coordinators.entry(data_file_path.clone())
        {
            Entry::Vacant(e) => {
                parent_dir_is_created = false;
                let disk = self.select_disk(&uid)?;
                let obj = e.insert_entry(Arc::new(PartitionCoordinator::from(disk)));
                obj.get().clone()
            }
            Entry::Occupied(v) => v.get().clone(),
        };

        let partition_write_lock = partition_coordinator
            .write_lock
            .lock()
            .instrument_await("waiting the localfile partition lock")
            .await;
        let local_disk = &partition_coordinator.disk;
        let next_offset = partition_coordinator.pointer.load(SeqCst);

        if local_disk.is_corrupted()? {
            return Err(WorkerError::PARTIAL_DATA_LOST(local_disk.root()));
        }

        if !local_disk.is_healthy()? {
            return Err(WorkerError::LOCAL_DISK_UNHEALTHY(local_disk.root()));
        }

        if !parent_dir_is_created {
            if let Some(path) = Path::new(&data_file_path).parent() {
                let path = format!("{}/", path.to_str().unwrap()).as_str().to_owned();
                local_disk
                    .create(path.as_str(), CreateOptions::DIR)
                    .instrument_await(format!("creating the directory: {}", path.as_str()))
                    .await?;
            }
        }

        let shuffle_file_format = self.create_shuffle_format(blocks, next_offset as i64)?;
        let options = if self.direct_io_enable && self.direct_io_append_enable {
            WriteOptions::with_append_of_direct_io(shuffle_file_format.data, next_offset)
        } else {
            WriteOptions::with_append_of_buffer_io(shuffle_file_format.data)
        };
        let append_future = local_disk.write(&data_file_path, options);
        append_future
            .instrument_await(format!(
                "data flushing with {} bytes. path: {}",
                shuffle_file_format.len, &data_file_path
            ))
            .await?;
        let index_bytes_len = shuffle_file_format.index.len();
        local_disk
            .write(
                &index_file_path,
                WriteOptions::with_append_of_buffer_io(shuffle_file_format.index),
            )
            .instrument_await(format!(
                "index flushing with {} bytes. path: {}",
                index_bytes_len, &index_file_path
            ))
            .await?;

        TOTAL_LOCALFILE_USED.inc_by(shuffle_file_format.len as u64);
        GAUGE_LOCAL_DISK_SERVICE_USED
            .with_label_values(&[&local_disk.root()])
            .add(shuffle_file_format.len as i64);

        partition_coordinator
            .deref()
            .pointer
            .store(shuffle_file_format.offset as u64, SeqCst);

        Ok(())
    }

    fn delete_all_files(dir: &Path) -> Result<()> {
        let entries = fs::read_dir(dir)?;
        for entry in entries {
            let path = entry?.path();
            if path.is_file() {
                fs::remove_file(&path)?;
            }
        }
        Ok(())
    }

    // To detect the index consistency with data file len for debug.
    pub(crate) fn detect_index_inconsistency(
        data: &Bytes,
        data_file_len: i64,
        root: &String,
        index_file_path: &String,
        data_file_path: &String,
    ) -> Result<bool> {
        let last_block_raw_bytes = data.slice(data.len() - INDEX_BLOCK_SIZE..);
        match IndexCodec::decode(last_block_raw_bytes) {
            Ok(index_block) => {
                let index_indicated_data_len = index_block.offset + index_block.length as i64;
                if data_file_len != index_indicated_data_len {
                    TOTAL_DETECTED_LOCALFILE_IN_CONSISTENCY.inc();
                    let timestamp = util::now_timestamp_as_millis();
                    warn!("Attention: index indicated data len:{} != recorded data len:{}. root: {}. index path: {}. data path: {}. timestamp: {}",
                            index_indicated_data_len, data_file_len, root, &index_file_path, &data_file_path, timestamp);
                    let main_dir = Path::new("/tmp/riffle-detection");
                    if !main_dir.exists() {
                        fs::create_dir(main_dir)?;
                    }
                    // clear the previous file.
                    LocalFileStore::delete_all_files(main_dir)?;

                    let index_target_file_name = format!(
                        "{}/{}-{}",
                        &main_dir.to_string_lossy(),
                        &index_file_path.replace("/", "-"),
                        timestamp
                    );
                    let data_target_file_name = format!(
                        "{}/{}-{}",
                        &main_dir.to_string_lossy(),
                        &data_file_path.replace("/", "-"),
                        timestamp
                    );

                    fs::copy(
                        &Path::new(&format!("{}/{}", root, index_file_path)),
                        &Path::new(index_target_file_name.as_str()),
                    )?;
                    if data_file_len < 1024 * 1024 * 1024 {
                        fs::copy(
                            &Path::new(&format!("{}/{}", root, data_file_path)),
                            &Path::new(data_target_file_name.as_str()),
                        )?;
                    } else {
                        error!("Ignore copying data file due to the too large file. data_file_path: {}", &data_file_path);
                    }

                    return Ok(false);
                }
            }
            Err(err) => {
                error!("Errors on decoding the raw block. {:?}", err);
                return Ok(false);
            }
        }

        Ok(true)
    }
}

#[async_trait]
impl Store for LocalFileStore {
    fn start(self: Arc<Self>) {
        todo!()
    }

    async fn insert(&self, ctx: WritingViewContext) -> Result<(), WorkerError> {
        if ctx.data_blocks.len() <= 0 {
            return Ok(());
        }

        let uid = ctx.uid;
        let blocks: Vec<&Block> = ctx.data_blocks.iter().collect();
        self.data_insert(uid, blocks).await
    }

    async fn get(&self, ctx: ReadingViewContext) -> Result<ResponseData, WorkerError> {
        let uid = ctx.uid;
        let rpc_source = ctx.rpc_source;
        let client_sendfile_enabled = ctx.sendfile_enabled;
        let (offset, len) = match ctx.reading_options {
            FILE_OFFSET_AND_LEN(offset, len) => (offset as u64, len as u64),
            _ => (0, 0),
        };

        if len == 0 {
            warn!("There is no data in localfile for [{:?}]", &uid);
            return Ok(ResponseData::Local(PartitionedLocalData {
                data: Default::default(),
            }));
        }

        let (data_file_path, _) = LocalFileStore::gen_relative_path_for_partition(&uid);
        match self.partition_coordinators.get(&data_file_path) {
            None => {
                warn!(
                    "There is no cached data in localfile store for [{:?}]",
                    &uid
                );
                Ok(ResponseData::Local(PartitionedLocalData {
                    data: Default::default(),
                }))
            }
            Some(coordinator) => {
                let local_disk = &coordinator.disk;
                if local_disk.is_corrupted()? {
                    return Err(WorkerError::LOCAL_DISK_OWNED_BY_PARTITION_CORRUPTED(
                        local_disk.root(),
                    ));
                }
                let read_options = if self.direct_io_enable && self.direct_io_read_enable {
                    ReadOptions::with_read_of_direct_io(offset, len)
                } else if (self.read_io_sendfile_enable
                    && rpc_source == RpcType::URPC
                    && client_sendfile_enabled)
                {
                    ReadOptions::with_sendfile(offset, len)
                } else {
                    ReadOptions::with_read_of_buffer_io(offset, len)
                };
                let future_read = local_disk.read(&data_file_path, read_options);
                let data = future_read
                    .instrument_await(format!(
                        "getting data from offset:{} with expected {} bytes from localfile: {}",
                        offset, len, &data_file_path
                    ))
                    .await?;

                RPC_BATCH_DATA_BYTES_HISTOGRAM
                    .with_label_values(
                        &[&RPC_BATCH_BYTES_OPERATION::LOCALFILE_GET_DATA.to_string()],
                    )
                    .observe(data.len() as f64);

                Ok(ResponseData::Local(PartitionedLocalData { data }))
            }
        }
    }

    async fn get_index(
        &self,
        ctx: ReadingIndexViewContext,
    ) -> Result<ResponseDataIndex, WorkerError> {
        let uid = &ctx.partition_id;
        let (data_file_path, index_file_path) =
            LocalFileStore::gen_relative_path_for_partition(&uid);

        match self.partition_coordinators.get(&data_file_path) {
            None => {
                warn!(
                    "There is no cached data in localfile store for [{:?}]",
                    &uid
                );
                Ok(Local(LocalDataIndex {
                    index_data: Default::default(),
                    data_file_len: 0,
                }))
            }
            Some(partition_coordinator) => {
                let local_disk = &partition_coordinator.disk;
                if local_disk.is_corrupted()? {
                    return Err(WorkerError::LOCAL_DISK_OWNED_BY_PARTITION_CORRUPTED(
                        local_disk.root(),
                    ));
                }
                let len = partition_coordinator.pointer.load(SeqCst);
                let mut data = local_disk
                    .read(&index_file_path, ReadOptions::with_read_all())
                    .instrument_await(format!(
                        "reading index data from file: {:?}",
                        &index_file_path
                    ))
                    .await?;
                RPC_BATCH_DATA_BYTES_HISTOGRAM
                    .with_label_values(&[
                        &RPC_BATCH_BYTES_OPERATION::LOCALFILE_GET_INDEX.to_string()
                    ])
                    .observe(data.len() as f64);

                // Detect inconsistent data
                if self.conf.index_consistency_detection_enable && data.len() > INDEX_BLOCK_SIZE {
                    data = DataBytes::Direct(data.freeze());
                    if let Err(e) = LocalFileStore::detect_index_inconsistency(
                        &data.get_direct(),
                        len as i64,
                        &local_disk.root(),
                        &index_file_path,
                        &data_file_path,
                    ) {
                        error!("Errors on detecting index inconsistency. err: {}", e);
                    }
                }

                Ok(Local(LocalDataIndex {
                    index_data: data,
                    data_file_len: len as i64,
                }))
            }
        }
    }

    async fn purge(&self, ctx: &PurgeDataContext) -> Result<i64> {
        let (app_id, shuffle_id_option) = ctx.extract();
        let raw_app_id = app_id.to_string();
        let data_relative_dir_path = match shuffle_id_option {
            Some(shuffle_id) => {
                LocalFileStore::gen_relative_path_for_shuffle(&raw_app_id, shuffle_id)
            }
            _ => LocalFileStore::gen_relative_path_for_app(&raw_app_id),
        };

        for local_disk_ref in &self.local_disks {
            let disk = local_disk_ref.clone();
            disk.delete(&data_relative_dir_path).await?;
        }

        let keys_to_delete: Vec<_> = self
            .partition_coordinators
            .iter()
            .filter(|entry| entry.key().starts_with(&data_relative_dir_path))
            .map(|entry| entry.key().to_string())
            .collect();

        let mut removed_data_size = 0u64;
        for key in keys_to_delete {
            let meta = self.partition_coordinators.remove(&key);
            if let Some(x) = meta {
                let p_lock = x.1.write_lock.lock().await;
                let size = x.1.pointer.load(SeqCst);
                removed_data_size += size;
                GAUGE_LOCAL_DISK_SERVICE_USED
                    .with_label_values(&[&x.1.disk.root()])
                    .sub(size as i64);
            }
        }

        Ok(removed_data_size as i64)
    }

    async fn is_healthy(&self) -> Result<bool> {
        self.healthy_check()
    }

    async fn require_buffer(
        &self,
        _ctx: RequireBufferContext,
    ) -> Result<RequireBufferResponse, WorkerError> {
        todo!()
    }

    async fn release_ticket(&self, _ctx: ReleaseTicketContext) -> Result<i64, WorkerError> {
        todo!()
    }

    fn register_app(&self, _ctx: RegisterAppContext) -> Result<()> {
        Ok(())
    }

    async fn name(&self) -> StorageType {
        StorageType::LOCALFILE
    }

    async fn spill_insert(&self, ctx: SpillWritingViewContext) -> Result<(), WorkerError> {
        let uid = ctx.uid;
        let mut data = vec![];
        let batch_memory_block = ctx.data_blocks;
        for blocks in batch_memory_block.iter() {
            for block in blocks {
                data.push(block);
            }
        }
        // for AQE
        data.sort_by_key(|block| block.task_attempt_id);
        self.data_insert(uid, data)
            .instrument_await("data insert")
            .await
    }

    async fn pre_check(&self) -> Result<(), WorkerError> {
        // todo: check the localfile permission
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use crate::app_manager::request_context::{
        PurgeDataContext, ReadingIndexViewContext, ReadingOptions, ReadingViewContext, RpcType,
        WritingViewContext,
    };
    use crate::store::localfile::LocalFileStore;

    use crate::app_manager::application_identifier::ApplicationId;
    use crate::app_manager::partition_identifier::PartitionUId;
    use crate::app_manager::purge_event::PurgeReason;
    use crate::error::WorkerError;
    use crate::store::index_codec::{IndexBlock, IndexCodec};
    use crate::store::local::LocalDiskStorage;
    use crate::store::{Block, ResponseData, ResponseDataIndex, Store};
    use bytes::{Buf, Bytes, BytesMut};
    use log::{error, info};

    fn create_writing_ctx() -> WritingViewContext {
        let uid = PartitionUId::new(&Default::default(), 0, 0);
        let data = b"hello world!hello china!";
        let size = data.len();
        let writing_ctx = WritingViewContext::create_for_test(
            uid.clone(),
            vec![
                Block {
                    block_id: 0,
                    length: size as i32,
                    uncompress_length: 200,
                    crc: 0,
                    data: Bytes::copy_from_slice(data),
                    task_attempt_id: 0,
                },
                Block {
                    block_id: 1,
                    length: size as i32,
                    uncompress_length: 200,
                    crc: 0,
                    data: Bytes::copy_from_slice(data),
                    task_attempt_id: 0,
                },
            ],
        );

        writing_ctx
    }

    #[test]
    fn local_disk_under_exception_test() -> anyhow::Result<()> {
        let temp_dir = tempdir::TempDir::new("local_disk_under_exception_test").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("init local file path: {}", &temp_path);
        let local_store = LocalFileStore::new(vec![temp_path.to_string()]);

        let runtime = local_store.runtime_manager.clone();

        let writing_view_ctx = create_writing_ctx();
        let insert_result = runtime.wait(local_store.insert(writing_view_ctx));

        if insert_result.is_err() {
            println!("{:?}", insert_result.err());
            panic!()
        }

        // case1: mark the local disk unhealthy, that will the following flush throw exception directly.
        let local_disk = local_store.local_disks[0].clone();
        local_disk.mark_operation_abnormal();

        let writing_view_ctx = create_writing_ctx();
        let insert_result = runtime.wait(local_store.insert(writing_view_ctx));
        match insert_result {
            Err(WorkerError::LOCAL_DISK_UNHEALTHY(_)) => {}
            _ => panic!(),
        }

        // case2: mark the local disk healthy, all things work!
        local_disk.mark_operation_normal();
        let writing_view_ctx = create_writing_ctx();
        let insert_result = runtime.wait(local_store.insert(writing_view_ctx));
        match insert_result {
            Err(WorkerError::LOCAL_DISK_UNHEALTHY(_)) => panic!(),
            _ => {}
        }

        // case3: mark the local disk corrupted, fail directly.
        local_disk.mark_corrupted();
        let writing_view_ctx = create_writing_ctx();
        let insert_result = runtime.wait(local_store.insert(writing_view_ctx));
        match insert_result {
            Err(WorkerError::PARTIAL_DATA_LOST(_)) => {}
            _ => panic!(),
        }

        Ok(())
    }

    fn create_writing_ctx_by_uid(uid: &PartitionUId) -> WritingViewContext {
        let data = b"hello world!hello china!";
        let size = data.len();
        let writing_ctx = WritingViewContext::create_for_test(
            uid.clone(),
            vec![
                Block {
                    block_id: 0,
                    length: size as i32,
                    uncompress_length: 200,
                    crc: 0,
                    data: Bytes::copy_from_slice(data),
                    task_attempt_id: 0,
                },
                Block {
                    block_id: 1,
                    length: size as i32,
                    uncompress_length: 200,
                    crc: 0,
                    data: Bytes::copy_from_slice(data),
                    task_attempt_id: 0,
                },
            ],
        );
        writing_ctx
    }

    #[test]
    fn purge_test() -> anyhow::Result<()> {
        let temp_dir = tempdir::TempDir::new("test_local_store").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("init local file path: {}", &temp_path);
        let local_store = LocalFileStore::new(vec![temp_path.clone()]);

        let runtime = local_store.runtime_manager.clone();

        let app_id = "application_1747379850000_7237726_1749434628480";
        let application_id = ApplicationId::from(app_id);

        let shuffle_id_1 = 1;
        let shuffle_id_2 = 13;

        let uid_1 = PartitionUId::new(&application_id, shuffle_id_1, 0);
        let uid_2 = PartitionUId::new(&application_id, shuffle_id_2, 0);

        // for shuffle_id = 1
        let writing_ctx_1 = create_writing_ctx_by_uid(&uid_1);
        let insert_result = runtime.wait(local_store.insert(writing_ctx_1));
        if insert_result.is_err() {
            println!("{:?}", insert_result.err());
            panic!()
        }
        assert_eq!(
            true,
            runtime.wait(tokio::fs::try_exists(format!(
                "{}/{}/{}/partition-{}.data",
                &temp_path, &app_id, shuffle_id_1, "0"
            )))?
        );

        // for shuffle_id = 13
        let writing_ctx_2 = create_writing_ctx_by_uid(&uid_2);
        let insert_result = runtime.wait(local_store.insert(writing_ctx_2));
        if insert_result.is_err() {
            println!("{:?}", insert_result.err());
            panic!()
        }
        assert_eq!(
            true,
            runtime.wait(tokio::fs::try_exists(format!(
                "{}/{}/{}/partition-{}.data",
                &temp_path, &app_id, shuffle_id_2, "0"
            )))?
        );

        // shuffle level purge
        runtime
            .wait(local_store.purge(&PurgeDataContext::new(
                &PurgeReason::SHUFFLE_LEVEL_EXPLICIT_UNREGISTER(
                    application_id.to_owned(),
                    shuffle_id_1,
                ),
            )))
            .expect("");
        assert_eq!(
            false,
            runtime.wait(tokio::fs::try_exists(format!(
                "{}/{}/{}",
                &temp_path, &app_id, shuffle_id_1
            )))?
        );
        // the shuffle_id = 1 deletion will not effect shuffle_id = 13
        let reading_ctx = ReadingIndexViewContext {
            partition_id: uid_2.clone(),
        };
        let reading_result = runtime.wait(local_store.get_index(reading_ctx)).expect("");
        if let ResponseDataIndex::Local(index) = reading_result {
            assert!(index.data_file_len > 0);
        }

        // app level purge
        runtime.wait(local_store.purge(&PurgeDataContext {
            purge_reason: PurgeReason::APP_LEVEL_EXPLICIT_UNREGISTER(application_id.to_owned()),
        }))?;
        assert_eq!(
            false,
            runtime.wait(tokio::fs::try_exists(format!("{}/{}", &temp_path, &app_id)))?
        );

        Ok(())
    }

    #[test]
    #[ignore]
    fn local_store_test() {
        let temp_dir = tempdir::TempDir::new("test_local_store").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        info!("init local file path: {}", temp_path);
        let mut local_store = LocalFileStore::new(vec![temp_path]);

        let runtime = local_store.runtime_manager.clone();

        let uid = PartitionUId::new(&Default::default(), 0, 0);
        let data = b"hello world!hello china!";
        let size = data.len();
        let writing_ctx = WritingViewContext::create_for_test(
            uid.clone(),
            vec![
                Block {
                    block_id: 0,
                    length: size as i32,
                    uncompress_length: 200,
                    crc: 0,
                    data: Bytes::copy_from_slice(data),
                    task_attempt_id: 0,
                },
                Block {
                    block_id: 1,
                    length: size as i32,
                    uncompress_length: 200,
                    crc: 0,
                    data: Bytes::copy_from_slice(data),
                    task_attempt_id: 0,
                },
            ],
        );

        let insert_result = runtime.wait(local_store.insert(writing_ctx));
        if insert_result.is_err() {
            println!("{:?}", insert_result.err());
            panic!()
        }

        async fn get_and_check_partitial_data(
            local_store: &mut LocalFileStore,
            uid: PartitionUId,
            size: i64,
            expected: &[u8],
        ) {
            let reading_ctx = ReadingViewContext::new(
                uid,
                ReadingOptions::FILE_OFFSET_AND_LEN(0, size as i64),
                RpcType::GRPC,
            );

            let read_result = local_store.get(reading_ctx).await;
            if read_result.is_err() {
                error!("failed to get the localfile data: {:?}", read_result.err());
                panic!()
            }

            match read_result.unwrap() {
                ResponseData::Local(partitioned_data) => {
                    assert_eq!(expected, partitioned_data.data.freeze().as_ref());
                }
                _ => panic!(),
            }
        }

        // case1: read the one partition block data
        runtime.wait(get_and_check_partitial_data(
            &mut local_store,
            uid.clone(),
            size as i64,
            data,
        ));

        // case2: read the complete block data
        let mut expected = BytesMut::with_capacity(size * 2);
        expected.extend_from_slice(data);
        expected.extend_from_slice(data);
        runtime.wait(get_and_check_partitial_data(
            &mut local_store,
            uid.clone(),
            size as i64 * 2,
            expected.freeze().as_ref(),
        ));

        // case3: get the index data
        let reading_index_view_ctx = ReadingIndexViewContext {
            partition_id: uid.clone(),
        };
        let result = runtime.wait(local_store.get_index(reading_index_view_ctx));
        if result.is_err() {
            panic!()
        }

        match result.unwrap() {
            ResponseDataIndex::Local(data) => {
                let mut index = data.index_data.freeze();
                let offset_1 = index.get_i64();
                assert_eq!(0, offset_1);
                let length_1 = index.get_i32();
                assert_eq!(size as i32, length_1);
                index.get_i32();
                index.get_i64();
                let block_id_1 = index.get_i64();
                assert_eq!(0, block_id_1);
                let task_id = index.get_i64();
                assert_eq!(0, task_id);

                let offset_2 = index.get_i64();
                assert_eq!(size as i64, offset_2);
                assert_eq!(size as i32, index.get_i32());
            }
        }

        temp_dir.close().unwrap();
    }

    #[test]
    fn test_index_consistency() -> anyhow::Result<()> {
        let temp_dir = tempdir::TempDir::new("test_index_consistency").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        info!("init local file path: {}", temp_path);

        let mut raw_bytes = BytesMut::new();
        IndexCodec::encode(
            &IndexBlock {
                offset: 0,
                length: 10,
                uncompress_length: 0,
                crc: 0,
                block_id: 0,
                task_attempt_id: 0,
            },
            &mut raw_bytes,
        )?;
        let data_file_len = 10;

        let raw_bytes = raw_bytes.freeze();

        // case1: legal pass
        assert_eq!(
            true,
            LocalFileStore::detect_index_inconsistency(
                &raw_bytes,
                data_file_len,
                &"/".to_owned(),
                &"i.1".to_owned(),
                &"d.1".to_owned()
            )?
        );

        // case2: Illegal
        let data_file_len = 9;
        // create the index file in the dir of temp_path
        let index_file_path = "app-1/patition-1.index";
        let data_file_path = "app-1/partition-1.data";

        let abs_index_file_path = format!("{}/{}", &temp_path, index_file_path);
        let abs_data_file_path = format!("{}/{}", &temp_path, data_file_path);
        // create the empty file for the abs_index_file_path. empty file
        std::fs::create_dir_all(Path::new(&abs_index_file_path).parent().unwrap())?;
        std::fs::create_dir_all(Path::new(&abs_data_file_path).parent().unwrap())?;
        std::fs::write(&abs_index_file_path, &raw_bytes)?;
        std::fs::write(&abs_data_file_path, &raw_bytes)?;

        assert_eq!(
            false,
            LocalFileStore::detect_index_inconsistency(
                &raw_bytes,
                data_file_len,
                &temp_path,
                &index_file_path.to_owned(),
                &data_file_path.to_owned()
            )?
        );

        Ok(())
    }
}
