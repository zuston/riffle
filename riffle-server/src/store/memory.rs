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

use crate::app_manager::request_context::ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE;
use crate::app_manager::request_context::{
    PurgeDataContext, ReadingIndexViewContext, ReadingViewContext, RegisterAppContext,
    ReleaseTicketContext, RequireBufferContext, WritingViewContext,
};
use crate::config::{MemoryStoreConfig, StorageType};
use crate::error::WorkerError;
use crate::metric::{RPC_BATCH_BYTES_OPERATION, RPC_BATCH_DATA_BYTES_HISTOGRAM, TOTAL_MEMORY_USED};
use crate::store::{Block, RequireBufferResponse, ResponseData, ResponseDataIndex, Store};
use crate::*;
use async_trait::async_trait;
use dashmap::DashMap;

use std::collections::{BTreeMap, HashMap};
use std::hash::BuildHasherDefault;

use std::str::FromStr;

use crate::app_manager::partition_identifier::PartitionUId;
use crate::ddashmap::DDashMap;
use crate::runtime::manager::RuntimeManager;
use crate::store::mem::budget::MemoryBudget;
use crate::store::mem::buffer::default_buffer::DefaultMemoryBuffer;
use crate::store::mem::buffer::opt_buffer::OptStagingMemoryBuffer;
use crate::store::mem::buffer::{BufferOptions, BufferType, MemoryBuffer};
use crate::store::mem::capacity::CapacitySnapshot;
use crate::store::mem::ticket::TicketManager;
use crate::store::spill::SpillWritingViewContext;
use anyhow::anyhow;
use anyhow::Result;
use bytesize::ByteSize;
use croaring::Treemap;
use fastrace::trace;
use fxhash::{FxBuildHasher, FxHasher};
use log::{debug, info, warn};
use std::sync::Arc;

pub struct MemoryStore<B: MemoryBuffer + Send + Sync + 'static = DefaultMemoryBuffer> {
    memory_capacity: i64,
    state: DDashMap<PartitionUId, Arc<B>>,
    budget: MemoryBudget,
    runtime_manager: RuntimeManager,
    ticket_manager: TicketManager,
    cfg: Option<MemoryStoreConfig>,
}

unsafe impl<B: MemoryBuffer + Send + Sync> Send for MemoryStore<B> {}
unsafe impl<B: MemoryBuffer + Send + Sync> Sync for MemoryStore<B> {}

impl<B: MemoryBuffer + Send + Sync + 'static> MemoryStore<B> {
    // only for test cases
    pub fn new(max_memory_size: i64) -> Self {
        let budget = MemoryBudget::new(max_memory_size);
        let runtime_manager: RuntimeManager = Default::default();

        let budget_clone = budget.clone();
        let release_allocated_func =
            move |size: i64| budget_clone.dec_allocated(size).map_or(false, |v| v);

        let ticket_manager =
            TicketManager::new(5 * 60, 10, release_allocated_func, runtime_manager.clone());
        MemoryStore {
            budget,
            state: DDashMap::default(),
            memory_capacity: max_memory_size,
            ticket_manager,
            runtime_manager,
            cfg: None,
        }
    }

    pub fn from(conf: MemoryStoreConfig, runtime_manager: RuntimeManager) -> Self {
        let capacity = ByteSize::from_str(&conf.capacity).unwrap();
        let budget = MemoryBudget::new(capacity.as_u64() as i64);

        let budget_clone = budget.clone();
        let release_allocated_func =
            move |size: i64| budget_clone.dec_allocated(size).map_or(false, |v| v);

        let ticket_manager = TicketManager::new(
            conf.buffer_ticket_timeout_sec,
            conf.buffer_ticket_check_interval_sec,
            release_allocated_func,
            runtime_manager.clone(),
        );

        MemoryStore {
            state: DDashMap::default(),
            budget: MemoryBudget::new(capacity.as_u64() as i64),
            memory_capacity: capacity.as_u64() as i64,
            ticket_manager,
            runtime_manager,
            cfg: Some(conf),
        }
    }

    pub fn memory_snapshot(&self) -> Result<CapacitySnapshot> {
        Ok(self.budget.snapshot())
    }

    pub fn get_capacity(&self) -> Result<i64> {
        Ok(self.memory_capacity)
    }

    // only for tests
    pub fn inc_used(&self, size: i64) -> Result<bool> {
        self.budget.inc_used(size)
    }

    fn dec_used(&self, size: i64) -> Result<bool> {
        self.budget.dec_used(size)
    }

    pub fn dec_allocated(&self, size: i64) -> Result<bool> {
        self.budget.dec_allocated(size)
    }

    pub fn move_allocated_to_used(&self, size: i64) -> Result<bool> {
        self.budget.move_allocated_to_used(size)
    }

    pub fn lookup_spill_buffers(
        &self,
        expected_spill_total_bytes: i64,
    ) -> Result<HashMap<PartitionUId, Arc<B>>, anyhow::Error> {
        // 1. sort by the staging size.
        // 2. get the spill buffers until reaching the single max batch size

        let mut sorted_tree_map = BTreeMap::new();

        let buffers = self.state.clone().into_read_only();
        for buffer in buffers.iter() {
            let key = buffer.0;
            let memory_buf = buffer.1;
            let staging_size = memory_buf.staging_size()?;
            if staging_size == 0 {
                continue;
            }
            let valset = sorted_tree_map
                .entry(staging_size)
                .or_insert_with(|| vec![]);
            valset.push(key);
        }

        let mut real_spill_total_bytes = 0;
        let mut spill_candidates = HashMap::new();

        let iter = sorted_tree_map.iter().rev();
        'outer: for (size, vals) in iter {
            for pid in vals {
                if real_spill_total_bytes >= expected_spill_total_bytes {
                    break 'outer;
                }
                let partition_uid = (*pid).clone();
                let buffer = self.get_buffer(*pid);
                if buffer.is_err() {
                    continue;
                }
                real_spill_total_bytes += *size;
                spill_candidates.insert(partition_uid, buffer?);
            }
        }

        info!(
            "[Spill] Candidate spill bytes. excepted/real: {}/{}",
            &expected_spill_total_bytes, &real_spill_total_bytes
        );
        Ok(spill_candidates)
    }

    pub fn get_buffer_size(&self, uid: &PartitionUId) -> Result<u64> {
        let buffer = self.get_buffer(uid)?;
        Ok(buffer.total_size()? as u64)
    }

    pub fn get_buffer_staging_size(&self, uid: &PartitionUId) -> Result<u64> {
        let buffer = self.get_buffer(uid)?;
        Ok(buffer.staging_size()? as u64)
    }

    pub async fn clear_spilled_buffer(
        &self,
        uid: PartitionUId,
        flight_id: u64,
        flight_len: u64,
    ) -> Result<()> {
        let buffer = self.get_buffer(&uid)?;
        buffer.clear(flight_id, flight_len)?;
        self.dec_used(flight_len as i64)?;
        Ok(())
    }

    // only invoked when inserting
    pub fn get_or_create_buffer(&self, uid: PartitionUId) -> Arc<B> {
        let buf_opts = BufferOptions {
            buffer_type: self
                .cfg
                .as_ref()
                .map(|x| x.buffer_type)
                .unwrap_or(BufferType::DEFAULT),
        };
        self.state
            .compute_if_absent(uid, || Arc::new(B::new(buf_opts)))
    }

    pub fn get_buffer(&self, uid: &PartitionUId) -> Result<Arc<B>> {
        let buffer = self.state.get(uid);
        if buffer.is_none() {
            return Err(anyhow!(format!(
                "No such existing buffer for: {:?}. This may has been deleted.",
                uid
            )));
        }
        Ok(buffer.unwrap().clone())
    }

    pub(crate) fn read_partial_data_with_max_size_limit_and_filter<'a>(
        &'a self,
        blocks: Vec<&'a Block>,
        fetched_size_limit: i64,
        serialized_expected_task_ids_bitmap: Option<Treemap>,
    ) -> (Vec<&'a Block>, i64) {
        let mut fetched = vec![];
        let mut fetched_size = 0;

        for block in blocks {
            if let Some(ref filter) = serialized_expected_task_ids_bitmap {
                if !filter.contains(block.task_attempt_id as u64) {
                    continue;
                }
            }
            if fetched_size >= fetched_size_limit {
                break;
            }
            fetched_size += block.length as i64;
            fetched.push(block);
        }

        (fetched, fetched_size)
    }
}

#[async_trait]
impl<B: MemoryBuffer + Send + Sync + 'static> Store for MemoryStore<B> {
    fn start(self: Arc<Self>) {
        // ignore
    }
    #[trace]
    async fn insert(&self, ctx: WritingViewContext) -> Result<(), WorkerError> {
        let uid = ctx.uid;
        let blocks = ctx.data_blocks;
        let size = ctx.data_size;

        let buffer = self.get_or_create_buffer(uid);
        buffer.append(blocks, ctx.data_size)?;

        Ok(())
    }

    #[trace]
    async fn get(&self, ctx: ReadingViewContext) -> Result<ResponseData, WorkerError> {
        let uid = ctx.uid;
        let buffer = self.get_buffer(&uid)?;
        let options = ctx.reading_options;
        let read_data = match options {
            MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(last_block_id, max_size) => {
                buffer.get(last_block_id, max_size, ctx.task_ids_filter)?
            }
            _ => panic!("Should not happen."),
        };
        RPC_BATCH_DATA_BYTES_HISTOGRAM
            .with_label_values(&[&RPC_BATCH_BYTES_OPERATION::MEMORY_GET_DATA.to_string()])
            .observe(read_data.data.len() as f64);

        Ok(ResponseData::Mem(read_data))
    }

    #[trace]
    async fn get_index(
        &self,
        _ctx: ReadingIndexViewContext,
    ) -> Result<ResponseDataIndex, WorkerError> {
        panic!("It should not be invoked.")
    }

    #[trace]
    async fn purge(&self, ctx: &PurgeDataContext) -> Result<i64> {
        let (app_id, shuffle_id_option) = ctx.extract();

        // remove the corresponding app's data
        let read_only_state_view = self.state.clone().into_read_only();
        let mut _removed_list = vec![];
        for entry in read_only_state_view.iter() {
            let pid = entry.0;
            if pid.app_id == app_id {
                if shuffle_id_option.is_some() {
                    if pid.shuffle_id == shuffle_id_option.unwrap() {
                        _removed_list.push(pid);
                    } else {
                        continue;
                    }
                } else {
                    _removed_list.push(pid);
                }
            }
        }

        let mut used = 0;
        for removed_pid in _removed_list {
            if let Some(entry) = self.state.remove(removed_pid) {
                used += entry.1.total_size()?;
            }
        }

        // free used
        self.budget.dec_used(used)?;

        info!(
            "Removed used memory buffer size:[{}] for [{}], [{:?}]",
            used, &app_id, shuffle_id_option
        );

        Ok(used)
    }

    #[trace]
    async fn is_healthy(&self) -> Result<bool> {
        Ok(true)
    }

    #[trace]
    async fn require_buffer(
        &self,
        ctx: RequireBufferContext,
    ) -> Result<RequireBufferResponse, WorkerError> {
        let (succeed, ticket_id) = self.budget.require_allocated(ctx.size)?;
        debug!(
            "gotten the requirement: {:?} for uid: {:?}",
            succeed, &ctx.uid
        );
        match succeed {
            true => {
                let require_buffer_resp = RequireBufferResponse::new(ticket_id);
                self.ticket_manager.insert(
                    ticket_id,
                    ctx.size,
                    require_buffer_resp.allocated_timestamp,
                    &ctx.uid.app_id,
                );
                debug!("Inserted into the ticket for uid: {:?}", &ctx.uid);
                Ok(require_buffer_resp)
            }
            _ => Err(WorkerError::NO_ENOUGH_MEMORY_TO_BE_ALLOCATED),
        }
    }

    #[trace]
    async fn release_ticket(&self, ctx: ReleaseTicketContext) -> Result<i64, WorkerError> {
        let ticket_id = ctx.ticket_id;
        self.ticket_manager.delete(ticket_id)
    }

    #[trace]
    fn register_app(&self, _ctx: RegisterAppContext) -> Result<()> {
        Ok(())
    }

    #[trace]
    async fn name(&self) -> StorageType {
        StorageType::MEMORY
    }

    async fn spill_insert(&self, _ctx: SpillWritingViewContext) -> Result<(), WorkerError> {
        todo!()
    }

    async fn pre_check(&self) -> Result<(), WorkerError> {
        Ok(())
    }
}

pub struct MemorySnapshot {
    capacity: i64,
    allocated: i64,
    used: i64,
}

impl From<(i64, i64, i64)> for MemorySnapshot {
    fn from(value: (i64, i64, i64)) -> Self {
        MemorySnapshot {
            capacity: value.0,
            allocated: value.1,
            used: value.2,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::app_manager::request_context::{
        PurgeDataContext, ReadingOptions, ReadingViewContext, RequireBufferContext, RpcType,
        WritingViewContext,
    };

    use crate::store::memory::MemoryStore;
    use crate::store::ResponseData::Mem;

    use crate::store::{Block, PartitionedMemoryData, ResponseData, Store};

    use bytes::BytesMut;
    use core::panic;
    use std::sync::Arc;

    use crate::app_manager::application_identifier::ApplicationId;
    use crate::app_manager::partition_identifier::PartitionUId;
    use crate::app_manager::purge_event::PurgeReason;
    use crate::store::mem::buffer::default_buffer::DefaultMemoryBuffer;
    use crate::store::mem::buffer::opt_buffer::OptStagingMemoryBuffer;
    use crate::store::mem::buffer::MemoryBuffer;
    use anyhow::Result;
    use croaring::Treemap;

    fn run_test_read_buffer_in_flight<B: MemoryBuffer + Send + Sync + 'static>() {
        let store: MemoryStore<B> = MemoryStore::new(1024);
        let runtime = store.runtime_manager.clone();

        let uid = PartitionUId::new(&Default::default(), 0, 0);
        let writing_view_ctx = create_writing_ctx_with_blocks(10, 10, uid.clone());
        let _ = runtime.wait(store.insert(writing_view_ctx));

        let default_single_read_size = 20;

        // case1: read from -1
        let mem_data = runtime.wait(get_data_with_last_block_id(
            default_single_read_size,
            -1,
            &store,
            uid.clone(),
        ));
        assert_eq!(2, mem_data.shuffle_data_block_segments.len());
        assert_eq!(
            0,
            mem_data
                .shuffle_data_block_segments
                .get(0)
                .unwrap()
                .block_id
        );
        assert_eq!(
            1,
            mem_data
                .shuffle_data_block_segments
                .get(1)
                .unwrap()
                .block_id
        );

        // case2: when the last_block_id doesn't exist, it should return the data like when last_block_id=-1
        let mem_data = runtime.wait(get_data_with_last_block_id(
            default_single_read_size,
            100,
            &store,
            uid.clone(),
        ));
        assert_eq!(2, mem_data.shuffle_data_block_segments.len());
        assert_eq!(
            0,
            mem_data
                .shuffle_data_block_segments
                .get(0)
                .unwrap()
                .block_id
        );
        assert_eq!(
            1,
            mem_data
                .shuffle_data_block_segments
                .get(1)
                .unwrap()
                .block_id
        );

        // case3: read from 3
        let mem_data = runtime.wait(get_data_with_last_block_id(
            default_single_read_size,
            3,
            &store,
            uid.clone(),
        ));
        assert_eq!(2, mem_data.shuffle_data_block_segments.len());
        assert_eq!(
            4,
            mem_data
                .shuffle_data_block_segments
                .get(0)
                .unwrap()
                .block_id
        );
        assert_eq!(
            5,
            mem_data
                .shuffle_data_block_segments
                .get(1)
                .unwrap()
                .block_id
        );

        // // case4: some data are in inflight blocks
        // let buffer = store.get_or_create_underlying_staging_buffer(uid.clone());
        // let owned = buffer.staging.to_owned();
        // buffer.staging.clear();
        // let mut idx = 0;
        // for block in owned {
        //     buffer.in_flight.insert(idx, vec![block]);
        //     idx += 1;
        // }
        // drop(buffer);
        //
        // // all data will be fetched from in_flight data
        // let mem_data = runtime.wait(get_data_with_last_block_id(
        //     default_single_read_size,
        //     3,
        //     &store,
        //     uid.clone(),
        // ));
        // assert_eq!(2, mem_data.shuffle_data_block_segments.len());
        // assert_eq!(
        //     4,
        //     mem_data
        //         .shuffle_data_block_segments
        //         .get(0)
        //         .unwrap()
        //         .block_id
        // );
        // assert_eq!(
        //     5,
        //     mem_data
        //         .shuffle_data_block_segments
        //         .get(1)
        //         .unwrap()
        //         .block_id
        // );
        //
        // // case5: old data in in_flight and latest data in staging.
        // // read it from the block id 9, and read size of 30
        // let buffer = store.get_or_create_underlying_staging_buffer(uid.clone());
        // let mut buffer = buffer.lock();
        // buffer.staging.push(PartitionedDataBlock {
        //     block_id: 20,
        //     length: 10,
        //     uncompress_length: 0,
        //     crc: 0,
        //     data: BytesMut::with_capacity(10).freeze(),
        //     task_attempt_id: 0,
        // });
        // drop(buffer);
        //
        // let mem_data = runtime.wait(get_data_with_last_block_id(30, 7, &store, uid.clone()));
        // assert_eq!(3, mem_data.shuffle_data_block_segments.len());
        // assert_eq!(
        //     8,
        //     mem_data
        //         .shuffle_data_block_segments
        //         .get(0)
        //         .unwrap()
        //         .block_id
        // );
        // assert_eq!(
        //     9,
        //     mem_data
        //         .shuffle_data_block_segments
        //         .get(1)
        //         .unwrap()
        //         .block_id
        // );
        // assert_eq!(
        //     20,
        //     mem_data
        //         .shuffle_data_block_segments
        //         .get(2)
        //         .unwrap()
        //         .block_id
        // );
        //
        // // case6: read the end to return empty result
        // let mem_data = runtime.wait(get_data_with_last_block_id(30, 20, &store, uid.clone()));
        // assert_eq!(0, mem_data.shuffle_data_block_segments.len());
    }

    #[test]
    fn test_read_buffer_in_flight() {
        run_test_read_buffer_in_flight::<DefaultMemoryBuffer>();
        run_test_read_buffer_in_flight::<OptStagingMemoryBuffer>();
    }

    async fn get_data_with_last_block_id<B: MemoryBuffer + Send + Sync + 'static>(
        default_single_read_size: i64,
        last_block_id: i64,
        store: &MemoryStore<B>,
        uid: PartitionUId,
    ) -> PartitionedMemoryData {
        let ctx = ReadingViewContext::new(
            uid.clone(),
            ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(
                last_block_id,
                default_single_read_size,
            ),
            RpcType::GRPC,
        );
        if let Ok(data) = store.get(ctx).await {
            match data {
                Mem(mem_data) => mem_data,
                _ => panic!(),
            }
        } else {
            panic!();
        }
    }

    fn create_writing_ctx_with_blocks(
        _block_number: i32,
        single_block_size: i32,
        uid: PartitionUId,
    ) -> WritingViewContext {
        let mut data_blocks = vec![];
        for idx in 0..=9 {
            data_blocks.push(Block {
                block_id: idx,
                length: single_block_size.clone(),
                uncompress_length: 0,
                crc: 0,
                data: BytesMut::with_capacity(single_block_size as usize).freeze(),
                task_attempt_id: 0,
            });
        }
        WritingViewContext::create_for_test(uid, data_blocks)
    }

    fn run_test_allocated_and_purge_for_memory<B: MemoryBuffer + Send + Sync + 'static>() {
        let store: MemoryStore<B> = MemoryStore::new(1024 * 1024 * 1024);
        let runtime = store.runtime_manager.clone();

        let app_id = ApplicationId::from("application_1_1_2");
        let ctx = RequireBufferContext {
            uid: PartitionUId::new(&app_id, 0, 0),
            size: 10000,
            partition_ids: vec![],
        };
        match runtime.default_runtime.block_on(store.require_buffer(ctx)) {
            Ok(_) => {
                let _ = runtime
                    .default_runtime
                    .block_on(store.purge(&PurgeDataContext {
                        purge_reason: PurgeReason::APP_LEVEL_EXPLICIT_UNREGISTER(app_id),
                    }));
            }
            _ => panic!(),
        }

        let snapshot = store.budget.snapshot();
        assert_eq!(0, snapshot.used());
        assert_eq!(1024 * 1024 * 1024, snapshot.capacity());
    }

    #[test]
    fn test_allocated_and_purge_for_memory() {
        run_test_allocated_and_purge_for_memory::<DefaultMemoryBuffer>();
        run_test_allocated_and_purge_for_memory::<OptStagingMemoryBuffer>();
    }

    fn run_test_purge<B: MemoryBuffer + Send + Sync + 'static>() -> Result<()> {
        let store: MemoryStore<B> = MemoryStore::new(1024);
        let runtime = store.runtime_manager.clone();

        let shuffle_id = 1;
        let partition = 1;

        let app_id = ApplicationId::from("application_0_1_1");
        let uid = PartitionUId::new(&app_id, 0, 0);

        // the buffer requested

        let _buffer = runtime
            .wait(store.require_buffer(RequireBufferContext::create_for_test(uid.clone(), 40)))
            .expect("");

        let writing_ctx = WritingViewContext::create_for_test(
            uid.clone(),
            vec![Block {
                block_id: 0,
                length: 10,
                uncompress_length: 100,
                crc: 99,
                data: Default::default(),
                task_attempt_id: 0,
            }],
        );
        runtime.wait(store.insert(writing_ctx)).expect("");

        let reading_ctx = ReadingViewContext::new(
            uid.clone(),
            ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(-1, 1000000),
            RpcType::GRPC,
        );
        let data = runtime.wait(store.get(reading_ctx.clone())).expect("");
        assert_eq!(1, data.from_memory().shuffle_data_block_segments.len());

        // get weak reference to ensure purge can successfully free memory
        let weak_ref_before = store
            .state
            .get(&uid)
            .map(|entry| Arc::downgrade(&entry.value()));
        assert!(
            weak_ref_before.is_some(),
            "Failed to obtain weak reference before purge"
        );

        // partial purge for app's one shuffle data
        runtime
            .wait(store.purge(&PurgeDataContext::new(
                &PurgeReason::SHUFFLE_LEVEL_EXPLICIT_UNREGISTER(app_id.to_owned(), shuffle_id),
            )))
            .expect("");
        assert!(!store
            .state
            .contains_key(&PartitionUId::new(&app_id, shuffle_id, partition)));

        // purge
        runtime
            .wait(store.purge(&PurgeDataContext::new(
                &PurgeReason::APP_LEVEL_EXPLICIT_UNREGISTER(app_id.to_owned()),
            )))
            .expect("");
        assert!(
            weak_ref_before.clone().unwrap().upgrade().is_none(),
            "Arc should not exist after purge"
        );
        let snapshot = store.budget.snapshot();
        assert_eq!(snapshot.used(), 0);
        assert_eq!(snapshot.capacity(), 1024);
        let data = runtime.wait(store.get(reading_ctx.clone()));
        if let Ok(_) = data {
            panic!();
        }
        Ok(())
    }

    #[test]
    fn test_purge() {
        run_test_purge::<DefaultMemoryBuffer>();
        run_test_purge::<OptStagingMemoryBuffer>();
    }

    fn run_test_put_and_get_for_memory<B: MemoryBuffer + Send + Sync + 'static>() {
        let store: MemoryStore<B> = MemoryStore::new(1024 * 1024 * 1024);
        let runtime = store.runtime_manager.clone();

        let writing_ctx = WritingViewContext::create_for_test(
            Default::default(),
            vec![
                Block {
                    block_id: 0,
                    length: 10,
                    uncompress_length: 100,
                    crc: 99,
                    data: Default::default(),
                    task_attempt_id: 0,
                },
                Block {
                    block_id: 1,
                    length: 20,
                    uncompress_length: 200,
                    crc: 99,
                    data: Default::default(),
                    task_attempt_id: 1,
                },
            ],
        );
        runtime.wait(store.insert(writing_ctx)).unwrap();

        let reading_ctx = ReadingViewContext::new(
            Default::default(),
            ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(-1, 1000000),
            RpcType::GRPC,
        );

        match runtime.wait(store.get(reading_ctx)).unwrap() {
            ResponseData::Mem(data) => {
                assert_eq!(data.shuffle_data_block_segments.len(), 2);
                assert_eq!(data.shuffle_data_block_segments.get(0).unwrap().offset, 0);
                assert_eq!(data.shuffle_data_block_segments.get(1).unwrap().offset, 10);
            }
            _ => panic!("should not"),
        }
    }

    #[test]
    fn test_put_and_get_for_memory() {
        run_test_put_and_get_for_memory::<DefaultMemoryBuffer>();
        run_test_put_and_get_for_memory::<OptStagingMemoryBuffer>();
    }

    fn run_test_block_id_filter_for_memory<B: MemoryBuffer + Send + Sync + 'static>() {
        let store: MemoryStore<B> = MemoryStore::new(1024 * 1024 * 1024);
        let runtime = store.runtime_manager.clone();

        // 1. insert 2 block
        let writing_ctx = WritingViewContext::create_for_test(
            Default::default(),
            vec![
                Block {
                    block_id: 0,
                    length: 10,
                    uncompress_length: 100,
                    crc: 99,
                    data: Default::default(),
                    task_attempt_id: 0,
                },
                Block {
                    block_id: 1,
                    length: 20,
                    uncompress_length: 200,
                    crc: 99,
                    data: Default::default(),
                    task_attempt_id: 1,
                },
            ],
        );
        runtime.wait(store.insert(writing_ctx)).unwrap();

        // 2. block_ids_filter is empty, should return 2 blocks
        let mut reading_ctx = ReadingViewContext::new(
            Default::default(),
            ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(-1, 1000000),
            RpcType::GRPC,
        );

        match runtime.wait(store.get(reading_ctx)).unwrap() {
            Mem(data) => {
                assert_eq!(data.shuffle_data_block_segments.len(), 2);
            }
            _ => panic!("should not"),
        }

        // 3. set serialized_expected_task_ids_bitmap, and set last_block_id equals 1, should return 1 block
        let mut bitmap = Treemap::default();
        bitmap.add(1);
        reading_ctx = ReadingViewContext::new(
            Default::default(),
            ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(0, 1000000),
            RpcType::GRPC,
        );

        match runtime.wait(store.get(reading_ctx)).unwrap() {
            Mem(data) => {
                assert_eq!(data.shuffle_data_block_segments.len(), 1);
                assert_eq!(data.shuffle_data_block_segments.get(0).unwrap().offset, 0);
                assert_eq!(
                    data.shuffle_data_block_segments
                        .get(0)
                        .unwrap()
                        .uncompress_length,
                    200
                );
            }
            _ => panic!("should not"),
        }
    }
    #[test]
    fn test_block_id_filter_for_memory() {
        run_test_block_id_filter_for_memory::<DefaultMemoryBuffer>();
        run_test_block_id_filter_for_memory::<OptStagingMemoryBuffer>();
    }
}
