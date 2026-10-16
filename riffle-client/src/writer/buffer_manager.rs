use std::collections::{hash_map::Entry, HashMap};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use bytes::Bytes;
use riffle_server::grpc::protobuf::uniffle::ShuffleBlock;
use riffle_server::id_layout::DEFAULT_BLOCK_ID_LAYOUT;
use riffle_server::util::get_crc;
use tokio::time::timeout;

use super::buffer::{Buffer, INITIAL_CAPACITY_BYTES};
use super::data_pusher::DataPusher;
use super::memory_tracker::{AcquireResult, MemoryReservation, MemoryTracker};
use super::BufferedBlock;
use crate::{PartitionId, ShuffleAssignment, TaskId, BLOCK_SEQUENCE_LIMIT};

const APPEND_TIMEOUT: Duration = Duration::from_secs(10 * 60);

pub struct BufferManager {
    buffers: HashMap<PartitionId, Buffer>,
    pusher: Arc<DataPusher>,
    memory: MemoryTracker,
    assignment: Arc<ShuffleAssignment>,
    task_id: TaskId,
    sequence_counter: u32,
    block_ids: Vec<i64>,
}

impl BufferManager {
    pub fn new(
        task_id: TaskId,
        assignment: Arc<ShuffleAssignment>,
        memory: MemoryTracker,
        pusher: Arc<DataPusher>,
    ) -> Self {
        Self {
            buffers: HashMap::new(),
            pusher,
            memory,
            assignment,
            task_id,
            sequence_counter: 0,
            block_ids: Vec::new(),
        }
    }

    fn get_buffer(&mut self, partition_id: PartitionId) -> Result<Option<&mut Buffer>> {
        match self.buffers.entry(partition_id) {
            Entry::Occupied(entry) => Ok(Some(entry.into_mut())),
            Entry::Vacant(entry) => match self.memory.try_acquire(INITIAL_CAPACITY_BYTES)? {
                AcquireResult::Acquired(memory) => Ok(Some(entry.insert(Buffer::new(memory)))),
                AcquireResult::ExceededThreshold => Ok(None),
            },
        }
    }

    pub async fn append(&mut self, partition_id: PartitionId, data: Bytes) -> Result<()> {
        timeout(APPEND_TIMEOUT, self.append_inner(partition_id, data))
            .await
            .context("append timed out")?
    }

    async fn append_inner(&mut self, partition_id: PartitionId, data: Bytes) -> Result<()> {
        let memory = self.memory.clone();
        loop {
            let released = memory.released();
            self.pusher.check_failure()?;
            if let Some(buffer) = self.get_buffer(partition_id)? {
                if buffer.append(&data)? {
                    if buffer.is_full() {
                        let buffer = self.buffers.remove(&partition_id).unwrap();
                        let (data, memory) = buffer.clear().unwrap();
                        let block = self.create_block(partition_id, data, memory)?;
                        self.pusher.add(vec![block])?;
                    }
                    return Ok(());
                }
            }

            // Release empty reservations left by cancelled appends to other partitions.
            self.buffers
                .retain(|&partition, buffer| partition == partition_id || !buffer.is_empty());
            let blocks = self.clear()?;
            debug_assert!(self.buffers.values().all(Buffer::is_empty));
            if !blocks.is_empty() {
                self.pusher.add(blocks)?;
                // A fresh buffer may fit without waiting for the flushed blocks to finish.
                continue;
            }
            tokio::select! {
                biased;
                error = self.pusher.failed() => return Err(error),
                _ = released => {}
            }
        }
    }

    // todo: more fine-grained flushing policy
    pub fn clear(&mut self) -> Result<Vec<BufferedBlock>> {
        let partitions: Vec<_> = self
            .buffers
            .iter()
            .filter(|(_, buffer)| !buffer.is_empty())
            .map(|(&partition_id, _)| partition_id)
            .collect();
        partitions
            .into_iter()
            .map(|partition_id| {
                let buffer = self.buffers.remove(&partition_id).unwrap();
                let (data, memory) = buffer.clear().unwrap();
                self.create_block(partition_id, data, memory)
            })
            .collect()
    }

    pub fn into_block_ids(self) -> Vec<i64> {
        self.block_ids
    }

    fn create_block(
        &mut self,
        partition_id: PartitionId,
        data: Bytes,
        memory: MemoryReservation,
    ) -> Result<BufferedBlock> {
        ensure!(
            self.sequence_counter < BLOCK_SEQUENCE_LIMIT,
            "block sequence limit reached"
        );
        let endpoint = self.assignment.get_endpoint(partition_id)?;
        let block_id = DEFAULT_BLOCK_ID_LAYOUT.get_block_id(
            self.sequence_counter.into(),
            partition_id as i64,
            self.task_id as i64,
        );
        let block = ShuffleBlock {
            block_id,
            length: data.len() as i32,
            uncompress_length: data.len() as i32,
            crc: get_crc(&data),
            data,
            task_attempt_id: self.task_id as i64,
        };
        self.sequence_counter += 1;
        self.block_ids.push(block_id);
        Ok(BufferedBlock {
            endpoint,
            partition_id,
            block,
            memory,
        })
    }
}
