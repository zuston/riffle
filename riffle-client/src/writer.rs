// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::write_client::{PreparedBlock, WriteClient};
use crate::{
    BlockId, BlockPayload, MapOutput, PartitionId, RiffleError, ShuffleHandle, ShuffleServer,
    ShuffleWriterConfig, TaskAttemptId,
};
#[cfg(test)]
use bytes::Bytes;
use futures::future::join_all;
use riffle_proto::uniffle::{PartitionStats, PartitionToBlockIds, TaskAttemptIdToRecords};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct ShuffleWriter {
    inner: Arc<WriterInner>,
}

struct WriterInner {
    handle: ShuffleHandle,
    config: ShuffleWriterConfig,
    clients: BTreeMap<ShuffleServer, WriteClient>,
    inflight_bytes: Arc<Semaphore>,
    inflight_requests: Arc<Semaphore>,
    used_attempts: Mutex<HashSet<TaskAttemptId>>,
}

impl ShuffleWriter {
    pub fn from_handle(
        handle: ShuffleHandle,
        config: ShuffleWriterConfig,
    ) -> Result<Self, RiffleError> {
        handle.validate()?;
        config.validate()?;
        if handle.data_replica != 1 {
            return Err(RiffleError::Unsupported(
                "the initial ShuffleWriter supports one replica only".to_string(),
            ));
        }
        let clients = handle
            .servers()
            .into_iter()
            .map(|server| {
                let client = WriteClient::new(&server, &handle, &config)?;
                Ok((server, client))
            })
            .collect::<Result<BTreeMap<_, _>, RiffleError>>()?;
        Ok(Self {
            inner: Arc::new(WriterInner {
                inflight_bytes: Arc::new(Semaphore::new(config.max_inflight_bytes)),
                inflight_requests: Arc::new(Semaphore::new(config.max_inflight_requests)),
                clients,
                handle,
                config,
                used_attempts: Mutex::new(HashSet::new()),
            }),
        })
    }

    pub fn handle(&self) -> &ShuffleHandle {
        &self.inner.handle
    }

    pub fn open_attempt(&self, task_attempt_id: TaskAttemptId) -> Result<PushAttempt, RiffleError> {
        if task_attempt_id.value() > self.inner.handle.block_id_layout.max_task_attempt_id() {
            return Err(RiffleError::InvalidArgument(format!(
                "task attempt id {task_attempt_id} exceeds the block id layout capacity"
            )));
        }
        let mut attempts = self.inner.used_attempts.lock().map_err(|_| {
            RiffleError::InvalidArgument("writer attempt registry is poisoned".to_string())
        })?;
        if !attempts.insert(task_attempt_id) {
            return Err(RiffleError::InvalidArgument(format!(
                "task attempt id {task_attempt_id} was already opened by this writer"
            )));
        }
        drop(attempts);
        Ok(PushAttempt {
            writer: self.clone(),
            task_attempt_id,
            next_sequences: BTreeMap::new(),
            reported_blocks: BTreeMap::new(),
            record_counts: BTreeMap::new(),
            blocks_written: 0,
            bytes_written: 0,
            failed: false,
            finished: false,
        })
    }
}

pub struct PushAttempt {
    writer: ShuffleWriter,
    task_attempt_id: TaskAttemptId,
    next_sequences: BTreeMap<PartitionId, u64>,
    reported_blocks: BTreeMap<ShuffleServer, BTreeMap<PartitionId, Vec<BlockId>>>,
    record_counts: BTreeMap<PartitionId, Option<u64>>,
    blocks_written: u64,
    bytes_written: u64,
    failed: bool,
    finished: bool,
}

impl PushAttempt {
    pub fn task_attempt_id(&self) -> TaskAttemptId {
        self.task_attempt_id
    }

    pub async fn push(
        &mut self,
        partition_id: PartitionId,
        blocks: Vec<BlockPayload>,
    ) -> Result<Vec<BlockId>, RiffleError> {
        self.ensure_active()?;
        if blocks.is_empty() {
            return Ok(Vec::new());
        }
        let route = self.writer.inner.handle.route_for(partition_id)?.clone();
        let server = route.replicas.first().cloned().ok_or_else(|| {
            RiffleError::InvalidAssignment(format!(
                "partition {partition_id} has no shuffle server"
            ))
        })?;
        let prepared = match self.prepare_blocks(partition_id, blocks) {
            Ok(prepared) => prepared,
            Err(error) => {
                self.failed = true;
                return Err(error);
            }
        };
        let batches = match split_batches(&prepared, self.writer.inner.config.max_batch_bytes) {
            Ok(batches) => batches,
            Err(error) => {
                self.failed = true;
                return Err(error);
            }
        };
        let next_stats = match self.next_stats(partition_id, &prepared) {
            Ok(next_stats) => next_stats,
            Err(error) => {
                self.failed = true;
                return Err(error);
            }
        };

        for batch in batches {
            if let Err(error) = self.send_batch(&server, partition_id, batch).await {
                self.failed = true;
                return Err(error);
            }
        }

        let block_ids = prepared
            .iter()
            .map(|block| block.block_id)
            .collect::<Vec<_>>();
        self.reported_blocks
            .entry(server)
            .or_default()
            .entry(partition_id)
            .or_default()
            .extend(block_ids.iter().copied());
        self.blocks_written = next_stats.blocks_written;
        self.bytes_written = next_stats.bytes_written;
        self.record_counts
            .insert(partition_id, next_stats.partition_records);

        Ok(block_ids)
    }

    pub async fn finish(mut self) -> Result<MapOutput, RiffleError> {
        self.ensure_active()?;
        let records_written = match total_record_counts(&self.record_counts) {
            Ok(records_written) => records_written,
            Err(error) => {
                self.failed = true;
                return Err(error);
            }
        };
        let report_futures = self
            .reported_blocks
            .iter()
            .map(|(server, partitions)| self.report_server(server, partitions));
        let results = join_all(report_futures).await;
        if let Some(error) = results.into_iter().find_map(Result::err) {
            self.failed = true;
            return Err(error);
        }
        self.finished = true;

        let partitions_written = self
            .reported_blocks
            .values()
            .flat_map(|partitions| partitions.keys().copied())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Ok(MapOutput {
            task_attempt_id: self.task_attempt_id,
            blocks_written: self.blocks_written,
            bytes_written: self.bytes_written,
            records_written,
            partitions_written,
        })
    }

    fn ensure_active(&self) -> Result<(), RiffleError> {
        if self.failed {
            Err(RiffleError::AttemptPoisoned)
        } else if self.finished {
            Err(RiffleError::InvalidArgument(
                "push attempt is already finished".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    fn prepare_blocks(
        &mut self,
        partition_id: PartitionId,
        blocks: Vec<BlockPayload>,
    ) -> Result<Vec<PreparedBlock>, RiffleError> {
        let sequence = self.next_sequences.entry(partition_id).or_default();
        let mut prepared = Vec::with_capacity(blocks.len());
        for block in blocks {
            let length = i32::try_from(block.data.len()).map_err(|_| {
                RiffleError::InvalidArgument(format!(
                    "block for partition {partition_id} exceeds i32::MAX"
                ))
            })?;
            let uncompressed_length = i32::try_from(block.uncompressed_length).map_err(|_| {
                RiffleError::InvalidArgument(format!(
                    "uncompressed block length {} exceeds i32::MAX",
                    block.uncompressed_length
                ))
            })?;
            let block_id = self.writer.inner.handle.block_id_layout.compose(
                *sequence,
                partition_id,
                self.task_attempt_id,
            )?;
            *sequence = sequence.checked_add(1).ok_or_else(|| {
                RiffleError::InvalidArgument("block sequence number overflowed u64".to_string())
            })?;
            prepared.push(PreparedBlock {
                block_id,
                length,
                uncompressed_length,
                crc: crc32fast::hash(&block.data),
                data: block.data,
                record_count: block.record_count,
            });
        }
        Ok(prepared)
    }

    fn next_stats(
        &self,
        partition_id: PartitionId,
        blocks: &[PreparedBlock],
    ) -> Result<PendingWriteStats, RiffleError> {
        let batch_blocks = u64::try_from(blocks.len()).map_err(|_| {
            RiffleError::InvalidArgument("block count exceeds u64::MAX".to_string())
        })?;
        let blocks_written = self
            .blocks_written
            .checked_add(batch_blocks)
            .ok_or_else(|| {
                RiffleError::InvalidArgument("block count overflowed u64".to_string())
            })?;
        let batch_bytes = blocks.iter().try_fold(0_u64, |total, block| {
            let block_bytes = u64::try_from(block.data.len()).map_err(|_| {
                RiffleError::InvalidArgument("block length exceeds u64::MAX".to_string())
            })?;
            total.checked_add(block_bytes).ok_or_else(|| {
                RiffleError::InvalidArgument("byte count overflowed u64".to_string())
            })
        })?;
        let bytes_written = self
            .bytes_written
            .checked_add(batch_bytes)
            .ok_or_else(|| RiffleError::InvalidArgument("byte count overflowed u64".to_string()))?;
        let batch_records = total_block_records(blocks)?;
        let current_records = self
            .record_counts
            .get(&partition_id)
            .copied()
            .unwrap_or(Some(0));
        let partition_records = merge_record_counts(current_records, batch_records)?;
        if let Some(records) = partition_records {
            i64::try_from(records).map_err(|_| {
                RiffleError::InvalidArgument(format!(
                    "partition record count {records} exceeds i64::MAX"
                ))
            })?;
        }

        Ok(PendingWriteStats {
            blocks_written,
            bytes_written,
            partition_records,
        })
    }

    async fn send_batch(
        &self,
        server: &ShuffleServer,
        partition_id: PartitionId,
        blocks: &[PreparedBlock],
    ) -> Result<(), RiffleError> {
        let total_size = blocks.iter().map(|block| block.data.len()).sum::<usize>();
        let byte_permit = self
            .writer
            .inner
            .inflight_bytes
            .clone()
            .acquire_many_owned(total_size as u32)
            .await
            .map_err(|_| RiffleError::Closed)?;
        let request_permit = self
            .writer
            .inner
            .inflight_requests
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| RiffleError::Closed)?;
        let client = self.writer.inner.clients.get(server).ok_or_else(|| {
            RiffleError::InvalidAssignment(format!(
                "shuffle server {} is missing from the writer client registry",
                server.id
            ))
        })?;
        let result = client
            .send_shuffle_data(partition_id, self.task_attempt_id, blocks)
            .await;
        drop(request_permit);
        drop(byte_permit);
        result
    }

    async fn report_server(
        &self,
        server: &ShuffleServer,
        partitions: &BTreeMap<PartitionId, Vec<BlockId>>,
    ) -> Result<(), RiffleError> {
        let client = self.writer.inner.clients.get(server).ok_or_else(|| {
            RiffleError::InvalidAssignment(format!(
                "shuffle server {} is missing from the writer client registry",
                server.id
            ))
        })?;
        let partition_to_block_ids = partitions
            .iter()
            .map(|(partition_id, block_ids)| {
                Ok(PartitionToBlockIds {
                    partition_id: partition_id.as_i32()?,
                    block_ids: block_ids
                        .iter()
                        .map(|block_id| block_id.as_i64())
                        .collect::<Result<Vec<_>, RiffleError>>()?,
                })
            })
            .collect::<Result<Vec<_>, RiffleError>>()?;
        let partition_stats = partitions
            .keys()
            .filter_map(|partition_id| {
                self.record_counts
                    .get(partition_id)
                    .copied()
                    .flatten()
                    .map(|records| (*partition_id, records))
            })
            .map(|(partition_id, records)| {
                Ok(PartitionStats {
                    partition_id: partition_id.as_i32()?,
                    task_attempt_id_to_records: vec![TaskAttemptIdToRecords {
                        task_attempt_id: self.task_attempt_id.as_i64()?,
                        record_number: i64::try_from(records).map_err(|_| {
                            RiffleError::InvalidArgument(format!(
                                "record count {records} exceeds i64::MAX"
                            ))
                        })?,
                    }],
                })
            })
            .collect::<Result<Vec<_>, RiffleError>>()?;

        client
            .report_shuffle_result(
                self.task_attempt_id,
                partition_to_block_ids,
                partition_stats,
            )
            .await
    }
}

struct PendingWriteStats {
    blocks_written: u64,
    bytes_written: u64,
    partition_records: Option<u64>,
}

fn total_block_records(blocks: &[PreparedBlock]) -> Result<Option<u64>, RiffleError> {
    blocks.iter().try_fold(Some(0_u64), |total, block| {
        merge_record_counts(total, block.record_count)
    })
}

fn total_record_counts(
    record_counts: &BTreeMap<PartitionId, Option<u64>>,
) -> Result<Option<u64>, RiffleError> {
    record_counts
        .values()
        .try_fold(Some(0_u64), |total, records| {
            merge_record_counts(total, *records)
        })
}

fn merge_record_counts(left: Option<u64>, right: Option<u64>) -> Result<Option<u64>, RiffleError> {
    match (left, right) {
        (Some(left), Some(right)) => left
            .checked_add(right)
            .map(Some)
            .ok_or_else(|| RiffleError::InvalidArgument("record count overflowed u64".to_string())),
        _ => Ok(None),
    }
}

fn split_batches(
    blocks: &[PreparedBlock],
    max_batch_bytes: usize,
) -> Result<Vec<&[PreparedBlock]>, RiffleError> {
    let mut batches = Vec::new();
    let mut start = 0;
    let mut bytes = 0;
    for (index, block) in blocks.iter().enumerate() {
        if block.data.len() > max_batch_bytes {
            return Err(RiffleError::InvalidArgument(format!(
                "block {} has {} bytes, exceeding max_batch_bytes {}",
                block.block_id,
                block.data.len(),
                max_batch_bytes
            )));
        }
        if bytes != 0 && bytes + block.data.len() > max_batch_bytes {
            batches.push(&blocks[start..index]);
            start = index;
            bytes = 0;
        }
        bytes += block.data.len();
    }
    if start < blocks.len() {
        batches.push(&blocks[start..]);
    }
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prepared(id: u64, length: usize) -> PreparedBlock {
        PreparedBlock {
            block_id: BlockId::new(id),
            length: length as i32,
            uncompressed_length: length as i32,
            crc: 0,
            data: Bytes::from(vec![0; length]),
            record_count: None,
        }
    }

    #[test]
    fn batches_respect_byte_limit_without_splitting_blocks() {
        let blocks = vec![prepared(1, 3), prepared(2, 4), prepared(3, 2)];
        let batches = split_batches(&blocks, 6).unwrap();

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), 1);
        assert_eq!(batches[1].len(), 2);
    }

    #[test]
    fn record_count_aggregation_preserves_unknown_and_rejects_overflow() {
        assert_eq!(merge_record_counts(Some(2), Some(3)).unwrap(), Some(5));
        assert_eq!(merge_record_counts(Some(2), None).unwrap(), None);
        assert!(merge_record_counts(Some(u64::MAX), Some(1)).is_err());
    }
}
