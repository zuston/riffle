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

use crate::channel_pool::{GrpcChannelPool, GrpcClientSettings};
use crate::error::RemoteStatus;
use crate::{
    BlockId, BlockPayload, MapOutput, PartitionId, RiffleError, ShuffleHandle, ShuffleServer,
    ShuffleWriterConfig, TaskAttemptId,
};
use bytes::{BufMut, Bytes, BytesMut};
use futures::future::join_all;
use riffle_proto::uniffle::{
    CombinedShuffleData, PartitionStats, PartitionToBlockIds, ReportShuffleResultRequest,
    RequireBufferRequest, SendShuffleDataRequest, TaskAttemptIdToRecords,
};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct ShuffleWriter {
    inner: Arc<WriterInner>,
}

struct WriterInner {
    handle: ShuffleHandle,
    config: ShuffleWriterConfig,
    channel_pool: GrpcChannelPool,
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
        let settings = GrpcClientSettings {
            connect_timeout: config.connect_timeout,
            request_timeout: config.request_timeout,
            max_encoding_message_size: config.max_encoding_message_size,
            max_decoding_message_size: config.max_decoding_message_size,
        };
        Ok(Self {
            inner: Arc::new(WriterInner {
                inflight_bytes: Arc::new(Semaphore::new(config.max_inflight_bytes)),
                inflight_requests: Arc::new(Semaphore::new(config.max_inflight_requests)),
                channel_pool: GrpcChannelPool::new(settings),
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
        let result = self
            .send_batch_with_permits(server, partition_id, blocks, total_size)
            .await;
        drop(request_permit);
        drop(byte_permit);
        result
    }

    async fn send_batch_with_permits(
        &self,
        server: &ShuffleServer,
        partition_id: PartitionId,
        blocks: &[PreparedBlock],
        total_size: usize,
    ) -> Result<(), RiffleError> {
        let policy = &self.writer.inner.config.retry_policy;
        let endpoint = server.grpc_endpoint();
        for attempt in 1..=policy.max_attempts {
            let channel = match self.writer.inner.channel_pool.channel(server).await {
                Ok(channel) => channel,
                Err(_error) if attempt < policy.max_attempts => {
                    tokio::time::sleep(policy.delay_for(attempt)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let mut client = self.writer.inner.channel_pool.client(channel);
            let ticket = match client
                .require_buffer(RequireBufferRequest {
                    require_size: i32::try_from(total_size).map_err(|_| {
                        RiffleError::InvalidArgument(
                            "writer batch size exceeds i32::MAX".to_string(),
                        )
                    })?,
                    app_id: self.writer.inner.handle.application_id.as_str().to_string(),
                    shuffle_id: self.writer.inner.handle.shuffle_id.value(),
                    partition_ids: vec![partition_id.as_i32()?],
                })
                .await
            {
                Ok(response) => {
                    let response = response.into_inner();
                    match RemoteStatus::from(response.status) {
                        RemoteStatus::Success => response.require_buffer_id,
                        RemoteStatus::NoBuffer if attempt < policy.max_attempts => {
                            tokio::time::sleep(policy.delay_for(attempt)).await;
                            continue;
                        }
                        status => {
                            return Err(RiffleError::Remote {
                                operation: "require_buffer",
                                status,
                                message: response.ret_msg,
                            });
                        }
                    }
                }
                Err(_error) if attempt < policy.max_attempts => {
                    tokio::time::sleep(policy.delay_for(attempt)).await;
                    continue;
                }
                Err(error) => {
                    return Err(RiffleError::Transport {
                        operation: "require_buffer",
                        endpoint,
                        message: error.to_string(),
                    });
                }
            };

            let response = client
                .send_shuffle_data(SendShuffleDataRequest {
                    app_id: self.writer.inner.handle.application_id.as_str().to_string(),
                    shuffle_id: self.writer.inner.handle.shuffle_id.value(),
                    require_buffer_id: ticket,
                    shuffle_data: Vec::new(),
                    timestamp: now_millis(),
                    stage_attempt_number: 0,
                    combined_shuffle_data: Some(combined_data(
                        partition_id,
                        self.task_attempt_id,
                        blocks,
                    )?),
                })
                .await;
            match response {
                Ok(response) => {
                    let response = response.into_inner();
                    let status = RemoteStatus::from(response.status);
                    match status {
                        RemoteStatus::Success => return Ok(()),
                        RemoteStatus::NoBuffer if attempt < policy.max_attempts => {
                            tokio::time::sleep(policy.delay_for(attempt)).await;
                        }
                        RemoteStatus::NoBuffer => {
                            return Err(RiffleError::Remote {
                                operation: "send_shuffle_data",
                                status,
                                message: response.ret_msg,
                            });
                        }
                        _ => {
                            return Err(RiffleError::AmbiguousWrite {
                                operation: "send_shuffle_data",
                                endpoint,
                                message: format!("remote status {status:?}: {}", response.ret_msg),
                            });
                        }
                    }
                }
                Err(error) => {
                    return Err(RiffleError::AmbiguousWrite {
                        operation: "send_shuffle_data",
                        endpoint,
                        message: error.to_string(),
                    });
                }
            }
        }
        Err(RiffleError::Remote {
            operation: "send_shuffle_data",
            status: RemoteStatus::NoBuffer,
            message: "buffer retry budget exhausted".to_string(),
        })
    }

    async fn report_server(
        &self,
        server: &ShuffleServer,
        partitions: &BTreeMap<PartitionId, Vec<BlockId>>,
    ) -> Result<(), RiffleError> {
        let endpoint = server.grpc_endpoint();
        let channel = self.writer.inner.channel_pool.channel(server).await?;
        let mut client = self.writer.inner.channel_pool.client(channel);
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

        let response = client
            .report_shuffle_result(ReportShuffleResultRequest {
                app_id: self.writer.inner.handle.application_id.as_str().to_string(),
                shuffle_id: self.writer.inner.handle.shuffle_id.value(),
                task_attempt_id: self.task_attempt_id.as_i64()?,
                bitmap_num: 0,
                partition_to_block_ids,
                partition_stats,
            })
            .await
            .map_err(|error| RiffleError::AmbiguousWrite {
                operation: "report_shuffle_result",
                endpoint: endpoint.clone(),
                message: error.to_string(),
            })?
            .into_inner();
        let status = RemoteStatus::from(response.status);
        if status == RemoteStatus::Success {
            Ok(())
        } else {
            Err(RiffleError::AmbiguousWrite {
                operation: "report_shuffle_result",
                endpoint,
                message: format!("remote status {status:?}: {}", response.ret_msg),
            })
        }
    }
}

#[derive(Clone)]
struct PreparedBlock {
    block_id: BlockId,
    length: i32,
    uncompressed_length: i32,
    crc: u32,
    data: Bytes,
    record_count: Option<u64>,
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

fn combined_data(
    partition_id: PartitionId,
    task_attempt_id: TaskAttemptId,
    blocks: &[PreparedBlock],
) -> Result<CombinedShuffleData, RiffleError> {
    let total_size = blocks.iter().map(|block| block.data.len()).sum();
    let mut data = BytesMut::with_capacity(total_size);
    for block in blocks {
        data.put_slice(&block.data);
    }
    Ok(CombinedShuffleData {
        partition_ids: vec![partition_id.as_i32()?],
        partition_block_counts: vec![i32::try_from(blocks.len()).map_err(|_| {
            RiffleError::InvalidArgument("writer batch has too many blocks".to_string())
        })?],
        block_ids: blocks
            .iter()
            .map(|block| block.block_id.as_i64())
            .collect::<Result<Vec<_>, RiffleError>>()?,
        lengths: blocks.iter().map(|block| block.length).collect(),
        uncompress_lengths: blocks
            .iter()
            .map(|block| block.uncompressed_length)
            .collect(),
        crcs: blocks.iter().map(|block| i64::from(block.crc)).collect(),
        task_attempt_ids: vec![task_attempt_id.as_i64()?; blocks.len()],
        combined_data: data.freeze(),
    })
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
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
    fn combined_payload_preserves_block_boundaries() {
        let blocks = vec![prepared(1, 2), prepared(2, 3)];
        let combined = combined_data(PartitionId::new(4), TaskAttemptId::new(7), &blocks).unwrap();

        assert_eq!(combined.partition_ids, vec![4]);
        assert_eq!(combined.partition_block_counts, vec![2]);
        assert_eq!(combined.lengths, vec![2, 3]);
        assert_eq!(combined.combined_data.len(), 5);
    }

    #[test]
    fn record_count_aggregation_preserves_unknown_and_rejects_overflow() {
        assert_eq!(merge_record_counts(Some(2), Some(3)).unwrap(), Some(5));
        assert_eq!(merge_record_counts(Some(2), None).unwrap(), None);
        assert!(merge_record_counts(Some(u64::MAX), Some(1)).is_err());
    }
}
