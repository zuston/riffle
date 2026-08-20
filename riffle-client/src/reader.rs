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
    BlockId, PartitionId, ReadPartitionRequest, RiffleError, ShuffleBlock, ShuffleHandle,
    ShuffleReaderConfig, ShuffleServer, TaskAttemptId,
};
use bytes::{Buf, Bytes};
use croaring::{JvmLegacy, Treemap};
use futures::Stream;
use riffle_proto::uniffle::{
    BlockIdLayout as ProtoBlockIdLayout, GetLocalShuffleDataRequest, GetLocalShuffleDataResponse,
    GetLocalShuffleIndexRequest, GetLocalShuffleIndexResponse, GetMemoryShuffleDataRequest,
    GetMemoryShuffleDataResponse, GetShuffleResultRequest, GetShuffleResultResponse,
};
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

const INDEX_BLOCK_SIZE: usize = 40;
const MISSING_BLOCK_SAMPLE_SIZE: usize = 16;

pub type ShuffleBlockStream =
    Pin<Box<dyn Stream<Item = Result<ShuffleBlock, RiffleError>> + Send + 'static>>;

#[derive(Clone)]
pub struct ShuffleReader {
    inner: Arc<ReaderInner>,
}

struct ReaderInner {
    handle: ShuffleHandle,
    config: ShuffleReaderConfig,
    channel_pool: GrpcChannelPool,
}

struct ReadPass<'a> {
    channel: &'a Channel,
    server: &'a ShuffleServer,
    partition_id: PartitionId,
    expected: &'a HashSet<BlockId>,
    seen: &'a mut HashSet<BlockId>,
    sender: &'a mpsc::Sender<Result<ShuffleBlock, RiffleError>>,
}

impl ShuffleReader {
    pub fn from_handle(
        handle: ShuffleHandle,
        config: ShuffleReaderConfig,
    ) -> Result<Self, RiffleError> {
        handle.validate()?;
        config.validate()?;
        if handle.data_replica != 1 {
            return Err(RiffleError::Unsupported(
                "the initial ShuffleReader supports one replica only".to_string(),
            ));
        }
        let settings = GrpcClientSettings {
            connect_timeout: config.connect_timeout,
            request_timeout: config.request_timeout,
            max_encoding_message_size: config.max_encoding_message_size,
            max_decoding_message_size: config.max_decoding_message_size,
        };
        Ok(Self {
            inner: Arc::new(ReaderInner {
                handle,
                channel_pool: GrpcChannelPool::new(settings),
                config,
            }),
        })
    }

    pub fn handle(&self) -> &ShuffleHandle {
        &self.inner.handle
    }

    pub async fn read_partition(
        &self,
        request: ReadPartitionRequest,
    ) -> Result<ShuffleBlockStream, RiffleError> {
        let route = self.inner.handle.route_for(request.partition_id)?;
        let server = route.replicas.first().cloned().ok_or_else(|| {
            RiffleError::InvalidAssignment(format!(
                "partition {} has no shuffle server",
                request.partition_id
            ))
        })?;
        let accepted_attempts = request
            .accepted_task_attempt_ids
            .into_iter()
            .collect::<HashSet<_>>();
        for attempt in &accepted_attempts {
            if attempt.value() > self.inner.handle.block_id_layout.max_task_attempt_id() {
                return Err(RiffleError::InvalidArgument(format!(
                    "accepted task attempt id {attempt} exceeds the block id layout capacity"
                )));
            }
        }
        if accepted_attempts.is_empty() {
            return Ok(Box::pin(futures::stream::empty()));
        }

        let channel = self.inner.channel_pool.channel(&server).await?;
        let expected = self
            .fetch_expected_blocks(
                channel.clone(),
                &server,
                request.partition_id,
                &accepted_attempts,
            )
            .await?;
        if expected.is_empty() {
            return Ok(Box::pin(futures::stream::empty()));
        }

        let (sender, receiver) = mpsc::channel(self.inner.config.stream_channel_capacity);
        let reader = self.clone();
        let partition_id = request.partition_id;
        tokio::spawn(async move {
            let result = reader
                .read_partition_into(
                    channel,
                    server,
                    partition_id,
                    accepted_attempts,
                    expected,
                    sender.clone(),
                )
                .await;
            if let Err(error) = result {
                let _ = sender.send(Err(error)).await;
            }
        });
        Ok(Box::pin(ReceiverStream::new(receiver)))
    }

    async fn fetch_expected_blocks(
        &self,
        channel: Channel,
        server: &ShuffleServer,
        partition_id: PartitionId,
        accepted_attempts: &HashSet<TaskAttemptId>,
    ) -> Result<HashSet<BlockId>, RiffleError> {
        let layout = self.inner.handle.block_id_layout;
        let response = self
            .get_shuffle_result(channel, server, partition_id)
            .await?;
        let reported = if response.serialized_bitmap.is_empty() {
            Treemap::new()
        } else {
            Treemap::try_deserialize::<JvmLegacy>(&response.serialized_bitmap).ok_or_else(|| {
                RiffleError::InvalidResponse {
                    operation: "get_shuffle_result",
                    message: "reported block bitmap is not valid JvmLegacy roaring data"
                        .to_string(),
                }
            })?
        };
        let mut expected = HashSet::new();
        for raw_block_id in reported.iter() {
            let block_id = BlockId::new(raw_block_id);
            if block_id.value() > layout.max_block_id() {
                return Err(RiffleError::InvalidResponse {
                    operation: "get_shuffle_result",
                    message: format!("reported block {block_id} exceeds the configured ID layout"),
                });
            }
            if layout.partition_id(block_id) != partition_id {
                return Err(RiffleError::InvalidResponse {
                    operation: "get_shuffle_result",
                    message: format!(
                        "reported block {block_id} belongs to partition {}, expected {}",
                        layout.partition_id(block_id),
                        partition_id
                    ),
                });
            }
            if accepted_attempts.contains(&layout.task_attempt_id(block_id)) {
                expected.insert(block_id);
            }
        }
        Ok(expected)
    }

    async fn read_partition_into(
        &self,
        channel: Channel,
        server: ShuffleServer,
        partition_id: PartitionId,
        accepted_attempts: HashSet<TaskAttemptId>,
        expected: HashSet<BlockId>,
        sender: mpsc::Sender<Result<ShuffleBlock, RiffleError>>,
    ) -> Result<(), RiffleError> {
        let expected_attempt_bitmap = serialize_attempts(&accepted_attempts);
        let mut seen = HashSet::with_capacity(expected.len());
        let mut pass = ReadPass {
            channel: &channel,
            server: &server,
            partition_id,
            expected: &expected,
            seen: &mut seen,
            sender: &sender,
        };
        for scan in 0..=self.inner.config.spill_race_retries {
            self.read_memory(&mut pass, &expected_attempt_bitmap)
                .await?;
            if pass.seen.len() == pass.expected.len() {
                return Ok(());
            }
            self.read_local(&mut pass, &accepted_attempts).await?;
            if pass.seen.len() == pass.expected.len() {
                return Ok(());
            }
            if scan < self.inner.config.spill_race_retries {
                tokio::time::sleep(self.inner.config.retry_policy.delay_for(scan + 1)).await;
            }
        }

        let mut missing = pass
            .expected
            .difference(pass.seen)
            .copied()
            .collect::<Vec<_>>();
        missing.sort();
        Err(RiffleError::MissingBlocks {
            partition_id,
            missing_count: missing.len(),
            sample: missing
                .into_iter()
                .take(MISSING_BLOCK_SAMPLE_SIZE)
                .collect(),
        })
    }

    async fn read_memory(
        &self,
        pass: &mut ReadPass<'_>,
        expected_attempt_bitmap: &Bytes,
    ) -> Result<(), RiffleError> {
        let mut cursor = -1_i64;
        loop {
            let response = self
                .get_memory_page(
                    pass.channel.clone(),
                    pass.server,
                    pass.partition_id,
                    cursor,
                    expected_attempt_bitmap.clone(),
                )
                .await?;
            if response.shuffle_data_block_segments.is_empty() {
                return Ok(());
            }

            let next_cursor = next_memory_cursor(
                cursor,
                response
                    .shuffle_data_block_segments
                    .last()
                    .map(|segment| segment.block_id),
            )?;
            for segment in response.shuffle_data_block_segments {
                let block_id = non_negative_block_id("memory segment block id", segment.block_id)?;
                if !pass.expected.contains(&block_id) || pass.seen.contains(&block_id) {
                    continue;
                }
                let offset = usize::try_from(segment.offset).map_err(|_| {
                    invalid_response(
                        "get_memory_shuffle_data",
                        format!("block {block_id} has negative offset {}", segment.offset),
                    )
                })?;
                let length = usize::try_from(segment.length).map_err(|_| {
                    invalid_response(
                        "get_memory_shuffle_data",
                        format!("block {block_id} has negative length {}", segment.length),
                    )
                })?;
                let end = offset.checked_add(length).ok_or_else(|| {
                    invalid_response(
                        "get_memory_shuffle_data",
                        format!("block {block_id} offset and length overflow"),
                    )
                })?;
                if end > response.data.len() {
                    return Err(invalid_response(
                        "get_memory_shuffle_data",
                        format!(
                            "block {block_id} range {offset}..{end} exceeds payload length {}",
                            response.data.len()
                        ),
                    ));
                }
                let task_attempt_id = non_negative_attempt_id(
                    "memory segment task attempt id",
                    segment.task_attempt_id,
                )?;
                self.validate_block_identity(block_id, pass.partition_id, task_attempt_id)?;
                let uncompressed_length =
                    u32::try_from(segment.uncompress_length).map_err(|_| {
                        invalid_response(
                            "get_memory_shuffle_data",
                            format!(
                                "block {block_id} has negative uncompressed length {}",
                                segment.uncompress_length
                            ),
                        )
                    })?;
                let crc = u32::try_from(segment.crc).map_err(|_| {
                    invalid_response(
                        "get_memory_shuffle_data",
                        format!("block {block_id} has invalid crc {}", segment.crc),
                    )
                })?;
                let data = response.data.slice(offset..end);
                validate_crc(block_id, crc, &data)?;
                pass.seen.insert(block_id);
                pass.sender
                    .send(Ok(ShuffleBlock {
                        block_id,
                        partition_id: pass.partition_id,
                        task_attempt_id,
                        uncompressed_length,
                        crc,
                        data,
                    }))
                    .await
                    .map_err(|_| RiffleError::Closed)?;
            }

            if response.is_end == Some(true) {
                return Ok(());
            }
            cursor = next_cursor;
        }
    }

    async fn read_local(
        &self,
        pass: &mut ReadPass<'_>,
        accepted_attempts: &HashSet<TaskAttemptId>,
    ) -> Result<(), RiffleError> {
        let response = self
            .get_local_index(pass.channel.clone(), pass.server, pass.partition_id)
            .await?;
        let entries = parse_index(response.index_data, response.data_file_len)?;
        let mut selected = entries
            .into_iter()
            .filter(|entry| {
                pass.expected.contains(&entry.block_id)
                    && !pass.seen.contains(&entry.block_id)
                    && accepted_attempts.contains(&entry.task_attempt_id)
            })
            .collect::<Vec<_>>();
        selected.sort_by_key(|entry| entry.offset);
        let spans = build_spans(selected, self.inner.config.read_buffer_size)?;
        let storage_id = response.storage_ids.first().copied().unwrap_or(0);

        for span in spans {
            let response = self
                .get_local_data(
                    pass.channel.clone(),
                    pass.server,
                    pass.partition_id,
                    span.offset,
                    span.length,
                    storage_id,
                )
                .await?;
            if response.data.len() != span.length as usize {
                return Err(invalid_response(
                    "get_local_shuffle_data",
                    format!(
                        "requested {} bytes at offset {}, received {}",
                        span.length,
                        span.offset,
                        response.data.len()
                    ),
                ));
            }
            for entry in span.entries {
                if pass.seen.contains(&entry.block_id) {
                    continue;
                }
                self.validate_block_identity(
                    entry.block_id,
                    pass.partition_id,
                    entry.task_attempt_id,
                )?;
                let relative_offset =
                    usize::try_from(entry.offset - span.offset).map_err(|_| {
                        invalid_response(
                            "get_local_shuffle_data",
                            format!("block {} has an invalid relative offset", entry.block_id),
                        )
                    })?;
                let end = relative_offset
                    .checked_add(entry.length as usize)
                    .ok_or_else(|| {
                        invalid_response(
                            "get_local_shuffle_data",
                            format!("block {} range overflowed", entry.block_id),
                        )
                    })?;
                if end > response.data.len() {
                    return Err(invalid_response(
                        "get_local_shuffle_data",
                        format!(
                            "block {} range {}..{} exceeds response length {}",
                            entry.block_id,
                            relative_offset,
                            end,
                            response.data.len()
                        ),
                    ));
                }
                let data = response.data.slice(relative_offset..end);
                validate_crc(entry.block_id, entry.crc, &data)?;
                pass.seen.insert(entry.block_id);
                pass.sender
                    .send(Ok(ShuffleBlock {
                        block_id: entry.block_id,
                        partition_id: pass.partition_id,
                        task_attempt_id: entry.task_attempt_id,
                        uncompressed_length: entry.uncompressed_length,
                        crc: entry.crc,
                        data,
                    }))
                    .await
                    .map_err(|_| RiffleError::Closed)?;
            }
        }
        Ok(())
    }

    fn validate_block_identity(
        &self,
        block_id: BlockId,
        partition_id: PartitionId,
        task_attempt_id: TaskAttemptId,
    ) -> Result<(), RiffleError> {
        let layout = self.inner.handle.block_id_layout;
        if block_id.value() > layout.max_block_id()
            || layout.partition_id(block_id) != partition_id
            || layout.task_attempt_id(block_id) != task_attempt_id
        {
            return Err(invalid_response(
                "read_shuffle_block",
                format!(
                    "block {block_id} metadata does not match partition {partition_id} and attempt {task_attempt_id}"
                ),
            ));
        }
        Ok(())
    }

    async fn get_shuffle_result(
        &self,
        channel: Channel,
        server: &ShuffleServer,
        partition_id: PartitionId,
    ) -> Result<GetShuffleResultResponse, RiffleError> {
        let operation = "get_shuffle_result";
        for attempt in 1..=self.inner.config.retry_policy.max_attempts {
            let mut client = self.inner.channel_pool.client(channel.clone());
            match client
                .get_shuffle_result(GetShuffleResultRequest {
                    app_id: self.inner.handle.application_id.as_str().to_string(),
                    shuffle_id: self.inner.handle.shuffle_id.value(),
                    partition_id: partition_id.as_i32()?,
                    block_id_layout: Some(ProtoBlockIdLayout {
                        sequence_no_bits: i32::from(
                            self.inner.handle.block_id_layout.sequence_no_bits,
                        ),
                        partition_id_bits: i32::from(
                            self.inner.handle.block_id_layout.partition_id_bits,
                        ),
                        task_attempt_id_bits: i32::from(
                            self.inner.handle.block_id_layout.task_attempt_id_bits,
                        ),
                    }),
                })
                .await
            {
                Ok(response) => {
                    let response = response.into_inner();
                    let status = RemoteStatus::from(response.status);
                    if status == RemoteStatus::Success {
                        return Ok(response);
                    }
                    if status.is_retryable_read()
                        && attempt < self.inner.config.retry_policy.max_attempts
                    {
                        self.sleep_after(attempt).await;
                        continue;
                    }
                    return Err(RiffleError::Remote {
                        operation,
                        status,
                        message: response.ret_msg,
                    });
                }
                Err(_error) if attempt < self.inner.config.retry_policy.max_attempts => {
                    self.sleep_after(attempt).await;
                }
                Err(error) => return Err(read_transport(operation, server, error)),
            }
        }
        unreachable!("retry policy always has at least one attempt")
    }

    async fn get_memory_page(
        &self,
        channel: Channel,
        server: &ShuffleServer,
        partition_id: PartitionId,
        cursor: i64,
        expected_attempts: Bytes,
    ) -> Result<GetMemoryShuffleDataResponse, RiffleError> {
        let operation = "get_memory_shuffle_data";
        for attempt in 1..=self.inner.config.retry_policy.max_attempts {
            let mut client = self.inner.channel_pool.client(channel.clone());
            match client
                .get_memory_shuffle_data(GetMemoryShuffleDataRequest {
                    app_id: self.inner.handle.application_id.as_str().to_string(),
                    shuffle_id: self.inner.handle.shuffle_id.value(),
                    partition_id: partition_id.as_i32()?,
                    last_block_id: cursor,
                    read_buffer_size: i32::try_from(self.inner.config.read_buffer_size).map_err(
                        |_| {
                            RiffleError::InvalidArgument(
                                "read_buffer_size exceeds i32::MAX".to_string(),
                            )
                        },
                    )?,
                    timestamp: now_millis(),
                    serialized_expected_task_ids_bitmap: expected_attempts.clone(),
                })
                .await
            {
                Ok(response) => {
                    let response = response.into_inner();
                    let status = RemoteStatus::from(response.status);
                    if status == RemoteStatus::Success {
                        return Ok(response);
                    }
                    if status.is_retryable_read()
                        && attempt < self.inner.config.retry_policy.max_attempts
                    {
                        self.sleep_after(attempt).await;
                        continue;
                    }
                    return Err(RiffleError::Remote {
                        operation,
                        status,
                        message: response.ret_msg,
                    });
                }
                Err(_error) if attempt < self.inner.config.retry_policy.max_attempts => {
                    self.sleep_after(attempt).await;
                }
                Err(error) => return Err(read_transport(operation, server, error)),
            }
        }
        unreachable!("retry policy always has at least one attempt")
    }

    async fn get_local_index(
        &self,
        channel: Channel,
        server: &ShuffleServer,
        partition_id: PartitionId,
    ) -> Result<GetLocalShuffleIndexResponse, RiffleError> {
        let operation = "get_local_shuffle_index";
        for attempt in 1..=self.inner.config.retry_policy.max_attempts {
            let mut client = self.inner.channel_pool.client(channel.clone());
            match client
                .get_local_shuffle_index(GetLocalShuffleIndexRequest {
                    app_id: self.inner.handle.application_id.as_str().to_string(),
                    shuffle_id: self.inner.handle.shuffle_id.value(),
                    partition_id: partition_id.as_i32()?,
                    partition_num_per_range: i32::try_from(self.inner.handle.partition_range_size)
                        .map_err(|_| {
                            RiffleError::InvalidArgument(
                                "partition_range_size exceeds i32::MAX".to_string(),
                            )
                        })?,
                    partition_num: i32::try_from(self.inner.handle.partition_count).map_err(
                        |_| {
                            RiffleError::InvalidArgument(
                                "partition_count exceeds i32::MAX".to_string(),
                            )
                        },
                    )?,
                })
                .await
            {
                Ok(response) => {
                    let response = response.into_inner();
                    let status = RemoteStatus::from(response.status);
                    if status == RemoteStatus::Success {
                        return Ok(response);
                    }
                    if status.is_retryable_read()
                        && attempt < self.inner.config.retry_policy.max_attempts
                    {
                        self.sleep_after(attempt).await;
                        continue;
                    }
                    return Err(RiffleError::Remote {
                        operation,
                        status,
                        message: response.ret_msg,
                    });
                }
                Err(_error) if attempt < self.inner.config.retry_policy.max_attempts => {
                    self.sleep_after(attempt).await;
                }
                Err(error) => return Err(read_transport(operation, server, error)),
            }
        }
        unreachable!("retry policy always has at least one attempt")
    }

    async fn get_local_data(
        &self,
        channel: Channel,
        server: &ShuffleServer,
        partition_id: PartitionId,
        offset: i64,
        length: i32,
        storage_id: i32,
    ) -> Result<GetLocalShuffleDataResponse, RiffleError> {
        let operation = "get_local_shuffle_data";
        for attempt in 1..=self.inner.config.retry_policy.max_attempts {
            let mut client = self.inner.channel_pool.client(channel.clone());
            match client
                .get_local_shuffle_data(GetLocalShuffleDataRequest {
                    app_id: self.inner.handle.application_id.as_str().to_string(),
                    shuffle_id: self.inner.handle.shuffle_id.value(),
                    partition_id: partition_id.as_i32()?,
                    partition_num_per_range: i32::try_from(self.inner.handle.partition_range_size)
                        .map_err(|_| {
                            RiffleError::InvalidArgument(
                                "partition_range_size exceeds i32::MAX".to_string(),
                            )
                        })?,
                    partition_num: i32::try_from(self.inner.handle.partition_count).map_err(
                        |_| {
                            RiffleError::InvalidArgument(
                                "partition_count exceeds i32::MAX".to_string(),
                            )
                        },
                    )?,
                    offset,
                    length,
                    timestamp: now_millis(),
                    storage_id,
                })
                .await
            {
                Ok(response) => {
                    let response = response.into_inner();
                    let status = RemoteStatus::from(response.status);
                    if status == RemoteStatus::Success {
                        return Ok(response);
                    }
                    if status.is_retryable_read()
                        && attempt < self.inner.config.retry_policy.max_attempts
                    {
                        self.sleep_after(attempt).await;
                        continue;
                    }
                    return Err(RiffleError::Remote {
                        operation,
                        status,
                        message: response.ret_msg,
                    });
                }
                Err(_error) if attempt < self.inner.config.retry_policy.max_attempts => {
                    self.sleep_after(attempt).await;
                }
                Err(error) => return Err(read_transport(operation, server, error)),
            }
        }
        unreachable!("retry policy always has at least one attempt")
    }

    async fn sleep_after(&self, attempt: u32) {
        tokio::time::sleep(self.inner.config.retry_policy.delay_for(attempt)).await;
    }
}

#[derive(Clone, Debug)]
struct IndexEntry {
    offset: i64,
    length: u32,
    uncompressed_length: u32,
    crc: u32,
    block_id: BlockId,
    task_attempt_id: TaskAttemptId,
}

#[derive(Debug)]
struct ReadSpan {
    offset: i64,
    length: i32,
    entries: Vec<IndexEntry>,
}

fn parse_index(mut data: Bytes, data_file_len: i64) -> Result<Vec<IndexEntry>, RiffleError> {
    if data.len() % INDEX_BLOCK_SIZE != 0 {
        return Err(invalid_response(
            "get_local_shuffle_index",
            format!(
                "index length {} is not a multiple of {INDEX_BLOCK_SIZE}",
                data.len()
            ),
        ));
    }
    if data_file_len < 0 {
        return Err(invalid_response(
            "get_local_shuffle_index",
            format!("negative data file length {data_file_len}"),
        ));
    }
    let mut entries = Vec::with_capacity(data.len() / INDEX_BLOCK_SIZE);
    while data.has_remaining() {
        let offset = data.get_i64();
        let raw_length = data.get_i32();
        let raw_uncompressed_length = data.get_i32();
        let raw_crc = data.get_i64();
        let raw_block_id = data.get_i64();
        let raw_task_attempt_id = data.get_i64();
        let length = u32::try_from(raw_length).map_err(|_| {
            invalid_response(
                "get_local_shuffle_index",
                format!("negative block length {raw_length}"),
            )
        })?;
        let uncompressed_length = u32::try_from(raw_uncompressed_length).map_err(|_| {
            invalid_response(
                "get_local_shuffle_index",
                format!("negative uncompressed block length {raw_uncompressed_length}"),
            )
        })?;
        let crc = u32::try_from(raw_crc).map_err(|_| {
            invalid_response("get_local_shuffle_index", format!("invalid crc {raw_crc}"))
        })?;
        let block_id = non_negative_block_id("local index block id", raw_block_id)?;
        let task_attempt_id =
            non_negative_attempt_id("local index task attempt id", raw_task_attempt_id)?;
        if offset < 0 {
            return Err(invalid_response(
                "get_local_shuffle_index",
                format!("block {block_id} has negative offset {offset}"),
            ));
        }
        let end = offset.checked_add(i64::from(length)).ok_or_else(|| {
            invalid_response(
                "get_local_shuffle_index",
                format!("block {block_id} offset and length overflow"),
            )
        })?;
        if end > data_file_len {
            return Err(invalid_response(
                "get_local_shuffle_index",
                format!("block {block_id} ends at {end}, beyond data file length {data_file_len}"),
            ));
        }
        entries.push(IndexEntry {
            offset,
            length,
            uncompressed_length,
            crc,
            block_id,
            task_attempt_id,
        });
    }
    Ok(entries)
}

fn build_spans(entries: Vec<IndexEntry>, target_size: usize) -> Result<Vec<ReadSpan>, RiffleError> {
    let mut spans = Vec::<ReadSpan>::new();
    for entry in entries {
        let entry_length = i32::try_from(entry.length).map_err(|_| {
            invalid_response(
                "get_local_shuffle_index",
                format!("block {} length exceeds i32::MAX", entry.block_id),
            )
        })?;
        if let Some(span) = spans.last_mut() {
            let span_end = span.offset + i64::from(span.length);
            let combined_length = i64::from(span.length) + i64::from(entry_length);
            if span_end == entry.offset
                && combined_length <= i32::MAX as i64
                && combined_length as usize <= target_size
            {
                span.length = combined_length as i32;
                span.entries.push(entry);
                continue;
            }
        }
        spans.push(ReadSpan {
            offset: entry.offset,
            length: entry_length,
            entries: vec![entry],
        });
    }
    Ok(spans)
}

fn serialize_attempts(attempts: &HashSet<TaskAttemptId>) -> Bytes {
    let bitmap = attempts
        .iter()
        .map(|attempt| attempt.value())
        .collect::<Treemap>();
    Bytes::from(bitmap.serialize::<JvmLegacy>())
}

fn validate_crc(block_id: BlockId, expected: u32, data: &Bytes) -> Result<(), RiffleError> {
    let actual = crc32fast::hash(data);
    if expected == actual {
        Ok(())
    } else {
        Err(RiffleError::CrcMismatch {
            block_id,
            expected,
            actual,
        })
    }
}

fn next_memory_cursor(current: i64, last_block_id: Option<i64>) -> Result<i64, RiffleError> {
    let next = last_block_id.ok_or_else(|| {
        invalid_response(
            "get_memory_shuffle_data",
            "memory response lost its pagination cursor".to_string(),
        )
    })?;
    if next == current {
        return Err(invalid_response(
            "get_memory_shuffle_data",
            format!("memory pagination cursor repeated block {current}"),
        ));
    }
    Ok(next)
}

fn non_negative_block_id(name: &str, value: i64) -> Result<BlockId, RiffleError> {
    u64::try_from(value)
        .map(BlockId::new)
        .map_err(|_| invalid_response("read_shuffle_block", format!("{name} is negative: {value}")))
}

fn non_negative_attempt_id(name: &str, value: i64) -> Result<TaskAttemptId, RiffleError> {
    u64::try_from(value)
        .map(TaskAttemptId::new)
        .map_err(|_| invalid_response("read_shuffle_block", format!("{name} is negative: {value}")))
}

fn invalid_response(operation: &'static str, message: String) -> RiffleError {
    RiffleError::InvalidResponse { operation, message }
}

fn read_transport(
    operation: &'static str,
    server: &ShuffleServer,
    error: tonic::Status,
) -> RiffleError {
    RiffleError::Transport {
        operation,
        endpoint: server.grpc_endpoint(),
        message: error.to_string(),
    }
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
    use bytes::{BufMut, BytesMut};

    fn index_entry(offset: i64, length: u32, block_id: u64) -> IndexEntry {
        IndexEntry {
            offset,
            length,
            uncompressed_length: length,
            crc: 0,
            block_id: BlockId::new(block_id),
            task_attempt_id: TaskAttemptId::new(1),
        }
    }

    #[test]
    fn parses_fixed_width_local_index() {
        let mut data = BytesMut::new();
        data.put_i64(10);
        data.put_i32(4);
        data.put_i32(8);
        data.put_i64(42);
        data.put_i64(100);
        data.put_i64(7);

        let entries = parse_index(data.freeze(), 14).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].offset, 10);
        assert_eq!(entries[0].length, 4);
        assert_eq!(entries[0].task_attempt_id, TaskAttemptId::new(7));
    }

    #[test]
    fn rejects_truncated_or_out_of_bounds_index() {
        assert!(parse_index(Bytes::from_static(&[0; 39]), 100).is_err());

        let mut data = BytesMut::new();
        data.put_i64(10);
        data.put_i32(4);
        data.put_i32(4);
        data.put_i64(0);
        data.put_i64(1);
        data.put_i64(1);
        assert!(parse_index(data.freeze(), 12).is_err());
    }

    #[test]
    fn coalesces_only_adjacent_index_entries_within_target() {
        let spans = build_spans(
            vec![
                index_entry(0, 4, 1),
                index_entry(4, 4, 2),
                index_entry(10, 2, 3),
            ],
            8,
        )
        .unwrap();

        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0].length, 8);
        assert_eq!(spans[0].entries.len(), 2);
        assert_eq!(spans[1].offset, 10);
    }

    #[test]
    fn memory_cursor_follows_server_order_instead_of_numeric_order() {
        assert_eq!(next_memory_cursor(100, Some(5)).unwrap(), 5);
        assert!(next_memory_cursor(5, Some(5)).is_err());
        assert!(next_memory_cursor(5, None).is_err());
    }
}
