use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use bytes::Bytes;
use croaring::{JvmLegacy, Treemap};
use riffle_server::grpc::protobuf::uniffle::shuffle_server_client::ShuffleServerClient;
use riffle_server::grpc::protobuf::uniffle::{
    GetLocalShuffleDataRequest, GetLocalShuffleIndexRequest, GetMemoryShuffleDataRequest,
};
use riffle_server::id_layout::DEFAULT_BLOCK_ID_LAYOUT;
use riffle_server::store::index_codec::{IndexCodec, INDEX_BLOCK_SIZE};
use riffle_server::util::{get_crc, now_timestamp_as_millis};

use crate::{
    assignment, check_status, connect, Endpoint, PartitionId, ReadContext, ShuffleRecord,
    ShuffleStream, BLOCK_SEQUENCE_LIMIT, MAX_MESSAGE_BYTES, MAX_PARTITION_BYTES, TASK_ID_LIMIT,
};

pub struct ShuffleReader {
    context: ReadContext,
    routes: HashMap<PartitionId, Endpoint>,
}

impl ShuffleReader {
    pub async fn new(mut context: ReadContext) -> Result<Self> {
        for result in &context.write_results {
            ensure!(
                result.app_id == context.shuffle.app_id
                    && result.shuffle_id == context.shuffle.shuffle_id,
                "foreign write result"
            );
            ensure!(
                result.task_id < TASK_ID_LIMIT,
                "task ID exceeds 23-bit block ID range"
            );
            for (partition, count) in &result.blocks_number {
                ensure!(
                    *partition < context.shuffle.partition_count as usize
                        && *count > 0
                        && *count <= BLOCK_SEQUENCE_LIMIT,
                    "invalid block summary"
                );
            }
        }
        context.partitions.sort_unstable();
        context.partitions.dedup();
        ensure!(
            context
                .partitions
                .iter()
                .all(|p| *p < context.shuffle.partition_count as usize),
            "partition ID outside shuffle"
        );
        let routes = assignment(&context.shuffle)?;
        Ok(Self { context, routes })
    }

    pub fn stream(self) -> ShuffleStream {
        let partitions = VecDeque::from(self.context.partitions.clone());
        Box::pin(futures::stream::try_unfold(
            (self, partitions, VecDeque::new()),
            |(reader, mut partitions, mut records)| async move {
                loop {
                    if let Some(record) = records.pop_front() {
                        return Ok(Some((record, (reader, partitions, records))));
                    }
                    let Some(partition) = partitions.pop_front() else {
                        return Ok(None);
                    };
                    records = reader.read_partition(partition).await?;
                }
            },
        ))
    }

    async fn read_partition(&self, partition: PartitionId) -> Result<VecDeque<ShuffleRecord>> {
        let mut expected = HashSet::new();
        let mut tasks = Treemap::new();
        for result in &self.context.write_results {
            tasks.add(result.task_id as u64);
            for sequence in 0..result.blocks_number.get(&partition).copied().unwrap_or(0) {
                expected.insert(DEFAULT_BLOCK_ID_LAYOUT.get_block_id(
                    sequence.into(),
                    partition as i64,
                    result.task_id as i64,
                ));
            }
        }
        if expected.is_empty() {
            return Ok(VecDeque::new());
        }
        let shuffle_id = i32::try_from(self.context.shuffle.shuffle_id)
            .context("shuffle ID exceeds protocol i32 range")?;
        let endpoint = self
            .routes
            .get(&partition)
            .context("partition has no assignment")?;
        let mut client = ShuffleServerClient::new(connect(&endpoint.uri()).await?)
            .max_decoding_message_size(MAX_MESSAGE_BYTES);
        let mut blocks = BTreeMap::new();
        let mut total_bytes = 0;
        // shortcut: buffer one bounded partition, add incremental prefetch for large shuffles.
        for _ in 0..5 {
            let mut cursor = -1;
            loop {
                let previous_cursor = cursor;
                let response = client
                    .get_memory_shuffle_data(GetMemoryShuffleDataRequest {
                        app_id: self.context.shuffle.app_id.clone(),
                        shuffle_id,
                        partition_id: partition as i32,
                        last_block_id: cursor,
                        read_buffer_size: 4 * 1024 * 1024,
                        timestamp: now_timestamp_as_millis() as i64,
                        serialized_expected_task_ids_bitmap: Bytes::from(
                            tasks.serialize::<JvmLegacy>(),
                        ),
                    })
                    .await?
                    .into_inner();
                check_status("getMemoryShuffleData", response.status, &response.ret_msg)?;
                let empty = response.shuffle_data_block_segments.is_empty();
                for segment in response.shuffle_data_block_segments {
                    ensure!(
                        segment.offset >= 0 && segment.length >= 0,
                        "invalid memory segment"
                    );
                    ensure!(
                        segment.task_attempt_id == segment.block_id & (TASK_ID_LIMIT as i64 - 1),
                        "memory block task ID mismatch"
                    );
                    let start = usize::try_from(segment.offset)?;
                    let end = start
                        .checked_add(segment.length as usize)
                        .context("segment overflow")?;
                    ensure!(end <= response.data.len(), "truncated memory segment");
                    cursor = segment.block_id;
                    if expected.contains(&segment.block_id)
                        && !blocks.contains_key(&segment.block_id)
                    {
                        let data = response.data.slice(start..end);
                        ensure!(get_crc(&data) == segment.crc, "memory block CRC mismatch");
                        total_bytes += data.len();
                        ensure!(
                            total_bytes <= MAX_PARTITION_BYTES,
                            "partition exceeds read buffer limit"
                        );
                        blocks.insert(segment.block_id, data);
                    }
                }
                if empty
                    || response.is_end.unwrap_or(false)
                    || blocks.len() == expected.len()
                    || cursor == previous_cursor
                {
                    break;
                }
            }
            if blocks.len() == expected.len() {
                break;
            }
            let index = client
                .get_local_shuffle_index(GetLocalShuffleIndexRequest {
                    app_id: self.context.shuffle.app_id.clone(),
                    shuffle_id,
                    partition_id: partition as i32,
                    partition_num_per_range: 1,
                    partition_num: self.context.shuffle.partition_count as i32,
                })
                .await?
                .into_inner();
            check_status("getLocalShuffleIndex", index.status, &index.ret_msg)?;
            ensure!(
                index.index_data.len() % INDEX_BLOCK_SIZE == 0,
                "truncated localfile index"
            );
            for entry in index.index_data.chunks(INDEX_BLOCK_SIZE) {
                let entry = IndexCodec::decode(Bytes::copy_from_slice(entry))?;
                ensure!(
                    entry.task_attempt_id == entry.block_id & (TASK_ID_LIMIT as i64 - 1),
                    "localfile block task ID mismatch"
                );
                if !expected.contains(&entry.block_id) || blocks.contains_key(&entry.block_id) {
                    continue;
                }
                ensure!(
                    entry.offset >= 0
                        && entry.length >= 0
                        && entry
                            .offset
                            .checked_add(entry.length as i64)
                            .is_some_and(|end| end <= index.data_file_len),
                    "invalid localfile range"
                );
                let response = client
                    .get_local_shuffle_data(GetLocalShuffleDataRequest {
                        app_id: self.context.shuffle.app_id.clone(),
                        shuffle_id,
                        partition_id: partition as i32,
                        partition_num_per_range: 1,
                        partition_num: self.context.shuffle.partition_count as i32,
                        offset: entry.offset,
                        length: entry.length,
                        timestamp: now_timestamp_as_millis() as i64,
                        storage_id: 0,
                    })
                    .await?
                    .into_inner();
                check_status("getLocalShuffleData", response.status, &response.ret_msg)?;
                ensure!(
                    response.data.len() == entry.length as usize
                        && get_crc(&response.data) == entry.crc,
                    "localfile block length or CRC mismatch"
                );
                total_bytes += response.data.len();
                ensure!(
                    total_bytes <= MAX_PARTITION_BYTES,
                    "partition exceeds read buffer limit"
                );
                blocks.insert(entry.block_id, response.data);
            }
            if blocks.len() == expected.len() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        ensure!(
            blocks.len() == expected.len(),
            "shuffle partition {partition} is missing {} blocks",
            expected.len() - blocks.len()
        );
        Ok(blocks
            .into_values()
            .map(|data| ShuffleRecord {
                partition_id: partition,
                data,
            })
            .collect())
    }
}
