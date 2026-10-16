use std::collections::HashSet;

use anyhow::{ensure, Context, Result};
use bytes::Bytes;
use croaring::{JvmLegacy, Treemap};
use prost::Message;
use riffle_server::grpc::protobuf::uniffle::{
    GetMemoryShuffleDataRequest, GetMemoryShuffleDataResponse,
};
use riffle_server::util::now_timestamp_as_millis;

use super::buffer::ReadBatch;
use super::read_handler::{PartitionReadContext, ReadHandler, READ_BATCH_BYTES};
use crate::rpc_client::transport::{call, check_status, request, retry};
use crate::{MAX_MESSAGE_BYTES, TASK_ID_LIMIT};

pub(super) struct MemoryReadHandler<'a> {
    context: PartitionReadContext<'a>,
    tasks: Bytes,
    cursor: i64,
    seen_cursors: HashSet<i64>,
    finished: bool,
}

impl<'a> MemoryReadHandler<'a> {
    pub fn new(context: PartitionReadContext<'a>, block_ids: &HashSet<i64>) -> Self {
        let mut tasks = Treemap::new();
        for &block_id in block_ids {
            tasks.add((block_id & (TASK_ID_LIMIT as i64 - 1)) as u64);
        }
        Self {
            context,
            tasks: Bytes::from(tasks.serialize::<JvmLegacy>()),
            cursor: -1,
            seen_cursors: HashSet::new(),
            finished: false,
        }
    }

    async fn read_page(&self) -> Result<GetMemoryShuffleDataResponse> {
        let message = GetMemoryShuffleDataRequest {
            app_id: self.context.shuffle.app_id.clone(),
            shuffle_id: self.context.shuffle.shuffle_id as i32,
            partition_id: self.context.partition as i32,
            last_block_id: self.cursor,
            read_buffer_size: READ_BATCH_BYTES,
            timestamp: -1,
            serialized_expected_task_ids_bitmap: self.tasks.clone(),
        };
        ensure!(
            message.encoded_len() <= MAX_MESSAGE_BYTES,
            "memory read request exceeds RPC message limit"
        );
        retry(&self.context.endpoint, "getMemoryShuffleData", || {
            let mut client = self.context.client.clone();
            let mut message = message.clone();
            message.timestamp = now_timestamp_as_millis() as i64;
            async move {
                let response = call(client.get_memory_shuffle_data(request(message))).await?;
                check_status("getMemoryShuffleData", response.status, &response.ret_msg)?;
                Ok(response)
            }
        })
        .await
    }
}

#[tonic::async_trait]
impl ReadHandler for MemoryReadHandler<'_> {
    async fn read(&mut self) -> Result<Option<ReadBatch>> {
        if self.finished {
            return Ok(None);
        }
        let response = self.read_page().await?;
        let Some(last) = response.shuffle_data_block_segments.last() else {
            self.finished = true;
            return Ok(None);
        };
        let next_cursor = last.block_id;
        let mut batch = ReadBatch::new(self.context.partition);
        for segment in response.shuffle_data_block_segments {
            self.context.validate_block(
                segment.block_id,
                segment.task_attempt_id,
                segment.length,
                segment.uncompress_length,
            )?;
            let start = usize::try_from(segment.offset).context("negative memory offset")?;
            let end = start
                .checked_add(segment.length as usize)
                .context("memory segment overflow")?;
            ensure!(end <= response.data.len(), "truncated memory segment");
            batch.push(
                segment.block_id,
                response.data.slice(start..end),
                segment.crc,
            );
        }
        self.cursor = next_cursor;
        self.finished = response.is_end.unwrap_or(false) || !self.seen_cursors.insert(next_cursor);
        Ok(Some(batch))
    }
}
