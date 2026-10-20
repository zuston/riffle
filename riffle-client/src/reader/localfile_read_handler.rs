use std::collections::HashSet;

use anyhow::{ensure, Context, Result};
use bytes::{Buf, Bytes};
use riffle_server::grpc::protobuf::uniffle::{
    GetLocalShuffleDataRequest, GetLocalShuffleIndexRequest, GetLocalShuffleIndexResponse,
};
use riffle_server::store::index_codec::{IndexBlock, IndexCodec, INDEX_BLOCK_SIZE};
use riffle_server::util::now_timestamp_as_millis;

use super::buffer::ReadBatch;
use super::read_handler::{PartitionReadContext, ReadHandler, READ_BATCH_BYTES};
use crate::rpc_client::{call, ensure_attempt_success, request, retry};

pub(super) struct LocalfileReadHandler<'a> {
    context: PartitionReadContext<'a>,
    block_ids: HashSet<i64>,
    index: Option<GetLocalShuffleIndexResponse>,
}

impl<'a> LocalfileReadHandler<'a> {
    pub fn new(context: PartitionReadContext<'a>, block_ids: HashSet<i64>) -> Self {
        Self {
            context,
            block_ids,
            index: None,
        }
    }

    async fn read_index(&self) -> Result<GetLocalShuffleIndexResponse> {
        let message = GetLocalShuffleIndexRequest {
            app_id: self.context.shuffle.app_id.clone(),
            shuffle_id: self.context.shuffle.shuffle_id as i32,
            partition_id: self.context.partition as i32,
            partition_num_per_range: 1,
            partition_num: self.context.shuffle.partition_count as i32,
        };
        let index = retry(&self.context.endpoint, "getLocalShuffleIndex", || {
            let mut client = self.context.client.clone();
            let message = message.clone();
            async move {
                let response = call(client.get_local_shuffle_index(request(message))).await?;
                ensure_attempt_success("getLocalShuffleIndex", response.status, &response.ret_msg)?;
                Ok(response)
            }
        })
        .await?;
        ensure!(index.data_file_len >= 0, "negative localfile length");
        Ok(index)
    }

    async fn read_range(&self, offset: i64, length: i32) -> Result<Bytes> {
        let message = GetLocalShuffleDataRequest {
            app_id: self.context.shuffle.app_id.clone(),
            shuffle_id: self.context.shuffle.shuffle_id as i32,
            partition_id: self.context.partition as i32,
            partition_num_per_range: 1,
            partition_num: self.context.shuffle.partition_count as i32,
            offset,
            length,
            timestamp: 0,
            storage_id: 0,
        };
        let response = retry(&self.context.endpoint, "getLocalShuffleData", || {
            let mut client = self.context.client.clone();
            let mut message = message.clone();
            message.timestamp = now_timestamp_as_millis() as i64;
            async move {
                let response = call(client.get_local_shuffle_data(request(message))).await?;
                ensure_attempt_success("getLocalShuffleData", response.status, &response.ret_msg)?;
                Ok(response)
            }
        })
        .await?;
        ensure!(
            response.data.len() == length as usize,
            "truncated localfile range"
        );
        Ok(response.data)
    }
}

#[tonic::async_trait]
impl ReadHandler for LocalfileReadHandler<'_> {
    async fn read(&mut self) -> Result<Option<ReadBatch>> {
        if self.block_ids.is_empty() {
            return Ok(None);
        }
        if self.index.is_none() {
            self.index = Some(self.read_index().await?);
        }
        let index = self.index.as_mut().unwrap();
        let mut entries: Vec<IndexBlock> = Vec::new();
        let mut length = 0;
        while index.index_data.len() >= INDEX_BLOCK_SIZE {
            let entry = IndexCodec::decode(index.index_data.slice(..INDEX_BLOCK_SIZE))?;
            if !self.block_ids.contains(&entry.block_id) {
                index.index_data.advance(INDEX_BLOCK_SIZE);
                continue;
            }
            self.context.validate_block(
                entry.block_id,
                entry.task_attempt_id,
                entry.length,
                entry.uncompress_length,
            )?;
            ensure!(entry.offset >= 0, "negative localfile offset");
            let end = entry
                .offset
                .checked_add(entry.length as i64)
                .context("localfile range overflow")?;
            if end > index.data_file_len {
                index.index_data.advance(INDEX_BLOCK_SIZE);
                continue;
            }
            if let Some(previous) = entries.last() {
                if entry.offset != previous.offset + previous.length as i64
                    || length + entry.length > READ_BATCH_BYTES
                {
                    break;
                }
            }
            index.index_data.advance(INDEX_BLOCK_SIZE);
            self.block_ids.remove(&entry.block_id);
            length += entry.length;
            entries.push(entry);
        }
        let Some(first) = entries.first() else {
            return Ok(None);
        };
        let offset = first.offset;
        let data = self.read_range(offset, length).await?;
        let mut batch = ReadBatch::new(self.context.partition);
        for entry in entries {
            let start = (entry.offset - offset) as usize;
            let end = start + entry.length as usize;
            batch.push(entry.block_id, data.slice(start..end), entry.crc);
        }
        Ok(Some(batch))
    }
}
