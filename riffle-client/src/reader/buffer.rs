use std::collections::{HashSet, VecDeque};

use anyhow::{ensure, Context, Result};
use bytes::{Buf, Bytes};
use riffle_server::util::get_crc;

use crate::codec::{run_codec_task, Codec};
use crate::{CompressionCodec, PartitionId, ShuffleRecord, MAX_PAYLOAD_BYTES, RECORD_HEADER_BYTES};

pub(super) struct ReadBatch {
    partition_id: PartitionId,
    blocks: VecDeque<ReadBlock>,
    records: Bytes,
    codec: &'static dyn Codec,
}

struct ReadBlock {
    block_id: i64,
    data: Bytes,
    crc: i64,
    uncompress_length: usize,
}

impl ReadBatch {
    pub(super) fn new(partition_id: PartitionId, compression_codec: CompressionCodec) -> Self {
        Self {
            partition_id,
            blocks: VecDeque::new(),
            records: Bytes::new(),
            codec: compression_codec.codec(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.blocks.is_empty() && self.records.is_empty()
    }

    pub(super) fn block_ids(&self) -> impl Iterator<Item = i64> + '_ {
        self.blocks.iter().map(|block| block.block_id)
    }

    pub(super) fn push(&mut self, block_id: i64, data: Bytes, crc: i64, uncompress_length: usize) {
        self.blocks.push_back(ReadBlock {
            block_id,
            data,
            crc,
            uncompress_length,
        });
    }

    pub(super) fn filter(
        mut self,
        expected_block_ids: &HashSet<i64>,
        accepted_block_ids: &mut HashSet<i64>,
    ) -> Self {
        self.blocks.retain(|block| {
            expected_block_ids.contains(&block.block_id)
                && accepted_block_ids.insert(block.block_id)
        });
        self
    }

    pub(super) async fn next_record(&mut self) -> Result<Option<ShuffleRecord>> {
        if self.records.is_empty() {
            let Some(block) = self.blocks.pop_front() else {
                return Ok(None);
            };
            let block_id = block.block_id;
            let codec = self.codec;
            // todo: make this async and run in a separate thread pool to avoid blocking the main thread
            self.records = run_codec_task(move || {
                ensure!(get_crc(&block.data) == block.crc, "block CRC mismatch");
                let data = codec.decode(block.data, block.uncompress_length)?;
                let mut records = data.clone();
                while !records.is_empty() {
                    decode_record(&mut records)?;
                }
                Ok(data)
            })
            .await
            .with_context(|| {
                format!(
                    "invalid block {block_id} in partition {}",
                    self.partition_id
                )
            })?;
        }
        let record = ShuffleRecord {
            partition_id: self.partition_id,
            data: decode_record(&mut self.records)?,
        };
        Ok(Some(record))
    }
}

fn decode_record(data: &mut Bytes) -> Result<Bytes> {
    ensure!(data.len() >= RECORD_HEADER_BYTES, "truncated record header");
    let length = data.get_u32() as usize;
    ensure!(length <= MAX_PAYLOAD_BYTES, "record exceeds payload limit");
    ensure!(length <= data.len(), "truncated record payload");
    Ok(data.split_to(length))
}
