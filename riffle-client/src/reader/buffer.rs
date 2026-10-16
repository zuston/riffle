use std::collections::{HashSet, VecDeque};

use anyhow::{ensure, Context, Result};
use bytes::{Buf, Bytes};
use riffle_server::util::get_crc;

use crate::{PartitionId, ShuffleRecord, MAX_PAYLOAD_BYTES, RECORD_HEADER_BYTES};

pub(super) struct ReadBatch {
    partition_id: PartitionId,
    blocks: VecDeque<ReadBlock>,
}

struct ReadBlock {
    block_id: i64,
    data: Bytes,
    crc: i64,
}

impl ReadBatch {
    pub(super) fn new(partition_id: PartitionId) -> Self {
        Self {
            partition_id,
            blocks: VecDeque::new(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    pub(super) fn push(&mut self, block_id: i64, data: Bytes, crc: i64) {
        self.blocks.push_back(ReadBlock {
            block_id,
            data,
            crc,
        });
    }

    pub(super) fn validate_and_filter(
        self,
        expected_block_ids: &HashSet<i64>,
        accepted_block_ids: &mut HashSet<i64>,
    ) -> Result<Self> {
        let mut batch = Self::new(self.partition_id);
        for block in self.blocks {
            if !expected_block_ids.contains(&block.block_id)
                || accepted_block_ids.contains(&block.block_id)
            {
                continue;
            }
            let validate = || -> Result<()> {
                ensure!(!block.data.is_empty(), "empty shuffle block");
                ensure!(get_crc(&block.data) == block.crc, "block CRC mismatch");
                let mut records = block.data.clone();
                while !records.is_empty() {
                    decode_record(&mut records)?;
                }
                Ok(())
            };
            validate().with_context(|| {
                format!(
                    "invalid block {} in partition {}",
                    block.block_id, self.partition_id
                )
            })?;
            accepted_block_ids.insert(block.block_id);
            batch.blocks.push_back(block);
        }
        Ok(batch)
    }

    pub(super) fn next_record(&mut self) -> Result<Option<ShuffleRecord>> {
        let Some(block) = self.blocks.front_mut() else {
            return Ok(None);
        };
        let record = ShuffleRecord {
            partition_id: self.partition_id,
            data: decode_record(&mut block.data)?,
        };
        if block.data.is_empty() {
            self.blocks.pop_front();
        }
        Ok(Some(record))
    }
}

// todo: align with the writer side codec
fn decode_record(data: &mut Bytes) -> Result<Bytes> {
    ensure!(data.len() >= RECORD_HEADER_BYTES, "truncated record header");
    let length = data.get_u32() as usize;
    ensure!(length <= MAX_PAYLOAD_BYTES, "record exceeds payload limit");
    ensure!(length <= data.len(), "truncated record payload");
    Ok(data.split_to(length))
}
