use super::{BatchMemoryBlock, BufferOps, BufferSpillResult};
use crate::composed_bytes;
use crate::composed_bytes::ComposedBytes;
use crate::constant::INVALID_BLOCK_ID;
use crate::store::DataBytes;
use crate::store::{Block, DataSegment, PartitionedMemoryData};
use anyhow::Result;
use croaring::Treemap;
use fastrace::trace;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::hash::Hash;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

#[derive(Debug)]
pub struct BufferInternal {
    pub total_size: i64,
    pub staging_size: i64,
    pub flight_size: i64,

    pub staging: BatchMemoryBlock,

    pub flight: HashMap<u64, Arc<BatchMemoryBlock>>,
    pub flight_counter: u64,
}

impl BufferInternal {
    pub fn new() -> Self {
        BufferInternal {
            total_size: 0,
            staging_size: 0,
            flight_size: 0,
            staging: Default::default(),
            flight: Default::default(),
            flight_counter: 0,
        }
    }
}
pub struct MemoryBuffer {
    buffer: Mutex<BufferInternal>,
}

impl BufferOps for MemoryBuffer {
    fn new() -> MemoryBuffer {
        MemoryBuffer {
            buffer: Mutex::new(BufferInternal::new()),
        }
    }

    fn total_size(&self) -> Result<i64>
    where
        Self: Send + Sync,
    {
        return Ok(self.buffer.lock().total_size);
    }

    fn flight_size(&self) -> Result<i64>
    where
        Self: Send + Sync,
    {
        return Ok(self.buffer.lock().flight_size);
    }

    fn staging_size(&self) -> Result<i64>
    where
        Self: Send + Sync,
    {
        return Ok(self.buffer.lock().staging_size);
    }

    fn clear(&self, flight_id: u64, flight_size: u64) -> Result<()>
    where
        Self: Send + Sync,
    {
        let mut buffer = self.buffer.lock();
        let flight = &mut buffer.flight;
        let removed = flight.remove(&flight_id);
        if let Some(block_ref) = removed {
            buffer.total_size -= flight_size as i64;
            buffer.flight_size -= flight_size as i64;
        }
        Ok(())
    }

    fn get(
        &self,
        last_block_id: i64,
        read_bytes_limit_len: i64,
        task_ids: Option<Treemap>,
    ) -> Result<PartitionedMemoryData>
    where
        Self: Send + Sync,
    {
        /// read sequence
        /// 1. from flight (expect: last_block_id not found or last_block_id == -1)
        /// 2. from staging
        let buffer = self.buffer.lock();

        let mut read_result = vec![];
        let mut read_len = 0i64;
        let mut flight_found = false;

        let mut exit = false;
        while !exit {
            exit = true;
            {
                if last_block_id == INVALID_BLOCK_ID {
                    flight_found = true;
                }
                for (_, batch_block) in buffer.flight.iter() {
                    for blocks in batch_block.iter() {
                        for block in blocks {
                            if !flight_found && block.block_id == last_block_id {
                                flight_found = true;
                                continue;
                            }
                            if !flight_found {
                                continue;
                            }
                            if read_len >= read_bytes_limit_len {
                                break;
                            }
                            if let Some(ref expected_task_id) = task_ids {
                                if !expected_task_id.contains(block.task_attempt_id as u64) {
                                    continue;
                                }
                            }
                            read_len += block.length as i64;
                            read_result.push(block);
                        }
                    }
                }
            }

            {
                for blocks in buffer.staging.iter() {
                    for block in blocks {
                        if !flight_found && block.block_id == last_block_id {
                            flight_found = true;
                            continue;
                        }
                        if !flight_found {
                            continue;
                        }
                        if read_len >= read_bytes_limit_len {
                            break;
                        }
                        if let Some(ref expected_task_id) = task_ids {
                            if !expected_task_id.contains(block.task_attempt_id as u64) {
                                continue;
                            }
                        }
                        read_len += block.length as i64;
                        read_result.push(block);
                    }
                }
            }

            if !flight_found {
                flight_found = true;
                exit = false;
            }
        }

        let mut block_bytes = Vec::with_capacity(read_result.len());
        let mut segments = Vec::with_capacity(read_result.len());
        let mut offset = 0;
        for block in read_result {
            let data = &block.data;
            block_bytes.push(data.clone());
            segments.push(DataSegment {
                block_id: block.block_id,
                offset,
                length: block.length,
                uncompress_length: block.uncompress_length,
                crc: block.crc,
                task_attempt_id: block.task_attempt_id,
            });
            offset += block.length as i64;
        }
        let total_bytes = offset as usize;

        // Note: is_end is computed as total_bytes < read_bytes_limit_len. This works in general,
        // but it can incorrectly be false in the edge case where total_bytes == read_bytes_limit_len
        // and the buffer has no more blocks left. In that situation, buffer is actually fully read,
        // so the client code may need to perform an additional empty-check to handle this case.
        let is_end = total_bytes < read_bytes_limit_len as usize;

        let composed_bytes = ComposedBytes::from(block_bytes, total_bytes);
        Ok(PartitionedMemoryData {
            shuffle_data_block_segments: segments,
            data: DataBytes::Composed(composed_bytes),
            is_end,
        })
    }

    // when there is no any staging data, it will return the None
    fn spill(&self) -> Result<Option<BufferSpillResult>>
    where
        Self: Send + Sync,
    {
        let mut buffer = self.buffer.lock();
        if buffer.staging_size == 0 {
            return Ok(None);
        }

        let staging: BatchMemoryBlock = { mem::replace(&mut buffer.staging, Default::default()) };
        let staging_ref = Arc::new(staging);
        let flight_id = buffer.flight_counter;

        let flight = &mut buffer.flight;
        flight.insert(flight_id, staging_ref.clone());

        let spill_size = buffer.staging_size;
        buffer.flight_counter += 1;
        buffer.flight_size += spill_size;
        buffer.staging_size = 0;

        Ok(Some(BufferSpillResult {
            flight_id,
            flight_len: spill_size as u64,
            blocks: staging_ref.clone(),
        }))
    }

    fn append(&self, blocks: Vec<Block>, size: u64) -> Result<()>
    where
        Self: Send + Sync,
    {
        let mut buffer = self.buffer.lock();
        let mut staging = &mut buffer.staging;
        staging.push(blocks);

        buffer.staging_size += size as i64;
        buffer.total_size += size as i64;

        Ok(())
    }

    #[cfg(test)]
    fn direct_push(&self, blocks: Vec<Block>) -> Result<()>
    where
        Self: Send + Sync,
    {
        let len: u64 = blocks.iter().map(|block| block.length).sum::<i32>() as u64;
        self.append(blocks, len)
    }
}
