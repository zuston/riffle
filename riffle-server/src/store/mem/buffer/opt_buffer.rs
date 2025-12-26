use crate::composed_bytes::ComposedBytes;
use crate::constant::INVALID_BLOCK_ID;
use crate::store::mem::buffer::default_buffer::DefaultMemoryBuffer;
use crate::store::mem::buffer::{BufferOptions, BufferSpillResult, MemBlockBatch, MemoryBuffer};
use crate::store::{Block, DataBytes, DataSegment, PartitionedMemoryData};
use croaring::Treemap;
use fastrace::trace;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

/// the optimized implementation is from https://github.com/zuston/riffle/pull/564
#[derive(Debug)]
pub struct OptStagingBufferInternal {
    pub total_size: i64,
    pub staging_size: i64,
    pub flight_size: i64,

    pub staging: Vec<Block>,
    pub batch_boundaries: Vec<usize>, // Track where each batch starts
    pub block_position_index: HashMap<i64, usize>, // Maps block_id to Vec index

    pub flight: HashMap<u64, Arc<MemBlockBatch>>,
    pub flight_counter: u64,
}

impl OptStagingBufferInternal {
    pub fn new() -> Self {
        OptStagingBufferInternal {
            total_size: 0,
            staging_size: 0,
            flight_size: 0,
            staging: Vec::new(),
            batch_boundaries: Vec::new(),
            block_position_index: HashMap::new(),
            flight: Default::default(),
            flight_counter: 0,
        }
    }
}

#[derive(Debug)]
pub struct OptStagingMemoryBuffer {
    buffer: Mutex<OptStagingBufferInternal>,
}

impl MemoryBuffer for OptStagingMemoryBuffer {
    #[trace]
    fn new(opt: BufferOptions) -> Self {
        OptStagingMemoryBuffer {
            buffer: Mutex::new(OptStagingBufferInternal::new()),
        }
    }
    #[trace]
    fn total_size(&self) -> anyhow::Result<i64>
    where
        Self: Send + Sync,
    {
        return Ok(self.buffer.lock().total_size);
    }

    #[trace]
    fn flight_size(&self) -> anyhow::Result<i64>
    where
        Self: Send + Sync,
    {
        return Ok(self.buffer.lock().flight_size);
    }

    #[trace]
    fn staging_size(&self) -> anyhow::Result<i64>
    where
        Self: Send + Sync,
    {
        return Ok(self.buffer.lock().staging_size);
    }

    #[trace]
    fn clear(&self, flight_id: u64, flight_size: u64) -> anyhow::Result<()>
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

    #[trace]
    fn get(
        &self,
        last_block_id: i64,
        read_bytes_limit_len: i64,
        task_ids: Option<Treemap>,
    ) -> anyhow::Result<PartitionedMemoryData>
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

        const FIRST_ATTEMP: u8 = 0;
        const FALLBACK: u8 = 1;
        let strategies = [FIRST_ATTEMP, FALLBACK];

        for loop_index in strategies {
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

            // Handle staging with Vec + index optimization
            let staging_start_idx = if loop_index == FIRST_ATTEMP && !flight_found {
                // Try to find position after last_block_id
                // Always set flight_found = true for the next searching
                flight_found = true;
                if let Some(&position) = buffer.block_position_index.get(&last_block_id) {
                    position + 1
                } else {
                    // Not found in staging, will handle in fallback
                    continue;
                }
            } else {
                // Fallback: read from beginning
                0
            };

            for block in &buffer.staging[staging_start_idx..] {
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

            // // If we found data in first attempt, no need for fallback
            if flight_found && loop_index == FIRST_ATTEMP {
                break;
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
    #[trace]
    fn spill(&self) -> anyhow::Result<Option<BufferSpillResult>> {
        let mut buffer = self.buffer.lock();
        if buffer.staging_size == 0 {
            return Ok(None);
        }

        // Reconstruct batches from boundaries
        let mut batches = Vec::new();
        let mut start = 0;
        for i in 0..buffer.batch_boundaries.len() {
            let end = buffer.batch_boundaries[i];
            if end >= buffer.staging.len() {
                break;
            }

            // Find next boundary or use end of staging
            let next_boundary = if i + 1 < buffer.batch_boundaries.len() {
                buffer.batch_boundaries[i + 1]
            } else {
                buffer.staging.len()
            };

            batches.push(buffer.staging[start..next_boundary].to_vec());
            start = next_boundary;
        }

        let staging: MemBlockBatch = MemBlockBatch(batches);

        // Clear everything
        buffer.staging.clear();
        buffer.block_position_index.clear();
        buffer.batch_boundaries.clear();

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

    #[trace]
    fn append(&self, blocks: Vec<Block>, size: u64) -> anyhow::Result<()> {
        let mut buffer = self.buffer.lock();
        let current_position = buffer.staging.len();
        let block_count = blocks.len();

        // Pre-allocate capacities
        buffer.staging.reserve(block_count);
        buffer.block_position_index.reserve(block_count);

        // Record batch boundary
        if !blocks.is_empty() {
            buffer.batch_boundaries.push(current_position);
        }

        for (idx, block) in blocks.into_iter().enumerate() {
            buffer
                .block_position_index
                .insert(block.block_id, current_position + idx);
            buffer.staging.push(block);
        }
        buffer.staging_size += size as i64;
        buffer.total_size += size as i64;
        Ok(())
    }

    #[cfg(test)]
    #[trace]
    fn direct_push(&self, blocks: Vec<Block>) -> anyhow::Result<()> {
        let len: u64 = blocks.iter().map(|block| block.length).sum::<i32>() as u64;
        self.append(blocks, len)
    }
}
