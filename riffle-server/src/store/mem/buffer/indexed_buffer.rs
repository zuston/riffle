use crate::composed_bytes::ComposedBytes;
use crate::constant::INVALID_BLOCK_ID;
use crate::store::mem::buffer::{BufferOptions, BufferSpillResult, MemBlockBatch, MemoryBuffer};
use crate::store::{Block, DataBytes, DataSegment, PartitionedMemoryData};
use croaring::Treemap;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::mem;
use std::sync::Arc;

/// The indexed implementation is from https://github.com/zuston/riffle/pull/564.
///
/// The staging layout is identical to the default buffer (batches of blocks),
/// so `append` stays an O(1) batch move. The block-position index is built
/// lazily on the read path: it maps `block_id` to `(batch_idx, offset_in_batch)`
/// and is caught up incrementally (tracked by `indexed_batches`) only when a
/// `get` actually needs to locate a cursor inside staging. Data that is spilled
/// before ever being read this way never pays any indexing cost.
#[derive(Debug)]
pub struct IndexedBufferInternal {
    pub total_size: i64,
    pub staging_size: i64,
    pub flight_size: i64,

    pub staging: MemBlockBatch,
    pub block_position_index: HashMap<i64, (usize, usize)>,
    // Number of leading staging batches already covered by the index.
    pub indexed_batches: usize,

    pub flight: HashMap<u64, Arc<MemBlockBatch>>,
    pub flight_counter: u64,
}

impl IndexedBufferInternal {
    pub fn new() -> Self {
        IndexedBufferInternal {
            total_size: 0,
            staging_size: 0,
            flight_size: 0,
            staging: Default::default(),
            block_position_index: HashMap::new(),
            indexed_batches: 0,
            flight: Default::default(),
            flight_counter: 0,
        }
    }

    // Index the staging batches appended since the last catch-up.
    fn catch_up_index(&mut self) {
        let total_batches = self.staging.len();
        if self.indexed_batches >= total_batches {
            return;
        }

        let pending_blocks: usize = self.staging[self.indexed_batches..]
            .iter()
            .map(|batch| batch.len())
            .sum();
        self.block_position_index.reserve(pending_blocks);

        for batch_idx in self.indexed_batches..total_batches {
            for (block_idx, block) in self.staging[batch_idx].iter().enumerate() {
                self.block_position_index
                    .insert(block.block_id, (batch_idx, block_idx));
            }
        }
        self.indexed_batches = total_batches;
    }
}

#[derive(Debug)]
pub struct IndexedMemoryBuffer {
    buffer: Mutex<IndexedBufferInternal>,
}

impl MemoryBuffer for IndexedMemoryBuffer {
    fn new(_options: BufferOptions) -> Self {
        IndexedMemoryBuffer {
            buffer: Mutex::new(IndexedBufferInternal::new()),
        }
    }
    fn total_size(&self) -> anyhow::Result<i64>
    where
        Self: Send + Sync,
    {
        return Ok(self.buffer.lock().total_size);
    }

    fn flight_size(&self) -> anyhow::Result<i64>
    where
        Self: Send + Sync,
    {
        return Ok(self.buffer.lock().flight_size);
    }

    fn staging_size(&self) -> anyhow::Result<i64>
    where
        Self: Send + Sync,
    {
        return Ok(self.buffer.lock().staging_size);
    }

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
        let mut buffer = self.buffer.lock();

        // The index catch-up must happen before any block reference is taken,
        // because it mutates the internal state.
        if last_block_id != INVALID_BLOCK_ID {
            buffer.catch_up_index();
        }
        let buffer = &*buffer;

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

            // Handle staging with the block-position index optimization
            let (start_batch, start_block) = if loop_index == FIRST_ATTEMP && !flight_found {
                // Try to find position after last_block_id
                // Always set flight_found = true for the next searching
                flight_found = true;
                if let Some(&(batch_idx, block_idx)) =
                    buffer.block_position_index.get(&last_block_id)
                {
                    (batch_idx, block_idx + 1)
                } else {
                    // Not found in staging, will handle in fallback
                    continue;
                }
            } else {
                // Fallback: read from beginning
                (0, 0)
            };

            'staging: for (batch_idx, blocks) in buffer.staging.iter().enumerate().skip(start_batch)
            {
                let skip = if batch_idx == start_batch {
                    start_block
                } else {
                    0
                };
                for block in &blocks[skip.min(blocks.len())..] {
                    if read_len >= read_bytes_limit_len {
                        break 'staging;
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
    fn spill(&self) -> anyhow::Result<Option<BufferSpillResult>> {
        let mut buffer = self.buffer.lock();
        if buffer.staging_size == 0 {
            return Ok(None);
        }

        let staging: MemBlockBatch = mem::replace(&mut buffer.staging, Default::default());
        buffer.block_position_index.clear();
        buffer.indexed_batches = 0;

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

    fn append(&self, blocks: Vec<Block>, size: u64) -> anyhow::Result<()> {
        let mut buffer = self.buffer.lock();
        buffer.staging.push(blocks);

        buffer.staging_size += size as i64;
        buffer.total_size += size as i64;
        Ok(())
    }

    #[cfg(test)]
    fn direct_push(&self, blocks: Vec<Block>) -> anyhow::Result<()> {
        let len: u64 = blocks.iter().map(|block| block.length).sum::<i32>() as u64;
        self.append(blocks, len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::test_utils::create_blocks;

    #[test]
    fn spill_moves_staging_allocation() -> anyhow::Result<()> {
        let buffer = IndexedMemoryBuffer::new(Default::default());
        buffer.append(create_blocks(0, 4, 10), 40)?;

        let batch_ptr = buffer.buffer.lock().staging[0].as_ptr();
        let spill_result = buffer
            .spill()?
            .expect("staging blocks should produce a spill result");

        assert_eq!(1, spill_result.blocks.len());
        assert_eq!(batch_ptr, spill_result.blocks[0].as_ptr());
        assert!(buffer.buffer.lock().staging.is_empty());
        Ok(())
    }

    #[test]
    fn lazy_index_catches_up_on_read() -> anyhow::Result<()> {
        let buffer = IndexedMemoryBuffer::new(Default::default());
        buffer.append(create_blocks(0, 4, 10), 40)?;
        buffer.append(create_blocks(4, 4, 10), 40)?;

        // Append alone should not build the index.
        assert!(buffer.buffer.lock().block_position_index.is_empty());

        // A cursor read inside staging triggers the catch-up.
        let result = buffer.get(3, 40, None)?;
        assert_eq!(4, result.shuffle_data_block_segments.len());
        assert_eq!(3 + 4, result.shuffle_data_block_segments[3].block_id);
        {
            let internal = buffer.buffer.lock();
            assert_eq!(8, internal.block_position_index.len());
            assert_eq!(2, internal.indexed_batches);
        }

        // New appends after a catch-up are indexed incrementally on next read.
        buffer.append(create_blocks(8, 4, 10), 40)?;
        let result = buffer.get(7, 40, None)?;
        assert_eq!(4, result.shuffle_data_block_segments.len());
        assert_eq!(11, result.shuffle_data_block_segments[3].block_id);
        assert_eq!(12, buffer.buffer.lock().block_position_index.len());

        // Spill drops the index together with staging.
        buffer.spill()?;
        {
            let internal = buffer.buffer.lock();
            assert!(internal.block_position_index.is_empty());
            assert_eq!(0, internal.indexed_batches);
        }
        Ok(())
    }
}
