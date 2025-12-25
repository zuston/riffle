use super::buffer_core::{BatchMemoryBlock, BufferOps, BufferSpillResult};
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
pub struct OptStagingBufferInternal {
    pub total_size: i64,
    pub staging_size: i64,
    pub flight_size: i64,

    pub staging: Vec<Block>,
    pub batch_boundaries: Vec<usize>, // Track where each batch starts
    pub block_position_index: HashMap<i64, usize>, // Maps block_id to Vec index

    pub flight: HashMap<u64, Arc<BatchMemoryBlock>>,
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

impl OptStagingMemoryBuffer {
    pub fn new() -> OptStagingMemoryBuffer {
        OptStagingMemoryBuffer {
            buffer: Mutex::new(OptStagingBufferInternal::new()),
        }
    }
}

impl BufferOps for OptStagingMemoryBuffer {
    #[trace]
    fn total_size(&self) -> Result<i64> {
        return Ok(self.buffer.lock().total_size);
    }

    #[trace]
    fn flight_size(&self) -> Result<i64> {
        return Ok(self.buffer.lock().flight_size);
    }

    #[trace]
    fn staging_size(&self) -> Result<i64> {
        return Ok(self.buffer.lock().staging_size);
    }

    #[trace]
    fn clear(&self, flight_id: u64, flight_size: u64) -> Result<()> {
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
    ) -> Result<PartitionedMemoryData> {
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
    fn spill(&self) -> Result<Option<BufferSpillResult>> {
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

        let staging: BatchMemoryBlock = BatchMemoryBlock(batches);

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
    fn append(&self, blocks: Vec<Block>, size: u64) -> Result<()> {
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
    fn direct_push(&self, blocks: Vec<Block>) -> Result<()> {
        let len: u64 = blocks.iter().map(|block| block.length).sum::<i32>() as u64;
        self.append(blocks, len)
    }
}

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

impl MemoryBuffer {
    pub fn new() -> MemoryBuffer {
        MemoryBuffer {
            buffer: Mutex::new(BufferInternal::new()),
        }
    }
}
impl BufferOps for MemoryBuffer {
    #[trace]
    fn total_size(&self) -> Result<i64> {
        return Ok(self.buffer.lock().total_size);
    }

    #[trace]
    fn flight_size(&self) -> Result<i64> {
        return Ok(self.buffer.lock().flight_size);
    }

    #[trace]
    fn staging_size(&self) -> Result<i64> {
        return Ok(self.buffer.lock().staging_size);
    }

    #[trace]
    fn clear(&self, flight_id: u64, flight_size: u64) -> Result<()> {
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
    ) -> Result<PartitionedMemoryData> {
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
    fn spill(&self) -> Result<Option<BufferSpillResult>> {
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

    #[trace]
    fn append(&self, blocks: Vec<Block>, size: u64) -> Result<()> {
        let mut buffer = self.buffer.lock();
        let mut staging = &mut buffer.staging;
        staging.push(blocks);

        buffer.staging_size += size as i64;
        buffer.total_size += size as i64;

        Ok(())
    }

    #[cfg(test)]
    fn direct_push(&self, blocks: Vec<Block>) -> Result<()> {
        let len: u64 = blocks.iter().map(|block| block.length).sum::<i32>() as u64;
        self.append(blocks, len)
    }
}

#[cfg(test)]
mod test {
    use crate::store::mem::buffer::{MemoryBuffer, OptStagingMemoryBuffer};
    use crate::store::mem::buffer_core::BufferOps;
    use crate::store::Block;
    use hashlink::LinkedHashMap;
    use std::collections::LinkedList;
    use std::ops::Deref;
    use std::sync::RwLock;

    fn create_blocks(start_block_idx: i32, cnt: i32, block_len: i32) -> Vec<Block> {
        let mut blocks = vec![];
        for idx in 0..cnt {
            blocks.push(Block {
                block_id: (start_block_idx + idx) as i64,
                length: block_len,
                uncompress_length: 0,
                crc: 0,
                data: Default::default(),
                task_attempt_id: idx as i64,
            });
        }
        return blocks;
    }

    fn create_block(block_len: i32, block_id: i64) -> Block {
        Block {
            block_id,
            length: block_len,
            uncompress_length: 0,
            crc: 0,
            data: Default::default(),
            task_attempt_id: 0,
        }
    }

    trait TestBuffer: BufferOps {
        fn new() -> Self;
    }

    impl TestBuffer for MemoryBuffer {
        fn new() -> Self {
            MemoryBuffer::new()
        }
    }

    impl TestBuffer for OptStagingMemoryBuffer {
        fn new() -> Self {
            OptStagingMemoryBuffer::new()
        }
    }

    fn run_test_with_block_id_zero<B: TestBuffer + 'static>() -> anyhow::Result<()> {
        let mut buffer = B::new();
        let block_1 = create_block(10, 100);
        let block_2 = create_block(10, 0);

        buffer.direct_push(vec![block_1, block_2])?;

        let mut cnt = 0;
        let mut last_block_id = -1;
        loop {
            if cnt > 1 {
                panic!();
            }
            let mem_data = &buffer.get(last_block_id, 19, None)?;
            let segs = &mem_data.shuffle_data_block_segments;
            if segs.len() > 0 {
                let last = segs.get(segs.len() - 1).unwrap();
                last_block_id = last.block_id;
                cnt += 1;
            } else {
                break;
            }
        }

        Ok(())
    }

    #[test]
    fn test_with_block_id_zero() -> anyhow::Result<()> {
        run_test_with_block_id_zero::<MemoryBuffer>()?;
        run_test_with_block_id_zero::<OptStagingMemoryBuffer>()?;
        Ok(())
    }

    fn run_test_put_get<B: TestBuffer + 'static>() -> anyhow::Result<()> {
        let mut buffer = B::new();

        /// case1
        buffer.direct_push(create_blocks(0, 10, 10))?;
        assert_eq!(10 * 10, buffer.total_size()?);
        assert_eq!(10 * 10, buffer.staging_size()?);
        assert_eq!(0, buffer.flight_size()?);

        /// case2
        buffer.direct_push(create_blocks(10, 10, 10))?;
        assert_eq!(10 * 10 * 2, buffer.total_size()?);
        assert_eq!(10 * 10 * 2, buffer.staging_size()?);
        assert_eq!(0, buffer.flight_size()?);

        /// case3: make all staging to spill
        let spill_result = buffer.spill()?.unwrap();
        assert_eq!(10 * 10 * 2, spill_result.flight_len);
        assert_eq!(2, spill_result.blocks.len());
        assert_eq!(
            10 * 2,
            spill_result
                .blocks
                .deref()
                .iter()
                .flat_map(|x| x.iter())
                .count()
        );
        assert_eq!(10 * 10 * 2, buffer.total_size()?);
        assert_eq!(10 * 10 * 2, buffer.flight_size()?);
        assert_eq!(0, buffer.staging_size()?);

        /// case4: write blocks into staging and then reading
        buffer.direct_push(create_blocks(20, 10, 10))?;
        assert_eq!(10 * 10 * 3, buffer.total_size()?);
        assert_eq!(10 * 10 * 2, buffer.flight_size()?);
        assert_eq!(10 * 10, buffer.staging_size()?);

        /// case5: read from the flight. expected blockId: 0 -> 9
        let read_result = buffer.get(-1, 10 * 10, None)?;
        assert_eq!(10 * 10, read_result.data.len());
        assert_eq!(10, read_result.shuffle_data_block_segments.len());
        assert_eq!(
            9,
            read_result
                .shuffle_data_block_segments
                .last()
                .unwrap()
                .block_id
        );

        /// case6: read from flight again. expected blockId: 10 -> 19
        let read_result = buffer.get(9, 10 * 10, None)?;
        assert_eq!(10 * 10, read_result.data.len());
        assert_eq!(10, read_result.shuffle_data_block_segments.len());
        assert_eq!(
            19,
            read_result
                .shuffle_data_block_segments
                .last()
                .unwrap()
                .block_id
        );

        /// case7: read from staging. expected blockId: 20 -> 29
        let read_result = buffer.get(19, 10 * 10, None)?;
        assert_eq!(10 * 10, read_result.data.len());
        assert_eq!(10, read_result.shuffle_data_block_segments.len());
        assert_eq!(
            29,
            read_result
                .shuffle_data_block_segments
                .last()
                .unwrap()
                .block_id
        );

        /// case8: blockId not found, and then read from the flight -> staging.
        let read_result = buffer.get(100, 10 * 10, None)?;
        assert_eq!(10 * 10, read_result.data.len());
        assert_eq!(10, read_result.shuffle_data_block_segments.len());
        assert_eq!(
            9,
            read_result
                .shuffle_data_block_segments
                .last()
                .unwrap()
                .block_id
        );

        /// case9: remove the spill result after flushed
        let flight_id = spill_result.flight_id;
        let flight_len = spill_result.flight_len;
        buffer.clear(flight_id, flight_len)?;
        assert_eq!(10 * 10, buffer.total_size()?);
        assert_eq!(10 * 10, buffer.staging_size()?);
        assert_eq!(0, buffer.flight_size()?);

        Ok(())
    }

    #[test]
    fn test_put_get() -> anyhow::Result<()> {
        run_test_put_get::<MemoryBuffer>()?;
        run_test_put_get::<OptStagingMemoryBuffer>()?;
        Ok(())
    }

    fn run_test_get_v2_is_end_with_only_staging<B: TestBuffer + 'static>() -> anyhow::Result<()> {
        let buffer = B::new();
        // 0 -> 10 blocks with total 100 bytes
        let cnt = 10;
        let block_len = 10;
        buffer.direct_push(create_blocks(0, cnt, block_len))?;

        // case1: read all
        let result = buffer.get(-1, (cnt * block_len) as i64 + 1, None)?;
        assert_eq!(result.shuffle_data_block_segments.len(), cnt as usize);
        assert_eq!(result.data.len(), (cnt * block_len) as usize);
        assert_eq!(result.is_end, true); // no more data

        // case2: read partial data without reaching to end
        let result = buffer.get(-1, (cnt * block_len / 2) as i64 - 1, None)?;
        assert_eq!(result.shuffle_data_block_segments.len(), (cnt / 2) as usize);
        assert_eq!(result.data.len(), (cnt * block_len / 2) as usize);
        assert_eq!(result.is_end, false);

        // case3: read partial data and reaches end
        let result = buffer.get(5, (cnt * block_len) as i64, None)?;
        // blockIds [6, 7, 8, 9]
        assert_eq!(result.shuffle_data_block_segments.len(), 4);
        assert_eq!(result.data.len(), 4 * block_len as usize);
        assert_eq!(result.is_end, true);

        Ok(())
    }

    #[test]
    fn test_get_v2_is_end_with_only_staging() -> anyhow::Result<()> {
        run_test_get_v2_is_end_with_only_staging::<MemoryBuffer>()?;
        run_test_get_v2_is_end_with_only_staging::<OptStagingMemoryBuffer>()?;
        Ok(())
    }

    fn run_test_get_v2_is_end_across_flight_and_staging<B: TestBuffer + 'static>(
    ) -> anyhow::Result<()> {
        let buffer = B::new();

        // staging: 0..2
        buffer.direct_push(create_blocks(0, 3, 5))?;

        // spill: now flight has 0..2
        buffer.spill()?;

        // staging: 3..4
        buffer.direct_push(create_blocks(3, 2, 5))?;

        // first read: should return 0..2 from flight
        let result1 = buffer.get(-1, 15, None)?;
        assert_eq!(result1.shuffle_data_block_segments.len(), 3);
        assert_eq!(result1.is_end, false); // still staging blocks remaining

        // second read: should return 3..4 from staging
        let result2 = buffer.get(2, 15, None)?;
        assert_eq!(result2.shuffle_data_block_segments.len(), 2);
        assert_eq!(result2.is_end, true); // no more data after staging
        Ok(())
    }

    #[test]
    fn test_get_v2_is_end_across_flight_and_staging() -> anyhow::Result<()> {
        run_test_get_v2_is_end_across_flight_and_staging::<MemoryBuffer>()?;
        run_test_get_v2_is_end_across_flight_and_staging::<OptStagingMemoryBuffer>()?;
        Ok(())
    }
}
