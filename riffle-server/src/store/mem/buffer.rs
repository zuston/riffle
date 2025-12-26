pub mod default_buffer;
pub mod opt_buffer;
mod route_buffer;

use crate::composed_bytes::ComposedBytes;
use crate::store::mem::buffer::default_buffer::MemoryBuffer;
use crate::store::mem::buffer::opt_buffer::OptStagingMemoryBuffer;
use crate::store::DataBytes;
use crate::store::{Block, DataSegment, PartitionedMemoryData};
use anyhow::Result;
use croaring::Treemap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum MemoryBufferType {
    // the default memory_buffer type
    DEFAULT,
    // the experimental memory_buffer type
    EXPERIMENTAL,
}

#[derive(Default, Debug)]
pub struct BatchMemoryBlock(pub Vec<Vec<Block>>);
impl Deref for BatchMemoryBlock {
    type Target = Vec<Vec<Block>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for BatchMemoryBlock {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug)]
pub struct BufferSpillResult {
    pub flight_id: u64,
    pub flight_len: u64,
    pub blocks: Arc<BatchMemoryBlock>,
}

impl BufferSpillResult {
    pub fn flight_id(&self) -> u64 {
        self.flight_id
    }
    pub fn flight_len(&self) -> u64 {
        self.flight_len
    }
    pub fn blocks(&self) -> Arc<BatchMemoryBlock> {
        self.blocks.clone()
    }
}

pub trait BufferOps {
    /// Creates a new buffer instance
    fn new() -> Self
    where
        Self: Sized;

    /// Returns the total size of the buffer.
    fn total_size(&self) -> Result<i64>
    where
        Self: Send + Sync;
    /// Returns the size of data in flight (spilled but not cleared).
    fn flight_size(&self) -> Result<i64>
    where
        Self: Send + Sync;

    /// Returns the size of data in staging (not yet spilled).
    fn staging_size(&self) -> Result<i64>
    where
        Self: Send + Sync;

    /// Clears a specific flight by ID and size.
    fn clear(&self, flight_id: u64, flight_size: u64) -> Result<()>
    where
        Self: Send + Sync;

    /// Reads data starting after last_block_id, up to read_bytes_limit_len.
    fn get(
        &self,
        last_block_id: i64,
        read_bytes_limit_len: i64,
        task_ids: Option<Treemap>,
    ) -> Result<PartitionedMemoryData>
    where
        Self: Send + Sync;

    /// Spills staging data to flight, returns None if no staging data.
    fn spill(&self) -> Result<Option<BufferSpillResult>>
    where
        Self: Send + Sync;

    /// Appends blocks to staging area.
    fn append(&self, blocks: Vec<Block>, size: u64) -> Result<()>
    where
        Self: Send + Sync;

    /// push directly, just use only in test
    #[cfg(test)]
    fn direct_push(&self, blocks: Vec<Block>) -> anyhow::Result<()>
    where
        Self: Send + Sync;
}

#[cfg(test)]
mod test {
    use crate::store::mem::buffer::default_buffer::MemoryBuffer;
    use crate::store::mem::buffer::opt_buffer::OptStagingMemoryBuffer;
    use crate::store::mem::buffer::BufferOps;
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

    fn run_test_with_block_id_zero<B: BufferOps + Send + Sync + 'static>() -> anyhow::Result<()> {
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

    fn run_test_put_get<B: BufferOps + Send + Sync + 'static>() -> anyhow::Result<()> {
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

    fn run_test_get_v2_is_end_with_only_staging<B: BufferOps + Send + Sync + 'static>(
    ) -> anyhow::Result<()> {
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

    fn run_test_get_v2_is_end_across_flight_and_staging<B: BufferOps + Send + Sync + 'static>(
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
