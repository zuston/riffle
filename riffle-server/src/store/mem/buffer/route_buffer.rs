use crate::store::mem::buffer::default_buffer::DefaultMemoryBuffer;
use crate::store::mem::buffer::opt_buffer::OptStagingMemoryBuffer;
use crate::store::mem::buffer::{BufferOps, BufferSpillResult};
use crate::store::{Block, PartitionedMemoryData};
use croaring::Treemap;

/// this is the router to delegate to the underlying concrete implementation without any cost
pub enum RouterBuffer {
    DEFAULT(DefaultMemoryBuffer),
    EXPERIMENTAL(OptStagingMemoryBuffer),
}

impl BufferOps for RouterBuffer {
    fn new() -> Self
    where
        Self: Sized,
    {
        todo!()
    }

    fn total_size(&self) -> anyhow::Result<i64>
    where
        Self: Send + Sync,
    {
        todo!()
    }

    fn flight_size(&self) -> anyhow::Result<i64>
    where
        Self: Send + Sync,
    {
        todo!()
    }

    fn staging_size(&self) -> anyhow::Result<i64>
    where
        Self: Send + Sync,
    {
        todo!()
    }

    fn clear(&self, flight_id: u64, flight_size: u64) -> anyhow::Result<()>
    where
        Self: Send + Sync,
    {
        todo!()
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
        todo!()
    }

    fn spill(&self) -> anyhow::Result<Option<BufferSpillResult>>
    where
        Self: Send + Sync,
    {
        todo!()
    }

    fn append(&self, blocks: Vec<Block>, size: u64) -> anyhow::Result<()>
    where
        Self: Send + Sync,
    {
        todo!()
    }

    #[cfg(test)]
    fn direct_push(&self, blocks: Vec<Block>) -> anyhow::Result<()>
    where
        Self: Send + Sync,
    {
        todo!()
    }
}
