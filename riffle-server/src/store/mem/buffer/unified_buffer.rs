use crate::store::mem::buffer::default_buffer::DefaultMemoryBuffer;
use crate::store::mem::buffer::opt_buffer::OptStagingMemoryBuffer;
use crate::store::mem::buffer::unified_buffer::UnifiedBuffer::{DEFAULT, EXPERIMENTAL};
use crate::store::mem::buffer::{BufferOptions, BufferSpillResult, BufferType, MemoryBuffer};
use crate::store::{Block, PartitionedMemoryData};
use croaring::Treemap;

/// this is the router to delegate to the underlying concrete implementation without any cost
pub enum UnifiedBuffer {
    DEFAULT(DefaultMemoryBuffer),
    EXPERIMENTAL(OptStagingMemoryBuffer),
}

impl MemoryBuffer for UnifiedBuffer {
    fn new(opts: BufferOptions) -> Self
    where
        Self: Sized,
    {
        match opts.buffer_type {
            BufferType::DEFAULT => DEFAULT(DefaultMemoryBuffer::new(opts)),
            BufferType::EXPERIMENTAL => EXPERIMENTAL(OptStagingMemoryBuffer::new(opts)),
        }
    }

    fn total_size(&self) -> anyhow::Result<i64>
    where
        Self: Send + Sync,
    {
        match &self {
            DEFAULT(x) => x.total_size(),
            EXPERIMENTAL(x) => x.total_size(),
        }
    }

    fn flight_size(&self) -> anyhow::Result<i64>
    where
        Self: Send + Sync,
    {
        match &self {
            DEFAULT(x) => x.flight_size(),
            EXPERIMENTAL(x) => x.flight_size(),
        }
    }

    fn staging_size(&self) -> anyhow::Result<i64>
    where
        Self: Send + Sync,
    {
        match &self {
            DEFAULT(x) => x.staging_size(),
            EXPERIMENTAL(x) => x.staging_size(),
        }
    }

    fn clear(&self, flight_id: u64, flight_size: u64) -> anyhow::Result<()>
    where
        Self: Send + Sync,
    {
        match &self {
            DEFAULT(x) => x.clear(flight_id, flight_size),
            EXPERIMENTAL(x) => x.clear(flight_id, flight_size),
        }
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
        match &self {
            DEFAULT(x) => x.get(last_block_id, read_bytes_limit_len, task_ids),
            EXPERIMENTAL(x) => x.get(last_block_id, read_bytes_limit_len, task_ids),
        }
    }

    fn spill(&self) -> anyhow::Result<Option<BufferSpillResult>>
    where
        Self: Send + Sync,
    {
        match &self {
            DEFAULT(x) => x.spill(),
            EXPERIMENTAL(x) => x.spill(),
        }
    }

    fn append(&self, blocks: Vec<Block>, size: u64) -> anyhow::Result<()>
    where
        Self: Send + Sync,
    {
        match &self {
            DEFAULT(x) => x.append(blocks, size),
            EXPERIMENTAL(x) => x.append(blocks, size),
        }
    }

    #[cfg(test)]
    fn direct_push(&self, blocks: Vec<Block>) -> anyhow::Result<()>
    where
        Self: Send + Sync,
    {
        match &self {
            DEFAULT(x) => x.direct_push(blocks),
            EXPERIMENTAL(x) => x.direct_push(blocks),
        }
    }
}
