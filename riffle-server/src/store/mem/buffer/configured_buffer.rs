use crate::store::mem::buffer::configured_buffer::ConfiguredMemoryBuffer::{DEFAULT, INDEXED};
use crate::store::mem::buffer::default_buffer::DefaultMemoryBuffer;
use crate::store::mem::buffer::indexed_buffer::IndexedMemoryBuffer;
use crate::store::mem::buffer::{BufferOptions, BufferSpillResult, BufferType, MemoryBuffer};
use crate::store::{Block, PartitionedMemoryData};
use croaring::Treemap;

/// Delegates to the memory buffer implementation selected by configuration.
pub enum ConfiguredMemoryBuffer {
    DEFAULT(DefaultMemoryBuffer),
    INDEXED(IndexedMemoryBuffer),
}

impl MemoryBuffer for ConfiguredMemoryBuffer {
    fn new(options: BufferOptions) -> Self
    where
        Self: Sized,
    {
        match options.buffer_type {
            BufferType::DEFAULT => DEFAULT(DefaultMemoryBuffer::new(options)),
            BufferType::INDEXED => INDEXED(IndexedMemoryBuffer::new(options)),
        }
    }

    fn total_size(&self) -> anyhow::Result<i64>
    where
        Self: Send + Sync,
    {
        match &self {
            DEFAULT(x) => x.total_size(),
            INDEXED(x) => x.total_size(),
        }
    }

    fn flight_size(&self) -> anyhow::Result<i64>
    where
        Self: Send + Sync,
    {
        match &self {
            DEFAULT(x) => x.flight_size(),
            INDEXED(x) => x.flight_size(),
        }
    }

    fn staging_size(&self) -> anyhow::Result<i64>
    where
        Self: Send + Sync,
    {
        match &self {
            DEFAULT(x) => x.staging_size(),
            INDEXED(x) => x.staging_size(),
        }
    }

    fn clear(&self, flight_id: u64, flight_size: u64) -> anyhow::Result<()>
    where
        Self: Send + Sync,
    {
        match &self {
            DEFAULT(x) => x.clear(flight_id, flight_size),
            INDEXED(x) => x.clear(flight_id, flight_size),
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
            INDEXED(x) => x.get(last_block_id, read_bytes_limit_len, task_ids),
        }
    }

    fn spill(&self) -> anyhow::Result<Option<BufferSpillResult>>
    where
        Self: Send + Sync,
    {
        match &self {
            DEFAULT(x) => x.spill(),
            INDEXED(x) => x.spill(),
        }
    }

    fn append(&self, blocks: Vec<Block>, size: u64) -> anyhow::Result<()>
    where
        Self: Send + Sync,
    {
        match &self {
            DEFAULT(x) => x.append(blocks, size),
            INDEXED(x) => x.append(blocks, size),
        }
    }

    #[cfg(test)]
    fn direct_push(&self, blocks: Vec<Block>) -> anyhow::Result<()>
    where
        Self: Send + Sync,
    {
        match &self {
            DEFAULT(x) => x.direct_push(blocks),
            INDEXED(x) => x.direct_push(blocks),
        }
    }
}
