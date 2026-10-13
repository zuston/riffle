use self::Buffer::{DEFAULT, INDEXED};
use crate::app_manager::app_stats::AppStats;
use crate::store::mem::buffer::default_buffer::DefaultMemoryBuffer;
use crate::store::mem::buffer::indexed_buffer::IndexedMemoryBuffer;
use crate::store::mem::buffer::{BufferOptions, BufferSpillResult, BufferType, MemoryBuffer};
use crate::store::{Block, PartitionedMemoryData};
use croaring::Treemap;

/// Delegates to the memory buffer implementation selected by configuration.
pub struct ConfiguredMemoryBuffer {
    inner: Buffer,
    app_stats: AppStats,
}

enum Buffer {
    DEFAULT(DefaultMemoryBuffer),
    INDEXED(IndexedMemoryBuffer),
}

impl ConfiguredMemoryBuffer {
    pub fn app_stats(&self) -> AppStats {
        self.app_stats.clone()
    }
}

impl MemoryBuffer for ConfiguredMemoryBuffer {
    fn new(options: BufferOptions) -> Self
    where
        Self: Sized,
    {
        let app_stats = options.app_stats.clone();
        let inner = match options.buffer_type {
            BufferType::DEFAULT => DEFAULT(DefaultMemoryBuffer::new(options)),
            BufferType::INDEXED => INDEXED(IndexedMemoryBuffer::new(options)),
        };
        Self { inner, app_stats }
    }

    fn total_size(&self) -> anyhow::Result<i64>
    where
        Self: Send + Sync,
    {
        match &self.inner {
            DEFAULT(x) => x.total_size(),
            INDEXED(x) => x.total_size(),
        }
    }

    fn flight_size(&self) -> anyhow::Result<i64>
    where
        Self: Send + Sync,
    {
        match &self.inner {
            DEFAULT(x) => x.flight_size(),
            INDEXED(x) => x.flight_size(),
        }
    }

    fn staging_size(&self) -> anyhow::Result<i64>
    where
        Self: Send + Sync,
    {
        match &self.inner {
            DEFAULT(x) => x.staging_size(),
            INDEXED(x) => x.staging_size(),
        }
    }

    fn clear(&self, flight_id: u64, flight_size: u64) -> anyhow::Result<u64>
    where
        Self: Send + Sync,
    {
        let released_size = match &self.inner {
            DEFAULT(x) => x.clear(flight_id, flight_size),
            INDEXED(x) => x.clear(flight_id, flight_size),
        }?;
        self.app_stats.dec_memory(released_size);
        Ok(released_size)
    }

    fn purge(&self) -> anyhow::Result<u64> {
        match &self.inner {
            DEFAULT(buffer) => buffer.purge(),
            INDEXED(buffer) => buffer.purge(),
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
        match &self.inner {
            DEFAULT(x) => x.get(last_block_id, read_bytes_limit_len, task_ids),
            INDEXED(x) => x.get(last_block_id, read_bytes_limit_len, task_ids),
        }
    }

    fn spill(&self) -> anyhow::Result<Option<BufferSpillResult>>
    where
        Self: Send + Sync,
    {
        match &self.inner {
            DEFAULT(x) => x.spill(),
            INDEXED(x) => x.spill(),
        }
    }

    fn append(&self, blocks: Vec<Block>, size: u64) -> anyhow::Result<()>
    where
        Self: Send + Sync,
    {
        let result = match &self.inner {
            DEFAULT(x) => x.append(blocks, size),
            INDEXED(x) => x.append(blocks, size),
        };
        if result.is_ok() {
            self.app_stats.inc_memory(size);
        }
        result
    }

    #[cfg(test)]
    fn direct_push(&self, blocks: Vec<Block>) -> anyhow::Result<()>
    where
        Self: Send + Sync,
    {
        let size = blocks.iter().map(|block| block.length).sum::<i32>() as u64;
        self.append(blocks, size)
    }
}
