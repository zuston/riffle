use crate::composed_bytes::ComposedBytes;
use crate::store::DataBytes;
use crate::store::{Block, DataSegment, PartitionedMemoryData};
use anyhow::Result;
use croaring::Treemap;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

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
