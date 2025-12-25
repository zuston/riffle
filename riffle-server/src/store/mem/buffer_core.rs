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

#[derive(Debug)]
pub struct MemoryBuffer {
    buffer: Mutex<BufferInternal>,
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

#[derive(Debug)]
pub struct BufferInternal {
    pub total_size: i64,
    pub staging_size: i64,
    pub flight_size: i64,

    pub staging: Vec<Block>,
    pub batch_boundaries: Vec<usize>, // Track where each batch starts
    pub block_position_index: HashMap<i64, usize>, // Maps block_id to Vec index

    pub flight: HashMap<u64, Arc<BatchMemoryBlock>>,
    pub flight_counter: u64,
}

impl BufferInternal {
    pub fn new() -> Self {
        BufferInternal {
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


pub trait BufferOps {
    /// Returns the total size of the buffer.
    fn total_size(&self) -> Result<i64>;

    /// Returns the size of data in flight (spilled but not cleared).
    fn flight_size(&self) -> Result<i64>;

    /// Returns the size of data in staging (not yet spilled).
    fn staging_size(&self) -> Result<i64>;

    /// Clears a specific flight by ID and size.
    fn clear(&self, flight_id: u64, flight_size: u64) -> Result<()>;

    /// Reads data starting after last_block_id, up to read_bytes_limit_len.
    fn get(
        &self,
        last_block_id: i64,
        read_bytes_limit_len: i64,
        task_ids: Option<Treemap>,
    ) -> Result<PartitionedMemoryData>;

    /// Spills staging data to flight, returns None if no staging data.
    fn spill(&self) -> Result<Option<BufferSpillResult>>;

    /// Appends blocks to staging area.
    fn append(&self, blocks: Vec<Block>, size: u64) -> Result<()>;
}
