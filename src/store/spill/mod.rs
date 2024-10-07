use crate::app::PartitionedUId;
use crate::store::hybrid::PersistentStore;
use crate::store::mem::buffer::BatchMemoryBlock;
use std::sync::Arc;

pub mod event_handler;

#[derive(Clone)]
pub struct SpillMessage {
    pub ctx: SpillWritingViewContext,
    pub size: i64,
    pub retry_cnt: i32,
    pub previous_spilled_storage: Option<Arc<Box<dyn PersistentStore>>>,
    pub flight_id: u64,
}

unsafe impl Send for SpillMessage {}
unsafe impl Sync for SpillMessage {}

#[derive(Debug, Clone)]
pub struct SpillWritingViewContext {
    pub uid: PartitionedUId,
    pub data_blocks: Arc<BatchMemoryBlock>,
}
unsafe impl Send for SpillWritingViewContext {}
unsafe impl Sync for SpillWritingViewContext {}

impl SpillWritingViewContext {
    pub fn new(uid: PartitionedUId, blocks: Arc<BatchMemoryBlock>) -> Self {
        Self {
            uid,
            data_blocks: blocks,
        }
    }
}
