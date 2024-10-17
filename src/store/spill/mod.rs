use crate::app::PartitionedUId;
use crate::store::hybrid::PersistentStore;
use crate::store::mem::buffer::BatchMemoryBlock;
use std::sync::Arc;

pub mod event_handler;
mod spill_test;

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

#[derive(Clone)]
pub struct SpillWritingViewContext {
    pub uid: PartitionedUId,
    pub data_blocks: Arc<BatchMemoryBlock>,
    app_is_exist_func: Arc<Box<dyn Fn(&str) -> bool + 'static>>,
}
unsafe impl Send for SpillWritingViewContext {}
unsafe impl Sync for SpillWritingViewContext {}

impl SpillWritingViewContext {
    pub fn new<F>(uid: PartitionedUId, blocks: Arc<BatchMemoryBlock>, func: F) -> Self
    where
        F: Fn(&str) -> bool + 'static,
    {
        Self {
            uid,
            data_blocks: blocks,
            app_is_exist_func: Arc::new(Box::new(func)),
        }
    }

    pub fn is_valid(&self) -> bool {
        let app_id = &self.uid.app_id;
        (self.app_is_exist_func)(app_id)
    }
}
