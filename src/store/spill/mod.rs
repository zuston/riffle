use crate::app::PartitionedUId;
use crate::config::StorageType;
use crate::store::hybrid::PersistentStore;
use crate::store::mem::buffer::BatchMemoryBlock;
use parking_lot::Mutex;
use std::sync::Arc;

pub mod event_handler;
mod hierarchy_event_bus;
mod spill_test;

#[derive(Clone)]
pub struct SpillMessage {
    pub ctx: SpillWritingViewContext,
    pub size: i64,
    pub retry_cnt: i32,
    pub flight_id: u64,
    pub candidate_store_type: Arc<Mutex<Option<StorageType>>>,
}

impl SpillMessage {
    pub fn has_candidate_storage(&self) -> bool {
        let guard = self.candidate_store_type.lock();
        guard.is_some()
    }

    pub fn get_candidate_storage_type(&self) -> Option<StorageType> {
        let guard = self.candidate_store_type.lock();
        if guard.is_none() {
            None
        } else {
            Some(guard.unwrap().clone())
        }
    }

    pub fn set_candidate_storage_type(&self, storage_type: StorageType) {
        let mut guard = self.candidate_store_type.lock();
        *guard = Some(storage_type.clone())
    }
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
