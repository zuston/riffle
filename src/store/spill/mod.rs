use crate::app::PartitionedUId;
use crate::config::StorageType;
use crate::error::WorkerError;
use crate::metric::{
    TOTAL_MEMORY_SPILL_OPERATION_FAILED, TOTAL_MEMORY_SPILL_TO_HDFS_OPERATION_FAILED,
    TOTAL_MEMORY_SPILL_TO_LOCALFILE_OPERATION_FAILED, TOTAL_SPILL_EVENTS_DROPPED,
    TOTAL_SPILL_EVENTS_DROPPED_WITH_APP_NOT_FOUND,
};
use crate::store::hybrid::{HybridStore, PersistentStore};
use crate::store::mem::buffer::BatchMemoryBlock;
use log::{debug, error, warn};
use parking_lot::Mutex;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;

pub mod hierarchy_event_bus;
mod spill_test;
pub mod storage_flush_handler;
pub mod storage_select_handler;

#[derive(Clone)]
pub struct SpillMessage {
    pub ctx: SpillWritingViewContext,
    pub size: i64,
    pub retry_cnt: Arc<AtomicU32>,
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

    pub fn inc_retry_counter(&self) {
        self.retry_cnt.fetch_add(1, SeqCst);
    }

    pub fn get_retry_counter(&self) -> u32 {
        self.retry_cnt.load(SeqCst)
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

async fn handle_spill_failure_whatever_error(message: &SpillMessage, store_ref: Arc<HybridStore>) {
    // Ignore all errors when app is not found. Because the pending spill operation may happen after app has been purged.
    let ctx = &message.ctx;
    let is_valid_app = ctx.is_valid();
    if !is_valid_app {
        debug!("Dropping the spill event for app: {:?}. Ths app is not found, may be purged. Ignore this", &message.ctx.uid.app_id);
        TOTAL_SPILL_EVENTS_DROPPED_WITH_APP_NOT_FOUND.inc();
    } else {
        warn!(
            "Dropping the spill event for app: {:?}. Attention: this will make data lost!",
            &message.ctx.uid.app_id
        );
        if let Err(err) = store_ref
            .release_memory_buffer(message.size, &message)
            .await
        {
            error!("Errors on releasing memory data when dropping the spill event, that should not happen. err: {:#?}", err);
        }
        TOTAL_SPILL_EVENTS_DROPPED.inc();
        TOTAL_MEMORY_SPILL_OPERATION_FAILED.inc();
    }
    store_ref.finish_spill_event(message.size as u64);
}

// handle the spill failure to release resource for the spill event.
async fn handle_spill_failure(
    err: WorkerError,
    message: &SpillMessage,
    store_ref: Arc<HybridStore>,
) -> bool {
    match err {
        WorkerError::SPILL_EVENT_EXCEED_RETRY_MAX_LIMIT(_)
        | WorkerError::PARTIAL_DATA_LOST(_)
        | WorkerError::LOCAL_DISK_UNHEALTHY(_)
        | WorkerError::APP_HAS_BEEN_PURGED
        | WorkerError::APP_IS_NOT_FOUND => {
            handle_spill_failure_whatever_error(message, store_ref).await;
            false
        }
        error => {
            TOTAL_MEMORY_SPILL_OPERATION_FAILED.inc();
            if let Some(stype) = message.get_candidate_storage_type() {
                match stype {
                    StorageType::LOCALFILE => {
                        TOTAL_MEMORY_SPILL_TO_LOCALFILE_OPERATION_FAILED.inc()
                    }
                    StorageType::HDFS => TOTAL_MEMORY_SPILL_TO_HDFS_OPERATION_FAILED.inc(),
                    _ => {}
                }
            }
            error!(
                "Errors on spill memory data to persistent storage. The error: {:#?}",
                error
            );
            // could be retry?
            true
        }
    }
}

async fn handle_spill_success(message: &SpillMessage, store_ref: Arc<HybridStore>) {
    if let Err(err) = store_ref
        .release_memory_buffer(message.size, &message)
        .await
    {
        debug!(
            "Errors on releasing memory data, that should not happen. err: {:#?}",
            err
        );
    }
    store_ref.finish_spill_event(message.size as u64);
}
