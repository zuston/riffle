use crate::event_bus::{Event, Subscriber};
use crate::store::hybrid::HybridStore;
use crate::store::spill::metrics::FlushingMetricsMonitor;
use crate::store::spill::{handle_spill_failure, handle_spill_success, SpillMessage};
use async_trait::async_trait;
use std::sync::Arc;

#[derive(Clone)]
pub struct StorageFlushHandler {
    pub store: Arc<HybridStore>,
}
impl StorageFlushHandler {
    pub fn new(store: &Arc<HybridStore>) -> Self {
        Self {
            store: store.clone(),
        }
    }
}
unsafe impl Send for StorageFlushHandler {}
unsafe impl Sync for StorageFlushHandler {}

#[async_trait]
impl Subscriber for StorageFlushHandler {
    type Input = SpillMessage;

    async fn on_event(&self, event: &Event<Self::Input>) -> bool {
        let message = event.get_data();

        let _ = FlushingMetricsMonitor::new(message.size, message.get_candidate_storage_type());

        let result = self.store.flush_storage_for_buffer(message).await;
        let result = if result.is_ok() {
            // release resource
            handle_spill_success(message, self.store.clone()).await;
            true
        } else {
            if let Err(err) = result {
                message.inc_retry_counter();
                let could_be_retried = handle_spill_failure(err, message, self.store.clone()).await;
                !could_be_retried
            } else {
                true
            }
        };

        result
    }
}
