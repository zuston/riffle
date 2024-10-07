use crate::error::WorkerError;
use crate::event_bus::{Event, Subscriber};
use crate::metric::{
    GAUGE_IN_SPILL_DATA_SIZE, GAUGE_MEMORY_SPILL_OPERATION, TOTAL_MEMORY_SPILL_OPERATION,
    TOTAL_MEMORY_SPILL_OPERATION_FAILED, TOTAL_SPILL_EVENTS_DROPPED,
};
use crate::store::hybrid::HybridStore;
use crate::store::spill::SpillMessage;
use async_trait::async_trait;
use await_tree::InstrumentAwait;
use log::{debug, error, warn};
use std::sync::Arc;

struct SpillEventHandler {
    store: Arc<HybridStore>,
}

unsafe impl Send for SpillEventHandler {}
unsafe impl Sync for SpillEventHandler {}

#[async_trait]
impl Subscriber for SpillEventHandler {
    type Input = SpillMessage;

    async fn on_event(&self, event: &Event<Self::Input>) {
        let message = event.get_data();
        let size = message.size;

        GAUGE_IN_SPILL_DATA_SIZE.add(size);
        TOTAL_MEMORY_SPILL_OPERATION.inc();
        GAUGE_MEMORY_SPILL_OPERATION.inc();

        let store_ref = &self.store;
        match store_ref
            .memory_spill_to_persistent_store(message.clone())
            .instrument_await("memory_spill_to_persistent_store.")
            .await
        {
            Ok(msg) => {
                debug!("{}", msg);
                if let Err(err) = store_ref.release_data_in_memory(size, &message).await {
                    error!(
                        "Errors on releasing memory data, that should not happen. err: {:#?}",
                        err
                    );
                }
                store_ref.dec_spill_event_num(1);
            }
            Err(WorkerError::SPILL_EVENT_EXCEED_RETRY_MAX_LIMIT(_))
            | Err(WorkerError::PARTIAL_DATA_LOST(_))
            | Err(WorkerError::LOCAL_DISK_UNHEALTHY(_)) => {
                warn!(
                    "Dropping the spill event for app: {:?}. Attention: this will make data lost!",
                    message.ctx.uid.app_id
                );
                if let Err(err) = store_ref.release_data_in_memory(size, &message).await {
                    error!("Errors on releasing memory data when dropping the spill event, that should not happen. err: {:#?}", err);
                }
                TOTAL_SPILL_EVENTS_DROPPED.inc();
                TOTAL_MEMORY_SPILL_OPERATION_FAILED.inc();
                store_ref.dec_spill_event_num(1);
            }
            Err(error) => {
                TOTAL_MEMORY_SPILL_OPERATION_FAILED.inc();
                error!(
                    "Errors on spill memory data to persistent storage. The error: {:#?}",
                    error
                );

                let mut new_message = message.clone();
                new_message.retry_cnt = message.retry_cnt + 1;
                // re-push to the queue to execute
                let _ = store_ref.event_bus.publish(new_message.into()).await;
            }
        }
        GAUGE_IN_SPILL_DATA_SIZE.sub(size);
        GAUGE_MEMORY_SPILL_OPERATION.dec();
    }
}
