use crate::config::StorageType;
use crate::config::StorageType::{HDFS, LOCALFILE};
use crate::event_bus::{Event, EventBus, Subscriber};
use crate::runtime::manager::RuntimeManager;
use crate::store::spill::SpillMessage;
use anyhow::Result;
use dashmap::DashMap;
use log::warn;

// This is the predefined event bus for the spill operations.
// the parent is the dispatcher, it will firstly get the candidate
// storage, and then send these concrete storage event into the corresponding
// child storage specific eventbus.
//
// This is to isolate the localfile / hdfs writing for better performance to avoid
// slow down scheduling in the same runtime or concurrency limit.

struct HierarchyEventBus<T> {
    parent: EventBus<T>,
    children: DashMap<StorageType, EventBus<T>>,
}

impl HierarchyEventBus<SpillMessage> {
    pub fn new(runtime_manager: &RuntimeManager) -> Self {
        let parent: EventBus<SpillMessage> = EventBus::new(
            &runtime_manager.dispatch_runtime,
            "Hierarchy-Parent".to_string(),
            10000,
        );
        let child_localfile: EventBus<SpillMessage> = EventBus::new(
            &runtime_manager.localfile_write_runtime,
            "Hierarchy-Child-localfile".to_string(),
            10000,
        );
        let child_hdfs: EventBus<SpillMessage> = EventBus::new(
            &runtime_manager.hdfs_write_runtime,
            "Hierarchy-Child-hdfs".to_string(),
            10000,
        );

        let localfile_cloned = child_localfile.clone();
        let hdfs_cloned = child_hdfs.clone();
        let hook = move |msg: Event<SpillMessage>, is_succeed: bool| {
            let msg = msg.data;
            let stype = &msg.get_candidate_storage_type();
            if stype.is_none() {
                warn!(
                    "No any candidates storage. Ignore this. app: {}",
                    &msg.ctx.uid.app_id
                );
            } else {
                if let Some(stype) = stype {
                    let _ = match stype {
                        LOCALFILE => localfile_cloned.sync_publish(msg.into()),
                        HDFS => hdfs_cloned.sync_publish(msg.into()),
                        _ => Ok(()),
                    };
                };
            }
        };
        parent.with_hook(Box::new(hook));

        let parent_cloned = parent.clone();
        let children = DashMap::new();
        children.insert(LOCALFILE, child_localfile);
        children.insert(HDFS, child_hdfs);

        Self {
            parent: parent_cloned,
            children,
        }
    }

    pub fn subscribe<R: Subscriber<Input = SpillMessage> + 'static + Send + Sync + Clone>(
        &self,
        storage_selection_handler: R,
        storage_flush_handler: R,
    ) {
        self.parent.subscribe(storage_selection_handler);
        for bus in self.children.iter() {
            bus.subscribe(storage_flush_handler.clone());
        }
    }

    pub async fn publish(&self, event: Event<SpillMessage>) -> Result<()> {
        self.parent.publish(event).await
    }
}

#[cfg(test)]
mod tests {
    use crate::config::StorageType::LOCALFILE;
    use crate::event_bus::{Event, Subscriber};
    use crate::runtime::manager::RuntimeManager;
    use crate::store::spill::hierarchy_event_bus::HierarchyEventBus;
    use crate::store::spill::{SpillMessage, SpillWritingViewContext};
    use anyhow::Result;
    use async_trait::async_trait;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::Arc;
    use std::time::Duration;

    #[derive(Clone)]
    struct SelectionHandler {
        ops: Arc<AtomicU64>,
    }
    #[async_trait]
    impl Subscriber for SelectionHandler {
        type Input = SpillMessage;

        async fn on_event(&self, event: &Event<Self::Input>) {
            let msg = &event.data;
            msg.set_candidate_storage_type(LOCALFILE);
            self.ops.fetch_add(1, SeqCst);
        }
    }

    #[derive(Clone)]
    struct FlushHandler {
        ops: Arc<AtomicU64>,
    }
    #[async_trait]
    impl Subscriber for FlushHandler {
        type Input = SpillMessage;

        async fn on_event(&self, event: &Event<Self::Input>) {
            println!("Flushed");
            self.ops.fetch_add(1, SeqCst);
        }
    }

    #[test]
    fn test_event_bus() -> Result<()> {
        let runtime_manager = RuntimeManager::default();
        let event_bus = HierarchyEventBus::new(&runtime_manager);

        let select_handler_ops = Arc::new(AtomicU64::new(0));
        let cloned = select_handler_ops.clone();
        let select_handler = SelectionHandler { ops: cloned };

        let flush_handler_ops = Arc::new(AtomicU64::new(0));
        let cloned = flush_handler_ops.clone();
        let flush_handler = SelectionHandler { ops: cloned };

        event_bus.subscribe(select_handler, flush_handler);

        let spill_msg = SpillMessage {
            ctx: SpillWritingViewContext {
                uid: Default::default(),
                data_blocks: Arc::new(Default::default()),
                app_is_exist_func: Arc::new(Box::new((|app| true))),
            },
            size: 0,
            retry_cnt: 0,
            flight_id: 0,
            candidate_store_type: Arc::new(parking_lot::Mutex::new(None)),
        };
        let f = event_bus.publish(spill_msg.into());
        let _ = runtime_manager.wait(f);

        awaitility::at_most(Duration::from_secs(1)).until(|| select_handler_ops.load(SeqCst) == 1);
        awaitility::at_most(Duration::from_secs(1)).until(|| flush_handler_ops.load(SeqCst) == 1);

        Ok(())
    }
}
