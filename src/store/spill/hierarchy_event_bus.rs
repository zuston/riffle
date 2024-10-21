use crate::config::StorageType::{HDFS, LOCALFILE};
use crate::config::{Config, StorageType};
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

pub struct HierarchyEventBus<T> {
    parent: EventBus<T>,
    children: DashMap<StorageType, EventBus<T>>,
}

impl HierarchyEventBus<SpillMessage> {
    pub fn new(runtime_manager: &RuntimeManager, config: &Config) -> Self {
        let parent: EventBus<SpillMessage> = EventBus::new(
            &runtime_manager.dispatch_runtime,
            "Hierarchy-Parent".to_string(),
            config.hybrid_store.memory_spill_to_localfile_concurrency as usize
                + config.hybrid_store.memory_spill_to_hdfs_concurrency as usize,
        );
        let child_localfile: EventBus<SpillMessage> = EventBus::new(
            &runtime_manager.localfile_write_runtime,
            "Hierarchy-Child-localfile".to_string(),
            config.hybrid_store.memory_spill_to_localfile_concurrency as usize,
        );
        let child_hdfs: EventBus<SpillMessage> = EventBus::new(
            &runtime_manager.hdfs_write_runtime,
            "Hierarchy-Child-hdfs".to_string(),
            config.hybrid_store.memory_spill_to_hdfs_concurrency as usize,
        );

        // dispatch event into the concrete handler when the selection is finished
        let localfile_cloned = child_localfile.clone();
        let hdfs_cloned = child_hdfs.clone();
        let hook = move |msg: Event<SpillMessage>, is_succeed: bool| {
            if !is_succeed {
                return;
            }

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

        // setup the retry or drop hook for children handlers
        let parent_cloned = parent.clone();
        let hook = move |msg: Event<SpillMessage>, is_succeed: bool| {
            if is_succeed {
                return;
            }
            if let Err(err) = parent_cloned.sync_publish(msg) {
                warn!(
                    "Errors on resending the event into parent event bus. err: {:#?}",
                    err
                );
            }
        };
        child_localfile.with_hook(Box::new(hook.clone()));
        child_hdfs.with_hook(Box::new(hook));

        let parent_cloned = parent.clone();
        let children = DashMap::new();
        children.insert(LOCALFILE, child_localfile);
        children.insert(HDFS, child_hdfs);

        Self {
            parent: parent_cloned,
            children,
        }
    }

    pub fn subscribe<
        R: Subscriber<Input = SpillMessage> + 'static + Send + Sync + Clone,
        T: Subscriber<Input = SpillMessage> + 'static + Send + Sync + Clone,
    >(
        &self,
        storage_selection_handler: R,
        storage_flush_handler: T,
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
    use crate::config::Config;
    use crate::config::StorageType::LOCALFILE;
    use crate::event_bus::{Event, Subscriber};
    use crate::runtime::manager::RuntimeManager;
    use crate::store::spill::hierarchy_event_bus::HierarchyEventBus;
    use crate::store::spill::{SpillMessage, SpillWritingViewContext};
    use anyhow::Result;
    use async_trait::async_trait;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::sync::Arc;
    use std::time::Duration;

    #[derive(Clone)]
    struct SelectionHandler {
        ops: Arc<AtomicU64>,
        result_ref: Arc<AtomicBool>,
    }
    #[async_trait]
    impl Subscriber for SelectionHandler {
        type Input = SpillMessage;

        async fn on_event(&self, event: &Event<Self::Input>) -> bool {
            let msg = &event.data;
            msg.set_candidate_storage_type(LOCALFILE);
            self.ops.fetch_add(1, SeqCst);
            self.result_ref.load(SeqCst)
        }
    }

    #[derive(Clone)]
    struct FlushHandler {
        ops: Arc<AtomicU64>,
        result_ref: Arc<AtomicBool>,
        failure_counter: Arc<AtomicU64>,
        failure_max: u64,
    }
    #[async_trait]
    impl Subscriber for FlushHandler {
        type Input = SpillMessage;

        async fn on_event(&self, event: &Event<Self::Input>) -> bool {
            println!("Flushed");
            self.ops.fetch_add(1, SeqCst);
            let is_succeed = self.result_ref.load(SeqCst);
            if !is_succeed {
                return if self.failure_counter.load(SeqCst) >= self.failure_max {
                    true
                } else {
                    self.failure_counter.fetch_add(1, SeqCst);
                    false
                };
            }
            return true;
        }
    }

    #[test]
    fn test_event_bus() -> Result<()> {
        let runtime_manager = RuntimeManager::default();
        let config = Config::create_simple_config();
        let event_bus = HierarchyEventBus::new(&runtime_manager, &config);

        let select_handler_ops = Arc::new(AtomicU64::new(0));
        let select_handler_result = Arc::new(AtomicBool::new(true));

        let cloned = select_handler_ops.clone();
        let result_cloned = select_handler_result.clone();
        let select_handler = SelectionHandler {
            ops: cloned,
            result_ref: result_cloned,
        };

        let flush_handler_ops = Arc::new(AtomicU64::new(0));
        let flush_handler_result = Arc::new(AtomicBool::new(true));

        let cloned = flush_handler_ops.clone();
        let result_cloned = flush_handler_result.clone();
        let flush_handler = FlushHandler {
            ops: cloned,
            result_ref: result_cloned,
            failure_counter: Default::default(),
            failure_max: 3,
        };

        event_bus.subscribe(select_handler, flush_handler);

        let spill_msg = SpillMessage {
            ctx: SpillWritingViewContext {
                uid: Default::default(),
                data_blocks: Arc::new(Default::default()),
                app_is_exist_func: Arc::new(Box::new((|app| true))),
            },
            size: 0,
            retry_cnt: Default::default(),
            flight_id: 0,
            candidate_store_type: Arc::new(parking_lot::Mutex::new(None)),
        };
        let f = event_bus.publish(spill_msg.clone().into());
        let _ = runtime_manager.wait(f);

        // case1
        awaitility::at_most(Duration::from_secs(1)).until(|| select_handler_ops.load(SeqCst) == 1);
        awaitility::at_most(Duration::from_secs(1)).until(|| flush_handler_ops.load(SeqCst) == 1);
        select_handler_ops.store(0, SeqCst);
        flush_handler_ops.store(0, SeqCst);

        // case2: the event will be drop by the parent event bus because it returns false.
        select_handler_result.store(false, SeqCst);
        let f = event_bus.publish(spill_msg.clone().into());
        let _ = runtime_manager.wait(f);
        awaitility::at_most(Duration::from_secs(1)).until(|| select_handler_ops.load(SeqCst) == 1);
        awaitility::at_most(Duration::from_secs(1)).until(|| flush_handler_ops.load(SeqCst) == 0);

        select_handler_ops.store(0, SeqCst);
        select_handler_result.store(true, SeqCst);

        // case3: the failure event in flush handler will be retry until it returns true
        flush_handler_result.store(false, SeqCst);
        let f = event_bus.publish(spill_msg.clone().into());
        let _ = runtime_manager.wait(f);

        awaitility::at_most(Duration::from_secs(1)).until(|| flush_handler_ops.load(SeqCst) == 4);
        assert_eq!(4, select_handler_ops.load(SeqCst));

        Ok(())
    }
}
