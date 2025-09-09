use crate::runtime::manager::RuntimeManager;
use crate::runtime::RuntimeRef;
use crate::store::local::io_layer_read_ahead::ReadPlanReadAheadTask;
use dashmap::DashMap;
use log::error;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct ReadPlanReadAheadTaskProcessor {
    // key: uid
    tasks: Arc<DashMap<u64, ReadPlanReadAheadTask>>,
}

impl ReadPlanReadAheadTaskProcessor {
    pub fn new(runtime_manager: &RuntimeManager, semphore: Arc<Semaphore>) -> Self {
        let processor = Self {
            tasks: Arc::new(Default::default()),
        };
        Self::loop_process(&processor, runtime_manager, &semphore);
        processor
    }

    fn loop_process(
        processor: &ReadPlanReadAheadTaskProcessor,
        runtime_manager: &RuntimeManager,
        semphore: &Arc<Semaphore>,
    ) {
        let processor = processor.clone();
        let semphore = semphore.clone();
        let external_rt = runtime_manager.dispatch_runtime.clone();
        let internal_rt = runtime_manager.localfile_write_runtime.clone();
        external_rt.clone().spawn(async move {
            loop {
                let mut tasks = vec![];
                let view = processor.tasks.deref().clone().into_read_only();
                for (tid, task) in view.iter() {
                    tasks.push(task.clone());
                }

                for task in tasks {
                    let tid = task.uid;
                    if let Ok(segment) = task.recv.try_recv() {
                        let permit = semphore.clone().acquire_owned().await;
                        internal_rt.spawn(async move {
                            let _permit = permit;
                            if let Err(e) = task.do_load(segment) {
                                error!("Errors on read ahead for task_id: {}. err: {}", tid, e);
                            }
                        });
                    }
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });
    }

    pub fn add_task(&self, task: &ReadPlanReadAheadTask) {
        self.tasks.insert(task.uid, task.clone());
    }

    pub fn remove_task(&self, task_id: u64) {
        self.tasks.remove(&task_id);
    }
}
