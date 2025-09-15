use crate::runtime::manager::RuntimeManager;
use crate::runtime::RuntimeRef;
use crate::store::local::io_layer_read_ahead::ReadPlanReadAheadTask;
use await_tree::InstrumentAwait;
use dashmap::DashMap;
use log::error;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

/// This processor manages background read-ahead tasks keyed by uid, and uses a two-stage model:
/// a dispatcher runtime that polls for new segments and enqueues them, and a processor runtime
/// that consumes the queue and performs the actual I/O operations.
///
/// A `Semaphore` is used to limit concurrency, ensuring that only a limited number of tasks
/// proceed concurrently. Tasks are stored in a `DashMap` for efficient concurrent access.
/// `Arc` is used to allow safe sharing of the processor and its tasks across asynchronous tasks.
///
#[derive(Clone)]
pub struct ReadPlanReadAheadTaskProcessor {
    // key: uid
    tasks: Arc<DashMap<u64, ReadPlanReadAheadTask>>,
}

impl ReadPlanReadAheadTaskProcessor {
    pub fn new(runtime_manager: &RuntimeManager, semphore: Arc<Semaphore>, root: &str) -> Self {
        let processor = Self {
            tasks: Arc::new(Default::default()),
        };
        Self::loop_process(&processor, &semphore, runtime_manager, root);
        processor
    }

    fn loop_process(
        processor: &ReadPlanReadAheadTaskProcessor,
        semphore: &Arc<Semaphore>,
        runtime_manager: &RuntimeManager,
        root: &str,
    ) {
        let processor = processor.clone();
        let semphore = semphore.clone();

        let dispatch_runtime = runtime_manager.dispatch_runtime.clone();
        let process_runtime = runtime_manager.read_ahead_runtime.clone();

        let dispatch_log = format!("read-plan-read-ahead-tasks-dispatch [{}]", &root);
        let (send, recv) = async_channel::unbounded();
        dispatch_runtime.spawn_with_await_tree(&dispatch_log, async move {
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
                        send.send((segment, task, permit, tid))
                            .instrument_await("sending to the queue...")
                            .await;
                    }
                }
                tokio::time::sleep(Duration::from_millis(1))
                    .instrument_await("sleeping...")
                    .await;
            }
        });

        let processor_log = format!("read-plan-read-ahead-tasks-processor [{}]", &root);
        process_runtime.spawn_with_await_tree(&processor_log, async move {
            while let Ok((segment, task, permit, uid)) =
                recv.recv().instrument_await("recv from the queue...").await
            {
                let _permit = permit;
                if let Err(e) = task
                    .do_load(segment)
                    .instrument_await("loading for the read ahead...")
                    .await
                {
                    error!("Errors on read ahead for task_id: {}. err: {}", uid, e);
                }
            }
        });
    }

    pub fn add_task(&self, task: &ReadPlanReadAheadTask) {
        self.tasks.insert(task.uid, task.clone());
    }

    pub fn remove_task(&self, task_id: u64) {
        self.tasks.remove(&task_id);
    }

    pub fn get_task(&self, uid: u64) -> Option<ReadPlanReadAheadTask> {
        self.tasks.get(&uid).map(|task| task.clone())
    }

    pub fn task_size(&self) -> usize {
        self.tasks.len()
    }
}
