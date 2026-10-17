use crate::TaskId;
use riffle_server::ddashmap::DDashMap;
use std::sync::atomic::AtomicUsize;

#[derive(Clone, Hash, PartialEq, Eq)]
enum TaskKey {
    MapIdAndAttemptId(usize, usize),
    TaskId(usize),
}
pub struct TaskIdManager {
    pub key_to_id: DDashMap<TaskKey, TaskId>,
    pub counter: AtomicUsize,
}

impl TaskIdManager {
    pub fn new() -> Self {
        Self {
            key_to_id: Default::default(),
            counter: Default::default(),
        }
    }

    pub fn get(&self, key: &TaskKey) -> TaskId {
        self.key_to_id
            .entry(key.clone())
            .or_insert_with(|| match key {
                TaskKey::MapIdAndAttemptId(_, _) => self
                    .counter
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                TaskKey::TaskId(x) => *x,
            })
            .value()
            .clone()
    }
}