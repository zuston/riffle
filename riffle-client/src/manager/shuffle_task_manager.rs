use crate::TaskId;
use riffle_server::ddashmap::DDashMap;
use std::sync::atomic::AtomicUsize;

#[derive(Clone, Hash, PartialEq, Eq)]
pub enum TaskKey {
    MapIdAndAttemptId(usize, usize),
    TaskId(usize),
}

// to manage the uniq task ids
pub struct ShuffleTaskManager {
    pub key_to_id: DDashMap<TaskKey, TaskId>,
    pub counter: AtomicUsize,
    pub map_id_to_max_attempt_id: DDashMap<usize, usize>,
}

impl ShuffleTaskManager {
    pub fn new() -> Self {
        Self {
            key_to_id: Default::default(),
            counter: Default::default(),
            map_id_to_max_attempt_id: Default::default(),
        }
    }

    pub fn register_task(&self, key: &TaskKey) -> TaskId {
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

    pub fn report_finished_task(&self, key: &TaskKey) {
        match key {
            TaskKey::MapIdAndAttemptId(map_id, attempt_id) => {
                // Register the task before making its completion visible.
                self.register_task(key);
                self.map_id_to_max_attempt_id
                    .entry(*map_id)
                    .and_modify(|max_attempt_id| {
                        *max_attempt_id = (*max_attempt_id).max(*attempt_id);
                    })
                    .or_insert(*attempt_id);
            }
            TaskKey::TaskId(_) => {
                todo!()
            }
        }
    }

    pub fn get_finished_tasks(&self) -> Vec<TaskId> {
        let mut task_ids: Vec<_> = self
            .map_id_to_max_attempt_id
            .iter()
            .map(|entry| {
                let key = TaskKey::MapIdAndAttemptId(*entry.key(), *entry.value());
                *self
                    .key_to_id
                    .get(&key)
                    .expect("finished task must have a registered ID")
            })
            .collect();
        task_ids.sort_unstable();
        task_ids
    }
}
