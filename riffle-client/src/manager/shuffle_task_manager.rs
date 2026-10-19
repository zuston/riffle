use crate::TaskId;
use anyhow::{Context, Result};
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
    id_to_key: DDashMap<TaskId, TaskKey>,
    finished_task_ids: DDashMap<TaskId, ()>,
}

impl ShuffleTaskManager {
    pub fn new() -> Self {
        Self {
            key_to_id: Default::default(),
            counter: Default::default(),
            map_id_to_max_attempt_id: Default::default(),
            id_to_key: Default::default(),
            finished_task_ids: Default::default(),
        }
    }

    pub fn register_task(&self, key: &TaskKey) -> TaskId {
        self.key_to_id
            .entry(key.clone())
            .or_insert_with(|| match key {
                TaskKey::MapIdAndAttemptId(_, _) => loop {
                    let task_id = self
                        .counter
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if self.id_to_key.compute_if_absent(task_id, || key.clone()) == *key {
                        break task_id;
                    }
                },
                TaskKey::TaskId(id) => {
                    self.id_to_key.entry(*id).or_insert_with(|| key.clone());
                    *id
                }
            })
            .value()
            .clone()
    }

    pub fn report_finished_task(&self, key: &TaskKey) -> Result<()> {
        let key = match key {
            TaskKey::TaskId(id) => self
                .id_to_key
                .get(id)
                .with_context(|| format!("task {id} is not registered"))?
                .clone(),
            key => key.clone(),
        };
        match &key {
            TaskKey::MapIdAndAttemptId(map_id, attempt_id) => {
                // Register the task before making its completion visible.
                self.register_task(&key);
                self.map_id_to_max_attempt_id
                    .entry(*map_id)
                    .and_modify(|max_attempt_id| {
                        *max_attempt_id = (*max_attempt_id).max(*attempt_id);
                    })
                    .or_insert(*attempt_id);
            }
            TaskKey::TaskId(id) => {
                self.finished_task_ids.insert(*id, ());
            }
        }
        Ok(())
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
        task_ids.extend(self.finished_task_ids.iter().map(|entry| *entry.key()));
        task_ids.sort_unstable();
        task_ids
    }
}
