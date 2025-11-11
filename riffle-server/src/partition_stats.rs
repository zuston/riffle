use crate::ddashmap::DDashMap;
use std::ops::Deref;
use std::sync::Arc;

/// The PartitionStatsManager will be initialized by the every app, so the app id will not be scoped here.
pub struct PartitionStatsManager {
    // shuffle_id -> partition_id -> task_attempt_id -> record_number
    stats: DDashMap<(i32, i32), Vec<TaskAttemptIdToRecordRef>>,
}

impl PartitionStatsManager {
    pub fn new() -> Self {
        Self {
            stats: Default::default(),
        }
    }

    pub fn push(&self, shuffle_id: i32, partition_id: i32, records: Vec<TaskAttemptIdToRecord>) {
        todo!()
    }

    pub fn get(&self, shuffle_id: i32, partition_id: i32) -> Option<Vec<TaskAttemptIdToRecordRef>> {
        self.stats
            .get(&(shuffle_id, partition_id))
            .map(|x| x.clone())
    }

    pub fn purge(&self, shuffle_id: i32) {
        let view = self.stats.clone().into_read_only();
        let mut deletion_keys = vec![];
        for (v_shuffle_id, v_partition_id) in view.keys() {
            if *v_shuffle_id == shuffle_id {
                deletion_keys.push((shuffle_id, *v_partition_id));
            }
        }
        drop(view);
        for deletion_key in deletion_keys {
            self.stats.remove(&deletion_key);
        }
    }
}

#[derive(Debug, Default)]
pub struct TaskAttemptIdToRecord {
    task_attempt_id: i64,
    record_number: i64,
}

pub type TaskAttemptIdToRecordRef = Arc<TaskAttemptIdToRecord>;
