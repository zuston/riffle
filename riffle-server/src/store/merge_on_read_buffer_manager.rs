use crate::app_manager::partition_identifier::PartitionUId;
use crate::store::mem::buffer::MemoryBuffer;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;

pub struct MergeOnReadBufferManager {
    // Base state - buffers sorted by staging size
    base_map: Arc<Mutex<BTreeMap<i64, Vec<PartitionUId>>>>,

    // Set of partition IDs that changed since last merge
    changed_set: Arc<Mutex<HashSet<PartitionUId>>>,
}

impl MergeOnReadBufferManager {
    pub fn new() -> Self {
        Self {
            base_map: Arc::new(Mutex::new(BTreeMap::new())),
            changed_set: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    // Mark partition as changed
    pub async fn mark_changed(&self, uid: PartitionUId) {
        let mut changed = self.changed_set.lock().unwrap();
        changed.insert(uid);
    }

    // Update base map with changed partitions and return sorted list
    pub async fn merge(
        &self,
        get_buffer: impl Fn(&PartitionUId) -> Option<Arc<dyn MemoryBuffer + Send + Sync + 'static>>,
    ) -> BTreeMap<i64, Vec<PartitionUId>> {
        let mut base_map = self.base_map.lock().unwrap();
        let mut changed_set = self.changed_set.lock().unwrap();

        // For each changed partition, update its position in base_map
        for uid in changed_set.drain() {
            // Remove from all size buckets first
            for bucket in base_map.values_mut() {
                bucket.retain(|id| id != &uid);
            }
            base_map.retain(|_, bucket| !bucket.is_empty());

            // Get current buffer and size
            if let Some(buffer) = get_buffer(&uid) {
                if let Ok(staging_size) = buffer.staging_size() {
                    if staging_size > 0 {
                        // Add to correct size bucket
                        base_map
                            .entry(staging_size)
                            .or_insert_with(Vec::new)
                            .push(uid);
                    }
                }
            }
        }

        base_map.clone()
    }
}
