use crate::app_manager::partition_identifier::PartitionUId;
use crate::store::mem::buffer::MemoryBuffer;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;
use std::sync::{Arc, RwLock};

pub struct MergeOnReadBufferManager {
    // Base state - buffers sorted by staging size
    base_map: Arc<Mutex<BTreeMap<i64, Vec<PartitionUId>>>>,

    positions: Arc<Mutex<HashMap<PartitionUId, i64>>>,
    // Set of partition IDs that changed since last merge
    changed_set: Arc<Mutex<HashSet<PartitionUId>>>,
}

impl MergeOnReadBufferManager {
    pub fn new() -> Self {
        Self {
            base_map: Arc::new(Mutex::new(BTreeMap::new())),
            positions: Arc::new(Mutex::new(HashMap::new())),
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
        let mut positions = self.positions.write().unwrap();
        let mut changed_set = self.changed_set.lock().unwrap();

        // For each changed partition, update its position in base_map
        for uid in changed_set.drain() {
            // Remove from old size bucket
            if let Some(old_size) = positions.remove(&uid) {
                if let Some(bucket) = base_map.get_mut(&old_size) {
                    bucket.retain(|id| id != &uid);
                    if bucket.is_empty() {
                        base_map.remove(&old_size);
                    }
                }
            }

            // Update with new size
            if let Some(buffer) = get_buffer(&uid) {
                if let Ok(staging_size) = buffer.staging_size() {
                    if staging_size > 0 {
                        positions.insert(uid.clone(), staging_size);
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
