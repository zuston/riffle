use crate::app_manager::partition_identifier::PartitionUId;
use crate::store::mem::buffer::MemoryBuffer;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct MergeOnReadBufferManager {
    // Base state - buffers sorted by staging size
    base_map: Arc<Mutex<BTreeMap<i64, Vec<PartitionUId>>>>,
    // The positions of partitions at the last merge
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
        let mut positions = self.positions.lock().unwrap();
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

    pub async fn get_changed_count(&self) -> usize {
        let changed_set = self.changed_set.lock().unwrap();
        changed_set.len()
    }
}

mod tests {
    use super::*;
    use crate::app_manager::partition_identifier::PartitionUId;
    use crate::store::mem::buffer::default_buffer::DefaultMemoryBuffer;
    use crate::store::mem::buffer::opt_buffer::OptStagingMemoryBuffer;
    use crate::store::mem::buffer::MemoryBuffer;
    use crate::store::Block;
    use std::sync::Arc;

    fn create_blocks(start_block_idx: i32, cnt: i32, block_len: i32) -> Vec<Block> {
        let mut blocks = vec![];
        for idx in 0..cnt {
            blocks.push(Block {
                block_id: (start_block_idx + idx) as i64,
                length: block_len,
                uncompress_length: 0,
                crc: 0,
                data: Default::default(),
                task_attempt_id: idx as i64,
            });
        }
        return blocks;
    }

    // Helper to create a buffer with test data
    fn create_test_buffer<B: MemoryBuffer + Send + Sync + 'static>(
        block_count: i32,
        block_size: i32,
    ) -> Arc<B> {
        let buffer = Arc::new(B::new(Default::default()));
        let blocks = create_blocks(0, block_count, block_size);
        buffer.direct_push(blocks).unwrap();
        buffer
    }

    #[tokio::test]
    async fn test_mark_changed() {
        let manager = MergeOnReadBufferManager::new();
        let uid = PartitionUId::new(&Default::default(), 1, 0);

        // Initially no changes
        assert_eq!(manager.get_changed_count().await, 0);

        // Mark as changed
        manager.mark_changed(uid.clone()).await;
        assert_eq!(manager.get_changed_count().await, 1);

        // Mark same uid again - should still be 1 (set semantics)
        manager.mark_changed(uid.clone()).await;
        assert_eq!(manager.get_changed_count().await, 1);
    }

    #[tokio::test]
    async fn test_merge_with_default_buffers() {
        let manager = MergeOnReadBufferManager::new();
        let app_id = Default::default();

        // Create test buffers with different sizes
        let buffer1 = create_test_buffer::<DefaultMemoryBuffer>(5, 10); // 50 bytes
        let buffer2 = create_test_buffer::<DefaultMemoryBuffer>(3, 20); // 60 bytes
        let buffer3 = create_test_buffer::<DefaultMemoryBuffer>(2, 15); // 30 bytes

        let uid1 = PartitionUId::new(&app_id, 1, 0);
        let uid2 = PartitionUId::new(&app_id, 1, 1);
        let uid3 = PartitionUId::new(&app_id, 1, 2);

        // Mark some as changed
        manager.mark_changed(uid1.clone()).await;
        manager.mark_changed(uid2.clone()).await;

        // Mock get_buffer function
        let mut buffers = std::collections::HashMap::new();
        buffers.insert(uid1.clone(), buffer1.clone());
        buffers.insert(uid2.clone(), buffer2.clone());
        buffers.insert(uid3.clone(), buffer3.clone());

        let sorted_map = manager
            .merge(|uid| {
                buffers
                    .get(uid)
                    .cloned()
                    .map(|b| b as Arc<dyn MemoryBuffer + Send + Sync>)
            })
            .await;

        // Should have entries for changed buffers only
        assert_eq!(sorted_map.len(), 2);

        // Verify sorted order (largest first)
        let mut sizes: Vec<_> = sorted_map.keys().collect();
        sizes.sort_by(|a, b| b.cmp(a));
        assert_eq!(sizes, vec![&60, &50]);
    }

    #[tokio::test]
    async fn test_merge_with_opt_buffers() {
        let manager = MergeOnReadBufferManager::new();
        let app_id = Default::default();

        // Create test buffers using OptStagingMemoryBuffer
        let buffer1 = create_test_buffer::<OptStagingMemoryBuffer>(4, 25); // 100 bytes
        let buffer2 = create_test_buffer::<OptStagingMemoryBuffer>(2, 50); // 100 bytes

        let uid1 = PartitionUId::new(&app_id, 2, 0);
        let uid2 = PartitionUId::new(&app_id, 2, 1);

        manager.mark_changed(uid1.clone()).await;
        manager.mark_changed(uid2.clone()).await;

        let mut buffers = std::collections::HashMap::new();
        buffers.insert(uid1.clone(), buffer1.clone());
        buffers.insert(uid2.clone(), buffer2.clone());

        let sorted_map = manager
            .merge(|uid| {
                buffers
                    .get(uid)
                    .cloned()
                    .map(|b| b as Arc<dyn MemoryBuffer + Send + Sync>)
            })
            .await;

        // Both buffers have same size, should be in same bucket
        assert_eq!(sorted_map.len(), 1);
        assert_eq!(sorted_map.get(&100).unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_merge_removes_deleted_buffers() {
        let manager = MergeOnReadBufferManager::new();
        let uid = PartitionUId::new(&Default::default(), 3, 0);

        // Mark as changed
        manager.mark_changed(uid.clone()).await;

        // Mock get_buffer that returns None (buffer deleted)
        let sorted_map = manager.merge(|_| None).await;

        // Should be empty since buffer doesn't exist
        assert!(sorted_map.is_empty());
    }
    #[tokio::test]
    async fn test_concurrent_mark_changed() {
        let manager = MergeOnReadBufferManager::new();
        let mut handles = vec![];

        // Spawn 10 concurrent tasks
        for i in 0..10 {
            let manager_clone = manager.clone();
            let handle = tokio::spawn(async move {
                let uid = PartitionUId::new(&Default::default(), i, 0);
                manager_clone.mark_changed(uid).await;
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify all changes were recorded
        assert_eq!(manager.get_changed_count().await, 10);
    }
}
