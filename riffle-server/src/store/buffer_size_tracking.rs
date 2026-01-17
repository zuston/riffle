use crate::app_manager::partition_identifier::PartitionUId;
use crate::store::mem::buffer::MemoryBuffer;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct BufferSizeTracking {
    // Base state - buffers sorted by staging size
    base_map: Arc<Mutex<BTreeMap<i64, Vec<PartitionUId>>>>,
    // The positions of partitions at the last merge
    positions: Arc<Mutex<HashMap<PartitionUId, i64>>>,
    // Set of partition IDs that changed since last merge
    changed_set: Arc<Mutex<HashSet<PartitionUId>>>,
    // Function to get buffer by partition ID  
    get_buffer: Arc<dyn Fn(&PartitionUId) -> Option<Arc<dyn MemoryBuffer + Send + Sync + 'static>> + Send + Sync>, 
}

impl BufferSizeTracking {
    pub fn new() -> Self {
        Self {
            base_map: Arc::new(Mutex::new(BTreeMap::new())),
            positions: Arc::new(Mutex::new(HashMap::new())),
            changed_set: Arc::new(Mutex::new(HashSet::new())),
            get_buffer: Arc::new(get_buffer),  
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
            if let Some(buffer) = (self.get_buffer)(&uid) {
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
    use crate::store::test_utils::create_blocks;
    use crate::store::Block;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_mark_changed() {
        let manager = BufferSizeTracking::new();
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
        let manager = BufferSizeTracking::new();
        let mut buffers = HashMap::new();

        // Create test buffers with DefaultMemoryBuffer
        for i in 0..5 {
            let uid = PartitionUId::new(&Default::default(), i, 0);
            let buffer = Arc::new(DefaultMemoryBuffer::new(Default::default()));

            // Add data to buffer
            let blocks = create_blocks(i * 10, 2, 10);
            MemoryBuffer::direct_push(&*buffer, blocks).unwrap();

            buffers.insert(uid.clone(), buffer);
            manager.mark_changed(uid).await;
        }

        // Verify all buffers are marked as changed
        assert_eq!(manager.get_changed_count().await, 5);

        // Merge and verify sorted order
        let sorted_map = manager
            .merge(|uid| {
                buffers
                    .get(uid)
                    .cloned()
                    .map(|b| b as Arc<dyn MemoryBuffer + Send + Sync>)
            })
            .await;

        // Should have 5 buffers, each with size 20 (2 blocks * 10 bytes)
        assert_eq!(sorted_map.len(), 1);
        assert!(sorted_map.contains_key(&20));
        assert_eq!(sorted_map[&20].len(), 5);

        // Verify no changes after merge
        assert_eq!(manager.get_changed_count().await, 0);
    }

    #[tokio::test]
    async fn test_merge_with_opt_buffers() {
        let manager = BufferSizeTracking::new();
        let mut buffers = HashMap::new();

        // Create test buffers with OptStagingMemoryBuffer
        for i in 0..5 {
            let uid = PartitionUId::new(&Default::default(), i, 0);
            let buffer = Arc::new(OptStagingMemoryBuffer::new(Default::default()));

            // Add data to buffer
            let blocks = create_blocks(i * 10, 2, 10);
            MemoryBuffer::direct_push(&*buffer, blocks).unwrap();

            buffers.insert(uid.clone(), buffer);
            manager.mark_changed(uid).await;
        }

        // Verify all buffers are marked as changed
        assert_eq!(manager.get_changed_count().await, 5);

        // Merge and verify sorted order
        let sorted_map = manager
            .merge(|uid| {
                buffers
                    .get(uid)
                    .cloned()
                    .map(|b| b as Arc<dyn MemoryBuffer + Send + Sync>)
            })
            .await;

        // Should have 5 buffers, each with size 20 (2 blocks * 10 bytes)
        assert_eq!(sorted_map.len(), 1);
        assert!(sorted_map.contains_key(&20));
        assert_eq!(sorted_map[&20].len(), 5);

        // Verify no changes after merge
        assert_eq!(manager.get_changed_count().await, 0);
    }
    #[tokio::test]
    async fn test_merge_removes_deleted_buffers() {
        let manager = BufferSizeTracking::new();
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
        let manager = BufferSizeTracking::new();
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
