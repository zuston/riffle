use crate::app_manager::application_identifier::ApplicationId;
use std::hash::{DefaultHasher, Hash, Hasher};

#[derive(Ord, PartialOrd, Default, Debug, Hash, Clone, PartialEq, Eq)]
pub struct PartitionedUId {
    pub app_id: ApplicationId,
    pub shuffle_id: i32,
    pub partition_id: i32,
}

impl PartitionedUId {
    pub fn new(app_id: &ApplicationId, shuffle_id: i32, partition_id: i32) -> PartitionedUId {
        PartitionedUId {
            app_id: app_id.clone(),
            shuffle_id,
            partition_id,
        }
    }

    pub fn get_hash(uid: &PartitionedUId) -> u64 {
        let mut hasher = DefaultHasher::new();

        uid.hash(&mut hasher);
        let hash_value = hasher.finish();

        hash_value
    }
}
