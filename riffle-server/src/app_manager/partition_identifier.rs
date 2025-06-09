use crate::app_manager::application_identifier::ApplicationId;
use std::hash::{DefaultHasher, Hash, Hasher};

#[derive(Ord, PartialOrd, Default, Debug, Hash, Clone, PartialEq, Eq)]
pub struct PartitionUId {
    pub app_id: ApplicationId,
    pub shuffle_id: i32,
    pub partition_id: i32,
}

impl PartitionUId {
    pub fn new(app_id: &ApplicationId, shuffle_id: i32, partition_id: i32) -> PartitionUId {
        PartitionUId {
            app_id: app_id.clone(),
            shuffle_id,
            partition_id,
        }
    }

    pub fn get_hash(uid: &PartitionUId) -> u64 {
        let mut hasher = DefaultHasher::new();

        uid.hash(&mut hasher);
        let hash_value = hasher.finish();

        hash_value
    }
}
