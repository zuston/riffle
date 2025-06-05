use std::hash::{DefaultHasher, Hash, Hasher};

#[derive(Ord, PartialOrd, Eq, PartialEq, Default, Debug, Hash, Clone)]
pub struct PartitionedUId {
    pub app_id: String,
    pub shuffle_id: i32,
    pub partition_id: i32,
}

impl PartitionedUId {
    pub fn from(app_id: String, shuffle_id: i32, partition_id: i32) -> PartitionedUId {
        PartitionedUId {
            app_id,
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
