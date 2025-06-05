use crate::app_manager::partition_identifier::PartitionedUId;
use anyhow::Result;
use log::warn;
use parking_lot::RwLock;
use std::sync::Arc;

#[derive(Clone)]
pub struct PartitionMeta {
    inner: Arc<RwLock<Inner>>,
}

struct Inner {
    total_size: u64,
    is_huge_partition: bool,

    is_split: bool,
}

impl PartitionMeta {
    pub fn new() -> Self {
        PartitionMeta {
            inner: Arc::new(RwLock::new(Inner {
                total_size: 0,
                is_huge_partition: false,
                is_split: false,
            })),
        }
    }

    pub fn is_split(&self, uid: &PartitionedUId, threshold: u64) -> Result<bool> {
        let mut meta = self.inner.write();
        if meta.is_split {
            return Ok(true);
        }

        if (meta.total_size > threshold) {
            meta.is_split = true;
            warn!(
                "Split partition(actual/threshold: {}/{}) for app:{}. shuffle_id:{}. partition_id:{}",
                meta.total_size, threshold, uid.app_id, uid.shuffle_id, uid.partition_id
            );
            return Ok(true);
        }

        Ok(false)
    }

    pub fn get_size(&self) -> Result<u64> {
        let meta = self.inner.read();
        Ok(meta.total_size)
    }

    pub fn inc_size(&mut self, data_size: i32) -> Result<()> {
        let mut meta = self.inner.write();
        meta.total_size += data_size as u64;
        Ok(())
    }

    pub fn is_huge_partition(&self) -> bool {
        self.inner.read().is_huge_partition
    }

    pub fn mark_as_huge_partition(&mut self) {
        let mut meta = self.inner.write();
        meta.is_huge_partition = true
    }
}
