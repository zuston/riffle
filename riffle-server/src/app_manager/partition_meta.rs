use crate::app_manager::partition_identifier::PartitionUId;
use anyhow::Result;
use futures::AsyncWriteExt;
use log::warn;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct PartitionMeta {
    inner: Arc<Inner>,
}

struct Inner {
    total_size: AtomicU64,
    is_huge_partition: AtomicBool,
    is_split: AtomicBool,

    // this option is for the read ahead mechanism in localfile
    is_sequential_read: AtomicBool,
}

impl PartitionMeta {
    pub fn new() -> Self {
        PartitionMeta {
            inner: Arc::new(Inner {
                total_size: Default::default(),
                is_huge_partition: Default::default(),
                is_split: Default::default(),
                is_sequential_read: AtomicBool::new(false),
            }),
        }
    }

    pub fn get_size(&self) -> u64 {
        self.inner.total_size.load(Ordering::SeqCst)
    }

    pub fn inc_size(&self, data_size: u64) -> u64 {
        self.inner.total_size.fetch_add(data_size, Ordering::SeqCst)
    }

    // about huge partition
    pub fn is_huge_partition(&self) -> bool {
        self.inner.is_huge_partition.load(Ordering::SeqCst)
    }

    pub fn mark_as_huge_partition(&self) {
        self.inner.is_huge_partition.store(true, Ordering::SeqCst);
    }

    // about partition split tag
    pub fn is_split(&self) -> bool {
        self.inner.is_split.load(Ordering::SeqCst)
    }

    pub fn mark_as_split(&self) {
        self.inner.is_split.store(true, Ordering::SeqCst);
    }

    pub fn mark_as_sequential_read(&self) {
        self.inner.is_sequential_read.store(true, Ordering::SeqCst);
    }

    pub fn is_sequential_read(&self) -> bool {
        self.inner.is_sequential_read.load(Ordering::SeqCst)
    }
}
