use crate::metric::RESIDENT_BYTES;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

/// this struct is used to record the app level stats,
/// such as received bytes, memory bytes, localfile bytes, hdfs bytes.
#[derive(Clone, Debug, Default)]
pub struct AppStats {
    inner: Arc<Inner>,
}

#[derive(Debug, Default)]
pub struct Inner {
    received_bytes: AtomicU64,
    memory_bytes: AtomicU64,
    localfile_bytes: AtomicU64,
    hdfs_bytes: AtomicU64,
}

pub struct AppStatsSnapshot {
    pub received_bytes: u64,
    pub resident_bytes: u64,
    pub memory_bytes: u64,
    pub localfile_bytes: u64,
    pub hdfs_bytes: u64,
}

impl AppStats {
    pub fn inc_received(&self, size: u64) {
        self.inner.received_bytes.fetch_add(size, Relaxed);
    }

    pub fn inc_memory(&self, size: u64) {
        self.inner.memory_bytes.fetch_add(size, Relaxed);
        RESIDENT_BYTES.add(size as i64);
    }

    pub fn dec_memory(&self, size: u64) {
        self.inner.memory_bytes.fetch_sub(size, Relaxed);
        RESIDENT_BYTES.sub(size as i64);
    }

    pub fn inc_localfile(&self, size: u64) {
        self.inner.localfile_bytes.fetch_add(size, Relaxed);
        RESIDENT_BYTES.add(size as i64);
    }

    pub fn dec_localfile(&self, size: u64) {
        self.inner.localfile_bytes.fetch_sub(size, Relaxed);
        RESIDENT_BYTES.sub(size as i64);
    }

    pub fn inc_hdfs(&self, size: u64) {
        self.inner.hdfs_bytes.fetch_add(size, Relaxed);
        RESIDENT_BYTES.add(size as i64);
    }

    pub fn dec_hdfs(&self, size: u64) {
        self.inner.hdfs_bytes.fetch_sub(size, Relaxed);
        RESIDENT_BYTES.sub(size as i64);
    }

    pub fn snapshot(&self) -> AppStatsSnapshot {
        let memory_bytes = self.inner.memory_bytes.load(Relaxed);
        let localfile_bytes = self.inner.localfile_bytes.load(Relaxed);
        let hdfs_bytes = self.inner.hdfs_bytes.load(Relaxed);
        AppStatsSnapshot {
            received_bytes: self.inner.received_bytes.load(Relaxed),
            resident_bytes: memory_bytes
                .saturating_add(localfile_bytes)
                .saturating_add(hdfs_bytes),
            memory_bytes,
            localfile_bytes,
            hdfs_bytes,
        }
    }
}
