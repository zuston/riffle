use crate::app::{GetMultiBlockIdsContext, ReportMultiBlockIdsContext};
use crate::block_id_manager::BlockIdManagerType::DEFAULT;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use croaring::{JvmLegacy, Treemap};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::sync::Arc;

/// The block id manager is used by the every app, so the app id will not be scoped here.
#[async_trait]
pub trait BlockIdManager: Send + Sync {
    async fn get_multi_block_ids(&self, ctx: GetMultiBlockIdsContext) -> Result<Bytes>;
    async fn report_multi_block_ids(&self, ctx: ReportMultiBlockIdsContext) -> Result<()>;
    async fn purge_block_ids(&self, shuffle_id: i32) -> Result<()>;
    async fn get_blocks_number(&self) -> Result<u64>;
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum BlockIdManagerType {
    DEFAULT,
    PARTITIONED,
}
impl Default for BlockIdManagerType {
    fn default() -> Self {
        DEFAULT
    }
}

pub fn get_block_id_manager(b_type: &BlockIdManagerType) -> Arc<Box<dyn BlockIdManager>> {
    match b_type {
        BlockIdManagerType::PARTITIONED => Arc::new(Box::new(PartitionedBlockIdManager::default())),
        BlockIdManagerType::DEFAULT => Arc::new(Box::new(DefaultBlockIdManager::default())),
    }
}

#[derive(Default)]
pub struct PartitionedBlockIdManager {
    block_id_bitmap: DashMap<i32, Arc<RwLock<Treemap>>>,
    number: AtomicU64,
}

#[async_trait]
impl BlockIdManager for PartitionedBlockIdManager {
    async fn get_multi_block_ids(&self, ctx: GetMultiBlockIdsContext) -> Result<Bytes> {
        let shuffle_id = &ctx.shuffle_id;
        let block_id_layout = &ctx.layout;
        let partitions: HashSet<&i32> = HashSet::from_iter(&ctx.partition_ids);

        let treemap = self
            .block_id_bitmap
            .entry(*shuffle_id)
            .or_insert_with(|| Arc::new(RwLock::new(Treemap::new())))
            .clone();
        let treemap = treemap.read();
        let mut retrieved = Treemap::new();
        for element in treemap.iter() {
            let partition_id = block_id_layout.get_partition_id(element as i64);
            if partitions.contains(&(partition_id as i32)) {
                retrieved.add(element);
            }
        }
        Ok(Bytes::from(retrieved.serialize::<JvmLegacy>()))
    }

    async fn report_multi_block_ids(&self, ctx: ReportMultiBlockIdsContext) -> Result<()> {
        let shuffle_id = &ctx.shuffle_id;
        let treemap = self
            .block_id_bitmap
            .entry(*shuffle_id)
            .or_insert_with(|| Arc::new(RwLock::new(Treemap::new())))
            .clone();
        let partitioned_block_ids = ctx.block_ids;
        let mut treemap = treemap.write();
        let mut number = 0;
        for (_, block_ids) in partitioned_block_ids {
            number += block_ids.len();
            for block_id in block_ids {
                treemap.add(block_id as u64);
            }
        }
        self.number.fetch_add(number as u64, SeqCst);
        Ok(())
    }

    async fn purge_block_ids(&self, shuffle_id: i32) -> Result<()> {
        if let Some(treemap) = self.block_id_bitmap.remove(&shuffle_id) {
            let map = treemap.1.read();
            self.number.fetch_sub(map.cardinality(), SeqCst);
        }
        Ok(())
    }

    async fn get_blocks_number(&self) -> Result<u64> {
        let number = self.number.load(SeqCst);
        Ok(number)
    }
}

#[derive(Default)]
struct DefaultBlockIdManager {
    number: AtomicU64,
    // key: (shuffle_id, partition_id)
    block_id_bitmap: DashMap<(i32, i32), Arc<RwLock<Treemap>>>,
}

#[async_trait]
impl BlockIdManager for DefaultBlockIdManager {
    async fn get_multi_block_ids(&self, ctx: GetMultiBlockIdsContext) -> Result<Bytes> {
        let shuffle_id = ctx.shuffle_id;
        let partition_ids = ctx.partition_ids;
        let mut treemap = Treemap::new();
        for pid in partition_ids {
            if let Some(bitmap) = self.block_id_bitmap.get(&(shuffle_id, pid)) {
                let bitmap = bitmap.clone();
                let bitmap = bitmap.read();
                treemap.extend(bitmap.iter());
            }
        }
        Ok(Bytes::from(treemap.serialize::<JvmLegacy>()))
    }

    async fn report_multi_block_ids(&self, ctx: ReportMultiBlockIdsContext) -> Result<()> {
        let shuffle_id = ctx.shuffle_id;
        let partitioned_block_ids = ctx.block_ids;
        let mut number = 0;
        for (pid, block_ids) in partitioned_block_ids {
            number += block_ids.len();
            let treemap = self
                .block_id_bitmap
                .entry((shuffle_id, pid))
                .or_insert_with(|| Arc::new(RwLock::new(Treemap::new())))
                .clone();
            let mut treemap = treemap.write();
            for block_id in block_ids {
                treemap.add(block_id as u64);
            }
        }
        self.number.fetch_add(number as u64, SeqCst);
        Ok(())
    }

    async fn purge_block_ids(&self, shuffle_id: i32) -> Result<()> {
        let view = self.block_id_bitmap.clone().into_read_only();
        let mut deletion_keys = vec![];
        for (v_shuffle_id, v_partition_id) in view.keys() {
            if *v_shuffle_id == shuffle_id {
                deletion_keys.push((shuffle_id, *v_partition_id));
            }
        }
        drop(view);
        let mut number = 0;
        for deletion_key in deletion_keys {
            if let Some(bitmap) = self.block_id_bitmap.remove(&deletion_key) {
                let bitmap = bitmap.1.read();
                number -= bitmap.cardinality();
            }
        }
        self.number.fetch_sub(number, SeqCst);
        Ok(())
    }

    async fn get_blocks_number(&self) -> Result<u64> {
        Ok(self.number.load(SeqCst))
    }
}
