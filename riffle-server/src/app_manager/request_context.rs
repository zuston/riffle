use crate::app_manager::app_configs::AppConfigOptions;
use crate::app_manager::partition_identifier::PartitionedUId;
use crate::app_manager::purge_event::PurgeReason;
use crate::id_layout::IdLayout;
use crate::store::Block;
use croaring::Treemap;
use std::collections::HashMap;
use std::ops::Deref;

#[derive(Debug, Clone)]
pub struct PurgeDataContext {
    pub purge_reason: PurgeReason,
}

impl PurgeDataContext {
    pub fn new(reason: &PurgeReason) -> PurgeDataContext {
        PurgeDataContext {
            purge_reason: reason.clone(),
        }
    }
}

impl Deref for PurgeDataContext {
    type Target = PurgeReason;

    fn deref(&self) -> &Self::Target {
        &self.purge_reason
    }
}

#[derive(Debug, Clone)]
pub struct ReportBlocksContext {
    pub(crate) uid: PartitionedUId,
    pub(crate) blocks: Vec<i64>,
}

#[derive(Debug, Clone)]
pub struct ReportMultiBlockIdsContext {
    pub shuffle_id: i32,
    pub block_ids: HashMap<i32, Vec<i64>>,
}
impl ReportMultiBlockIdsContext {
    pub fn new(shuffle_id: i32, block_ids: HashMap<i32, Vec<i64>>) -> ReportMultiBlockIdsContext {
        Self {
            shuffle_id,
            block_ids,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GetMultiBlockIdsContext {
    pub shuffle_id: i32,
    pub partition_ids: Vec<i32>,
    pub layout: IdLayout,
}

#[derive(Debug, Clone)]
pub struct GetBlocksContext {
    pub(crate) uid: PartitionedUId,
}

#[derive(Debug, Clone)]
pub struct WritingViewContext {
    pub uid: PartitionedUId,
    pub data_blocks: Vec<Block>,
    pub data_size: u64,
}

impl WritingViewContext {
    // only for test
    pub fn create_for_test(uid: PartitionedUId, data_blocks: Vec<Block>) -> Self {
        WritingViewContext {
            uid,
            data_blocks,
            data_size: 0,
        }
    }

    // only for test
    pub fn new_with_size(uid: PartitionedUId, data_blocks: Vec<Block>, data_size: u64) -> Self {
        WritingViewContext {
            uid,
            data_blocks,
            data_size,
        }
    }

    pub fn new(uid: PartitionedUId, data_blocks: Vec<Block>) -> Self {
        let len: u64 = data_blocks.iter().map(|block| block.length).sum::<i32>() as u64;
        WritingViewContext {
            uid,
            data_blocks,
            data_size: len,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReadingViewContext {
    pub uid: PartitionedUId,
    pub reading_options: ReadingOptions,
    pub serialized_expected_task_ids_bitmap: Option<Treemap>,
}

pub struct ReadingIndexViewContext {
    pub partition_id: PartitionedUId,
}

#[derive(Debug, Clone)]
pub struct RequireBufferContext {
    pub uid: PartitionedUId,
    pub size: i64,
    // todo: we should replace uid with (app_id, shuffle_id).
    pub partition_ids: Vec<i32>,
}

#[derive(Debug, Clone)]
pub struct RegisterAppContext {
    pub app_id: String,
    pub app_config_options: AppConfigOptions,
}

#[derive(Debug, Clone)]
pub struct ReleaseTicketContext {
    pub(crate) ticket_id: i64,
}

impl From<i64> for ReleaseTicketContext {
    fn from(value: i64) -> Self {
        Self { ticket_id: value }
    }
}

impl RequireBufferContext {
    pub fn create_for_test(uid: PartitionedUId, size: i64) -> Self {
        Self {
            uid,
            size,
            partition_ids: vec![],
        }
    }
}

#[derive(Debug, Clone)]
pub enum ReadingOptions {
    #[allow(non_camel_case_types)]
    MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(i64, i64),
    #[allow(non_camel_case_types)]
    FILE_OFFSET_AND_LEN(i64, i64),
}
