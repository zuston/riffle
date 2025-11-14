use crate::app_manager::app_configs::AppConfigOptions;
use crate::app_manager::partition_identifier::PartitionUId;
use crate::app_manager::purge_event::PurgeReason;
use crate::id_layout::IdLayout;
use crate::partition_stats::{PartitionStats, TaskToRecordStatRef};
use crate::store::Block;
use crate::urpc::command::ReadSegment;
use bytes::Bytes;
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
pub struct ReportShuffleResultContext {
    pub shuffle_id: i32,
    // the uniq id from client side.
    pub task_attempt_id: i64,
    pub block_ids: HashMap<i32, Vec<i64>>,
    // partition_id -> record_number
    pub record_numbers: HashMap<i32, i64>,
}
impl ReportShuffleResultContext {
    pub fn new(
        shuffle_id: i32,
        task_attempt_id: i64,
        block_ids: HashMap<i32, Vec<i64>>,
        partition_record_numbers: HashMap<i32, i64>,
    ) -> ReportShuffleResultContext {
        Self {
            shuffle_id,
            task_attempt_id,
            block_ids,
            record_numbers: partition_record_numbers,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GetShuffleResultContext {
    pub shuffle_id: i32,
    pub partition_ids: Vec<i32>,
    pub layout: IdLayout,
}

#[derive(Debug, Clone)]
pub struct ShuffleResult {
    pub serialized_block_ids: Bytes,
    pub partition_stats: Vec<PartitionStats>,
}

#[derive(Debug, Clone)]
pub struct WritingViewContext {
    pub uid: PartitionUId,
    pub data_blocks: Vec<Block>,
    pub data_size: u64,
}

impl WritingViewContext {
    // only for test
    pub fn create_for_test(uid: PartitionUId, data_blocks: Vec<Block>) -> Self {
        WritingViewContext {
            uid,
            data_blocks,
            data_size: 0,
        }
    }

    // only for test
    pub fn new_with_size(uid: PartitionUId, data_blocks: Vec<Block>, data_size: u64) -> Self {
        WritingViewContext {
            uid,
            data_blocks,
            data_size,
        }
    }

    pub fn new(uid: PartitionUId, data_blocks: Vec<Block>) -> Self {
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
    pub uid: PartitionUId,
    pub reading_options: ReadingOptions,
    pub task_ids_filter: Option<Treemap>,
    pub rpc_source: RpcType,
    pub sendfile_enabled: bool,

    // tag whether the read-ahead is enabled
    pub read_ahead_client_enabled: bool,

    // for the localfile read_ahead layer
    pub sequential: bool,
    pub read_ahead_batch_number: Option<usize>,
    pub read_ahead_batch_size: Option<usize>,

    // next read segments
    pub localfile_next_read_segments: Vec<ReadSegment>,
    pub task_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RpcType {
    GRPC,
    URPC,
}

impl ReadingViewContext {
    pub fn new(uid: PartitionUId, reading_options: ReadingOptions, rpc_source: RpcType) -> Self {
        ReadingViewContext {
            uid,
            reading_options,
            task_ids_filter: None,
            rpc_source,
            sendfile_enabled: false,
            read_ahead_client_enabled: false,
            sequential: false,
            read_ahead_batch_number: None,
            read_ahead_batch_size: None,
            localfile_next_read_segments: vec![],
            task_id: 0,
        }
    }

    // task_ids_filter and sequential_read are mutually exclusive.
    pub fn with_task_ids_filter(mut self, bitmap: Treemap) -> Self {
        self.task_ids_filter = Some(bitmap);
        self
    }

    pub fn with_sendfile_enabled(mut self) -> Self {
        self.sendfile_enabled = true;
        self
    }

    pub fn with_sequential(
        mut self,
        batch_number: Option<usize>,
        batch_size: Option<usize>,
    ) -> Self {
        self.sequential = true;
        self.read_ahead_batch_number = batch_number;
        self.read_ahead_batch_size = batch_size;
        self
    }

    pub fn with_localfile_next_read_segments(mut self, segments: Vec<ReadSegment>) -> Self {
        self.localfile_next_read_segments = segments;
        self
    }

    pub fn with_task_id(mut self, task_id: i64) -> Self {
        self.task_id = task_id;
        self
    }

    pub fn with_read_ahead_client_enabled(mut self, enabled: bool) -> Self {
        self.read_ahead_client_enabled = enabled;
        self
    }
}

pub struct ReadingIndexViewContext {
    pub partition_id: PartitionUId,
}

#[derive(Debug, Clone)]
pub struct RequireBufferContext {
    pub uid: PartitionUId,
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
    pub fn create_for_test(uid: PartitionUId, size: i64) -> Self {
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
