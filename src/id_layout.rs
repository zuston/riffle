use crate::grpc::protobuf::uniffle::BlockIdLayout;
use once_cell::sync::Lazy;
use std::ops::Deref;

pub const DEFAULT_BLOCK_ID_LAYOUT: Lazy<IdLayout> = Lazy::new(|| {
    IdLayout::new(
        DEFAULT_SEQUENCE_NO_BIT,
        DEFAULT_PARTITION_ID_BIT,
        DEFAULT_TASK_ID_BIT,
    )
});

const DEFAULT_SEQUENCE_NO_BIT: i32 = 18;
const DEFAULT_PARTITION_ID_BIT: i32 = 24;
const DEFAULT_TASK_ID_BIT: i32 = 21;

#[derive(Debug, Clone)]
pub struct IdLayout {
    sequence_no_bits: i32,
    partition_id_bits: i32,
    task_attempt_id_bits: i32,

    partition_id_mask: i64,
    partition_id_offset: i32,
}

impl IdLayout {
    fn new(sequence_no_bits: i32, partition_id_bits: i32, task_attempt_id_bits: i32) -> Self {
        let max_partition_id = (1 << partition_id_bits) - 1;
        Self {
            sequence_no_bits,
            partition_id_bits,
            task_attempt_id_bits,
            partition_id_mask: max_partition_id << task_attempt_id_bits,
            partition_id_offset: task_attempt_id_bits,
        }
    }

    pub fn get_partition_id(&self, block_id: i64) -> i64 {
        (block_id & self.partition_id_mask) >> self.partition_id_offset
    }

    pub fn get_block_id(&self, sequence_no: i64, partition_id: i64, task_attempt_id: i64) -> i64 {
        let s = sequence_no << (self.partition_id_bits + self.task_attempt_id_bits);
        let p = partition_id << self.task_attempt_id_bits;
        let t = task_attempt_id << 0;
        let result = s | p | t;
        result
    }
}

impl From<&BlockIdLayout> for IdLayout {
    fn from(value: &BlockIdLayout) -> Self {
        Self::new(
            value.sequence_no_bits,
            value.partition_id_bits,
            value.task_attempt_id_bits,
        )
    }
}

pub fn to_layout(layout: Option<BlockIdLayout>) -> IdLayout {
    if let Some(block_id_layout) = layout {
        IdLayout::new(
            block_id_layout.sequence_no_bits,
            block_id_layout.partition_id_bits,
            block_id_layout.task_attempt_id_bits,
        )
    } else {
        IdLayout::new(
            DEFAULT_SEQUENCE_NO_BIT,
            DEFAULT_PARTITION_ID_BIT,
            DEFAULT_TASK_ID_BIT,
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::grpc::protobuf::uniffle::BlockIdLayout;
    use crate::id_layout::{
        to_layout, IdLayout, DEFAULT_PARTITION_ID_BIT, DEFAULT_SEQUENCE_NO_BIT, DEFAULT_TASK_ID_BIT,
    };

    #[test]
    fn test_id_layout() {
        let layout = IdLayout::new(18, 24, 21);
        let block_id = layout.get_block_id(123, 45, 67);
        assert_eq!(45, layout.get_partition_id(block_id));
    }

    #[test]
    fn test_into() {
        // case1: empty
        let layout: Option<BlockIdLayout> = None;
        let layout: IdLayout = to_layout(layout);
        assert_eq!(DEFAULT_PARTITION_ID_BIT, layout.partition_id_bits);
        assert_eq!(DEFAULT_SEQUENCE_NO_BIT, layout.sequence_no_bits);
        assert_eq!(DEFAULT_TASK_ID_BIT, layout.task_attempt_id_bits);

        // case2
        let layout: Option<BlockIdLayout> = Some(BlockIdLayout {
            sequence_no_bits: 20,
            partition_id_bits: 20,
            task_attempt_id_bits: 23,
        });
        let layout: IdLayout = to_layout(layout);
        assert_eq!(20, layout.partition_id_bits);
        assert_eq!(20, layout.sequence_no_bits);
        assert_eq!(23, layout.task_attempt_id_bits);
    }
}
