use anyhow::{ensure, Result};
use riffle_server::id_layout::DEFAULT_BLOCK_ID_LAYOUT;

use super::buffer::ReadBatch;
use crate::rpc_client::ServerClient;
use crate::{
    CompressionCodec, Endpoint, PartitionId, ShuffleHandle, MAX_MESSAGE_BYTES, RECORD_HEADER_BYTES,
    TASK_ID_LIMIT,
};

pub(super) const READ_BATCH_BYTES: i32 = 14 * 1024 * 1024;

#[tonic::async_trait]
pub(super) trait ReadHandler: Send {
    /// None exhausts this handler; only the hybrid handler guarantees partition completeness.
    async fn read(&mut self) -> Result<Option<ReadBatch>>;
}

#[derive(Clone)]
pub(super) struct PartitionReadContext<'a> {
    pub shuffle: &'a ShuffleHandle,
    pub partition: PartitionId,
    // todo: support multi server read
    pub endpoint: Endpoint,
    pub client: ServerClient,
    pub compression_codec: CompressionCodec,
}

impl PartitionReadContext<'_> {
    pub fn validate_block(
        &self,
        block_id: i64,
        task_id: i64,
        length: i32,
        uncompress_length: i32,
    ) -> Result<()> {
        ensure!(
            block_id >= 0
                && DEFAULT_BLOCK_ID_LAYOUT.get_partition_id(block_id) == self.partition as i64
                && task_id == block_id & (TASK_ID_LIMIT as i64 - 1),
            "block {block_id} has inconsistent partition or task ID"
        );
        ensure!(
            length > 0 && length as usize <= MAX_MESSAGE_BYTES,
            "invalid length for block {block_id}"
        );
        ensure!(
            uncompress_length >= RECORD_HEADER_BYTES as i32
                && uncompress_length as usize <= MAX_MESSAGE_BYTES
                && length <= uncompress_length,
            "invalid uncompressed length for block {block_id}"
        );
        Ok(())
    }
}
