mod buffer;
mod buffer_manager;
mod data_pusher;
mod memory_tracker;
mod rpc;

use std::sync::Arc;

use anyhow::{ensure, Result};
use bytes::Bytes;
use riffle_server::grpc::protobuf::uniffle::ShuffleBlock;

use self::buffer_manager::BufferManager;
use self::data_pusher::DataPusher;
use self::memory_tracker::{MemoryReservation, MemoryTracker};
use crate::rpc_client::ServerCoonPool;
use crate::{
    ApplicationId, Endpoint, PartitionId, ShuffleAssignment, ShuffleId, TaskId, TaskInfo,
    WriteResult, MAX_PAYLOAD_BYTES, TASK_ID_LIMIT,
};

const BLOCK_OVERHEAD_BYTES: usize = 128;
const BUFFER_CAPACITY_BYTES: usize = 64 * 1024 * 1024;
const MAX_IN_FLIGHT_REQUESTS: usize = 20;

struct WriterContext {
    app_id: ApplicationId,
    shuffle_id: ShuffleId,
    task_id: TaskId,
    conn: ServerCoonPool,
}

struct BufferedBlock {
    endpoint: Endpoint,
    partition_id: PartitionId,
    block: ShuffleBlock,
    memory: MemoryReservation,
}

pub struct ShuffleWriter {
    context: Arc<WriterContext>,
    assignment: Arc<ShuffleAssignment>,
    buffer_manager: BufferManager,
    pusher: Arc<DataPusher>,
}

impl ShuffleWriter {
    pub async fn new(task: TaskInfo) -> Result<Self> {
        ensure!(
            task.task_id < TASK_ID_LIMIT,
            "task ID exceeds 23-bit block ID range"
        );
        let memory = MemoryTracker::new(BUFFER_CAPACITY_BYTES);
        let assignment = Arc::new(task.shuffle.assignment);
        let context = Arc::new(WriterContext {
            app_id: task.shuffle.app_id,
            shuffle_id: task.shuffle.shuffle_id,
            task_id: task.task_id,
            conn: ServerCoonPool::new(),
        });
        let pusher = Arc::new(DataPusher::new(context.clone()));
        let buffers = BufferManager::new(task.task_id, assignment.clone(), memory, pusher.clone());
        Ok(Self {
            context,
            assignment,
            buffer_manager: buffers,
            pusher,
        })
    }

    /// Accepts one payload into the bounded send buffer. Finish waits for server acknowledgements.
    /// Cancellation while waiting for capacity does not enqueue data.
    pub async fn write(&mut self, partition_id: PartitionId, data: Bytes) -> Result<()> {
        self.pusher.check_failure()?;
        self.assignment.get_endpoint(partition_id)?;
        ensure!(
            data.len() <= MAX_PAYLOAD_BYTES,
            "payload exceeds {MAX_PAYLOAD_BYTES} bytes"
        );
        self.buffer_manager
            .append(partition_id, data)
            .await
            .map_err(|error| self.pusher.fail(error))
    }

    /// Drains accepted writes and reports their block IDs before publishing a WriteResult.
    /// This acknowledges server acceptance, not fsync or replication.
    pub async fn finish(mut self) -> Result<WriteResult> {
        self.pusher.check_failure()?;
        self.pusher.add(self.buffer_manager.clear()?)?;
        let block_ids = self.buffer_manager.into_block_ids();
        // Consuming the buffer manager leaves the writer as the only pusher owner.
        Arc::into_inner(self.pusher).unwrap().finish().await?;
        rpc::report_results(&self.context, &self.assignment, &block_ids).await?;
        Ok(WriteResult {
            app_id: self.context.app_id.clone(),
            shuffle_id: self.context.shuffle_id,
            task_id: self.context.task_id,
            block_ids,
        })
    }

    /// Cancels local work. Accepted remote blocks remain until shuffle or app cleanup.
    pub async fn abort(self) -> Result<()> {
        drop(self.buffer_manager);
        Arc::into_inner(self.pusher).unwrap().close().await
    }
}
