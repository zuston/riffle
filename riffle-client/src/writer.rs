mod buffer;
mod buffer_manager;
mod data_pusher;
mod memory_tracker;
mod rpc;

use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use bytes::Bytes;
use riffle_server::grpc::protobuf::uniffle::ShuffleBlock;

use self::buffer_manager::BufferManager;
use self::data_pusher::DataPusher;
use self::memory_tracker::{MemoryReservation, MemoryTracker};
use crate::manager::manager_rpc_client::ManagerRpcClient;
use crate::rpc_client::ServerCoonPool;
use crate::{
    ApplicationId, Endpoint, PartitionId, ShuffleAssignment, ShuffleId, TaskId, WriteTaskInfo,
    MAX_PAYLOAD_BYTES, TASK_ID_LIMIT,
};

const BLOCK_OVERHEAD_BYTES: usize = 128;
const BUFFER_CAPACITY_BYTES: usize = 64 * 1024 * 1024;
const MAX_IN_FLIGHT_REQUESTS: usize = 20;
const PARTITION_ID_LIMIT: usize = 1 << 24;

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
    manager: ManagerRpcClient,
    assignment: Arc<ShuffleAssignment>,
    buffer_manager: BufferManager,
    pusher: Arc<DataPusher>,
}

impl ShuffleWriter {
    /// Opens a task in a shuffle already registered with the manager under
    /// the key `<job_id>_<stage_id>`. The application ID must match that manager.
    pub async fn new(
        app_id: ApplicationId,
        manager_endpoint: Endpoint,
        task: WriteTaskInfo,
    ) -> Result<Self> {
        ensure!(!app_id.is_empty(), "application ID is empty");
        ensure!(task.num_mappers > 0, "mapper count must be positive");
        ensure!(task.map_id < task.num_mappers, "map ID is out of range");
        ensure!(
            task.num_partitions > 0 && task.num_partitions <= PARTITION_ID_LIMIT,
            "partition count must be between 1 and {PARTITION_ID_LIMIT}"
        );
        let manager = ManagerRpcClient::new(manager_endpoint)?;
        let (assignment, task_id) = manager.get_write_shuffle_handle(&task).await?;
        ensure!(
            task_id < TASK_ID_LIMIT,
            "task ID exceeds 23-bit block ID range"
        );
        let memory = MemoryTracker::new(BUFFER_CAPACITY_BYTES);
        let assignment = Arc::new(assignment);
        let context = Arc::new(WriterContext {
            app_id,
            shuffle_id: assignment.shuffle_id,
            task_id,
            conn: ServerCoonPool::new(),
        });
        let pusher = Arc::new(DataPusher::new(context.clone()));
        let buffers = BufferManager::new(task_id, assignment.clone(), memory, pusher.clone());
        Ok(Self {
            context,
            manager,
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

    /// Drains accepted writes and reports their block IDs before marking the task complete.
    /// This acknowledges server acceptance, not fsync or replication.
    pub async fn finish(mut self) -> Result<()> {
        self.pusher.check_failure()?;
        self.pusher.add(self.buffer_manager.clear()?)?;
        let block_ids = self.buffer_manager.into_block_ids();
        // Consuming the buffer manager leaves the writer as the only pusher owner.
        Arc::into_inner(self.pusher)
            .context("shuffle data pusher is still shared")?
            .finish()
            .await?;
        rpc::report_results(&self.context, &self.assignment, &block_ids).await?;
        self.manager
            .report_shuffle_result(self.context.shuffle_id, self.context.task_id)
            .await
    }

    /// Cancels local work without marking the task complete. Accepted remote blocks
    /// remain until shuffle or application cleanup.
    pub async fn abort(self) -> Result<()> {
        drop(self.buffer_manager);
        Arc::into_inner(self.pusher)
            .context("shuffle data pusher is still shared")?
            .close()
            .await
    }
}
