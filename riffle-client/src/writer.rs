mod buffer;
mod buffer_manager;
mod data_pusher;
mod memory_tracker;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use bytes::Bytes;
use riffle_server::client_configs::ClientRssConf;

use self::buffer_manager::BufferManager;
use self::data_pusher::DataPusher;
use self::memory_tracker::{MemoryReservation, MemoryTracker};
use crate::configs::COMPRESSION_CODEC;
use crate::rpc_client::manager_rpc_client::ManagerRpcClient;
use crate::rpc_client::writer_rpc_client::WriterRpcClient;
use crate::{
    ApplicationId, Endpoint, PartitionId, ShuffleAssignment, TaskId, WriteTaskInfo,
    MAX_PAYLOAD_BYTES, TASK_ID_LIMIT,
};

const BLOCK_OVERHEAD_BYTES: usize = 128;
const BUFFER_CAPACITY_BYTES: usize = 64 * 1024 * 1024;
const PARTITION_ID_LIMIT: usize = 1 << 24;

struct PendingBlock {
    endpoint: Endpoint,
    partition_id: PartitionId,
    block_id: i64,
    task_attempt_id: i64,
    data: Bytes,
    memory: MemoryReservation,
}

pub struct ShuffleWriter {
    rpc_client: Arc<WriterRpcClient>,
    task_id: TaskId,
    manager: ManagerRpcClient,
    assignment: Arc<ShuffleAssignment>,
    buffer_manager: BufferManager,
    pusher: Arc<DataPusher>,
    num_records: HashMap<PartitionId, u64>,
}

impl ShuffleWriter {
    /// Opens a task, registering the shuffle on first use under the key
    /// `(job_id, stage_id)`. The application ID must match the manager.
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
        let manager = ManagerRpcClient::new(app_id.clone(), manager_endpoint)?;
        let (assignment, task_id, configs) = manager.get_write_shuffle_handle(&task).await?;
        let compression_codec = ClientRssConf::from(configs)
            .get(&COMPRESSION_CODEC)
            .context("invalid riffle.client.compression.codec; expected lz4 or zstd")?;
        ensure!(
            task_id < TASK_ID_LIMIT,
            "task ID exceeds 23-bit block ID range"
        );
        let memory = MemoryTracker::new(BUFFER_CAPACITY_BYTES);
        let assignment = Arc::new(assignment);
        let rpc_client = Arc::new(WriterRpcClient::new(app_id, assignment.shuffle_id, task_id));
        let pusher = Arc::new(DataPusher::new(
            rpc_client.clone(),
            manager.clone(),
            assignment.clone(),
            compression_codec,
        ));
        let buffer_manager =
            BufferManager::new(task_id, assignment.clone(), memory, pusher.clone());
        Ok(Self {
            rpc_client,
            task_id,
            manager,
            assignment,
            buffer_manager,
            pusher,
            num_records: HashMap::new(),
        })
    }

    /// Accepts one record (including an empty payload) into the bounded send buffer.
    /// Blocks use the user's configured codec off the async runtime before sending.
    /// Finish waits for compression and server acknowledgements.
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
            .map_err(|error| self.pusher.fail(error))?;
        *self.num_records.entry(partition_id).or_default() += 1;
        Ok(())
    }

    /// Drains accepted writes and reports block IDs and record counts before marking the task complete.
    /// This acknowledges server acceptance, not fsync or replication.
    pub async fn finish(mut self) -> Result<()> {
        self.pusher.check_failure()?;
        self.pusher.add(self.buffer_manager.clear()?)?;
        drop(self.buffer_manager);
        // Consuming the buffer manager leaves the writer as the only pusher owner.
        Arc::into_inner(self.pusher)
            .context("shuffle data pusher is still shared")?
            .finish()
            .await?;
        self.rpc_client.report_results(&self.num_records).await?;
        self.manager
            .report_shuffle_result(self.assignment.shuffle_id, self.task_id)
            .await
    }

    /// Cancels local work without marking the task complete. Accepted remote blocks
    /// remain until shuffle or application cleanup. Running codec jobs finish in the background.
    pub async fn abort(self) -> Result<()> {
        drop(self.buffer_manager);
        Arc::into_inner(self.pusher)
            .context("shuffle data pusher is still shared")?
            .close()
            .await
    }
}
