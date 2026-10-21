use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{anyhow, ensure, Context, Result};
use futures::future::{join_all, BoxFuture};
use log::info;
use riffle_server::grpc::protobuf::uniffle::ShuffleBlock;
use riffle_server::util::get_crc;
use tokio::sync::{mpsc, watch, Mutex, Semaphore};
use tokio::task::JoinSet;

use super::memory_tracker::MemoryReservation;
use super::{PendingBlock, BLOCK_OVERHEAD_BYTES};
use crate::codec::{run_codec_task, Codec};
use crate::rpc_client::manager_rpc_client::ManagerRpcClient;
use crate::rpc_client::writer_rpc_client::{WriterRpcClient, MAX_IN_FLIGHT_REQUESTS};
use crate::rpc_client::AttemptError;
use crate::{CompressionCodec, Endpoint, PartitionId, ShuffleAssignment};

const BATCH_SIZE_BYTES: usize = 4 * 1024 * 1024;
const MAX_BATCH_BLOCKS: usize = 1024;
const MAX_REASSIGNMENTS: usize = 3;

pub struct DataPusher {
    queue: mpsc::UnboundedSender<Batch>,
    failure: watch::Sender<Option<String>>,
    // Dropping the pusher also cancels its workers through JoinSet.
    workers: JoinSet<()>,
}

impl DataPusher {
    pub fn new(
        rpc_client: Arc<WriterRpcClient>,
        manager: ManagerRpcClient,
        assignment: Arc<ShuffleAssignment>,
        compression_codec: CompressionCodec,
    ) -> Self {
        let sender = Arc::new(BatchSender {
            rpc_client,
            manager,
            assignment,
            reassignment_lock: Mutex::new(()),
            send_slots: Semaphore::new(MAX_IN_FLIGHT_REQUESTS),
        });
        let (queue, receiver) = mpsc::unbounded_channel();
        let receiver = Arc::new(Mutex::new(receiver));
        let (failure, _) = watch::channel(None);
        let mut workers = JoinSet::new();
        for _ in 0..MAX_IN_FLIGHT_REQUESTS {
            workers.spawn(send_worker(
                sender.clone(),
                receiver.clone(),
                failure.clone(),
                compression_codec.codec(),
            ));
        }
        Self {
            queue,
            failure,
            workers,
        }
    }

    pub fn add(&self, blocks: Vec<PendingBlock>) -> Result<()> {
        self.check_failure()?;
        let mut batches: HashMap<Endpoint, Batch> = HashMap::new();
        for block in blocks {
            let endpoint = block.endpoint.clone();
            let batch = batches.entry(endpoint.clone()).or_default();
            batch.push(block);
            if batch.is_full() {
                let batch = batches.remove(&endpoint).unwrap();
                self.enqueue(batch)?;
            }
        }
        for batch in batches.into_values() {
            self.enqueue(batch)?;
        }
        Ok(())
    }

    fn enqueue(&self, batch: Batch) -> Result<()> {
        // Reservations bound queued bytes, so enqueueing never needs to wait for capacity.
        self.queue.send(batch).map_err(|_| self.send_error())
    }

    pub async fn finish(mut self) -> Result<()> {
        drop(self.queue);
        while let Some(result) = self.workers.join_next().await {
            if let Err(error) = result.context("shuffle send worker failed") {
                record_failure(&self.failure, error);
            }
        }
        if let Some(error) = self.failure.borrow().as_ref() {
            return Err(anyhow!("shuffle writer failed: {error}"));
        }
        Ok(())
    }

    pub async fn close(mut self) -> Result<()> {
        drop(self.queue);
        self.workers.abort_all();
        let mut outcome = Ok(());
        while let Some(result) = self.workers.join_next().await {
            if let Err(error) = result {
                if !error.is_cancelled() && outcome.is_ok() {
                    outcome = Err(error.into());
                }
            }
        }
        outcome
    }

    pub fn check_failure(&self) -> Result<()> {
        if self.failure.borrow().is_some() || self.queue.is_closed() {
            return Err(self.send_error());
        }
        Ok(())
    }

    pub fn fail(&self, error: anyhow::Error) -> anyhow::Error {
        record_failure(&self.failure, error);
        self.send_error()
    }

    pub async fn failed(&self) -> anyhow::Error {
        let mut failure = self.failure.subscribe();
        let _ = failure.wait_for(Option::is_some).await;
        self.send_error()
    }

    fn send_error(&self) -> anyhow::Error {
        match self.failure.borrow().as_ref() {
            Some(error) => anyhow!("shuffle writer failed: {error}"),
            None => anyhow!("shuffle data pusher closed"),
        }
    }
}

async fn send_worker(
    sender: Arc<BatchSender>,
    receiver: Arc<Mutex<mpsc::UnboundedReceiver<Batch>>>,
    failure: watch::Sender<Option<String>>,
    codec: &'static dyn Codec,
) {
    loop {
        let next = {
            let mut receiver = receiver.lock().await;
            receiver.recv().await
        };
        let Some(batch) = next else {
            return;
        };
        // Drain queued reservations after failure without cancelling in-flight sends.
        if failure.borrow().is_some() {
            continue;
        }
        let blocks = match batch.encode(codec).await {
            Ok(blocks) => blocks,
            Err(error) => {
                record_failure(&failure, error);
                continue;
            }
        };
        if failure.borrow().is_some() {
            continue;
        }
        if let Err(error) = sender.send_blocks(blocks, 0).await {
            record_failure(&failure, error);
        }
    }
}

struct BatchSender {
    rpc_client: Arc<WriterRpcClient>,
    manager: ManagerRpcClient,
    assignment: Arc<ShuffleAssignment>,
    reassignment_lock: Mutex<()>,
    send_slots: Semaphore,
}

impl BatchSender {
    fn send_blocks(
        &self,
        blocks: Vec<BufferedBlock>,
        reassignments: usize,
    ) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let mut servers: HashMap<Endpoint, Vec<BufferedBlock>> = HashMap::new();
            for block in blocks {
                // Queued blocks may predate a reassignment by another worker.
                let endpoint = self.assignment.get_endpoint(block.partition_id)?;
                servers.entry(endpoint).or_default().push(block);
            }
            // Each server owns its recovery; wait for all siblings even if one fails.
            join_all(
                servers
                    .into_iter()
                    .map(|(endpoint, blocks)| self.send_server(endpoint, blocks, reassignments)),
            )
            .await
            .into_iter()
            .collect()
        })
    }

    async fn send_server(
        &self,
        endpoint: Endpoint,
        blocks: Vec<BufferedBlock>,
        reassignments: usize,
    ) -> Result<()> {
        let permit = self.send_slots.acquire().await?;
        let result = self
            .rpc_client
            .send_batch(
                endpoint.clone(),
                blocks
                    .iter()
                    .map(|block| (block.partition_id, block.block.clone()))
                    .collect(),
            )
            .await;
        // A manager request must not occupy a slot needed by healthy servers.
        drop(permit);
        let Err(error) = result else {
            if reassignments > 0 {
                info!(
                    "Writer resend succeeded, app={}, shuffle={}, task={}, server={}, blocks={}, reassignments={reassignments}",
                    self.rpc_client.app_id(), self.assignment.shuffle_id,
                    self.rpc_client.task_id(), endpoint.uri(), blocks.len()
                );
            }
            return Ok(());
        };
        let Some(status) = error
            .downcast_ref::<AttemptError>()
            .and_then(|error| error.reassignment_status)
        else {
            return Err(error);
        };
        info!(
            "Writer shuffle send requires reassignment, app={}, shuffle={}, task={}, failed_server={}, failure_status={status}, reassignments={reassignments}/{MAX_REASSIGNMENTS}, blocks={}, error={error:#}",
            self.rpc_client.app_id(), self.assignment.shuffle_id,
            self.rpc_client.task_id(), endpoint.uri(), blocks.len()
        );
        if reassignments == MAX_REASSIGNMENTS {
            return Err(error.context("shuffle reassignment limit reached"));
        }
        self.reassign(&endpoint, &blocks, status)
            .await
            .inspect_err(|error| {
                info!(
                    "Writer partition reassignment failed, app={}, shuffle={}, task={}, failed_server={}, error={error:#}",
                    self.rpc_client.app_id(), self.assignment.shuffle_id,
                    self.rpc_client.task_id(), endpoint.uri()
                );
            })
            .with_context(|| format!("reassignment after write failure: {error:#}"))?;
        // Route only this server's failed blocks again, preserving IDs and reservations.
        self.send_blocks(blocks, reassignments + 1).await
    }

    async fn reassign(
        &self,
        failed_endpoint: &Endpoint,
        blocks: &[BufferedBlock],
        status: i32,
    ) -> Result<()> {
        let _guard = self.reassignment_lock.lock().await;
        let mut partitions = HashSet::new();
        for block in blocks {
            if self.assignment.get_endpoint(block.partition_id)? == *failed_endpoint {
                partitions.insert(block.partition_id);
            }
        }
        // Another worker may already have obtained the replacement.
        if partitions.is_empty() {
            info!(
                "Writer skipped reassignment because routing already changed, app={}, shuffle={}, task={}, failed_server={}",
                self.rpc_client.app_id(), self.assignment.shuffle_id,
                self.rpc_client.task_id(), failed_endpoint.uri()
            );
            return Ok(());
        }
        let partitions: Vec<_> = partitions.into_iter().collect();
        info!(
            "Writer requesting partition reassignment, app={}, shuffle={}, task={}, partitions={partitions:?}, failed_server={}, failure_status={status}",
            self.rpc_client.app_id(), self.assignment.shuffle_id,
            self.rpc_client.task_id(), failed_endpoint.uri()
        );
        let assignments = self
            .manager
            .reassign_partitions(
                self.assignment.shuffle_id,
                &partitions,
                failed_endpoint,
                status,
            )
            .await?;
        // Validate the whole response before publishing any routing changes.
        for (&partition, endpoints) in &assignments {
            ensure!(
                endpoints.last() != Some(failed_endpoint),
                "reassignment returned the failed endpoint for partition {partition}"
            );
            let previous = self
                .assignment
                .assignments
                .get(&partition)
                .context("reassigned partition has no previous assignment")?;
            ensure!(
                endpoints.starts_with(previous.value()),
                "reassignment lost endpoint history for partition {partition}"
            );
        }
        info!(
            "Writer applying partition reassignment, app={}, shuffle={}, task={}, failed_server={}, assignments={assignments:?}",
            self.rpc_client.app_id(), self.assignment.shuffle_id,
            self.rpc_client.task_id(), failed_endpoint.uri()
        );
        for (partition, endpoints) in assignments {
            self.assignment.assignments.insert(partition, endpoints);
        }
        Ok(())
    }
}

fn record_failure(failure: &watch::Sender<Option<String>>, error: anyhow::Error) {
    failure.send_if_modified(|stored| {
        if stored.is_some() {
            return false;
        }
        *stored = Some(format!("{error:#}"));
        true
    });
}

#[derive(Default)]
struct Batch {
    blocks: Vec<PendingBlock>,
    bytes: usize,
}

struct BufferedBlock {
    partition_id: PartitionId,
    block: ShuffleBlock,
    _memory: MemoryReservation,
}

impl Batch {
    async fn encode(self, codec: &'static dyn Codec) -> Result<Vec<BufferedBlock>> {
        run_codec_task(move || {
            let mut blocks = Vec::with_capacity(self.blocks.len());
            for pending in self.blocks {
                let uncompress_length = pending.data.len() as i32;
                let data = codec.encode(pending.data)?;
                let block = ShuffleBlock {
                    block_id: pending.block_id,
                    length: data.len() as i32,
                    uncompress_length,
                    crc: get_crc(&data),
                    data,
                    task_attempt_id: pending.task_attempt_id,
                };
                blocks.push(BufferedBlock {
                    partition_id: pending.partition_id,
                    block,
                    _memory: pending.memory,
                });
            }
            Ok(blocks)
        })
        .await
    }

    fn push(&mut self, block: PendingBlock) {
        debug_assert!(!self.is_full());
        self.bytes += block.data.len() + BLOCK_OVERHEAD_BYTES;
        self.blocks.push(block);
    }

    fn is_full(&self) -> bool {
        self.bytes >= BATCH_SIZE_BYTES || self.blocks.len() >= MAX_BATCH_BLOCKS
    }
}
