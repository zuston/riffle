mod rpc;

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::{anyhow, ensure, Context, Result};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use riffle_server::grpc::protobuf::uniffle::ShuffleBlock;
use riffle_server::id_layout::DEFAULT_BLOCK_ID_LAYOUT;
use riffle_server::util::get_crc;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};

use crate::rpc_client::SharableShuffleServerClientFactory;
use crate::{
    assignment, Endpoint, PartitionId, ShuffleId, TaskId, TaskInfo, WriteResult,
    BLOCK_SEQUENCE_LIMIT, MAX_MESSAGE_BYTES, MAX_PAYLOAD_BYTES, RPC_TIMEOUT, TASK_ID_LIMIT,
};

const BLOCK_OVERHEAD_BYTES: usize = 128;
const MAX_QUEUED_BLOCKS: usize = 1024;
const MAX_BATCH_BLOCKS: usize = 1024;

#[derive(Clone, Debug)]
pub struct WriterOptions {
    /// Payload bytes plus a per-block metadata allowance, including queued and in-flight data.
    /// Transport encoding buffers and memory retained by shared Bytes allocations are additional.
    pub buffer_capacity_bytes: usize,
    /// Flush threshold per server; an individual payload is never split.
    pub batch_size_bytes: usize,
    pub max_in_flight_requests: usize,
    pub flush_interval: Duration,
    pub rpc_timeout: Duration,
    /// Total attempts per batch or result report, including the initial attempt.
    pub max_attempts: usize,
}

impl Default for WriterOptions {
    fn default() -> Self {
        Self {
            buffer_capacity_bytes: 64 * 1024 * 1024,
            batch_size_bytes: 4 * 1024 * 1024,
            max_in_flight_requests: 4,
            flush_interval: Duration::from_millis(20),
            rpc_timeout: RPC_TIMEOUT,
            max_attempts: 8,
        }
    }
}

impl WriterOptions {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.buffer_capacity_bytes > BLOCK_OVERHEAD_BYTES
                && self.buffer_capacity_bytes <= Semaphore::MAX_PERMITS.min(u32::MAX as usize),
            "invalid writer buffer capacity"
        );
        ensure!(
            self.batch_size_bytes > 0
                && self.batch_size_bytes <= self.buffer_capacity_bytes
                && self.batch_size_bytes <= MAX_MESSAGE_BYTES / 2,
            "batch size must fit the writer buffer and half the RPC message limit"
        );
        ensure!(
            self.max_in_flight_requests > 0 && self.max_attempts > 0,
            "writer concurrency and attempt count must be positive"
        );
        ensure!(
            !self.flush_interval.is_zero() && !self.rpc_timeout.is_zero(),
            "writer flush interval and RPC timeout must be positive"
        );
        Ok(())
    }
}

struct WriterContext {
    app_id: String,
    shuffle_id: ShuffleId,
    protocol_shuffle_id: i32,
    task_id: TaskId,
    options: WriterOptions,
    clients: SharableShuffleServerClientFactory,
}

struct BufferedBlock {
    endpoint: Endpoint,
    partition_id: PartitionId,
    block: ShuffleBlock,
    memory: OwnedSemaphorePermit,
}

#[derive(Default)]
struct Batch {
    blocks: Vec<BufferedBlock>,
    bytes: usize,
}

impl Batch {
    fn is_full(&self, options: &WriterOptions) -> bool {
        self.bytes >= options.batch_size_bytes || self.blocks.len() >= MAX_BATCH_BLOCKS
    }
}

pub struct ShuffleWriter {
    context: Arc<WriterContext>,
    routes: HashMap<PartitionId, Endpoint>,
    blocks_number: BTreeMap<PartitionId, u32>,
    sender: Option<mpsc::Sender<BufferedBlock>>,
    memory: Arc<Semaphore>,
    failure: Arc<OnceLock<anyhow::Error>>,
    send_task: JoinHandle<()>,
}

impl ShuffleWriter {
    pub async fn new(task: TaskInfo) -> Result<Self> {
        Self::with_options(task, WriterOptions::default()).await
    }

    pub async fn with_options(task: TaskInfo, options: WriterOptions) -> Result<Self> {
        options.validate()?;
        ensure!(
            task.task_id < TASK_ID_LIMIT,
            "task ID exceeds 23-bit block ID range"
        );
        let routes = assignment(&task.shuffle)?;
        let protocol_shuffle_id = i32::try_from(task.shuffle.shuffle_id)
            .context("shuffle ID exceeds protocol i32 range")?;
        let memory = Arc::new(Semaphore::new(options.buffer_capacity_bytes));
        let context = Arc::new(WriterContext {
            app_id: task.shuffle.app_id,
            shuffle_id: task.shuffle.shuffle_id,
            protocol_shuffle_id,
            task_id: task.task_id,
            options,
            clients: SharableShuffleServerClientFactory::new(),
        });
        let (sender, mut receiver) = mpsc::channel(MAX_QUEUED_BLOCKS);
        let failure = Arc::new(OnceLock::new());
        let send_task = tokio::spawn({
            let context = context.clone();
            let failure = failure.clone();
            let memory = memory.clone();
            async move {
                if let Err(error) = send_loop(context, &mut receiver).await {
                    let _ = failure.set(error);
                    memory.close();
                }
            }
        });
        Ok(Self {
            context,
            routes,
            blocks_number: BTreeMap::new(),
            sender: Some(sender),
            memory,
            failure,
            send_task,
        })
    }

    /// Accepts one payload into the bounded send buffer. Finish waits for server acknowledgements.
    /// Cancellation while waiting for capacity does not consume a block ID or enqueue data.
    pub async fn write(&mut self, partition_id: PartitionId, data: Bytes) -> Result<()> {
        self.check_failure()?;
        let endpoint = self
            .routes
            .get(&partition_id)
            .context("partition has no assignment")?
            .clone();
        ensure!(
            data.len() <= MAX_PAYLOAD_BYTES,
            "payload exceeds {MAX_PAYLOAD_BYTES} bytes"
        );
        let charge = data.len() + BLOCK_OVERHEAD_BYTES;
        ensure!(
            charge <= self.context.options.buffer_capacity_bytes,
            "payload exceeds writer buffer capacity"
        );
        let sequence = self.blocks_number.get(&partition_id).copied().unwrap_or(0);
        ensure!(
            sequence < BLOCK_SEQUENCE_LIMIT,
            "block sequence limit reached"
        );

        let memory = self
            .memory
            .clone()
            .acquire_many_owned(charge as u32)
            .await
            .map_err(|_| self.send_error())?;
        let slot = self
            .sender
            .as_ref()
            .unwrap()
            .reserve()
            .await
            .map_err(|_| self.send_error())?;
        self.check_failure()?;
        let block = ShuffleBlock {
            block_id: DEFAULT_BLOCK_ID_LAYOUT.get_block_id(
                sequence.into(),
                partition_id as i64,
                self.context.task_id as i64,
            ),
            length: data.len() as i32,
            uncompress_length: data.len() as i32,
            crc: get_crc(&data),
            data,
            task_attempt_id: self.context.task_id as i64,
        };
        // No await between admission and sequence advancement, so cancellation cannot create holes.
        slot.send(BufferedBlock {
            endpoint,
            partition_id,
            block,
            memory,
        });
        self.blocks_number.insert(partition_id, sequence + 1);
        Ok(())
    }

    /// Drains accepted writes and reports their block IDs before publishing a WriteResult.
    /// This acknowledges server acceptance, not fsync or replication.
    pub async fn finish(mut self) -> Result<WriteResult> {
        self.sender.take();
        (&mut self.send_task)
            .await
            .context("shuffle send task failed")?;
        self.check_failure()?;
        rpc::report_results(&self.context, &self.routes, &self.blocks_number).await?;
        Ok(WriteResult {
            app_id: self.context.app_id.clone(),
            shuffle_id: self.context.shuffle_id,
            task_id: self.context.task_id,
            blocks_number: std::mem::take(&mut self.blocks_number),
        })
    }

    /// Cancels local work. Accepted remote blocks remain until shuffle or app cleanup.
    pub async fn abort(mut self) -> Result<()> {
        self.send_task.abort();
        self.sender.take();
        match (&mut self.send_task).await {
            Ok(()) => Ok(()),
            Err(error) if error.is_cancelled() => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn check_failure(&self) -> Result<()> {
        if self.failure.get().is_some() {
            return Err(self.send_error());
        }
        Ok(())
    }

    fn send_error(&self) -> anyhow::Error {
        match self.failure.get() {
            Some(error) => anyhow!("shuffle writer failed: {error:#}"),
            None => anyhow!("shuffle send task stopped"),
        }
    }
}

impl Drop for ShuffleWriter {
    fn drop(&mut self) {
        self.send_task.abort();
    }
}

async fn send_loop(
    context: Arc<WriterContext>,
    receiver: &mut mpsc::Receiver<BufferedBlock>,
) -> Result<()> {
    let mut pending: HashMap<Endpoint, Batch> = HashMap::new();
    let mut in_flight: FuturesUnordered<BoxFuture<'static, Result<()>>> = FuturesUnordered::new();
    let mut timer = tokio::time::interval_at(
        Instant::now() + context.options.flush_interval,
        context.options.flush_interval,
    );
    timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut input_closed = false;
    let mut flush_pending = false;
    loop {
        while in_flight.len() < context.options.max_in_flight_requests {
            let endpoint = pending
                .iter()
                .find(|(_, batch)| input_closed || flush_pending || batch.is_full(&context.options))
                .map(|(endpoint, _)| endpoint.clone());
            let Some(endpoint) = endpoint else {
                break;
            };
            let batch = pending.remove(&endpoint).unwrap();
            in_flight.push(Box::pin(rpc::send_batch(
                context.clone(),
                endpoint,
                batch.blocks,
            )));
        }
        if pending.is_empty() {
            flush_pending = false;
            if input_closed && in_flight.is_empty() {
                return Ok(());
            }
        }
        let receive = !input_closed
            && !flush_pending
            && !pending
                .values()
                .any(|batch| batch.is_full(&context.options));
        tokio::select! {
            biased;
            Some(result) = in_flight.next(), if !in_flight.is_empty() => result?,
            _ = timer.tick(), if !flush_pending && !pending.is_empty() => {
                flush_pending = true;
            }
            block = receiver.recv(), if receive => {
                match block {
                    Some(block) => {
                        let batch = pending.entry(block.endpoint.clone()).or_default();
                        batch.bytes += block.block.data.len() + BLOCK_OVERHEAD_BYTES;
                        batch.blocks.push(block);
                    }
                    None => input_closed = true,
                }
            }
        }
    }
}
