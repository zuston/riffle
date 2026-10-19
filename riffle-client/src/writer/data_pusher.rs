use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::JoinSet;

use super::{BufferedBlock, BLOCK_OVERHEAD_BYTES};
use crate::rpc_client::writer_rpc_client::{WriterRpcClient, MAX_IN_FLIGHT_REQUESTS};
use crate::Endpoint;

const BATCH_SIZE_BYTES: usize = 4 * 1024 * 1024;
const MAX_BATCH_BLOCKS: usize = 1024;

pub struct DataPusher {
    queue: mpsc::UnboundedSender<(Endpoint, Batch)>,
    failure: watch::Sender<Option<String>>,
    // Dropping the pusher also cancels its workers through JoinSet.
    workers: JoinSet<()>,
}

impl DataPusher {
    pub fn new(rpc_client: Arc<WriterRpcClient>) -> Self {
        let (queue, receiver) = mpsc::unbounded_channel();
        let receiver = Arc::new(Mutex::new(receiver));
        let (failure, _) = watch::channel(None);
        let mut workers = JoinSet::new();
        for _ in 0..MAX_IN_FLIGHT_REQUESTS {
            workers.spawn(send_worker(
                rpc_client.clone(),
                receiver.clone(),
                failure.clone(),
            ));
        }
        Self {
            queue,
            failure,
            workers,
        }
    }

    pub fn add(&self, blocks: Vec<BufferedBlock>) -> Result<()> {
        self.check_failure()?;
        let mut batches: HashMap<Endpoint, Batch> = HashMap::new();
        for block in blocks {
            let endpoint = block.endpoint.clone();
            let batch = batches.entry(endpoint.clone()).or_default();
            batch.push(block);
            if batch.is_full() {
                let batch = batches.remove(&endpoint).unwrap();
                self.enqueue(endpoint, batch)?;
            }
        }
        for (endpoint, batch) in batches {
            self.enqueue(endpoint, batch)?;
        }
        Ok(())
    }

    fn enqueue(&self, endpoint: Endpoint, batch: Batch) -> Result<()> {
        // Reservations bound queued bytes, so enqueueing never needs to wait for capacity.
        self.queue
            .send((endpoint, batch))
            .map_err(|_| self.send_error())
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

    // todo: reassignment of blocks from failed endpoints to other endpoints
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
    rpc_client: Arc<WriterRpcClient>,
    receiver: Arc<Mutex<mpsc::UnboundedReceiver<(Endpoint, Batch)>>>,
    failure: watch::Sender<Option<String>>,
) {
    loop {
        let next = {
            let mut receiver = receiver.lock().await;
            receiver.recv().await
        };
        let Some((endpoint, batch)) = next else {
            return;
        };
        // Drain queued reservations after failure without cancelling in-flight sends.
        if failure.borrow().is_some() {
            continue;
        }
        let (blocks, reservations): (Vec<_>, Vec<_>) = batch
            .blocks
            .into_iter()
            .map(|block| ((block.partition_id, block.block), block.memory))
            .unzip();
        let result = rpc_client.send_batch(endpoint, blocks).await;
        // Account for the payload until the entire batch succeeds or exhausts its retries.
        drop(reservations);
        if let Err(error) = result {
            record_failure(&failure, error);
        }
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
    blocks: Vec<BufferedBlock>,
    bytes: usize,
}

impl Batch {
    fn push(&mut self, block: BufferedBlock) {
        debug_assert!(!self.is_full());
        self.bytes += block.block.data.len() + BLOCK_OVERHEAD_BYTES;
        self.blocks.push(block);
    }

    fn is_full(&self) -> bool {
        self.bytes >= BATCH_SIZE_BYTES || self.blocks.len() >= MAX_BATCH_BLOCKS
    }
}
