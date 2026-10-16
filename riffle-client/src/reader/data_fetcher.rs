use std::collections::{BTreeMap, HashSet};

use anyhow::{Context, Result};
use tokio::sync::mpsc::Sender;

use super::buffer::ReadBatch;
use super::hybrid_read_handler::HybridReadHandler;
use super::localfile_read_handler::LocalfileReadHandler;
use super::memory_read_handler::MemoryReadHandler;
use super::read_handler::{PartitionReadContext, ReadHandler};
use crate::rpc_client::{ServerConnOptions, ServerCoonPool};
use crate::{PartitionId, ShuffleHandle};

pub(super) struct DataFetcher {
    shuffle: ShuffleHandle,
    partitions: BTreeMap<PartitionId, HashSet<i64>>,
    sender: Sender<ReadBatch>,
    clients: ServerCoonPool,
}

impl DataFetcher {
    pub(super) fn new(
        shuffle: ShuffleHandle,
        partitions: BTreeMap<PartitionId, HashSet<i64>>,
        sender: Sender<ReadBatch>,
    ) -> Self {
        Self {
            shuffle,
            partitions,
            sender,
            clients: ServerCoonPool::new(),
        }
    }

    pub(super) async fn run(self) -> Result<()> {
        for (partition, expected_block_ids) in self.partitions {
            let endpoint = self.shuffle.assignment.get_endpoint(partition)?;
            let client = self
                .clients
                .get_or_create_client(ServerConnOptions::new(endpoint.clone()))?;
            let context = PartitionReadContext {
                shuffle: &self.shuffle,
                partition,
                endpoint: endpoint.clone(),
                client,
            };
            let handlers: Vec<Box<dyn ReadHandler + '_>> = vec![
                Box::new(MemoryReadHandler::new(context.clone(), &expected_block_ids)),
                Box::new(LocalfileReadHandler::new(
                    context.clone(),
                    expected_block_ids.clone(),
                )),
            ];
            let mut handler = HybridReadHandler::new(context, expected_block_ids, handlers);
            loop {
                let permit = self
                    .sender
                    .reserve()
                    .await
                    .context("shuffle reader closed")?;
                let batch = handler.read().await.with_context(|| {
                    format!(
                        "shuffle read failed at {}, app={}, shuffle={}, partition={partition}",
                        endpoint.uri(),
                        self.shuffle.app_id,
                        self.shuffle.shuffle_id
                    )
                })?;
                match batch {
                    Some(batch) => permit.send(batch),
                    None => break,
                }
            }
        }
        Ok(())
    }
}
