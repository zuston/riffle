use std::collections::{HashMap, HashSet};

use anyhow::{ensure, Context, Result};

use super::buffer::ReadBatch;
use super::hybrid_read_handler::HybridReadHandler;
use super::localfile_read_handler::LocalfileReadHandler;
use super::memory_read_handler::MemoryReadHandler;
use super::read_handler::{PartitionReadContext, ReadHandler};
use crate::rpc_client::{ServerConnOptions, ServerCoonPool};
use crate::{CompressionCodec, Endpoint, PartitionId, ShuffleHandle};

pub(super) type ServerBlockIds = HashMap<Endpoint, HashSet<i64>>;

pub(super) struct MultiServersReadHandler<'a> {
    shuffle: &'a ShuffleHandle,
    partition: PartitionId,
    clients: &'a ServerCoonPool,
    compression_codec: CompressionCodec,
    servers: std::vec::IntoIter<(Endpoint, HashSet<i64>)>,
    current_handler: Option<HybridReadHandler<'a>>,
    accepted_block_ids: HashSet<i64>,
}

impl<'a> MultiServersReadHandler<'a> {
    pub fn new(
        shuffle: &'a ShuffleHandle,
        partition: PartitionId,
        mut block_ids: ServerBlockIds,
        clients: &'a ServerCoonPool,
        compression_codec: CompressionCodec,
    ) -> Result<Self> {
        let endpoints = shuffle
            .assignment
            .assignments
            .get(&partition)
            .with_context(|| format!("partition {partition} has no assignment"))?;
        let servers: Vec<_> = endpoints
            .iter()
            .filter_map(|endpoint| {
                block_ids
                    .remove(endpoint)
                    .map(|blocks| (endpoint.clone(), blocks))
            })
            .collect();
        ensure!(
            block_ids.is_empty(),
            "partition {partition} has blocks on an unassigned server"
        );
        Ok(Self {
            shuffle,
            partition,
            clients,
            compression_codec,
            servers: servers.into_iter(),
            current_handler: None,
            accepted_block_ids: HashSet::new(),
        })
    }
}

#[tonic::async_trait]
impl ReadHandler for MultiServersReadHandler<'_> {
    async fn read(&mut self) -> Result<Option<ReadBatch>> {
        loop {
            if let Some(handler) = &mut self.current_handler {
                if let Some(batch) = handler.read().await? {
                    self.accepted_block_ids.extend(batch.block_ids());
                    return Ok(Some(batch));
                }
                self.current_handler = None;
            }
            let Some((endpoint, mut expected_block_ids)) = self.servers.next() else {
                return Ok(None);
            };
            // A retried block can be reported by more than one historical server.
            expected_block_ids.retain(|block_id| !self.accepted_block_ids.contains(block_id));
            if expected_block_ids.is_empty() {
                continue;
            }
            let client = self
                .clients
                .get_or_create_client(ServerConnOptions::new(endpoint.clone()))?;
            let context = PartitionReadContext {
                shuffle: self.shuffle,
                partition: self.partition,
                endpoint,
                client,
                compression_codec: self.compression_codec,
            };
            let handlers: Vec<Box<dyn ReadHandler + '_>> = vec![
                Box::new(MemoryReadHandler::new(context.clone(), &expected_block_ids)),
                Box::new(LocalfileReadHandler::new(
                    context.clone(),
                    expected_block_ids.clone(),
                )),
            ];
            self.current_handler = Some(HybridReadHandler::new(
                context,
                expected_block_ids,
                handlers,
            ));
        }
    }
}
