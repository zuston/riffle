use std::collections::BTreeMap;

use anyhow::{Context, Result};
use tokio::sync::mpsc::Sender;

use super::buffer::ReadBatch;
use super::multi_servers_read_handler::{MultiServersReadHandler, ServerBlockIds};
use super::read_handler::ReadHandler;
use crate::rpc_client::ServerCoonPool;
use crate::{CompressionCodec, PartitionId, ShuffleHandle};

pub(super) struct DataFetcher {
    shuffle: ShuffleHandle,
    partitions: BTreeMap<PartitionId, ServerBlockIds>,
    sender: Sender<ReadBatch>,
    clients: ServerCoonPool,
    compression_codec: CompressionCodec,
}

impl DataFetcher {
    pub(super) fn new(
        shuffle: ShuffleHandle,
        partitions: BTreeMap<PartitionId, ServerBlockIds>,
        sender: Sender<ReadBatch>,
        clients: ServerCoonPool,
        compression_codec: CompressionCodec,
    ) -> Self {
        Self {
            shuffle,
            partitions,
            sender,
            clients,
            compression_codec,
        }
    }

    pub(super) async fn run(self) -> Result<()> {
        for (partition, server_block_ids) in self.partitions {
            let mut handler = MultiServersReadHandler::new(
                &self.shuffle,
                partition,
                server_block_ids,
                &self.clients,
                self.compression_codec,
            )?;
            loop {
                let permit = self
                    .sender
                    .reserve()
                    .await
                    .context("shuffle reader closed")?;
                let batch = handler.read().await.with_context(|| {
                    format!(
                        "shuffle read failed, app={}, shuffle={}, partition={partition}",
                        self.shuffle.app_id, self.shuffle.shuffle_id
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
