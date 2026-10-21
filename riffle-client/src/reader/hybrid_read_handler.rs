use std::collections::HashSet;

use anyhow::{bail, Result};
use log::info;

use super::buffer::ReadBatch;
use super::read_handler::{PartitionReadContext, ReadHandler};

pub(super) struct HybridReadHandler<'a> {
    context: PartitionReadContext<'a>,
    expected_block_ids: HashSet<i64>,
    accepted_block_ids: HashSet<i64>,
    handlers: Vec<Box<dyn ReadHandler + 'a>>,
    current_handler_index: usize,
}

impl<'a> HybridReadHandler<'a> {
    pub fn new(
        context: PartitionReadContext<'a>,
        expected_block_ids: HashSet<i64>,
        handlers: Vec<Box<dyn ReadHandler + 'a>>,
    ) -> Self {
        Self {
            context,
            expected_block_ids,
            accepted_block_ids: HashSet::new(),
            handlers,
            current_handler_index: 0,
        }
    }

    fn finish_current_handler(&mut self) {
        info!(
            "Finished shuffle read handler at {}, app={}, shuffle={}, partition={}, handler_index={}, accepted_blocks={}, expected_blocks={}",
            self.context.endpoint.uri(),
            self.context.shuffle.app_id,
            self.context.shuffle.shuffle_id,
            self.context.partition,
            self.current_handler_index,
            self.accepted_block_ids.len(),
            self.expected_block_ids.len()
        );
        self.current_handler_index += 1;
    }

    async fn read_next(&mut self) -> Result<ReadBatch> {
        loop {
            if let Some(handler) = self.handlers.get_mut(self.current_handler_index) {
                if let Some(batch) = handler.read().await? {
                    let batch =
                        batch.filter(&self.expected_block_ids, &mut self.accepted_block_ids);
                    if !batch.is_empty() {
                        if self.accepted_block_ids == self.expected_block_ids {
                            self.finish_current_handler();
                        }
                        return Ok(batch);
                    }
                    continue;
                }
                self.finish_current_handler();
                continue;
            }
            bail!(
                "shuffle partition {} is missing {} blocks after reading all storage layers",
                self.context.partition,
                self.expected_block_ids.len() - self.accepted_block_ids.len()
            );
        }
    }
}

#[tonic::async_trait]
impl ReadHandler for HybridReadHandler<'_> {
    async fn read(&mut self) -> Result<Option<ReadBatch>> {
        if self.accepted_block_ids == self.expected_block_ids {
            return Ok(None);
        }
        self.read_next().await.map(Some)
    }
}
