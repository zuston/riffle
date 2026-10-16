mod buffer;
mod data_fetcher;
mod hybrid_read_handler;
mod localfile_read_handler;
mod memory_read_handler;
mod read_handler;

use std::collections::{BTreeMap, HashSet};

use anyhow::{Context, Result};
use futures::StreamExt;
use riffle_server::id_layout::DEFAULT_BLOCK_ID_LAYOUT;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use self::buffer::ReadBatch;
use self::data_fetcher::DataFetcher;
use crate::{PartitionId, ReadContext, ShuffleStream};

pub struct ShuffleReader {
    fetcher: DataFetcher,
    batches: mpsc::Receiver<ReadBatch>,
}

impl ShuffleReader {
    pub async fn new(context: ReadContext) -> Result<Self> {
        let mut partitions_to_block_ids: BTreeMap<PartitionId, HashSet<i64>> = BTreeMap::new();
        for partition in context.partitions {
            partitions_to_block_ids.entry(partition).or_default();
        }
        for result in context.write_results {
            for block_id in result.block_ids {
                let partition = DEFAULT_BLOCK_ID_LAYOUT.get_partition_id(block_id);
                if let Some(expected) = partitions_to_block_ids.get_mut(&(partition as PartitionId))
                {
                    expected.insert(block_id);
                }
            }
        }
        partitions_to_block_ids.retain(|_, expected| !expected.is_empty());
        // Reserve the queued slot before fetching, leaving one batch for the consumer.
        let (sender, batches) = mpsc::channel(1);
        Ok(Self {
            fetcher: DataFetcher::new(context.shuffle, partitions_to_block_ids, sender),
            batches,
        })
    }

    /// Streams records without an ordering guarantee. Dropping the stream cancels prefetch.
    pub fn stream(self) -> ShuffleStream {
        Box::pin(
            futures::stream::once(async move {
                let mut workers = JoinSet::new();
                workers.spawn(self.fetcher.run());
                futures::stream::try_unfold(
                    (self.batches, workers, None::<ReadBatch>),
                    |(mut batches, mut workers, mut current)| async move {
                        loop {
                            if let Some(batch) = &mut current {
                                if let Some(record) = batch.next_record()? {
                                    return Ok(Some((record, (batches, workers, current))));
                                }
                            }
                            match batches.recv().await {
                                Some(batch) => current = Some(batch),
                                None => {
                                    while let Some(result) = workers.join_next().await {
                                        result.context("shuffle fetch worker failed")??;
                                    }
                                    return Ok(None);
                                }
                            }
                        }
                    },
                )
            })
            .flatten(),
        )
    }
}
