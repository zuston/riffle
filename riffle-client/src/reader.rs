mod buffer;
mod data_fetcher;
mod hybrid_read_handler;
mod localfile_read_handler;
mod memory_read_handler;
mod read_handler;

use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{Context, Result};
use croaring::{JvmLegacy, Treemap};
use futures::StreamExt;
use riffle_server::grpc::protobuf::uniffle::{BlockIdLayout, GetShuffleResultForMultiPartRequest};
use riffle_server::id_layout::DEFAULT_BLOCK_ID_LAYOUT;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use self::buffer::ReadBatch;
use self::data_fetcher::DataFetcher;
use crate::manager::manager_rpc_client::ManagerRpcClient;
use crate::rpc_client::transport::{call, ensure_success, request, retry};
use crate::rpc_client::{ServerConnOptions, ServerCoonPool};
use crate::{
    ApplicationId, Endpoint, PartitionId, ReadTaskInfo, ShuffleStream, TaskId, TASK_ID_LIMIT,
};

pub struct ShuffleReader {
    fetcher: DataFetcher,
    batches: mpsc::Receiver<ReadBatch>,
}

impl ShuffleReader {
    pub async fn new(
        application_id: ApplicationId,
        manager_endpoint: Endpoint,
        read_task_info: ReadTaskInfo,
    ) -> Result<Self> {
        let manager = ManagerRpcClient::new(application_id, manager_endpoint)?;
        let (shuffle, expected_task_ids) = manager.get_read_shuffle_handle(&read_task_info).await?;
        let expected_task_ids: HashSet<_> = expected_task_ids.into_iter().collect();
        let mut partitions_to_block_ids: BTreeMap<PartitionId, HashSet<i64>> = read_task_info
            .partitions
            .into_iter()
            .map(|partition| (partition, HashSet::new()))
            .collect();
        let mut servers: HashMap<Endpoint, Vec<i32>> = HashMap::new();
        for &partition in partitions_to_block_ids.keys() {
            servers
                .entry(shuffle.assignment.get_endpoint(partition)?)
                .or_default()
                .push(partition as i32);
        }

        let clients = ServerCoonPool::new();
        for (endpoint, partitions) in servers {
            let client = clients.get_or_create_client(ServerConnOptions::new(endpoint.clone()))?;
            let message = GetShuffleResultForMultiPartRequest {
                app_id: shuffle.app_id.clone(),
                shuffle_id: shuffle.shuffle_id as i32,
                partitions,
                block_id_layout: Some(BlockIdLayout {
                    sequence_no_bits: 16,
                    partition_id_bits: 24,
                    task_attempt_id_bits: 23,
                }),
            };
            let response = retry(&endpoint, "getShuffleResultForMultiPart", || {
                let mut client = client.clone();
                let message = message.clone();
                async move {
                    let response =
                        call(client.get_shuffle_result_for_multi_part(request(message))).await?;
                    ensure_success(
                        "getShuffleResultForMultiPart",
                        response.status,
                        &response.ret_msg,
                    )?;
                    Ok(response)
                }
            })
            .await?;
            collect_block_ids(
                &response.serialized_bitmap,
                &expected_task_ids,
                &mut partitions_to_block_ids,
            )
            .with_context(|| format!("invalid shuffle result from {}", endpoint.uri()))?;
        }
        partitions_to_block_ids.retain(|_, expected| !expected.is_empty());
        // Reserve the queued slot before fetching, leaving one batch for the consumer.
        let (sender, batches) = mpsc::channel(1);
        Ok(Self {
            fetcher: DataFetcher::new(shuffle, partitions_to_block_ids, sender, clients),
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

fn collect_block_ids(
    serialized_bitmap: &[u8],
    expected_task_ids: &HashSet<TaskId>,
    partitions: &mut BTreeMap<PartitionId, HashSet<i64>>,
) -> Result<()> {
    // The server returns empty bytes for a partition with no reported blocks.
    if serialized_bitmap.is_empty() {
        return Ok(());
    }
    let block_ids = Treemap::try_deserialize::<JvmLegacy>(serialized_bitmap)
        .context("invalid block ID bitmap")?;
    for block_id in block_ids.iter() {
        let task_id = (block_id & (TASK_ID_LIMIT as u64 - 1)) as TaskId;
        if !expected_task_ids.contains(&task_id) {
            continue;
        }
        let block_id = block_id as i64;
        let partition_id = DEFAULT_BLOCK_ID_LAYOUT.get_partition_id(block_id) as PartitionId;
        if let Some(expected) = partitions.get_mut(&partition_id) {
            expected.insert(block_id);
        }
    }
    Ok(())
}
