mod buffer;
mod data_fetcher;
mod hybrid_read_handler;
mod localfile_read_handler;
mod memory_read_handler;
mod multi_servers_read_handler;
mod read_handler;

use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{ensure, Context, Result};
use croaring::{JvmLegacy, Treemap};
use futures::StreamExt;
use riffle_server::client_configs::ClientRssConf;
use riffle_server::grpc::protobuf::uniffle::{BlockIdLayout, GetShuffleResultForMultiPartRequest};
use riffle_server::id_layout::DEFAULT_BLOCK_ID_LAYOUT;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use self::buffer::ReadBatch;
use self::data_fetcher::DataFetcher;
use self::multi_servers_read_handler::ServerBlockIds;
use crate::configs::COMPRESSION_CODEC;
use crate::rpc_client::manager_rpc_client::ManagerRpcClient;
use crate::rpc_client::{
    call, ensure_attempt_success, request, retry, ServerConnOptions, ServerCoonPool,
};
use crate::{
    ApplicationId, Endpoint, PartitionId, ReadTaskInfo, ShuffleStream, TaskId, TASK_ID_LIMIT,
};

pub struct ShuffleReader {
    fetcher: DataFetcher,
    batches: mpsc::Receiver<ReadBatch>,
    expected_num_records: HashMap<PartitionId, u64>,
}

impl ShuffleReader {
    pub async fn new(
        application_id: ApplicationId,
        manager_endpoint: Endpoint,
        read_task_info: ReadTaskInfo,
    ) -> Result<Self> {
        let manager = ManagerRpcClient::new(application_id, manager_endpoint)?;
        let (shuffle, expected_task_ids) = manager.get_read_shuffle_handle(&read_task_info).await?;
        let compression_codec = ClientRssConf::from(shuffle.configs.clone())
            .get(&COMPRESSION_CODEC)
            .context("invalid riffle.client.compression.codec; expected lz4 or zstd")?;
        let expected_task_ids: HashSet<_> = expected_task_ids.into_iter().collect();
        let mut partitions_to_block_ids: BTreeMap<PartitionId, ServerBlockIds> = read_task_info
            .partitions
            .into_iter()
            .map(|partition| (partition, HashMap::new()))
            .collect();
        let mut servers: HashMap<Endpoint, Vec<i32>> = HashMap::new();
        for (&partition, block_ids) in &mut partitions_to_block_ids {
            let endpoints = shuffle
                .assignment
                .assignments
                .get(&partition)
                .with_context(|| format!("partition {partition} has no assignment"))?;
            ensure!(
                !endpoints.is_empty(),
                "partition {partition} has no assigned servers"
            );
            for endpoint in endpoints.iter() {
                if block_ids.insert(endpoint.clone(), HashSet::new()).is_none() {
                    servers
                        .entry(endpoint.clone())
                        .or_default()
                        .push(partition as i32);
                }
            }
        }

        let mut task_record_counts = HashMap::new();
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
                    ensure_attempt_success(
                        "getShuffleResultForMultiPart",
                        response.status,
                        &response.ret_msg,
                    )?;
                    Ok(response)
                }
            })
            .await?;
            for stats in response.partition_stats {
                let partition = PartitionId::try_from(stats.partition_id)
                    .context("negative statistics partition ID")?;
                ensure!(
                    partitions_to_block_ids
                        .get(&partition)
                        .is_some_and(|servers| servers.contains_key(&endpoint)),
                    "unexpected statistics for partition {partition} from {}",
                    endpoint.uri()
                );
                for task in stats.task_attempt_id_to_records {
                    let task_id = TaskId::try_from(task.task_attempt_id)
                        .context("negative statistics task ID")?;
                    if !expected_task_ids.contains(&task_id) {
                        continue;
                    }
                    let count =
                        u64::try_from(task.record_number).context("negative record count")?;
                    // Historical servers hold the same full task count after reassignment.
                    if let Some(previous) = task_record_counts.insert((partition, task_id), count) {
                        ensure!(
                            previous == count,
                            "conflicting record counts for partition {partition}, task {task_id}"
                        );
                    }
                }
            }
            collect_block_ids(
                &response.serialized_bitmap,
                &expected_task_ids,
                &endpoint,
                &mut partitions_to_block_ids,
            )
            .with_context(|| format!("invalid shuffle result from {}", endpoint.uri()))?;
        }
        let mut expected_num_records: HashMap<_, u64> = partitions_to_block_ids
            .keys()
            .map(|&partition| (partition, 0))
            .collect();
        for (&partition, servers) in &partitions_to_block_ids {
            for block_id in servers.values().flatten() {
                let task_id = (*block_id as u64 & (TASK_ID_LIMIT as u64 - 1)) as TaskId;
                ensure!(
                    task_record_counts.contains_key(&(partition, task_id)),
                    "missing record count for partition {partition}, task {task_id}"
                );
            }
        }
        for ((partition, _), count) in task_record_counts {
            let total = expected_num_records.get_mut(&partition).unwrap();
            *total = total
                .checked_add(count)
                .with_context(|| format!("record count overflow for partition {partition}"))?;
        }
        partitions_to_block_ids.retain(|_, servers| {
            servers.retain(|_, expected| !expected.is_empty());
            !servers.is_empty()
        });
        // Reserve the queued slot before fetching, leaving one batch for the consumer.
        let (sender, batches) = mpsc::channel(1);
        Ok(Self {
            fetcher: DataFetcher::new(
                shuffle,
                partitions_to_block_ids,
                sender,
                clients,
                compression_codec,
            ),
            batches,
            expected_num_records,
        })
    }

    /// Streams records without an ordering guarantee. Dropping the stream cancels prefetch.
    /// Exhausting the stream validates each partition's record count against upstream tasks.
    pub fn stream(self) -> ShuffleStream {
        Box::pin(
            futures::stream::once(async move {
                let mut workers = JoinSet::new();
                workers.spawn(self.fetcher.run());
                let counts: HashMap<_, _> = self
                    .expected_num_records
                    .into_iter()
                    .map(|(partition, expected)| (partition, (expected, 0_u64)))
                    .collect();
                futures::stream::try_unfold(
                    (self.batches, workers, None::<ReadBatch>, counts),
                    |(mut batches, mut workers, mut current, mut counts)| async move {
                        loop {
                            if let Some(batch) = &mut current {
                                if let Some(record) = batch.next_record().await? {
                                    let (_, actual) = counts.get_mut(&record.partition_id)
                                        .context("record belongs to an unrequested partition")?;
                                    *actual += 1;
                                    return Ok(Some((record, (batches, workers, current, counts))));
                                }
                            }
                            match batches.recv().await {
                                Some(batch) => current = Some(batch),
                                None => {
                                    while let Some(result) = workers.join_next().await {
                                        result.context("shuffle fetch worker failed")??;
                                    }
                                    for (partition, (expected, actual)) in counts {
                                        ensure!(
                                            actual == expected,
                                            "shuffle integrity validation failed for partition {partition}: expected {expected} records, read {actual}"
                                        );
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
    endpoint: &Endpoint,
    partitions: &mut BTreeMap<PartitionId, ServerBlockIds>,
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
        if let Some(expected) = partitions
            .get_mut(&partition_id)
            .and_then(|servers| servers.get_mut(endpoint))
        {
            expected.insert(block_id);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, Bytes, BytesMut};
    use futures::TryStreamExt;
    use riffle_server::ddashmap::DDashMap;
    use riffle_server::util::get_crc;

    use crate::{CompressionCodec, ShuffleAssignment, ShuffleHandle};

    #[tokio::test]
    async fn validates_record_counts_at_end_of_stream() -> Result<()> {
        for expected in [0, 1, 2, 3] {
            let (sender, batches) = mpsc::channel(1);
            let mut batch = ReadBatch::new(0, CompressionCodec::Lz4);
            let mut data = BytesMut::new();
            // Empty bytes are still a record, followed by one nonempty record.
            data.put_u32(0);
            data.put_u32(1);
            data.put_u8(42);
            let uncompressed_len = data.len();
            let compressed = Bytes::from(lz4_flex::block::compress(&data));
            batch.push(
                0,
                compressed.clone(),
                get_crc(&compressed),
                uncompressed_len,
            );
            sender.send(batch).await.unwrap();
            let reader = ShuffleReader {
                fetcher: DataFetcher::new(
                    ShuffleHandle {
                        app_id: "integrity-test".into(),
                        shuffle_id: 0,
                        partition_count: 2,
                        assignment: ShuffleAssignment::new(0, DDashMap::default()),
                        configs: HashMap::new(),
                    },
                    BTreeMap::new(),
                    sender,
                    ServerCoonPool::new(),
                    CompressionCodec::Lz4,
                ),
                batches,
                expected_num_records: HashMap::from([(0, expected), (1, 0)]),
            };
            let mut stream = reader.stream();
            assert!(stream.try_next().await?.unwrap().data.is_empty());
            assert_eq!(stream.try_next().await?.unwrap().data.as_ref(), &[42]);
            if expected == 2 {
                assert!(stream.try_next().await?.is_none());
            } else {
                let error = stream
                    .try_next()
                    .await
                    .err()
                    .context("expected count mismatch")?;
                assert_eq!(
                    error.to_string(),
                    format!("shuffle integrity validation failed for partition 0: expected {expected} records, read 2")
                );
                assert!(stream.try_next().await?.is_none());
            }
        }
        Ok(())
    }
}
