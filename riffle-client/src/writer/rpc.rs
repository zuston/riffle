use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use futures::{stream, StreamExt, TryStreamExt};
use prost::Message;
use riffle_server::grpc::protobuf::uniffle::{
    PartitionToBlockIds, ReportShuffleResultRequest, RequireBufferRequest, SendShuffleDataRequest,
    ShuffleBlock, ShuffleData,
};
use riffle_server::id_layout::DEFAULT_BLOCK_ID_LAYOUT;
use riffle_server::util::now_timestamp_as_millis;

use super::{BufferedBlock, WriterContext, MAX_IN_FLIGHT_REQUESTS};
use crate::rpc_client::transport::{call, ensure_success, request, retry};
use crate::rpc_client::{ServerClient, ServerConnOptions};
use crate::{Endpoint, PartitionId, ShuffleAssignment, MAX_MESSAGE_BYTES};

const REPORT_BLOCKS_PER_REQUEST: usize = 16 * 1024;

pub(super) async fn send_batch(
    context: Arc<WriterContext>,
    endpoint: Endpoint,
    blocks: Vec<BufferedBlock>,
) -> Result<()> {
    let mut partitions: BTreeMap<i32, Vec<ShuffleBlock>> = BTreeMap::new();
    let mut reservations = Vec::with_capacity(blocks.len());
    let mut payload_bytes = 0;
    for block in blocks {
        payload_bytes += block.block.data.len();
        partitions
            .entry(block.partition_id as i32)
            .or_default()
            .push(block.block);
        reservations.push(block.memory);
    }
    let buffer_request = RequireBufferRequest {
        require_size: i32::try_from(payload_bytes.max(1))
            .context("batch exceeds buffer ticket size")?,
        app_id: context.app_id.clone(),
        shuffle_id: context.shuffle_id as i32,
        partition_ids: partitions.keys().copied().collect(),
    };
    let data_request = SendShuffleDataRequest {
        app_id: context.app_id.clone(),
        shuffle_id: context.shuffle_id as i32,
        // Size the request using the longest possible varints before reserving remote memory.
        require_buffer_id: -1,
        timestamp: -1,
        shuffle_data: partitions
            .into_iter()
            .map(|(partition_id, block)| ShuffleData {
                partition_id,
                block,
            })
            .collect(),
        ..Default::default()
    };
    ensure!(
        data_request.encoded_len() <= MAX_MESSAGE_BYTES,
        "batch exceeds RPC message limit"
    );
    let client = context
        .conn
        .get_or_create_client(ServerConnOptions::new(endpoint.clone()))?;
    let result = retry(&endpoint, "sendShuffleData", || {
        let mut client = client.clone();
        let buffer_request = buffer_request.clone();
        let mut data_request = data_request.clone();
        async move {
            // A previous attempt may have consumed its ticket and inserted only part of the batch.
            // Always reserve a new ticket while preserving the original block IDs and bytes.
            let buffer = call(client.require_buffer(request(buffer_request))).await?;
            ensure_success("requireBuffer", buffer.status, &buffer.ret_msg)?;
            data_request.require_buffer_id = buffer.require_buffer_id;
            data_request.timestamp = now_timestamp_as_millis() as i64;
            let response = call(client.send_shuffle_data(request(data_request))).await?;
            ensure_success("sendShuffleData", response.status, &response.ret_msg)
        }
    })
    .await
    .with_context(|| {
        format!(
            "shuffle write failed, app={}, shuffle={}, task={}",
            context.app_id, context.shuffle_id, context.task_id
        )
    });
    // Account for the payload until the entire batch succeeds or exhausts its retries.
    drop(reservations);
    result
}

pub(super) async fn report_results(
    context: &WriterContext,
    assignment: &ShuffleAssignment,
    block_ids: &[i64],
) -> Result<()> {
    let mut servers: HashMap<Endpoint, BTreeMap<PartitionId, Vec<i64>>> = HashMap::new();
    for &block_id in block_ids {
        let partition = DEFAULT_BLOCK_ID_LAYOUT.get_partition_id(block_id) as PartitionId;
        servers
            .entry(assignment.get_endpoint(partition)?)
            .or_default()
            .entry(partition)
            .or_default()
            .push(block_id);
    }
    stream::iter(servers)
        .map(|(endpoint, partitions)| report_server(context, endpoint, partitions))
        .buffer_unordered(MAX_IN_FLIGHT_REQUESTS)
        .try_for_each(|()| async { Ok(()) })
        .await
        .with_context(|| {
            format!(
                "shuffle result report failed, app={}, shuffle={}, task={}",
                context.app_id, context.shuffle_id, context.task_id
            )
        })
}

async fn report_server(
    context: &WriterContext,
    endpoint: Endpoint,
    partitions: BTreeMap<PartitionId, Vec<i64>>,
) -> Result<()> {
    let client = context
        .conn
        .get_or_create_client(ServerConnOptions::new(endpoint.clone()))?;
    let mut report = ReportShuffleResultRequest {
        app_id: context.app_id.clone(),
        shuffle_id: context.shuffle_id as i32,
        task_attempt_id: context.task_id as i64,
        bitmap_num: 1,
        partition_to_block_ids: Vec::new(),
        partition_stats: Vec::new(),
    };
    let mut report_count = 0;
    for (partition, block_ids) in partitions {
        let mut remaining = block_ids.as_slice();
        while !remaining.is_empty() {
            let count = remaining
                .len()
                .min(REPORT_BLOCKS_PER_REQUEST - report_count);
            let (chunk, rest) = remaining.split_at(count);
            report.partition_to_block_ids.push(PartitionToBlockIds {
                partition_id: partition as i32,
                block_ids: chunk.to_vec(),
            });
            report_count += count;
            remaining = rest;
            if report_count == REPORT_BLOCKS_PER_REQUEST {
                report_chunk(&endpoint, &client, &report).await?;
                report.partition_to_block_ids.clear();
                report_count = 0;
            }
        }
    }
    if report_count > 0 {
        report_chunk(&endpoint, &client, &report).await?;
    }
    Ok(())
}

async fn report_chunk(
    endpoint: &Endpoint,
    client: &ServerClient,
    report: &ReportShuffleResultRequest,
) -> Result<()> {
    ensure!(
        report.encoded_len() <= MAX_MESSAGE_BYTES,
        "result report exceeds RPC message limit"
    );
    retry(endpoint, "reportShuffleResult", || {
        let mut client = client.clone();
        let report = report.clone();
        async move {
            let response = call(client.report_shuffle_result(request(report))).await?;
            ensure_success("reportShuffleResult", response.status, &response.ret_msg)
        }
    })
    .await
}
