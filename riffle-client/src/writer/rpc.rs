use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, ensure, Context, Result};
use futures::{stream, StreamExt, TryStreamExt};
use log::warn;
use prost::Message;
use riffle_server::grpc::protobuf::uniffle::{
    PartitionToBlockIds, ReportShuffleResultRequest, RequireBufferRequest, SendShuffleDataRequest,
    ShuffleBlock, ShuffleData, StatusCode,
};
use riffle_server::id_layout::DEFAULT_BLOCK_ID_LAYOUT;
use riffle_server::util::now_timestamp_as_millis;
use tonic::{Code, Request, Response, Status};

use super::{BufferedBlock, WriterContext, WriterOptions};
use crate::rpc_client::ShuffleServerConnOptions;
use crate::{Endpoint, PartitionId, MAX_MESSAGE_BYTES};

const REPORT_BLOCKS_PER_REQUEST: u32 = 16 * 1024;

pub(super) async fn send_batch(
    context: Arc<WriterContext>,
    endpoint: Endpoint,
    blocks: Vec<BufferedBlock>,
) -> Result<()> {
    let mut partitions: BTreeMap<i32, Vec<ShuffleBlock>> = BTreeMap::new();
    let mut permits = Vec::with_capacity(blocks.len());
    let mut payload_bytes = 0;
    for block in blocks {
        payload_bytes += block.block.data.len();
        partitions
            .entry(block.partition_id as i32)
            .or_default()
            .push(block.block);
        permits.push(block.memory);
    }
    let buffer_request = RequireBufferRequest {
        require_size: i32::try_from(payload_bytes.max(1))
            .context("batch exceeds buffer ticket size")?,
        app_id: context.app_id.clone(),
        shuffle_id: context.protocol_shuffle_id,
        partition_ids: partitions.keys().copied().collect(),
    };
    let data_request = SendShuffleDataRequest {
        app_id: context.app_id.clone(),
        shuffle_id: context.protocol_shuffle_id,
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
        .clients
        .get_or_create_client(ShuffleServerConnOptions {
            endpoint: endpoint.clone(),
            timeout: context.options.rpc_timeout,
        })?;
    let result = retry(&context, &endpoint, "sendShuffleData", || {
        let mut client = client.clone();
        let buffer_request = buffer_request.clone();
        let mut data_request = data_request.clone();
        let options = &context.options;
        async move {
            // A previous attempt may have consumed its ticket and inserted only part of the batch.
            // Always reserve a new ticket while preserving the original block IDs and bytes.
            let buffer = call(
                client.require_buffer(request(buffer_request, options)),
                options,
            )
            .await?;
            check_status("requireBuffer", buffer.status, &buffer.ret_msg)?;
            data_request.require_buffer_id = buffer.require_buffer_id;
            data_request.timestamp = now_timestamp_as_millis() as i64;
            let response = call(
                client.send_shuffle_data(request(data_request, options)),
                options,
            )
            .await?;
            check_status("sendShuffleData", response.status, &response.ret_msg)
        }
    })
    .await;
    // Account for the payload until the entire batch succeeds or exhausts its retries.
    drop(permits);
    result
}

pub(super) async fn report_results(
    context: &WriterContext,
    routes: &HashMap<PartitionId, Endpoint>,
    blocks_number: &BTreeMap<PartitionId, u32>,
) -> Result<()> {
    let mut servers: HashMap<Endpoint, Vec<(PartitionId, u32)>> = HashMap::new();
    for (&partition, &count) in blocks_number {
        servers
            .entry(routes[&partition].clone())
            .or_default()
            .push((partition, count));
    }
    stream::iter(servers)
        .map(|(endpoint, partitions)| report_server(context, endpoint, partitions))
        .buffer_unordered(context.options.max_in_flight_requests)
        .try_for_each(|()| async { Ok(()) })
        .await
}

async fn report_server(
    context: &WriterContext,
    endpoint: Endpoint,
    partitions: Vec<(PartitionId, u32)>,
) -> Result<()> {
    let client = context
        .clients
        .get_or_create_client(ShuffleServerConnOptions {
            endpoint: endpoint.clone(),
            timeout: context.options.rpc_timeout,
        })?;
    let mut report = ReportShuffleResultRequest {
        app_id: context.app_id.clone(),
        shuffle_id: context.protocol_shuffle_id,
        task_attempt_id: context.task_id as i64,
        bitmap_num: 1,
        partition_to_block_ids: Vec::new(),
        partition_stats: Vec::new(),
    };
    let mut report_count = 0;
    for (partition, count) in partitions {
        let mut sequence = 0;
        while sequence < count {
            let end = (sequence + REPORT_BLOCKS_PER_REQUEST - report_count).min(count);
            report.partition_to_block_ids.push(PartitionToBlockIds {
                partition_id: partition as i32,
                block_ids: (sequence..end)
                    .map(|sequence| {
                        DEFAULT_BLOCK_ID_LAYOUT.get_block_id(
                            sequence.into(),
                            partition as i64,
                            context.task_id as i64,
                        )
                    })
                    .collect(),
            });
            report_count += end - sequence;
            sequence = end;
            if report_count == REPORT_BLOCKS_PER_REQUEST {
                report_chunk(context, &endpoint, &client, &report).await?;
                report.partition_to_block_ids.clear();
                report_count = 0;
            }
        }
    }
    if report_count > 0 {
        report_chunk(context, &endpoint, &client, &report).await?;
    }
    Ok(())
}

async fn report_chunk(
    context: &WriterContext,
    endpoint: &Endpoint,
    client: &crate::rpc_client::ServerClient,
    report: &ReportShuffleResultRequest,
) -> Result<()> {
    ensure!(
        report.encoded_len() <= MAX_MESSAGE_BYTES,
        "result report exceeds RPC message limit"
    );
    retry(context, endpoint, "reportShuffleResult", || {
        let mut client = client.clone();
        let report = report.clone();
        async move {
            let response = call(
                client.report_shuffle_result(request(report, &context.options)),
                &context.options,
            )
            .await?;
            check_status("reportShuffleResult", response.status, &response.ret_msg)
        }
    })
    .await
}

struct AttemptError {
    error: anyhow::Error,
    retryable: bool,
}

fn check_status(operation: &str, status: i32, message: &str) -> Result<(), AttemptError> {
    if status == StatusCode::Success as i32 {
        return Ok(());
    }
    Err(AttemptError {
        error: anyhow!("{operation} failed with status {status}: {message}"),
        retryable: matches!(
            StatusCode::try_from(status),
            Ok(StatusCode::NoBuffer
                | StatusCode::NoBufferForHugePartition
                | StatusCode::InternalError
                | StatusCode::Timeout)
        ),
    })
}

fn request<T>(message: T, options: &WriterOptions) -> Request<T> {
    let mut request = Request::new(message);
    request.set_timeout(options.rpc_timeout);
    request
}

async fn call<T>(
    future: impl Future<Output = Result<Response<T>, Status>>,
    options: &WriterOptions,
) -> Result<T, AttemptError> {
    match tokio::time::timeout(options.rpc_timeout, future).await {
        Ok(Ok(response)) => Ok(response.into_inner()),
        Ok(Err(status)) => Err(AttemptError {
            retryable: matches!(
                status.code(),
                Code::Unavailable
                    | Code::DeadlineExceeded
                    | Code::ResourceExhausted
                    | Code::Aborted
                    | Code::Internal
                    | Code::Unknown
            ),
            error: status.into(),
        }),
        Err(error) => Err(AttemptError {
            error: error.into(),
            retryable: true,
        }),
    }
}

async fn retry<T, F, Fut>(
    context: &WriterContext,
    endpoint: &Endpoint,
    operation: &str,
    mut attempt: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, AttemptError>>,
{
    for number in 1..=context.options.max_attempts {
        let error = match attempt().await {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        if !error.retryable || number == context.options.max_attempts {
            return Err(error.error.context(format!(
                "{operation} at {} failed after {number} attempt(s), app={}, shuffle={}, task={}",
                endpoint.uri(),
                context.app_id,
                context.shuffle_id,
                context.task_id,
            )));
        }
        let ceiling_ms = (100u64 << (number - 1).min(5)).min(2000);
        let delay_ms = ceiling_ms / 2 + rand::random::<u64>() % (ceiling_ms / 2 + 1);
        warn!(
            "Retrying {operation} at {} after attempt {number} in {delay_ms}ms: {:#}",
            endpoint.uri(),
            error.error
        );
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
    unreachable!("writer options require at least one attempt")
}
