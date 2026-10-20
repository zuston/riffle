use std::collections::{HashMap, HashSet};

use anyhow::{ensure, Context, Result};
use riffle_server::ddashmap::DDashMap;
use tonic::transport::Channel;

use crate::rpc::get_write_shuffle_handle_request::ShuffleIdentifier;
use crate::rpc::shuffle_manager_service_client::ShuffleManagerServiceClient;
use crate::rpc::{
    GetReadShuffleHandleRequest, GetWriteShuffleHandleRequest, JobIdAndStageId,
    PartitionAssignment, ReassignPartition, ReassignPartitionsRequest, ReportShuffleResultRequest,
};
use crate::rpc_client::{call, request, retry};
use crate::{
    ApplicationId, Endpoint, PartitionId, ReadTaskInfo, ShuffleAssignment, ShuffleHandle,
    ShuffleId, TaskId, WriteTaskInfo, MAX_MESSAGE_BYTES, RPC_TIMEOUT,
};

#[derive(Clone)]
pub struct ManagerRpcClient {
    app_id: ApplicationId,
    endpoint: Endpoint,
    client: ShuffleManagerServiceClient<Channel>,
}

impl ManagerRpcClient {
    pub fn new(app_id: ApplicationId, endpoint: Endpoint) -> Result<Self> {
        let channel = tonic::transport::Endpoint::from_shared(endpoint.uri())?
            .connect_timeout(RPC_TIMEOUT)
            .timeout(RPC_TIMEOUT)
            .connect_lazy();
        Ok(Self {
            app_id,
            endpoint,
            client: ShuffleManagerServiceClient::new(channel)
                .max_encoding_message_size(MAX_MESSAGE_BYTES)
                .max_decoding_message_size(MAX_MESSAGE_BYTES),
        })
    }

    pub async fn get_write_shuffle_handle(
        &self,
        task: &WriteTaskInfo,
    ) -> Result<(ShuffleAssignment, TaskId, HashMap<String, String>)> {
        let message = GetWriteShuffleHandleRequest {
            app_id: self.app_id.clone(),
            shuffle_identifier: Some(ShuffleIdentifier::JobIdAndStageId(JobIdAndStageId {
                job_id: task.job_id,
                stage_id: task.stage_id,
            })),
            map_id: task.map_id as u64,
            attempt_id: task.attempt_id as u64,
            num_partitions: u32::try_from(task.num_partitions)
                .context("partition count exceeds u32 range")?,
        };
        let response = retry(&self.endpoint, "getWriteShuffleHandle", || {
            let mut client = self.client.clone();
            let message = message.clone();
            async move { call(client.get_write_shuffle_handle(request(message))).await }
        })
        .await?;
        let shuffle_id = response.shuffle_id as usize;
        let task_id = response.task_id as usize;
        let assignments = DDashMap::with_capacity(response.assignments.len());
        for (partition, endpoints) in parse_assignments(response.assignments)? {
            assignments.insert(partition, endpoints);
        }
        Ok((
            ShuffleAssignment::new(shuffle_id, assignments),
            task_id,
            response.configs,
        ))
    }

    pub async fn reassign_partitions(
        &self,
        shuffle_id: ShuffleId,
        partitions: &[PartitionId],
        failed_endpoint: &Endpoint,
        failure_status_code: i32,
    ) -> Result<HashMap<PartitionId, Vec<Endpoint>>> {
        let message = ReassignPartitionsRequest {
            app_id: self.app_id.clone(),
            shuffle_id: shuffle_id as u64,
            reassign_partitions: partitions
                .iter()
                .map(|&partition_id| ReassignPartition {
                    partition_id: partition_id as u64,
                    endpoint: Some(crate::rpc::Endpoint {
                        host: failed_endpoint.host.clone(),
                        grpc_port: i32::from(failed_endpoint.grpc_port),
                        urpc_port: None,
                    }),
                    failure_status_code,
                })
                .collect(),
        };
        let response = retry(&self.endpoint, "reassignPartitions", || {
            let mut client = self.client.clone();
            let message = message.clone();
            async move { call(client.reassign_partitions(request(message))).await }
        })
        .await?;
        let assignments = parse_assignments(response.assignments)?;
        let expected: HashSet<_> = partitions.iter().copied().collect();
        ensure!(
            assignments.len() == expected.len()
                && assignments
                    .keys()
                    .all(|partition| expected.contains(partition)),
            "reassignment does not cover exactly the requested partitions"
        );
        Ok(assignments)
    }

    pub async fn report_shuffle_result(
        &self,
        shuffle_id: ShuffleId,
        task_id: TaskId,
    ) -> Result<()> {
        let message = ReportShuffleResultRequest {
            app_id: self.app_id.clone(),
            shuffle_id: shuffle_id as u64,
            task_id: task_id as u64,
        };
        retry(&self.endpoint, "reportShuffleResult", || {
            let mut client = self.client.clone();
            let message = message.clone();
            async move { call(client.report_shuffle_result(request(message))).await }
        })
        .await?;
        Ok(())
    }

    pub async fn get_read_shuffle_handle(
        &self,
        task: &ReadTaskInfo,
    ) -> Result<(ShuffleHandle, Vec<TaskId>)> {
        use crate::rpc::get_read_shuffle_handle_request::ShuffleIdentifier;

        let message = GetReadShuffleHandleRequest {
            app_id: self.app_id.clone(),
            shuffle_identifier: Some(ShuffleIdentifier::JobIdAndStageId(JobIdAndStageId {
                job_id: task.job_id,
                stage_id: task.upstream_stage_id,
            })),
            partition_ids: task
                .partitions
                .iter()
                .copied()
                .collect::<HashSet<_>>()
                .into_iter()
                .map(|id| id as u64)
                .collect(),
        };
        let response = retry(&self.endpoint, "getReadShuffleHandle", || {
            let mut client = self.client.clone();
            let message = message.clone();
            async move { call(client.get_read_shuffle_handle(request(message))).await }
        })
        .await?;
        let shuffle_id = response.shuffle_id as usize;
        let task_ids = response
            .task_ids
            .into_iter()
            .map(|id| id as usize)
            .collect();
        let assignments = DDashMap::with_capacity(response.assignments.len());
        for (partition, endpoints) in parse_assignments(response.assignments)? {
            assignments.insert(partition, endpoints);
        }
        Ok((
            ShuffleHandle {
                app_id: self.app_id.clone(),
                shuffle_id,
                partition_count: response.partition_count,
                assignment: ShuffleAssignment::new(shuffle_id, assignments),
                configs: response.configs,
            },
            task_ids,
        ))
    }
}

fn parse_assignments(
    assignments: Vec<PartitionAssignment>,
) -> Result<HashMap<PartitionId, Vec<Endpoint>>> {
    let mut parsed = HashMap::with_capacity(assignments.len());
    for assignment in assignments {
        let partition = PartitionId::try_from(assignment.partition_id)
            .context("negative assignment partition ID")?;
        ensure!(
            partition < 1 << 24,
            "assignment partition ID exceeds block ID range"
        );
        ensure!(
            !assignment.endpoints.is_empty(),
            "assignment has no endpoints"
        );
        let endpoints = assignment
            .endpoints
            .into_iter()
            .map(|endpoint| {
                let endpoint = Endpoint {
                    host: endpoint.host,
                    grpc_port: u16::try_from(endpoint.grpc_port)
                        .context("invalid shuffle server port")?,
                };
                ensure!(
                    !endpoint.host.is_empty() && endpoint.grpc_port != 0,
                    "invalid shuffle server address"
                );
                tonic::transport::Endpoint::from_shared(endpoint.uri())
                    .context("invalid shuffle server address")?;
                Ok(endpoint)
            })
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            parsed.insert(partition, endpoints).is_none(),
            "duplicate assignment for partition {partition}"
        );
    }
    Ok(parsed)
}
