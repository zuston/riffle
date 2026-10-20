use std::collections::HashMap;

use anyhow::{Context, Result};
use riffle_server::ddashmap::DDashMap;
use tonic::transport::Channel;

use crate::rpc::get_write_shuffle_handle_request::ShuffleIdentifier;
use crate::rpc::shuffle_manager_service_client::ShuffleManagerServiceClient;
use crate::rpc::{
    GetReadShuffleHandleRequest, GetWriteShuffleHandleRequest, JobIdAndStageId,
    ReportShuffleResultRequest,
};
use crate::rpc_client::{call, request, retry};
use crate::{
    ApplicationId, Endpoint, ReadTaskInfo, ShuffleAssignment, ShuffleHandle, ShuffleId, TaskId,
    WriteTaskInfo, MAX_MESSAGE_BYTES, RPC_TIMEOUT,
};

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
        for assignment in response.assignments {
            let partition_id = assignment.partition_id as usize;
            let endpoint = assignment.endpoint.context("assignment has no endpoint")?;
            let endpoint = Endpoint {
                host: endpoint.host,
                grpc_port: endpoint.grpc_port as u16,
            };
            tonic::transport::Endpoint::from_shared(endpoint.uri())
                .context("invalid shuffle server address")?;
            assignments.insert(partition_id, vec![endpoint]);
        }
        Ok((
            ShuffleAssignment::new(shuffle_id, assignments),
            task_id,
            response.configs,
        ))
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
            partition_ids: task.partitions.iter().map(|&id| id as u64).collect(),
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
        for assignment in response.assignments {
            let partition_id = assignment.partition_id as usize;
            let endpoint = assignment.endpoint.context("assignment has no endpoint")?;
            let endpoint = Endpoint {
                host: endpoint.host,
                grpc_port: endpoint.grpc_port as u16,
            };
            assignments.insert(partition_id, vec![endpoint]);
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
