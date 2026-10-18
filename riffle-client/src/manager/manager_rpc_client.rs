use anyhow::{ensure, Context, Result};
use riffle_server::ddashmap::DDashMap;
use tonic::transport::Channel;

use crate::rpc::get_write_shuffle_handle_request::ShuffleIdentifier;
use crate::rpc::shuffle_manager_service_client::ShuffleManagerServiceClient;
use crate::rpc::{GetWriteShuffleHandleRequest, ReportShuffleResultRequest};
use crate::rpc_client::transport::{call, request, retry};
use crate::{
    Endpoint, ShuffleAssignment, ShuffleId, TaskId, WriteTaskInfo, MAX_MESSAGE_BYTES, RPC_TIMEOUT,
};

pub struct ManagerRpcClient {
    endpoint: Endpoint,
    client: ShuffleManagerServiceClient<Channel>,
}

impl ManagerRpcClient {
    pub fn new(endpoint: Endpoint) -> Result<Self> {
        ensure!(
            !endpoint.host.is_empty() && endpoint.grpc_port != 0,
            "invalid manager endpoint"
        );
        let channel = tonic::transport::Endpoint::from_shared(endpoint.uri())?
            .connect_timeout(RPC_TIMEOUT)
            .timeout(RPC_TIMEOUT)
            .connect_lazy();
        Ok(Self {
            endpoint,
            client: ShuffleManagerServiceClient::new(channel)
                .max_encoding_message_size(MAX_MESSAGE_BYTES)
                .max_decoding_message_size(MAX_MESSAGE_BYTES),
        })
    }

    pub async fn get_write_shuffle_handle(
        &self,
        task: &WriteTaskInfo,
    ) -> Result<(ShuffleAssignment, TaskId)> {
        let message = GetWriteShuffleHandleRequest {
            shuffle_identifier: Some(ShuffleIdentifier::ShuffleKey(format!(
                "{}_{}",
                task.job_id, task.stage_id
            ))),
            map_id: task.map_id as u64,
            attempt_id: task.attempt_id as u64,
        };
        let response = retry(&self.endpoint, "getWriteShuffleHandle", || {
            let mut client = self.client.clone();
            let message = message.clone();
            async move { call(client.get_write_shuffle_handle(request(message))).await }
        })
        .await?;
        let shuffle_id = usize::try_from(response.shuffle_id).context("shuffle ID is too large")?;
        ensure!(
            i32::try_from(shuffle_id).is_ok(),
            "shuffle ID exceeds protocol i32 range"
        );
        let task_id = usize::try_from(response.task_id).context("task ID is too large")?;
        let assignments = DDashMap::with_capacity(response.assignments.len());
        for assignment in response.assignments {
            let partition_id = usize::try_from(assignment.partition_id)
                .context("negative assignment partition ID")?;
            ensure!(
                partition_id < task.num_partitions,
                "assignment partition {partition_id} is out of range"
            );
            let endpoint = assignment.endpoint.context("assignment has no endpoint")?;
            let endpoint = Endpoint {
                host: endpoint.host,
                grpc_port: u16::try_from(endpoint.grpc_port)
                    .context("invalid shuffle server port")?,
            };
            ensure!(
                !endpoint.host.is_empty() && endpoint.grpc_port != 0,
                "invalid shuffle server endpoint"
            );
            tonic::transport::Endpoint::from_shared(endpoint.uri())
                .context("invalid shuffle server address")?;
            ensure!(
                assignments.insert(partition_id, vec![endpoint]).is_none(),
                "duplicate assignment for partition {partition_id}"
            );
        }
        ensure!(
            assignments.len() == task.num_partitions,
            "incomplete shuffle assignment"
        );
        Ok((ShuffleAssignment::new(shuffle_id, assignments), task_id))
    }

    pub async fn report_shuffle_result(
        &self,
        shuffle_id: ShuffleId,
        task_id: TaskId,
    ) -> Result<()> {
        let message = ReportShuffleResultRequest {
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
}
