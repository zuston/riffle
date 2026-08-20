// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::connection_pool::{Connection, ConnectionPool, ConnectionSettings};
use crate::error::RemoteStatus;
use crate::{
    PartitionId, RetryPolicy, RiffleError, ShuffleHandle, ShuffleReaderConfig, ShuffleServer,
};
use bytes::Bytes;
use riffle_proto::uniffle::shuffle_server_client::ShuffleServerClient;
use riffle_proto::uniffle::{
    BlockIdLayout as ProtoBlockIdLayout, GetLocalShuffleDataRequest, GetLocalShuffleDataResponse,
    GetLocalShuffleIndexRequest, GetLocalShuffleIndexResponse, GetMemoryShuffleDataRequest,
    GetMemoryShuffleDataResponse, GetShuffleResultRequest, GetShuffleResultResponse,
};
use std::future::Future;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::transport::Channel;
use tonic::Status;

#[derive(Clone, Debug)]
pub(crate) struct ReadClient {
    connection: Arc<Connection>,
    application_id: String,
    shuffle_id: i32,
    partition_count: i32,
    partition_range_size: i32,
    block_id_layout: ProtoBlockIdLayout,
    read_buffer_size: i32,
    client: ShuffleServerClient<Channel>,
    retry_policy: RetryPolicy,
}

impl ReadClient {
    pub(crate) fn new(
        server: &ShuffleServer,
        handle: &ShuffleHandle,
        config: &ShuffleReaderConfig,
    ) -> Result<Self, RiffleError> {
        let settings = ConnectionSettings {
            connect_timeout: config.connect_timeout,
            request_timeout: config.request_timeout,
            max_encoding_message_size: config.max_encoding_message_size,
            max_decoding_message_size: config.max_decoding_message_size,
        };
        let (connection, client) =
            ConnectionPool::global().shuffle_server_client(server, &settings)?;
        Ok(Self {
            connection,
            application_id: handle.application_id.as_str().to_string(),
            shuffle_id: handle.shuffle_id.value(),
            partition_count: i32::try_from(handle.partition_count).map_err(|_| {
                RiffleError::InvalidArgument("partition_count exceeds i32::MAX".to_string())
            })?,
            partition_range_size: i32::try_from(handle.partition_range_size).map_err(|_| {
                RiffleError::InvalidArgument("partition_range_size exceeds i32::MAX".to_string())
            })?,
            block_id_layout: ProtoBlockIdLayout {
                sequence_no_bits: i32::from(handle.block_id_layout.sequence_no_bits),
                partition_id_bits: i32::from(handle.block_id_layout.partition_id_bits),
                task_attempt_id_bits: i32::from(handle.block_id_layout.task_attempt_id_bits),
            },
            read_buffer_size: i32::try_from(config.read_buffer_size).map_err(|_| {
                RiffleError::InvalidArgument("read_buffer_size exceeds i32::MAX".to_string())
            })?,
            client,
            retry_policy: config.retry_policy.clone(),
        })
    }

    pub(crate) async fn get_shuffle_result(
        &self,
        partition_id: PartitionId,
    ) -> Result<GetShuffleResultResponse, RiffleError> {
        let request = GetShuffleResultRequest {
            app_id: self.application_id.clone(),
            shuffle_id: self.shuffle_id,
            partition_id: partition_id.as_i32()?,
            block_id_layout: Some(self.block_id_layout.clone()),
        };
        self.call("get_shuffle_result", |mut client| {
            let request = request.clone();
            async move {
                let response = client.get_shuffle_result(request).await?.into_inner();
                let status = RemoteStatus::from(response.status);
                let message = response.ret_msg.clone();
                Ok((response, status, message))
            }
        })
        .await
    }

    pub(crate) async fn get_memory_page(
        &self,
        partition_id: PartitionId,
        cursor: i64,
        expected_attempts: Bytes,
    ) -> Result<GetMemoryShuffleDataResponse, RiffleError> {
        let request = GetMemoryShuffleDataRequest {
            app_id: self.application_id.clone(),
            shuffle_id: self.shuffle_id,
            partition_id: partition_id.as_i32()?,
            last_block_id: cursor,
            read_buffer_size: self.read_buffer_size,
            timestamp: now_millis(),
            serialized_expected_task_ids_bitmap: expected_attempts,
        };
        self.call("get_memory_shuffle_data", |mut client| {
            let request = request.clone();
            async move {
                let response = client.get_memory_shuffle_data(request).await?.into_inner();
                let status = RemoteStatus::from(response.status);
                let message = response.ret_msg.clone();
                Ok((response, status, message))
            }
        })
        .await
    }

    pub(crate) async fn get_local_index(
        &self,
        partition_id: PartitionId,
    ) -> Result<GetLocalShuffleIndexResponse, RiffleError> {
        let request = GetLocalShuffleIndexRequest {
            app_id: self.application_id.clone(),
            shuffle_id: self.shuffle_id,
            partition_id: partition_id.as_i32()?,
            partition_num_per_range: self.partition_range_size,
            partition_num: self.partition_count,
        };
        self.call("get_local_shuffle_index", |mut client| {
            let request = request.clone();
            async move {
                let response = client.get_local_shuffle_index(request).await?.into_inner();
                let status = RemoteStatus::from(response.status);
                let message = response.ret_msg.clone();
                Ok((response, status, message))
            }
        })
        .await
    }

    pub(crate) async fn get_local_data(
        &self,
        partition_id: PartitionId,
        offset: i64,
        length: i32,
        storage_id: i32,
    ) -> Result<GetLocalShuffleDataResponse, RiffleError> {
        let request = GetLocalShuffleDataRequest {
            app_id: self.application_id.clone(),
            shuffle_id: self.shuffle_id,
            partition_id: partition_id.as_i32()?,
            partition_num_per_range: self.partition_range_size,
            partition_num: self.partition_count,
            offset,
            length,
            timestamp: now_millis(),
            storage_id,
        };
        self.call("get_local_shuffle_data", |mut client| {
            let request = request.clone();
            async move {
                let response = client.get_local_shuffle_data(request).await?.into_inner();
                let status = RemoteStatus::from(response.status);
                let message = response.ret_msg.clone();
                Ok((response, status, message))
            }
        })
        .await
    }

    async fn call<T, F, Fut>(&self, operation: &'static str, mut rpc: F) -> Result<T, RiffleError>
    where
        F: FnMut(ShuffleServerClient<Channel>) -> Fut,
        Fut: Future<Output = Result<(T, RemoteStatus, String), Status>>,
    {
        for attempt in 1..=self.retry_policy.max_attempts {
            match rpc(self.client.clone()).await {
                Ok((response, RemoteStatus::Success, _)) => return Ok(response),
                Ok((_response, status, _message))
                    if status.is_retryable_read() && attempt < self.retry_policy.max_attempts =>
                {
                    self.sleep_after(attempt).await;
                }
                Ok((_response, status, message)) => {
                    return Err(RiffleError::Remote {
                        operation,
                        status,
                        message,
                    });
                }
                Err(_error) if attempt < self.retry_policy.max_attempts => {
                    self.sleep_after(attempt).await;
                }
                Err(error) => {
                    return Err(RiffleError::Transport {
                        operation,
                        endpoint: self.connection.endpoint().to_string(),
                        message: error.to_string(),
                    });
                }
            }
        }
        unreachable!("retry policy always has at least one attempt")
    }

    async fn sleep_after(&self, attempt: u32) {
        tokio::time::sleep(self.retry_policy.delay_for(attempt)).await;
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
