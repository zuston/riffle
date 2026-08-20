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
    BlockId, PartitionId, RetryPolicy, RiffleError, ShuffleHandle, ShuffleServer,
    ShuffleWriterConfig, TaskAttemptId,
};
use bytes::{BufMut, Bytes, BytesMut};
use riffle_proto::uniffle::shuffle_server_client::ShuffleServerClient;
use riffle_proto::uniffle::{
    CombinedShuffleData, PartitionStats, PartitionToBlockIds, ReportShuffleResultRequest,
    RequireBufferRequest, SendShuffleDataRequest,
};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::transport::Channel;

#[derive(Clone, Debug)]
pub(crate) struct WriteClient {
    connection: Arc<Connection>,
    application_id: String,
    shuffle_id: i32,
    client: ShuffleServerClient<Channel>,
    retry_policy: RetryPolicy,
}

#[derive(Clone)]
pub(crate) struct PreparedBlock {
    pub(crate) block_id: BlockId,
    pub(crate) length: i32,
    pub(crate) uncompressed_length: i32,
    pub(crate) crc: u32,
    pub(crate) data: Bytes,
    pub(crate) record_count: Option<u64>,
}

impl WriteClient {
    pub(crate) fn new(
        server: &ShuffleServer,
        handle: &ShuffleHandle,
        config: &ShuffleWriterConfig,
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
            client,
            retry_policy: config.retry_policy.clone(),
        })
    }

    pub(crate) async fn send_shuffle_data(
        &self,
        partition_id: PartitionId,
        task_attempt_id: TaskAttemptId,
        blocks: &[PreparedBlock],
    ) -> Result<(), RiffleError> {
        let total_size = blocks.iter().map(|block| block.data.len()).sum::<usize>();
        for attempt in 1..=self.retry_policy.max_attempts {
            let mut client = self.client.clone();
            let ticket = match client
                .require_buffer(RequireBufferRequest {
                    require_size: i32::try_from(total_size).map_err(|_| {
                        RiffleError::InvalidArgument(
                            "writer batch size exceeds i32::MAX".to_string(),
                        )
                    })?,
                    app_id: self.application_id.clone(),
                    shuffle_id: self.shuffle_id,
                    partition_ids: vec![partition_id.as_i32()?],
                })
                .await
            {
                Ok(response) => {
                    let response = response.into_inner();
                    match RemoteStatus::from(response.status) {
                        RemoteStatus::Success => response.require_buffer_id,
                        RemoteStatus::NoBuffer if attempt < self.retry_policy.max_attempts => {
                            self.sleep_after(attempt).await;
                            continue;
                        }
                        status => {
                            return Err(RiffleError::Remote {
                                operation: "require_buffer",
                                status,
                                message: response.ret_msg,
                            });
                        }
                    }
                }
                Err(_error) if attempt < self.retry_policy.max_attempts => {
                    self.sleep_after(attempt).await;
                    continue;
                }
                Err(error) => {
                    return Err(RiffleError::Transport {
                        operation: "require_buffer",
                        endpoint: self.connection.endpoint().to_string(),
                        message: error.to_string(),
                    });
                }
            };

            let response = client
                .send_shuffle_data(SendShuffleDataRequest {
                    app_id: self.application_id.clone(),
                    shuffle_id: self.shuffle_id,
                    require_buffer_id: ticket,
                    shuffle_data: Vec::new(),
                    timestamp: now_millis(),
                    stage_attempt_number: 0,
                    combined_shuffle_data: Some(combined_data(
                        partition_id,
                        task_attempt_id,
                        blocks,
                    )?),
                })
                .await;
            match response {
                Ok(response) => {
                    let response = response.into_inner();
                    let status = RemoteStatus::from(response.status);
                    match status {
                        RemoteStatus::Success => return Ok(()),
                        RemoteStatus::NoBuffer if attempt < self.retry_policy.max_attempts => {
                            self.sleep_after(attempt).await;
                        }
                        RemoteStatus::NoBuffer => {
                            return Err(RiffleError::Remote {
                                operation: "send_shuffle_data",
                                status,
                                message: response.ret_msg,
                            });
                        }
                        _ => {
                            return Err(RiffleError::AmbiguousWrite {
                                operation: "send_shuffle_data",
                                endpoint: self.connection.endpoint().to_string(),
                                message: format!("remote status {status:?}: {}", response.ret_msg),
                            });
                        }
                    }
                }
                Err(error) => {
                    return Err(RiffleError::AmbiguousWrite {
                        operation: "send_shuffle_data",
                        endpoint: self.connection.endpoint().to_string(),
                        message: error.to_string(),
                    });
                }
            }
        }
        Err(RiffleError::Remote {
            operation: "send_shuffle_data",
            status: RemoteStatus::NoBuffer,
            message: "buffer retry budget exhausted".to_string(),
        })
    }

    pub(crate) async fn report_shuffle_result(
        &self,
        task_attempt_id: TaskAttemptId,
        partition_to_block_ids: Vec<PartitionToBlockIds>,
        partition_stats: Vec<PartitionStats>,
    ) -> Result<(), RiffleError> {
        let mut client = self.client.clone();
        let response = client
            .report_shuffle_result(ReportShuffleResultRequest {
                app_id: self.application_id.clone(),
                shuffle_id: self.shuffle_id,
                task_attempt_id: task_attempt_id.as_i64()?,
                bitmap_num: 0,
                partition_to_block_ids,
                partition_stats,
            })
            .await
            .map_err(|error| RiffleError::AmbiguousWrite {
                operation: "report_shuffle_result",
                endpoint: self.connection.endpoint().to_string(),
                message: error.to_string(),
            })?
            .into_inner();
        let status = RemoteStatus::from(response.status);
        if status == RemoteStatus::Success {
            Ok(())
        } else {
            Err(RiffleError::AmbiguousWrite {
                operation: "report_shuffle_result",
                endpoint: self.connection.endpoint().to_string(),
                message: format!("remote status {status:?}: {}", response.ret_msg),
            })
        }
    }

    async fn sleep_after(&self, attempt: u32) {
        tokio::time::sleep(self.retry_policy.delay_for(attempt)).await;
    }
}

fn combined_data(
    partition_id: PartitionId,
    task_attempt_id: TaskAttemptId,
    blocks: &[PreparedBlock],
) -> Result<CombinedShuffleData, RiffleError> {
    let total_size = blocks.iter().map(|block| block.data.len()).sum();
    let mut data = BytesMut::with_capacity(total_size);
    for block in blocks {
        data.put_slice(&block.data);
    }
    Ok(CombinedShuffleData {
        partition_ids: vec![partition_id.as_i32()?],
        partition_block_counts: vec![i32::try_from(blocks.len()).map_err(|_| {
            RiffleError::InvalidArgument("writer batch has too many blocks".to_string())
        })?],
        block_ids: blocks
            .iter()
            .map(|block| block.block_id.as_i64())
            .collect::<Result<Vec<_>, RiffleError>>()?,
        lengths: blocks.iter().map(|block| block.length).collect(),
        uncompress_lengths: blocks
            .iter()
            .map(|block| block.uncompressed_length)
            .collect(),
        crcs: blocks.iter().map(|block| i64::from(block.crc)).collect(),
        task_attempt_ids: vec![task_attempt_id.as_i64()?; blocks.len()],
        combined_data: data.freeze(),
    })
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prepared(id: u64, length: usize) -> PreparedBlock {
        PreparedBlock {
            block_id: BlockId::new(id),
            length: length as i32,
            uncompressed_length: length as i32,
            crc: 0,
            data: Bytes::from(vec![0; length]),
            record_count: None,
        }
    }

    #[test]
    fn combined_payload_preserves_block_boundaries() {
        let blocks = vec![prepared(1, 2), prepared(2, 3)];
        let combined = combined_data(PartitionId::new(4), TaskAttemptId::new(7), &blocks).unwrap();

        assert_eq!(combined.partition_ids, vec![4]);
        assert_eq!(combined.partition_block_counts, vec![2]);
        assert_eq!(combined.lengths, vec![2, 3]);
        assert_eq!(combined.combined_data.len(), 5);
    }
}
