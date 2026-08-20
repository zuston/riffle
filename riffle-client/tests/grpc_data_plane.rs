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

use bytes::Bytes;
use futures::TryStreamExt;
use riffle_client::{
    ApplicationId, BlockIdLayout, BlockPayload, PartitionId, PartitionRoute, ReadPartitionRequest,
    ShuffleHandle, ShuffleId, ShuffleReader, ShuffleReaderConfig, ShuffleServer, ShuffleWriter,
    ShuffleWriterConfig, TaskAttemptId,
};
use riffle_proto::uniffle::shuffle_server_client::ShuffleServerClient;
use riffle_proto::uniffle::{ShuffleRegisterRequest, ShuffleUnregisterRequest};
use riffle_server::config::Config;
use riffle_server::{mini_riffle, util};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writer_and_reader_filter_attempts_and_follow_server_pagination_order() {
    let grpc_port = util::find_available_port().expect("an available gRPC port");
    let http_port = util::find_available_port().expect("an available HTTP port");
    let mut server_config = Config::create_simple_config();
    server_config.grpc_port = grpc_port;
    server_config.http_port = http_port;
    server_config.fallback_random_ports_enable = false;
    mini_riffle::start(&server_config)
        .await
        .expect("mini Riffle server starts");

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut raw_client = connect_with_retry(&endpoint).await;
    let application_id = ApplicationId::new("riffle-client-integration").unwrap();
    let shuffle_id = ShuffleId::new(1).unwrap();
    let registration = raw_client
        .register_shuffle(ShuffleRegisterRequest {
            app_id: application_id.as_str().to_string(),
            shuffle_id: shuffle_id.value(),
            partition_ranges: Vec::new(),
            remote_storage: None,
            user: "integration-test".to_string(),
            shuffle_data_distribution: 0,
            max_concurrency_per_partition_to_write: 1,
            merge_context: None,
            properties: Default::default(),
        })
        .await
        .expect("register RPC succeeds")
        .into_inner();
    assert_eq!(registration.status, 0);

    let server = ShuffleServer {
        id: "mini-server".to_string(),
        host: "127.0.0.1".to_string(),
        grpc_port,
        urpc_port: None,
        http_port: Some(http_port),
    };
    let handle = ShuffleHandle::new(
        application_id.clone(),
        shuffle_id,
        1,
        1,
        1,
        BlockIdLayout::default(),
        vec![PartitionRoute {
            start: PartitionId::new(0),
            end: PartitionId::new(0),
            replicas: vec![server],
        }],
    )
    .unwrap();
    let writer = ShuffleWriter::from_handle(handle.clone(), ShuffleWriterConfig::default())
        .expect("writer is created from a serializable handle");

    let accepted_high_attempt = TaskAttemptId::new(8);
    let mut accepted_writer = writer.open_attempt(accepted_high_attempt).unwrap();
    accepted_writer
        .push(
            PartitionId::new(0),
            vec![BlockPayload::new(Bytes::from_static(b"accepted-high")).unwrap()],
        )
        .await
        .expect("accepted attempt writes");
    accepted_writer
        .finish()
        .await
        .expect("accepted attempt reports its blocks");

    let accepted_low_attempt = TaskAttemptId::new(7);
    let mut accepted_writer = writer.open_attempt(accepted_low_attempt).unwrap();
    accepted_writer
        .push(
            PartitionId::new(0),
            vec![BlockPayload::new(Bytes::from_static(b"accepted-low")).unwrap()],
        )
        .await
        .expect("accepted attempt writes");
    accepted_writer
        .finish()
        .await
        .expect("accepted attempt reports its blocks");

    let stale_attempt = TaskAttemptId::new(9);
    let mut stale_writer = writer.open_attempt(stale_attempt).unwrap();
    stale_writer
        .push(
            PartitionId::new(0),
            vec![BlockPayload::new(Bytes::from_static(b"stale")).unwrap()],
        )
        .await
        .expect("stale attempt writes");
    stale_writer
        .finish()
        .await
        .expect("stale attempt reports its blocks");

    let reader = ShuffleReader::from_handle(
        handle,
        ShuffleReaderConfig {
            read_buffer_size: 8,
            ..ShuffleReaderConfig::default()
        },
    )
    .expect("reader is created independently from the same handle");
    let blocks = reader
        .read_partition(ReadPartitionRequest::new(
            PartitionId::new(0),
            vec![accepted_high_attempt, accepted_low_attempt],
        ))
        .await
        .expect("partition read starts")
        .try_collect::<Vec<_>>()
        .await
        .expect("partition read completes without missing blocks");

    assert_eq!(blocks.len(), 2);
    let mut blocks = blocks;
    blocks.sort_by_key(|block| block.task_attempt_id);
    assert_eq!(blocks[0].task_attempt_id, accepted_low_attempt);
    assert_eq!(blocks[0].data, Bytes::from_static(b"accepted-low"));
    assert_eq!(blocks[1].task_attempt_id, accepted_high_attempt);
    assert_eq!(blocks[1].data, Bytes::from_static(b"accepted-high"));

    let cleanup = raw_client
        .unregister_shuffle(ShuffleUnregisterRequest {
            app_id: application_id.as_str().to_string(),
            shuffle_id: shuffle_id.value(),
        })
        .await
        .expect("cleanup RPC succeeds")
        .into_inner();
    assert_eq!(cleanup.status, 0);
}

async fn connect_with_retry(endpoint: &str) -> ShuffleServerClient<tonic::transport::Channel> {
    for _ in 0..50 {
        if let Ok(client) = ShuffleServerClient::connect(endpoint.to_string()).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("mini Riffle server did not become ready at {endpoint}");
}
