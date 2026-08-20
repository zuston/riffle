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
    RetryPolicy, ShuffleHandle, ShuffleId, ShuffleReader, ShuffleReaderConfig, ShuffleServer,
    ShuffleWriter, ShuffleWriterConfig, TaskAttemptId,
};
use riffle_proto::uniffle::shuffle_server_client::ShuffleServerClient;
use riffle_proto::uniffle::{
    GetLocalShuffleIndexRequest, ShuffleRegisterRequest, ShuffleUnregisterRequest,
};
use riffle_server::config::Config;
use riffle_server::{mini_riffle, util};
use std::time::Duration;

const BLOCK_SIZE: usize = 16 * 1024;
const BLOCK_COUNT: usize = 8;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_unifies_memory_and_spilled_localfile_blocks() {
    let data_dir = tempfile::tempdir().expect("temporary localfile directory");
    let grpc_port = util::find_available_port().expect("an available gRPC port");
    let http_port = util::find_available_port().expect("an available HTTP port");
    let mut server_config = Config::create_mem_localfile_config(
        i32::from(grpc_port),
        "64K".to_string(),
        data_dir.path().to_string_lossy().into_owned(),
    );
    server_config.http_port = http_port;
    server_config.fallback_random_ports_enable = false;
    server_config
        .hybrid_store
        .memory_single_buffer_max_spill_size = Some("16K".to_string());
    let localfile_store = server_config
        .localfile_store
        .as_mut()
        .expect("localfile store is configured");
    localfile_store.disk_high_watermark = 1.0;
    localfile_store.disk_low_watermark = 1.0;
    mini_riffle::start(&server_config)
        .await
        .expect("mini Riffle server starts");

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut raw_client = connect_with_retry(&endpoint).await;
    let application_id = ApplicationId::new("riffle-client-spill-integration").unwrap();
    let shuffle_id = ShuffleId::new(2).unwrap();
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
            replicas: vec![ShuffleServer {
                id: "spill-server".to_string(),
                host: "127.0.0.1".to_string(),
                grpc_port,
                urpc_port: None,
                http_port: Some(http_port),
            }],
        }],
    )
    .unwrap();
    let writer_config = ShuffleWriterConfig {
        max_batch_bytes: BLOCK_SIZE,
        retry_policy: RetryPolicy {
            max_attempts: 30,
            initial_backoff: Duration::from_millis(20),
            max_backoff: Duration::from_millis(100),
        },
        ..ShuffleWriterConfig::default()
    };
    let writer = ShuffleWriter::from_handle(handle.clone(), writer_config).unwrap();
    let task_attempt_id = TaskAttemptId::new(9);
    let mut attempt = writer.open_attempt(task_attempt_id).unwrap();
    let payloads = (0..BLOCK_COUNT)
        .map(|index| BlockPayload::new(Bytes::from(vec![index as u8; BLOCK_SIZE])).unwrap())
        .collect();
    attempt
        .push(PartitionId::new(0), payloads)
        .await
        .expect("bounded writer waits for spill capacity");
    attempt.finish().await.expect("blocks are reported");

    wait_for_local_index(&mut raw_client, application_id.as_str(), shuffle_id.value()).await;

    let reader = ShuffleReader::from_handle(handle, ShuffleReaderConfig::default()).unwrap();
    let mut blocks = reader
        .read_partition(ReadPartitionRequest::new(
            PartitionId::new(0),
            vec![task_attempt_id],
        ))
        .await
        .expect("partition read starts")
        .try_collect::<Vec<_>>()
        .await
        .expect("memory and localfile blocks form one complete stream");
    blocks.sort_by_key(|block| block.data[0]);

    assert_eq!(blocks.len(), BLOCK_COUNT);
    for (index, block) in blocks.iter().enumerate() {
        assert_eq!(block.data.len(), BLOCK_SIZE);
        assert!(block.data.iter().all(|byte| *byte == index as u8));
    }

    raw_client
        .unregister_shuffle(ShuffleUnregisterRequest {
            app_id: application_id.as_str().to_string(),
            shuffle_id: shuffle_id.value(),
        })
        .await
        .expect("cleanup RPC succeeds");
}

async fn wait_for_local_index(
    client: &mut ShuffleServerClient<tonic::transport::Channel>,
    application_id: &str,
    shuffle_id: i32,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let response = client
                .get_local_shuffle_index(GetLocalShuffleIndexRequest {
                    app_id: application_id.to_string(),
                    shuffle_id,
                    partition_id: 0,
                    partition_num_per_range: 1,
                    partition_num: 1,
                })
                .await
                .expect("local index RPC succeeds")
                .into_inner();
            assert_eq!(response.status, 0, "{}", response.ret_msg);
            if !response.index_data.is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("at least one block spills to localfile");
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
