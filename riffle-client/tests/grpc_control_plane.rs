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
use croaring::{JvmLegacy, Treemap};
use riffle_client::{
    ApplicationId, ApplicationSpec, BlockPayload, Driver, DriverConfig, PartitionId, ShuffleId,
    ShuffleSpec, ShuffleWriter, ShuffleWriterConfig, TaskAttemptId,
};
use riffle_proto::uniffle::coordinator_server_server::{
    CoordinatorServer, CoordinatorServerServer,
};
use riffle_proto::uniffle::shuffle_server_client::ShuffleServerClient;
use riffle_proto::uniffle::*;
use riffle_server::config::Config;
use riffle_server::{mini_riffle, util};
use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn driver_creates_serializable_handle_and_owns_remote_lifecycle() {
    let shuffle_port = util::find_available_port().expect("an available shuffle port");
    let http_port = util::find_available_port().expect("an available HTTP port");
    let mut shuffle_config = Config::create_simple_config();
    shuffle_config.grpc_port = shuffle_port;
    shuffle_config.http_port = http_port;
    shuffle_config.fallback_random_ports_enable = false;
    mini_riffle::start(&shuffle_config)
        .await
        .expect("mini Riffle server starts");
    let shuffle_endpoint = format!("http://127.0.0.1:{shuffle_port}");
    let mut shuffle_client = connect_shuffle_with_retry(&shuffle_endpoint).await;

    let heartbeat_count = Arc::new(AtomicUsize::new(0));
    let coordinator = MockCoordinator {
        shuffle_port,
        heartbeat_count: heartbeat_count.clone(),
    };
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("coordinator listener binds");
    let coordinator_addr = listener.local_addr().unwrap();
    let coordinator_task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(CoordinatorServerServer::new(coordinator))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });

    let mut driver_config = DriverConfig::new(vec![format!("http://{coordinator_addr}")]);
    driver_config.heartbeat_interval = Duration::from_millis(20);
    let driver = Driver::connect(driver_config)
        .await
        .expect("driver connects to the coordinator");
    let application_id = ApplicationId::new("driver-integration").unwrap();
    let session = driver
        .open_application(ApplicationSpec::new(
            application_id.clone(),
            "integration-test",
        ))
        .await
        .expect("application opens");

    let shuffle_id = ShuffleId::new(11).unwrap();
    let handle = session
        .create_shuffle(ShuffleSpec::new(shuffle_id, 2))
        .await
        .expect("driver gets assignment and registers the shuffle server");
    assert_eq!(handle.routes.len(), 1);
    assert_eq!(handle.routes[0].end, PartitionId::new(1));
    let serialized = serde_json::to_vec(&handle).expect("handle serializes as task metadata");
    assert!(!serialized.is_empty());

    let writer = ShuffleWriter::from_handle(handle.clone(), ShuffleWriterConfig::default())
        .expect("worker constructs writer without a Driver reference");
    let mut attempt = writer.open_attempt(TaskAttemptId::new(3)).unwrap();
    attempt
        .push(
            PartitionId::new(1),
            vec![BlockPayload::new(Bytes::from_static(b"driver-flow")).unwrap()],
        )
        .await
        .expect("registered shuffle accepts data");
    attempt.finish().await.expect("map output is reported");

    tokio::time::timeout(Duration::from_secs(1), async {
        while heartbeat_count.load(Ordering::Relaxed) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("application heartbeat is emitted");

    session
        .close_shuffle(&handle)
        .await
        .expect("shuffle cleanup succeeds");
    let response = shuffle_client
        .get_shuffle_result(GetShuffleResultRequest {
            app_id: application_id.as_str().to_string(),
            shuffle_id: shuffle_id.value(),
            partition_id: 1,
            block_id_layout: None,
        })
        .await
        .expect("server remains reachable")
        .into_inner();
    assert_eq!(response.status, StatusCode::Success as i32);
    assert!(Treemap::deserialize::<JvmLegacy>(&response.serialized_bitmap).is_empty());

    session.close().await.expect("application session closes");
    coordinator_task.abort();
}

#[derive(Clone)]
struct MockCoordinator {
    shuffle_port: u16,
    heartbeat_count: Arc<AtomicUsize>,
}

#[tonic::async_trait]
impl CoordinatorServer for MockCoordinator {
    async fn get_shuffle_server_list(
        &self,
        _request: Request<()>,
    ) -> Result<Response<GetShuffleServerListResponse>, Status> {
        Err(Status::unimplemented("get_shuffle_server_list"))
    }

    async fn get_shuffle_server_num(
        &self,
        _request: Request<()>,
    ) -> Result<Response<GetShuffleServerNumResponse>, Status> {
        Err(Status::unimplemented("get_shuffle_server_num"))
    }

    async fn get_shuffle_assignments(
        &self,
        request: Request<GetShuffleServerRequest>,
    ) -> Result<Response<GetShuffleAssignmentsResponse>, Status> {
        let request = request.into_inner();
        Ok(Response::new(GetShuffleAssignmentsResponse {
            status: StatusCode::Success as i32,
            assignments: vec![PartitionRangeAssignment {
                start_partition: 0,
                // Deliberately emulate the coordinator's rounded final range.
                end_partition: request.partition_num,
                server: vec![ShuffleServerId {
                    id: "mini-server".to_string(),
                    ip: "127.0.0.1".to_string(),
                    port: i32::from(self.shuffle_port),
                    netty_port: 0,
                    jetty_port: 0,
                }],
            }],
            ret_msg: String::new(),
        }))
    }

    async fn app_heartbeat(
        &self,
        _request: Request<AppHeartBeatRequest>,
    ) -> Result<Response<AppHeartBeatResponse>, Status> {
        self.heartbeat_count.fetch_add(1, Ordering::Relaxed);
        Ok(Response::new(AppHeartBeatResponse {
            status: StatusCode::Success as i32,
            ret_msg: String::new(),
        }))
    }

    async fn heartbeat(
        &self,
        _request: Request<ShuffleServerHeartBeatRequest>,
    ) -> Result<Response<ShuffleServerHeartBeatResponse>, Status> {
        Err(Status::unimplemented("heartbeat"))
    }

    async fn get_shuffle_data_storage_info(
        &self,
        _request: Request<()>,
    ) -> Result<Response<GetShuffleDataStorageInfoResponse>, Status> {
        Err(Status::unimplemented("get_shuffle_data_storage_info"))
    }

    async fn check_service_available(
        &self,
        _request: Request<()>,
    ) -> Result<Response<CheckServiceAvailableResponse>, Status> {
        Err(Status::unimplemented("check_service_available"))
    }

    async fn report_client_operation(
        &self,
        _request: Request<ReportShuffleClientOpRequest>,
    ) -> Result<Response<ReportShuffleClientOpResponse>, Status> {
        Err(Status::unimplemented("report_client_operation"))
    }

    async fn register_application_info(
        &self,
        request: Request<ApplicationInfoRequest>,
    ) -> Result<Response<ApplicationInfoResponse>, Status> {
        if request.get_ref().app_id.is_empty() {
            return Err(Status::invalid_argument("app_id is required"));
        }
        Ok(Response::new(ApplicationInfoResponse {
            status: StatusCode::Success as i32,
            ret_msg: String::new(),
        }))
    }

    async fn access_cluster(
        &self,
        _request: Request<AccessClusterRequest>,
    ) -> Result<Response<AccessClusterResponse>, Status> {
        Ok(Response::new(AccessClusterResponse {
            status: StatusCode::Success as i32,
            ret_msg: String::new(),
            uuid: "access-token".to_string(),
        }))
    }

    async fn fetch_client_conf(
        &self,
        _request: Request<()>,
    ) -> Result<Response<FetchClientConfResponse>, Status> {
        Err(Status::unimplemented("fetch_client_conf"))
    }

    async fn fetch_client_conf_v2(
        &self,
        _request: Request<FetchClientConfRequest>,
    ) -> Result<Response<FetchClientConfResponse>, Status> {
        Err(Status::unimplemented("fetch_client_conf_v2"))
    }

    async fn fetch_remote_storage(
        &self,
        _request: Request<FetchRemoteStorageRequest>,
    ) -> Result<Response<FetchRemoteStorageResponse>, Status> {
        Err(Status::unimplemented("fetch_remote_storage"))
    }
}

async fn connect_shuffle_with_retry(
    endpoint: &str,
) -> ShuffleServerClient<tonic::transport::Channel> {
    for _ in 0..50 {
        if let Ok(client) = ShuffleServerClient::connect(endpoint.to_string()).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("mini Riffle server did not become ready at {endpoint}");
}
