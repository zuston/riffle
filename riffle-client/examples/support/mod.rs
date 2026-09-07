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

use riffle_proto::uniffle::coordinator_server_server::{
    CoordinatorServer, CoordinatorServerServer,
};
use riffle_proto::uniffle::shuffle_server_client::ShuffleServerClient;
use riffle_proto::uniffle::*;
use riffle_server::config::Config;
use riffle_server::{mini_riffle, util};
use std::error::Error;
use std::net::Ipv4Addr;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

type DynError = Box<dyn Error + Send + Sync>;

pub struct DemoCluster {
    coordinator_endpoint: String,
    coordinator_task: JoinHandle<()>,
}

impl DemoCluster {
    pub async fn start() -> Result<Self, DynError> {
        let shuffle_port = util::find_available_port()
            .ok_or_else(|| io_error("no port is available for the shuffle server".to_string()))?;
        let http_port = loop {
            let port = util::find_available_port()
                .ok_or_else(|| io_error("no port is available for the HTTP server".to_string()))?;
            if port != shuffle_port {
                break port;
            }
        };
        let mut shuffle_config = Config::create_simple_config();
        shuffle_config.grpc_port = shuffle_port;
        shuffle_config.http_port = http_port;
        shuffle_config.fallback_random_ports_enable = false;
        mini_riffle::start(&shuffle_config).await?;
        wait_for_shuffle_server(shuffle_port).await?;

        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let coordinator_address = listener.local_addr()?;
        let coordinator_task = tokio::spawn(async move {
            let result = tonic::transport::Server::builder()
                .add_service(CoordinatorServerServer::new(DemoCoordinator {
                    shuffle_port,
                }))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await;
            if let Err(error) = result {
                eprintln!("Embedded coordinator failed: {error}");
            }
        });

        Ok(Self {
            coordinator_endpoint: format!("http://{coordinator_address}"),
            coordinator_task,
        })
    }

    pub fn coordinator_endpoint(&self) -> &str {
        &self.coordinator_endpoint
    }
}

impl Drop for DemoCluster {
    fn drop(&mut self) {
        self.coordinator_task.abort();
    }
}

#[derive(Clone)]
struct DemoCoordinator {
    shuffle_port: u16,
}

#[tonic::async_trait]
impl CoordinatorServer for DemoCoordinator {
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
        if request.partition_num <= 0 {
            return Err(Status::invalid_argument("partition_num must be positive"));
        }
        Ok(Response::new(GetShuffleAssignmentsResponse {
            status: StatusCode::Success as i32,
            assignments: vec![PartitionRangeAssignment {
                start_partition: 0,
                end_partition: request.partition_num - 1,
                server: vec![ShuffleServerId {
                    id: "embedded-shuffle-server".to_string(),
                    ip: "127.0.0.1".to_string(),
                    port: i32::from(self.shuffle_port),
                    netty_port: 0,
                    jetty_port: 0,
                }],
            }],
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

    async fn app_heartbeat(
        &self,
        _request: Request<AppHeartBeatRequest>,
    ) -> Result<Response<AppHeartBeatResponse>, Status> {
        Ok(Response::new(AppHeartBeatResponse {
            status: StatusCode::Success as i32,
            ret_msg: String::new(),
        }))
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
            uuid: "embedded-access-token".to_string(),
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

async fn wait_for_shuffle_server(port: u16) -> Result<(), DynError> {
    let endpoint = format!("http://127.0.0.1:{port}");
    for _ in 0..100 {
        if ShuffleServerClient::connect(endpoint.clone()).await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Err(io_error(format!(
        "embedded shuffle server did not become ready at {endpoint}"
    ))
    .into())
}

fn io_error(message: String) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::TimedOut, message)
}
