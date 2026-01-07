// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use log::{debug, info, warn};
use std::collections::HashMap;
use tonic::{Request, Response, Status};

use crate::cluster::cluster_manager::{AssignmentRequest, HeartbeatInfo};
use crate::cluster::ClusterManagerRef;
use crate::grpc::protobuf::uniffle::coordinator_server_server::CoordinatorServer;
use crate::grpc::protobuf::uniffle::*;

/// Type alias for StatusCode from protobuf
type StatusCode = crate::grpc::protobuf::uniffle::StatusCode;

/// Default implementation of CoordinatorServer
#[derive(Clone)]
pub struct DefaultCoordinatorServer {
    cluster_manager: ClusterManagerRef,
}

impl DefaultCoordinatorServer {
    pub fn new(cluster_manager: ClusterManagerRef) -> Self {
        Self { cluster_manager }
    }
}

fn server_status_from_i32(status: i32) -> crate::cluster::server_node::ServerStatus {
    match status {
        1 => crate::cluster::server_node::ServerStatus::Decommissioning,
        2 => crate::cluster::server_node::ServerStatus::Decommissioned,
        3 => crate::cluster::server_node::ServerStatus::Lost,
        4 => crate::cluster::server_node::ServerStatus::Unhealthy,
        5 => crate::cluster::server_node::ServerStatus::Excluded,
        _ => crate::cluster::server_node::ServerStatus::Active,
    }
}

#[tonic::async_trait]
impl CoordinatorServer for DefaultCoordinatorServer {
    // ==================== 1. getShuffleServerList ====================

    async fn get_shuffle_server_list(
        &self,
        _request: Request<()>,
    ) -> Result<Response<GetShuffleServerListResponse>, Status> {
        let servers = self.cluster_manager.get_shuffle_server_list();

        let server_ids: Vec<ShuffleServerId> = servers
            .into_iter()
            .map(|node| ShuffleServerId {
                id: node.id,
                ip: node.ip,
                port: node.grpc_port,
                netty_port: node.netty_port,
                jetty_port: node.http_port,
            })
            .collect();

        Ok(Response::new(GetShuffleServerListResponse {
            servers: server_ids,
        }))
    }

    // ==================== 2. getShuffleServerNum ====================

    async fn get_shuffle_server_num(
        &self,
        _request: Request<()>,
    ) -> Result<Response<GetShuffleServerNumResponse>, Status> {
        let num = self.cluster_manager.get_shuffle_server_num();
        Ok(Response::new(GetShuffleServerNumResponse { num }))
    }

    // ==================== 3. getShuffleAssignments ====================

    async fn get_shuffle_assignments(
        &self,
        request: Request<GetShuffleServerRequest>,
    ) -> Result<Response<GetShuffleAssignmentsResponse>, Status> {
        let inner = request.into_inner();

        let assignment_request = AssignmentRequest {
            app_id: inner.application_id.clone(),
            shuffle_id: inner.shuffle_id,
            partition_num: inner.partition_num,
            partition_num_per_range: inner.partition_num_per_range,
            data_replica: inner.data_replica,
            require_tags: inner.require_tags,
            assignment_server_num: inner.assignment_shuffle_server_number,
        };

        match self
            .cluster_manager
            .get_shuffle_assignments(assignment_request)
        {
            Ok(result) => {
                let assignments: Vec<PartitionRangeAssignment> = result
                    .assignments
                    .into_iter()
                    .map(|a| PartitionRangeAssignment {
                        start_partition: a.start_partition,
                        end_partition: a.end_partition,
                        server: a
                            .server_ids
                            .into_iter()
                            .filter_map(|id| {
                                self.cluster_manager.get_server_by_id(&id).map(|node| {
                                    ShuffleServerId {
                                        id: node.id,
                                        ip: node.ip,
                                        port: node.grpc_port,
                                        netty_port: node.netty_port,
                                        jetty_port: node.http_port,
                                    }
                                })
                            })
                            .collect(),
                    })
                    .collect();

                Ok(Response::new(GetShuffleAssignmentsResponse {
                    status: StatusCode::Success.into(),
                    assignments,
                    ret_msg: "".to_string(),
                }))
            }
            Err(e) => {
                warn!("Failed to get shuffle assignments: {:?}", e);
                Ok(Response::new(GetShuffleAssignmentsResponse {
                    status: StatusCode::InternalError.into(),
                    assignments: vec![],
                    ret_msg: e.to_string(),
                }))
            }
        }
    }

    // ==================== 4. heartbeat ====================

    async fn heartbeat(
        &self,
        request: Request<ShuffleServerHeartBeatRequest>,
    ) -> Result<Response<ShuffleServerHeartBeatResponse>, Status> {
        let inner = request.into_inner();

        let server_id = inner
            .server_id
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("server_id is required"))?;

        let heartbeat_info = HeartbeatInfo {
            server_id: server_id.id.clone(),
            ip: server_id.ip.clone(),
            grpc_port: server_id.port,
            netty_port: server_id.netty_port,
            http_port: server_id.jetty_port,
            used_memory: inner.used_memory,
            available_memory: inner.available_memory,
            pre_allocated_memory: inner.pre_allocated_memory,
            event_num_in_flush: inner.event_num_in_flush,
            tags: inner.tags,
            is_healthy: inner.is_healthy.unwrap_or(true),
            status: server_status_from_i32(inner.status),
            storage_info: inner
                .storage_info
                .into_iter()
                .map(|(k, v)| {
                    let media = match v.storage_media {
                        1 => crate::cluster::server_node::StorageMedia::Hdd,
                        2 => crate::cluster::server_node::StorageMedia::Ssd,
                        3 => crate::cluster::server_node::StorageMedia::Hdfs,
                        4 => crate::cluster::server_node::StorageMedia::ObjectStore,
                        _ => crate::cluster::server_node::StorageMedia::StorageTypeUnknown,
                    };
                    let status = match v.status {
                        1 => crate::cluster::server_node::StorageStatus::Normal,
                        2 => crate::cluster::server_node::StorageStatus::Unhealthy,
                        3 => crate::cluster::server_node::StorageStatus::Overused,
                        _ => crate::cluster::server_node::StorageStatus::StorageStatusUnknown,
                    };
                    (
                        k,
                        crate::cluster::server_node::StorageInfo {
                            mount_point: v.mount_point,
                            storage_media: media,
                            capacity: v.capacity,
                            used_bytes: v.used_bytes,
                            status,
                        },
                    )
                })
                .collect(),
            version: inner.version,
            git_commit_id: inner.git_commit_id,
            start_time_ms: inner.start_time_ms,
        };

        self.cluster_manager.handle_heartbeat(heartbeat_info);

        Ok(Response::new(ShuffleServerHeartBeatResponse {
            status: StatusCode::Success.into(),
            ret_msg: "".to_string(),
        }))
    }

    // ==================== 5. getShuffleDataStorageInfo ====================

    async fn get_shuffle_data_storage_info(
        &self,
        _request: Request<()>,
    ) -> Result<Response<GetShuffleDataStorageInfoResponse>, Status> {
        let config = self.cluster_manager.config();
        let default_path = config
            .default_remote_storage_path
            .as_ref()
            .cloned()
            .unwrap_or_default();

        Ok(Response::new(GetShuffleDataStorageInfoResponse {
            storage: "LOCALFILE".to_string(),
            storage_path: default_path,
            storage_pattern: "".to_string(),
        }))
    }

    // ==================== 6. checkServiceAvailable ====================

    async fn check_service_available(
        &self,
        _request: Request<()>,
    ) -> Result<Response<CheckServiceAvailableResponse>, Status> {
        let server_num = self.cluster_manager.get_shuffle_server_num();
        let available = server_num > 0;

        Ok(Response::new(CheckServiceAvailableResponse {
            status: StatusCode::Success.into(),
            available,
        }))
    }

    // ==================== 7. appHeartbeat ====================

    async fn app_heartbeat(
        &self,
        request: Request<AppHeartBeatRequest>,
    ) -> Result<Response<AppHeartBeatResponse>, Status> {
        let inner = request.into_inner();
        self.cluster_manager.app_heartbeat(&inner.app_id);

        Ok(Response::new(AppHeartBeatResponse {
            status: StatusCode::Success.into(),
            ret_msg: "".to_string(),
        }))
    }

    // ==================== 8. reportClientOperation ====================

    async fn report_client_operation(
        &self,
        request: Request<ReportShuffleClientOpRequest>,
    ) -> Result<Response<ReportShuffleClientOpResponse>, Status> {
        let inner = request.into_inner();
        debug!(
            "Client operation reported: host={}, port={}, op={}",
            inner.client_host, inner.client_port, inner.operation
        );

        Ok(Response::new(ReportShuffleClientOpResponse {
            status: StatusCode::Success.into(),
            ret_msg: "".to_string(),
        }))
    }

    // ==================== 9. registerApplicationInfo ====================

    async fn register_application_info(
        &self,
        request: Request<ApplicationInfoRequest>,
    ) -> Result<Response<ApplicationInfoResponse>, Status> {
        let inner = request.into_inner();
        self.cluster_manager
            .register_application(inner.app_id, inner.user);

        Ok(Response::new(ApplicationInfoResponse {
            status: StatusCode::Success.into(),
            ret_msg: "".to_string(),
        }))
    }

    // ==================== 10. accessCluster ====================

    async fn access_cluster(
        &self,
        request: Request<AccessClusterRequest>,
    ) -> Result<Response<AccessClusterResponse>, Status> {
        let inner = request.into_inner();

        // Check if there are available servers matching the required tags
        let available_servers = self.cluster_manager.get_shuffle_server_list();
        let has_matching_server = if inner.tags.is_empty() {
            !available_servers.is_empty()
        } else {
            available_servers
                .iter()
                .any(|server| inner.tags.iter().all(|tag| server.tags.contains(tag)))
        };

        if has_matching_server {
            let uuid = uuid::Uuid::new_v4().to_string();
            Ok(Response::new(AccessClusterResponse {
                status: StatusCode::Success.into(),
                ret_msg: "".to_string(),
                uuid,
            }))
        } else {
            Ok(Response::new(AccessClusterResponse {
                status: StatusCode::AccessDenied.into(),
                ret_msg: "No available shuffle server matches the required tags".to_string(),
                uuid: "".to_string(),
            }))
        }
    }

    // ==================== 11. fetchClientConf ====================

    async fn fetch_client_conf(
        &self,
        _request: Request<()>,
    ) -> Result<Response<FetchClientConfResponse>, Status> {
        let config = self.cluster_manager.config();
        let client_conf: Vec<ClientConfItem> = config
            .client_conf
            .iter()
            .map(|(k, v)| ClientConfItem {
                key: k.clone(),
                value: v.clone(),
            })
            .collect();

        Ok(Response::new(FetchClientConfResponse {
            status: StatusCode::Success.into(),
            ret_msg: "".to_string(),
            client_conf,
        }))
    }

    // ==================== 12. fetchRemoteStorage ====================

    async fn fetch_remote_storage(
        &self,
        request: Request<FetchRemoteStorageRequest>,
    ) -> Result<Response<FetchRemoteStorageResponse>, Status> {
        let _inner = request.into_inner();
        let config = self.cluster_manager.config();

        let remote_storage =
            config
                .default_remote_storage_path
                .as_ref()
                .map(|path| RemoteStorage {
                    path: path.clone(),
                    remote_storage_conf: vec![],
                });

        Ok(Response::new(FetchRemoteStorageResponse {
            status: StatusCode::Success.into(),
            remote_storage,
        }))
    }
}
