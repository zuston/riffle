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

use log::{debug, warn};
use tonic::{Request, Response, Status};

use crate::application::ApplicationManager;
use crate::cluster::cluster_manager::{AssignmentRequest, NodeHeartbeatInfo};
use crate::cluster::server_node::ShuffleServerNode;
use crate::cluster::ClusterManagerRef;
use crate::grpc::protobuf::uniffle::coordinator_server_server::CoordinatorServer;
use crate::grpc::protobuf::uniffle::*;

/// Type alias for StatusCode from protobuf
type StatusCode = crate::grpc::protobuf::uniffle::StatusCode;

/// Default implementation of CoordinatorServer
#[derive(Clone)]
pub struct DefaultCoordinatorServer {
    cluster_manager: ClusterManagerRef,
    application_manager: ApplicationManager,
}

impl DefaultCoordinatorServer {
    pub fn new(
        cluster_manager: &ClusterManagerRef,
        application_manager: &ApplicationManager,
    ) -> Self {
        Self {
            cluster_manager: cluster_manager.clone(),
            application_manager: application_manager.clone(),
        }
    }

    fn parse_assignment_request(
        &self,
        request: GetShuffleServerRequest,
    ) -> Result<AssignmentRequest, crate::cluster::assignment::AssignmentError> {
        use crate::cluster::assignment::AssignmentError;

        if request.application_id.is_empty() {
            return Err(AssignmentError::InvalidParameters {
                message: "application_id must not be empty".to_string(),
            });
        }
        if request.shuffle_id < 0 {
            return Err(AssignmentError::InvalidParameters {
                message: "shuffle_id must be non-negative".to_string(),
            });
        }

        let partition_num = positive_i32("partition_num", request.partition_num)?;

        Ok(AssignmentRequest {
            app_id: request.application_id,
            shuffle_id: request.shuffle_id,
            partition_num,
            partition_num_per_range: positive_i32(
                "partition_num_per_range",
                request.partition_num_per_range,
            )?,
            data_replica: positive_i32("data_replica", request.data_replica)?,
            required_tags: request.require_tags,
            required_server_num: usize::try_from(request.assignment_shuffle_server_number)
                .ok()
                .filter(|value| *value > 0),
            estimate_task_concurrency: usize::try_from(request.estimate_task_concurrency)
                .unwrap_or_default(),
            exclusive_server_ids: request.faulty_server_ids.into_iter().collect(),
        })
    }
}

fn positive_i32(
    field: &str,
    value: i32,
) -> Result<usize, crate::cluster::assignment::AssignmentError> {
    if value <= 0 {
        return Err(
            crate::cluster::assignment::AssignmentError::InvalidParameters {
                message: format!("{field} must be positive"),
            },
        );
    }
    usize::try_from(value).map_err(|_| {
        crate::cluster::assignment::AssignmentError::InvalidParameters {
            message: format!("{field} exceeds the platform usize range"),
        }
    })
}

fn assignment_error_response(
    error: crate::cluster::assignment::AssignmentError,
) -> GetShuffleAssignmentsResponse {
    let status = match &error {
        crate::cluster::assignment::AssignmentError::InvalidParameters { .. } => {
            StatusCode::InvalidRequest
        }
        _ => StatusCode::InternalError,
    };
    GetShuffleAssignmentsResponse {
        status: status.into(),
        assignments: vec![],
        ret_msg: error.to_string(),
    }
}

fn non_negative_i32(field: &str, value: i32) -> Result<usize, Status> {
    usize::try_from(value)
        .map_err(|_| Status::invalid_argument(format!("{field} must be non-negative")))
}

fn non_negative_i64(field: &str, value: i64) -> Result<usize, Status> {
    usize::try_from(value)
        .map_err(|_| Status::invalid_argument(format!("{field} must be non-negative")))
}

fn shuffle_server_id(node: &ShuffleServerNode) -> Result<ShuffleServerId, Status> {
    Ok(ShuffleServerId {
        id: node.id.clone(),
        ip: node.ip.clone(),
        port: i32::try_from(node.grpc_port)
            .map_err(|_| Status::internal("grpc port exceeds i32 range"))?,
        netty_port: i32::try_from(node.netty_port)
            .map_err(|_| Status::internal("urpc port exceeds i32 range"))?,
        jetty_port: i32::try_from(node.http_port)
            .map_err(|_| Status::internal("http port exceeds i32 range"))?,
    })
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
        let servers = self.cluster_manager.list_all();

        let server_ids = servers
            .iter()
            .map(shuffle_server_id)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Response::new(GetShuffleServerListResponse {
            servers: server_ids,
        }))
    }

    // ==================== 2. getShuffleServerNum ====================

    async fn get_shuffle_server_num(
        &self,
        _request: Request<()>,
    ) -> Result<Response<GetShuffleServerNumResponse>, Status> {
        let num = self.cluster_manager.list_all().len();
        Ok(Response::new(GetShuffleServerNumResponse {
            num: num as i32,
        }))
    }

    // ==================== 3. getShuffleAssignments ====================

    async fn get_shuffle_assignments(
        &self,
        request: Request<GetShuffleServerRequest>,
    ) -> Result<Response<GetShuffleAssignmentsResponse>, Status> {
        let inner = request.into_inner();
        let assignment_request = match self.parse_assignment_request(inner) {
            Ok(request) => request,
            Err(error) => return Ok(Response::new(assignment_error_response(error))),
        };

        match self.cluster_manager.assign(assignment_request) {
            Ok(result) => {
                let server_nodes = result.servers;
                let assignments: Result<Vec<PartitionRangeAssignment>, Status> = result
                    .assignments
                    .into_iter()
                    .map(|assignment| {
                        let servers = assignment
                            .server_ids
                            .into_iter()
                            .map(|id| {
                                let node = server_nodes.get(&id).ok_or_else(|| {
                                    Status::internal(format!(
                                        "assignment references unknown shuffle server {id}"
                                    ))
                                })?;
                                shuffle_server_id(node)
                            })
                            .collect::<Result<Vec<_>, Status>>()?;
                        Ok(PartitionRangeAssignment {
                            start_partition: assignment.start_partition,
                            end_partition: assignment.end_partition,
                            server: servers,
                        })
                    })
                    .collect();

                Ok(Response::new(GetShuffleAssignmentsResponse {
                    status: StatusCode::Success.into(),
                    assignments: assignments?,
                    ret_msg: "".to_string(),
                }))
            }
            Err(e) => {
                warn!("Failed to get shuffle assignments: {e:?}");
                Ok(Response::new(assignment_error_response(e)))
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
        if server_id.id.is_empty() {
            return Err(Status::invalid_argument("server_id.id must not be empty"));
        }
        if server_id.port <= 0 {
            return Err(Status::invalid_argument("server_id.port must be positive"));
        }

        let heartbeat_info = NodeHeartbeatInfo {
            server_id: server_id.id.clone(),
            ip: server_id.ip.clone(),
            grpc_port: non_negative_i32("server_id.port", server_id.port)?,
            urpc_port: non_negative_i32("server_id.netty_port", server_id.netty_port)?,
            http_port: non_negative_i32("server_id.jetty_port", server_id.jetty_port)?,
            used_memory: non_negative_i64("used_memory", inner.used_memory)?,
            free_memory: non_negative_i64("available_memory", inner.available_memory)?,
            reserved_memory: non_negative_i64("pre_allocated_memory", inner.pre_allocated_memory)?,
            event_num_in_flush: non_negative_i32("event_num_in_flush", inner.event_num_in_flush)?,
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

        self.cluster_manager.heartbeat(heartbeat_info);

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
        Err(Status::unimplemented(
            "getShuffleDataStorageInfo is not supported",
        ))
    }

    // ==================== 6. checkServiceAvailable ====================

    async fn check_service_available(
        &self,
        _request: Request<()>,
    ) -> Result<Response<CheckServiceAvailableResponse>, Status> {
        let server_num = self.cluster_manager.list_all().len();
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
        if inner.app_id.is_empty() {
            return Err(Status::invalid_argument("app_id must not be empty"));
        }
        self.application_manager.heartbeat(&inner.app_id);

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
        if inner.app_id.is_empty() {
            return Err(Status::invalid_argument("app_id must not be empty"));
        }
        self.application_manager.register(
            inner.app_id,
            inner.user,
            inner.version,
            inner.git_commit_id,
        );

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
        let has_matching_server = !self.cluster_manager.list_available(&inner.tags).is_empty();

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
        Ok(Response::new(FetchClientConfResponse {
            status: StatusCode::Success.into(),
            ret_msg: "".to_string(),
            client_conf: vec![],
        }))
    }

    async fn fetch_client_conf_v2(
        &self,
        _request: Request<FetchClientConfRequest>,
    ) -> Result<Response<FetchClientConfResponse>, Status> {
        Ok(Response::new(FetchClientConfResponse {
            status: StatusCode::Success.into(),
            ret_msg: "".to_string(),
            client_conf: vec![],
        }))
    }

    // ==================== 12. fetchRemoteStorage ====================

    async fn fetch_remote_storage(
        &self,
        _request: Request<FetchRemoteStorageRequest>,
    ) -> Result<Response<FetchRemoteStorageResponse>, Status> {
        Ok(Response::new(FetchRemoteStorageResponse {
            status: StatusCode::Success.into(),
            remote_storage: None,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::ClusterManager;
    use crate::config::Config;

    fn service() -> DefaultCoordinatorServer {
        let cluster_manager = ClusterManager::new(&Config::default());
        let application_manager = ApplicationManager::default();
        DefaultCoordinatorServer::new(&cluster_manager, &application_manager)
    }

    fn valid_assignment_request() -> GetShuffleServerRequest {
        GetShuffleServerRequest {
            application_id: "app-1".to_string(),
            shuffle_id: 1,
            partition_num: 10,
            partition_num_per_range: 1,
            data_replica: 1,
            assignment_shuffle_server_number: -1,
            estimate_task_concurrency: -1,
            faulty_server_ids: vec!["faulty-1".to_string()],
            ..Default::default()
        }
    }

    #[test]
    fn parses_uniffle_default_assignment_sentinels_without_unsigned_wraparound() {
        let request = service()
            .parse_assignment_request(valid_assignment_request())
            .unwrap();

        assert_eq!(request.required_server_num, None);
        assert_eq!(request.estimate_task_concurrency, 0);
        assert!(request.exclusive_server_ids.contains("faulty-1"));
    }

    #[test]
    fn rejects_negative_partition_fields() {
        let coordinator = service();
        for request in [
            valid_assignment_request(),
            valid_assignment_request(),
            valid_assignment_request(),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, mut request)| {
            match index {
                0 => request.partition_num = -1,
                1 => request.partition_num_per_range = -1,
                _ => request.data_replica = -1,
            }
            request
        }) {
            assert!(coordinator.parse_assignment_request(request).is_err());
        }
    }
}
