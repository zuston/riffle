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

use crate::channel_pool::{connect_channel, GrpcChannelPool, GrpcClientSettings};
use crate::error::{ensure_success, RemoteStatus};
use crate::types::{as_i32, PartitionRoute};
use crate::{
    ApplicationSpec, DriverConfig, PartitionId, RemoteStorageSpec, RiffleError, ShuffleHandle,
    ShuffleId, ShuffleServer, ShuffleSpec,
};
use futures::future::join_all;
use riffle_proto::uniffle::coordinator_server_client::CoordinatorServerClient;
use riffle_proto::uniffle::{
    AccessClusterRequest, AppHeartBeatRequest, ApplicationInfoRequest, GetShuffleServerRequest,
    PartitionRangeAssignment, RemoteStorage, RemoteStorageConfItem, ShufflePartitionRange,
    ShuffleRegisterRequest, ShuffleUnregisterRequest,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Weak};
use tokio::sync::{watch, Mutex};
use tokio::task::JoinHandle;
use tonic::transport::Channel;
use tracing::warn;

#[derive(Clone, Debug)]
pub struct Driver {
    config: DriverConfig,
    coordinator_endpoint: String,
    coordinator_channel: Channel,
}

impl Driver {
    pub async fn connect(config: DriverConfig) -> Result<Self, RiffleError> {
        config.validate()?;
        let mut errors = Vec::new();
        let endpoints = config.coordinator_endpoints.clone();
        for endpoint in endpoints {
            match connect_channel(
                &endpoint,
                config.connect_timeout,
                config.request_timeout,
                "connect_coordinator",
            )
            .await
            {
                Ok(channel) => {
                    return Ok(Self {
                        config,
                        coordinator_endpoint: endpoint,
                        coordinator_channel: channel,
                    });
                }
                Err(error) => errors.push(error.to_string()),
            }
        }
        Err(RiffleError::NoAvailableCoordinator { errors })
    }

    pub async fn open_application(
        &self,
        spec: ApplicationSpec,
    ) -> Result<ApplicationSession, RiffleError> {
        let mut client = self.coordinator_client();
        let access = client
            .access_cluster(AccessClusterRequest {
                access_id: self.config.access_id.clone(),
                tags: self.config.required_tags.clone(),
                extra_properties: self.config.access_properties.clone().into_iter().collect(),
                user: spec.user.clone(),
            })
            .await
            .map_err(|error| self.coordinator_transport("access_cluster", error))?
            .into_inner();
        ensure_success("access_cluster", access.status, access.ret_msg)?;

        let registration = client
            .register_application_info(ApplicationInfoRequest {
                app_id: spec.application_id.as_str().to_string(),
                user: spec.user.clone(),
                version: spec.version.clone(),
                git_commit_id: spec.git_commit_id.clone(),
            })
            .await
            .map_err(|error| self.coordinator_transport("register_application", error))?
            .into_inner();
        ensure_success(
            "register_application",
            registration.status,
            registration.ret_msg,
        )?;

        ApplicationSession::new(
            self.config.clone(),
            self.coordinator_endpoint.clone(),
            self.coordinator_channel.clone(),
            spec,
        )
        .await
    }

    fn coordinator_client(&self) -> CoordinatorServerClient<Channel> {
        CoordinatorServerClient::new(self.coordinator_channel.clone())
            .max_encoding_message_size(self.config.max_encoding_message_size)
            .max_decoding_message_size(self.config.max_decoding_message_size)
    }

    fn coordinator_transport(&self, operation: &'static str, error: tonic::Status) -> RiffleError {
        RiffleError::Transport {
            operation,
            endpoint: self.coordinator_endpoint.clone(),
            message: error.to_string(),
        }
    }
}

pub struct ApplicationSession {
    inner: Arc<SessionInner>,
}

struct SessionInner {
    config: DriverConfig,
    coordinator_endpoint: String,
    coordinator_channel: Channel,
    application: ApplicationSpec,
    channel_pool: GrpcChannelPool,
    lifecycle_guard: Mutex<()>,
    state: Mutex<SessionState>,
    shutdown_tx: watch::Sender<bool>,
    heartbeat_task: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Default)]
struct SessionState {
    closed: bool,
    shuffles: BTreeMap<ShuffleId, ShuffleHandle>,
}

impl ApplicationSession {
    async fn new(
        config: DriverConfig,
        coordinator_endpoint: String,
        coordinator_channel: Channel,
        application: ApplicationSpec,
    ) -> Result<Self, RiffleError> {
        let settings = GrpcClientSettings {
            connect_timeout: config.connect_timeout,
            request_timeout: config.request_timeout,
            max_encoding_message_size: config.max_encoding_message_size,
            max_decoding_message_size: config.max_decoding_message_size,
        };
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let inner = Arc::new(SessionInner {
            config,
            coordinator_endpoint,
            coordinator_channel,
            application,
            channel_pool: GrpcChannelPool::new(settings),
            lifecycle_guard: Mutex::new(()),
            state: Mutex::new(SessionState::default()),
            shutdown_tx,
            heartbeat_task: Mutex::new(None),
        });
        let weak = Arc::downgrade(&inner);
        let task = tokio::spawn(heartbeat_loop(weak, shutdown_rx));
        *inner.heartbeat_task.lock().await = Some(task);
        Ok(Self { inner })
    }

    pub fn application_id(&self) -> &crate::ApplicationId {
        &self.inner.application.application_id
    }

    pub async fn create_shuffle(&self, spec: ShuffleSpec) -> Result<ShuffleHandle, RiffleError> {
        spec.validate()?;
        let _guard = self.inner.lifecycle_guard.lock().await;
        {
            let state = self.inner.state.lock().await;
            if state.closed {
                return Err(RiffleError::Closed);
            }
            if state.shuffles.contains_key(&spec.shuffle_id) {
                return Err(RiffleError::InvalidArgument(format!(
                    "shuffle {} is already registered in this application session",
                    spec.shuffle_id
                )));
            }
        }

        let routes = self.fetch_assignments(&spec).await?;
        let handle = ShuffleHandle::new(
            self.inner.application.application_id.clone(),
            spec.shuffle_id,
            spec.partition_count,
            spec.partition_range_size,
            spec.data_replica,
            spec.block_id_layout,
            routes,
        )?;

        let registrations = registrations_by_server(&handle);
        let results = join_all(
            registrations
                .iter()
                .map(|(server, ranges)| self.register_on_server(server, ranges, &spec)),
        )
        .await;
        if let Some(error) = results.into_iter().find_map(Result::err) {
            let _ = join_all(
                registrations
                    .keys()
                    .map(|server| self.unregister_on_server(server, spec.shuffle_id)),
            )
            .await;
            return Err(error);
        }

        self.inner
            .state
            .lock()
            .await
            .shuffles
            .insert(spec.shuffle_id, handle.clone());
        Ok(handle)
    }

    pub async fn close_shuffle(&self, handle: &ShuffleHandle) -> Result<(), RiffleError> {
        let _guard = self.inner.lifecycle_guard.lock().await;
        self.close_shuffle_unlocked(handle).await
    }

    pub async fn close(self) -> Result<(), RiffleError> {
        let _guard = self.inner.lifecycle_guard.lock().await;
        let handles = self
            .inner
            .state
            .lock()
            .await
            .shuffles
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut first_error = None;
        for handle in handles {
            if let Err(error) = self.close_shuffle_unlocked(&handle).await {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        self.inner.state.lock().await.closed = true;
        let _ = self.inner.shutdown_tx.send(true);
        if let Some(task) = self.inner.heartbeat_task.lock().await.take() {
            let _ = task.await;
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn fetch_assignments(
        &self,
        spec: &ShuffleSpec,
    ) -> Result<Vec<PartitionRoute>, RiffleError> {
        let required_server_count = match spec.required_server_count {
            Some(value) => as_i32("required_server_count", value)?,
            None => -1,
        };
        let estimate_task_concurrency =
            as_i32("estimate_task_concurrency", spec.estimate_task_concurrency)?;
        let mut client = self.coordinator_client();
        let response = client
            .get_shuffle_assignments(GetShuffleServerRequest {
                client_host: self.inner.config.client_host.clone(),
                client_port: self.inner.config.client_port.clone(),
                client_property: self.inner.config.client_property.clone(),
                application_id: self.inner.application.application_id.as_str().to_string(),
                shuffle_id: spec.shuffle_id.value(),
                partition_num: as_i32("partition_count", spec.partition_count)?,
                partition_num_per_range: as_i32("partition_range_size", spec.partition_range_size)?,
                data_replica: as_i32("data_replica", spec.data_replica)?,
                require_tags: self.inner.config.required_tags.clone(),
                assignment_shuffle_server_number: required_server_count,
                estimate_task_concurrency,
                faulty_server_ids: Vec::new(),
            })
            .await
            .map_err(|error| self.coordinator_transport("get_shuffle_assignments", error))?
            .into_inner();
        ensure_success("get_shuffle_assignments", response.status, response.ret_msg)?;
        response
            .assignments
            .into_iter()
            .map(route_from_proto)
            .collect()
    }

    async fn register_on_server(
        &self,
        server: &ShuffleServer,
        ranges: &[ShufflePartitionRange],
        spec: &ShuffleSpec,
    ) -> Result<(), RiffleError> {
        let channel = self.inner.channel_pool.channel(server).await?;
        let mut client = self.inner.channel_pool.client(channel);
        let response = client
            .register_shuffle(ShuffleRegisterRequest {
                app_id: self.inner.application.application_id.as_str().to_string(),
                shuffle_id: spec.shuffle_id.value(),
                partition_ranges: ranges.to_vec(),
                remote_storage: spec.remote_storage.as_ref().map(remote_storage_to_proto),
                user: self.inner.application.user.clone(),
                shuffle_data_distribution: spec.data_distribution.proto_value(),
                max_concurrency_per_partition_to_write: as_i32(
                    "max_concurrency_per_partition_to_write",
                    spec.max_concurrency_per_partition_to_write,
                )?,
                merge_context: None,
                properties: spec.properties.clone().into_iter().collect(),
            })
            .await
            .map_err(|error| RiffleError::Transport {
                operation: "register_shuffle",
                endpoint: server.grpc_endpoint(),
                message: error.to_string(),
            })?
            .into_inner();
        let status = RemoteStatus::from(response.status);
        if matches!(status, RemoteStatus::Success | RemoteStatus::DoubleRegister) {
            Ok(())
        } else {
            Err(RiffleError::Remote {
                operation: "register_shuffle",
                status,
                message: response.ret_msg,
            })
        }
    }

    async fn close_shuffle_unlocked(&self, handle: &ShuffleHandle) -> Result<(), RiffleError> {
        if handle.application_id != self.inner.application.application_id {
            return Err(RiffleError::InvalidArgument(
                "shuffle handle belongs to a different application".to_string(),
            ));
        }
        let registered_handle = self
            .inner
            .state
            .lock()
            .await
            .shuffles
            .get(&handle.shuffle_id)
            .cloned();
        let Some(registered_handle) = registered_handle else {
            return Ok(());
        };

        let results = join_all(
            registered_handle
                .servers()
                .iter()
                .map(|server| self.unregister_on_server(server, handle.shuffle_id)),
        )
        .await;
        if let Some(error) = results.into_iter().find_map(Result::err) {
            return Err(error);
        }
        self.inner
            .state
            .lock()
            .await
            .shuffles
            .remove(&handle.shuffle_id);
        Ok(())
    }

    async fn unregister_on_server(
        &self,
        server: &ShuffleServer,
        shuffle_id: ShuffleId,
    ) -> Result<(), RiffleError> {
        let channel = self.inner.channel_pool.channel(server).await?;
        let mut client = self.inner.channel_pool.client(channel);
        let response = client
            .unregister_shuffle(ShuffleUnregisterRequest {
                app_id: self.inner.application.application_id.as_str().to_string(),
                shuffle_id: shuffle_id.value(),
            })
            .await
            .map_err(|error| RiffleError::Transport {
                operation: "unregister_shuffle",
                endpoint: server.grpc_endpoint(),
                message: error.to_string(),
            })?
            .into_inner();
        let status = RemoteStatus::from(response.status);
        if matches!(
            status,
            RemoteStatus::Success | RemoteStatus::NoRegister | RemoteStatus::AppNotFound
        ) {
            Ok(())
        } else {
            Err(RiffleError::Remote {
                operation: "unregister_shuffle",
                status,
                message: response.ret_msg,
            })
        }
    }

    fn coordinator_client(&self) -> CoordinatorServerClient<Channel> {
        CoordinatorServerClient::new(self.inner.coordinator_channel.clone())
            .max_encoding_message_size(self.inner.config.max_encoding_message_size)
            .max_decoding_message_size(self.inner.config.max_decoding_message_size)
    }

    fn coordinator_transport(&self, operation: &'static str, error: tonic::Status) -> RiffleError {
        RiffleError::Transport {
            operation,
            endpoint: self.inner.coordinator_endpoint.clone(),
            message: error.to_string(),
        }
    }
}

impl Drop for ApplicationSession {
    fn drop(&mut self) {
        let _ = self.inner.shutdown_tx.send(true);
    }
}

async fn heartbeat_loop(inner: Weak<SessionInner>, mut shutdown_rx: watch::Receiver<bool>) {
    let Some(session) = inner.upgrade() else {
        return;
    };
    let mut interval = tokio::time::interval(session.config.heartbeat_interval);
    drop(session);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let Some(session) = inner.upgrade() else {
                    return;
                };
                let mut heartbeat_shutdown_rx = shutdown_rx.clone();
                tokio::select! {
                    _ = heartbeat_once(&session) => {}
                    _ = shutdown_requested(&mut heartbeat_shutdown_rx) => return,
                }
            }
            _ = shutdown_requested(&mut shutdown_rx) => return,
        }
    }
}

async fn shutdown_requested(shutdown_rx: &mut watch::Receiver<bool>) {
    while !*shutdown_rx.borrow() {
        if shutdown_rx.changed().await.is_err() {
            return;
        }
    }
}

async fn heartbeat_once(session: &SessionInner) {
    let mut coordinator = CoordinatorServerClient::new(session.coordinator_channel.clone())
        .max_encoding_message_size(session.config.max_encoding_message_size)
        .max_decoding_message_size(session.config.max_decoding_message_size);
    match coordinator
        .app_heartbeat(AppHeartBeatRequest {
            app_id: session.application.application_id.as_str().to_string(),
        })
        .await
    {
        Ok(response) => {
            let response = response.into_inner();
            if let Err(error) = ensure_success(
                "coordinator_app_heartbeat",
                response.status,
                response.ret_msg,
            ) {
                warn!(error = %error, "Riffle coordinator heartbeat failed");
            }
        }
        Err(error) => warn!(
            endpoint = %session.coordinator_endpoint,
            error = %error,
            "Riffle coordinator heartbeat transport failed"
        ),
    }

    let servers = session
        .state
        .lock()
        .await
        .shuffles
        .values()
        .flat_map(ShuffleHandle::servers)
        .collect::<std::collections::BTreeSet<_>>();
    join_all(servers.iter().map(|server| async move {
        let result = async {
            let channel = session.channel_pool.channel(server).await?;
            let mut client = session.channel_pool.client(channel);
            let response = client
                .app_heartbeat(AppHeartBeatRequest {
                    app_id: session.application.application_id.as_str().to_string(),
                })
                .await
                .map_err(|error| RiffleError::Transport {
                    operation: "shuffle_server_app_heartbeat",
                    endpoint: server.grpc_endpoint(),
                    message: error.to_string(),
                })?
                .into_inner();
            ensure_success(
                "shuffle_server_app_heartbeat",
                response.status,
                response.ret_msg,
            )
        }
        .await;
        if let Err(error) = result {
            warn!(server = %server.id, error = %error, "Riffle shuffle server heartbeat failed");
        }
    }))
    .await;
}

fn route_from_proto(route: PartitionRangeAssignment) -> Result<PartitionRoute, RiffleError> {
    let start = u32::try_from(route.start_partition).map_err(|_| {
        RiffleError::InvalidAssignment(format!("negative route start {}", route.start_partition))
    })?;
    let end = u32::try_from(route.end_partition).map_err(|_| {
        RiffleError::InvalidAssignment(format!("negative route end {}", route.end_partition))
    })?;
    let replicas = route
        .server
        .into_iter()
        .map(|server| {
            let grpc_port = u16::try_from(server.port).map_err(|_| {
                RiffleError::InvalidAssignment(format!(
                    "server {} has invalid gRPC port {}",
                    server.id, server.port
                ))
            })?;
            let urpc_port = u16::try_from(server.netty_port)
                .ok()
                .filter(|port| *port != 0);
            let http_port = u16::try_from(server.jetty_port)
                .ok()
                .filter(|port| *port != 0);
            Ok(ShuffleServer {
                id: server.id,
                host: server.ip,
                grpc_port,
                urpc_port,
                http_port,
            })
        })
        .collect::<Result<Vec<_>, RiffleError>>()?;
    Ok(PartitionRoute {
        start: PartitionId::new(start),
        end: PartitionId::new(end),
        replicas,
    })
}

fn registrations_by_server(
    handle: &ShuffleHandle,
) -> BTreeMap<ShuffleServer, Vec<ShufflePartitionRange>> {
    let mut registrations = BTreeMap::<ShuffleServer, Vec<ShufflePartitionRange>>::new();
    for route in &handle.routes {
        for server in &route.replicas {
            registrations
                .entry(server.clone())
                .or_default()
                .push(ShufflePartitionRange {
                    start: route.start.value() as i32,
                    end: route.end.value() as i32,
                });
        }
    }
    registrations
}

fn remote_storage_to_proto(storage: &RemoteStorageSpec) -> RemoteStorage {
    RemoteStorage {
        path: storage.path.clone(),
        remote_storage_conf: storage
            .properties
            .iter()
            .map(|(key, value)| RemoteStorageConfItem {
                key: key.clone(),
                value: value.clone(),
            })
            .collect(),
    }
}
