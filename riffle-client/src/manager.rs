pub mod shuffle_id_manager;
pub mod task_id_manager;
pub mod manager_client;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use futures::future::join_all;
use log::{info, warn};
use riffle_server::client_configs::ClientRssConf;
use riffle_server::ddashmap::DDashMap;
use riffle_server::grpc::protobuf::uniffle::{
    AppHeartBeatRequest, ShufflePartitionRange, ShuffleRegisterRequest,
    ShuffleUnregisterByAppIdRequest, ShuffleUnregisterRequest,
};
use riffle_server::scheduled_task::ScheduledTask;
use riffle_server::util::get_local_ip;
use tokio::net::TcpListener;
use tokio::sync::{watch, Mutex as AsyncMutex};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::configs::{
    ASSIGNMENT_SERVER_NUMBER, ASSIGNMENT_TAGS, COORDINATOR_QUORUM, HEARTBEAT_INTERVAL_SECONDS,
};
use crate::rpc::shuffle_manager_service_server::{
    ShuffleManagerService, ShuffleManagerServiceServer,
};
use crate::rpc_client::{
    CoordinatorQuorumClient, GetAssignmentContext, ServerClient, ServerConnOptions, ServerCoonPool,
};
use crate::{
    check_status, rpc, Endpoint, ShuffleHandle, ShuffleId, TaskId, MAX_MESSAGE_BYTES, TASK_ID_LIMIT,
};

pub struct ApplicationOptions {
    pub app_id: String,
    pub user: String,
    pub configs: HashMap<String, String>,
}

impl ApplicationOptions {
    pub fn new(app_id: String, user: String, configs: HashMap<String, String>) -> Self {
        Self {
            app_id,
            user,
            configs,
        }
    }
}

pub struct ShuffleManager {
    app_id: String,
    user: String,
    configs: ClientRssConf,
    manager_endpoint: Endpoint,
    shuffles: Arc<DDashMap<ShuffleId, RegisteredShuffle>>,
    shuffle_ids: Arc<DDashMap<String, ShuffleId>>,
    retired_shuffle_ids: Mutex<HashSet<ShuffleId>>,
    coordinator_client: CoordinatorQuorumClient,
    server_client_factory: Arc<ServerCoonPool>,
    lifecycle_lock: AsyncMutex<()>,
    coordinator_task: ScheduledTask,
    heartbeat_server_task: ScheduledTask,
    // close signal
    rpc_shutdown: watch::Sender<bool>,
    rpc_task: Mutex<Option<JoinHandle<()>>>,
}

impl ShuffleManager {
    pub async fn new(app_options: ApplicationOptions) -> Result<Self> {
        let ApplicationOptions {
            app_id,
            user,
            configs,
        } = app_options;
        let configs = ClientRssConf::from(configs);
        let quorum = configs
            .get(&COORDINATOR_QUORUM)
            .context(format!("{:?} is required", COORDINATOR_QUORUM.get_key()))?
            .into_iter()
            .map(|address| address.parse())
            .collect::<Result<Vec<Endpoint>>>()?;
        let coordinator_client = CoordinatorQuorumClient::new(quorum)?;
        let interval = Duration::from_secs(configs.get(&HEARTBEAT_INTERVAL_SECONDS).context(
            format!("{:?} is required", HEARTBEAT_INTERVAL_SECONDS.get_key()),
        )?);
        ensure!(!interval.is_zero(), "heartbeat interval must be positive");
        let listener = TcpListener::bind("0.0.0.0:0").await?;
        let manager_endpoint = Endpoint {
            host: get_local_ip()?.to_string(),
            grpc_port: listener.local_addr()?.port(),
        };
        coordinator_client.app_register(&app_id, &user).await?;

        let coordinator_task = ScheduledTask::new(interval, {
            let client = coordinator_client.clone();
            let app_id = app_id.clone();
            move || {
                let client = client.clone();
                let app_id = app_id.clone();
                async move {
                    if let Err(error) = client.app_heartbeat(&app_id).await {
                        warn!("Coordinator heartbeat failed for app {app_id}: {error:#}");
                    }
                }
            }
        });

        let shuffles = Arc::new(DDashMap::<ShuffleId, RegisteredShuffle>::default());
        let shuffle_ids = Arc::new(DDashMap::default());
        let shuffle_server_clients = Arc::new(ServerCoonPool::new());

        let heartbeat_server_task = ScheduledTask::new(interval, {
            let shuffles = shuffles.clone();
            let clients = shuffle_server_clients.clone();
            let app_id = app_id.clone();
            move || {
                let mut endpoints = HashSet::new();
                for shuffle in shuffles.iter() {
                    for assignment in shuffle.handle.assignment.assignments.iter() {
                        endpoints.extend(assignment.value().iter().cloned());
                    }
                }
                let clients = clients.clone();
                let app_id = app_id.clone();
                async move {
                    join_all(endpoints.into_iter().map(|endpoint| {
                        let clients = &clients;
                        let app_id = app_id.clone();
                        async move {
                            let result = async {
                                let mut client = clients.get_or_create_client(
                                    ServerConnOptions::new(endpoint.clone()),
                                )?;
                                let response = client
                                    .app_heartbeat(AppHeartBeatRequest {
                                        app_id: app_id.clone(),
                                    })
                                    .await?
                                    .into_inner();
                                check_status("appHeartbeat", response.status, &response.ret_msg)
                            }
                            .await;
                            if let Err(error) = result {
                                warn!(
                                    "Server heartbeat failed for app {app_id} at {}: {error:#}",
                                    endpoint.uri()
                                );
                            }
                        }
                    }))
                    .await;
                }
            }
        });

        let service = ManagerService {
            shuffles: shuffles.clone(),
            shuffle_ids: shuffle_ids.clone(),
        };
        let (rpc_shutdown, mut shutdown) = watch::channel(false);
        let rpc_task = tokio::spawn(async move {
            let result = Server::builder()
                .add_service(
                    ShuffleManagerServiceServer::new(service)
                        .max_encoding_message_size(MAX_MESSAGE_BYTES)
                        .max_decoding_message_size(MAX_MESSAGE_BYTES),
                )
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    let _ = shutdown.changed().await;
                })
                .await;
            if let Err(error) = result {
                warn!("Shuffle manager gRPC server failed: {error}");
            }
        });
        Ok(Self {
            app_id,
            user,
            configs,
            manager_endpoint,
            shuffles,
            shuffle_ids,
            retired_shuffle_ids: Mutex::new(HashSet::new()),
            coordinator_client,
            server_client_factory: shuffle_server_clients,
            lifecycle_lock: AsyncMutex::new(()),
            coordinator_task,
            heartbeat_server_task,
            rpc_shutdown,
            rpc_task: Mutex::new(Some(rpc_task)),
        })
    }

    pub fn get_shuffle_handle(&self, shuffle_id: ShuffleId) -> Result<ShuffleHandle> {
        self.shuffles
            .get(&shuffle_id)
            .map(|entry| entry.handle.clone())
            .with_context(|| format!("shuffle {shuffle_id} is not registered"))
    }

    /// Registers routing before writers and readers request handles over RPC.
    /// The engine supplies the same key for a writer stage and its downstream readers.
    /// Shuffle IDs must not be reused during the app lifetime.
    pub async fn register_shuffle(
        &self,
        shuffle_id: ShuffleId,
        shuffle_key: String,
        partition_count: u32,
    ) -> Result<ShuffleHandle> {
        let _guard = self.lifecycle_lock.lock().await;
        ensure!(!shuffle_key.is_empty(), "shuffle key must not be empty");
        ensure!(
            shuffle_id <= i32::MAX as usize,
            "shuffle ID exceeds server protocol i32 range"
        );
        ensure!(
            partition_count > 0 && partition_count <= 1 << 24,
            "partition count exceeds 24-bit block ID range or is zero"
        );
        ensure!(
            !self
                .retired_shuffle_ids
                .lock()
                .unwrap()
                .contains(&shuffle_id),
            "shuffle {shuffle_id} has been unregistered and cannot be reused"
        );
        if let Some(registered_id) = self.shuffle_ids.get(&shuffle_key) {
            ensure!(
                *registered_id == shuffle_id,
                "shuffle key {shuffle_key} is already registered with ID {}",
                *registered_id
            );
        }
        if let Some(shuffle) = self.shuffles.get(&shuffle_id) {
            ensure!(
                shuffle.shuffle_key == shuffle_key
                    && shuffle.handle.partition_count == partition_count,
                "shuffle {shuffle_id} is already registered with different metadata"
            );
            info!(
                "Shuffle {shuffle_id} is already registered for app {}",
                self.app_id
            );
            return Ok(shuffle.handle.clone());
        }
        let assignment = self
            .coordinator_client
            .get_assignment(GetAssignmentContext {
                app_id: self.app_id.clone(),
                shuffle_id,
                partition_count,
                required_tags: self.configs.get(&ASSIGNMENT_TAGS).unwrap(),
                required_server_number: self
                    .configs
                    .get(&ASSIGNMENT_SERVER_NUMBER)
                    .context("invalid assignment server number")?,
            })
            .await?;
        let mut ranges: HashMap<Endpoint, Vec<ShufflePartitionRange>> = HashMap::new();
        for entry in assignment.assignments.iter() {
            let partition = *entry.key() as i32;
            for endpoint in entry.value() {
                ranges
                    .entry(endpoint.clone())
                    .or_default()
                    .push(ShufflePartitionRange {
                        start: partition,
                        end: partition,
                    });
            }
        }
        let mut registrations = Vec::with_capacity(ranges.len());
        for (endpoint, partition_ranges) in ranges {
            let client = self
                .server_client_factory
                .get_or_create_client(ServerConnOptions::new(endpoint.clone()))?;
            registrations.push((
                endpoint,
                client,
                ShuffleRegisterRequest {
                    app_id: self.app_id.clone(),
                    shuffle_id: shuffle_id as i32,
                    partition_ranges,
                    user: self.user.clone(),
                    properties: self.configs.properties().clone(),
                    ..Default::default()
                },
            ));
        }
        let results = join_all(registrations.into_iter().map(
            |(endpoint, mut client, request)| async move {
                let response = client
                    .register_shuffle(request)
                    .await
                    .with_context(|| format!("register shuffle on {}", endpoint.uri()))?
                    .into_inner();
                check_status("registerShuffle", response.status, &response.ret_msg)
            },
        ))
        .await;
        // Keep partial registrations until app cleanup: asynchronous server purges
        // could otherwise delete a successful registration retry.
        results.into_iter().collect::<Result<Vec<_>>>()?;
        let handle = ShuffleHandle {
            app_id: self.app_id.clone(),
            shuffle_id,
            partition_count,
            manager_endpoint: self.manager_endpoint.clone(),
            assignment,
        };
        self.shuffles.insert(
            shuffle_id,
            RegisteredShuffle {
                shuffle_key: shuffle_key.clone(),
                handle: handle.clone(),
                tasks: Mutex::new(ShuffleTasks::default()),
            },
        );
        self.shuffle_ids.insert(shuffle_key, shuffle_id);
        Ok(handle)
    }

    pub async fn unregister_shuffle(&self, shuffle_id: ShuffleId) -> Result<()> {
        let _guard = self.lifecycle_lock.lock().await;
        let handle = self.get_shuffle_handle(shuffle_id)?;
        let endpoints: HashSet<_> = handle
            .assignment
            .assignments
            .iter()
            .flat_map(|entry| entry.value().clone())
            .collect();
        let clients = endpoints
            .into_iter()
            .map(|endpoint| {
                let client = self
                    .server_client_factory
                    .get_or_create_client(ServerConnOptions::new(endpoint.clone()))?;
                Ok((endpoint, client))
            })
            .collect::<Result<_>>()?;
        self.unregister_servers(shuffle_id as i32, clients).await?;
        if let Some((_, shuffle)) = self.shuffles.remove(&shuffle_id) {
            self.shuffle_ids.remove(&shuffle.shuffle_key);
            self.retired_shuffle_ids.lock().unwrap().insert(shuffle_id);
        }
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        let _guard = self.lifecycle_lock.lock().await;
        self.coordinator_task.close().await;
        self.heartbeat_server_task.close().await;
        let _ = self.rpc_shutdown.send(true);
        let task = self.rpc_task.lock().unwrap().take();
        if let Some(task) = task {
            task.await?;
        }
        let results = join_all(self.server_client_factory.clients().into_iter().map(
            |(endpoint, mut client)| async move {
                let response = client
                    .unregister_shuffle_by_app_id(ShuffleUnregisterByAppIdRequest {
                        app_id: self.app_id.clone(),
                    })
                    .await
                    .with_context(|| format!("unregister app on {}", endpoint.uri()))?
                    .into_inner();
                check_status(
                    "unregisterShuffleByAppId",
                    response.status,
                    &response.ret_msg,
                )
            },
        ))
        .await;
        results.into_iter().collect::<Result<Vec<_>>>()?;
        self.retired_shuffle_ids
            .lock()
            .unwrap()
            .extend(self.shuffles.iter().map(|entry| *entry.key()));
        self.shuffle_ids.clear();
        self.shuffles.clear();
        self.server_client_factory.clear();
        Ok(())
    }

    async fn unregister_servers(
        &self,
        shuffle_id: i32,
        clients: Vec<(Endpoint, ServerClient)>,
    ) -> Result<()> {
        let results = join_all(
            clients
                .into_iter()
                .map(|(endpoint, mut client)| async move {
                    let response = client
                        .unregister_shuffle(ShuffleUnregisterRequest {
                            app_id: self.app_id.clone(),
                            shuffle_id,
                        })
                        .await
                        .with_context(|| format!("unregister shuffle on {}", endpoint.uri()))?
                        .into_inner();
                    check_status("unregisterShuffle", response.status, &response.ret_msg)
                }),
        )
        .await;
        results.into_iter().collect::<Result<Vec<_>>>()?;
        Ok(())
    }
}

impl Drop for ShuffleManager {
    fn drop(&mut self) {
        let _ = self.rpc_shutdown.send(true);
    }
}

struct RegisteredShuffle {
    shuffle_key: String,
    handle: ShuffleHandle,
    tasks: Mutex<ShuffleTasks>,
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
struct MapAttempt {
    map_id: u64,
    attempt_id: u64,
}

#[derive(Default)]
struct ShuffleTasks {
    next_task_id: TaskId,
    task_ids: HashMap<MapAttempt, TaskId>,
    attempts: HashMap<TaskId, MapAttempt>,
    successful_tasks: HashMap<u64, TaskId>,
}

impl ShuffleTasks {
    fn allocate(&mut self, attempt: MapAttempt) -> Result<TaskId, Status> {
        if let Some(&task_id) = self.task_ids.get(&attempt) {
            return Ok(task_id);
        }
        if self.next_task_id >= TASK_ID_LIMIT {
            return Err(Status::resource_exhausted(
                "shuffle task ID range exhausted",
            ));
        }
        let task_id = self.next_task_id;
        self.next_task_id += 1;
        self.task_ids.insert(attempt, task_id);
        self.attempts.insert(task_id, attempt);
        Ok(task_id)
    }

    fn report_success(&mut self, task_id: TaskId) -> Result<(), Status> {
        let attempt = self
            .attempts
            .get(&task_id)
            .ok_or_else(|| Status::not_found(format!("task {task_id} is not registered")))?;
        let selected = self
            .successful_tasks
            .entry(attempt.map_id)
            .or_insert(task_id);
        // Completion order must not let an older attempt replace a newer successful one.
        if attempt.attempt_id > self.attempts[selected].attempt_id {
            *selected = task_id;
        }
        Ok(())
    }

    fn successful_task_ids(&self) -> Vec<u64> {
        let mut task_ids: Vec<_> = self
            .successful_tasks
            .values()
            .map(|&id| id as u64)
            .collect();
        task_ids.sort_unstable();
        task_ids
    }
}

// The RPC service shares metadata without retaining the manager's background tasks.
struct ManagerService {
    shuffles: Arc<DDashMap<ShuffleId, RegisteredShuffle>>,
    shuffle_ids: Arc<DDashMap<String, ShuffleId>>,

    // 要么是 string, 需要做唯一性映射
    // 要么直接是 usize, 就不需要做映射了

}

impl ManagerService {
    fn resolve_key(&self, shuffle_key: &str) -> Result<ShuffleId, Status> {
        if shuffle_key.is_empty() {
            return Err(Status::invalid_argument("shuffle key must not be empty"));
        }
        self.shuffle_ids
            .get(shuffle_key)
            .map(|entry| *entry.value())
            .ok_or_else(|| {
                Status::not_found(format!("shuffle key {shuffle_key} is not registered"))
            })
    }

    fn parse_shuffle_id(shuffle_id: u64) -> Result<ShuffleId, Status> {
        if shuffle_id > i32::MAX as u64 {
            return Err(Status::invalid_argument(
                "shuffle ID exceeds server protocol i32 range",
            ));
        }
        Ok(shuffle_id as ShuffleId)
    }

    fn assignment(
        handle: &ShuffleHandle,
        partition_id: u64,
    ) -> Result<rpc::PartitionAssignment, Status> {
        if partition_id >= u64::from(handle.partition_count) {
            return Err(Status::invalid_argument(format!(
                "partition {partition_id} is out of range"
            )));
        }
        let endpoint = handle
            .assignment
            .get_endpoint(partition_id as usize)
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(rpc::PartitionAssignment {
            partition_id: partition_id as i32,
            endpoint: Some(rpc::Endpoint {
                host: endpoint.host,
                grpc_port: endpoint.grpc_port.into(),
                urpc_port: None,
            }),
        })
    }
}

#[tonic::async_trait]
impl ShuffleManagerService for ManagerService {
    async fn get_write_shuffle_handle(
        &self,
        request: Request<rpc::GetWriteShuffleHandleRequest>,
    ) -> Result<Response<rpc::GetWriteShuffleHandleResponse>, Status> {
        use rpc::get_write_shuffle_handle_request::ShuffleIdentifier;

        let request = request.into_inner();
        let shuffle_id = match request.shuffle_identifier {
            Some(ShuffleIdentifier::ShuffleKey(key)) => self.resolve_key(&key)?,
            Some(ShuffleIdentifier::ShuffleId(id)) => Self::parse_shuffle_id(id)?,
            None => return Err(Status::invalid_argument("shuffle identifier is required")),
        };
        let shuffle = self
            .shuffles
            .get(&shuffle_id)
            .ok_or_else(|| Status::not_found(format!("shuffle {shuffle_id} is not registered")))?;
        let assignments = (0..shuffle.handle.partition_count)
            .map(|partition| Self::assignment(&shuffle.handle, partition.into()))
            .collect::<Result<_, _>>()?;
        let task_id = shuffle
            .tasks
            .lock()
            .map_err(|_| Status::internal("shuffle task state is poisoned"))?
            .allocate(MapAttempt {
                map_id: request.map_id,
                attempt_id: request.attempt_id,
            })?;
        Ok(Response::new(rpc::GetWriteShuffleHandleResponse {
            shuffle_id: shuffle_id as u64,
            assignments,
            task_id: task_id as u64,
        }))
    }

    async fn report_shuffle_result(
        &self,
        request: Request<rpc::ReportShuffleResultRequest>,
    ) -> Result<Response<rpc::ReportShuffleResultResponse>, Status> {
        let request = request.into_inner();
        let shuffle_id = Self::parse_shuffle_id(request.shuffle_id)?;
        if request.task_id >= TASK_ID_LIMIT as u64 {
            return Err(Status::invalid_argument(
                "task ID exceeds 23-bit block ID range",
            ));
        }
        let shuffle = self
            .shuffles
            .get(&shuffle_id)
            .ok_or_else(|| Status::not_found(format!("shuffle {shuffle_id} is not registered")))?;
        shuffle
            .tasks
            .lock()
            .map_err(|_| Status::internal("shuffle task state is poisoned"))?
            .report_success(request.task_id as TaskId)?;
        Ok(Response::new(rpc::ReportShuffleResultResponse {}))
    }

    async fn get_read_shuffle_handle(
        &self,
        request: Request<rpc::GetReadShuffleHandleRequest>,
    ) -> Result<Response<rpc::GetReadShuffleHandleResponse>, Status> {
        use rpc::get_read_shuffle_handle_request::ShuffleIdentifier;

        let request = request.into_inner();
        let shuffle_id = match request.shuffle_identifier {
            Some(ShuffleIdentifier::ShuffleKey(key)) => self.resolve_key(&key)?,
            Some(ShuffleIdentifier::ShuffleId(id)) => Self::parse_shuffle_id(id)?,
            None => return Err(Status::invalid_argument("shuffle identifier is required")),
        };
        let shuffle = self
            .shuffles
            .get(&shuffle_id)
            .ok_or_else(|| Status::not_found(format!("shuffle {shuffle_id} is not registered")))?;
        let mut partitions = request.partition_ids;
        partitions.sort_unstable();
        partitions.dedup();
        let assignments = partitions
            .into_iter()
            .map(|partition| Self::assignment(&shuffle.handle, partition))
            .collect::<Result<Vec<_>, _>>()?;
        // An empty partition list reads nothing. Otherwise return the shuffle-wide
        // winners; server block metadata determines which tasks wrote each partition.
        let task_ids = if assignments.is_empty() {
            Vec::new()
        } else {
            shuffle
                .tasks
                .lock()
                .map_err(|_| Status::internal("shuffle task state is poisoned"))?
                .successful_task_ids()
        };
        Ok(Response::new(rpc::GetReadShuffleHandleResponse {
            shuffle_id: shuffle_id as u64,
            assignments,
            task_ids,
        }))
    }
}
