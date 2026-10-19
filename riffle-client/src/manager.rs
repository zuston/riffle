pub mod shuffle_id_manager;
pub mod shuffle_task_manager;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use futures::future::join_all;
use log::{info, warn};
use riffle_server::client_configs::ClientRssConf;
use riffle_server::ddashmap::DDashMap;
use riffle_server::grpc::protobuf::uniffle::{
    AppHeartBeatRequest, ShuffleRegisterRequest, ShuffleUnregisterByAppIdRequest,
    ShuffleUnregisterRequest,
};
use riffle_server::scheduled_task::ScheduledTask;
use tokio::sync::Mutex as AsyncMutex;
use tonic::{Request, Response, Status};

use crate::configs::{
    ASSIGNMENT_SERVER_NUMBER, ASSIGNMENT_TAGS, COORDINATOR_QUORUM, HEARTBEAT_INTERVAL_SECONDS,
};
use crate::manager::shuffle_id_manager::{ShuffleIdManager, ShuffleKey};
use crate::manager::shuffle_task_manager::{ShuffleTaskManager, TaskKey};
use crate::rpc::shuffle_manager_service_server::ShuffleManagerService;
use crate::rpc_client::coordinator_rpc_client::{CoordinatorQuorumClient, GetAssignmentContext};
use crate::rpc_client::{ServerClient, ServerConnOptions, ServerCoonPool};
use crate::{ensure_success, rpc, ApplicationId, Endpoint, PartitionId, ShuffleHandle, ShuffleId};

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
    // assignment options
    assignment_tags: Vec<String>,
    assignment_server_num: i32,
    // shuffle meta info
    shuffle_id_manager: ShuffleIdManager,
    shuffles: Arc<DDashMap<ShuffleId, RegisteredShuffle>>,
    // connections
    coordinator_client: CoordinatorQuorumClient,
    server_conn: Arc<ServerCoonPool>,
    // heartbeat tasks
    heartbeat_coordinator_task: ScheduledTask,
    heartbeat_server_task: ScheduledTask,
    // global lock for app level
    lifecycle_lock: AsyncMutex<()>,
    closed: AtomicBool,
}

impl ShuffleManager {
    pub fn new(app_options: ApplicationOptions) -> Result<Self> {
        let ApplicationOptions {
            app_id,
            user,
            configs,
        } = app_options;
        ensure!(!app_id.is_empty(), "application ID is empty");
        let configs = ClientRssConf::from(configs);

        // set up the coordinator heartbeat task
        let coordinator_quorum = configs
            .get(&COORDINATOR_QUORUM)
            .context("missing or invalid riffle.client.coordinator.quorum")?
            .into_iter()
            .map(|address| address.parse())
            .collect::<Result<Vec<Endpoint>>>()?;
        let coordinator_client = CoordinatorQuorumClient::new(coordinator_quorum)?;
        let interval = configs
            .get(&HEARTBEAT_INTERVAL_SECONDS)
            .context("invalid riffle.client.heartbeat.intervalSeconds")?;
        ensure!(interval > 0, "heartbeat interval must be positive");
        let interval = Duration::from_secs(interval);
        let tags = configs
            .get(&ASSIGNMENT_TAGS)
            .context("invalid assignment tags")?;
        let server_num = configs
            .get(&ASSIGNMENT_SERVER_NUMBER)
            .context("invalid riffle.client.assignment.serverNumber")?;
        let coordinator_task = ScheduledTask::new(interval, {
            let client = coordinator_client.clone();
            let app_id = app_id.clone();
            let user = user.clone();
            move || {
                let client = client.clone();
                let app_id = app_id.clone();
                let user = user.clone();
                async move {
                    if let Err(err) = client.app_register(&app_id, &user).await {
                        warn!("Coordinator app registration failed for app {app_id} and user {user}: {err:#}");
                    }
                    if let Err(error) = client.app_heartbeat(&app_id).await {
                        warn!("Coordinator heartbeat failed for app {app_id}: {error:#}");
                    }
                }
            }
        });

        let shuffles = Arc::new(DDashMap::<ShuffleId, RegisteredShuffle>::default());
        let server_conns = Arc::new(ServerCoonPool::new());

        // set up the servers heartbeat task
        let heartbeat_server_task = ScheduledTask::new(interval, {
            let shuffles = shuffles.clone();
            let server_conns = server_conns.clone();
            let app_id = app_id.clone();
            move || heartbeat_servers(shuffles.clone(), server_conns.clone(), app_id.clone())
        });

        Ok(Self {
            app_id,
            user,
            configs,
            assignment_tags: tags,
            assignment_server_num: server_num,
            shuffle_id_manager: ShuffleIdManager::new(),
            shuffles,
            coordinator_client,
            server_conn: server_conns,
            lifecycle_lock: AsyncMutex::new(()),
            closed: AtomicBool::new(false),
            heartbeat_coordinator_task: coordinator_task,
            heartbeat_server_task,
        })
    }

    pub fn get_shuffle_handle(&self, shuffle_id: ShuffleId) -> Result<ShuffleHandle> {
        self.shuffles
            .get(&shuffle_id)
            .map(|entry| entry.handle.clone())
            .with_context(|| format!("shuffle {shuffle_id} is not registered"))
    }

    pub async fn register_shuffle(
        &self,
        shuffle_key: ShuffleKey,
        partition_count: u32,
    ) -> Result<ShuffleHandle> {
        let _guard = self.lifecycle_lock.lock().await;
        ensure!(
            !self.closed.load(Ordering::Acquire),
            "application is closed"
        );
        ensure!(
            partition_count > 0 && partition_count <= 1 << 24,
            "partition count must be between 1 and {}",
            1 << 24
        );
        let shuffle_id = self.shuffle_id_manager.get_or_create(&shuffle_key);
        if let Some(shuffle) = self.shuffles.get(&shuffle_id) {
            ensure!(
                shuffle.handle.partition_count == partition_count,
                "shuffle {shuffle_id} was registered with {} partitions, requested {partition_count}",
                shuffle.handle.partition_count
            );
            info!(
                "Shuffle {shuffle_id} is already registered for app {}",
                self.app_id
            );
            return Ok(shuffle.handle.clone());
        }

        // request the assignment for this shuffle
        let assignment = self
            .coordinator_client
            .get_assignment(GetAssignmentContext {
                app_id: self.app_id.clone(),
                shuffle_id,
                partition_count,
                required_tags: self.assignment_tags.clone(),
                required_server_number: self.assignment_server_num,
            })
            .await?;

        // do the server registration after assignment
        let endpoints: HashSet<_> = assignment
            .assignments
            .iter()
            .flat_map(|entry| entry.value().clone())
            .collect();
        let results = join_all(endpoints.into_iter().map(|endpoint| async move {
            let mut client = self
                .server_conn
                .get_or_create_client(ServerConnOptions::new(endpoint.clone()))?;
            let response = client
                .register_shuffle(ShuffleRegisterRequest {
                    app_id: self.app_id.clone(),
                    shuffle_id: shuffle_id as i32,
                    user: self.user.clone(),
                    properties: self.configs.properties().clone(),
                    ..Default::default()
                })
                .await
                .with_context(|| format!("register shuffle on {}", endpoint.uri()))?
                .into_inner();
            ensure_success("registerShuffle", response.status, &response.ret_msg)
        }))
        .await;
        results.into_iter().collect::<Result<()>>()?;

        // store the shuffle handle
        let handle = ShuffleHandle {
            app_id: self.app_id.clone(),
            shuffle_id,
            partition_count,
            assignment,
        };
        self.shuffles.insert(
            shuffle_id,
            RegisteredShuffle {
                handle: handle.clone(),
                shuffle_task_manager: ShuffleTaskManager::new(),
            },
        );
        Ok(handle)
    }

    pub async fn unregister_shuffle(&self, shuffle_id: ShuffleId) -> Result<()> {
        let _guard = self.lifecycle_lock.lock().await;
        let Some(handle) = self
            .shuffles
            .get(&shuffle_id)
            .map(|entry| entry.handle.clone())
        else {
            return Ok(());
        };
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
                    .server_conn
                    .get_or_create_client(ServerConnOptions::new(endpoint.clone()))?;
                Ok((endpoint, client))
            })
            .collect::<Result<_>>()?;
        self.unregister_servers(shuffle_id as i32, clients).await?;
        self.shuffles.remove(&shuffle_id);
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        let _guard = self.lifecycle_lock.lock().await;
        self.closed.store(true, Ordering::Release);
        self.heartbeat_coordinator_task.close().await;
        self.heartbeat_server_task.close().await;
        // do app unregistration to shuffle servers
        join_all(
            self.server_conn
                .clients()
                .into_iter()
                .map(|(endpoint, mut client)| async move {
                    let response = client
                        .unregister_shuffle_by_app_id(ShuffleUnregisterByAppIdRequest {
                            app_id: self.app_id.clone(),
                        })
                        .await
                        .with_context(|| format!("unregister app on {}", endpoint.uri()))?
                        .into_inner();
                    ensure_success(
                        "unregisterShuffleByAppId",
                        response.status,
                        &response.ret_msg,
                    )
                }),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        self.shuffles.clear();
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
                    ensure_success("unregisterShuffle", response.status, &response.ret_msg)
                }),
        )
        .await;
        results.into_iter().collect::<Result<Vec<_>>>()?;
        Ok(())
    }

    pub fn find_shuffle_id(&self, shuffle_key: ShuffleKey) -> Option<ShuffleId> {
        self.shuffle_id_manager.find(&shuffle_key)
    }
}

struct RegisteredShuffle {
    handle: ShuffleHandle,
    shuffle_task_manager: ShuffleTaskManager,
}

impl RegisteredShuffle {
    fn assignments(
        &self,
        partitions: impl IntoIterator<Item = PartitionId>,
    ) -> Result<Vec<rpc::PartitionAssignment>, Status> {
        partitions
            .into_iter()
            .map(|partition_id| {
                if partition_id >= self.handle.partition_count as usize {
                    return Err(Status::invalid_argument(format!(
                        "partition {partition_id} is out of range"
                    )));
                }
                let endpoint = self
                    .handle
                    .assignment
                    .get_endpoint(partition_id)
                    .map_err(|error| Status::internal(error.to_string()))?;
                Ok(rpc::PartitionAssignment {
                    partition_id: i32::try_from(partition_id)
                        .map_err(|_| Status::internal("partition ID exceeds protocol i32 range"))?,
                    endpoint: Some(rpc::Endpoint {
                        host: endpoint.host,
                        grpc_port: i32::from(endpoint.grpc_port),
                        urpc_port: None,
                    }),
                })
            })
            .collect()
    }
}

async fn heartbeat_servers(
    shuffles: Arc<DDashMap<ShuffleId, RegisteredShuffle>>,
    server_conns: Arc<ServerCoonPool>,
    app_id: String,
) {
    let mut endpoints = HashSet::new();
    for shuffle in shuffles.iter() {
        for assignment in shuffle.handle.assignment.assignments.iter() {
            endpoints.extend(assignment.value().iter().cloned());
        }
    }

    let server_conns = server_conns.as_ref();
    let app_id = app_id.as_str();
    join_all(endpoints.iter().map(|endpoint| async move {
        if let Err(error) = heartbeat_server(server_conns, app_id, endpoint).await {
            warn!(
                "Server heartbeat failed for app {app_id} at {}: {error:#}",
                endpoint.uri()
            );
        }
    }))
    .await;
}

async fn heartbeat_server(
    server_conns: &ServerCoonPool,
    app_id: &str,
    endpoint: &Endpoint,
) -> Result<()> {
    let mut client = server_conns.get_or_create_client(ServerConnOptions::new(endpoint.clone()))?;
    let response = client
        .app_heartbeat(AppHeartBeatRequest {
            app_id: app_id.to_owned(),
        })
        .await?
        .into_inner();
    ensure_success("appHeartbeat", response.status, &response.ret_msg)
}

pub type ShuffleManagerRef = Arc<ShuffleManager>;

#[derive(Default)]
pub struct ManagerRpcService {
    app_shuffle_managers: DDashMap<ApplicationId, ShuffleManagerRef>,
    lifecycle_lock: AsyncMutex<()>,
}

impl ManagerRpcService {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn create_shuffle_manager(
        &self,
        app_id: &str,
        user: &str,
        configs: HashMap<String, String>,
    ) -> Result<ShuffleManagerRef> {
        let _guard = self.lifecycle_lock.lock().await;
        if let Some(manager) = self.app_shuffle_managers.get(app_id) {
            ensure!(
                !manager.closed.load(Ordering::Acquire),
                "application is closed"
            );
            return Ok(manager.clone());
        }
        let manager = Arc::new(ShuffleManager::new(ApplicationOptions::new(
            app_id.to_owned(),
            user.to_owned(),
            configs,
        ))?);
        self.app_shuffle_managers
            .insert(app_id.to_owned(), manager.clone());
        Ok(manager)
    }

    pub fn get_shuffle_manager(&self, app_id: &str) -> Result<ShuffleManagerRef, Status> {
        if app_id.is_empty() {
            return Err(Status::invalid_argument("application ID is empty"));
        }
        let manager = self
            .app_shuffle_managers
            .get(app_id)
            .map(|entry| entry.clone())
            .ok_or_else(|| Status::not_found(format!("application {app_id} is not registered")))?;
        if manager.closed.load(Ordering::Acquire) {
            return Err(Status::failed_precondition("application is closed"));
        }
        Ok(manager)
    }

    /// Stops the application before removing it. Failed remote cleanup can be retried.
    pub async fn remove_shuffle_manager(&self, app_id: &str) -> Result<()> {
        let _guard = self.lifecycle_lock.lock().await;
        let manager = self
            .app_shuffle_managers
            .get(app_id)
            .map(|entry| entry.clone());
        if let Some(manager) = manager {
            manager.close().await?;
        }
        self.app_shuffle_managers.remove(app_id);
        Ok(())
    }
}

macro_rules! checked_task_id {
    ($value:expr) => {
        checked_task_id!($value, ::tonic::Status::invalid_argument)
    };
    ($value:expr, $error:path) => {
        match $crate::TaskId::try_from($value) {
            Ok(task_id) if task_id < $crate::TASK_ID_LIMIT => task_id,
            _ => return Err($error("task ID exceeds 23-bit block ID range")),
        }
    };
}

#[tonic::async_trait]
impl ShuffleManagerService for ManagerRpcService {
    async fn get_write_shuffle_handle(
        &self,
        request: Request<rpc::GetWriteShuffleHandleRequest>,
    ) -> Result<Response<rpc::GetWriteShuffleHandleResponse>, Status> {
        use rpc::get_write_shuffle_handle_request::ShuffleIdentifier;

        let request = request.into_inner();
        let app_id = request.app_id;
        let shuffle_identifier = request.shuffle_identifier;
        let map_id = request.map_id as usize;
        let attempt_id = request.attempt_id as usize;

        let manager = self.get_shuffle_manager(&app_id)?;
        let key = match shuffle_identifier {
            Some(ShuffleIdentifier::ShuffleKey(key)) => ShuffleKey::JobIdAndStageId(key),
            Some(ShuffleIdentifier::ShuffleId(id)) => ShuffleKey::ShuffleId(id as ShuffleId),
            None => return Err(Status::invalid_argument("shuffle identifier is missing")),
        };
        let shuffle_id = manager
            .register_shuffle(key, request.num_partitions)
            .await
            .map_err(|error| Status::internal(format!("register shuffle: {error:#}")))?
            .shuffle_id();
        let shuffle = manager
            .shuffles
            .get(&shuffle_id)
            .ok_or_else(|| Status::not_found(format!("shuffle {shuffle_id} is not registered")))?;

        let assignments = shuffle.assignments(0..shuffle.handle.partition_count as usize)?;
        let task_id = checked_task_id!(
            shuffle
                .shuffle_task_manager
                .register_task(&TaskKey::MapIdAndAttemptId(map_id, attempt_id)),
            Status::resource_exhausted
        );
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
        let app_id = request.app_id;
        let shuffle_id = request.shuffle_id as ShuffleId;
        let task_id = checked_task_id!(request.task_id);

        let manager = self.get_shuffle_manager(&app_id)?;
        let shuffle = manager
            .shuffles
            .get(&shuffle_id)
            .ok_or_else(|| Status::not_found(format!("shuffle {shuffle_id} is not registered")))?;
        shuffle
            .shuffle_task_manager
            .report_finished_task(&TaskKey::TaskId(task_id))
            .map_err(|error| Status::not_found(error.to_string()))?;
        Ok(Response::new(rpc::ReportShuffleResultResponse {}))
    }

    async fn get_read_shuffle_handle(
        &self,
        request: Request<rpc::GetReadShuffleHandleRequest>,
    ) -> Result<Response<rpc::GetReadShuffleHandleResponse>, Status> {
        use rpc::get_read_shuffle_handle_request::ShuffleIdentifier;

        let request = request.into_inner();
        let app_id = request.app_id;
        let partitions = request.partition_ids;

        let manager = self.get_shuffle_manager(&app_id)?;
        let key = match request.shuffle_identifier {
            Some(ShuffleIdentifier::ShuffleKey(key)) => ShuffleKey::JobIdAndStageId(key),
            Some(ShuffleIdentifier::ShuffleId(id)) => ShuffleKey::ShuffleId(id as ShuffleId),
            None => return Err(Status::invalid_argument("shuffle identifier is missing")),
        };
        let shuffle_id = manager
            .find_shuffle_id(key)
            .ok_or_else(|| Status::not_found("shuffle is not registered"))?;
        let shuffle = manager
            .shuffles
            .get(&shuffle_id)
            .ok_or_else(|| Status::not_found(format!("shuffle {shuffle_id} is not registered")))?;

        Ok(Response::new(rpc::GetReadShuffleHandleResponse {
            shuffle_id: shuffle_id as u64,
            assignments: shuffle.assignments(partitions.into_iter().map(|id| id as PartitionId))?,
            task_ids: shuffle
                .shuffle_task_manager
                .get_finished_tasks()
                .into_iter()
                .map(|id| id as u64)
                .collect(),
            partition_count: shuffle.handle.partition_count,
        }))
    }
}
