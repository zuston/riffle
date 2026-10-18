pub mod shuffle_id_manager;
pub mod shuffle_task_manager;
pub mod manager_rpc_client;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use futures::future::join_all;
use log::{info, warn};
use riffle_server::client_configs::ClientRssConf;
use riffle_server::ddashmap::DDashMap;
use riffle_server::grpc::protobuf::uniffle::{
    AppHeartBeatRequest, ShuffleRegisterRequest,
    ShuffleUnregisterByAppIdRequest, ShuffleUnregisterRequest,
};
use riffle_server::scheduled_task::ScheduledTask;
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
    ensure_success, rpc, Endpoint, ShuffleHandle, ShuffleId, TaskId, MAX_MESSAGE_BYTES, TASK_ID_LIMIT,
};
use crate::manager::shuffle_id_manager::{ShuffleIdManager, ShuffleKey};
use crate::manager::shuffle_task_manager::ShuffleTaskManager;

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
}

impl ShuffleManager {
    pub async fn new(app_options: ApplicationOptions) -> Result<Self> {
        let ApplicationOptions {
            app_id,
            user,
            configs,
        } = app_options;
        let configs = ClientRssConf::from(configs);

        // register app to coordinator
        let coordinator_quorum = configs
            .get_or_panic(&COORDINATOR_QUORUM)
            .into_iter()
            .map(|address| address.parse())
            .collect::<Result<Vec<Endpoint>>>()?;
        let coordinator_client = CoordinatorQuorumClient::new(coordinator_quorum)?;
        coordinator_client.app_register(&app_id, &user).await?;

        // set up the coordinator heartbeat task
        let interval = Duration::from_secs(configs.get_or_panic(&HEARTBEAT_INTERVAL_SECONDS));
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
        let server_conns = Arc::new(ServerCoonPool::new());

        // set up the servers heartbeat task
        let heartbeat_server_task = ScheduledTask::new(interval, {
            let shuffles = shuffles.clone();
            let server_conns = server_conns.clone();
            let app_id = app_id.clone();
            move || heartbeat_servers(shuffles.clone(), server_conns.clone(), app_id.clone())
        });

        let tags = configs.get_or_panic(&ASSIGNMENT_TAGS);
        let server_num = configs.get_or_panic(&ASSIGNMENT_SERVER_NUMBER);

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
        let shuffle_id = self.shuffle_id_manager.get(&shuffle_key);
        if let Some(shuffle) = self.shuffles.get(&shuffle_id) {
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
                shuffle_key: shuffle_key.clone(),
                handle: handle.clone(),
                shuffle_task_manager: ShuffleTaskManager::new(),
            },
        );
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
                    .server_conn
                    .get_or_create_client(ServerConnOptions::new(endpoint.clone()))?;
                Ok((endpoint, client))
            })
            .collect::<Result<_>>()?;
        self.unregister_servers(shuffle_id as i32, clients).await?;
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        let _guard = self.lifecycle_lock.lock().await;
        self.heartbeat_coordinator_task.close().await;
        self.heartbeat_server_task.close().await;
        // do app unregistration to shuffle servers
        join_all(self.server_conn.clients().into_iter().map(
            |(endpoint, mut client)| async move {
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
            },
        )).await.into_iter().collect::<Result<Vec<_>>>()?;
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
}

struct RegisteredShuffle {
    shuffle_key: ShuffleKey,
    handle: ShuffleHandle,
    shuffle_task_manager: ShuffleTaskManager,
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

struct ManagerRpcService {}

#[tonic::async_trait]
impl ShuffleManagerService for ManagerRpcService {
    async fn get_write_shuffle_handle(
        &self,
        request: Request<rpc::GetWriteShuffleHandleRequest>,
    ) -> Result<Response<rpc::GetWriteShuffleHandleResponse>, Status> {
        todo!()
    }

    async fn report_shuffle_result(
        &self,
        request: Request<rpc::ReportShuffleResultRequest>,
    ) -> Result<Response<rpc::ReportShuffleResultResponse>, Status> {
        todo!()
    }

    async fn get_read_shuffle_handle(
        &self,
        request: Request<rpc::GetReadShuffleHandleRequest>,
    ) -> Result<Response<rpc::GetReadShuffleHandleResponse>, Status> {
        todo!()
    }
}
