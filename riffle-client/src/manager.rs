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
use crate::{check_status, rpc, Endpoint, ShuffleHandle, ShuffleId, MAX_MESSAGE_BYTES};

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
    shuffles: Arc<DDashMap<ShuffleId, ShuffleHandle>>,
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

        let shuffles = Arc::new(DDashMap::<ShuffleId, ShuffleHandle>::default());
        let shuffle_server_clients = Arc::new(ServerCoonPool::new());

        let heartbeat_server_task = ScheduledTask::new(interval, {
            let shuffles = shuffles.clone();
            let clients = shuffle_server_clients.clone();
            let app_id = app_id.clone();
            move || {
                let mut endpoints = HashSet::new();
                for shuffle in shuffles.iter() {
                    for assignment in shuffle.assignment.assignments.iter() {
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

        // rpc service for writer/reader to get assignment from manager
        // todo: for the further reassignment mechanism.
        let service = AssignmentService {
            shuffles: shuffles.clone(),
        };
        let (rpc_shutdown, mut shutdown) = watch::channel(false);
        let rpc_task = tokio::spawn(async move {
            let result = Server::builder()
                .add_service(
                    ShuffleManagerServiceServer::new(service)
                        .max_encoding_message_size(MAX_MESSAGE_BYTES),
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
            .map(|entry| entry.value().clone())
            .with_context(|| format!("shuffle {shuffle_id} is not registered"))
    }

    /// The engine must not reuse shuffle IDs during the app lifetime.
    pub async fn register_shuffle(
        &self,
        shuffle_id: ShuffleId,
        partition_count: u32,
    ) -> Result<ShuffleHandle> {
        let _guard = self.lifecycle_lock.lock().await;
        if let Some(handle) = self.shuffles.get(&shuffle_id) {
            info!(
                "Shuffle {shuffle_id} is already registered for app {}",
                self.app_id
            );
            return Ok(handle.value().clone());
        }
        ensure!(
            partition_count > 0 && partition_count <= 1 << 24,
            "partition count exceeds 24-bit block ID range or is zero"
        );
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
        self.shuffles.insert(shuffle_id, handle.clone());
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
        self.shuffles.remove(&shuffle_id);
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

// The RPC service shares routing state without retaining the manager's background tasks.
struct AssignmentService {
    shuffles: Arc<DDashMap<ShuffleId, ShuffleHandle>>,
}

#[tonic::async_trait]
impl ShuffleManagerService for AssignmentService {
    async fn get_assignment(
        &self,
        request: Request<rpc::GetAssignmentRequest>,
    ) -> std::result::Result<Response<rpc::GetAssignmentResponse>, Status> {
        let shuffle_id = usize::try_from(request.into_inner().shuffle_id)
            .map_err(|_| Status::invalid_argument("negative shuffle ID"))?;
        let handle = self
            .shuffles
            .get(&shuffle_id)
            .ok_or_else(|| Status::not_found(format!("shuffle {shuffle_id} is not registered")))?;
        let assignments = handle
            .assignment
            .assignments
            .iter()
            .map(|entry| {
                let endpoint = &entry.value()[0];
                rpc::PartitionAssignment {
                    partition_id: *entry.key() as i32,
                    endpoint: Some(rpc::Endpoint {
                        host: endpoint.host.clone(),
                        port: endpoint.grpc_port.into(),
                    }),
                }
            })
            .collect();
        Ok(Response::new(rpc::GetAssignmentResponse { assignments }))
    }
}
