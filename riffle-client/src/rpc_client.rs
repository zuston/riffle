pub(crate) mod transport;

use std::time::Duration;

use anyhow::{anyhow, ensure, Context, Result};
use futures::future::join_all;
use log::warn;
use riffle_server::ddashmap::DDashMap;
use riffle_server::grpc::protobuf::uniffle::coordinator_server_client::CoordinatorServerClient;
use riffle_server::grpc::protobuf::uniffle::shuffle_server_client::ShuffleServerClient;
use riffle_server::grpc::protobuf::uniffle::{
    AppHeartBeatRequest, ApplicationInfoRequest, GetShuffleServerRequest,
};
use tonic::transport::Channel;

use crate::check_status;
use crate::{
    ApplicationId, Endpoint, ShuffleAssignment, ShuffleId, MAX_MESSAGE_BYTES, RPC_TIMEOUT,
};

pub struct GetAssignmentContext {
    pub app_id: ApplicationId,
    pub shuffle_id: ShuffleId,
    pub partition_count: u32,
    pub required_tags: Vec<String>,
    pub required_server_number: i32,
}

#[derive(Clone)]
pub struct CoordinatorQuorumClient {
    clients: Vec<CoordinatorServerClient<Channel>>,
}

impl CoordinatorQuorumClient {
    pub fn new(quorum: Vec<Endpoint>) -> Result<Self> {
        ensure!(!quorum.is_empty(), "coordinator quorum is empty");
        let clients = quorum
            .iter()
            .map(|endpoint| {
                Ok(
                    CoordinatorServerClient::new(lazy_channel(endpoint, RPC_TIMEOUT)?)
                        .max_decoding_message_size(MAX_MESSAGE_BYTES),
                )
            })
            .collect::<Result<_>>()?;
        Ok(Self { clients })
    }

    pub async fn app_register(&self, app_id: &str, user: &str) -> Result<()> {
        let request = ApplicationInfoRequest {
            app_id: app_id.into(),
            user: user.into(),
            version: Some(env!("CARGO_PKG_VERSION").into()),
            ..Default::default()
        };
        let results = join_all(self.clients.iter().cloned().map(|mut client| {
            let request = request.clone();
            async move {
                let response = client
                    .register_application_info(request)
                    .await?
                    .into_inner();
                check_status(
                    "registerApplicationInfo",
                    response.status,
                    &response.ret_msg,
                )
            }
        }))
        .await;
        check_coordinator_results("registerApplicationInfo", results)
    }

    pub async fn app_heartbeat(&self, app_id: &str) -> Result<()> {
        let request = AppHeartBeatRequest {
            app_id: app_id.into(),
        };
        let results = join_all(self.clients.iter().cloned().map(|mut client| {
            let request = request.clone();
            async move {
                let response = client.app_heartbeat(request).await?.into_inner();
                check_status("appHeartbeat", response.status, &response.ret_msg)
            }
        }))
        .await;
        check_coordinator_results("appHeartbeat", results)
    }

    pub async fn get_assignment(&self, context: GetAssignmentContext) -> Result<ShuffleAssignment> {
        let request = GetShuffleServerRequest {
            application_id: context.app_id,
            shuffle_id: context.shuffle_id as i32,
            partition_num: i32::try_from(context.partition_count)
                .context("partition count exceeds protocol i32 range")?,
            partition_num_per_range: 1,
            data_replica: 1,
            require_tags: context.required_tags,
            assignment_shuffle_server_number: context.required_server_number,
            ..Default::default()
        };
        let mut last_error = anyhow!("no coordinator available for assignment");
        for mut client in self.clients.iter().cloned() {
            let result = async {
                let response = client
                    .get_shuffle_assignments(request.clone())
                    .await?
                    .into_inner();
                check_status("getShuffleAssignments", response.status, &response.ret_msg)?;
                Ok::<_, anyhow::Error>(response)
            }
            .await;
            match result {
                Ok(response) => {
                    let assignments = DDashMap::default();
                    for assignment in response.assignments {
                        ensure!(
                            assignment.start_partition >= 0
                                && assignment.start_partition <= assignment.end_partition
                                && assignment.end_partition < request.partition_num,
                            "invalid assignment partition range"
                        );
                        let endpoints = assignment
                            .server
                            .into_iter()
                            .map(|server| {
                                Ok(Endpoint {
                                    host: server.ip,
                                    grpc_port: u16::try_from(server.port)
                                        .context("invalid shuffle server port")?,
                                })
                            })
                            .collect::<Result<Vec<_>>>()?;
                        ensure!(endpoints.len() == 1, "expected a single shuffle replica");
                        for partition in assignment.start_partition..=assignment.end_partition {
                            assignments.insert(
                                usize::try_from(partition)
                                    .context("negative assignment partition ID")?,
                                endpoints.clone(),
                            );
                        }
                    }
                    ensure!(
                        assignments.len() == context.partition_count as usize,
                        "incomplete shuffle assignment"
                    );
                    return Ok(ShuffleAssignment::new(context.shuffle_id, assignments));
                }
                Err(error) => {
                    warn!("Coordinator assignment failed: {error:#}");
                    last_error = error;
                }
            }
        }
        Err(last_error)
    }
}

fn check_coordinator_results(operation: &str, results: Vec<Result<()>>) -> Result<()> {
    let mut succeeded = false;
    let mut last_error = anyhow!("no coordinator available for {operation}");
    for result in results {
        match result {
            Ok(()) => succeeded = true,
            Err(error) => {
                warn!("Coordinator {operation} failed: {error:#}");
                last_error = error;
            }
        }
    }
    if succeeded {
        Ok(())
    } else {
        Err(last_error)
    }
}

pub type ServerClient = ShuffleServerClient<Channel>;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct ServerConnOptions {
    pub endpoint: Endpoint,
    pub timeout: Duration,
}

impl ServerConnOptions {
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            timeout: RPC_TIMEOUT,
        }
    }
}

impl From<Endpoint> for ServerConnOptions {
    fn from(endpoint: Endpoint) -> Self {
        Self::new(endpoint)
    }
}

#[derive(Default)]
pub struct ServerCoonPool {
    clients: DDashMap<ServerConnOptions, ServerClient>,
}

impl ServerCoonPool {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_or_create_client(&self, conn_options: ServerConnOptions) -> Result<ServerClient> {
        if let Some(client) = self.clients.get(&conn_options) {
            return Ok(client.value().clone());
        }
        let channel = lazy_channel(&conn_options.endpoint, conn_options.timeout)?;
        Ok(self.clients.compute_if_absent(conn_options, || {
            ServerClient::new(channel)
                .max_encoding_message_size(MAX_MESSAGE_BYTES)
                .max_decoding_message_size(MAX_MESSAGE_BYTES)
        }))
    }

    pub(crate) fn clients(&self) -> Vec<(Endpoint, ServerClient)> {
        self.clients
            .iter()
            .map(|entry| (entry.key().endpoint.clone(), entry.value().clone()))
            .collect()
    }

    pub(crate) fn clear(&self) {
        self.clients.clear();
    }
}

fn lazy_channel(endpoint: &Endpoint, timeout: Duration) -> Result<Channel> {
    Ok(tonic::transport::Endpoint::from_shared(endpoint.uri())?
        .connect_timeout(timeout)
        .timeout(timeout)
        .connect_lazy())
}
