use crate::app::{AppManagerRef, SHUFFLE_SERVER_ID, SHUFFLE_SERVER_IP};
use crate::config::Config;
use crate::grpc::protobuf::uniffle::coordinator_server_client::CoordinatorServerClient;
use crate::grpc::protobuf::uniffle::{
    ServerStatus, ShuffleServerHeartBeatRequest, ShuffleServerId,
};
use crate::health_service::HealthService;
use crate::metric::SERVICE_IS_HEALTHY;
use crate::runtime::manager::RuntimeManager;
use crate::server_state_manager::ServerStateManager;
use await_tree::InstrumentAwait;
use log::{error, info};
use std::time::Duration;
use tonic::transport::Channel;

const DEFAULT_SHUFFLE_SERVER_TAG: &str = "ss_v4";

pub struct HeartbeatTask;

impl HeartbeatTask {
    pub fn run(
        config: &Config,
        runtime_manager: &RuntimeManager,
        app_manager: &AppManagerRef,
        health_service: &HealthService,
        server_state_manager: &ServerStateManager,
    ) {
        let runtime_manager = runtime_manager.clone();
        let app_manager = app_manager.clone();
        let health_service = health_service.clone();
        let server_state_manager = server_state_manager.clone();

        let coordinator_quorum = config.coordinator_quorum.clone();
        let tags = config.tags.clone().unwrap_or(vec![]);

        let grpc_port = config.grpc_port;
        let urpc_port = config.urpc_port.unwrap_or(0);
        let http_port = config.http_port;

        let interval_seconds = config.heartbeat_interval_seconds;

        let ip = SHUFFLE_SERVER_IP.get().unwrap().to_string();
        let id = SHUFFLE_SERVER_ID.get().unwrap().to_string();
        info!("Machine ip: {}", &ip);

        let shuffle_server_id = ShuffleServerId {
            id,
            ip,
            port: grpc_port,
            netty_port: urpc_port,
            jetty_port: http_port as i32,
        };
        let start_time = std::time::Instant::now();
        let git_commit_id = env!("GIT_COMMIT_HASH").to_string();
        let version = env!("CARGO_PKG_VERSION").to_string();

        runtime_manager.default_runtime.spawn_with_await_tree(
            "Coordinator heartbeat task",
            async move {
                let mut multi_coordinator_clients: Vec<CoordinatorServerClient<Channel>> =
                    futures::future::try_join_all(coordinator_quorum.iter().map(|quorum| {
                        CoordinatorServerClient::connect(format!("http://{}", quorum))
                    }))
                    .await
                    .unwrap();

                loop {
                    tokio::time::sleep(Duration::from_secs(interval_seconds as u64))
                        .instrument_await("sleeping")
                        .await;

                    let mut all_tags = vec![];
                    all_tags.push(DEFAULT_SHUFFLE_SERVER_TAG.to_string());
                    all_tags.extend_from_slice(&*tags);

                    let mut healthy = health_service.is_healthy().await.unwrap_or(false);
                    SERVICE_IS_HEALTHY.set(if healthy { 0 } else { 1 });

                    let memory_snapshot = app_manager
                        .store_memory_snapshot()
                        .await
                        .unwrap_or((0, 0, 0).into());
                    let memory_spill_event_num =
                        app_manager.store_memory_spill_event_num().unwrap_or(0) as i32;

                    let server_state = server_state_manager.get_server_status();

                    // Compatible with uniffle coordinator's logic
                    // https://github.com/apache/uniffle/blob/6843c06b1b1e4fe8c8c2be84e6a15f831c4b9a4c/coordinator/src/main/java/org/apache/uniffle/coordinator/CoordinatorGrpcService.java#L512
                    if server_state != ServerStatus::Active {
                        healthy = false;
                    }

                    let heartbeat_req = ShuffleServerHeartBeatRequest {
                        server_id: Some(shuffle_server_id.clone()),
                        used_memory: memory_snapshot.used(),
                        pre_allocated_memory: memory_snapshot.allocated(),
                        available_memory: memory_snapshot.available(),
                        event_num_in_flush: memory_spill_event_num,
                        tags: all_tags,
                        is_healthy: Some(healthy),
                        status: server_state.into(),
                        storage_info: Default::default(),
                        version: Some(version.to_string()),
                        git_commit_id: Some(git_commit_id.clone()),
                        // todo: add start time
                        start_time_ms: None,
                        // todo: add active application infos
                        application_info: vec![],
                        display_metrics: Default::default(),
                    };

                    // It must use the 0..len to avoid borrow check in loop.
                    for idx in 0..multi_coordinator_clients.len() {
                        let client = multi_coordinator_clients.get_mut(idx).unwrap();
                        match client
                            .heartbeat(tonic::Request::new(heartbeat_req.clone()))
                            .await
                        {
                            Err(err) => {
                                error!(
                                    "Errors on heartbeat with coordinator idx: {}. errors: {}",
                                    idx, err
                                );
                            }
                            _ => {}
                        }
                    }
                }
            },
        );
    }
}
