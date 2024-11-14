use crate::app::{AppManagerRef, SHUFFLE_SERVER_ID, SHUFFLE_SERVER_IP};
use crate::config::Config;
use crate::grpc::protobuf::uniffle::coordinator_server_client::CoordinatorServerClient;
use crate::grpc::protobuf::uniffle::{ShuffleServerHeartBeatRequest, ShuffleServerId};
use crate::health_service::HealthService;
use crate::metric::SERVICE_IS_HEALTHY;
use crate::runtime::manager::RuntimeManager;
use log::info;
use std::time::Duration;
use tonic::transport::Channel;

const DEFAULT_SHUFFLE_SERVER_TAG: &str = "ss_v4";

pub struct HeartbeatTask;

impl HeartbeatTask {
    pub fn init(
        config: &Config,
        runtime_manager: &RuntimeManager,
        app_manager: &AppManagerRef,
        health_service: &HealthService,
    ) {
        let runtime_manager = runtime_manager.clone();
        let app_manager = app_manager.clone();
        let health_service = health_service.clone();

        let coordinator_quorum = config.coordinator_quorum.clone();
        let tags = config.tags.clone().unwrap_or(vec![]);

        let grpc_port = config.grpc_port;
        let urpc_port = config.urpc_port.unwrap_or(0);

        let interval_seconds = config.heartbeat_interval_seconds;

        runtime_manager.default_runtime.spawn(async move {
            let ip = SHUFFLE_SERVER_IP.get().unwrap().to_string();
            info!("machine ip: {}", &ip);

            let shuffle_server_id = ShuffleServerId {
                id: SHUFFLE_SERVER_ID.get().unwrap().to_string(),
                ip,
                port: grpc_port,
                netty_port: urpc_port,
            };

            let mut multi_coordinator_clients: Vec<CoordinatorServerClient<Channel>> =
                futures::future::try_join_all(
                    coordinator_quorum.iter().map(|quorum| {
                        CoordinatorServerClient::connect(format!("http://{}", quorum))
                    }),
                )
                .await
                .unwrap();

            loop {
                // todo: add interval as config var
                tokio::time::sleep(Duration::from_secs(interval_seconds as u64)).await;

                let mut all_tags = vec![];
                all_tags.push(DEFAULT_SHUFFLE_SERVER_TAG.to_string());
                all_tags.extend_from_slice(&*tags);

                let healthy = health_service.is_healthy().await.unwrap_or(false);
                SERVICE_IS_HEALTHY.set(if healthy { 0 } else { 1 });

                let memory_snapshot = app_manager
                    .store_memory_snapshot()
                    .await
                    .unwrap_or((0, 0, 0).into());
                let memory_spill_event_num =
                    app_manager.store_memory_spill_event_num().unwrap_or(0) as i32;

                let heartbeat_req = ShuffleServerHeartBeatRequest {
                    server_id: Some(shuffle_server_id.clone()),
                    used_memory: memory_snapshot.used(),
                    pre_allocated_memory: memory_snapshot.allocated(),
                    available_memory: memory_snapshot.capacity()
                        - memory_snapshot.used()
                        - memory_snapshot.allocated(),
                    event_num_in_flush: memory_spill_event_num,
                    tags: all_tags,
                    is_healthy: Some(healthy),
                    status: 0,
                    storage_info: Default::default(),
                };

                // It must use the 0..len to avoid borrow check in loop.
                for idx in 0..multi_coordinator_clients.len() {
                    let client = multi_coordinator_clients.get_mut(idx).unwrap();
                    let _ = client
                        .heartbeat(tonic::Request::new(heartbeat_req.clone()))
                        .await;
                }
            }
        });
    }
}
