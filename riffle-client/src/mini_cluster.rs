use anyhow::{ensure, Context, Result};
use futures::future::join_all;
use riffle_coordinator::config::Config as CoordinatorConfig;
use riffle_coordinator::coordinator::Coordinator;
use riffle_server::config::{Config as ServerConfig, MemoryStoreConfig, RuntimeConfig};
use riffle_server::server::Server;
use std::time::Duration;

use crate::Endpoint;

#[derive(Clone, Debug)]
pub struct MiniClusterConfig {
    pub num_servers: usize,
}

impl Default for MiniClusterConfig {
    fn default() -> Self {
        Self { num_servers: 1 }
    }
}

/// An in-process coordinator and memory-only shuffle servers for local use.
/// Server HTTP administration and metrics still share process-global state.
pub struct MiniCluster {
    config: MiniClusterConfig,
    servers: Vec<Server>,
    coordinator: Option<Coordinator>,
}

impl MiniCluster {
    pub fn new(config: MiniClusterConfig) -> Self {
        Self {
            config,
            servers: Vec::new(),
            coordinator: None,
        }
    }

    /// Start on random ports and wait until all servers are available for assignment.
    pub async fn start(&mut self) -> Result<()> {
        ensure!(self.config.num_servers > 0, "num_servers must be positive");
        ensure!(
            self.coordinator.is_none(),
            "mini cluster is already started"
        );

        let coordinator = Coordinator::start(CoordinatorConfig {
            grpc_port: 0,
            http_port: 0,
            max_assignment_servers: self.config.num_servers,
            ..CoordinatorConfig::default()
        })
        .await
        .context("failed to start mini cluster coordinator")?;
        let quorum = format!("127.0.0.1:{}", coordinator.grpc_addr().port());
        let mut servers = Vec::with_capacity(self.config.num_servers);
        for index in 0..self.config.num_servers {
            let mut config = ServerConfig::create_simple_config();
            config.grpc_port = 0;
            config.http_port = 0;
            config.coordinator_quorum = vec![quorum.clone()];
            config.heartbeat_interval_seconds = 1;
            config.memory_store = Some(MemoryStoreConfig::new("64M".to_owned()));
            config.runtime_config = RuntimeConfig {
                read_thread_num: 1,
                localfile_write_thread_num: 1,
                hdfs_write_thread_num: 1,
                http_thread_num: 1,
                default_thread_num: 1,
                dispatch_thread_num: 1,
                read_ahead_thread_number: 1,
            };
            servers.push(
                Server::start(config)
                    .await
                    .with_context(|| format!("failed to start mini cluster server {index}"))?,
            );
        }

        let cluster_manager = coordinator.cluster_manager();
        tokio::time::timeout(Duration::from_secs(10), async {
            while cluster_manager.list_available(&[]).len() != servers.len() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .context("timed out waiting for mini cluster servers to register")?;

        self.servers = servers;
        self.coordinator = Some(coordinator);
        Ok(())
    }

    pub fn get_servers(&self) -> &[Server] {
        &self.servers
    }

    pub fn get_coordinator(&self) -> Option<&Coordinator> {
        self.coordinator.as_ref()
    }

    pub fn coordinator_endpoint(&self) -> Result<Endpoint> {
        let coordinator = self
            .coordinator
            .as_ref()
            .context("mini cluster is not started")?;
        Ok(Endpoint {
            host: "127.0.0.1".to_owned(),
            grpc_port: coordinator.grpc_addr().port(),
        })
    }

    /// Stop every server before stopping the coordinator, even if a server fails.
    pub async fn shutdown(self) -> Result<()> {
        let results = join_all(self.servers.into_iter().map(Server::shutdown)).await;
        let coordinator_result = match self.coordinator {
            Some(coordinator) => coordinator.shutdown().await,
            None => Ok(()),
        };
        for result in results {
            result.context("failed to shut down mini cluster server")?;
        }
        coordinator_result.context("failed to shut down mini cluster coordinator")
    }
}
