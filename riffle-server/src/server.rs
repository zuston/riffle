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

use crate::app_manager::{AppManager, AppManagerRef, APP_MANAGER_REF};
use crate::common::init_global_variable;
use crate::config::Config;
use crate::config_reconfigure::ReconfigurableConfManager;
use crate::health_service::HealthService;
use crate::heartbeat::HeartbeatTask;
use crate::http::HttpMonitorService;
use crate::metric::MetricService;
use crate::rpc::DefaultRpcService;
use crate::runtime::manager::RuntimeManager;
use crate::server_state_manager::{ServerStateManager, SERVER_STATE_MANAGER_REF};
use crate::service_tags_manager::{ServiceTagsManager, SERVICE_TAGS_MANAGER_REF};
use crate::storage::{HybridStorage, StorageService};
use crate::tracing::FastraceWrapper;
use crate::util;
use anyhow::{Context, Result};
use log::info;
use std::collections::HashSet;
use tokio::sync::broadcast;

pub struct Server {
    config: Config,
    app_manager: AppManagerRef,
    storage: HybridStorage,
    runtime_manager: RuntimeManager,
    server_state_manager: ServerStateManager,
    service_tags_manager: ServiceTagsManager,
    shutdown_tx: broadcast::Sender<()>,
    task: crate::runtime::JoinHandle<Result<()>>,
}

impl Server {
    /// Start the shuffle server, leaving process logging and signals to the caller.
    /// An empty coordinator quorum disables heartbeats for standalone use.
    pub async fn start(mut config: Config) -> Result<Self> {
        prepare_ports(&mut config)?;
        init_global_variable(&config);
        util::inject_into_env(vec![
            ("GRPC_PORT".to_owned(), config.grpc_port.to_string()),
            ("HTTP_PORT".to_owned(), config.http_port.to_string()),
        ]);

        let runtime_manager = RuntimeManager::from(config.runtime_config.clone());
        let reload_options = config
            .config_path
            .as_deref()
            .filter(|_| config.conf_reload_enable)
            .map(|path| (path, 60, &runtime_manager.default_runtime).into());
        let reconf_manager = ReconfigurableConfManager::new(&config, reload_options)?;
        let storage = StorageService::init(&runtime_manager, &config, &reconf_manager)?;
        let app_manager = AppManager::get_ref(
            runtime_manager.clone(),
            config.clone(),
            &storage,
            &reconf_manager,
        );
        storage.with_app_manager(&app_manager);
        let _ = APP_MANAGER_REF.set(app_manager.clone());

        let health_service =
            HealthService::new(&app_manager, &storage, &config.health_service_config);
        let server_state_manager = ServerStateManager::new(&app_manager, &config);
        let _ = SERVER_STATE_MANAGER_REF.set(server_state_manager.clone());
        let service_tags_manager = ServiceTagsManager::new(&config);
        let _ = SERVICE_TAGS_MANAGER_REF.set(service_tags_manager.clone());

        MetricService::init(&config, runtime_manager.clone());
        FastraceWrapper::init(config.clone());

        let (shutdown_tx, _) = broadcast::channel(1);
        let mut rpc = DefaultRpcService {}.start(
            &config,
            app_manager.clone(),
            &server_state_manager,
            shutdown_tx.subscribe(),
        )?;
        let mut http_shutdown = shutdown_tx.subscribe();
        HttpMonitorService::init(&config, runtime_manager.clone(), async move {
            let _ = http_shutdown.recv().await;
        })?;
        HeartbeatTask::run(
            &config,
            &runtime_manager,
            &app_manager,
            &health_service,
            &server_state_manager,
            &service_tags_manager,
            shutdown_tx.subscribe(),
        )
        .await?;
        let task_shutdown_tx = shutdown_tx.clone();
        let task = runtime_manager.default_runtime.spawn(async move {
            let result = rpc.wait().await;
            let _ = task_shutdown_tx.send(());
            result
        });

        Ok(Self {
            config,
            app_manager,
            storage,
            runtime_manager,
            server_state_manager,
            service_tags_manager,
            shutdown_tx,
            task,
        })
    }

    /// Configuration with the actual ports selected at startup.
    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn app_manager(&self) -> AppManagerRef {
        self.app_manager.clone()
    }

    pub fn storage(&self) -> HybridStorage {
        self.storage.clone()
    }

    pub fn runtime_manager(&self) -> RuntimeManager {
        self.runtime_manager.clone()
    }

    pub fn server_state_manager(&self) -> ServerStateManager {
        self.server_state_manager.clone()
    }

    pub fn service_tags_manager(&self) -> ServiceTagsManager {
        self.service_tags_manager.clone()
    }

    /// Wait for the gRPC and URPC services to exit.
    pub async fn wait(&mut self) -> Result<()> {
        (&mut self.task).await?
    }

    /// Notify all services to stop, then wait for gRPC and URPC to exit.
    pub async fn shutdown(mut self) -> Result<()> {
        let _ = self.shutdown_tx.send(());
        self.wait().await
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(());
    }
}

fn prepare_ports(config: &mut Config) -> Result<()> {
    let mut ports = HashSet::new();
    for port in [&mut config.grpc_port, &mut config.http_port]
        .into_iter()
        .chain(config.urpc_port.iter_mut())
    {
        if *port == 0 || (config.fallback_random_ports_enable && util::is_port_in_used(*port)) {
            loop {
                *port = util::find_available_port().context("no available service port")?;
                if !ports.contains(port) {
                    break;
                }
            }
        }
        anyhow::ensure!(ports.insert(*port), "duplicate service port: {port}");
        anyhow::ensure!(
            !util::is_port_in_used(*port),
            "service port {port} is already in use"
        );
    }
    info!(
        "Service ports. grpc: {}. http: {}. urpc: {:?}",
        config.grpc_port, config.http_port, config.urpc_port
    );
    Ok(())
}
