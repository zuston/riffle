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

use crate::application::ApplicationManager;
use crate::cluster::{ClusterManager, ClusterManagerRef};
use crate::config::Config;
use crate::grpc::protobuf::uniffle::coordinator_server_server::CoordinatorServerServer;
use crate::grpc::service::DefaultCoordinatorServer;
use crate::http;
use anyhow::{Context, Result};
use log::info;
use std::net::{Ipv4Addr, SocketAddr};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tonic::transport::{server::TcpIncoming, Server};

/// A running coordinator. Dropping it requests shutdown of both services.
pub struct Coordinator {
    application_manager: ApplicationManager,
    cluster_manager: ClusterManagerRef,
    grpc_addr: SocketAddr,
    http_addr: SocketAddr,
    shutdown_tx: watch::Sender<()>,
    task: JoinHandle<Result<()>>,
}

impl Coordinator {
    /// Start the gRPC and HTTP services on the current Tokio runtime.
    pub async fn start(config: Config) -> Result<Self> {
        config.validate()?;
        let grpc_listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, config.grpc_port))
            .await
            .context("failed to bind coordinator gRPC listener")?;
        let http_listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, config.http_port))
            .await
            .context("failed to bind coordinator HTTP listener")?;
        let grpc_addr = grpc_listener.local_addr()?;
        let http_addr = http_listener.local_addr()?;
        let incoming = TcpIncoming::from_listener(grpc_listener, true, None)
            .map_err(anyhow::Error::from_boxed)?;

        let application_manager = ApplicationManager::default();
        let cluster_manager = ClusterManager::new(&config);
        let coordinator_server =
            DefaultCoordinatorServer::new(&cluster_manager, &application_manager);
        let service = CoordinatorServerServer::new(coordinator_server)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX);

        let (shutdown_tx, mut grpc_shutdown) = watch::channel(());
        let mut http_shutdown = grpc_shutdown.clone();
        let http_application_manager = application_manager.clone();
        let task = tokio::spawn(async move {
            info!("Starting gRPC server on {grpc_addr}");
            tokio::try_join!(
                async {
                    Server::builder()
                        .add_service(service)
                        .serve_with_incoming_shutdown(incoming, async move {
                            let _ = grpc_shutdown.changed().await;
                        })
                        .await
                        .context("coordinator gRPC server failed")
                },
                async {
                    http::serve(http_application_manager, http_listener, async move {
                        let _ = http_shutdown.changed().await;
                    })
                    .await
                    .context("coordinator HTTP server failed")
                },
            )?;
            Ok(())
        });

        Ok(Self {
            application_manager,
            cluster_manager,
            grpc_addr,
            http_addr,
            shutdown_tx,
            task,
        })
    }

    pub fn application_manager(&self) -> ApplicationManager {
        self.application_manager.clone()
    }

    pub fn cluster_manager(&self) -> ClusterManagerRef {
        self.cluster_manager.clone()
    }

    pub fn grpc_addr(&self) -> SocketAddr {
        self.grpc_addr
    }

    pub fn http_addr(&self) -> SocketAddr {
        self.http_addr
    }

    /// Wait for the services to exit, propagating any background failure.
    pub async fn wait(&mut self) -> Result<()> {
        (&mut self.task).await.context("coordinator task failed")?
    }

    /// Stop accepting requests and wait for existing connections to finish.
    pub async fn shutdown(mut self) -> Result<()> {
        let _ = self.shutdown_tx.send(());
        self.wait().await
    }
}
