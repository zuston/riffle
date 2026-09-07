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

pub mod access;
mod application;
mod cluster;
mod config;
mod grpc;

use crate::application::ApplicationManager;
use crate::cluster::ClusterManager;
use crate::config::{Config, LogConfig, RotationConfig};
use crate::grpc::protobuf::uniffle::coordinator_server_server::CoordinatorServerServer;
use crate::grpc::service::DefaultCoordinatorServer;
use clap::Parser;
use log::info;
#[cfg(feature = "logforth")]
use log::LevelFilter;
#[cfg(feature = "logforth")]
use logforth::append;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tonic::transport::Server;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter, Registry};

const LOG_FILE_NAME: &str = "riffle-coordinator.log";

#[derive(Parser, Debug)]
#[command(name = "riffle-coordinator")]
#[command(author = "Apache Software Foundation")]
#[command(version)]
#[command(about = "Riffle Coordinator - Uniffle Shuffle Service Coordinator", long_about = None)]
struct Args {
    /// Configuration file path
    #[arg[short, long]]
    config: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let config = Config::load(args.config.as_deref())?;

    // Initialize logging
    let _log_guard = match &config.log {
        None => {
            #[cfg(feature = "logforth")]
            logforth::builder()
                .dispatch(|d| {
                    d.filter(LevelFilter::Info)
                        .append(append::Stdout::default())
                })
                .apply();
            #[cfg(not(feature = "logforth"))]
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::INFO)
                .init();
            None
        }
        Some(log_config) => {
            let _guard = init_file_logging(log_config);
            Some(_guard)
        }
    };

    let application_manager = ApplicationManager::default();
    let cluster_manager = ClusterManager::new(&config);

    // Create gRPC service
    let coordinator_server = DefaultCoordinatorServer::new(&cluster_manager, &application_manager);
    let service = CoordinatorServerServer::new(coordinator_server)
        .max_decoding_message_size(usize::MAX)
        .max_encoding_message_size(usize::MAX);

    // Build socket address
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), config.grpc_port);

    // Start gRPC server
    info!("Starting gRPC server on {addr}");
    Server::builder().add_service(service).serve(addr).await?;

    Ok(())
}

fn init_file_logging(log: &LogConfig) -> WorkerGuard {
    let file_appender = match log.rotation {
        RotationConfig::Hourly => tracing_appender::rolling::hourly(&log.path, LOG_FILE_NAME),
        RotationConfig::Daily => tracing_appender::rolling::daily(&log.path, LOG_FILE_NAME),
        RotationConfig::Never => tracing_appender::rolling::never(&log.path, LOG_FILE_NAME),
    };
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let formatting_layer = fmt::layer().pretty().with_writer(std::io::stderr);
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
    let file_layer = fmt::layer()
        .with_ansi(false)
        .with_line_number(true)
        .with_writer(non_blocking);

    Registry::default()
        .with(env_filter)
        .with(formatting_layer)
        .with(file_layer)
        .init();

    guard
}
