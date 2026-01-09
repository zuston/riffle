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

mod cluster;
mod config;
mod grpc;

use crate::config::Config;
use crate::grpc::protobuf::uniffle::coordinator_server_server::CoordinatorServerServer;
use crate::grpc::service::DefaultCoordinatorServer;
use clap::Parser;
use log::{info, LevelFilter};
use logforth::append;
use riffle_server::log_service::LogService;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tonic::transport::Server;

#[derive(Parser, Debug)]
#[command(name = "riffle-coordinator")]
#[command(author = "Apache Software Foundation")]
#[command(version = "0.20.0")]
#[command(about = "Riffle Coordinator - Uniffle Shuffle Service Coordinator", long_about = None)]
struct Args {
    /// Configuration file path
    #[arg[short, long]]
    config: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let config = Config::from(&args.config);

    // Initialize logging
    let _log_guard = match &config.log {
        None => {
            logforth::builder()
                .dispatch(|d| {
                    d.filter(LevelFilter::Info)
                        .append(append::Stdout::default())
                })
                .apply();
            None
        }
        Some(log_config) => {
            let _guard = LogService::init(log_config);
            Some(_guard)
        }
    };

    // Initialize ClusterManager
    let cluster_manager = cluster::ClusterManager::new(config.clone());

    // Create gRPC service
    let coordinator_server = DefaultCoordinatorServer::new(cluster_manager.clone());
    let service = CoordinatorServerServer::new(coordinator_server)
        .max_decoding_message_size(usize::MAX)
        .max_encoding_message_size(usize::MAX);

    // Build socket address
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), config.grpc_port);

    // Start gRPC server
    info!("Starting gRPC server on {}", addr);
    Server::builder().add_service(service).serve(addr).await?;

    Ok(())
}
