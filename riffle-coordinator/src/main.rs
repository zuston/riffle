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

use clap::Parser;
use log::info;
use riffle_coordinator::config::Config;
use riffle_coordinator::grpc::protobuf::uniffle::coordinator_server_server::CoordinatorServerServer;
use riffle_coordinator::grpc::service::DefaultCoordinatorServer;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tonic::transport::Server;

#[derive(Parser, Debug)]
#[command(name = "riffle-coordinator")]
#[command(author = "Apache Software Foundation")]
#[command(version = "0.20.0")]
#[command(about = "Riffle Coordinator - Uniffle Shuffle Service Coordinator", long_about = None)]
struct Args {
    /// gRPC port to listen on
    #[arg(long, default_value = "19999")]
    grpc_port: u16,

    /// HTTP port for metrics and health checks
    #[arg(long, default_value = "19998")]
    http_port: u16,

    /// Heartbeat timeout in seconds
    #[arg(long, default_value = "60")]
    heartbeat_timeout_seconds: i64,

    /// Node expiry check interval in seconds
    #[arg(long, default_value = "30")]
    node_expiry_check_interval_seconds: i64,

    /// Memory weight for assignment strategy
    #[arg(long, default_value = "1.0")]
    memory_weight: f64,

    /// Partition weight for assignment strategy
    #[arg(long, default_value = "1000.0")]
    partition_weight: f64,

    /// Default remote storage path
    #[arg(long)]
    default_remote_storage_path: Option<String>,

    /// Configuration file path
    #[arg(long)]
    config_file: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::init();

    // Parse command line arguments
    let args = Args::parse();

    // Load configuration from file if provided
    let mut config = if let Some(config_file) = &args.config_file {
        load_config_from_file(config_file)?
    } else {
        Config::default()
    };

    // Override with command line arguments
    config.grpc_port = args.grpc_port;
    config.http_port = args.http_port;
    config.heartbeat_timeout_seconds = args.heartbeat_timeout_seconds;
    config.node_expiry_check_interval_seconds = args.node_expiry_check_interval_seconds;
    config.memory_weight = args.memory_weight;
    config.partition_weight = args.partition_weight;
    config.default_remote_storage_path = args.default_remote_storage_path;

    info!(
        "Starting Riffle Coordinator on gRPC port: {}",
        config.grpc_port
    );
    info!(
        "Memory weight: {}, Partition weight: {}",
        config.memory_weight, config.partition_weight
    );

    // Initialize ClusterManager
    let cluster_manager = riffle_coordinator::cluster::ClusterManager::new(config.clone());

    // Create gRPC service
    let coordinator_server = DefaultCoordinatorServer::new(cluster_manager.clone());
    let service = CoordinatorServerServer::new(coordinator_server)
        .max_decoding_message_size(usize::MAX)
        .max_encoding_message_size(usize::MAX);

    // Build socket address
    let addr = SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        config.grpc_port as u16,
    );

    // Start gRPC server
    info!("Starting gRPC server on {}", addr);
    Server::builder().add_service(service).serve(addr).await?;

    Ok(())
}

fn load_config_from_file(config_file: &str) -> Result<Config, Box<dyn std::error::Error>> {
    let content = std::fs::read_to_string(config_file)?;
    let config: Config = toml::from_str(&content)?;
    Ok(config)
}
