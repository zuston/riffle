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

#![feature(impl_trait_in_assoc_type)]

use crate::app::{AppManager, SHUFFLE_SERVER_ID};
use crate::config::Config;
use crate::http::{HTTPServer, HTTP_SERVICE};
use crate::log_service::LogService;
use crate::mem_allocator::ALLOCATOR;
use crate::metric::init_metric_service;
use crate::readable_size::ReadableSize;
use crate::rpc::DefaultRpcService;
use crate::runtime::manager::RuntimeManager;
use crate::tracing::FastraceWrapper;
use crate::util::generate_worker_uid;

use crate::heartbeat::HeartbeatTask;
use anyhow::Result;
use clap::{App, Arg};
use log::info;
use std::str::FromStr;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub mod app;
mod await_tree;
pub mod config;
pub mod constant;
mod error;
pub mod grpc;
pub mod heartbeat;
mod http;
mod log_service;
mod mem_allocator;
mod metric;
mod readable_size;
pub mod rpc;
pub mod runtime;
pub mod signal;
pub mod store;
pub mod tracing;
pub mod urpc;
pub mod util;

const MAX_MEMORY_ALLOCATION_SIZE_ENV_KEY: &str = "MAX_MEMORY_ALLOCATION_SIZE";

fn main() -> Result<()> {
    setup_max_memory_allocation();

    let args_match = App::new("Uniffle Worker")
        .version("0.9.0-SNAPSHOT")
        .about("Rust based shuffle server for Apache Uniffle")
        .arg(
            Arg::with_name("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .default_value("./config.toml")
                .help("Sets a custom config file")
                .takes_value(true),
        )
        .get_matches();

    let config_path = args_match.value_of("config").unwrap_or("./config.toml");
    let config = Config::from(config_path);

    let _guard = LogService::init(&config.log.clone().unwrap_or(Default::default()));

    let worker_uid = generate_worker_uid(&config);
    // todo: remove some unnecessary worker_id transfer.
    SHUFFLE_SERVER_ID.get_or_init(|| worker_uid.clone());

    let runtime_manager = RuntimeManager::from(config.runtime_config.clone());
    let app_manager_ref = AppManager::get_ref(runtime_manager.clone(), config.clone());

    let metric_config = config.metrics.clone();
    init_metric_service(runtime_manager.clone(), &metric_config, worker_uid.clone());

    FastraceWrapper::init(config.clone());

    HeartbeatTask::init(&config, runtime_manager.clone(), app_manager_ref.clone());

    let http_port = config.http_monitor_service_port.unwrap_or(20010);
    info!(
        "Starting http monitor service with port:[{}] ......",
        http_port
    );
    HTTP_SERVICE.start(runtime_manager.clone(), http_port);

    DefaultRpcService {}.start(&config, runtime_manager, app_manager_ref)?;
    Ok(())
}

fn setup_max_memory_allocation() {
    let _ = std::env::var(MAX_MEMORY_ALLOCATION_SIZE_ENV_KEY).map(|v| {
        let readable_size = ReadableSize::from_str(v.as_str()).unwrap();
        ALLOCATOR.set_limit(readable_size.as_bytes() as usize)
    });
}
