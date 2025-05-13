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

mod admin;
mod apps;
mod await_tree;
mod historical_apps;
pub mod http_service;
mod metrics;
mod profile_cpu;
mod profile_heap;

use crate::config::Config;
use crate::http::await_tree::AwaitTreeHandler;
use crate::http::http_service::PoemHTTPServer;
use crate::http::metrics::MetricsHTTPHandler;
use crate::http::profile_cpu::ProfileCpuHandler;
use crate::runtime::manager::RuntimeManager;

use crate::app::AppManagerRef;
use crate::http::admin::AdminHandler;
use crate::http::apps::AppsHandler;
use crate::http::historical_apps::HistoricalAppsHandler;
use crate::http::profile_heap::ProfileHeapHandler;
use log::info;
use poem::RouteMethod;
use serde::{Deserialize, Serialize};

pub struct HttpMonitorService;
impl HttpMonitorService {
    pub fn init(config: &Config, runtime_manager: RuntimeManager) -> Box<PoemHTTPServer> {
        let http_port = config.http_port;
        println!(
            "Starting http monitor service with port:[{}] ......",
            http_port
        );
        let server = new_server();
        server.start(runtime_manager, http_port);
        server
    }
}

/// Implement the own handlers for concrete components
pub trait Handler {
    fn get_route_method(&self) -> RouteMethod;
    fn get_route_path(&self) -> String;
}

pub trait HTTPServer: Send + Sync {
    fn start(&self, runtime_manager: RuntimeManager, port: u16);
    fn register_handler(&self, handler: impl Handler + 'static);
}

fn new_server() -> Box<PoemHTTPServer> {
    let server = PoemHTTPServer::new();

    server.register_handler(ProfileCpuHandler::default());
    server.register_handler(ProfileHeapHandler::default());

    server.register_handler(MetricsHTTPHandler::default());
    server.register_handler(AwaitTreeHandler::default());
    server.register_handler(AppsHandler::default());
    server.register_handler(HistoricalAppsHandler::default());
    server.register_handler(AdminHandler::default());

    Box::new(server)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
enum Format {
    Pprof,
    Svg,
}
