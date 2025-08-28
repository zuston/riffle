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

use crate::error::WorkerError;

use poem::endpoint::make_sync;
use poem::error::ResponseError;
use poem::http::StatusCode;
use poem::listener::TcpListener;
use poem::{get, Route, RouteMethod, Server};

use crate::constant::CPU_ARCH;
use crate::http::{HTTPServer, Handler};
use crate::runtime::manager::RuntimeManager;
use crate::util::is_port_in_used;
use await_tree::{InstrumentAwait, SpanExt};
use std::sync::Mutex;

impl ResponseError for WorkerError {
    fn status(&self) -> StatusCode {
        StatusCode::BAD_REQUEST
    }
}

fn format_kv_pairs(data: Vec<(&str, &str)>) -> String {
    let max_key_length = data.iter().map(|(key, _)| key.len()).max().unwrap_or(0);
    let formatted_lines: Vec<String> = data
        .iter()
        .map(|(key, value)| format!("{:<width$}: {}", key, value, width = max_key_length))
        .collect();
    formatted_lines.join("\n")
}

struct IndexPageHandler {}
impl Handler for IndexPageHandler {
    fn get_route_method(&self) -> RouteMethod {
        get(make_sync(|_| {
            let infos = vec![
                ("git_commit_id", env!("GIT_COMMIT_HASH")),
                ("cpu_arch", CPU_ARCH),
                ("version", env!("CARGO_PKG_VERSION")),
                ("build_date", env!("BUILD_DATE")),
            ];
            format_kv_pairs(infos)
        }))
    }

    fn get_route_path(&self) -> String {
        "/".to_string()
    }
}

pub struct PoemHTTPServer {
    handlers: Mutex<Vec<Box<dyn Handler>>>,
}

unsafe impl Send for PoemHTTPServer {}
unsafe impl Sync for PoemHTTPServer {}

impl PoemHTTPServer {
    pub fn new() -> Self {
        let handlers: Vec<Box<dyn Handler>> = vec![Box::new(IndexPageHandler {})];
        Self {
            handlers: Mutex::new(handlers),
        }
    }
}

impl HTTPServer for PoemHTTPServer {
    fn start(&self, runtime_manager: RuntimeManager, port: u16) {
        if is_port_in_used(port) {
            panic!("The http service port:{:?} has been used.", port);
        }
        let mut app = Route::new();
        let handlers = self.handlers.lock().unwrap();
        for handler in handlers.iter() {
            app = app.at(handler.get_route_path(), handler.get_route_method());
        }
        runtime_manager
            .http_runtime
            .spawn_with_await_tree("Http service", async move {
                let _ = Server::new(TcpListener::bind(format!("0.0.0.0:{}", port)))
                    .name("uniffle-server-http-service")
                    .run(app)
                    .instrument_await("listening".long_running())
                    .await;
            });
    }

    fn register_handler(&self, handler: impl Handler + 'static) {
        let mut handlers = self.handlers.lock().unwrap();
        handlers.push(Box::new(handler));
    }
}
