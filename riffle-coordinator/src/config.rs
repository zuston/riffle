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

use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub grpc_port: u16,
    pub http_port: u16,
    pub heartbeat_timeout_seconds: i64,
    pub node_expiry_check_interval_seconds: i64,
    pub memory_weight: f64,
    pub partition_weight: f64,
    pub default_remote_storage_path: Option<String>,
    pub client_conf: Vec<(String, String)>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            grpc_port: 19999,
            http_port: 19998,
            heartbeat_timeout_seconds: 60,
            node_expiry_check_interval_seconds: 30,
            memory_weight: 1.0,
            partition_weight: 1000.0,
            default_remote_storage_path: None,
            client_conf: vec![],
        }
    }
}
