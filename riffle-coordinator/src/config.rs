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

use clap::builder::Str;
use riffle_server::config::LogConfig;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Config {
    #[serde(default = "as_default_grpc_port")]
    pub grpc_port: u16,

    #[serde(default = "as_default_http_port")]
    pub http_port: u16,

    #[serde(default = "as_default_node_heartbeat_timeout_seconds")]
    pub node_heartbeat_timeout_seconds: usize,

    #[serde(default = "as_default_node_expiry_check_interval_seconds")]
    pub node_expiry_check_interval_seconds: usize,

    pub log: Option<LogConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            grpc_port: as_default_grpc_port(),
            http_port: as_default_http_port(),
            node_heartbeat_timeout_seconds: as_default_node_heartbeat_timeout_seconds(),
            node_expiry_check_interval_seconds: as_default_node_expiry_check_interval_seconds(),
            log: None,
        }
    }
}

fn as_default_node_expiry_check_interval_seconds() -> usize {
    20
}

fn as_default_node_heartbeat_timeout_seconds() -> usize {
    60
}

fn as_default_grpc_port() -> u16 {
    20010
}

fn as_default_http_port() -> u16 {
    20020
}

impl Config {
    pub fn from(cfg_path: &Option<String>) -> Self {
        if let Some(path) = cfg_path {
            let path = Path::new(path);

            // Read the file content as a string
            let file_content = fs::read_to_string(path).expect("Failed to read file");

            toml::from_str(&file_content).unwrap()
        } else {
            Self::default()
        }
    }
}
