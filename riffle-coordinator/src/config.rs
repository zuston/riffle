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

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct LogConfig {
    pub path: String,
    #[serde(default = "default_rotation")]
    pub rotation: RotationConfig,
    #[serde(default = "default_max_file_size")]
    pub max_file_size: String,
    #[serde(default = "default_max_log_files")]
    pub max_log_files: usize,
    #[serde(default = "default_log_level")]
    pub log_level: LogLevel,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RotationConfig {
    Hourly,
    Daily,
    Never,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum LogLevel {
    DEBUG,
    INFO,
    WARN,
}

fn default_rotation() -> RotationConfig {
    RotationConfig::Daily
}

fn default_max_file_size() -> String {
    "512M".to_string()
}

fn default_max_log_files() -> usize {
    10
}

fn default_log_level() -> LogLevel {
    LogLevel::INFO
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AssignmentStrategyType {
    Basic,
    #[default]
    PartitionBalance,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HostAssignmentStrategy {
    MustDiff,
    #[default]
    PreferDiff,
    None,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionAssignmentStrategy {
    Round,
    #[default]
    Continuous,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Config {
    #[serde(default = "default_grpc_port")]
    pub grpc_port: u16,

    #[serde(default = "default_http_port")]
    pub http_port: u16,

    #[serde(default = "default_shuffle_nodes_max")]
    pub max_assignment_servers: usize,

    #[serde(default)]
    pub assignment_strategy: AssignmentStrategyType,

    #[serde(default)]
    pub assignment_host_strategy: HostAssignmentStrategy,

    #[serde(default)]
    pub select_partition_strategy: PartitionAssignmentStrategy,

    pub log: Option<LogConfig>,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read coordinator config {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to parse coordinator config {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },

    #[error("invalid coordinator config: {0}")]
    Invalid(String),
}

impl Default for Config {
    fn default() -> Self {
        Self {
            grpc_port: default_grpc_port(),
            http_port: default_http_port(),
            max_assignment_servers: default_shuffle_nodes_max(),
            assignment_strategy: AssignmentStrategyType::default(),
            assignment_host_strategy: HostAssignmentStrategy::default(),
            select_partition_strategy: PartitionAssignmentStrategy::default(),
            log: None,
        }
    }
}

impl Config {
    pub fn load(cfg_path: Option<&str>) -> Result<Self, ConfigError> {
        let config = match cfg_path {
            Some(path) => {
                let path = Path::new(path);
                let file_content =
                    fs::read_to_string(path).map_err(|source| ConfigError::Read {
                        path: path.to_path_buf(),
                        source,
                    })?;
                toml::from_str(&file_content).map_err(|source| ConfigError::Parse {
                    path: path.to_path_buf(),
                    source,
                })?
            }
            None => Self::default(),
        };

        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        validate_positive("shuffle_nodes_max", self.max_assignment_servers)?;
        Ok(())
    }
}

fn validate_positive(name: &str, value: usize) -> Result<(), ConfigError> {
    if value == 0 {
        return Err(ConfigError::Invalid(format!("{name} must be positive")));
    }
    Ok(())
}

fn default_shuffle_nodes_max() -> usize {
    9
}

fn default_grpc_port() -> u16 {
    20010
}

fn default_http_port() -> u16 {
    20020
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minimal_config_uses_uniffle_compatible_assignment_defaults() {
        let config: Config = toml::from_str("grpc_port = 21000").unwrap();

        assert_eq!(config.grpc_port, 21000);
        assert_eq!(config.max_assignment_servers, 9);
        assert_eq!(
            config.assignment_strategy,
            AssignmentStrategyType::PartitionBalance
        );
        assert_eq!(
            config.assignment_host_strategy,
            HostAssignmentStrategy::PreferDiff
        );
        assert_eq!(
            config.select_partition_strategy,
            PartitionAssignmentStrategy::Continuous
        );
    }

    #[test]
    fn rejects_zero_limits() {
        let config = Config {
            max_assignment_servers: 0,
            ..Config::default()
        };

        assert!(matches!(config.validate(), Err(ConfigError::Invalid(_))));
    }
}
