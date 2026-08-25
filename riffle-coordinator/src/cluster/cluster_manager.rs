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

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use log::{debug, info};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::assignment::{
    create_assignment_strategy, AssignmentError, AssignmentOptions, AssignmentStrategy,
    PartitionAssignment,
};
use super::server_node::{ServerStatus, ShuffleServerNode};
use crate::config::Config;

pub type ClusterManagerRef = Arc<ClusterManager>;

#[derive(Clone, Debug)]
pub struct AssignmentRequest {
    pub app_id: String,
    pub shuffle_id: i32,
    pub partition_num: usize,
    pub partition_num_per_range: usize,
    pub data_replica: usize,
    pub required_tags: Vec<String>,
    pub required_server_num: Option<usize>,
    pub estimate_task_concurrency: usize,
    pub exclusive_server_ids: HashSet<String>,
}

#[derive(Clone, Debug)]
pub struct AssignmentResult {
    pub assignments: Vec<PartitionAssignment>,
    pub servers: HashMap<String, ShuffleServerNode>,
}

#[derive(Clone, Debug)]
pub struct NodeHeartbeatInfo {
    // static info
    pub ip: String,
    pub server_id: String,
    pub tags: Vec<String>,
    pub version: Option<String>,
    pub git_commit_id: Option<String>,
    pub start_time_ms: Option<i64>,
    pub grpc_port: usize,
    pub urpc_port: usize,
    pub http_port: usize,

    // dynamic info
    pub used_memory: usize,
    pub free_memory: usize,
    pub reserved_memory: usize,
    pub event_num_in_flush: usize,
    pub is_healthy: bool,
    pub status: ServerStatus,
    pub storage_info: HashMap<String, super::server_node::StorageInfo>,
}

pub struct ClusterManager {
    servers: DashMap<String, ShuffleServerNode>,
    assignment_strategy: Box<dyn AssignmentStrategy>,
    exclusive_tags: HashSet<String>,
}

impl ClusterManager {
    pub fn new(config: &Config) -> ClusterManagerRef {
        let assignment_strategy = create_assignment_strategy(config);
        Arc::new(Self {
            servers: DashMap::new(),
            assignment_strategy,
            exclusive_tags: config.exclusive_tags.iter().cloned().collect(),
        })
    }

    pub fn heartbeat(&self, heartbeat: NodeHeartbeatInfo) {
        let now = Utc::now();
        let server_start_time = heartbeat
            .start_time_ms
            .and_then(DateTime::<Utc>::from_timestamp_millis);

        self.servers
            .entry(heartbeat.server_id.clone())
            .and_modify(|node| {
                node.ip = heartbeat.ip.clone();
                node.grpc_port = heartbeat.grpc_port;
                node.netty_port = heartbeat.urpc_port;
                node.http_port = heartbeat.http_port;
                node.used_memory = heartbeat.used_memory;
                node.free_memory = heartbeat.free_memory;
                node.reserved_memory = heartbeat.reserved_memory;
                node.event_num_in_flush = heartbeat.event_num_in_flush;
                node.tags = heartbeat.tags.clone();
                node.is_healthy = heartbeat.is_healthy;
                node.status = heartbeat.status.clone();
                node.storage_info = heartbeat.storage_info.clone();
                node.version = heartbeat.version.clone();
                node.git_commit_id = heartbeat.git_commit_id.clone();
                if let Some(server_start_time) = server_start_time.clone() {
                    node.server_start_time = server_start_time;
                }
            })
            .or_insert_with(|| {
                info!(
                    "New shuffle server registered: {} ({})",
                    heartbeat.server_id, heartbeat.ip
                );
                ShuffleServerNode {
                    id: heartbeat.server_id.clone(),
                    ip: heartbeat.ip,
                    grpc_port: heartbeat.grpc_port,
                    netty_port: heartbeat.urpc_port,
                    http_port: heartbeat.http_port,
                    used_memory: heartbeat.used_memory,
                    free_memory: heartbeat.free_memory,
                    reserved_memory: heartbeat.reserved_memory,
                    event_num_in_flush: heartbeat.event_num_in_flush,
                    tags: heartbeat.tags,
                    is_healthy: heartbeat.is_healthy,
                    status: heartbeat.status,
                    storage_info: heartbeat.storage_info,
                    version: heartbeat.version,
                    git_commit_id: heartbeat.git_commit_id,
                    server_start_time: server_start_time.unwrap_or(now),
                }
            });
    }

    /// Return every registered shuffle server.
    pub fn list_all(&self) -> Vec<ShuffleServerNode> {
        self.servers
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    pub fn list_available(&self, required_tags: &[String]) -> Vec<ShuffleServerNode> {
        self.servers
            .iter()
            .filter(|entry| {
                let node = entry.value();
                node.is_available() && node.matches_tags(required_tags, &self.exclusive_tags)
            })
            .map(|entry| entry.value().clone())
            .collect()
    }

    pub fn assign(&self, request: AssignmentRequest) -> Result<AssignmentResult, AssignmentError> {
        debug!(
            "Assigning shuffle servers for app_id={}, shuffle_id={}",
            request.app_id, request.shuffle_id
        );

        let available_servers: Vec<ShuffleServerNode> = self
            .list_available(&request.required_tags)
            .into_iter()
            .filter(|node| !request.exclusive_server_ids.contains(&node.id))
            .collect();

        if available_servers.is_empty() {
            return Err(AssignmentError::NoAvailableServers);
        }

        let options = AssignmentOptions {
            partition_num: request.partition_num,
            partition_num_per_range: request.partition_num_per_range,
            data_replica: request.data_replica,
            required_server_num: request.required_server_num,
            estimate_task_concurrency: request.estimate_task_concurrency,
        };
        let assignments = self
            .assignment_strategy
            .assign(&available_servers, &options)?;
        let servers = available_servers
            .into_iter()
            .map(|server| (server.id.clone(), server))
            .collect();

        Ok(AssignmentResult {
            assignments,
            servers,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn heartbeat(server_id: &str, tags: &[&str]) -> NodeHeartbeatInfo {
        NodeHeartbeatInfo {
            ip: "127.0.0.1".to_string(),
            server_id: server_id.to_string(),
            tags: tags.iter().map(|tag| (*tag).to_string()).collect(),
            version: None,
            git_commit_id: None,
            start_time_ms: None,
            grpc_port: 1,
            urpc_port: 2,
            http_port: 3,
            used_memory: 0,
            free_memory: 1,
            reserved_memory: 0,
            event_num_in_flush: 0,
            is_healthy: true,
            status: ServerStatus::Active,
            storage_info: HashMap::new(),
        }
    }

    #[test]
    fn filters_unrequested_exclusive_tags() {
        let config = Config {
            exclusive_tags: vec!["gpu".to_string()],
            ..Config::default()
        };
        let manager = ClusterManager::new(&config);
        manager.heartbeat(heartbeat("gpu-1", &["gpu"]));
        manager.heartbeat(heartbeat("general-1", &["cpu"]));

        let no_required_tags = Vec::new();
        let available_without_tags: HashSet<_> = manager
            .list_available(&no_required_tags)
            .into_iter()
            .map(|node| node.id)
            .collect();
        assert_eq!(
            available_without_tags,
            HashSet::from(["general-1".to_string()])
        );

        let required_tags = vec!["gpu".to_string()];
        let available_with_gpu: HashSet<_> = manager
            .list_available(&required_tags)
            .into_iter()
            .map(|node| node.id)
            .collect();
        assert_eq!(available_with_gpu, HashSet::from(["gpu-1".to_string()]));
    }
}
