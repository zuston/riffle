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

use chrono::{DateTime, Duration, Utc};
use dashmap::DashMap;
use log::{debug, info, warn};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

use super::application::ApplicationInfo;
use super::assignment::{
    AssignmentError, AssignmentStrategy, PartitionAssignment, WeightedAssignment,
};
use super::server_node::{ServerStatus, ShuffleServerNode};
use crate::config::Config;

pub type ClusterManagerRef = Arc<ClusterManager>;

/// Configuration for the cluster manager
#[derive(Clone, Debug)]
pub struct ClusterConfig {
    pub heartbeat_timeout_seconds: i64,
    pub node_expiry_check_interval_seconds: i64,
    pub memory_weight: f64,
    pub partition_weight: f64,
    pub default_remote_storage_path: Option<String>,
    pub client_conf: Vec<(String, String)>,
}

impl From<Config> for ClusterConfig {
    fn from(config: Config) -> Self {
        Self {
            heartbeat_timeout_seconds: config.heartbeat_timeout_seconds,
            node_expiry_check_interval_seconds: config.node_expiry_check_interval_seconds,
            memory_weight: config.memory_weight,
            partition_weight: config.partition_weight,
            default_remote_storage_path: config.default_remote_storage_path,
            client_conf: config.client_conf,
        }
    }
}

/// Request for shuffle assignment
#[derive(Clone, Debug)]
pub struct AssignmentRequest {
    pub app_id: String,
    pub shuffle_id: i32,
    pub partition_num: i32,
    pub partition_num_per_range: i32,
    pub data_replica: i32,
    pub require_tags: Vec<String>,
    pub assignment_server_num: i32,
}

/// Result of shuffle assignment
#[derive(Clone, Debug)]
pub struct AssignmentResult {
    pub app_id: String,
    pub shuffle_id: i32,
    pub assignments: Vec<PartitionAssignment>,
}

/// Heartbeat information from shuffle server
#[derive(Clone, Debug)]
pub struct HeartbeatInfo {
    pub server_id: String,
    pub ip: String,
    pub grpc_port: i32,
    pub netty_port: i32,
    pub http_port: i32,
    pub used_memory: i64,
    pub available_memory: i64,
    pub pre_allocated_memory: i64,
    pub event_num_in_flush: i32,
    pub tags: Vec<String>,
    pub is_healthy: bool,
    pub status: ServerStatus,
    pub storage_info: HashMap<String, super::server_node::StorageInfo>,
    pub version: Option<String>,
    pub git_commit_id: Option<String>,
    pub start_time_ms: Option<i64>,
}

/// Core cluster manager that tracks all shuffle servers and applications
pub struct ClusterManager {
    /// Shuffle servers indexed by server ID
    servers: DashMap<String, ShuffleServerNode>,

    /// Registered applications indexed by app ID
    applications: DashMap<String, ApplicationInfo>,

    /// Configuration
    config: ClusterConfig,

    /// Assignment strategy
    assignment_strategy: Arc<dyn AssignmentStrategy>,
}

impl ClusterManager {
    /// Create a new ClusterManager with the given configuration
    pub fn new(config: Config) -> ClusterManagerRef {
        let cluster_config = ClusterConfig::from(config);
        let assignment_strategy = Arc::new(WeightedAssignment::new(
            cluster_config.memory_weight,
            cluster_config.partition_weight,
        ));

        Arc::new(Self {
            servers: DashMap::new(),
            applications: DashMap::new(),
            config: cluster_config,
            assignment_strategy,
        })
    }

    // ==================== Heartbeat Handling ====================

    /// Handle heartbeat from a shuffle server
    /// Registers new servers or updates existing server state
    pub fn handle_heartbeat(&self, heartbeat: HeartbeatInfo) {
        let now = Utc::now();

        self.servers
            .entry(heartbeat.server_id.clone())
            .and_modify(|node| {
                // Update existing node information
                node.used_memory = heartbeat.used_memory;
                node.available_memory = heartbeat.available_memory;
                node.pre_allocated_memory = heartbeat.pre_allocated_memory;
                node.event_num_in_flush = heartbeat.event_num_in_flush;
                node.tags = heartbeat.tags.clone();
                node.is_healthy = heartbeat.is_healthy;
                node.status = heartbeat.status.clone();
                node.storage_info = heartbeat.storage_info.clone();
                node.last_heartbeat = now;
            })
            .or_insert_with(|| {
                // New server registration
                info!(
                    "New shuffle server registered: {} ({})",
                    heartbeat.server_id, heartbeat.ip
                );
                ShuffleServerNode {
                    id: heartbeat.server_id.clone(),
                    ip: heartbeat.ip,
                    grpc_port: heartbeat.grpc_port,
                    netty_port: heartbeat.netty_port,
                    http_port: heartbeat.http_port,
                    used_memory: heartbeat.used_memory,
                    available_memory: heartbeat.available_memory,
                    pre_allocated_memory: heartbeat.pre_allocated_memory,
                    event_num_in_flush: heartbeat.event_num_in_flush,
                    assigned_partition_count: 0,
                    tags: heartbeat.tags,
                    is_healthy: heartbeat.is_healthy,
                    status: heartbeat.status,
                    storage_info: heartbeat.storage_info,
                    version: heartbeat.version,
                    git_commit_id: heartbeat.git_commit_id,
                    start_time_ms: heartbeat.start_time_ms,
                    last_heartbeat: now,
                    registration_time: now,
                }
            });
    }

    // ==================== Server Query ====================

    /// Get list of all available shuffle servers
    pub fn get_shuffle_server_list(&self) -> Vec<ShuffleServerNode> {
        self.servers
            .iter()
            .filter(|entry| entry.value().is_available_for_assignment())
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get count of available shuffle servers
    pub fn get_shuffle_server_num(&self) -> i32 {
        self.servers
            .iter()
            .filter(|entry| entry.value().is_available_for_assignment())
            .count() as i32
    }

    /// Get a server by ID
    pub fn get_server_by_id(&self, server_id: &str) -> Option<ShuffleServerNode> {
        self.servers.get(server_id).map(|entry| entry.clone())
    }

    // ==================== Assignment ====================

    /// Get shuffle assignments for a request
    pub fn get_shuffle_assignments(
        &self,
        request: AssignmentRequest,
    ) -> Result<AssignmentResult, AssignmentError> {
        // 1. Filter available servers by required tags
        let available_servers: Vec<ShuffleServerNode> = self
            .servers
            .iter()
            .filter(|entry| {
                let node = entry.value();
                node.is_available_for_assignment() && node.matches_tags(&request.require_tags)
            })
            .map(|entry| entry.value().clone())
            .collect();

        if available_servers.is_empty() {
            return Err(AssignmentError::NoAvailableServers);
        }

        // 2. Apply assignment strategy
        let assignments = self.assignment_strategy.assign(
            &available_servers,
            request.partition_num,
            request.partition_num_per_range,
            request.data_replica,
            request.assignment_server_num,
        )?;

        // 3. Update partition count for selected servers
        for assignment in &assignments {
            let partition_count =
                (assignment.end_partition - assignment.start_partition + 1) as i64;
            for server_id in &assignment.server_ids {
                if let Some(mut node) = self.servers.get_mut(server_id) {
                    node.assigned_partition_count += partition_count;
                }
            }
        }

        Ok(AssignmentResult {
            app_id: request.app_id,
            shuffle_id: request.shuffle_id,
            assignments,
        })
    }

    // ==================== Application Management ====================

    /// Register a new application
    pub fn register_application(&self, app_id: String, user: String) {
        self.applications.entry(app_id.clone()).or_insert_with(|| {
            info!("Application registered: {} (user: {})", app_id, user);
            ApplicationInfo::new(app_id, user)
        });
    }

    /// Handle application heartbeat
    pub fn app_heartbeat(&self, app_id: &str) {
        let now = Utc::now();
        if let Some(mut app) = self.applications.get_mut(app_id) {
            app.last_heartbeat = now;
        }
        // If app doesn't exist, auto-register it
        else {
            self.register_application(app_id.to_string(), "unknown".to_string());
        }
    }

    // ==================== Cleanup ====================

    /// Remove expired nodes based on heartbeat timeout
    pub fn cleanup_expired_nodes(&self) {
        let timeout = Duration::seconds(self.config.heartbeat_timeout_seconds);
        let now = Utc::now();

        self.servers.retain(|id, node| {
            let expired = now - node.last_heartbeat > timeout;
            if expired {
                warn!("Removing expired shuffle server: {}", id);
            }
            !expired
        });
    }

    /// Remove expired applications
    pub fn cleanup_expired_applications(&self) {
        let timeout = Duration::seconds(self.config.heartbeat_timeout_seconds * 2);
        let now = Utc::now();

        self.applications.retain(|id, app| {
            let expired = now - app.last_heartbeat > timeout;
            if expired {
                warn!("Removing expired application: {}", id);
            }
            !expired
        });
    }

    // ==================== Getters ====================

    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    pub fn server_count(&self) -> usize {
        self.servers.len()
    }

    pub fn application_count(&self) -> usize {
        self.applications.len()
    }
}
