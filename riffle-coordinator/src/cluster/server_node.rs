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
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents a shuffle server node in the cluster
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShuffleServerNode {
    pub id: String,
    pub ip: String,
    pub grpc_port: i32,
    pub netty_port: i32,
    pub http_port: i32,

    // Resource state
    pub used_memory: i64,
    pub available_memory: i64,
    pub pre_allocated_memory: i64,
    pub event_num_in_flush: i32,

    // Assigned partition count (for weighted assignment)
    pub assigned_partition_count: i64,

    // Timestamp for detecting server restarts
    pub timestamp: i64,

    // Tags for filtering
    pub tags: Vec<String>,

    // Health status
    pub is_healthy: bool,
    pub status: ServerStatus,

    // Storage information
    pub storage_info: HashMap<String, StorageInfo>,

    // Metadata
    pub version: Option<String>,
    pub git_commit_id: Option<String>,
    pub start_time_ms: Option<i64>,

    // Timestamps
    pub last_heartbeat: DateTime<Utc>,
    pub registration_time: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServerStatus {
    Active,
    Decommissioning,
    Decommissioned,
    Lost,
    Unhealthy,
    Excluded,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageInfo {
    pub mount_point: String,
    pub storage_media: StorageMedia,
    pub capacity: i64,
    pub used_bytes: i64,
    pub status: StorageStatus,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StorageMedia {
    StorageTypeUnknown,
    Hdd,
    Ssd,
    Hdfs,
    ObjectStore,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StorageStatus {
    StorageStatusUnknown,
    Normal,
    Unhealthy,
    Overused,
}

impl ShuffleServerNode {
    /// Calculate the score for weighted assignment
    /// Higher score means more suitable for assignment
    pub fn calculate_score(&self, memory_weight: f64, partition_weight: f64) -> f64 {
        let memory_score = self.available_memory as f64;
        let partition_penalty = self.assigned_partition_count as f64;

        // Higher available memory = higher score
        // Higher partition count = lower score
        memory_weight * memory_score - partition_weight * partition_penalty
    }

    /// Check if the node is available for assignment
    pub fn is_available_for_assignment(&self) -> bool {
        self.is_healthy && self.status == ServerStatus::Active && self.available_memory > 0
    }

    /// Check if the node matches the required tags
    pub fn matches_tags(&self, required_tags: &[String]) -> bool {
        if required_tags.is_empty() {
            return true;
        }
        required_tags.iter().all(|tag| self.tags.contains(tag))
    }
}
