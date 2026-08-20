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
    pub grpc_port: usize,
    pub netty_port: usize,
    pub http_port: usize,

    // Resource state
    pub used_memory: usize,
    pub free_memory: usize,
    pub reserved_memory: usize,
    pub event_num_in_flush: usize,

    // Tags for filtering
    pub tags: Vec<String>,

    // Health status
    // todo: could be removed after heartbeat.rs to mark status as unhealthy
    pub is_healthy: bool,
    pub status: ServerStatus,

    // Storage information
    pub storage_info: HashMap<String, StorageInfo>,

    // Metadata
    pub version: Option<String>,
    pub git_commit_id: Option<String>,
    pub server_start_time: DateTime<Utc>,
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
    /// Check if the node is available for assignment
    pub fn is_available(&self) -> bool {
        self.is_healthy && self.status == ServerStatus::Active && self.free_memory > 0
    }

    /// Check if the node matches the required tags
    pub fn matches_tags(&self, required_tags: &[String]) -> bool {
        required_tags.iter().all(|tag| self.tags.contains(tag))
    }
}
