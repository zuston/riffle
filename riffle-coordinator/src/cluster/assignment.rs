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
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::server_node::ShuffleServerNode;
use chrono::Utc;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AssignmentError {
    #[error("No available servers for assignment")]
    NoAvailableServers,

    #[error("Insufficient servers for replication: required {required}, available {available}")]
    InsufficientServersForReplication { required: i32, available: usize },

    #[error("Invalid assignment parameters: {message}")]
    InvalidParameters { message: String },
}

/// Result of a partition assignment
#[derive(Clone, Debug)]
pub struct PartitionAssignment {
    pub start_partition: i32,
    pub end_partition: i32,
    pub server_ids: Vec<String>,
}

impl fmt::Display for PartitionAssignment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PartitionRange[{}-{}] -> servers={:?}",
            self.start_partition, self.end_partition, self.server_ids
        )
    }
}

/// Partition assignment information per server node
#[derive(Clone, Debug)]
pub struct PartitionAssignmentInfo {
    pub partition_num: i32,
    pub timestamp: i64,
}

impl PartitionAssignmentInfo {
    pub fn new() -> Self {
        Self {
            partition_num: 0,
            timestamp: Utc::now().timestamp_millis(),
        }
    }

    pub fn reset_partition_num(&mut self) {
        self.partition_num = 0;
    }

    pub fn increment_partition_num(&mut self) {
        self.partition_num += 1;
    }

    pub fn increment_partition_num_by(&mut self, val: i32) {
        self.partition_num += val;
    }

    pub fn get_partition_num(&self) -> i32 {
        self.partition_num
    }

    pub fn get_timestamp(&self) -> i64 {
        self.timestamp
    }

    pub fn set_timestamp(&mut self, timestamp: i64) {
        self.timestamp = timestamp;
    }
}

/// Trait for assignment strategies
pub trait AssignmentStrategy: Send + Sync {
    fn assign(
        &self,
        servers: &[ShuffleServerNode],
        partition_num: usize,
        partition_num_per_range: usize,
        data_replica: usize,
        max_server_num: usize,
        required_tags: &[String],
        exclude_server_ids: &[String],
    ) -> Result<Vec<PartitionAssignment>, AssignmentError>;
}

/// Partition balance assignment strategy that considers both available memory and partition count.
/// This strategy will sequentially process requests (not concurrently) to avoid multiple requests
/// competing for the same shuffle server.
///
/// The strategy chooses shuffle servers that provide the most available memory per partition:
/// `available_memory / (current_partition_count + assign_partitions)`
///
/// Example: With 3 servers S1(2G), S2(5G), S3(1G) and no partitions assigned:
/// - First request: S2 gets partition (highest score: 5G/1)
/// - Second request: S2 gets another (5G/2 vs 2G/1 vs 1G/1)
/// - Third request: S1 gets one (2G/1 vs 5G/3 vs 1G/1)
#[derive(Clone, Debug)]
pub struct PartitionBalanceAssignmentStrategy {
    /// Maps server ID to partition assignment info
    server_to_partitions: Arc<Mutex<HashMap<String, PartitionAssignmentInfo>>>,
}

impl PartitionBalanceAssignmentStrategy {
    pub fn new() -> Self {
        Self {
            server_to_partitions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Update partition info for servers, resetting if timestamp changed
    fn update_partition_info(
        &self,
        servers: &[ShuffleServerNode],
        server_to_partitions: &mut HashMap<String, PartitionAssignmentInfo>,
    ) {
        for node in servers {
            let entry = server_to_partitions
                .entry(node.id.clone())
                .or_insert_with(PartitionAssignmentInfo::new);

            // If node's timestamp is newer than our record, reset partition count
            // This indicates the node was restarted or state was reset
            if entry.timestamp < node.timestamp {
                entry.reset_partition_num();
                entry.set_timestamp(node.timestamp);
            }
        }
    }

    /// Calculate assignment score for a server
    /// Higher score = more suitable for assignment
    fn calculate_score(
        &self,
        node: &ShuffleServerNode,
        partition_info: &PartitionAssignmentInfo,
        assign_partitions: i32,
    ) -> f64 {
        let available_memory = node.available_memory as f64;
        let partition_count = partition_info.get_partition_num() as f64;
        // Score = available_memory / (partition_count + assign_partitions)
        // Higher is better
        if partition_count + assign_partitions as f64 > 0.0 {
            available_memory / (partition_count + assign_partitions as f64)
        } else {
            0.0
        }
    }

    /// Get candidate nodes based on expected count
    fn get_candidate_nodes<'a>(
        &self,
        nodes: &'a [ShuffleServerNode],
        expect_num: usize,
    ) -> Vec<&'a ShuffleServerNode> {
        let count = std::cmp::min(expect_num, nodes.len());
        nodes.iter().take(count).collect()
    }

    /// Generate partition assignments for the given parameters
    fn get_partition_assignment(
        &self,
        total_partition_num: usize,
        partition_num_per_range: usize,
        replica: usize,
        candidate_nodes: &[&ShuffleServerNode],
        _estimate_task_concurrency: i32,
    ) -> Vec<PartitionAssignment> {
        let range_num = (total_partition_num as i64 + partition_num_per_range as i64 - 1)
            / partition_num_per_range as i64;

        let mut assignments = Vec::with_capacity(range_num as usize);

        for range_idx in 0..range_num {
            let start_partition = range_idx * partition_num_per_range as i64;
            let end_partition = std::cmp::min(
                (range_idx + 1) * partition_num_per_range as i64 - 1,
                total_partition_num as i64 - 1,
            );

            // Select servers for replicas using round-robin
            let mut server_ids = Vec::with_capacity(replica);
            for replica_idx in 0..replica {
                let server_idx = ((range_idx as usize + replica_idx) % candidate_nodes.len());
                server_ids.push(candidate_nodes[server_idx].id.clone());
            }

            assignments.push(PartitionAssignment {
                start_partition: start_partition as i32,
                end_partition: end_partition as i32,
                server_ids,
            });
        }

        assignments
    }
}

impl Default for PartitionBalanceAssignmentStrategy {
    fn default() -> Self {
        Self::new()
    }
}

impl AssignmentStrategy for PartitionBalanceAssignmentStrategy {
    fn assign(
        &self,
        servers: &[ShuffleServerNode],
        partition_num: usize,
        partition_num_per_range: usize,
        data_replica: usize,
        max_server_num: usize,
        _required_tags: &[String],
        _exclude_server_ids: &[String],
    ) -> Result<Vec<PartitionAssignment>, AssignmentError> {
        // partition_num_per_range must be 1 for this strategy
        if partition_num_per_range != 1 {
            return Err(AssignmentError::InvalidParameters {
                message: "PartitionNumPerRange must be one".to_string(),
            });
        }

        if servers.is_empty() {
            return Err(AssignmentError::NoAvailableServers);
        }

        if data_replica > servers.len() {
            return Err(AssignmentError::InsufficientServersForReplication {
                required: data_replica as i32,
                available: servers.len(),
            });
        }

        let mut assignments: Vec<PartitionAssignment> = Vec::new();

        // Use mutex for synchronization (similar to Java synchronized block)
        let mut server_to_partitions = self.server_to_partitions.lock().unwrap();

        // Update partition info for each node, resetting if timestamp changed
        self.update_partition_info(servers, &mut server_to_partitions);

        // Calculate assign_partitions: max(averagePartitions, 1)
        // averagePartitions = totalPartitionNum * replica / shuffleNodesMax
        let shuffle_nodes_max = max_server_num.max(1);
        let average_partitions = (partition_num * data_replica) as i32 / shuffle_nodes_max as i32;
        let assign_partitions = std::cmp::max(average_partitions, 1);

        // Sort servers by score in descending order
        let mut sorted_servers: Vec<_> = servers.iter().collect();
        sorted_servers.sort_by(|a, b| {
            let info_a = server_to_partitions
                .get(&a.id)
                .unwrap_or(&PartitionAssignmentInfo::new());
            let info_b = server_to_partitions
                .get(&b.id)
                .unwrap_or(&PartitionAssignmentInfo::new());

            let score_a = self.calculate_score(a, info_a, assign_partitions);
            let score_b = self.calculate_score(b, info_b, assign_partitions);

            // Higher score first (descending order)
            score_b
                .partial_cmp(&score_a)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Determine expected number of servers
        let expect_num = if max_server_num > 0 && max_server_num < servers.len() {
            max_server_num
        } else {
            servers.len()
        };

        // Get candidate nodes
        let candidate_nodes = self.get_candidate_nodes(&sorted_servers, expect_num);

        // Generate partition assignments
        assignments = self.get_partition_assignment(
            partition_num,
            partition_num_per_range,
            data_replica,
            &candidate_nodes,
            0,
        );

        // Increment partition count for all servers in assignments
        for assignment in &assignments {
            let partition_count = assignment.server_ids.len() as i32;
            for server_id in &assignment.server_ids {
                if let Some(info) = server_to_partitions.get_mut(server_id) {
                    info.increment_partition_num_by(partition_count);
                }
            }
        }

        drop(server_to_partitions);

        Ok(assignments)
    }
}

/// Weighted assignment strategy based on available memory and partition count
#[derive(Clone, Debug)]
pub struct WeightedAssignment {
    memory_weight: f64,
    partition_weight: f64,
}

impl WeightedAssignment {
    pub fn new(memory_weight: f64, partition_weight: f64) -> Self {
        Self {
            memory_weight,
            partition_weight,
        }
    }

    /// Sort servers by score in descending order
    fn sort_servers_by_score(&self, servers: &mut Vec<ShuffleServerNode>) {
        servers.sort_by(|a, b| {
            let score_a = a.calculate_score(self.memory_weight, self.partition_weight);
            let score_b = b.calculate_score(self.memory_weight, self.partition_weight);
            score_b
                .partial_cmp(&score_a)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }
}

impl AssignmentStrategy for WeightedAssignment {
    fn assign(
        &self,
        servers: &[ShuffleServerNode],
        partition_num: usize,
        partition_num_per_range: usize,
        data_replica: usize,
        max_server_num: usize,
        _required_tags: &[String],
        _exclude_server_ids: &[String],
    ) -> Result<Vec<PartitionAssignment>, AssignmentError> {
        if servers.is_empty() {
            return Err(AssignmentError::NoAvailableServers);
        }

        if partition_num <= 0 || partition_num_per_range <= 0 {
            return Err(AssignmentError::InvalidParameters {
                message: "partition_num and partition_num_per_range must be positive".to_string(),
            });
        }

        // 1. Copy and sort servers by score (descending)
        let mut sorted_servers = servers.to_vec();
        self.sort_servers_by_score(&mut sorted_servers);

        // 2. Determine actual number of servers to use
        let actual_server_num = std::cmp::min(max_server_num as usize, sorted_servers.len());
        let selected_servers: Vec<_> = sorted_servers.into_iter().take(actual_server_num).collect();

        // 3. Check if we have enough servers for replication
        let replica_count = data_replica as usize;
        if replica_count > selected_servers.len() {
            return Err(AssignmentError::InsufficientServersForReplication {
                required: data_replica as i32,
                available: selected_servers.len(),
            });
        }

        // 4. Calculate number of partition ranges
        let range_num = (partition_num as i64 + partition_num_per_range as i64 - 1)
            / partition_num_per_range as i64;

        // 5. Generate assignments (Round-Robin assignment to servers)
        let mut assignments = Vec::with_capacity(range_num as usize);
        for range_idx in 0..range_num {
            let start_partition = range_idx * partition_num_per_range as i64;
            let end_partition = std::cmp::min(
                (range_idx + 1) * partition_num_per_range as i64 - 1,
                partition_num as i64 - 1,
            );

            // Select servers for replicas using round-robin
            let mut server_ids = Vec::with_capacity(replica_count);
            for replica_idx in 0..replica_count {
                let server_idx = ((range_idx as usize + replica_idx) % selected_servers.len());
                server_ids.push(selected_servers[server_idx].id.clone());
            }

            assignments.push(PartitionAssignment {
                start_partition: start_partition as i32,
                end_partition: end_partition as i32,
                server_ids,
            });
        }

        Ok(assignments)
    }
}
