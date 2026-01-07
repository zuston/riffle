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

use super::server_node::ShuffleServerNode;
use std::fmt;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AssignmentError {
    #[error("No available servers for assignment")]
    NoAvailableServers,

    #[error("Insufficient servers for replication: required {required}, available {available}")]
    InsufficientServersForReplication { required: i32, available: usize },

    #[error("Invalid assignment parameters")]
    InvalidParameters,
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

/// Trait for assignment strategies
pub trait AssignmentStrategy: Send + Sync {
    fn assign(
        &self,
        servers: &[ShuffleServerNode],
        partition_num: usize,
        partition_num_per_range: usize,
        data_replica: usize,
        max_server_num: usize,
    ) -> Result<Vec<PartitionAssignment>, AssignmentError>;
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
    ) -> Result<Vec<PartitionAssignment>, AssignmentError> {
        if servers.is_empty() {
            return Err(AssignmentError::NoAvailableServers);
        }

        if partition_num <= 0 || partition_num_per_range <= 0 {
            return Err(AssignmentError::InvalidParameters);
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
