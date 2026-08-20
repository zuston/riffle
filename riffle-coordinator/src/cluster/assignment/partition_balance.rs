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

use super::selection::{assign_partitions, select_candidate_nodes};
use super::{
    expected_server_count, validate_options, AssignmentError, AssignmentOptions,
    AssignmentStrategy, PartitionAssignment,
};
use crate::cluster::server_node::ShuffleServerNode;
use crate::config::{HostAssignmentStrategy, PartitionAssignmentStrategy};
use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

struct PartitionAssignmentInfo {
    partition_num: usize,
    server_start_time: DateTime<Utc>,
}

impl PartitionAssignmentInfo {
    fn new(server_start_time: DateTime<Utc>) -> Self {
        Self {
            partition_num: 0,
            server_start_time,
        }
    }
}

pub struct PartitionBalanceAssignmentStrategy {
    shuffle_nodes_max: usize,
    host_strategy: HostAssignmentStrategy,
    partition_strategy: PartitionAssignmentStrategy,
    server_to_assignments: Mutex<HashMap<String, PartitionAssignmentInfo>>,
}

impl PartitionBalanceAssignmentStrategy {
    pub fn new(
        shuffle_nodes_max: usize,
        host_strategy: HostAssignmentStrategy,
        partition_strategy: PartitionAssignmentStrategy,
    ) -> Self {
        Self {
            shuffle_nodes_max,
            host_strategy,
            partition_strategy,
            server_to_assignments: Mutex::new(HashMap::new()),
        }
    }

    fn update_assignment_info(
        servers: &[ShuffleServerNode],
        server_to_partitions: &mut HashMap<String, PartitionAssignmentInfo>,
    ) {
        let current_ids: HashSet<_> = servers.iter().map(|node| node.id.as_str()).collect();
        server_to_partitions.retain(|id, _| current_ids.contains(id.as_str()));

        for node in servers {
            let entry = server_to_partitions
                .entry(node.id.clone())
                .or_insert_with(|| PartitionAssignmentInfo::new(node.server_start_time));
            if entry.server_start_time < node.server_start_time {
                entry.partition_num = 0;
                entry.server_start_time = node.server_start_time;
            }
        }
    }

    fn score(
        node: &ShuffleServerNode,
        partition_info: &PartitionAssignmentInfo,
        assign_partitions: usize,
    ) -> f64 {
        node.free_memory as f64 / (partition_info.partition_num + assign_partitions) as f64
    }
}

impl AssignmentStrategy for PartitionBalanceAssignmentStrategy {
    fn assign(
        &self,
        servers: &[ShuffleServerNode],
        options: &AssignmentOptions,
    ) -> Result<Vec<PartitionAssignment>, AssignmentError> {
        validate_options(options)?;
        if options.partition_num_per_range != 1 {
            return Err(AssignmentError::InvalidParameters {
                message: "partition_num_per_range must be one for partition_balance".to_string(),
            });
        }
        if servers.is_empty() {
            return Err(AssignmentError::NoAvailableServers);
        }
        if servers.len() < options.data_replica {
            return Err(AssignmentError::InsufficientServersForReplication {
                required: options.data_replica,
                available: servers.len(),
            });
        }

        let mut server_to_partitions = self.server_to_assignments.lock().unwrap();
        Self::update_assignment_info(servers, &mut server_to_partitions);

        let average_partitions =
            options.partition_num.saturating_mul(options.data_replica) / self.shuffle_nodes_max;
        let assignment_width = average_partitions.max(1);

        let mut sorted_servers: Vec<_> = servers.iter().collect();
        sorted_servers.sort_by(|left, right| {
            let left_info = &server_to_partitions[&left.id];
            let right_info = &server_to_partitions[&right.id];
            Self::score(right, right_info, assignment_width)
                .total_cmp(&Self::score(left, left_info, assignment_width))
                .then_with(|| left.id.cmp(&right.id))
        });

        let expected = expected_server_count(
            sorted_servers.len(),
            options.required_server_num,
            self.shuffle_nodes_max,
        );
        let candidates = select_candidate_nodes(&sorted_servers, expected, self.host_strategy);
        if candidates.len() < options.data_replica {
            return Err(AssignmentError::InsufficientServersForReplication {
                required: options.data_replica,
                available: candidates.len(),
            });
        }

        let assignments = assign_partitions(&candidates, options, self.partition_strategy)?;
        for assignment in &assignments {
            for server_id in &assignment.server_ids {
                if let Some(info) = server_to_partitions.get_mut(server_id) {
                    info.partition_num += 1;
                }
            }
        }

        Ok(assignments)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::server_node::{ServerStatus, StorageInfo};
    use chrono::Utc;

    fn server(id: &str, memory: usize) -> ShuffleServerNode {
        let now = Utc::now();
        ShuffleServerNode {
            id: id.to_string(),
            ip: id.to_string(),
            grpc_port: 1,
            netty_port: 2,
            http_port: 3,
            used_memory: 0,
            free_memory: memory,
            reserved_memory: 0,
            event_num_in_flush: 0,
            tags: vec![],
            is_healthy: true,
            status: ServerStatus::Active,
            storage_info: HashMap::<String, StorageInfo>::new(),
            version: None,
            git_commit_id: None,
            server_start_time: now,
        }
    }

    #[test]
    fn increments_each_server_once_per_partition_replica() {
        let strategy = PartitionBalanceAssignmentStrategy::new(
            9,
            HostAssignmentStrategy::None,
            PartitionAssignmentStrategy::Round,
        );
        let servers = vec![server("s1", 100), server("s2", 100)];
        let options = AssignmentOptions {
            partition_num: 1,
            partition_num_per_range: 1,
            data_replica: 2,
            required_server_num: None,
            estimate_task_concurrency: 0,
        };

        strategy.assign(&servers, &options).unwrap();

        let counters = strategy.server_to_assignments.lock().unwrap();
        assert_eq!(counters["s1"].partition_num, 1);
        assert_eq!(counters["s2"].partition_num, 1);
    }

    #[test]
    fn drops_counters_for_servers_no_longer_available() {
        let strategy = PartitionBalanceAssignmentStrategy::new(
            9,
            HostAssignmentStrategy::None,
            PartitionAssignmentStrategy::Round,
        );
        let options = AssignmentOptions {
            partition_num: 1,
            partition_num_per_range: 1,
            data_replica: 1,
            required_server_num: None,
            estimate_task_concurrency: 0,
        };

        strategy
            .assign(&[server("s1", 100), server("s2", 90)], &options)
            .unwrap();
        strategy.assign(&[server("s1", 100)], &options).unwrap();

        let counters = strategy.server_to_assignments.lock().unwrap();
        assert!(counters.contains_key("s1"));
        assert!(!counters.contains_key("s2"));
    }
}
