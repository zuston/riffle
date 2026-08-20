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
use rand::seq::SliceRandom;

pub struct BasicAssignmentStrategy {
    shuffle_nodes_max: usize,
    host_strategy: HostAssignmentStrategy,
    partition_strategy: PartitionAssignmentStrategy,
}

impl BasicAssignmentStrategy {
    pub fn new(
        shuffle_nodes_max: usize,
        host_strategy: HostAssignmentStrategy,
        partition_strategy: PartitionAssignmentStrategy,
    ) -> Self {
        Self {
            shuffle_nodes_max,
            host_strategy,
            partition_strategy,
        }
    }
}

impl AssignmentStrategy for BasicAssignmentStrategy {
    fn assign(
        &self,
        servers: &[ShuffleServerNode],
        options: &AssignmentOptions,
    ) -> Result<Vec<PartitionAssignment>, AssignmentError> {
        validate_options(options)?;
        if servers.is_empty() {
            return Err(AssignmentError::NoAvailableServers);
        }

        let mut sorted_servers: Vec<_> = servers.iter().collect();
        sorted_servers.shuffle(&mut rand::thread_rng());
        sorted_servers.sort_by(|left, right| right.free_memory.cmp(&left.free_memory));

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

        assign_partitions(&candidates, options, self.partition_strategy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::server_node::{ServerStatus, StorageInfo};
    use chrono::Utc;
    use std::collections::{HashMap, HashSet};

    fn server(index: usize) -> ShuffleServerNode {
        let now = Utc::now();
        ShuffleServerNode {
            id: format!("s{index}"),
            ip: format!("host-{index}"),
            grpc_port: 1,
            netty_port: 2,
            http_port: 3,
            used_memory: 0,
            free_memory: index,
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
    fn applies_configured_max_when_request_uses_default_sentinel() {
        let strategy = BasicAssignmentStrategy::new(
            3,
            HostAssignmentStrategy::None,
            PartitionAssignmentStrategy::Round,
        );
        let servers: Vec<_> = (0..5).map(server).collect();
        let options = AssignmentOptions {
            partition_num: 3,
            partition_num_per_range: 1,
            data_replica: 1,
            required_server_num: None,
            estimate_task_concurrency: 0,
        };

        let assignments = strategy.assign(&servers, &options).unwrap();
        let selected: HashSet<_> = assignments
            .iter()
            .flat_map(|assignment| assignment.server_ids.iter().map(String::as_str))
            .collect();

        assert_eq!(selected.len(), 3);
        assert!(selected.contains("s4"));
        assert!(selected.contains("s3"));
        assert!(selected.contains("s2"));
    }
}
