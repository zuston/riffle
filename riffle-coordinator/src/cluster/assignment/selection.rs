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

use super::{AssignmentError, AssignmentOptions, PartitionAssignment};
use crate::cluster::server_node::ShuffleServerNode;
use crate::config::{HostAssignmentStrategy, PartitionAssignmentStrategy};
use std::collections::{BTreeMap, HashSet};

pub(super) fn select_candidate_nodes<'a>(
    nodes: &[&'a ShuffleServerNode],
    expected: usize,
    strategy: HostAssignmentStrategy,
) -> Vec<&'a ShuffleServerNode> {
    match strategy {
        HostAssignmentStrategy::None => nodes.iter().take(expected).copied().collect(),
        HostAssignmentStrategy::MustDiff => unique_hosts(nodes, expected),
        HostAssignmentStrategy::PreferDiff => {
            let mut candidates = unique_hosts(nodes, expected);
            if candidates.len() < expected {
                let selected_ids: HashSet<_> =
                    candidates.iter().map(|node| node.id.clone()).collect();
                candidates.extend(
                    nodes
                        .iter()
                        .filter(|node| !selected_ids.contains(&node.id))
                        .take(expected - candidates.len())
                        .copied(),
                );
            }
            candidates
        }
    }
}

fn unique_hosts<'a>(
    nodes: &[&'a ShuffleServerNode],
    expected: usize,
) -> Vec<&'a ShuffleServerNode> {
    let mut hosts = HashSet::new();
    nodes
        .iter()
        .filter(|node| hosts.insert(node.ip.as_str()))
        .take(expected)
        .copied()
        .collect()
}

pub(super) fn assign_partitions(
    candidates: &[&ShuffleServerNode],
    options: &AssignmentOptions,
    strategy: PartitionAssignmentStrategy,
) -> Result<Vec<PartitionAssignment>, AssignmentError> {
    if candidates.is_empty() {
        return Err(AssignmentError::NoAvailableServers);
    }

    match strategy {
        PartitionAssignmentStrategy::Round => assign_round(candidates, options),
        PartitionAssignmentStrategy::Continuous => assign_continuous(candidates, options),
    }
}

fn assign_round(
    candidates: &[&ShuffleServerNode],
    options: &AssignmentOptions,
) -> Result<Vec<PartitionAssignment>, AssignmentError> {
    let mut assignments = Vec::with_capacity(
        options
            .partition_num
            .div_ceil(options.partition_num_per_range),
    );
    let mut server_index = 0;

    for start in (0..options.partition_num).step_by(options.partition_num_per_range) {
        let end = start + options.partition_num_per_range - 1;
        let mut server_ids = Vec::with_capacity(options.data_replica);
        for _ in 0..options.data_replica {
            server_ids.push(candidates[server_index].id.clone());
            server_index = (server_index + 1) % candidates.len();
        }
        assignments.push(PartitionAssignment {
            start_partition: to_i32(start)?,
            end_partition: to_i32(end)?,
            server_ids,
        });
    }

    Ok(assignments)
}

fn assign_continuous(
    candidates: &[&ShuffleServerNode],
    options: &AssignmentOptions,
) -> Result<Vec<PartitionAssignment>, AssignmentError> {
    let range_groups = generate_range_groups(
        options.partition_num,
        options.partition_num_per_range,
        candidates.len(),
        options.estimate_task_concurrency,
    );
    let mut assignments: BTreeMap<(usize, usize), Vec<String>> = BTreeMap::new();

    for replica_index in 0..options.data_replica {
        for (group_index, ranges) in range_groups.iter().enumerate() {
            let server = candidates[(group_index + replica_index) % candidates.len()];
            for range in ranges {
                assignments
                    .entry(*range)
                    .or_default()
                    .push(server.id.clone());
            }
        }
    }

    assignments
        .into_iter()
        .map(|((start, end), server_ids)| {
            Ok(PartitionAssignment {
                start_partition: to_i32(start)?,
                end_partition: to_i32(end)?,
                server_ids,
            })
        })
        .collect()
}

fn generate_range_groups(
    total_partition_num: usize,
    partition_num_per_range: usize,
    server_num: usize,
    estimate_task_concurrency: usize,
) -> Vec<Vec<(usize, usize)>> {
    let estimate_task_concurrency = estimate_task_concurrency.min(total_partition_num);
    let server_partition_width = server_num.saturating_mul(partition_num_per_range);
    let range_per_group = if estimate_task_concurrency > server_partition_width {
        estimate_task_concurrency / server_partition_width
    } else {
        1
    };
    let total_ranges = total_partition_num.div_ceil(partition_num_per_range);
    let ranges_per_round = range_per_group.saturating_mul(server_num);
    let rounds = total_ranges / ranges_per_round;
    let remaining_ranges = total_ranges % ranges_per_round;
    let last_round_ranges_per_group = remaining_ranges / server_num;
    let last_round_remaining_ranges = remaining_ranges % server_num;

    let mut result = Vec::new();
    let mut group = Vec::new();
    let mut group_count = 0;

    for start in (0..total_partition_num).step_by(partition_num_per_range) {
        group.push((start, start + partition_num_per_range - 1));

        let is_last_round = group_count >= rounds * server_num;
        let group_index_in_round = group_count % server_num;
        let expected_group_size = if !is_last_round {
            range_per_group
        } else if group_index_in_round < last_round_remaining_ranges {
            last_round_ranges_per_group + 1
        } else {
            last_round_ranges_per_group
        };

        if expected_group_size > 0 && group.len() == expected_group_size {
            result.push(std::mem::take(&mut group));
            group_count += 1;
        }
    }

    if !group.is_empty() {
        result.push(group);
    }
    result
}

fn to_i32(value: usize) -> Result<i32, AssignmentError> {
    i32::try_from(value).map_err(|_| AssignmentError::InvalidParameters {
        message: format!("partition boundary {value} exceeds i32::MAX"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::server_node::{ServerStatus, StorageInfo};
    use chrono::Utc;
    use std::collections::HashMap;

    fn server(id: &str, ip: &str) -> ShuffleServerNode {
        let now = Utc::now();
        ShuffleServerNode {
            id: id.to_string(),
            ip: ip.to_string(),
            grpc_port: 1,
            netty_port: 2,
            http_port: 3,
            used_memory: 0,
            free_memory: 100,
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
    fn host_strategies_enforce_expected_diversity() {
        let servers = [
            server("s1", "host-a"),
            server("s2", "host-a"),
            server("s3", "host-b"),
        ];
        let nodes: Vec<_> = servers.iter().collect();

        let must_diff = select_candidate_nodes(&nodes, 3, HostAssignmentStrategy::MustDiff);
        assert_eq!(must_diff.len(), 2);
        assert_ne!(must_diff[0].ip, must_diff[1].ip);

        let prefer_diff = select_candidate_nodes(&nodes, 3, HostAssignmentStrategy::PreferDiff);
        assert_eq!(prefer_diff.len(), 3);
        assert_ne!(prefer_diff[0].ip, prefer_diff[1].ip);
    }

    #[test]
    fn round_selection_matches_uniffle_ranges_and_replica_rotation() {
        let servers = [server("s1", "a"), server("s2", "b")];
        let candidates: Vec<_> = servers.iter().collect();
        let options = AssignmentOptions {
            partition_num: 3,
            partition_num_per_range: 2,
            data_replica: 2,
            required_server_num: None,
            estimate_task_concurrency: 0,
        };

        let assignments = assign_round(&candidates, &options).unwrap();

        assert_eq!(assignments[0].start_partition, 0);
        assert_eq!(assignments[0].end_partition, 1);
        assert_eq!(assignments[1].start_partition, 2);
        assert_eq!(assignments[1].end_partition, 3);
        assert_eq!(assignments[0].server_ids, vec!["s1", "s2"]);
        assert_eq!(assignments[1].server_ids, vec!["s1", "s2"]);
    }

    #[test]
    fn continuous_grouping_matches_uniffle_examples() {
        let groups = generate_range_groups(52, 2, 5, 20);
        let group_sizes: Vec<_> = groups.iter().map(Vec::len).collect();

        assert_eq!(
            group_sizes,
            vec![2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1]
        );
    }
}
