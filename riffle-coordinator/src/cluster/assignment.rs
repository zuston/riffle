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

mod basic;
mod partition_balance;
mod selection;

use super::server_node::ShuffleServerNode;
use crate::config::{AssignmentStrategyType, Config};
use thiserror::Error;

pub use basic::BasicAssignmentStrategy;
pub use partition_balance::PartitionBalanceAssignmentStrategy;

#[derive(Error, Debug, Eq, PartialEq)]
pub enum AssignmentError {
    #[error("No available servers for assignment")]
    NoAvailableServers,

    #[error("Insufficient servers for replication: required {required}, available {available}")]
    InsufficientServersForReplication { required: usize, available: usize },

    #[error("Invalid assignment parameters: {message}")]
    InvalidParameters { message: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionAssignment {
    pub start_partition: i32,
    pub end_partition: i32,
    pub server_ids: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct AssignmentOptions {
    pub partition_num: usize,
    pub partition_num_per_range: usize,
    pub data_replica: usize,
    pub required_server_num: Option<usize>,
    pub estimate_task_concurrency: usize,
}

pub trait AssignmentStrategy: Send + Sync {
    fn assign(
        &self,
        servers: &[ShuffleServerNode],
        options: &AssignmentOptions,
    ) -> Result<Vec<PartitionAssignment>, AssignmentError>;
}

pub fn create_assignment_strategy(config: &Config) -> Box<dyn AssignmentStrategy> {
    match config.assignment_strategy {
        AssignmentStrategyType::Basic => Box::new(BasicAssignmentStrategy::new(
            config.max_assignment_servers,
            config.assignment_host_strategy,
            config.select_partition_strategy,
        )),
        AssignmentStrategyType::PartitionBalance => {
            Box::new(PartitionBalanceAssignmentStrategy::new(
                config.max_assignment_servers,
                config.assignment_host_strategy,
                config.select_partition_strategy,
            ))
        }
    }
}

pub(crate) fn expected_server_count(
    available: usize,
    requested: Option<usize>,
    shuffle_nodes_max: usize,
) -> usize {
    requested
        .unwrap_or(shuffle_nodes_max)
        .min(shuffle_nodes_max)
        .min(available)
}

pub(crate) fn validate_options(options: &AssignmentOptions) -> Result<(), AssignmentError> {
    for (name, value) in [
        ("partition_num", options.partition_num),
        ("partition_num_per_range", options.partition_num_per_range),
        ("data_replica", options.data_replica),
    ] {
        if value == 0 {
            return Err(AssignmentError::InvalidParameters {
                message: format!("{name} must be positive"),
            });
        }
    }
    Ok(())
}
