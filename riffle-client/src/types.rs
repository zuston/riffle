// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::{RetryPolicy, RiffleError};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};
use std::net::Ipv6Addr;
use std::time::Duration;

pub const SHUFFLE_HANDLE_VERSION: u16 = 1;

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ApplicationId(String);

impl ApplicationId {
    pub fn new(value: impl Into<String>) -> Result<Self, RiffleError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(RiffleError::InvalidArgument(
                "application id must not be empty".to_string(),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for ApplicationId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ShuffleId(i32);

impl ShuffleId {
    pub fn new(value: i32) -> Result<Self, RiffleError> {
        if value < 0 {
            return Err(RiffleError::InvalidArgument(
                "shuffle id must be non-negative".to_string(),
            ));
        }
        Ok(Self(value))
    }

    pub fn value(self) -> i32 {
        self.0
    }
}

impl Display for ShuffleId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct PartitionId(u32);

impl PartitionId {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn value(self) -> u32 {
        self.0
    }

    pub(crate) fn as_i32(self) -> Result<i32, RiffleError> {
        i32::try_from(self.0).map_err(|_| {
            RiffleError::InvalidArgument(format!("partition id {} exceeds i32::MAX", self.0))
        })
    }
}

impl Display for PartitionId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct TaskAttemptId(u64);

impl TaskAttemptId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn value(self) -> u64 {
        self.0
    }

    pub(crate) fn as_i64(self) -> Result<i64, RiffleError> {
        i64::try_from(self.0).map_err(|_| {
            RiffleError::InvalidArgument(format!("task attempt id {} exceeds i64::MAX", self.0))
        })
    }
}

impl Display for TaskAttemptId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct BlockId(u64);

impl BlockId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn value(self) -> u64 {
        self.0
    }

    pub(crate) fn as_i64(self) -> Result<i64, RiffleError> {
        i64::try_from(self.0).map_err(|_| {
            RiffleError::InvalidArgument(format!("block id {} exceeds i64::MAX", self.0))
        })
    }
}

impl Display for BlockId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BlockIdLayout {
    pub sequence_no_bits: u8,
    pub partition_id_bits: u8,
    pub task_attempt_id_bits: u8,
}

impl BlockIdLayout {
    pub fn validate(self) -> Result<(), RiffleError> {
        if self.sequence_no_bits == 0
            || self.partition_id_bits == 0
            || self.task_attempt_id_bits == 0
        {
            return Err(RiffleError::InvalidArgument(
                "block id layout fields must all be positive".to_string(),
            ));
        }
        let total = u16::from(self.sequence_no_bits)
            + u16::from(self.partition_id_bits)
            + u16::from(self.task_attempt_id_bits);
        if total > 63 {
            return Err(RiffleError::InvalidArgument(format!(
                "block id layout uses {total} bits; at most 63 are supported"
            )));
        }
        Ok(())
    }

    pub fn compose(
        self,
        sequence_no: u64,
        partition_id: PartitionId,
        task_attempt_id: TaskAttemptId,
    ) -> Result<BlockId, RiffleError> {
        self.validate()?;
        validate_fits("sequence number", sequence_no, self.sequence_no_bits)?;
        validate_fits(
            "partition id",
            u64::from(partition_id.value()),
            self.partition_id_bits,
        )?;
        validate_fits(
            "task attempt id",
            task_attempt_id.value(),
            self.task_attempt_id_bits,
        )?;

        let sequence_shift = u32::from(self.partition_id_bits + self.task_attempt_id_bits);
        let partition_shift = u32::from(self.task_attempt_id_bits);
        Ok(BlockId::new(
            (sequence_no << sequence_shift)
                | (u64::from(partition_id.value()) << partition_shift)
                | task_attempt_id.value(),
        ))
    }

    pub fn sequence_no(self, block_id: BlockId) -> u64 {
        shift_right(
            block_id.value(),
            u16::from(self.partition_id_bits) + u16::from(self.task_attempt_id_bits),
        )
    }

    pub fn partition_id(self, block_id: BlockId) -> PartitionId {
        let mask = bit_mask(self.partition_id_bits);
        PartitionId::new(
            (shift_right(block_id.value(), u16::from(self.task_attempt_id_bits)) & mask) as u32,
        )
    }

    pub fn task_attempt_id(self, block_id: BlockId) -> TaskAttemptId {
        TaskAttemptId::new(block_id.value() & bit_mask(self.task_attempt_id_bits))
    }

    pub fn max_sequence_no(self) -> u64 {
        bit_mask(self.sequence_no_bits)
    }

    pub fn max_partition_id(self) -> u64 {
        bit_mask(self.partition_id_bits)
    }

    pub fn max_task_attempt_id(self) -> u64 {
        bit_mask(self.task_attempt_id_bits)
    }

    pub fn max_block_id(self) -> u64 {
        bit_mask_u16(
            u16::from(self.sequence_no_bits)
                + u16::from(self.partition_id_bits)
                + u16::from(self.task_attempt_id_bits),
        )
    }
}

impl Default for BlockIdLayout {
    fn default() -> Self {
        Self {
            sequence_no_bits: 16,
            partition_id_bits: 24,
            task_attempt_id_bits: 23,
        }
    }
}

fn bit_mask(bits: u8) -> u64 {
    bit_mask_u16(u16::from(bits))
}

fn bit_mask_u16(bits: u16) -> u64 {
    match bits {
        0 => 0,
        1..=63 => (1_u64 << u32::from(bits)) - 1,
        _ => u64::MAX,
    }
}

fn shift_right(value: u64, bits: u16) -> u64 {
    if bits >= 64 {
        0
    } else {
        value >> u32::from(bits)
    }
}

fn validate_fits(name: &str, value: u64, bits: u8) -> Result<(), RiffleError> {
    let maximum = bit_mask(bits);
    if value > maximum {
        Err(RiffleError::InvalidArgument(format!(
            "{name} {value} exceeds the {bits}-bit maximum {maximum}"
        )))
    } else {
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ShuffleServer {
    pub id: String,
    pub host: String,
    pub grpc_port: u16,
    pub urpc_port: Option<u16>,
    pub http_port: Option<u16>,
}

impl ShuffleServer {
    pub fn validate(&self) -> Result<(), RiffleError> {
        if self.id.trim().is_empty() {
            return Err(RiffleError::InvalidAssignment(
                "shuffle server id must not be empty".to_string(),
            ));
        }
        if self.host.trim().is_empty() {
            return Err(RiffleError::InvalidAssignment(format!(
                "shuffle server {} has an empty host",
                self.id
            )));
        }
        if self.grpc_port == 0 {
            return Err(RiffleError::InvalidAssignment(format!(
                "shuffle server {} has an invalid gRPC port",
                self.id
            )));
        }
        Ok(())
    }

    pub fn grpc_endpoint(&self) -> String {
        let host = if self.host.parse::<Ipv6Addr>().is_ok() {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        format!("http://{host}:{}", self.grpc_port)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PartitionRoute {
    pub start: PartitionId,
    pub end: PartitionId,
    pub replicas: Vec<ShuffleServer>,
}

impl PartitionRoute {
    pub fn contains(&self, partition_id: PartitionId) -> bool {
        self.start <= partition_id && partition_id <= self.end
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ShuffleHandle {
    pub handle_version: u16,
    pub application_id: ApplicationId,
    pub shuffle_id: ShuffleId,
    pub partition_count: u32,
    pub partition_range_size: u32,
    pub data_replica: u32,
    pub block_id_layout: BlockIdLayout,
    pub routes: Vec<PartitionRoute>,
    #[serde(default)]
    pub feature_flags: BTreeSet<String>,
}

impl ShuffleHandle {
    pub fn new(
        application_id: ApplicationId,
        shuffle_id: ShuffleId,
        partition_count: u32,
        partition_range_size: u32,
        data_replica: u32,
        block_id_layout: BlockIdLayout,
        routes: Vec<PartitionRoute>,
    ) -> Result<Self, RiffleError> {
        let routes = normalize_routes(partition_count, data_replica, routes)?;
        let handle = Self {
            handle_version: SHUFFLE_HANDLE_VERSION,
            application_id,
            shuffle_id,
            partition_count,
            partition_range_size,
            data_replica,
            block_id_layout,
            routes,
            feature_flags: BTreeSet::new(),
        };
        handle.validate()?;
        Ok(handle)
    }

    pub fn validate(&self) -> Result<(), RiffleError> {
        if self.handle_version != SHUFFLE_HANDLE_VERSION {
            return Err(RiffleError::Unsupported(format!(
                "shuffle handle version {}",
                self.handle_version
            )));
        }
        if self.application_id.as_str().trim().is_empty() {
            return Err(RiffleError::InvalidArgument(
                "application id must not be empty".to_string(),
            ));
        }
        if self.shuffle_id.value() < 0 {
            return Err(RiffleError::InvalidArgument(
                "shuffle id must be non-negative".to_string(),
            ));
        }
        if self.partition_count == 0 || self.partition_range_size == 0 {
            return Err(RiffleError::InvalidArgument(
                "partition_count and partition_range_size must be positive".to_string(),
            ));
        }
        as_i32("partition_count", self.partition_count)?;
        as_i32("partition_range_size", self.partition_range_size)?;
        if self.data_replica == 0 {
            return Err(RiffleError::InvalidArgument(
                "data_replica must be positive".to_string(),
            ));
        }
        self.block_id_layout.validate()?;
        let maximum_partition = u64::from(self.partition_count - 1);
        if maximum_partition > self.block_id_layout.max_partition_id() {
            return Err(RiffleError::InvalidArgument(format!(
                "partition count {} exceeds the block id layout capacity",
                self.partition_count
            )));
        }
        let normalized =
            normalize_routes(self.partition_count, self.data_replica, self.routes.clone())?;
        if normalized != self.routes {
            return Err(RiffleError::InvalidAssignment(
                "shuffle routes are not normalized".to_string(),
            ));
        }
        Ok(())
    }

    pub fn route_for(&self, partition_id: PartitionId) -> Result<&PartitionRoute, RiffleError> {
        if partition_id.value() >= self.partition_count {
            return Err(RiffleError::InvalidArgument(format!(
                "partition {} is outside shuffle partition count {}",
                partition_id, self.partition_count
            )));
        }
        self.routes
            .iter()
            .find(|route| route.contains(partition_id))
            .ok_or_else(|| {
                RiffleError::InvalidAssignment(format!(
                    "partition {partition_id} has no shuffle server route"
                ))
            })
    }

    pub fn servers(&self) -> BTreeSet<ShuffleServer> {
        self.routes
            .iter()
            .flat_map(|route| route.replicas.iter().cloned())
            .collect()
    }
}

pub(crate) fn normalize_routes(
    partition_count: u32,
    data_replica: u32,
    mut routes: Vec<PartitionRoute>,
) -> Result<Vec<PartitionRoute>, RiffleError> {
    if partition_count == 0 {
        return Err(RiffleError::InvalidAssignment(
            "partition count must be positive".to_string(),
        ));
    }
    if data_replica == 0 {
        return Err(RiffleError::InvalidAssignment(
            "data replica must be positive".to_string(),
        ));
    }

    let last_partition = PartitionId::new(partition_count - 1);
    for route in &mut routes {
        if route.start > route.end {
            return Err(RiffleError::InvalidAssignment(format!(
                "route {}..={} is reversed",
                route.start, route.end
            )));
        }
        if route.start > last_partition {
            return Err(RiffleError::InvalidAssignment(format!(
                "route starts at {}, beyond final partition {}",
                route.start, last_partition
            )));
        }
        route.end = route.end.min(last_partition);
        route.replicas.sort();
        route.replicas.dedup();
        if route.replicas.len() != data_replica as usize {
            return Err(RiffleError::InvalidAssignment(format!(
                "route {}..={} has {} replicas; expected {}",
                route.start,
                route.end,
                route.replicas.len(),
                data_replica
            )));
        }
        for server in &route.replicas {
            server.validate()?;
        }
    }
    routes.sort_by_key(|route| (route.start, route.end));

    let mut expected_start = 0_u32;
    for route in &routes {
        if route.start.value() != expected_start {
            let kind = if route.start.value() < expected_start {
                "overlaps a previous route"
            } else {
                "leaves a gap before it"
            };
            return Err(RiffleError::InvalidAssignment(format!(
                "route {}..={} {kind}; expected start {}",
                route.start, route.end, expected_start
            )));
        }
        expected_start = route.end.value().checked_add(1).ok_or_else(|| {
            RiffleError::InvalidAssignment("partition route end overflowed".to_string())
        })?;
    }
    if expected_start != partition_count {
        return Err(RiffleError::InvalidAssignment(format!(
            "routes cover partitions 0..{}, expected 0..{}",
            expected_start.saturating_sub(1),
            partition_count - 1
        )));
    }
    Ok(routes)
}

#[derive(Clone, Debug)]
pub struct DriverConfig {
    pub coordinator_endpoints: Vec<String>,
    pub required_tags: Vec<String>,
    pub access_id: String,
    pub access_properties: BTreeMap<String, String>,
    pub client_host: String,
    pub client_port: String,
    pub client_property: String,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub max_encoding_message_size: usize,
    pub max_decoding_message_size: usize,
}

impl DriverConfig {
    pub fn new(coordinator_endpoints: Vec<String>) -> Self {
        Self {
            coordinator_endpoints,
            ..Self::default()
        }
    }

    pub(crate) fn validate(&self) -> Result<(), RiffleError> {
        if self.coordinator_endpoints.is_empty() {
            return Err(RiffleError::InvalidArgument(
                "at least one coordinator endpoint is required".to_string(),
            ));
        }
        if self
            .coordinator_endpoints
            .iter()
            .any(|endpoint| endpoint.trim().is_empty())
        {
            return Err(RiffleError::InvalidArgument(
                "coordinator endpoints must not be empty".to_string(),
            ));
        }
        if self.heartbeat_interval.is_zero() {
            return Err(RiffleError::InvalidArgument(
                "heartbeat interval must be positive".to_string(),
            ));
        }
        validate_rpc_settings(
            self.connect_timeout,
            self.request_timeout,
            self.max_encoding_message_size,
            self.max_decoding_message_size,
        )?;
        Ok(())
    }
}

impl Default for DriverConfig {
    fn default() -> Self {
        Self {
            coordinator_endpoints: Vec::new(),
            required_tags: Vec::new(),
            access_id: String::new(),
            access_properties: BTreeMap::new(),
            client_host: String::new(),
            client_port: String::new(),
            client_property: "riffle-rust-client".to_string(),
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(10),
            max_encoding_message_size: 64 * 1024 * 1024,
            max_decoding_message_size: 64 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ApplicationSpec {
    pub application_id: ApplicationId,
    pub user: String,
    pub version: Option<String>,
    pub git_commit_id: Option<String>,
}

impl ApplicationSpec {
    pub fn new(application_id: ApplicationId, user: impl Into<String>) -> Self {
        Self {
            application_id,
            user: user.into(),
            version: None,
            git_commit_id: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DataDistribution {
    #[default]
    Normal,
    LocalOrder,
}

impl DataDistribution {
    pub(crate) const fn proto_value(self) -> i32 {
        match self {
            Self::Normal => 0,
            Self::LocalOrder => 1,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct RemoteStorageSpec {
    pub path: String,
    pub properties: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct ShuffleSpec {
    pub shuffle_id: ShuffleId,
    pub partition_count: u32,
    pub partition_range_size: u32,
    pub data_replica: u32,
    pub required_server_count: Option<u32>,
    pub estimate_task_concurrency: u32,
    pub max_concurrency_per_partition_to_write: u32,
    pub data_distribution: DataDistribution,
    pub properties: BTreeMap<String, String>,
    pub remote_storage: Option<RemoteStorageSpec>,
    pub block_id_layout: BlockIdLayout,
}

impl ShuffleSpec {
    pub fn new(shuffle_id: ShuffleId, partition_count: u32) -> Self {
        Self {
            shuffle_id,
            partition_count,
            partition_range_size: 1,
            data_replica: 1,
            required_server_count: None,
            estimate_task_concurrency: 0,
            max_concurrency_per_partition_to_write: 1,
            data_distribution: DataDistribution::Normal,
            properties: BTreeMap::new(),
            remote_storage: None,
            block_id_layout: BlockIdLayout::default(),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), RiffleError> {
        if self.partition_count == 0 || self.partition_range_size == 0 {
            return Err(RiffleError::InvalidArgument(
                "partition_count and partition_range_size must be positive".to_string(),
            ));
        }
        if self.data_replica != 1 {
            return Err(RiffleError::Unsupported(
                "the initial gRPC client supports data_replica=1 only".to_string(),
            ));
        }
        if self.required_server_count == Some(0) {
            return Err(RiffleError::InvalidArgument(
                "required_server_count must be positive when set".to_string(),
            ));
        }
        if self.max_concurrency_per_partition_to_write == 0 {
            return Err(RiffleError::InvalidArgument(
                "max_concurrency_per_partition_to_write must be positive".to_string(),
            ));
        }
        self.block_id_layout.validate()?;
        if u64::from(self.partition_count - 1) > self.block_id_layout.max_partition_id() {
            return Err(RiffleError::InvalidArgument(
                "partition count exceeds the block id layout capacity".to_string(),
            ));
        }
        as_i32("partition_count", self.partition_count)?;
        as_i32("partition_range_size", self.partition_range_size)?;
        as_i32(
            "max_concurrency_per_partition_to_write",
            self.max_concurrency_per_partition_to_write,
        )?;
        Ok(())
    }
}

pub(crate) fn as_i32(name: &str, value: u32) -> Result<i32, RiffleError> {
    i32::try_from(value)
        .map_err(|_| RiffleError::InvalidArgument(format!("{name} {value} exceeds i32::MAX")))
}

#[derive(Clone, Debug)]
pub struct ShuffleWriterConfig {
    pub max_batch_bytes: usize,
    pub max_inflight_bytes: usize,
    pub max_inflight_requests: usize,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub max_encoding_message_size: usize,
    pub max_decoding_message_size: usize,
    pub retry_policy: RetryPolicy,
}

impl ShuffleWriterConfig {
    pub(crate) fn validate(&self) -> Result<(), RiffleError> {
        if self.max_batch_bytes == 0
            || self.max_inflight_bytes == 0
            || self.max_inflight_requests == 0
        {
            return Err(RiffleError::InvalidArgument(
                "writer batch and in-flight limits must be positive".to_string(),
            ));
        }
        if self.max_batch_bytes > self.max_inflight_bytes {
            return Err(RiffleError::InvalidArgument(
                "max_batch_bytes must not exceed max_inflight_bytes".to_string(),
            ));
        }
        if self.max_inflight_bytes > u32::MAX as usize {
            return Err(RiffleError::InvalidArgument(
                "max_inflight_bytes must not exceed u32::MAX".to_string(),
            ));
        }
        if self.max_batch_bytes > i32::MAX as usize {
            return Err(RiffleError::InvalidArgument(
                "max_batch_bytes must not exceed i32::MAX".to_string(),
            ));
        }
        if self.max_batch_bytes > self.max_encoding_message_size {
            return Err(RiffleError::InvalidArgument(
                "max_batch_bytes must not exceed max_encoding_message_size".to_string(),
            ));
        }
        validate_rpc_settings(
            self.connect_timeout,
            self.request_timeout,
            self.max_encoding_message_size,
            self.max_decoding_message_size,
        )?;
        self.retry_policy.validate()
    }
}

impl Default for ShuffleWriterConfig {
    fn default() -> Self {
        Self {
            max_batch_bytes: 4 * 1024 * 1024,
            max_inflight_bytes: 64 * 1024 * 1024,
            max_inflight_requests: 8,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            max_encoding_message_size: 64 * 1024 * 1024,
            max_decoding_message_size: 64 * 1024 * 1024,
            retry_policy: RetryPolicy::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ShuffleReaderConfig {
    pub read_buffer_size: usize,
    pub stream_channel_capacity: usize,
    pub spill_race_retries: u32,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub max_encoding_message_size: usize,
    pub max_decoding_message_size: usize,
    pub retry_policy: RetryPolicy,
}

impl ShuffleReaderConfig {
    pub(crate) fn validate(&self) -> Result<(), RiffleError> {
        if self.read_buffer_size == 0 || self.stream_channel_capacity == 0 {
            return Err(RiffleError::InvalidArgument(
                "reader buffer and stream capacity must be positive".to_string(),
            ));
        }
        i32::try_from(self.read_buffer_size).map_err(|_| {
            RiffleError::InvalidArgument("read_buffer_size exceeds i32::MAX".to_string())
        })?;
        if self.read_buffer_size > self.max_decoding_message_size {
            return Err(RiffleError::InvalidArgument(
                "read_buffer_size must not exceed max_decoding_message_size".to_string(),
            ));
        }
        validate_rpc_settings(
            self.connect_timeout,
            self.request_timeout,
            self.max_encoding_message_size,
            self.max_decoding_message_size,
        )?;
        self.retry_policy.validate()
    }
}

impl Default for ShuffleReaderConfig {
    fn default() -> Self {
        Self {
            read_buffer_size: 4 * 1024 * 1024,
            stream_channel_capacity: 16,
            spill_race_retries: 2,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            max_encoding_message_size: 64 * 1024 * 1024,
            max_decoding_message_size: 64 * 1024 * 1024,
            retry_policy: RetryPolicy::default(),
        }
    }
}

fn validate_rpc_settings(
    connect_timeout: Duration,
    request_timeout: Duration,
    max_encoding_message_size: usize,
    max_decoding_message_size: usize,
) -> Result<(), RiffleError> {
    if connect_timeout.is_zero() || request_timeout.is_zero() {
        return Err(RiffleError::InvalidArgument(
            "RPC timeouts must be positive".to_string(),
        ));
    }
    if max_encoding_message_size == 0 || max_decoding_message_size == 0 {
        return Err(RiffleError::InvalidArgument(
            "RPC message size limits must be positive".to_string(),
        ));
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct BlockPayload {
    pub data: Bytes,
    pub uncompressed_length: u32,
    pub record_count: Option<u64>,
}

impl BlockPayload {
    pub fn new(data: Bytes) -> Result<Self, RiffleError> {
        let uncompressed_length = u32::try_from(data.len()).map_err(|_| {
            RiffleError::InvalidArgument("block payload exceeds u32::MAX".to_string())
        })?;
        Ok(Self {
            data,
            uncompressed_length,
            record_count: None,
        })
    }
}

#[derive(Clone, Debug)]
pub struct ShuffleBlock {
    pub block_id: BlockId,
    pub partition_id: PartitionId,
    pub task_attempt_id: TaskAttemptId,
    pub uncompressed_length: u32,
    pub crc: u32,
    pub data: Bytes,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MapOutput {
    pub task_attempt_id: TaskAttemptId,
    pub blocks_written: u64,
    pub bytes_written: u64,
    pub records_written: Option<u64>,
    pub partitions_written: Vec<PartitionId>,
}

#[derive(Clone, Debug)]
pub struct ReadPartitionRequest {
    pub partition_id: PartitionId,
    pub accepted_task_attempt_ids: Vec<TaskAttemptId>,
}

impl ReadPartitionRequest {
    pub fn new(partition_id: PartitionId, accepted_task_attempt_ids: Vec<TaskAttemptId>) -> Self {
        Self {
            partition_id,
            accepted_task_attempt_ids,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server(id: &str) -> ShuffleServer {
        ShuffleServer {
            id: id.to_string(),
            host: "127.0.0.1".to_string(),
            grpc_port: 19999,
            urpc_port: None,
            http_port: None,
        }
    }

    #[test]
    fn block_id_layout_round_trips_all_fields() {
        let layout = BlockIdLayout::default();
        let block_id = layout
            .compose(123, PartitionId::new(456), TaskAttemptId::new(789))
            .unwrap();

        assert_eq!(layout.sequence_no(block_id), 123);
        assert_eq!(layout.partition_id(block_id), PartitionId::new(456));
        assert_eq!(layout.task_attempt_id(block_id), TaskAttemptId::new(789));
    }

    #[test]
    fn block_id_layout_rejects_overflow_instead_of_wrapping() {
        let layout = BlockIdLayout::default();
        assert!(layout
            .compose(
                layout.max_sequence_no() + 1,
                PartitionId::new(0),
                TaskAttemptId::new(0),
            )
            .is_err());
        assert!(layout
            .compose(
                0,
                PartitionId::new(0),
                TaskAttemptId::new(layout.max_task_attempt_id() + 1),
            )
            .is_err());
    }

    #[test]
    fn routes_are_sorted_and_final_coordinator_range_is_clamped() {
        let routes = normalize_routes(
            5,
            1,
            vec![
                PartitionRoute {
                    start: PartitionId::new(3),
                    end: PartitionId::new(5),
                    replicas: vec![server("b")],
                },
                PartitionRoute {
                    start: PartitionId::new(0),
                    end: PartitionId::new(2),
                    replicas: vec![server("a")],
                },
            ],
        )
        .unwrap();

        assert_eq!(routes[0].start, PartitionId::new(0));
        assert_eq!(routes[1].end, PartitionId::new(4));
    }

    #[test]
    fn routes_reject_gaps_and_overlaps() {
        let gap = vec![
            PartitionRoute {
                start: PartitionId::new(0),
                end: PartitionId::new(1),
                replicas: vec![server("a")],
            },
            PartitionRoute {
                start: PartitionId::new(3),
                end: PartitionId::new(3),
                replicas: vec![server("b")],
            },
        ];
        assert!(normalize_routes(4, 1, gap).is_err());

        let overlap = vec![
            PartitionRoute {
                start: PartitionId::new(0),
                end: PartitionId::new(2),
                replicas: vec![server("a")],
            },
            PartitionRoute {
                start: PartitionId::new(2),
                end: PartitionId::new(3),
                replicas: vec![server("b")],
            },
        ];
        assert!(normalize_routes(4, 1, overlap).is_err());
    }

    #[test]
    fn shuffle_handle_is_serializable_engine_metadata() {
        let handle = ShuffleHandle::new(
            ApplicationId::new("app").unwrap(),
            ShuffleId::new(1).unwrap(),
            1,
            1,
            1,
            BlockIdLayout::default(),
            vec![PartitionRoute {
                start: PartitionId::new(0),
                end: PartitionId::new(0),
                replicas: vec![server("server")],
            }],
        )
        .unwrap();

        let encoded = serde_json::to_string(&handle).unwrap();
        let decoded: ShuffleHandle = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, handle);
        decoded.validate().unwrap();
    }

    #[test]
    fn deserialized_handle_validation_rejects_negative_shuffle_id() {
        let mut handle = ShuffleHandle::new(
            ApplicationId::new("app").unwrap(),
            ShuffleId::new(1).unwrap(),
            1,
            1,
            1,
            BlockIdLayout::default(),
            vec![PartitionRoute {
                start: PartitionId::new(0),
                end: PartitionId::new(0),
                replicas: vec![server("server")],
            }],
        )
        .unwrap();
        handle.shuffle_id = ShuffleId(-1);

        assert!(handle.validate().is_err());
    }
}
