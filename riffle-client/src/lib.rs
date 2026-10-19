mod configs;
mod manager;
mod reader;
pub mod rpc_client;
mod writer;

use std::pin::Pin;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{bail, Context};
pub use anyhow::{Error, Result};
use bytes::Bytes;
use futures::Stream;
use serde::{Deserialize, Serialize};

pub use manager::shuffle_id_manager::ShuffleKey;
pub use manager::{ApplicationOptions, ManagerRpcService, ShuffleManager, ShuffleManagerRef};
pub use reader::ShuffleReader;
use riffle_server::ddashmap::DDashMap;
use riffle_server::grpc::protobuf::uniffle::StatusCode;
pub use writer::ShuffleWriter;

mod rpc {
    tonic::include_proto!("riffle.client");
}

const RPC_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;
const MAX_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
const RECORD_HEADER_BYTES: usize = std::mem::size_of::<u32>();
const TASK_ID_LIMIT: usize = 1 << 23;
const BLOCK_SEQUENCE_LIMIT: u32 = 1 << 16;

pub type ApplicationId = String;
pub type ShuffleId = usize;
pub type PartitionId = usize;
pub type TaskId = usize;

#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct Endpoint {
    pub host: String,
    pub grpc_port: u16,
}

impl Endpoint {
    fn uri(&self) -> String {
        let host = if self.host.contains(':') && !self.host.starts_with('[') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        format!("http://{host}:{}", self.grpc_port)
    }
}

impl FromStr for Endpoint {
    type Err = Error;

    fn from_str(address: &str) -> Result<Self> {
        let authority: tonic::codegen::http::uri::Authority =
            address.strip_prefix("http://").unwrap_or(address).parse()?;
        Ok(Self {
            host: authority
                .host()
                .trim_start_matches('[')
                .trim_end_matches(']')
                .to_owned(),
            grpc_port: authority.port_u16().context("endpoint has no valid port")?,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShuffleHandle {
    app_id: String,
    shuffle_id: ShuffleId,
    partition_count: u32,
    assignment: ShuffleAssignment,
}

pub struct WriteTaskInfo {
    pub job_id: u64,
    pub stage_id: usize,
    pub map_id: usize,
    pub attempt_id: usize,
    pub num_mappers: usize,
    pub num_partitions: usize,
}

pub struct ReadTaskInfo {
    pub job_id: u64,
    pub upstream_stage_id: usize,
    pub partitions: Vec<usize>,
}

pub struct ShuffleRecord {
    pub partition_id: PartitionId,
    pub data: Bytes,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ShuffleAssignment {
    pub shuffle_id: ShuffleId,
    // partitionId -> assigned servers
    pub assignments: DDashMap<PartitionId, Vec<Endpoint>>,
}

impl Clone for ShuffleAssignment {
    fn clone(&self) -> Self {
        let assignments = DDashMap::with_capacity(self.assignments.len());
        for entry in self.assignments.iter() {
            assignments.insert(*entry.key(), entry.value().clone());
        }
        Self::new(self.shuffle_id, assignments)
    }
}

impl ShuffleAssignment {
    pub fn new(shuffle_id: ShuffleId, assignments: DDashMap<PartitionId, Vec<Endpoint>>) -> Self {
        Self {
            shuffle_id,
            assignments,
        }
    }

    pub fn get_endpoint(&self, partition_id: PartitionId) -> Result<Endpoint> {
        let endpoints = self
            .assignments
            .get(&partition_id)
            .with_context(|| format!("partition {partition_id} has no assignment"))?;
        endpoints
            .first()
            .cloned()
            .with_context(|| format!("partition {partition_id} has no assigned server"))
    }
}

pub type ShuffleStream = Pin<Box<dyn Stream<Item = Result<ShuffleRecord>> + Send>>;

fn ensure_success(operation: &str, status: i32, message: &str) -> Result<()> {
    if status != StatusCode::Success as i32 {
        bail!("{operation} failed with status {status}: {message}");
    }
    Ok(())
}
