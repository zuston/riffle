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

pub use manager::{ApplicationOptions, ShuffleManager};
pub use reader::ShuffleReader;
use riffle_server::ddashmap::DDashMap;
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
    manager_endpoint: Endpoint,
    assignment: ShuffleAssignment,
}

pub struct TaskInfo {
    pub shuffle: ShuffleHandle,
    /// Engine-assigned ID, unique per shuffle and different for every retry.
    pub task_id: TaskId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WriteResult {
    app_id: ApplicationId,
    shuffle_id: ShuffleId,
    task_id: TaskId,
    /// Block IDs in writer sequence order.
    block_ids: Vec<i64>,
}

impl WriteResult {
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }
}

pub struct ReadContext {
    pub shuffle: ShuffleHandle,
    pub partitions: Vec<PartitionId>,
    pub write_results: Vec<WriteResult>,
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

    /// Returns an owned endpoint so callers do not hold a map guard across awaits.
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

fn check_status(operation: &str, status: i32, message: &str) -> Result<()> {
    if status != 0 {
        bail!("{operation} failed with status {status}: {message}");
    }
    Ok(())
}
