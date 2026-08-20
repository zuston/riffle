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

//! Engine-neutral Rust client for Riffle shuffle services.
//!
//! [`Driver`] owns the application and shuffle control plane. It produces a
//! serializable [`ShuffleHandle`], which an execution engine transports to
//! independently constructed [`ShuffleWriter`] and [`ShuffleReader`] values.
//! The three roles do not call one another and do not require an actor runtime.
//!
//! Block payloads are opaque bytes. Encoding, partitioning, scheduling, and
//! allocation of a unique [`TaskAttemptId`] for every physical execution
//! attempt remain the execution engine's responsibility. Readers must receive
//! only the attempt IDs accepted by that engine's scheduler.

mod connection_pool;
mod coordinator_client;
mod driver;
mod error;
mod read_client;
mod reader;
mod retry;
mod types;
mod write_client;
mod writer;

pub use driver::{ApplicationSession, Driver};
pub use error::{RemoteStatus, RiffleError};
pub use reader::{ShuffleBlockStream, ShuffleReader};
pub use retry::RetryPolicy;
pub use types::{
    ApplicationId, ApplicationSpec, BlockId, BlockIdLayout, BlockPayload, DataDistribution,
    DriverConfig, MapOutput, PartitionId, PartitionRoute, ReadPartitionRequest, RemoteStorageSpec,
    ShuffleBlock, ShuffleHandle, ShuffleId, ShuffleReaderConfig, ShuffleServer, ShuffleSpec,
    ShuffleWriterConfig, TaskAttemptId,
};
pub use writer::{PushAttempt, ShuffleWriter};
