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

use crate::{BlockId, PartitionId};
use riffle_proto::uniffle::StatusCode;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RemoteStatus {
    Success,
    DoubleRegister,
    NoBuffer,
    InvalidStorage,
    NoRegister,
    NoPartition,
    InternalError,
    Timeout,
    AccessDenied,
    InvalidRequest,
    NoBufferForHugePartition,
    StageRetryIgnore,
    ExceedHugePartitionHardLimit,
    AppNotFound,
    InternalNotRetryError,
    HardSplitFromServer,
    Unknown(i32),
}

impl RemoteStatus {
    pub(crate) fn is_success(self) -> bool {
        self == Self::Success
    }

    pub(crate) fn is_retryable_read(self) -> bool {
        matches!(self, Self::InternalError | Self::Timeout)
    }
}

impl From<i32> for RemoteStatus {
    fn from(value: i32) -> Self {
        match StatusCode::try_from(value).ok() {
            Some(StatusCode::Success) => Self::Success,
            Some(StatusCode::DoubleRegister) => Self::DoubleRegister,
            Some(StatusCode::NoBuffer) => Self::NoBuffer,
            Some(StatusCode::InvalidStorage) => Self::InvalidStorage,
            Some(StatusCode::NoRegister) => Self::NoRegister,
            Some(StatusCode::NoPartition) => Self::NoPartition,
            Some(StatusCode::InternalError) => Self::InternalError,
            Some(StatusCode::Timeout) => Self::Timeout,
            Some(StatusCode::AccessDenied) => Self::AccessDenied,
            Some(StatusCode::InvalidRequest) => Self::InvalidRequest,
            Some(StatusCode::NoBufferForHugePartition) => Self::NoBufferForHugePartition,
            Some(StatusCode::StageRetryIgnore) => Self::StageRetryIgnore,
            Some(StatusCode::ExceedHugePartitionHardLimit) => Self::ExceedHugePartitionHardLimit,
            Some(StatusCode::AppNotFound) => Self::AppNotFound,
            Some(StatusCode::InternalNotRetryError) => Self::InternalNotRetryError,
            Some(StatusCode::HardSplitFromServer) => Self::HardSplitFromServer,
            None => Self::Unknown(value),
        }
    }
}

#[derive(Debug, Error)]
pub enum RiffleError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("invalid shuffle assignment: {0}")]
    InvalidAssignment(String),

    #[error("no coordinator completed the request: {errors:?}")]
    NoAvailableCoordinator { errors: Vec<String> },

    #[error("{operation} transport failure at {endpoint}: {message}")]
    Transport {
        operation: &'static str,
        endpoint: String,
        message: String,
    },

    #[error("{operation} failed with remote status {status:?}: {message}")]
    Remote {
        operation: &'static str,
        status: RemoteStatus,
        message: String,
    },

    #[error("{operation} had an ambiguous write outcome at {endpoint}: {message}")]
    AmbiguousWrite {
        operation: &'static str,
        endpoint: String,
        message: String,
    },

    #[error("corrupt block {block_id}: expected crc {expected}, actual crc {actual}")]
    CrcMismatch {
        block_id: BlockId,
        expected: u32,
        actual: u32,
    },

    #[error("invalid response from {operation}: {message}")]
    InvalidResponse {
        operation: &'static str,
        message: String,
    },

    #[error("partition {partition_id} is missing {missing_count} reported blocks: {sample:?}")]
    MissingBlocks {
        partition_id: PartitionId,
        missing_count: usize,
        sample: Vec<BlockId>,
    },

    #[error("client operation is closed or cancelled")]
    Closed,

    #[error("push attempt cannot continue after a previous failure")]
    AttemptPoisoned,

    #[error("the requested capability is not supported: {0}")]
    Unsupported(String),
}

pub(crate) fn ensure_success(
    operation: &'static str,
    status: i32,
    message: String,
) -> Result<(), RiffleError> {
    let status = RemoteStatus::from(status);
    if status.is_success() {
        Ok(())
    } else {
        Err(RiffleError::Remote {
            operation,
            status,
            message,
        })
    }
}
