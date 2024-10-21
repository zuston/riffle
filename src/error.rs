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

use anyhow::Error;
use std::string::FromUtf8Error;

use log::error;
use poem::error::ParseQueryError;
use thiserror::Error;
use tokio::sync::AcquireError;

#[derive(Error, Debug)]
#[allow(non_camel_case_types)]
pub enum WorkerError {
    #[error("There is no available disks in local file store")]
    NO_AVAILABLE_LOCAL_DISK,

    #[error("Internal error, it should not happen")]
    INTERNAL_ERROR,

    #[error("App is not found")]
    APP_IS_NOT_FOUND,

    #[error("No candidate storage selected for this spill event")]
    NO_CANDIDATE_STORE,

    #[error("Partial data has been lost, corrupted path: {0}")]
    PARTIAL_DATA_LOST(String),

    #[error("Local disk:[{0}] is not healthy")]
    LOCAL_DISK_UNHEALTHY(String),

    #[error("Local disk:[{0}] owned by current partition has been corrupted")]
    LOCAL_DISK_OWNED_BY_PARTITION_CORRUPTED(String),

    #[error("No enough memory to be allocated.")]
    NO_ENOUGH_MEMORY_TO_BE_ALLOCATED,

    #[error("The memory usage is limited by huge partition mechanism")]
    MEMORY_USAGE_LIMITED_BY_HUGE_PARTITION,

    #[error(transparent)]
    Other(#[from] anyhow::Error),

    #[error("Http request failed. {0}")]
    HTTP_SERVICE_ERROR(String),

    #[error("Ticket id: {0} not exist")]
    TICKET_ID_NOT_EXIST(i64),

    #[error("Hdfs native client not found for app: {0}")]
    HDFS_NATIVE_CLIENT_NOT_FOUND(String),

    #[error("App has been purged")]
    APP_HAS_BEEN_PURGED,

    #[error("Data should be read from hdfs in client side instead of from server side")]
    NOT_READ_HDFS_DATA_FROM_SERVER,

    #[error("Spill event has been retried exceed the max reject for app: {0}")]
    SPILL_EVENT_EXCEED_RETRY_MAX_LIMIT(String),

    #[error("urpc stream is incomplete")]
    STREAM_INCOMPLETE,

    #[error("urpc stream is incorrect: {0}")]
    STREAM_INCORRECT(String),

    #[error("urpc stream is abnormal")]
    STREAM_ABNORMAL,

    #[error("urpc stream message type not found")]
    STREAM_MESSAGE_TYPE_NOT_FOUND,
}

impl From<AcquireError> for WorkerError {
    fn from(error: AcquireError) -> Self {
        WorkerError::Other(Error::new(error))
    }
}

impl From<ParseQueryError> for WorkerError {
    fn from(error: ParseQueryError) -> Self {
        WorkerError::Other(Error::new(error))
    }
}

impl From<std::io::Error> for WorkerError {
    fn from(err: std::io::Error) -> Self {
        WorkerError::Other(Error::new(err))
    }
}

impl From<FromUtf8Error> for WorkerError {
    fn from(value: FromUtf8Error) -> Self {
        WorkerError::Other(Error::new(value))
    }
}

#[cfg(test)]
mod tests {

    use anyhow::Result;

    #[test]
    pub fn error_test() -> Result<()> {
        // bail macro means it will return directly.
        // bail!(WorkerError::APP_PURGE_EVENT_SEND_ERROR("error_test_app_id".into(), None));
        Ok(())
    }
}
