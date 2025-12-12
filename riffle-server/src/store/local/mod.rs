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

use crate::error::WorkerError;
use crate::store::local::options::{CreateOptions, WriteOptions};
use crate::store::local::read_options::ReadOptions;
use crate::store::DataBytes;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;

pub mod delegator;
mod io_layer_await_tree;
mod io_layer_metrics;
pub mod io_layer_read_ahead;
mod io_layer_retry;
pub mod io_layer_throttle;
mod io_layer_timeout;
pub mod layers;
pub mod options;
pub mod read_options;
pub mod sync_io;

#[cfg(feature = "io-uring")]
pub mod uring_io;

pub struct FileStat {
    pub content_length: u64,
}

#[async_trait]
pub trait LocalIO: Send + Sync {
    async fn create(&self, path: &str, options: CreateOptions) -> Result<(), WorkerError>;
    async fn write(&self, path: &str, options: WriteOptions) -> Result<(), WorkerError>;
    async fn read(&self, path: &str, options: ReadOptions) -> Result<DataBytes, WorkerError>;
    async fn delete(&self, path: &str) -> Result<(), WorkerError>;
    async fn file_stat(&self, path: &str) -> Result<FileStat, WorkerError>;
}

pub trait LocalDiskStorage {
    fn is_healthy(&self) -> Result<bool>;
    fn is_corrupted(&self) -> Result<bool>;
}

pub struct DiskStat {
    pub(crate) root: String,
    pub(crate) used_ratio: f64,
}

pub struct LocalfileStoreStat {
    pub(crate) stats: Vec<DiskStat>,
}

impl LocalfileStoreStat {
    pub fn is_healthy(&self, used_ratio_threshold: f64) -> bool {
        for stat in &self.stats {
            if stat.used_ratio > used_ratio_threshold {
                return false;
            }
        }
        true
    }
}

impl Default for LocalfileStoreStat {
    fn default() -> Self {
        Self { stats: vec![] }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    #[test]
    fn test_bytes_as_mut_ptr() {
        struct RawBuf<T> {
            ptr: T,
            len: usize,
        }

        // 1. use bytesMut to write
        let mut bytes = BytesMut::zeroed(10);
        let ptr = bytes.as_mut_ptr();
        let raw_buf = RawBuf { ptr, len: 10 };
        unsafe {
            for i in 0..bytes.len() {
                *raw_buf.ptr.add(i) = 1;
            }
        }
        for &b in bytes.iter() {
            assert_eq!(b, 1);
        }

        // 2. use bytes to read
        let mut bytes = bytes.freeze();
        let prt = bytes.as_ptr();
        let raw_buf = RawBuf { ptr, len: 10 };
    }
}
