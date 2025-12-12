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

    #[test]
    fn test_uring_write_read() -> anyhow::Result<()> {
        use crate::runtime::manager::create_runtime;
        use crate::runtime::RuntimeRef;
        use crate::store::local::sync_io::SyncLocalIO;
        use crate::store::local::uring_io::UringIoEngineBuilder;
        use crate::store::local::LocalIO;
        use log::info;

        use crate::store::DataBytes;
        use bytes::Bytes;
        use tempdir::TempDir;
        use tokio::runtime::Runtime;

        let temp_dir = TempDir::new("test_write_read")?;
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        log::info!("init local file path: {}", temp_path);

        let r_runtime = create_runtime(1, "r");
        let w_runtime = create_runtime(2, "w");

        let sync_io_engine =
            SyncLocalIO::new(&r_runtime, &w_runtime, temp_path.as_str(), None, None);
        let uring_io_engine = UringIoEngineBuilder::new().build(sync_io_engine)?;

        // 1. write
        let write_data = b"hello io_uring test";
        let write_options = crate::store::local::options::WriteOptions {
            offset: Some(0),
            data: DataBytes::Direct(Bytes::from(write_data)),
            ..Default::default()
        };

        w_runtime.block_on(async {
            uring_io_engine
                .write("test_file", write_options)
                .await
                .unwrap();
        });

        // 2. read
        let read_options = crate::store::local::read_options::ReadOptions {
            task_id: 0,
            read_range: crate::store::local::read_options::ReadRange::ALL,
            ..Default::default()
        };

        let result = r_runtime.block_on(async {
            uring_io_engine
                .read("test_file", read_options)
                .await
                .unwrap()
        });

        // 3. validation
        match result {
            DataBytes::Direct(bytes) => {
                assert_eq!(bytes.as_ref(), write_data.as_slice());
            }
            _ => panic!("Expected direct bytes"),
        }

        Ok(())
    }
}
