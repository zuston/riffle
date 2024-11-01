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

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use opendal::Metadata;

pub mod async_io;
mod delegator;
pub mod disk;
pub mod sync_io;

pub struct FileStat {
    pub content_length: u64,
}

impl From<Metadata> for FileStat {
    fn from(meta: Metadata) -> Self {
        let content_length = meta.content_length();
        Self {
            content_length: content_length,
        }
    }
}

#[async_trait]
trait LocalIO: Clone {
    async fn create_dir(&self, dir: &str) -> Result<()>;
    async fn append(&self, path: &str, data: Bytes) -> Result<()>;
    async fn read(&self, path: &str, offset: i64, length: Option<i64>) -> Result<Bytes>;
    async fn delete(&self, path: &str) -> Result<()>;
    async fn write(&self, path: &str, data: Bytes) -> Result<()>;
    async fn file_stat(&self, path: &str) -> Result<FileStat>;
}

#[async_trait]
trait LocalDiskStorage: LocalIO {
    async fn is_healthy(&self) -> Result<bool>;
    async fn is_corrupted(&self) -> Result<bool>;

    async fn mark_healthy(&self) -> Result<()>;
    async fn mark_unhealthy(&self) -> Result<()>;
    async fn mark_corrupted(&self) -> Result<()>;
}
