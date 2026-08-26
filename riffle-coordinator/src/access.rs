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

use crate::grpc::protobuf::uniffle::AccessClusterRequest;

#[derive(Debug)]
pub enum AccessResult {
    Allow,
    Deny(String),
}

#[derive(Debug, thiserror::Error)]
#[error("access plugin failed: {0}")]
pub struct AccessError(pub String);

#[tonic::async_trait]
pub trait AccessPlugin: Send + Sync {
    async fn access(&self, request: &AccessClusterRequest) -> Result<AccessResult, AccessError>;
}

#[derive(Default)]
pub struct AllowAllAccessPlugin;

#[tonic::async_trait]
impl AccessPlugin for AllowAllAccessPlugin {
    async fn access(&self, _request: &AccessClusterRequest) -> Result<AccessResult, AccessError> {
        Ok(AccessResult::Allow)
    }
}
