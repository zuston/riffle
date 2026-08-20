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

use crate::connection_pool::{Connection, ConnectionPool, ConnectionSettings};
use crate::RiffleError;
use riffle_proto::uniffle::coordinator_server_client::CoordinatorServerClient;
use riffle_proto::uniffle::{
    AccessClusterRequest, AccessClusterResponse, AppHeartBeatRequest, AppHeartBeatResponse,
    ApplicationInfoRequest, ApplicationInfoResponse, GetShuffleAssignmentsResponse,
    GetShuffleServerRequest,
};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tonic::transport::Channel;
use tonic::{Code, Response, Status};

#[derive(Clone, Debug)]
pub(crate) struct CoordinatorClient {
    pool: ConnectionPool,
    endpoints: Arc<[CoordinatorEndpoint]>,
    settings: ConnectionSettings,
}

#[derive(Debug)]
struct CoordinatorEndpoint {
    endpoint: String,
    connection: OnceCell<Arc<Connection>>,
}

impl CoordinatorClient {
    pub(crate) async fn connect(
        endpoints: Vec<String>,
        settings: ConnectionSettings,
    ) -> Result<Self, RiffleError> {
        let client = Self {
            pool: ConnectionPool::global(),
            endpoints: endpoints
                .into_iter()
                .map(|endpoint| CoordinatorEndpoint {
                    endpoint,
                    connection: OnceCell::new(),
                })
                .collect::<Vec<_>>()
                .into(),
            settings,
        };
        let mut errors = Vec::new();
        for endpoint in client.endpoints.iter() {
            match endpoint.client(&client.pool, &client.settings).await {
                Ok(_) => return Ok(client),
                Err(error) => errors.push(error.to_string()),
            }
        }
        Err(RiffleError::NoAvailableCoordinator { errors })
    }

    pub(crate) async fn access_cluster(
        &self,
        request: AccessClusterRequest,
    ) -> Result<AccessClusterResponse, RiffleError> {
        self.call("access_cluster", |mut client| {
            let request = request.clone();
            async move { client.access_cluster(request).await }
        })
        .await
    }

    pub(crate) async fn register_application(
        &self,
        request: ApplicationInfoRequest,
    ) -> Result<ApplicationInfoResponse, RiffleError> {
        self.call("register_application", |mut client| {
            let request = request.clone();
            async move { client.register_application_info(request).await }
        })
        .await
    }

    pub(crate) async fn get_shuffle_assignments(
        &self,
        request: GetShuffleServerRequest,
    ) -> Result<GetShuffleAssignmentsResponse, RiffleError> {
        self.call("get_shuffle_assignments", |mut client| {
            let request = request.clone();
            async move { client.get_shuffle_assignments(request).await }
        })
        .await
    }

    pub(crate) async fn app_heartbeat(
        &self,
        request: AppHeartBeatRequest,
    ) -> Result<AppHeartBeatResponse, RiffleError> {
        self.call("coordinator_app_heartbeat", |mut client| {
            let request = request.clone();
            async move { client.app_heartbeat(request).await }
        })
        .await
    }

    async fn call<T, F, Fut>(&self, operation: &'static str, mut rpc: F) -> Result<T, RiffleError>
    where
        F: FnMut(CoordinatorServerClient<Channel>) -> Fut,
        Fut: Future<Output = Result<Response<T>, Status>>,
    {
        let mut errors = Vec::new();
        for endpoint in self.endpoints.iter() {
            let client = match endpoint.client(&self.pool, &self.settings).await {
                Ok(client) => client,
                Err(error) => {
                    errors.push(error.to_string());
                    continue;
                }
            };
            match rpc(client).await {
                Ok(response) => return Ok(response.into_inner()),
                Err(error) if is_retryable(&error) => errors.push(format!(
                    "{operation} transport failure at {}: {error}",
                    endpoint.endpoint
                )),
                Err(error) => {
                    return Err(RiffleError::Transport {
                        operation,
                        endpoint: endpoint.endpoint.clone(),
                        message: error.to_string(),
                    });
                }
            }
        }
        Err(RiffleError::NoAvailableCoordinator { errors })
    }
}

impl CoordinatorEndpoint {
    async fn client(
        &self,
        pool: &ConnectionPool,
        settings: &ConnectionSettings,
    ) -> Result<CoordinatorServerClient<Channel>, RiffleError> {
        let connection = self
            .connection
            .get_or_try_init(|| pool.connect(&self.endpoint, settings, "connect_coordinator"))
            .await?;
        Ok(CoordinatorServerClient::new(connection.channel())
            .max_encoding_message_size(settings.max_encoding_message_size)
            .max_decoding_message_size(settings.max_decoding_message_size))
    }
}

fn is_retryable(error: &Status) -> bool {
    matches!(
        error.code(),
        Code::Unknown
            | Code::Cancelled
            | Code::DeadlineExceeded
            | Code::Internal
            | Code::Unavailable
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_transient_statuses_are_retried() {
        assert!(is_retryable(&Status::unavailable("offline")));
        assert!(is_retryable(&Status::deadline_exceeded("slow")));
        assert!(!is_retryable(&Status::invalid_argument("invalid")));
        assert!(!is_retryable(&Status::permission_denied("denied")));
    }
}
