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

use crate::{RiffleError, ShuffleServer};
use riffle_proto::uniffle::shuffle_server_client::ShuffleServerClient;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint};

#[derive(Clone, Debug)]
pub(crate) struct GrpcClientSettings {
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub max_encoding_message_size: usize,
    pub max_decoding_message_size: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct GrpcChannelPool {
    settings: GrpcClientSettings,
    channels: Arc<RwLock<HashMap<String, Channel>>>,
}

impl GrpcChannelPool {
    pub(crate) fn new(settings: GrpcClientSettings) -> Self {
        Self {
            settings,
            channels: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub(crate) async fn channel(&self, server: &ShuffleServer) -> Result<Channel, RiffleError> {
        server.validate()?;
        let endpoint = server.grpc_endpoint();
        if let Some(channel) = self.channels.read().await.get(&endpoint).cloned() {
            return Ok(channel);
        }

        let channel = connect_channel(
            &endpoint,
            self.settings.connect_timeout,
            self.settings.request_timeout,
            "connect_shuffle_server",
        )
        .await?;
        let mut channels = self.channels.write().await;
        Ok(channels
            .entry(endpoint)
            .or_insert_with(|| channel.clone())
            .clone())
    }

    pub(crate) fn client(&self, channel: Channel) -> ShuffleServerClient<Channel> {
        ShuffleServerClient::new(channel)
            .max_encoding_message_size(self.settings.max_encoding_message_size)
            .max_decoding_message_size(self.settings.max_decoding_message_size)
    }
}

pub(crate) async fn connect_channel(
    endpoint: &str,
    connect_timeout: Duration,
    request_timeout: Duration,
    operation: &'static str,
) -> Result<Channel, RiffleError> {
    let endpoint = normalize_endpoint(endpoint)?;
    let channel = Endpoint::from_shared(endpoint.clone())
        .map_err(|error| {
            RiffleError::InvalidArgument(format!("invalid endpoint {endpoint}: {error}"))
        })?
        .connect_timeout(connect_timeout)
        .timeout(request_timeout)
        .connect()
        .await
        .map_err(|error| RiffleError::Transport {
            operation,
            endpoint,
            message: error.to_string(),
        })?;
    Ok(channel)
}

pub(crate) fn normalize_endpoint(endpoint: &str) -> Result<String, RiffleError> {
    let endpoint = endpoint.trim();
    if endpoint.is_empty() {
        return Err(RiffleError::InvalidArgument(
            "endpoint must not be empty".to_string(),
        ));
    }
    if endpoint.contains("://") {
        Ok(endpoint.to_string())
    } else {
        Ok(format!("http://{endpoint}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_scheme_is_optional() {
        assert_eq!(
            normalize_endpoint("127.0.0.1:19999").unwrap(),
            "http://127.0.0.1:19999"
        );
        assert_eq!(
            normalize_endpoint("https://coordinator.example:443").unwrap(),
            "https://coordinator.example:443"
        );
    }
}
