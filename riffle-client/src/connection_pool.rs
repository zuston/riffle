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
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};

#[derive(Clone, Debug)]
pub(crate) struct ConnectionSettings {
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
}

#[derive(Clone, Debug)]
pub(crate) struct ConnectionPool {
    inner: Arc<PoolInner>,
}

#[derive(Debug, Default)]
struct PoolInner {
    connections: Mutex<HashMap<ConnectionKey, Weak<Connection>>>,
}

#[derive(Debug)]
pub(crate) struct Connection {
    endpoint: String,
    channel: Channel,
}

#[derive(Debug, Eq, Hash, PartialEq)]
struct ConnectionKey {
    endpoint: String,
    connect_timeout: Duration,
    request_timeout: Duration,
}

static GLOBAL_POOL: OnceLock<ConnectionPool> = OnceLock::new();

impl ConnectionPool {
    fn new() -> Self {
        Self {
            inner: Arc::new(PoolInner::default()),
        }
    }

    pub(crate) fn global() -> Self {
        GLOBAL_POOL.get_or_init(Self::new).clone()
    }

    pub(crate) fn get_server(
        &self,
        server: &ShuffleServer,
        settings: &ConnectionSettings,
    ) -> Result<Arc<Connection>, RiffleError> {
        server.validate()?;
        self.get(&server.grpc_endpoint(), settings)
    }

    pub(crate) fn shuffle_server_client(
        &self,
        server: &ShuffleServer,
        settings: &ConnectionSettings,
    ) -> Result<(Arc<Connection>, ShuffleServerClient<Channel>), RiffleError> {
        let connection = self.get_server(server, settings)?;
        let client = ShuffleServerClient::new(connection.channel())
            .max_encoding_message_size(usize::MAX)
            .max_decoding_message_size(usize::MAX);
        Ok((connection, client))
    }

    pub(crate) fn get(
        &self,
        endpoint: &str,
        settings: &ConnectionSettings,
    ) -> Result<Arc<Connection>, RiffleError> {
        let (key, builder) = self.configured_endpoint(endpoint, settings)?;
        let mut connections = self.lock_connections(&key.endpoint)?;
        if let Some(connection) = connections.get(&key).and_then(Weak::upgrade) {
            return Ok(connection);
        }

        let connection = Arc::new(Connection {
            endpoint: key.endpoint.clone(),
            channel: builder.connect_lazy(),
        });
        connections.insert(key, Arc::downgrade(&connection));
        Ok(connection)
    }

    pub(crate) async fn connect(
        &self,
        endpoint: &str,
        settings: &ConnectionSettings,
        operation: &'static str,
    ) -> Result<Arc<Connection>, RiffleError> {
        let (key, builder) = self.configured_endpoint(endpoint, settings)?;
        if let Some(connection) = self
            .lock_connections(&key.endpoint)?
            .get(&key)
            .and_then(Weak::upgrade)
        {
            return Ok(connection);
        }

        let channel = builder
            .connect()
            .await
            .map_err(|error| RiffleError::Transport {
                operation,
                endpoint: key.endpoint.clone(),
                message: error.to_string(),
            })?;
        let connection = Arc::new(Connection {
            endpoint: key.endpoint.clone(),
            channel,
        });
        let mut connections = self.lock_connections(&key.endpoint)?;
        if let Some(existing) = connections.get(&key).and_then(Weak::upgrade) {
            return Ok(existing);
        }
        connections.insert(key, Arc::downgrade(&connection));
        Ok(connection)
    }

    fn configured_endpoint(
        &self,
        endpoint: &str,
        settings: &ConnectionSettings,
    ) -> Result<(ConnectionKey, Endpoint), RiffleError> {
        let endpoint = self.normalize_endpoint(endpoint)?;
        let builder = Endpoint::from_shared(endpoint.clone())
            .map_err(|error| {
                RiffleError::InvalidArgument(format!("invalid endpoint {endpoint}: {error}"))
            })?
            .connect_timeout(settings.connect_timeout)
            .timeout(settings.request_timeout);
        Ok((
            ConnectionKey {
                endpoint,
                connect_timeout: settings.connect_timeout,
                request_timeout: settings.request_timeout,
            },
            builder,
        ))
    }

    fn normalize_endpoint(&self, endpoint: &str) -> Result<String, RiffleError> {
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

    fn lock_connections(
        &self,
        endpoint: &str,
    ) -> Result<MutexGuard<'_, HashMap<ConnectionKey, Weak<Connection>>>, RiffleError> {
        self.inner
            .connections
            .lock()
            .map_err(|_| RiffleError::Transport {
                operation: "get_connection",
                endpoint: endpoint.to_string(),
                message: "connection pool is poisoned".to_string(),
            })
    }
}

impl Connection {
    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub(crate) fn channel(&self) -> Channel {
        self.channel.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_scheme_is_optional() {
        let pool = ConnectionPool::new();
        assert_eq!(
            pool.normalize_endpoint("127.0.0.1:19999").unwrap(),
            "http://127.0.0.1:19999"
        );
        assert_eq!(
            pool.normalize_endpoint("https://coordinator.example:443")
                .unwrap(),
            "https://coordinator.example:443"
        );
    }

    #[tokio::test]
    async fn connection_closes_after_last_reference_is_dropped() {
        let pool = ConnectionPool::new();
        let settings = ConnectionSettings {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
        };
        let first = pool
            .get("http://shared-connection.invalid:19999", &settings)
            .unwrap();
        let second = pool
            .get("http://shared-connection.invalid:19999", &settings)
            .unwrap();
        assert!(Arc::ptr_eq(&first, &second));

        let weak = Arc::downgrade(&first);
        drop(first);
        assert!(weak.upgrade().is_some());
        drop(second);
        assert!(weak.upgrade().is_none());
    }
}
