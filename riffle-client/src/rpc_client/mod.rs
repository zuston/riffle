pub mod coordinator_rpc_client;
pub mod manager_rpc_client;
pub mod reader_rpc_client;
pub mod writer_rpc_client;

use std::future::Future;
use std::time::Duration;

use crate::{Endpoint, MAX_MESSAGE_BYTES, RPC_TIMEOUT};
use anyhow::{anyhow, Result};
use log::warn;
use riffle_server::ddashmap::DDashMap;
use riffle_server::grpc::protobuf::uniffle::shuffle_server_client::ShuffleServerClient;
use riffle_server::grpc::protobuf::uniffle::StatusCode;
use tonic::transport::Channel;
use tonic::{Code, Request, Response, Status};

const MAX_ATTEMPTS: usize = 8;

pub(crate) struct AttemptError {
    error: anyhow::Error,
    retryable: bool,
}

pub(crate) fn request<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request.set_timeout(RPC_TIMEOUT);
    request
}

pub(crate) async fn call<T>(
    future: impl Future<Output = Result<Response<T>, Status>>,
) -> Result<T, AttemptError> {
    match tokio::time::timeout(RPC_TIMEOUT, future).await {
        Ok(Ok(response)) => Ok(response.into_inner()),
        Ok(Err(status)) => Err(AttemptError {
            retryable: matches!(
                status.code(),
                Code::Unavailable
                    | Code::DeadlineExceeded
                    | Code::ResourceExhausted
                    | Code::Aborted
                    | Code::Internal
                    | Code::Unknown
            ),
            error: status.into(),
        }),
        Err(error) => Err(AttemptError {
            error: error.into(),
            retryable: true,
        }),
    }
}

pub(crate) fn ensure_success(
    operation: &str,
    status: i32,
    message: &str,
) -> Result<(), AttemptError> {
    if status == StatusCode::Success as i32 {
        return Ok(());
    }
    Err(AttemptError {
        error: anyhow!("{operation} failed with status {status}: {message}"),
        retryable: matches!(
            StatusCode::try_from(status),
            Ok(StatusCode::NoBuffer
                | StatusCode::NoBufferForHugePartition
                | StatusCode::InternalError
                | StatusCode::Timeout)
        ),
    })
}

pub(crate) async fn retry<T, F, Fut>(
    endpoint: &Endpoint,
    operation: &str,
    mut attempt: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, AttemptError>>,
{
    for number in 1..=MAX_ATTEMPTS {
        let error = match attempt().await {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        if !error.retryable || number == MAX_ATTEMPTS {
            return Err(error.error.context(format!(
                "{operation} at {} failed after {number} attempt(s)",
                endpoint.uri(),
            )));
        }
        let ceiling_ms = (100u64 << (number - 1).min(5)).min(2000);
        let delay_ms = ceiling_ms / 2 + rand::random::<u64>() % (ceiling_ms / 2 + 1);
        warn!(
            "Retrying {operation} at {} after attempt {number} in {delay_ms}ms: {:#}",
            endpoint.uri(),
            error.error
        );
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
    unreachable!("retry loop always returns on the final attempt")
}

pub type ServerClient = ShuffleServerClient<Channel>;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct ServerConnOptions {
    pub endpoint: Endpoint,
    pub timeout: Duration,
}

impl ServerConnOptions {
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            timeout: RPC_TIMEOUT,
        }
    }
}

impl From<Endpoint> for ServerConnOptions {
    fn from(endpoint: Endpoint) -> Self {
        Self::new(endpoint)
    }
}

#[derive(Default)]
pub struct ServerCoonPool {
    clients: DDashMap<ServerConnOptions, ServerClient>,
}

impl ServerCoonPool {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_or_create_client(&self, conn_options: ServerConnOptions) -> Result<ServerClient> {
        if let Some(client) = self.clients.get(&conn_options) {
            return Ok(client.value().clone());
        }
        let channel = lazy_channel(&conn_options.endpoint, conn_options.timeout)?;
        Ok(self.clients.compute_if_absent(conn_options, || {
            ServerClient::new(channel)
                .max_encoding_message_size(MAX_MESSAGE_BYTES)
                .max_decoding_message_size(MAX_MESSAGE_BYTES)
        }))
    }

    pub(crate) fn clients(&self) -> Vec<(Endpoint, ServerClient)> {
        self.clients
            .iter()
            .map(|entry| (entry.key().endpoint.clone(), entry.value().clone()))
            .collect()
    }
}

fn lazy_channel(endpoint: &Endpoint, timeout: Duration) -> Result<Channel> {
    Ok(tonic::transport::Endpoint::from_shared(endpoint.uri())?
        .connect_timeout(timeout)
        .timeout(timeout)
        .connect_lazy())
}
