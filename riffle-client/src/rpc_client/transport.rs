use std::future::Future;
use std::time::Duration;

use anyhow::{anyhow, Result};
use log::warn;
use riffle_server::grpc::protobuf::uniffle::StatusCode;
use tonic::{Code, Request, Response, Status};

use crate::{Endpoint, RPC_TIMEOUT};

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

pub(crate) fn check_status(
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
