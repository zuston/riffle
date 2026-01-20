// Copyright 2025 foyer Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! io-uring based URPC server implementation.
//!
//! This module provides a high-performance URPC server using io-uring
//! for asynchronous I/O operations on Linux.

use crate::app_manager::AppManagerRef;
use crate::await_tree::AWAIT_TREE_REGISTRY;
use crate::error::WorkerError;
use crate::metric::{TOTAL_URPC_REQUEST, URPC_CONNECTION_NUMBER, URPC_REQUEST_PROCESSING_LATENCY};
use crate::urpc::command::Command;
use crate::urpc::connection::UringCompatibleConnection;
use crate::urpc::frame::Frame;
use crate::urpc::shutdown::Shutdown;
use anyhow::Result;
use await_tree::InstrumentAwait;
use log::{debug, error, info};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};

use super::connection::UringConnection;

const MAX_CONNECTIONS: usize = 40000;

struct Listener {
    listener: TcpListener,
    limit_connections: Arc<Semaphore>,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

impl Listener {
    async fn run(&mut self, app_manager_ref: AppManagerRef) -> Result<()> {
        debug!("Accepting inbound connections (io-uring mode)");

        loop {
            let app_manager = app_manager_ref.clone();
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .unwrap();

            let socket = self.accept().await?;
            let addr = socket.peer_addr()?.to_string();
            debug!("Accepted io-uring connection from client: {}", &addr);

            let mut handler = Handler {
                connection: UringConnection::new(socket)?,
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
                remote_addr: addr.to_string(),
            };

            tokio::spawn(async move {
                URPC_CONNECTION_NUMBER.inc();
                if let Err(error) = handler.run(app_manager).await {
                    error!("Errors on handling the request. {:#?}", error);
                }
                drop(permit);
                URPC_CONNECTION_NUMBER.dec();
            });
        }
    }

    async fn accept(&mut self) -> Result<TcpStream> {
        let mut backoff = 1;

        loop {
            match self.listener.accept().await {
                Ok((socket, _)) => {
                    // todo: fine-grained keep_alive options
                    let sock_ref = socket2::SockRef::from(&socket);
                    sock_ref.set_keepalive(true)?;
                    sock_ref.set_nodelay(true)?;
                    return Ok(socket);
                }
                Err(err) => {
                    if backoff > 64 {
                        return Err(err.into());
                    }
                }
            }

            tokio::time::sleep(Duration::from_secs(backoff)).await;
            backoff *= 2;
            info!("Backoff: {}", backoff);
        }
    }
}

#[derive(Debug)]
struct Handler<C: UringCompatibleConnection> {
    connection: C,
    shutdown: Shutdown,
    remote_addr: String,
    _shutdown_complete: mpsc::Sender<()>,
}

impl<C: UringCompatibleConnection> Handler<C> {
    /// when the shutdown signal is received, the connection is processed
    /// util it reaches a safe state, at which point it is terminated
    async fn run(&mut self, app_manager_ref: AppManagerRef) -> Result<(), WorkerError> {
        let await_registry = AWAIT_TREE_REGISTRY.clone();
        while !self.shutdown.is_shutdown() {
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    return Ok(());
                },
            };

            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            let path = frame.to_string();
            TOTAL_URPC_REQUEST.with_label_values(&[&"ALL"]).inc();
            TOTAL_URPC_REQUEST.with_label_values(&[&path]).inc();

            let timer = URPC_REQUEST_PROCESSING_LATENCY
                .with_label_values(&[&format!("{}", &frame)])
                .start_timer();
            let await_root = await_registry
                .register(format!(
                    "urpc io-uring connection with remote client: {}",
                    &self.remote_addr
                ))
                .await;
            await_root
                .instrument(Command::from_frame(frame)?.apply(
                    app_manager_ref.clone(),
                    &mut self.connection,
                    &mut self.shutdown,
                ))
                .await?;
        }
        Ok(())
    }
}

/// Run the io-uring URPC server.
///
/// This function starts a URPC server that uses io-uring for high-performance
/// I/O operations on Linux.
pub async fn run(listener: TcpListener, shutdown: impl Future, app_manager_ref: AppManagerRef) {
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

    let mut server = Listener {
        listener,
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_tx,
    };

    tokio::select! {
        res = server.run(app_manager_ref) => {
            if let Err(err) = res {
                error!("Errors on running urpc io-uring server. err: {:#?}", err);
            }
        }
        _ = shutdown => {
            info!("Accepting the shutdown signal for the urpc io-uring net service");
        }
    }

    let Listener {
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = server;

    // When `notify_shutdown` is dropped, all tasks which have `subscribe`d will
    // receive the shutdown signal and can exit
    drop(notify_shutdown);
    drop(shutdown_complete_tx);

    let _ = shutdown_complete_rx.recv().await;
}
