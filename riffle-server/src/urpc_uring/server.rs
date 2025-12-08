use log::{debug, error, info};
use monoio::net::{TcpListener, TcpStream};
use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;

use crate::urpc_uring::connection::Connection;

use crate::app_manager::AppManagerRef;
use crate::await_tree::AWAIT_TREE_REGISTRY;
use crate::error::WorkerError;
use crate::metric::{
    TOTAL_GRPC_REQUEST, TOTAL_URPC_REQUEST, URPC_CONNECTION_NUMBER, URPC_REQUEST_PROCESSING_LATENCY,
};
use crate::urpc_uring::command::Command;
use anyhow::Result;
use await_tree::InstrumentAwait;
use socket2::SockRef;
use tracing::Instrument;

struct Listener {
    listener: TcpListener,
}

impl Listener {
    async fn run(&mut self, app_manager_ref: AppManagerRef) -> Result<()> {
        debug!("Accepting inbound connections");

        loop {
            let app_manager = app_manager_ref.clone();

            let socket = self.accept().await?;
            let addr = socket.peer_addr()?.to_string();
            debug!("Accepted connection from client: {}", &addr);

            let mut handler = Handler {
                connection: Connection::new(socket),
                remote_addr: addr.to_string(),
            };

            monoio::spawn(async move {
                URPC_CONNECTION_NUMBER.inc();
                if let Err(error) = handler.run(app_manager).await {
                    error!("Errors on handling the request. {:#?}", error);
                }
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
                    let sock_ref = SockRef::from(&socket);
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

            monoio::time::sleep(Duration::from_secs(backoff)).await;
            backoff *= 2;
            info!("Backoff: {}", backoff);
        }
    }
}

#[derive(Debug)]
struct Handler {
    connection: Connection,
    remote_addr: String,
}

impl Handler {
    async fn run(&mut self, app_manager_ref: AppManagerRef) -> Result<(), WorkerError> {
        let await_registry = AWAIT_TREE_REGISTRY.clone();
        loop {
            let maybe_frame = self.connection.read_frame().await?;

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
                    "urpc connection with remote client: {}",
                    &self.remote_addr
                ))
                .await;
            await_root
                .instrument(
                    Command::from_frame(frame)?
                        .apply(app_manager_ref.clone(), &mut self.connection),
                )
                .await?;
        }
    }
}

pub async fn run(listener: TcpListener, shutdown: impl Future, app_manager_ref: AppManagerRef) {
    let mut server = Listener { listener };

    monoio::select! {
        res = server.run(app_manager_ref) => {
            if let Err(err) = res {
                error!("Errors on running urpc server. err: {:#?}", err);
            }
        }
        _ = shutdown => {
            info!("Accepting the shutdown signal for the urpc net service");
        }
    }
}
