use crate::app::AppManagerRef;
use crate::await_tree::AWAIT_TREE_REGISTRY;
use crate::config::Config;
use crate::grpc::layer::awaittree::AwaitTreeMiddlewareLayer;
use crate::grpc::layer::metric::MetricsMiddlewareLayer;
use crate::grpc::layer::tracing::TracingMiddleWareLayer;
use crate::grpc::protobuf::uniffle::shuffle_server_server::ShuffleServerServer;
use crate::grpc::service::{DefaultShuffleServer, MAX_CONNECTION_WINDOW_SIZE, STREAM_WINDOW_SIZE};
use crate::metric::GRPC_LATENCY_TIME_SEC;
use crate::runtime::manager::RuntimeManager;
use crate::signal::details::graceful_wait_for_signal;
use crate::urpc::server::urpc_serve;
use anyhow::Result;
use async_trait::async_trait;
use log::{debug, error, info};
use once_cell::sync::Lazy;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroUsize;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Sender;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

pub static GRPC_PARALLELISM: Lazy<NonZeroUsize> = Lazy::new(|| {
    let available_cores = std::thread::available_parallelism().unwrap();
    std::env::var("GRPC_PARALLELISM").map_or(available_cores, |v| {
        let parallelism: NonZeroUsize = v.as_str().parse().unwrap();
        parallelism
    })
});

#[async_trait]
trait RpcService {
    async fn start(
        &self,
        config: Config,
        runtime_manager: RuntimeManager,
        app_manager_ref: AppManagerRef,
    ) -> Result<()>;
}

pub struct DefaultRpcService;
impl DefaultRpcService {
    fn start_urpc(
        config: &Config,
        runtime_manager: RuntimeManager,
        tx: Sender<()>,
        app_manager_ref: AppManagerRef,
    ) -> Result<()> {
        let urpc_port = config.urpc_port.unwrap();

        async fn shutdown(tx: Sender<()>) -> Result<()> {
            let mut rx = tx.subscribe();
            if let Err(err) = rx.recv().await {
                error!("Errors on stopping the GRPC service, err: {:?}.", err);
            } else {
                debug!("GRPC service has been graceful stopped.");
            }
            Ok(())
        }

        runtime_manager.grpc_runtime.spawn(async move {
            info!("Starting urpc server with port:[{}] ......", urpc_port);
            urpc_serve(urpc_port as usize, shutdown(tx), app_manager_ref).await
        });

        Ok(())
    }

    fn start_grpc(
        config: &Config,
        runtime_manager: RuntimeManager,
        tx: Sender<()>,
        app_manager_ref: AppManagerRef,
    ) -> Result<()> {
        let grpc_port = config.grpc_port.unwrap();

        info!("Starting grpc server with port:[{}] ......", grpc_port);
        let parallelism = GRPC_PARALLELISM.get();
        info!("grpc service with parallelism: [{}]", &parallelism);

        for _ in 0..parallelism.into() {
            let shuffle_server = DefaultShuffleServer::from(app_manager_ref.clone());
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), grpc_port as u16);
            let service = ShuffleServerServer::new(shuffle_server)
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX);
            let service_tx = tx.subscribe();
            runtime_manager
                .grpc_runtime
                .spawn(async move { grpc_serve(service, addr, service_tx).await });
        }

        Ok(())
    }

    pub fn start(
        &self,
        config: &Config,
        runtime_manager: RuntimeManager,
        app_manager_ref: AppManagerRef,
    ) -> Result<()> {
        let (tx, _) = broadcast::channel(1);

        DefaultRpcService::start_grpc(
            config,
            runtime_manager.clone(),
            tx.clone(),
            app_manager_ref.clone(),
        )?;

        let urpc_port = config.urpc_port;
        if urpc_port.is_some() {
            DefaultRpcService::start_urpc(
                config,
                runtime_manager.clone(),
                tx.clone(),
                app_manager_ref.clone(),
            )?;
        }

        graceful_wait_for_signal(tx);

        Ok(())
    }
}

async fn grpc_serve(
    service: ShuffleServerServer<DefaultShuffleServer>,
    addr: SocketAddr,
    mut rx: broadcast::Receiver<()>,
) {
    let sock = socket2::Socket::new(
        match addr {
            SocketAddr::V4(_) => socket2::Domain::IPV4,
            SocketAddr::V6(_) => socket2::Domain::IPV6,
        },
        socket2::Type::STREAM,
        None,
    )
    .unwrap();

    sock.set_reuse_address(true).unwrap();
    sock.set_reuse_port(true).unwrap();
    sock.set_nonblocking(true).unwrap();
    sock.bind(&addr.into()).unwrap();
    sock.listen(8192).unwrap();

    let incoming = TcpListenerStream::new(TcpListener::from_std(sock.into()).unwrap());

    Server::builder()
        .initial_connection_window_size(MAX_CONNECTION_WINDOW_SIZE)
        .initial_stream_window_size(STREAM_WINDOW_SIZE)
        .tcp_nodelay(true)
        .layer(TracingMiddleWareLayer::new())
        .layer(MetricsMiddlewareLayer::new(GRPC_LATENCY_TIME_SEC.clone()))
        .layer(AwaitTreeMiddlewareLayer::new_optional(Some(
            AWAIT_TREE_REGISTRY.clone(),
        )))
        .add_service(service)
        .serve_with_incoming_shutdown(incoming, async {
            if let Err(err) = rx.recv().await {
                error!("Errors on stopping the GRPC service, err: {:?}.", err);
            } else {
                debug!("GRPC service has been graceful stopped.");
            }
        })
        .await
        .unwrap();
}
