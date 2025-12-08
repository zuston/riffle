use crate::app_manager::AppManagerRef;
use crate::await_tree::AWAIT_TREE_REGISTRY;
use crate::config::Config;
use crate::grpc::layer::awaittree::AwaitTreeMiddlewareLayer;
use crate::grpc::layer::metric::MetricsMiddlewareLayer;
use crate::grpc::layer::tracing::TracingMiddleWareLayer;
use crate::grpc::protobuf::uniffle::shuffle_server_internal_server::ShuffleServerInternalServer;
use crate::grpc::protobuf::uniffle::shuffle_server_server::ShuffleServerServer;
use crate::grpc::service::{DefaultShuffleServer, MAX_CONNECTION_WINDOW_SIZE, STREAM_WINDOW_SIZE};
use crate::metric::GRPC_LATENCY_TIME_SEC;
use crate::runtime::manager::RuntimeManager;
use crate::server_state_manager::ServerStateManager;
use crate::signal::details::graceful_wait_for_signal;
use crate::util::is_port_in_used;
use crate::{urpc, urpc_uring};
use anyhow::Result;
use async_trait::async_trait;
use log::{debug, error, info};
use once_cell::sync::Lazy;
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroUsize;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

pub static GRPC_PARALLELISM: Lazy<NonZeroUsize> = Lazy::new(|| {
    let available_cores = std::thread::available_parallelism().unwrap();
    std::env::var("GRPC_PARALLELISM").map_or(available_cores, |v| {
        let parallelism: NonZeroUsize = v.as_str().parse().unwrap();
        parallelism
    })
});

pub static URPC_PARALLELISM: Lazy<NonZeroUsize> = Lazy::new(|| {
    let available_cores = std::thread::available_parallelism().unwrap();
    std::env::var("URPC_PARALLELISM").map_or(available_cores, |v| {
        let parallelism: NonZeroUsize = v.as_str().parse().unwrap();
        parallelism
    })
});

#[async_trait]
pub trait RpcService {
    async fn start(
        &self,
        config: Config,
        runtime_manager: RuntimeManager,
        app_manager_ref: AppManagerRef,
    ) -> Result<()>;
}

pub struct DefaultRpcService;
impl DefaultRpcService {
    fn _start_urpc(
        config: &Config,
        runtime_manager: RuntimeManager,
        tx: Sender<()>,
        app_manager_ref: AppManagerRef,
    ) -> Result<()> {
        let urpc_port = config.urpc_port.unwrap();
        info!("Starting urpc server with port:[{}] ......", urpc_port);

        let core_ids = core_affinity::get_core_ids().unwrap();
        for (_, core_id) in core_ids.into_iter().enumerate() {
            let rx = tx.subscribe();
            async fn shutdown(mut rx: Receiver<()>) -> Result<()> {
                if let Err(err) = rx.recv().await {
                    error!("Errors on stopping the urpc service, err: {:?}.", err);
                } else {
                    debug!("urpc service has been graceful stopped.");
                }
                Ok(())
            }

            let app_manager = app_manager_ref.clone();
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), urpc_port as u16);

            if cfg!(feature = "urpc_uring") {
                let app_manager_ref = app_manager_ref.clone();
                let _ = monoio::spawn(async move {
                    urpc_serve(addr, shutdown(rx), app_manager_ref).await;
                });
            } else {
                std::thread::spawn(move || {
                    core_affinity::set_for_current(core_id);
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap()
                        .block_on(urpc_serve(addr, shutdown(rx), app_manager));
                });
            }
        }

        Ok(())
    }

    fn start_grpc(
        config: &Config,
        runtime_manager: RuntimeManager,
        tx: Sender<()>,
        app_manager_ref: AppManagerRef,
        server_state_manager: &ServerStateManager,
    ) -> Result<()> {
        let grpc_port = config.grpc_port;

        info!("Starting grpc server with port:[{}] ......", grpc_port);
        let parallelism = GRPC_PARALLELISM.get();
        info!("grpc service with parallelism: [{}]", &parallelism);

        let core_ids = core_affinity::get_core_ids().unwrap();
        for (_, core_id) in core_ids.into_iter().enumerate() {
            let shuffle_server =
                DefaultShuffleServer::from(app_manager_ref.clone(), server_state_manager);
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), grpc_port as u16);
            let service = ShuffleServerServer::new(shuffle_server.clone())
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX);
            let service_tx = tx.subscribe();

            let internal_service = ShuffleServerInternalServer::new(shuffle_server.clone());

            // every std::thread to bound the tokio thread to eliminate thread context switch.
            // this has been verified by benchmark of terasort 1TB that the p99 long tail latency
            // will be reduced from 2min -> 4sec.
            // And after binding the physical core with the grpc thread,
            // 1. the p99 transport time reduce from 4sec to 800ms.
            // 2. the p99 processing time reduce from 600ms to 60ms.
            std::thread::spawn(move || {
                core_affinity::set_for_current(core_id);

                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(grpc_serve(service, internal_service, addr, service_tx));
            });
        }

        Ok(())
    }

    // Only for tests.
    pub fn start_urpc(
        &self,
        config: &Config,
        runtime_manager: RuntimeManager,
        app_manager_ref: AppManagerRef,
        server_state_manager: &ServerStateManager,
    ) -> Result<Sender<()>> {
        let (tx, _) = broadcast::channel(1);
        let urpc_port = config.urpc_port.unwrap();
        DefaultRpcService::_start_urpc(
            config,
            runtime_manager.clone(),
            tx.clone(),
            app_manager_ref.clone(),
        )?;
        Ok(tx)
    }

    pub fn start(
        &self,
        config: &Config,
        runtime_manager: RuntimeManager,
        app_manager_ref: AppManagerRef,
        server_state_manager: &ServerStateManager,
    ) -> Result<()> {
        let (tx, _) = broadcast::channel(1);

        let grpc_port = config.grpc_port;
        if is_port_in_used(grpc_port as u16) {
            panic!("The grpc port of {:?} has been used.", grpc_port);
        }

        DefaultRpcService::start_grpc(
            config,
            runtime_manager.clone(),
            tx.clone(),
            app_manager_ref.clone(),
            server_state_manager,
        )?;

        let urpc_port = config.urpc_port;
        if urpc_port.is_some() {
            if is_port_in_used(urpc_port.unwrap() as u16) {
                panic!("The urpc port of {:?} has been used.", urpc_port.unwrap());
            }

            DefaultRpcService::_start_urpc(
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

async fn urpc_serve(addr: SocketAddr, shutdown: impl Future, app_manager_ref: AppManagerRef) {
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

    if cfg!(feature = "urpc_uring") {
        let listner: monoio::net::TcpListener = monoio::net::TcpListener::bind(addr).unwrap();
        let _ = urpc_uring::server::run(listner, shutdown, app_manager_ref).await;
    } else {
        let listener = TcpListener::from_std(sock.into()).unwrap();
        let _ = urpc::server::run(listener, shutdown, app_manager_ref).await;
    }
}

async fn grpc_serve(
    main_service: ShuffleServerServer<DefaultShuffleServer>,
    internal_service: ShuffleServerInternalServer<DefaultShuffleServer>,
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
        .add_service(main_service)
        .add_service(internal_service)
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
