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
use crate::server_state_manager::ServerStateManager;
use crate::urpc;
use crate::util::is_port_in_used;
use anyhow::{Context, Result};
use futures::{stream::FuturesUnordered, StreamExt};
use log::{debug, info};
use once_cell::sync::Lazy;
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroUsize;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::broadcast::Receiver;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

pub(crate) const TCP_KEEPALIVE_IDLE: Duration = Duration::from_secs(5 * 60);

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

type RpcCompletion = oneshot::Receiver<Result<()>>;

pub struct RpcServiceHandle {
    completions: FuturesUnordered<RpcCompletion>,
}

impl RpcServiceHandle {
    pub async fn wait(&mut self) -> Result<()> {
        while let Some(result) = self.completions.next().await {
            result.context("RPC thread exited unexpectedly")??;
        }
        Ok(())
    }
}

pub struct DefaultRpcService;
impl DefaultRpcService {
    fn _start_urpc(
        config: &Config,
        shutdown_rx: Receiver<()>,
        app_manager_ref: AppManagerRef,
    ) -> Result<Vec<RpcCompletion>> {
        let urpc_port = config.urpc_port.unwrap();

        let net_engine = config
            .urpc_config
            .as_ref()
            .map(|c| c.net_engine)
            .unwrap_or_default();
        if matches!(net_engine, crate::config::UrpcNetEngine::URING) {
            #[cfg(all(feature = "io-uring", target_os = "linux"))]
            {
                return Self::_start_urpc_uring(config, shutdown_rx, app_manager_ref);
            }
            #[cfg(not(all(feature = "io-uring", target_os = "linux")))]
            log::warn!(
                "urpc net_engine=URING requires the io-uring feature on linux, \
                 falling back to the TOKIO engine."
            );
        }

        info!("Starting urpc server with port:[{}] ......", urpc_port);

        let mut completions = Vec::new();
        #[cfg(target_arch = "aarch64")]
        {
            let rx = shutdown_rx.resubscribe();
            let app_manager = app_manager_ref.clone();
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), urpc_port);
            let listener = bind_rpc_listener(addr)?;
            completions.push(spawn_rpc_thread(
                None,
                URPC_PARALLELISM.get(),
                async move { urpc_serve(listener, rx, app_manager).await },
            )?);
        }

        #[cfg(not(target_arch = "aarch64"))]
        {
            let core_ids = core_affinity::get_core_ids()
                .filter(|cores| !cores.is_empty())
                .context("no logical CPU available for RPC threads")?;
            for (_, core_id) in core_ids.into_iter().enumerate() {
                let rx = shutdown_rx.resubscribe();

                let app_manager = app_manager_ref.clone();
                let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), urpc_port);
                let listener = bind_rpc_listener(addr)?;
                completions.push(spawn_rpc_thread(Some(core_id), 1, async move {
                    urpc_serve(listener, rx, app_manager).await
                })?);
            }
        }

        Ok(completions)
    }

    /// Serves urpc with the pluggable io_uring net engine: per-core engine
    /// threads own all socket I/O, while command processing runs on a
    /// single-worker tokio runtime pinned to the same logical CPU.
    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    fn _start_urpc_uring(
        config: &Config,
        mut shutdown_rx: Receiver<()>,
        app_manager_ref: AppManagerRef,
    ) -> Result<Vec<RpcCompletion>> {
        use crate::urpc::uring::bridge::create_handler_runtime;
        use crate::urpc::uring::{AppCommandBridgeHandler, UringServerConfig, UringUrpcServer};
        use anyhow::anyhow;

        let urpc_port = config.urpc_port.unwrap();
        let threads = URPC_PARALLELISM.get();
        info!(
            "Starting urpc server with the io_uring net engine. port:[{}], threads:[{}] ......",
            urpc_port, threads
        );

        let bound_cores = core_affinity::get_core_ids()
            .ok_or_else(|| anyhow!("failed to discover logical CPUs for the urpc uring server"))?;
        if bound_cores.is_empty() {
            return Err(anyhow!(
                "no logical CPU is available for the urpc uring server"
            ));
        }
        let bound_cores: Vec<_> = bound_cores.into_iter().take(threads).collect();
        let handler_runtimes = bound_cores
            .iter()
            .map(|core| create_handler_runtime(*core))
            .collect::<Result<Vec<_>>>()?;

        let mut engine_config = UringServerConfig::default();
        engine_config.bind_cores = Some(bound_cores.iter().map(|core| core.id as u32).collect());

        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), urpc_port);
        let app_manager = app_manager_ref.clone();
        let server = UringUrpcServer::start(addr, threads, engine_config, move |engine_index| {
            let runtime = handler_runtimes[engine_index % handler_runtimes.len()].clone();
            AppCommandBridgeHandler::new(app_manager.clone(), runtime)
        })?;

        let (completed_tx, completed_rx) = oneshot::channel();
        std::thread::Builder::new()
            .name("urpc-uring-shutdown".to_string())
            .spawn(move || {
                let _ = shutdown_rx.blocking_recv();
                info!("Shutting down the urpc io_uring net engine...");
                server.shutdown();
                let _ = completed_tx.send(Ok(()));
            })?;

        Ok(vec![completed_rx])
    }

    fn start_grpc(
        config: &Config,
        shutdown_rx: Receiver<()>,
        app_manager_ref: AppManagerRef,
        server_state_manager: &ServerStateManager,
    ) -> Result<Vec<RpcCompletion>> {
        let grpc_port = config.grpc_port;

        info!("Starting grpc server with port:[{}] ......", grpc_port);
        let parallelism = GRPC_PARALLELISM.get();
        info!("grpc service with parallelism: [{}]", &parallelism);
        let mut completions = Vec::new();

        #[cfg(not(target_arch = "aarch64"))]
        {
            let core_ids = core_affinity::get_core_ids()
                .filter(|cores| !cores.is_empty())
                .context("no logical CPU available for RPC threads")?;
            for (_, core_id) in core_ids.into_iter().enumerate() {
                let shuffle_server =
                    DefaultShuffleServer::from(app_manager_ref.clone(), server_state_manager);
                let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), grpc_port);
                let service = ShuffleServerServer::new(shuffle_server.clone())
                    .max_decoding_message_size(usize::MAX)
                    .max_encoding_message_size(usize::MAX);
                let service_rx = shutdown_rx.resubscribe();

                let internal_service = ShuffleServerInternalServer::new(shuffle_server.clone());

                // every std::thread to bound the tokio thread to eliminate thread context switch.
                // this has been verified by benchmark of terasort 1TB that the p99 long tail latency
                // will be reduced from 2min -> 4sec.
                // And after binding the physical core with the grpc thread,
                // 1. the p99 transport time reduce from 4sec to 800ms.
                // 2. the p99 processing time reduce from 600ms to 60ms.
                let listener = bind_rpc_listener(addr)?;
                completions.push(spawn_rpc_thread(Some(core_id), 1, async move {
                    grpc_serve(service, internal_service, listener, service_rx).await
                })?);
            }
        }

        #[cfg(target_arch = "aarch64")]
        {
            let shuffle_server =
                DefaultShuffleServer::from(app_manager_ref.clone(), server_state_manager);
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), grpc_port);
            let service = ShuffleServerServer::new(shuffle_server.clone())
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX);
            let service_rx = shutdown_rx.resubscribe();

            let internal_service = ShuffleServerInternalServer::new(shuffle_server.clone());

            let listener = bind_rpc_listener(addr)?;
            completions.push(spawn_rpc_thread(
                None,
                GRPC_PARALLELISM.get(),
                async move { grpc_serve(service, internal_service, listener, service_rx).await },
            )?);
        }

        Ok(completions)
    }

    // Only for tests.
    pub fn start_urpc(
        &self,
        config: &Config,
        app_manager_ref: AppManagerRef,
        shutdown_rx: Receiver<()>,
    ) -> Result<()> {
        Self::_start_urpc(config, shutdown_rx, app_manager_ref)?;
        Ok(())
    }

    pub fn start(
        &self,
        config: &Config,
        app_manager_ref: AppManagerRef,
        server_state_manager: &ServerStateManager,
        shutdown_rx: Receiver<()>,
    ) -> Result<RpcServiceHandle> {
        for port in [Some(config.grpc_port), config.urpc_port]
            .into_iter()
            .flatten()
        {
            anyhow::ensure!(!is_port_in_used(port), "RPC port {port} is already in use");
        }
        let mut completions = Self::start_grpc(
            config,
            shutdown_rx.resubscribe(),
            app_manager_ref.clone(),
            server_state_manager,
        )?;
        if config.urpc_port.is_some() {
            completions.extend(Self::_start_urpc(config, shutdown_rx, app_manager_ref)?);
        }
        Ok(RpcServiceHandle {
            completions: completions.into_iter().collect(),
        })
    }
}

fn spawn_rpc_thread(
    core: Option<core_affinity::CoreId>,
    parallelism: usize,
    serve: impl Future<Output = Result<()>> + Send + 'static,
) -> Result<RpcCompletion> {
    let (tx, rx) = oneshot::channel();
    std::thread::Builder::new()
        .name("riffle-rpc".to_string())
        .spawn(move || {
            let result = (|| {
                let mut builder = if let Some(core) = core {
                    core_affinity::set_for_current(core);
                    tokio::runtime::Builder::new_current_thread()
                } else {
                    let mut builder = tokio::runtime::Builder::new_multi_thread();
                    builder.worker_threads(std::cmp::max(parallelism, 16));
                    builder
                };
                let runtime = builder.enable_all().build()?;
                runtime.block_on(serve)
            })();
            let _ = tx.send(result);
        })?;
    Ok(rx)
}

fn bind_rpc_listener(addr: SocketAddr) -> Result<std::net::TcpListener> {
    let sock = socket2::Socket::new(
        match addr {
            SocketAddr::V4(_) => socket2::Domain::IPV4,
            SocketAddr::V6(_) => socket2::Domain::IPV6,
        },
        socket2::Type::STREAM,
        None,
    )?;
    sock.set_reuse_address(true)?;
    #[cfg(not(target_arch = "aarch64"))]
    sock.set_reuse_port(true)?;
    sock.set_nonblocking(true)?;
    sock.bind(&addr.into())?;
    sock.listen(8192)?;
    Ok(sock.into())
}

async fn urpc_serve(
    listener: std::net::TcpListener,
    shutdown_rx: Receiver<()>,
    app_manager_ref: AppManagerRef,
) -> Result<()> {
    urpc::server::run(
        TcpListener::from_std(listener)?,
        shutdown_rx,
        app_manager_ref,
    )
    .await;
    debug!("urpc service has been gracefully stopped.");
    Ok(())
}

async fn grpc_serve(
    main_service: ShuffleServerServer<DefaultShuffleServer>,
    internal_service: ShuffleServerInternalServer<DefaultShuffleServer>,
    listener: std::net::TcpListener,
    mut rx: Receiver<()>,
) -> Result<()> {
    let incoming = TcpListenerStream::new(TcpListener::from_std(listener)?);

    Server::builder()
        .initial_connection_window_size(MAX_CONNECTION_WINDOW_SIZE)
        .initial_stream_window_size(STREAM_WINDOW_SIZE)
        .tcp_keepalive(Some(TCP_KEEPALIVE_IDLE))
        .tcp_nodelay(true)
        .layer(TracingMiddleWareLayer::new())
        .layer(MetricsMiddlewareLayer::new(GRPC_LATENCY_TIME_SEC.clone()))
        .layer(AwaitTreeMiddlewareLayer::new_optional(Some(
            AWAIT_TREE_REGISTRY.clone(),
        )))
        .add_service(main_service)
        .add_service(internal_service)
        .serve_with_incoming_shutdown(incoming, async {
            let _ = rx.recv().await;
        })
        .await?;
    info!("GRPC service has been gracefully stopped.");
    Ok(())
}
