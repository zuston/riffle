//! Throughput microbench comparing the default tokio based urpc network
//! stack against the pluggable io_uring net engine.
//!
//! Both servers speak the same wire protocol and apply the same echo
//! semantics: parse an inbound `SendShuffleData` frame and answer with an
//! empty `RpcResponse`. The only difference is the network engine.
//!
//! Run with:
//!   cargo bench --bench urpc_net_engine --features io-uring
//!
//! Env knobs: BENCH_DURATION_SECS, BENCH_SERVER_THREADS, BENCH_CONNS,
//! BENCH_PIPELINE.

use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use riffle_server::urpc::command::RpcResponseCommand;
use riffle_server::urpc::connection::Connection;
use riffle_server::urpc::frame::{Frame, MessageType};
use riffle_server::urpc::uring::{Responder, UringServerConfig, UringUrpcServer};
use std::io::Write;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// header(9) + request_id(8) + status(4) + empty msg len(4)
const RPC_RESPONSE_WIRE_LEN: usize = 25;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn put_string(buf: &mut BytesMut, value: &str) {
    buf.put_i32(value.len() as i32);
    buf.put_slice(value.as_bytes());
}

fn build_send_shuffle_data_frame(request_id: i64, payload_len: usize) -> Bytes {
    let payload = vec![(request_id % 251) as u8; payload_len];
    let mut body = BytesMut::new();
    body.put_i64(request_id);
    put_string(&mut body, "app-bench");
    body.put_i32(7);
    body.put_i64(99);
    body.put_i32(1); // partition batch size
    body.put_i32(11); // partition id
    body.put_i32(1); // block batch size
    body.put_i32(11); // pid
    body.put_i64(1234); // block id
    body.put_i32(payload.len() as i32); // length
    body.put_i32(7); // shuffle id
    body.put_i64(88); // crc
    body.put_i64(9001); // task attempt id
    body.put_i32(payload.len() as i32);
    body.put_slice(&payload);
    body.put_i32(0); // shuffle servers
    body.put_i32(payload.len() as i32); // uncompress length
    body.put_i64(0); // free mem
    body.put_i64(123456); // timestamp

    let mut frame = BytesMut::with_capacity(9 + body.len());
    frame.put_i32(0);
    frame.put_u8(MessageType::SendShuffleData as u8);
    frame.put_i32(body.len() as i32);
    frame.extend_from_slice(&body);
    frame.freeze()
}

fn echo_response(frame: Frame) -> Frame {
    match frame {
        Frame::SendShuffleData(req) => {
            Frame::RpcResponse(RpcResponseCommand::new(req.request_id(), 0, String::new()))
        }
        other => panic!("unexpected frame in bench: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Default engine: the existing tokio network stack (Connection + Frame).
// Mirrors `rpc.rs`: per-core std threads, each with a current_thread runtime
// and a SO_REUSEPORT listener.
// ---------------------------------------------------------------------------

struct TokioEchoServer {
    addr: SocketAddr,
    stops: Vec<tokio::sync::oneshot::Sender<()>>,
    joins: Vec<std::thread::JoinHandle<()>>,
}

impl TokioEchoServer {
    fn start(threads: usize) -> Result<Self> {
        let first = build_reuseport_listener("127.0.0.1:0".parse()?)?;
        let addr = first.local_addr()?;

        let mut listeners = vec![first];
        for _ in 1..threads {
            listeners.push(build_reuseport_listener(addr)?);
        }

        let mut stops = Vec::new();
        let mut joins = Vec::new();
        for (i, listener) in listeners.into_iter().enumerate() {
            let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
            stops.push(stop_tx);
            let join = std::thread::Builder::new()
                .name(format!("bench-tokio-{i}"))
                .spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap();
                    rt.block_on(async move {
                        listener.set_nonblocking(true).unwrap();
                        let listener = TcpListener::from_std(listener).unwrap();
                        loop {
                            let socket = tokio::select! {
                                accepted = listener.accept() => match accepted {
                                    Ok((socket, _)) => socket,
                                    Err(_) => break,
                                },
                                _ = &mut stop_rx => break,
                            };
                            let sock_ref = socket2::SockRef::from(&socket);
                            let _ = sock_ref.set_keepalive(true);
                            let _ = sock_ref.set_nodelay(true);
                            tokio::spawn(async move {
                                // The default urpc server network path:
                                // streaming parse enabled + vectored writes.
                                let mut conn = Connection::new(socket, true);
                                loop {
                                    match conn.read_frame().await {
                                        Ok(Some(frame)) => {
                                            let resp = echo_response(frame);
                                            if conn.write_frame(&resp).await.is_err() {
                                                break;
                                            }
                                        }
                                        _ => break,
                                    }
                                }
                            });
                        }
                    });
                })?;
            joins.push(join);
        }
        Ok(Self { addr, stops, joins })
    }

    fn shutdown(self) {
        for stop in self.stops {
            let _ = stop.send(());
        }
        for join in self.joins {
            let _ = join.join();
        }
    }
}

fn build_reuseport_listener(addr: SocketAddr) -> Result<std::net::TcpListener> {
    let sock = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None)?;
    sock.set_reuse_address(true)?;
    sock.set_reuse_port(true)?;
    sock.bind(&addr.into())?;
    sock.listen(8192)?;
    Ok(sock.into())
}

// ---------------------------------------------------------------------------
// Load generation
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct Case {
    name: &'static str,
    payload_len: usize,
    pipeline: usize,
}

struct CaseResult {
    rps: f64,
    mbps: f64,
}

fn run_case(addr: SocketAddr, case: Case, conns: usize, duration: Duration) -> Result<CaseResult> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(conns.min(8))
        .enable_all()
        .build()?;

    // One pipelined batch, pre-serialized once.
    let mut batch = BytesMut::new();
    for id in 0..case.pipeline {
        batch.extend_from_slice(&build_send_shuffle_data_frame(id as i64, case.payload_len));
    }
    let batch = batch.freeze();

    let total_responses = Arc::new(AtomicU64::new(0));
    let result = rt.block_on(async {
        let warmup = Duration::from_millis(500);
        let deadline = Instant::now() + warmup + duration;
        let measure_start = Instant::now() + warmup;

        let mut tasks = Vec::new();
        for _ in 0..conns {
            let batch = batch.clone();
            let total_responses = total_responses.clone();
            tasks.push(tokio::spawn(async move {
                let mut stream = TcpStream::connect(addr).await?;
                stream.set_nodelay(true)?;
                let mut resp_buf = vec![0u8; RPC_RESPONSE_WIRE_LEN * case.pipeline];
                let pipeline = case.pipeline as u64;
                loop {
                    let now = Instant::now();
                    if now >= deadline {
                        break;
                    }
                    stream.write_all(&batch).await?;
                    stream.read_exact(&mut resp_buf).await?;
                    // Sanity check the first response header.
                    assert_eq!(MessageType::RpcResponse as u8, resp_buf[4]);
                    if now >= measure_start {
                        total_responses.fetch_add(pipeline, Ordering::Relaxed);
                    }
                }
                anyhow::Ok(())
            }));
        }
        for task in tasks {
            task.await??;
        }
        anyhow::Ok(())
    });
    result?;

    let total = total_responses.load(Ordering::Relaxed) as f64;
    let secs = duration.as_secs_f64();
    let rps = total / secs;
    let mbps = rps * case.payload_len as f64 / 1024.0 / 1024.0;
    Ok(CaseResult { rps, mbps })
}

fn main() -> Result<()> {
    let duration = Duration::from_secs(env_usize("BENCH_DURATION_SECS", 3) as u64);
    let server_threads = env_usize("BENCH_SERVER_THREADS", 2);
    let conns = env_usize("BENCH_CONNS", 8);

    let cases = [
        Case {
            name: "block=1KB",
            payload_len: 1024,
            pipeline: env_usize("BENCH_PIPELINE", 64),
        },
        Case {
            name: "block=64KB",
            payload_len: 64 * 1024,
            pipeline: 16,
        },
        Case {
            name: "block=512KB",
            payload_len: 512 * 1024,
            pipeline: 4,
        },
    ];

    println!(
        "urpc net engine bench: server_threads={}, conns={}, duration={:?}",
        server_threads, conns, duration
    );
    println!(
        "{:<14} {:>14} {:>12} {:>14} {:>12} {:>9}",
        "case", "tokio rps", "tokio MB/s", "uring rps", "uring MB/s", "speedup"
    );

    let mut all_pass = true;
    for case in cases {
        let tokio_server = TokioEchoServer::start(server_threads)?;
        let tokio_result = run_case(tokio_server.addr, case, conns, duration)?;
        tokio_server.shutdown();

        let uring_server = UringUrpcServer::start(
            "127.0.0.1:0".parse()?,
            server_threads,
            UringServerConfig::default(),
            |_| {
                |frame: Frame, responder: &mut Responder<'_>| {
                    let resp = echo_response(frame);
                    responder.respond(&resp).unwrap();
                }
            },
        )?;
        let uring_result = run_case(uring_server.local_addr(), case, conns, duration)?;
        uring_server.shutdown();

        let speedup = uring_result.rps / tokio_result.rps;
        if speedup <= 1.0 {
            all_pass = false;
        }
        println!(
            "{:<14} {:>14.0} {:>12.1} {:>14.0} {:>12.1} {:>8.2}x",
            case.name,
            tokio_result.rps,
            tokio_result.mbps,
            uring_result.rps,
            uring_result.mbps,
            speedup
        );
        std::io::stdout().flush()?;
    }

    println!(
        "\nresult: uring engine {} the default tokio engine on all cases",
        if all_pass { "BEATS" } else { "DOES NOT BEAT" }
    );
    Ok(())
}
