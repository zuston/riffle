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
//! BENCH_PIPELINE (overrides the 1 KiB case only).

use anyhow::{ensure, Result};
use bytes::{BufMut, Bytes, BytesMut};
use riffle_server::urpc::command::RpcResponseCommand;
use riffle_server::urpc::connection::Connection;
use riffle_server::urpc::frame::{Frame, MessageType};
use riffle_server::urpc::uring::encode::parse_request_frame;
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
    buf.put_i32(i32::try_from(value.len()).expect("string length must fit in i32"));
    buf.put_slice(value.as_bytes());
}

fn build_send_shuffle_data_frame(request_id: i64, case: Case) -> Bytes {
    let partition_count =
        i32::try_from(case.partition_count).expect("partition count must fit in i32");
    let blocks_per_partition =
        i32::try_from(case.blocks_per_partition).expect("block count must fit in i32");
    let block_len = i32::try_from(case.block_len).expect("block length must fit in i32");
    let payload = vec![(request_id % 251) as u8; case.block_len];
    let mut body = BytesMut::with_capacity(case.request_payload_len());
    body.put_i64(request_id);
    put_string(&mut body, "app-bench");
    body.put_i32(7);
    body.put_i64(99);
    body.put_i32(partition_count);

    let blocks_per_request =
        i64::try_from(case.blocks_per_request()).expect("block count must fit in i64");
    for partition_index in 0..case.partition_count {
        let partition_id = i32::try_from(partition_index).expect("partition id must fit in i32");
        body.put_i32(partition_id);
        body.put_i32(blocks_per_partition);

        for block_index in 0..case.blocks_per_partition {
            let block_offset = partition_index
                .checked_mul(case.blocks_per_partition)
                .and_then(|offset| offset.checked_add(block_index))
                .expect("block offset must fit in usize");
            let block_offset = i64::try_from(block_offset).expect("block offset must fit in i64");
            let block_id = request_id
                .checked_mul(blocks_per_request)
                .and_then(|base| base.checked_add(block_offset))
                .expect("block id must fit in i64");

            body.put_i32(partition_id); // pid
            body.put_i64(block_id);
            body.put_i32(block_len);
            body.put_i32(7); // shuffle id
            body.put_i64(88); // crc
            body.put_i64(9001); // task attempt id
            body.put_i32(block_len);
            body.put_slice(&payload);
            body.put_i32(0); // shuffle servers
            body.put_i32(block_len); // uncompress length
            body.put_i64(0); // free mem
        }
    }
    body.put_i64(123456); // timestamp

    let mut frame = BytesMut::with_capacity(9 + body.len());
    frame.put_i32(0);
    frame.put_u8(MessageType::SendShuffleData as u8);
    frame.put_i32(i32::try_from(body.len()).expect("request body length must fit in i32"));
    frame.extend_from_slice(&body);
    frame.freeze()
}

fn validate_case_frame(case: Case) -> Result<()> {
    ensure!(case.block_len > 0, "case {} has an empty block", case.name);
    ensure!(
        case.partition_count > 0,
        "case {} has no partitions",
        case.name
    );
    ensure!(
        case.blocks_per_partition > 0,
        "case {} has no blocks",
        case.name
    );
    ensure!(
        case.pipeline > 0,
        "case {} has an empty pipeline",
        case.name
    );

    let frame = parse_request_frame(build_send_shuffle_data_frame(0, case))?;
    let Frame::SendShuffleData(request) = frame else {
        anyhow::bail!("case {} did not produce SendShuffleData", case.name);
    };
    ensure!(
        request.data_len() == case.request_payload_len(),
        "case {} payload mismatch: expected {}, parsed {}",
        case.name,
        case.request_payload_len(),
        request.data_len()
    );
    Ok(())
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
    block_len: usize,
    partition_count: usize,
    blocks_per_partition: usize,
    pipeline: usize,
}

impl Case {
    fn blocks_per_request(self) -> usize {
        self.partition_count
            .checked_mul(self.blocks_per_partition)
            .expect("blocks per request must fit in usize")
    }

    fn request_payload_len(self) -> usize {
        self.blocks_per_request()
            .checked_mul(self.block_len)
            .expect("request payload length must fit in usize")
    }
}

struct CaseResult {
    rps: f64,
    mib_per_sec: f64,
}

fn run_case(addr: SocketAddr, case: Case, conns: usize, duration: Duration) -> Result<CaseResult> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(conns.min(8))
        .enable_all()
        .build()?;

    // One pipelined batch, pre-serialized once.
    let mut batch = BytesMut::new();
    for id in 0..case.pipeline {
        let request_id = i64::try_from(id).expect("request id must fit in i64");
        batch.extend_from_slice(&build_send_shuffle_data_frame(request_id, case));
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
                let pipeline = u64::try_from(case.pipeline).expect("pipeline must fit in u64");
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
    let mib_per_sec = rps * case.request_payload_len() as f64 / 1024.0 / 1024.0;
    Ok(CaseResult { rps, mib_per_sec })
}

fn main() -> Result<()> {
    let duration_secs = env_usize("BENCH_DURATION_SECS", 3);
    let server_threads = env_usize("BENCH_SERVER_THREADS", 2);
    let conns = env_usize("BENCH_CONNS", 8);
    ensure!(duration_secs > 0, "BENCH_DURATION_SECS must be positive");
    ensure!(server_threads > 0, "BENCH_SERVER_THREADS must be positive");
    ensure!(conns > 0, "BENCH_CONNS must be positive");
    let duration = Duration::from_secs(duration_secs as u64);

    let cases = [
        Case {
            name: "block=1KB",
            block_len: 1024,
            partition_count: 1,
            blocks_per_partition: 1,
            pipeline: env_usize("BENCH_PIPELINE", 64),
        },
        Case {
            name: "block=64KB",
            block_len: 64 * 1024,
            partition_count: 1,
            blocks_per_partition: 1,
            pipeline: 16,
        },
        Case {
            name: "block=512KB",
            block_len: 512 * 1024,
            partition_count: 1,
            blocks_per_partition: 1,
            pipeline: 4,
        },
        // Production shape: one request carries 500 blocks across 500 partitions.
        Case {
            name: "prod=500x10KB",
            block_len: 10 * 1024,
            partition_count: 500,
            blocks_per_partition: 1,
            pipeline: 1,
        },
    ];

    println!(
        "urpc net engine bench: server_threads={}, conns={}, duration={:?}",
        server_threads, conns, duration
    );
    println!(
        "{:<14} {:>14} {:>12} {:>14} {:>12} {:>9}",
        "case", "tokio rps", "tokio MiB/s", "uring rps", "uring MiB/s", "speedup"
    );

    let mut uring_wins = 0;
    for case in cases {
        validate_case_frame(case)?;

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
        if speedup > 1.0 {
            uring_wins += 1;
        }
        println!(
            "{:<14} {:>14.0} {:>12.1} {:>14.0} {:>12.1} {:>8.2}x",
            case.name,
            tokio_result.rps,
            tokio_result.mib_per_sec,
            uring_result.rps,
            uring_result.mib_per_sec,
            speedup
        );
        std::io::stdout().flush()?;
    }

    println!(
        "\nresult: uring engine beats the default tokio engine on {}/{} cases",
        uring_wins,
        cases.len()
    );
    Ok(())
}
