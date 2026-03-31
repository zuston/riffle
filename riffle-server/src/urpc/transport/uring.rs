// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! uRPC transport: **io_uring for `accept(2)` only**; connected sockets use Tokio `TcpStream` for
//! read/write (same I/O model as the epoll transport). This avoids sharing one io_uring worker
//! between listen-accept and per-connection I/O, which was fragile in practice.

use super::{AsRawFd as UrpcAsRawFd, Transport, TransportListener, TransportStream};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::BytesMut;
use core_affinity::CoreId;
use io_uring::{opcode, types::Fd, IoUring};
use libc::{sockaddr_storage, socklen_t};
use once_cell::sync::OnceCell;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::net::{SocketAddr, TcpListener};
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::sync::{mpsc, Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream as TokioTcpStream;
use tokio::sync::oneshot;

/// Global io-uring engine for network I/O
pub struct UringNetEngine {
    submission_tx: mpsc::SyncSender<UringOp>,
}

impl UringNetEngine {
    /// One dedicated thread owns a single `IoUring`, matching
    /// [tcp_simple](https://github.com/espoal/uring_examples/blob/main/examples/tcp_simple/src/main.rs):
    /// `submit_and_wait(1)` then read one CQE. Each engine is a separate ring + worker; optional
    /// `core` pins that worker to a CPU when affinity is available.
    pub fn new(engine_index: usize, core: Option<CoreId>) -> Result<Self> {
        let (submission_tx, submission_rx) = mpsc::sync_channel::<UringOp>(4096);

        std::thread::Builder::new()
            .name(format!("riffle-uring-net-{engine_index}"))
            .spawn(move || {
                if let Some(c) = core {
                    core_affinity::set_for_current(c);
                }
                uring_worker_loop(submission_rx);
            })?;

        Ok(Self { submission_tx })
    }

    /// Submit an operation to the io-uring engine
    fn submit(&self, op: UringOp) -> Result<()> {
        self.submission_tx
            .send(op)
            .map_err(|_| anyhow!("io-uring engine stopped"))?;
        Ok(())
    }
}

impl Clone for UringNetEngine {
    fn clone(&self) -> Self {
        Self {
            submission_tx: self.submission_tx.clone(),
        }
    }
}

/// Process-wide per-core engine registry.
static URING_ENGINE_REGISTRY: OnceCell<Arc<UringEngineRegistry>> = OnceCell::new();

thread_local! {
    static URING_ENGINE_INDEX: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

struct UringEngineRegistry {
    engines: Vec<Arc<UringNetEngine>>,
}

impl UringEngineRegistry {
    /// Spawns `engine_count` rings (one OS thread + `IoUring` each). When `core_ids` is non-empty,
    /// worker `i` is pinned to `core_ids[i]` (one logical core per engine for `i < core_ids.len()`).
    fn new(engine_count: usize, core_ids: Vec<CoreId>) -> Result<Self> {
        let n = std::cmp::max(engine_count, 1);
        let mut engines = Vec::with_capacity(n);
        for i in 0..n {
            let core = core_ids.get(i).copied();
            engines.push(Arc::new(UringNetEngine::new(i, core)?));
        }
        Ok(Self { engines })
    }

    fn get_engine_for_current_thread(&self) -> Arc<UringNetEngine> {
        let idx = if let Some(engine_idx) = URING_ENGINE_INDEX.with(|cell| cell.get()) {
            engine_idx % self.engines.len()
        } else {
            // Fallback when thread affinity is not set.
            let mut hasher = DefaultHasher::new();
            std::thread::current().id().hash(&mut hasher);
            (hasher.finish() as usize) % self.engines.len()
        };
        self.engines[idx].clone()
    }
}

/// Logical CPU count for sizing the engine pool: affinity list when present, else
/// [`std::thread::available_parallelism`].
fn logical_cpu_count() -> usize {
    match core_affinity::get_core_ids() {
        Some(ids) if !ids.is_empty() => ids.len(),
        _ => std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1),
    }
}

/// `threads`: `0` = one engine per logical CPU (see [`logical_cpu_count`]); `> 0` caps the number
/// of engines at `threads` (still at least one).
pub fn init_uring_engine(threads: usize) -> Result<()> {
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();
    let logical = logical_cpu_count().max(1);
    let engine_count = if threads == 0 {
        logical
    } else {
        logical.min(threads.max(1)).max(1)
    };
    let _ = URING_ENGINE_REGISTRY.get_or_try_init(|| {
        let registry = UringEngineRegistry::new(engine_count, core_ids)?;
        Ok::<Arc<UringEngineRegistry>, anyhow::Error>(Arc::new(registry))
    })?;
    Ok(())
}

pub(crate) fn set_current_engine_index(idx: usize) {
    URING_ENGINE_INDEX.with(|cell| cell.set(Some(idx)));
}

/// Get the global io-uring engine
pub(crate) fn get_engine() -> Result<Arc<UringNetEngine>> {
    URING_ENGINE_REGISTRY
        .get()
        .map(|registry| registry.get_engine_for_current_thread())
        .ok_or_else(|| anyhow!("io-uring engine not initialized"))
}

enum UringOpType {
    /// Non-blocking listen socket: idle accept completes with `-EAGAIN` and is re-submitted in the
    /// worker. Real peer addresses are taken from the kernel via `sockaddr_storage` (not null).
    Accept {
        fd: RawFd,
        addr: Box<sockaddr_storage>,
        addrlen: Box<socklen_t>,
    },
}

unsafe impl Send for UringOpType {}

/// Kernel returns negative errno in io_uring CQE `result` for many ops.
#[inline]
fn cqe_result_is_would_block(res: i32) -> bool {
    if res >= 0 {
        return false;
    }
    let e = -res;
    e == libc::EAGAIN as i32 || e == libc::EWOULDBLOCK as i32
}

fn set_socket_nonblocking(fd: RawFd) -> Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL, 0) };
    if flags < 0 {
        return Err(anyhow!("fcntl(F_GETFL) failed on fd {}", fd));
    }
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(anyhow!("fcntl(F_SETFL, O_NONBLOCK) failed on fd {}", fd));
    }
    Ok(())
}

struct UringOp {
    op_type: UringOpType,
    tx: Option<oneshot::Sender<Result<i32>>>,
}

fn push_sqe(uring: &mut IoUring, op_type: &UringOpType, user_data: u64) {
    let sqe = build_sqe(op_type).user_data(user_data);
    let mut sq = uring.submission();
    unsafe {
        sq.push(&sqe).expect("submission queue is full");
    }
}

fn build_sqe(op_type: &UringOpType) -> io_uring::squeue::Entry {
    match op_type {
        UringOpType::Accept { fd, addr, addrlen } => opcode::Accept::new(
            Fd(*fd),
            addr.as_ref() as *const _ as *mut _,
            addrlen.as_ref() as *const _ as *mut _,
        )
        .flags(libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC)
        .build(),
    }
}

/// Multiplexed `Accept` loop: many SQEs may be in flight so one listener spinning on `-EAGAIN`
/// does not block other accepts on the same ring.
fn uring_worker_loop(rx: mpsc::Receiver<UringOp>) {
    let mut uring = IoUring::new(1024).expect("Failed to create io-uring");
    let mut pending: HashMap<u64, UringOp> = HashMap::new();
    let mut next_id: u64 = 1;

    loop {
        while let Ok(op) = rx.try_recv() {
            let id = next_id;
            next_id += 1;
            push_sqe(&mut uring, &op.op_type, id);
            pending.insert(id, op);
        }

        if pending.is_empty() {
            let op = match rx.recv() {
                Ok(o) => o,
                Err(_) => break,
            };
            let id = next_id;
            next_id += 1;
            push_sqe(&mut uring, &op.op_type, id);
            pending.insert(id, op);
        }

        match uring.submit_and_wait(1) {
            Err(e) if e.raw_os_error() == Some(libc::EINTR) => continue,
            Err(e) => panic!("io_uring submit_and_wait failed: {e}"),
            Ok(_) => {}
        }

        let cqes: Vec<_> = uring.completion().collect();
        for cqe in cqes {
            let id = cqe.user_data();
            let r = cqe.result();

            let Some(op) = pending.get(&id) else {
                continue;
            };

            if cqe_result_is_would_block(r) {
                push_sqe(&mut uring, &op.op_type, id);
                continue;
            }

            let mut op = pending.remove(&id).expect("pending op");
            if let Some(tx) = op.tx.take() {
                let _ = tx.send(Ok(r));
            }
        }
    }
}

/// io-uring based transport
pub struct UringTransport;

#[async_trait]
impl Transport for UringTransport {
    type Listener = UringListener;
    type Stream = UringStream;

    async fn bind(addr: SocketAddr) -> Result<Self::Listener> {
        // Use TcpListener directly (same as epoll transport). The non-blocking mode is
        // set after binding so the fd stays valid while stored in UringListener.
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        let fd = listener.as_raw_fd();

        Ok(UringListener { fd, listener })
    }

    async fn connect(addr: SocketAddr) -> Result<Self::Stream> {
        let stream = TokioTcpStream::connect(addr)
            .await
            .map_err(|e| anyhow!("TcpStream::connect failed: {e}"))?;
        stream.set_nodelay(true)?;
        Ok(UringStream { inner: stream })
    }
}

pub struct UringListener {
    fd: RawFd,
    listener: TcpListener,
}

impl UringListener {
    pub async fn bind(addr: SocketAddr) -> Result<Self> {
        UringTransport::bind(addr).await
    }
}

#[async_trait]
impl TransportListener for UringListener {
    type Stream = UringStream;

    async fn accept(&self) -> Result<(Self::Stream, SocketAddr)> {
        let addr: sockaddr_storage = unsafe { std::mem::zeroed() };
        let addrlen: socklen_t = std::mem::size_of::<sockaddr_storage>() as socklen_t;

        let (tx, rx) = oneshot::channel();
        let op = UringOp {
            op_type: UringOpType::Accept {
                fd: self.fd,
                addr: Box::new(addr),
                addrlen: Box::new(addrlen),
            },
            tx: Some(tx),
        };

        get_engine()?.submit(op)?;
        let result = rx.await.map_err(|_| anyhow!("Operation cancelled"))??;

        if result < 0 {
            return Err(anyhow!("Accept failed: {}", result));
        }

        let client_fd = result;
        if let Err(e) = set_socket_nonblocking(client_fd) {
            unsafe {
                libc::close(client_fd);
            }
            return Err(e);
        }

        let std_stream = unsafe { std::net::TcpStream::from_raw_fd(client_fd) };
        let inner = TokioTcpStream::from_std(std_stream)
            .map_err(|e| anyhow!("TcpStream::from_std failed: {e}"))?;
        inner.set_nodelay(true)?;
        let peer_addr = inner.peer_addr()?;

        Ok((UringStream { inner }, peer_addr))
    }

    fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.listener.local_addr()?)
    }
}

pub struct UringStream {
    inner: TokioTcpStream,
}

impl UrpcAsRawFd for UringStream {
    fn as_raw_fd(&self) -> RawFd {
        std::os::fd::AsRawFd::as_raw_fd(&self.inner)
    }
}

#[async_trait]
impl TransportStream for UringStream {
    async fn read_buf(&mut self, buf: &mut BytesMut) -> Result<usize> {
        let n = self.inner.read_buf(buf).await?;
        Ok(n)
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        self.inner.write_all(buf).await?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.inner.flush().await?;
        Ok(())
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.inner.read_exact(buf).await?;
        Ok(())
    }

    fn peer_addr(&self) -> Result<SocketAddr> {
        Ok(self.inner.peer_addr()?)
    }

    fn set_keepalive(&self, keepalive: bool) -> Result<()> {
        use socket2::SockRef;
        let sock_ref = SockRef::from(&self.inner);
        sock_ref.set_keepalive(keepalive)?;
        Ok(())
    }

    fn set_nodelay(&self, nodelay: bool) -> Result<()> {
        use socket2::SockRef;
        let sock_ref = SockRef::from(&self.inner);
        sock_ref.set_nodelay(nodelay)?;
        Ok(())
    }
}
