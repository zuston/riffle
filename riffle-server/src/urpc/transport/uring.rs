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

//! uRPC transport: **io_uring** for listen `accept(2)` and for **connected** TCP I/O via
//! **`IORING_OP_RECV` / `IORING_OP_SEND`** (socket equivalents of `recv(2)` / `send(2)`). Sockets
//! are **not** registered with Tokio's epoll reactor for data I/O — only the async `await` bridge
//! runs on Tokio workers.

use super::{AsRawFd as UrpcAsRawFd, Transport, TransportListener, TransportStream};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::BytesMut;
use core_affinity::CoreId;
use io_uring::{opcode, types::Fd, IoUring};
use libc::{sockaddr_storage, socklen_t};
use log::{debug, info, warn};
use once_cell::sync::OnceCell;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::net::{SocketAddr, TcpListener};
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};
use tokio::net::TcpStream as TokioTcpStream;
use tokio::sync::oneshot;

/// Max bytes per `IORING_OP_READ` / `IORING_OP_WRITE` SQE (balance copy vs syscall batching).
const IO_URING_RW_CHUNK: usize = 256 * 1024;
const SUBMIT_BLOCK_WARN_THRESHOLD: Duration = Duration::from_millis(20);
const WORKER_STATS_LOG_INTERVAL: Duration = Duration::from_secs(5);

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
                uring_worker_loop(engine_index, submission_rx);
            })?;

        Ok(Self { submission_tx })
    }

    /// Submit an operation to the io-uring engine
    fn submit(&self, op: UringOp) -> Result<()> {
        let start = Instant::now();
        self.submission_tx
            .send(op)
            .map_err(|_| anyhow!("io-uring engine stopped"))?;
        let blocked = start.elapsed();
        if blocked >= SUBMIT_BLOCK_WARN_THRESHOLD {
            warn!(
                "io-uring submit blocked for {:?}, sender thread_id={:?}",
                blocked,
                std::thread::current().id()
            );
        }
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
static URING_PENDING_MISS_WARNED: AtomicBool = AtomicBool::new(false);

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

    fn get_accept_engine(&self) -> Arc<UringNetEngine> {
        self.engines[0].clone()
    }

    fn get_stream_engine_for_key(&self, key: u64) -> Arc<UringNetEngine> {
        if self.engines.len() == 1 {
            return self.engines[0].clone();
        }
        // Reserve engine 0 for Accept; spread stream I/O across remaining engines.
        let idx = 1 + ((key as usize) % (self.engines.len() - 1));
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

pub(crate) fn get_accept_engine() -> Result<Arc<UringNetEngine>> {
    URING_ENGINE_REGISTRY
        .get()
        .map(|registry| registry.get_accept_engine())
        .ok_or_else(|| anyhow!("io-uring engine not initialized"))
}

pub(crate) fn get_stream_engine_for_key(key: u64) -> Result<Arc<UringNetEngine>> {
    URING_ENGINE_REGISTRY
        .get()
        .map(|registry| registry.get_stream_engine_for_key(key))
        .ok_or_else(|| anyhow!("io-uring engine not initialized"))
}

enum UringOpType {
    /// Listener accept op. For blocking listeners, this stays pending in kernel until a new
    /// connection arrives.
    Accept {
        fd: RawFd,
        addr: Box<sockaddr_storage>,
        addrlen: Box<socklen_t>,
    },
    Read {
        fd: RawFd,
        buf: Vec<u8>,
    },
    Write {
        fd: RawFd,
        buf: Vec<u8>,
    },
}

unsafe impl Send for UringOpType {}

enum UringNotify {
    /// `accept` / `write`: kernel result as `i32` (bytes written or new fd; negative errno for accept).
    Int(oneshot::Sender<Result<i32>>),
    /// `read`: filled buffer (empty = EOF).
    Read(oneshot::Sender<Result<Vec<u8>>>),
}

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

fn set_socket_nodelay(fd: RawFd) -> Result<()> {
    let flag: libc::c_int = 1;
    let ret = unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            &flag as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if ret < 0 {
        return Err(anyhow!("setsockopt(TCP_NODELAY) failed on fd {}", fd));
    }
    Ok(())
}

fn get_peer_addr(fd: RawFd) -> Result<SocketAddr> {
    let mut addr: sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut addrlen = std::mem::size_of::<sockaddr_storage>() as socklen_t;
    let ret =
        unsafe { libc::getpeername(fd, &mut addr as *mut _ as *mut libc::sockaddr, &mut addrlen) };
    if ret < 0 {
        return Err(anyhow!("getpeername failed on fd {}", fd));
    }
    // Convert sockaddr_storage to SocketAddr
    if addrlen == 0 {
        return Err(anyhow!("getpeername returned empty address"));
    }
    let addr = unsafe {
        if addr.ss_family as libc::c_int == libc::AF_INET {
            let sin = &*(&addr as *const sockaddr_storage as *const libc::sockaddr_in);
            let ip = std::net::Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr));
            let port = u16::from_be(sin.sin_port);
            SocketAddr::from((ip, port))
        } else if addr.ss_family as libc::c_int == libc::AF_INET6 {
            let sin6 = &*(&addr as *const sockaddr_storage as *const libc::sockaddr_in6);
            let ip = std::net::Ipv6Addr::from(sin6.sin6_addr.s6_addr);
            let port = u16::from_be(sin6.sin6_port);
            SocketAddr::from((ip, port))
        } else {
            return Err(anyhow!("unknown address family: {}", addr.ss_family));
        }
    };
    Ok(addr)
}

struct UringOp {
    op_type: UringOpType,
    notify: Option<UringNotify>,
}

fn push_sqe(uring: &mut IoUring, op_type: &UringOpType, user_data: u64) -> bool {
    let sqe = build_sqe(op_type).user_data(user_data);
    let mut sq = uring.submission();
    unsafe { sq.push(&sqe).is_ok() }
}

#[derive(Default)]
struct WorkerStats {
    sq_full_retries: u64,
    submit_calls: u64,
    cqe_total: u64,
    write_eagain_resubmits: u64,
    pending_high_watermark: usize,
}

impl WorkerStats {
    fn note_pending(&mut self, pending: usize) {
        if pending > self.pending_high_watermark {
            self.pending_high_watermark = pending;
        }
    }

    fn maybe_log(&self, engine_index: usize, pending_now: usize, last_log_at: Instant) {
        if last_log_at.elapsed() < WORKER_STATS_LOG_INTERVAL {
            return;
        }
        info!(
            "io-uring worker stats: engine={}, pending_now={}, pending_high_watermark={}, submit_calls={}, cqe_total={}, sq_full_retries={}, write_eagain_resubmits={}",
            engine_index,
            pending_now,
            self.pending_high_watermark,
            self.submit_calls,
            self.cqe_total,
            self.sq_full_retries,
            self.write_eagain_resubmits
        );
    }
}

fn build_sqe(op_type: &UringOpType) -> io_uring::squeue::Entry {
    match op_type {
        UringOpType::Accept { fd, addr, addrlen } => {
            // SAFETY: kernel writes peer address into addr/addrlen during the async Accept.
            // We must pass mutable pointers. The Box-backed storage remains stable in pending.
            // Note: we do NOT use .flags() here; we manually set nonblocking/cloexec after accept
            // for wider kernel compatibility (uring_simple tests showed this approach works).
            let addr_ptr = addr.as_ref() as *const sockaddr_storage as *mut _;
            let addrlen_ptr = addrlen.as_ref() as *const socklen_t as *mut socklen_t;
            opcode::Accept::new(Fd(*fd), addr_ptr, addrlen_ptr).build()
        }
        UringOpType::Read { fd, buf } => {
            let len = buf.len() as u32;
            // SAFETY: kernel fills `buf`; it remains in `pending` until the CQE is processed.
            // Use `Recv` (not `Read`) for TCP — matches kernel expectations for sockets on all
            // supported kernels; `Read`/`Write` offset rules differ for non-regular files.
            let ptr = buf.as_ptr() as *mut u8;
            opcode::Recv::new(Fd(*fd), ptr, len).build()
        }
        UringOpType::Write { fd, buf } => {
            opcode::Send::new(Fd(*fd), buf.as_ptr(), buf.len() as u32).build()
        }
    }
}

/// Multiplexed loop: many SQEs may be in flight (accept, read, write on non-blocking fds).
fn push_with_submit_retry(
    uring: &mut IoUring,
    op_type: &UringOpType,
    user_data: u64,
    stats: &mut WorkerStats,
) {
    while !push_sqe(uring, op_type, user_data) {
        stats.sq_full_retries += 1;
        stats.submit_calls += 1;
        if let Err(e) = uring.submit() {
            warn!("io-uring submit failed while handling sq-full retry: {}", e);
            continue;
        }
    }
}

struct UringWorker {
    engine_index: usize,
    uring: IoUring,
    pending: HashMap<u64, UringOp>,
    next_id: u64,
    stats: WorkerStats,
    last_log_at: Instant,
}

impl UringWorker {
    fn new(engine_index: usize) -> Self {
        Self {
            engine_index,
            uring: IoUring::new(1024).expect("Failed to create io-uring"),
            pending: HashMap::new(),
            next_id: 1,
            stats: WorkerStats::default(),
            last_log_at: Instant::now(),
        }
    }

    fn enqueue_op(&mut self, op: UringOp) {
        let id = self.next_id;
        self.next_id += 1;
        push_with_submit_retry(&mut self.uring, &op.op_type, id, &mut self.stats);
        self.pending.insert(id, op);
        self.stats.note_pending(self.pending.len());
    }

    fn drain_submission_channel(&mut self, rx: &mpsc::Receiver<UringOp>) -> bool {
        let mut drained = false;
        while let Ok(op) = rx.try_recv() {
            self.enqueue_op(op);
            drained = true;
        }
        drained
    }

    fn process_completions(&mut self) {
        let cqes: Vec<_> = self.uring.completion().collect();
        self.stats.cqe_total += cqes.len() as u64;
        for cqe in cqes {
            self.handle_completion(cqe.user_data(), cqe.result());
        }
    }

    fn handle_completion(&mut self, id: u64, result: i32) {
        let Some(op) = self.pending.get(&id) else {
            if !URING_PENDING_MISS_WARNED.swap(true, Ordering::Relaxed) {
                warn!(
                    "io-uring completion miss: user_data={} not found in pending map",
                    id
                );
            }
            return;
        };

        if cqe_result_is_would_block(result) {
            if matches!(op.op_type, UringOpType::Write { .. }) {
                self.stats.write_eagain_resubmits += 1;
                debug!("io-uring write would-block, re-submit with same user_data={id}");
            }
            push_with_submit_retry(&mut self.uring, &op.op_type, id, &mut self.stats);
            return;
        }

        if result < 0 {
            self.notify_error(id, result);
            return;
        }

        self.notify_success(id, result);
    }

    fn notify_error(&mut self, id: u64, result: i32) {
        let is_accept = matches!(
            self.pending.get(&id).map(|op| &op.op_type),
            Some(UringOpType::Accept { .. })
        );
        let mut op = self.pending.remove(&id).expect("pending op");
        match op.notify.take() {
            Some(UringNotify::Int(tx)) => {
                if is_accept {
                    let _ = tx.send(Ok(result));
                } else {
                    let _ = tx.send(Err(anyhow!("io_uring op failed: errno {}", -result)));
                }
            }
            Some(UringNotify::Read(tx)) => {
                let _ = tx.send(Err(anyhow!("read failed: errno {}", -result)));
            }
            None => {}
        }
    }

    fn notify_success(&mut self, id: u64, result: i32) {
        let mut op = self.pending.remove(&id).expect("pending op");
        match (op.notify.take(), op.op_type) {
            (Some(UringNotify::Int(tx)), UringOpType::Accept { .. }) => {
                let _ = tx.send(Ok(result));
            }
            (Some(UringNotify::Int(tx)), UringOpType::Write { .. }) => {
                let _ = tx.send(Ok(result));
            }
            (Some(UringNotify::Read(tx)), UringOpType::Read { mut buf, .. }) => {
                if result == 0 {
                    buf.clear();
                } else {
                    buf.truncate(result as usize);
                }
                let _ = tx.send(Ok(buf));
            }
            _ => {}
        }
    }

    fn maybe_log(&mut self) {
        self.stats
            .maybe_log(self.engine_index, self.pending.len(), self.last_log_at);
        if self.last_log_at.elapsed() >= WORKER_STATS_LOG_INTERVAL {
            self.last_log_at = Instant::now();
        }
    }

    fn run(mut self, rx: mpsc::Receiver<UringOp>) {
        loop {
            self.drain_submission_channel(&rx);

            if self.pending.is_empty() {
                let op = match rx.recv() {
                    Ok(o) => o,
                    Err(_) => break,
                };
                self.enqueue_op(op);
            }

            self.stats.submit_calls += 1;
            match self.uring.submit_and_wait(1) {
                Err(e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(e) => panic!("io_uring submit_and_wait failed: {e}"),
                Ok(_) => {}
            }

            self.process_completions();
            self.maybe_log();
        }
    }
}

fn uring_worker_loop(engine_index: usize, rx: mpsc::Receiver<UringOp>) {
    UringWorker::new(engine_index).run(rx);
}

/// io-uring based transport
pub struct UringTransport;

#[async_trait]
impl Transport for UringTransport {
    type Listener = UringListener;
    type Stream = UringStream;

    async fn bind(addr: SocketAddr) -> Result<Self::Listener> {
        // Keep listener in blocking mode so accept does not spin on -EAGAIN when idle.
        let listener = TcpListener::bind(addr)?;
        let fd = listener.as_raw_fd();

        Ok(UringListener { fd, listener })
    }

    async fn connect(addr: SocketAddr) -> Result<Self::Stream> {
        // Client processes do not run `rpc::_start_uring`; stream I/O uses the same engine pool.
        init_uring_engine(0)?;
        let tokio_stream = TokioTcpStream::connect(addr)
            .await
            .map_err(|e| anyhow!("TcpStream::connect failed: {e}"))?;
        let mut std_stream = tokio_stream
            .into_std()
            .map_err(|e| anyhow!("into_std failed: {e}"))?;
        std_stream.set_nonblocking(true)?;
        std_stream.set_nodelay(true)?;
        let peer = std_stream.peer_addr()?;
        // Convert to raw fd. UringStream manages fd lifetime via Drop.
        let fd = std_stream.as_raw_fd();
        let _ = std_stream.into_raw_fd(); // Prevents drop from closing fd
        let stream_engine = get_stream_engine_for_key(fd as u64)?;
        Ok(UringStream {
            fd,
            peer,
            engine: stream_engine,
        })
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

        // Accept always runs on dedicated engine 0.
        let engine = get_accept_engine()?;

        let (tx, rx) = oneshot::channel();
        let op = UringOp {
            op_type: UringOpType::Accept {
                fd: self.fd,
                addr: Box::new(addr),
                addrlen: Box::new(addrlen),
            },
            notify: Some(UringNotify::Int(tx)),
        };

        engine.submit(op)?;
        let result = rx.await.map_err(|_| anyhow!("Operation cancelled"))??;

        if result < 0 {
            return Err(anyhow!("Accept failed: {}", result));
        }

        let client_fd = result;

        // Set socket options directly using libc (like uring_simple test).
        // Accept returns a blocking socket; we must set O_NONBLOCK for io-uring.
        set_socket_nonblocking(client_fd)?;
        set_socket_nodelay(client_fd)?;
        let peer_addr = get_peer_addr(client_fd)?;
        let stream_engine = get_stream_engine_for_key(client_fd as u64)?;

        Ok((
            UringStream {
                fd: client_fd,
                peer: peer_addr,
                engine: stream_engine,
            },
            peer_addr,
        ))
    }

    fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.listener.local_addr()?)
    }
}

/// TCP stream: data path uses io_uring (`IORING_OP_RECV` / `IORING_OP_SEND`); fd is not driven by
/// Tokio's epoll for read/write.
pub struct UringStream {
    fd: RawFd,
    peer: SocketAddr,
    engine: Arc<UringNetEngine>,
}

impl UrpcAsRawFd for UringStream {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

impl Drop for UringStream {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.fd);
        }
    }
}

impl UringStream {
    async fn read_once(&mut self, slice: &mut [u8]) -> Result<usize> {
        if slice.is_empty() {
            return Ok(0);
        }
        let want = slice.len().min(IO_URING_RW_CHUNK);
        let buf = vec![0u8; want];
        let fd = self.fd;
        let (tx, rx) = oneshot::channel();
        let op = UringOp {
            op_type: UringOpType::Read { fd, buf },
            notify: Some(UringNotify::Read(tx)),
        };
        self.engine.submit(op)?;
        let v = rx.await.map_err(|_| anyhow!("Operation cancelled"))??;
        if v.is_empty() {
            return Ok(0);
        }
        let n = v.len().min(slice.len());
        slice[..n].copy_from_slice(&v[..n]);
        Ok(n)
    }
}

#[async_trait]
impl TransportStream for UringStream {
    async fn read_buf(&mut self, buf: &mut BytesMut) -> Result<usize> {
        let chunk = vec![0u8; IO_URING_RW_CHUNK];
        let fd = self.fd;
        let (tx, rx) = oneshot::channel();
        let op = UringOp {
            op_type: UringOpType::Read { fd, buf: chunk },
            notify: Some(UringNotify::Read(tx)),
        };
        self.engine.submit(op)?;
        let v = rx.await.map_err(|_| anyhow!("Operation cancelled"))??;
        if v.is_empty() {
            return Ok(0);
        }
        let n = v.len();
        buf.extend_from_slice(&v);
        Ok(n)
    }

    async fn write_all(&mut self, mut buf: &[u8]) -> Result<()> {
        while !buf.is_empty() {
            let take = buf.len().min(std::u32::MAX as usize);
            let payload = buf[..take].to_vec();
            let mut offset = 0usize;
            let fd = self.fd;

            // Prefer one io_uring send for the whole frame; only retry unsent tail on short write.
            while offset < payload.len() {
                let submit_start = Instant::now();
                let remaining = payload[offset..].to_vec();
                let (tx, rx) = oneshot::channel();
                let op = UringOp {
                    op_type: UringOpType::Write { fd, buf: remaining },
                    notify: Some(UringNotify::Int(tx)),
                };
                self.engine.submit(op)?;
                let r = rx.await.map_err(|_| anyhow!("Operation cancelled"))??;
                let wait_elapsed = submit_start.elapsed();
                if wait_elapsed >= Duration::from_secs(1) {
                    warn!(
                        "io-uring write completion slow: fd={}, chunk_len={}, offset={}, wait={:?}, result={}",
                        fd,
                        take,
                        offset,
                        wait_elapsed,
                        r
                    );
                }
                if r <= 0 {
                    if r == 0 {
                        warn!(
                            "io-uring write returned 0: fd={}, chunk_len={}, offset={}",
                            fd, take, offset
                        );
                        return Err(anyhow!("write returned 0 (peer closed)"));
                    }
                    return Err(anyhow!("write failed: {}", r));
                }

                let n = r as usize;
                offset += n;

                debug!(
                    "io-uring write completion: fd={}, chunk_len={}, offset={}, result={}",
                    fd, take, offset, r
                );
            }
            buf = &buf[take..];
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        let mut filled = 0;
        while filled < buf.len() {
            let n = self.read_once(&mut buf[filled..]).await?;
            if n == 0 {
                return Err(anyhow!("unexpected EOF in read_exact"));
            }
            filled += n;
        }
        Ok(())
    }

    fn peer_addr(&self) -> Result<SocketAddr> {
        Ok(self.peer)
    }

    fn set_keepalive(&self, keepalive: bool) -> Result<()> {
        let flag: libc::c_int = if keepalive { 1 } else { 0 };
        let ret = unsafe {
            libc::setsockopt(
                self.fd,
                libc::SOL_SOCKET,
                libc::SO_KEEPALIVE,
                &flag as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if ret < 0 {
            return Err(anyhow!("setsockopt(SO_KEEPALIVE) failed on fd {}", self.fd));
        }
        Ok(())
    }

    fn set_nodelay(&self, nodelay: bool) -> Result<()> {
        let flag: libc::c_int = if nodelay { 1 } else { 0 };
        let ret = unsafe {
            libc::setsockopt(
                self.fd,
                libc::IPPROTO_TCP,
                libc::TCP_NODELAY,
                &flag as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if ret < 0 {
            return Err(anyhow!("setsockopt(TCP_NODELAY) failed on fd {}", self.fd));
        }
        Ok(())
    }
}
