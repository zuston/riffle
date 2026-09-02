//! A self-contained, completion-driven io_uring network engine for urpc.

use crate::urpc::frame::Frame;
use crate::urpc::uring::encode::{encode_frame_into, peek_request_header};
use anyhow::{anyhow, Context, Result};
use bytes::{Bytes, BytesMut};
use crossbeam::queue::SegQueue;
use io_uring::types::Fd;
use io_uring::{opcode, squeue, IoUring};
use log::{debug, error, info, warn};
use std::collections::VecDeque;
use std::net::{SocketAddr, TcpListener};
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

const KIND_REMOTE: u64 = 0;
const KIND_ACCEPT: u64 = 1;
const KIND_RECV: u64 = 2;
const KIND_SEND: u64 = 3;
const KIND_WAKE: u64 = 4;

#[inline]
fn pack_token(kind: u64, gen: u16, slot: u32) -> u64 {
    (kind << 56) | ((gen as u64) << 32) | (slot as u64)
}

#[inline]
fn unpack_token(token: u64) -> (u64, u16, u32) {
    (
        token >> 56,
        ((token >> 32) & 0xffff) as u16,
        (token & 0xffff_ffff) as u32,
    )
}

/// Tuning knobs of the engine. Defaults are sized for a shuffle workload.
#[derive(Debug, Clone)]
pub struct UringServerConfig {
    /// io_uring submission queue depth.
    pub ring_entries: u32,
    /// Enable kernel side SQ polling with the given idle time in ms.
    /// Requires CAP_SYS_ADMIN on kernels older than 5.13.
    pub sqpoll_idle_ms: Option<u32>,
    /// Minimum receive size while the next frame length is unknown.
    pub recv_chunk_size: usize,
    /// Initial per-connection read buffer capacity.
    pub initial_read_buffer_size: usize,
    /// Read buffers larger than this are shrunk once drained.
    pub read_buffer_shrink_threshold: usize,
    /// Hard cap of concurrent connections per engine thread.
    pub max_connections: usize,
    /// Optional cpu binding: engine thread `i` is pinned to
    /// `bind_cores[i % len]`.
    pub bind_cores: Option<Vec<u32>>,
}

impl Default for UringServerConfig {
    fn default() -> Self {
        Self {
            ring_entries: 1024,
            sqpoll_idle_ms: None,
            recv_chunk_size: 64 * 1024,
            initial_read_buffer_size: 32 * 1024,
            read_buffer_shrink_threshold: 1024 * 1024,
            max_connections: 65536,
            bind_cores: None,
        }
    }
}

/// Pluggable request handling hook.
///
/// `on_frame` is invoked on the engine thread for every complete request
/// frame. Cheap handlers should answer inline through
/// [`Responder::respond`]; expensive/async handlers should capture a
/// [`RemoteResponder`] (via [`Responder::remote`]) and complete the request
/// from another thread.
pub trait FrameHandler: Send + 'static {
    fn on_frame(&mut self, frame: Frame, responder: &mut Responder<'_>);
}

impl<F> FrameHandler for F
where
    F: FnMut(Frame, &mut Responder<'_>) + Send + 'static,
{
    fn on_frame(&mut self, frame: Frame, responder: &mut Responder<'_>) {
        self(frame, responder)
    }
}

pub(crate) struct EngineShared {
    remote_queue: SegQueue<(u64, Vec<Bytes>)>,
    wake_fd: RawFd,
    stopped: AtomicBool,
}

impl EngineShared {
    fn wake(&self) {
        let one: u64 = 1;
        let ret = unsafe {
            libc::write(
                self.wake_fd,
                &one as *const u64 as *const libc::c_void,
                std::mem::size_of::<u64>(),
            )
        };
        if ret < 0 {
            error!(
                "Failed to wake the uring engine eventfd: {}",
                std::io::Error::last_os_error()
            );
        }
    }
}

impl Drop for EngineShared {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.wake_fd);
        }
    }
}

/// Inline responder handed to [`FrameHandler::on_frame`].
pub struct Responder<'a> {
    conn: &'a mut Conn,
    token: u64,
    shared: &'a Arc<EngineShared>,
}

impl Responder<'_> {
    /// Encodes the response into the connection outbound queue. Multiple
    /// small responses within one receive batch are coalesced into a single
    /// send operation.
    pub fn respond(&mut self, frame: &Frame) -> Result<()> {
        let mut chunks = Vec::new();
        encode_frame_into(frame, &mut self.conn.out_head, &mut chunks)?;
        for chunk in chunks {
            self.conn.flush_head_to_queue();
            self.conn.out_queue.push_back(chunk);
        }
        Ok(())
    }

    /// Creates a thread-safe handle for completing this request later.
    pub fn remote(&self) -> RemoteResponder {
        RemoteResponder {
            token: self.token,
            shared: self.shared.clone(),
        }
    }
}

/// Thread-safe response handle. Responses posted after the connection has
/// been closed are silently dropped.
#[derive(Clone)]
pub struct RemoteResponder {
    token: u64,
    shared: Arc<EngineShared>,
}

impl RemoteResponder {
    pub fn respond(&self, frame: &Frame) -> Result<()> {
        let mut head = BytesMut::with_capacity(256);
        let mut chunks = Vec::new();
        encode_frame_into(frame, &mut head, &mut chunks)?;
        let mut bufs = Vec::with_capacity(1 + chunks.len());
        bufs.push(head.freeze());
        bufs.extend(chunks);
        self.shared.remote_queue.push((self.token, bufs));
        self.shared.wake();
        Ok(())
    }
}

struct Conn {
    fd: RawFd,
    gen: u16,
    read_buf: BytesMut,
    /// Staging buffer coalescing the meta parts of pending responses.
    out_head: BytesMut,
    out_queue: VecDeque<Bytes>,
    /// Bytes of the queue front that are already sent.
    out_offset: usize,
    recv_inflight: bool,
    send_inflight: bool,
    closing: bool,
    /// Total length of the partially received frame.
    pending_frame_len: Option<usize>,
}

impl Conn {
    fn new(fd: RawFd, gen: u16, initial_read_buffer_size: usize) -> Self {
        Self {
            fd,
            gen,
            read_buf: BytesMut::with_capacity(initial_read_buffer_size),
            out_head: BytesMut::new(),
            out_queue: VecDeque::new(),
            out_offset: 0,
            recv_inflight: false,
            send_inflight: false,
            closing: false,
            pending_frame_len: None,
        }
    }

    fn flush_head_to_queue(&mut self) {
        if !self.out_head.is_empty() {
            let head = self.out_head.split().freeze();
            self.out_queue.push_back(head);
        }
    }

    fn has_inflight(&self) -> bool {
        self.recv_inflight || self.send_inflight
    }
}

struct UringEngine<H: FrameHandler> {
    ring: IoUring,
    listener: TcpListener,
    cfg: UringServerConfig,
    conns: Vec<Option<Conn>>,
    /// Per-slot generation, bumped on every close to fence stale tokens.
    slot_gens: Vec<u16>,
    free_slots: Vec<u32>,
    handler: H,
    shared: Arc<EngineShared>,
    /// Target buffer of the eventfd read op; boxed for address stability.
    wake_buf: Box<u64>,
    accept_inflight: bool,
}

impl<H: FrameHandler> UringEngine<H> {
    fn new(listener: TcpListener, cfg: UringServerConfig, handler: H) -> Result<Self> {
        let mut builder = IoUring::builder();
        if let Some(idle) = cfg.sqpoll_idle_ms {
            builder.setup_sqpoll(idle);
        }
        let ring = builder
            .build(cfg.ring_entries)
            .context("failed to build io_uring instance")?;

        let wake_fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) };
        if wake_fd < 0 {
            return Err(anyhow!(
                "failed to create eventfd: {}",
                std::io::Error::last_os_error()
            ));
        }

        Ok(Self {
            ring,
            listener,
            cfg,
            conns: Vec::new(),
            slot_gens: Vec::new(),
            free_slots: Vec::new(),
            handler,
            shared: Arc::new(EngineShared {
                remote_queue: SegQueue::new(),
                wake_fd,
                stopped: AtomicBool::new(false),
            }),
            wake_buf: Box::new(0),
            accept_inflight: false,
        })
    }

    fn shared(&self) -> Arc<EngineShared> {
        self.shared.clone()
    }

    fn push_sqe(&mut self, sqe: squeue::Entry) {
        loop {
            let pushed = unsafe { self.ring.submission().push(&sqe) };
            if pushed.is_ok() {
                return;
            }
            // SQ full: flush what we have and retry.
            if let Err(e) = self.ring.submit() {
                error!("io_uring submit on full SQ failed: {}", e);
            }
        }
    }

    fn arm_wake(&mut self) {
        let ptr = &mut *self.wake_buf as *mut u64 as *mut u8;
        let sqe = opcode::Read::new(Fd(self.shared.wake_fd), ptr, 8)
            .build()
            .user_data(pack_token(KIND_WAKE, 0, 0));
        self.push_sqe(sqe);
    }

    fn arm_accept(&mut self) {
        if self.accept_inflight {
            return;
        }
        self.accept_inflight = true;
        let sqe = opcode::Accept::new(
            Fd(self.listener.as_raw_fd()),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
        .flags(libc::SOCK_CLOEXEC)
        .build()
        .user_data(pack_token(KIND_ACCEPT, 0, 0));
        self.push_sqe(sqe);
    }

    fn arm_recv(&mut self, slot: u32) {
        let (fd, token, ptr, len, recv_flags) = {
            let Some(conn) = self.conns[slot as usize].as_mut() else {
                return;
            };
            if conn.recv_inflight || conn.closing {
                return;
            }

            let frame_remaining = conn
                .pending_frame_len
                .and_then(|total| total.checked_sub(conn.read_buf.len()))
                .filter(|remaining| *remaining > 0);
            debug_assert_eq!(
                conn.pending_frame_len.is_some(),
                frame_remaining.is_some(),
                "pending frame must be incomplete before arming recv"
            );

            let spare_wanted = frame_remaining.unwrap_or(self.cfg.recv_chunk_size);
            let spare = conn.read_buf.capacity() - conn.read_buf.len();
            if spare < spare_wanted {
                conn.read_buf.reserve(spare_wanted);
            }
            let offset = conn.read_buf.len();
            let spare = conn.read_buf.capacity() - offset;
            let (recv_len, recv_flags) = match frame_remaining {
                // Waiting for the exact remainder completes a large frame in
                // one CQE without consuming the next pipelined frame.
                Some(remaining) => (remaining, libc::MSG_WAITALL),
                None => (spare, 0),
            };
            let ptr = unsafe { conn.read_buf.as_mut_ptr().add(offset) };
            conn.recv_inflight = true;
            (
                conn.fd,
                pack_token(KIND_RECV, conn.gen, slot),
                ptr,
                u32::try_from(recv_len).unwrap_or(u32::MAX),
                recv_flags,
            )
        };
        let sqe = opcode::Recv::new(Fd(fd), ptr, len)
            .flags(recv_flags)
            .build()
            .user_data(token);
        self.push_sqe(sqe);
    }

    fn arm_send(&mut self, slot: u32) {
        let (fd, token, ptr, len) = {
            let Some(conn) = self.conns[slot as usize].as_mut() else {
                return;
            };
            if conn.send_inflight {
                return;
            }
            conn.flush_head_to_queue();
            let Some(front) = conn.out_queue.front() else {
                return;
            };
            let ptr = unsafe { front.as_ptr().add(conn.out_offset) };
            let len = front.len() - conn.out_offset;
            conn.send_inflight = true;
            (
                conn.fd,
                pack_token(KIND_SEND, conn.gen, slot),
                ptr,
                len as u32,
            )
        };
        let sqe = opcode::Send::new(Fd(fd), ptr, len)
            .flags(libc::MSG_NOSIGNAL)
            .build()
            .user_data(token);
        self.push_sqe(sqe);
    }

    fn conn_mut(&mut self, slot: u32, gen: u16) -> Option<&mut Conn> {
        match self.conns.get_mut(slot as usize).and_then(Option::as_mut) {
            Some(conn) if conn.gen == gen => Some(conn),
            _ => None,
        }
    }

    fn on_accept(&mut self, res: i32) {
        self.accept_inflight = false;
        if !self.shared.stopped.load(Ordering::Acquire) {
            self.arm_accept();
        }
        if res < 0 {
            let errno = -res;
            // Transient accept errors are expected under churn.
            if errno != libc::EAGAIN && errno != libc::EINTR && errno != libc::ECONNABORTED {
                warn!("uring accept failed with errno: {}", errno);
            }
            return;
        }
        let fd = res as RawFd;
        let active = self.conns.iter().filter(|c| c.is_some()).count();
        if active >= self.cfg.max_connections {
            warn!("uring engine connection limit reached, rejecting fd {}", fd);
            unsafe { libc::close(fd) };
            return;
        }

        unsafe {
            let one: libc::c_int = 1;
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_NODELAY,
                &one as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_KEEPALIVE,
                &one as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }

        let slot = match self.free_slots.pop() {
            Some(slot) => slot,
            None => {
                self.conns.push(None);
                self.slot_gens.push(0);
                (self.conns.len() - 1) as u32
            }
        };
        let gen = self.slot_gens[slot as usize];
        self.conns[slot as usize] = Some(Conn::new(fd, gen, self.cfg.initial_read_buffer_size));
        self.arm_recv(slot);
    }

    fn on_recv(&mut self, slot: u32, gen: u16, res: i32) {
        let Some(conn) = self.conn_mut(slot, gen) else {
            return;
        };
        conn.recv_inflight = false;
        if conn.closing {
            self.maybe_finish_close(slot);
            return;
        }
        if res == 0 {
            // Clean EOF from the peer.
            self.begin_close(slot);
            return;
        }
        if res < 0 {
            let errno = -res;
            if errno == libc::EAGAIN || errno == libc::EWOULDBLOCK || errno == libc::EINTR {
                self.arm_recv(slot);
                return;
            }
            debug!("uring recv failed with errno {}, closing connection", errno);
            self.begin_close(slot);
            return;
        }

        {
            let conn = self.conns[slot as usize].as_mut().unwrap();
            let filled = conn.read_buf.len() + res as usize;
            debug_assert!(filled <= conn.read_buf.capacity());
            unsafe { conn.read_buf.set_len(filled) };
        }

        if let Err(e) = self.process_frames(slot) {
            warn!("Errors on handling the urpc frames, closing. err: {:#?}", e);
            self.begin_close(slot);
            return;
        }
        self.arm_send(slot);
        self.arm_recv(slot);
    }

    fn process_frames(&mut self, slot: u32) -> Result<()> {
        let this = &mut *self;
        let handler = &mut this.handler;
        let shared = &this.shared;
        let cfg = &this.cfg;
        let Some(conn) = this.conns[slot as usize].as_mut() else {
            return Ok(());
        };

        loop {
            let Some(header) = peek_request_header(&conn.read_buf)? else {
                conn.pending_frame_len = None;
                break;
            };
            let total = header.total_len();
            if conn.read_buf.len() < total {
                conn.pending_frame_len = Some(total);
                break;
            }
            conn.pending_frame_len = None;

            let frame_bytes = conn.read_buf.split_to(total).freeze();
            let frame = Frame::parse(frame_bytes)?;

            let mut responder = Responder {
                token: pack_token(KIND_REMOTE, conn.gen, slot),
                conn: &mut *conn,
                shared,
            };
            handler.on_frame(frame, &mut responder);
        }

        if conn.read_buf.is_empty() && conn.read_buf.capacity() > cfg.read_buffer_shrink_threshold {
            conn.read_buf = BytesMut::with_capacity(cfg.initial_read_buffer_size);
        }
        Ok(())
    }

    fn on_send(&mut self, slot: u32, gen: u16, res: i32) {
        let Some(conn) = self.conn_mut(slot, gen) else {
            return;
        };
        conn.send_inflight = false;
        if res < 0 {
            let errno = -res;
            if errno == libc::EAGAIN || errno == libc::EWOULDBLOCK || errno == libc::EINTR {
                if conn.closing {
                    self.maybe_finish_close(slot);
                } else {
                    self.arm_send(slot);
                }
                return;
            }
            debug!("uring send failed with errno {}, closing connection", errno);
            self.begin_close(slot);
            return;
        }

        conn.out_offset += res as usize;
        if let Some(front) = conn.out_queue.front() {
            if conn.out_offset >= front.len() {
                debug_assert_eq!(conn.out_offset, front.len());
                conn.out_queue.pop_front();
                conn.out_offset = 0;
            }
        }

        if conn.closing {
            self.maybe_finish_close(slot);
        } else {
            self.arm_send(slot);
        }
    }

    fn drain_remote_responses(&mut self) {
        while let Some((token, bufs)) = self.shared.remote_queue.pop() {
            let (_, gen, slot) = unpack_token(token);
            let Some(conn) = self.conn_mut(slot, gen) else {
                continue;
            };
            if conn.closing {
                continue;
            }
            // Preserve ordering with any inline-encoded pending head bytes.
            conn.flush_head_to_queue();
            conn.out_queue.extend(bufs);
            self.arm_send(slot);
        }
    }

    fn begin_close(&mut self, slot: u32) {
        let Some(conn) = self.conns[slot as usize].as_mut() else {
            return;
        };
        conn.closing = true;
        self.maybe_finish_close(slot);
    }

    fn maybe_finish_close(&mut self, slot: u32) {
        let should_close = match self.conns[slot as usize].as_ref() {
            Some(conn) => conn.closing && !conn.has_inflight(),
            None => false,
        };
        if !should_close {
            return;
        }
        let conn = self.conns[slot as usize].take().unwrap();
        unsafe { libc::close(conn.fd) };
        self.slot_gens[slot as usize] = self.slot_gens[slot as usize].wrapping_add(1);
        self.free_slots.push(slot);
    }

    fn handle_completion(&mut self, token: u64, res: i32) {
        let (kind, gen, slot) = unpack_token(token);
        match kind {
            KIND_ACCEPT => self.on_accept(res),
            KIND_RECV => self.on_recv(slot, gen, res),
            KIND_SEND => self.on_send(slot, gen, res),
            KIND_WAKE => {
                self.arm_wake();
                self.drain_remote_responses();
            }
            _ => warn!("unknown uring completion token kind: {}", kind),
        }
    }

    fn run(mut self) -> Result<()> {
        info!(
            "uring urpc engine started on {:?}, sqpoll: {:?}",
            self.listener.local_addr(),
            self.cfg.sqpoll_idle_ms
        );
        self.arm_wake();
        self.arm_accept();

        let mut completions: Vec<(u64, i32)> = Vec::with_capacity(1024);
        while !self.shared.stopped.load(Ordering::Acquire) {
            self.drain_remote_responses();
            self.ring
                .submit_and_wait(1)
                .context("io_uring submit_and_wait failed")?;
            completions.clear();
            {
                let cq = self.ring.completion();
                for cqe in cq {
                    completions.push((cqe.user_data(), cqe.result()));
                }
            }
            for &(token, res) in completions.iter() {
                self.handle_completion(token, res);
            }
        }

        // Shutdown: dropping the ring cancels all inflight operations.
        for conn in self.conns.iter().flatten() {
            unsafe { libc::close(conn.fd) };
        }
        info!("uring urpc engine stopped");
        Ok(())
    }
}

/// Handle of a running multi-threaded uring urpc server.
pub struct UringUrpcServer {
    shareds: Vec<Arc<EngineShared>>,
    joins: Vec<JoinHandle<()>>,
    local_addr: SocketAddr,
}

impl UringUrpcServer {
    /// Starts `threads` engine threads, each with its own `SO_REUSEPORT`
    /// listener and io_uring instance. `handler_factory(i)` builds the
    /// per-thread handler.
    pub fn start<H, F>(
        addr: SocketAddr,
        threads: usize,
        cfg: UringServerConfig,
        mut handler_factory: F,
    ) -> Result<Self>
    where
        H: FrameHandler,
        F: FnMut(usize) -> H,
    {
        if threads == 0 {
            return Err(anyhow!("uring engine thread number must be greater than 0"));
        }

        let mut listeners = Vec::with_capacity(threads);
        let first = build_reuseport_listener(addr)?;
        let local_addr = first.local_addr()?;
        listeners.push(first);
        for _ in 1..threads {
            listeners.push(build_reuseport_listener(local_addr)?);
        }

        let mut shareds = Vec::with_capacity(threads);
        let mut joins = Vec::with_capacity(threads);
        for (i, listener) in listeners.into_iter().enumerate() {
            let engine = UringEngine::new(listener, cfg.clone(), handler_factory(i))?;
            shareds.push(engine.shared());
            let bind_core = cfg
                .bind_cores
                .as_ref()
                .filter(|cores| !cores.is_empty())
                .map(|cores| cores[i % cores.len()]);
            let join = std::thread::Builder::new()
                .name(format!("urpc-uring-{i}"))
                .spawn(move || {
                    if let Some(core) = bind_core {
                        core_affinity::set_for_current(core_affinity::CoreId { id: core as _ });
                    }
                    if let Err(e) = engine.run() {
                        error!("uring urpc engine exited with error: {:#?}", e);
                    }
                })?;
            joins.push(join);
        }

        Ok(Self {
            shareds,
            joins,
            local_addr,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn shutdown(self) {
        for shared in &self.shareds {
            shared.stopped.store(true, Ordering::Release);
            shared.wake();
        }
        for join in self.joins {
            let _ = join.join();
        }
    }
}

fn build_reuseport_listener(addr: SocketAddr) -> Result<TcpListener> {
    let domain = match addr {
        SocketAddr::V4(_) => socket2::Domain::IPV4,
        SocketAddr::V6(_) => socket2::Domain::IPV6,
    };
    let sock = socket2::Socket::new(domain, socket2::Type::STREAM, None)?;
    sock.set_reuse_address(true)?;
    sock.set_reuse_port(true)?;
    sock.bind(&addr.into())?;
    sock.listen(8192)?;
    Ok(sock.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::urpc::command::RpcResponseCommand;
    use crate::urpc::frame::MessageType;
    use bytes::BufMut;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    fn put_string(buf: &mut BytesMut, value: &str) {
        buf.put_i32(value.len() as i32);
        buf.put_slice(value.as_bytes());
    }

    fn build_send_shuffle_data_frame(request_id: i64, payload: &[u8]) -> BytesMut {
        let mut body = BytesMut::new();
        body.put_i64(request_id);
        put_string(&mut body, "app-uring");
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
        body.put_slice(payload);
        body.put_i32(0); // shuffle servers
        body.put_i32(payload.len() as i32); // uncompress length
        body.put_i64(0); // free mem
        body.put_i64(123456); // timestamp

        let mut frame = BytesMut::with_capacity(9 + body.len());
        frame.put_i32(0);
        frame.put_u8(MessageType::SendShuffleData as u8);
        frame.put_i32(body.len() as i32);
        frame.extend_from_slice(&body);
        frame
    }

    async fn read_rpc_response(stream: &mut TcpStream) -> anyhow::Result<(i64, i32, String)> {
        let mut header = [0u8; 9];
        stream.read_exact(&mut header).await?;
        let content_len = i32::from_be_bytes(header[0..4].try_into().unwrap());
        assert_eq!(MessageType::RpcResponse as u8, header[4]);
        let body_len = i32::from_be_bytes(header[5..9].try_into().unwrap());
        assert_eq!(0, body_len);

        let mut content = vec![0u8; content_len as usize];
        stream.read_exact(&mut content).await?;
        let request_id = i64::from_be_bytes(content[0..8].try_into().unwrap());
        let status_code = i32::from_be_bytes(content[8..12].try_into().unwrap());
        let msg_len = i32::from_be_bytes(content[12..16].try_into().unwrap()) as usize;
        let msg = String::from_utf8(content[16..16 + msg_len].to_vec())?;
        Ok((request_id, status_code, msg))
    }

    async fn read_rpc_response_with_timeout(
        stream: &mut TcpStream,
    ) -> anyhow::Result<(i64, i32, String)> {
        Ok(tokio::time::timeout(Duration::from_secs(5), read_rpc_response(stream)).await??)
    }

    fn echo_handler(frame: Frame, responder: &mut Responder<'_>) {
        match frame {
            Frame::SendShuffleData(req) => {
                let resp = Frame::RpcResponse(RpcResponseCommand::new(
                    req.request_id(),
                    0,
                    format!("len={}", req.data_len()),
                ));
                responder.respond(&resp).unwrap();
            }
            other => panic!("unexpected frame: {other:?}"),
        }
    }

    fn start_echo_server() -> UringUrpcServer {
        UringUrpcServer::start(
            "127.0.0.1:0".parse().unwrap(),
            1,
            UringServerConfig::default(),
            |_| echo_handler,
        )
        .expect("uring server should start")
    }

    #[tokio::test]
    async fn echo_small_and_large_frames() -> anyhow::Result<()> {
        let server = start_echo_server();
        let mut stream = TcpStream::connect(server.local_addr()).await?;

        // Small frame.
        let payload = vec![1u8; 128];
        stream
            .write_all(&build_send_shuffle_data_frame(1, &payload))
            .await?;
        let (request_id, status, msg) = read_rpc_response(&mut stream).await?;
        assert_eq!(1, request_id);
        assert_eq!(0, status);
        assert_eq!("len=128", msg);

        // Large frame spanning many recv operations.
        let payload = vec![7u8; 4 * 1024 * 1024];
        stream
            .write_all(&build_send_shuffle_data_frame(2, &payload))
            .await?;
        let (request_id, status, msg) = read_rpc_response(&mut stream).await?;
        assert_eq!(2, request_id);
        assert_eq!(0, status);
        assert_eq!(format!("len={}", payload.len()), msg);

        drop(stream);
        server.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn echo_pipelined_requests() -> anyhow::Result<()> {
        let server = start_echo_server();
        let mut stream = TcpStream::connect(server.local_addr()).await?;

        let count: i64 = 1000;
        let payload = vec![3u8; 512];
        let mut batch = BytesMut::new();
        for id in 0..count {
            batch.extend_from_slice(&build_send_shuffle_data_frame(id, &payload));
        }
        stream.write_all(&batch).await?;

        for id in 0..count {
            let (request_id, status, _) = read_rpc_response(&mut stream).await?;
            assert_eq!(id, request_id);
            assert_eq!(0, status);
        }

        drop(stream);
        server.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn echo_fragmented_large_frame_preserves_pipelined_frame() -> anyhow::Result<()> {
        let server = start_echo_server();
        let mut stream = TcpStream::connect(server.local_addr()).await?;

        let large_payload = vec![5u8; 2 * 1024 * 1024];
        let large_frame = build_send_shuffle_data_frame(1, &large_payload);
        let small_payload = vec![6u8; 128];
        let small_frame = build_send_shuffle_data_frame(2, &small_payload);

        let first_chunk_len = 64 * 1024;
        stream.write_all(&large_frame[..first_chunk_len]).await?;
        tokio::time::sleep(Duration::from_millis(20)).await;

        let mut remainder_and_next =
            BytesMut::with_capacity(large_frame.len() - first_chunk_len + small_frame.len());
        remainder_and_next.extend_from_slice(&large_frame[first_chunk_len..]);
        remainder_and_next.extend_from_slice(&small_frame);
        stream.write_all(&remainder_and_next).await?;

        let (request_id, status, msg) = read_rpc_response_with_timeout(&mut stream).await?;
        assert_eq!(1, request_id);
        assert_eq!(0, status);
        assert_eq!(format!("len={}", large_payload.len()), msg);

        let (request_id, status, msg) = read_rpc_response_with_timeout(&mut stream).await?;
        assert_eq!(2, request_id);
        assert_eq!(0, status);
        assert_eq!(format!("len={}", small_payload.len()), msg);

        drop(stream);
        server.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn fragmented_large_frame_does_not_block_another_connection() -> anyhow::Result<()> {
        let server = start_echo_server();
        let mut slow_stream = TcpStream::connect(server.local_addr()).await?;

        let slow_payload = vec![7u8; 2 * 1024 * 1024];
        let slow_frame = build_send_shuffle_data_frame(1, &slow_payload);
        let first_chunk_len = 64 * 1024;
        slow_stream
            .write_all(&slow_frame[..first_chunk_len])
            .await?;
        tokio::time::sleep(Duration::from_millis(20)).await;

        let mut fast_stream = TcpStream::connect(server.local_addr()).await?;
        fast_stream
            .write_all(&build_send_shuffle_data_frame(2, &[8u8; 128]))
            .await?;
        let (request_id, status, msg) =
            tokio::time::timeout(Duration::from_secs(1), read_rpc_response(&mut fast_stream))
                .await??;
        assert_eq!(2, request_id);
        assert_eq!(0, status);
        assert_eq!("len=128", msg);

        slow_stream
            .write_all(&slow_frame[first_chunk_len..])
            .await?;
        let (request_id, status, msg) = read_rpc_response_with_timeout(&mut slow_stream).await?;
        assert_eq!(1, request_id);
        assert_eq!(0, status);
        assert_eq!(format!("len={}", slow_payload.len()), msg);

        drop(fast_stream);
        drop(slow_stream);
        server.shutdown();
        Ok(())
    }

    #[tokio::test]
    async fn remote_responder_completes_from_other_thread() -> anyhow::Result<()> {
        let handler = |frame: Frame, responder: &mut Responder<'_>| {
            let Frame::SendShuffleData(req) = frame else {
                panic!("unexpected frame");
            };
            let remote = responder.remote();
            let request_id = req.request_id();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(20));
                let resp =
                    Frame::RpcResponse(RpcResponseCommand::new(request_id, 0, "async".into()));
                remote.respond(&resp).unwrap();
            });
        };
        let server = UringUrpcServer::start(
            "127.0.0.1:0".parse().unwrap(),
            1,
            UringServerConfig::default(),
            |_| handler,
        )?;

        let mut stream = TcpStream::connect(server.local_addr()).await?;
        stream
            .write_all(&build_send_shuffle_data_frame(42, &[9u8; 64]))
            .await?;
        let (request_id, status, msg) = read_rpc_response(&mut stream).await?;
        assert_eq!(42, request_id);
        assert_eq!(0, status);
        assert_eq!("async", msg);

        drop(stream);
        server.shutdown();
        Ok(())
    }
}
