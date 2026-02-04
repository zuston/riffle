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

use super::{AsRawFd, Transport, TransportListener, TransportStream};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::BytesMut;
use io_uring::{opcode, types::Fd, IoUring};
use libc::{sa_family_t, sockaddr_in, sockaddr_in6, sockaddr_storage, socklen_t};
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::os::fd::{FromRawFd, RawFd};
use std::sync::mpsc;
use tokio::sync::oneshot;

/// Global io-uring engine for network I/O
pub struct UringNetEngine {
    submission_tx: mpsc::SyncSender<UringOp>,
}

impl UringNetEngine {
    /// Create a new io-uring network engine with the specified number of threads
    pub fn new(threads: usize) -> Result<Self> {
        let (submission_tx, submission_rx) = mpsc::sync_channel::<UringOp>(4096);

        // Spawn io-uring worker threads
        for i in 0..threads {
            let rx = submission_rx.clone();
            std::thread::Builder::new()
                .name(format!("riffle-uring-net-{}", i))
                .spawn(move || {
                    UringWorker::new(rx, i).run();
                })?;
        }

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

// Thread-local or global engine instance
thread_local! {
    static URING_ENGINE: std::cell::RefCell<Option<UringNetEngine>> = const { std::cell::RefCell::new(None) };
}

/// Initialize the global io-uring network engine
pub fn init_uring_engine(threads: usize) -> Result<()> {
    let engine = UringNetEngine::new(threads)?;
    URING_ENGINE.with(|e| {
        *e.borrow_mut() = Some(engine);
    });
    Ok(())
}

/// Get the global io-uring engine
fn get_engine() -> Result<UringNetEngine> {
    URING_ENGINE.with(|e| {
        e.borrow()
            .clone()
            .ok_or_else(|| anyhow!("io-uring engine not initialized"))
    })
}

/// Operation types for io-uring
enum UringOpType {
    Accept {
        fd: RawFd,
        addr: Box<sockaddr_storage>,
        addrlen: Box<socklen_t>,
    },
    Connect {
        fd: RawFd,
        addr: Box<sockaddr_storage>,
        addrlen: socklen_t,
    },
    Recv {
        fd: RawFd,
        buf: *mut u8,
        len: usize,
    },
    Send {
        fd: RawFd,
        buf: *const u8,
        len: usize,
    },
    Close {
        fd: RawFd,
    },
}

unsafe impl Send for UringOpType {}

struct UringOp {
    op_type: UringOpType,
    tx: oneshot::Sender<Result<i32>>,
}

/// io-uring worker thread
struct UringWorker {
    rx: mpsc::Receiver<UringOp>,
    uring: IoUring,
    pending: Vec<UringOp>,
    worker_id: usize,
}

impl UringWorker {
    fn new(rx: mpsc::Receiver<UringOp>, worker_id: usize) -> Self {
        // Create io-uring instance with 1024 entries
        let uring = IoUring::new(1024).expect("Failed to create io-uring");
        Self {
            rx,
            uring,
            pending: Vec::with_capacity(1024),
            worker_id,
        }
    }

    fn run(mut self) {
        loop {
            // Collect operations from channel
            while self.pending.len() < 1024 {
                match self.rx.try_recv() {
                    Ok(op) => self.pending.push(op),
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        if self.pending.is_empty() {
                            return;
                        }
                        break;
                    }
                }
            }

            // Submit operations to io-uring
            if !self.pending.is_empty() {
                let mut submitted = 0;
                {
                    let mut sq = self.uring.submission();
                    for op in &self.pending {
                        let sqe = match &op.op_type {
                            UringOpType::Accept { fd, addr, addrlen } => opcode::Accept::new(
                                Fd(*fd),
                                addr.as_ref() as *const _ as *mut _,
                                addrlen.as_ref() as *const _ as *mut _,
                            )
                            .build(),
                            UringOpType::Connect { fd, addr, addrlen } => opcode::Connect::new(
                                Fd(*fd),
                                addr.as_ref() as *const _ as *const _,
                                *addrlen,
                            )
                            .build(),
                            UringOpType::Recv { fd, buf, len } => {
                                opcode::Recv::new(Fd(*fd), *buf, *len as u32).build()
                            }
                            UringOpType::Send { fd, buf, len } => {
                                opcode::Send::new(Fd(*fd), *buf, *len as u32).build()
                            }
                            UringOpType::Close { fd } => opcode::Close::new(Fd(*fd)).build(),
                        };

                        let user_data = self.pending.as_ptr() as u64 + submitted;
                        unsafe {
                            if sq.push(sqe.user_data(user_data)).is_err() {
                                break;
                            }
                        }
                        submitted += 1;
                    }
                }

                if submitted > 0 {
                    self.uring.submit().expect("Failed to submit sqes");
                }
            }

            // Wait for completions
            if !self.pending.is_empty() {
                self.uring
                    .submit_and_wait(1)
                    .expect("Failed to wait for completions");

                let cqes: Vec<_> = self.uring.completion().collect();
                for cqe in cqes {
                    let idx = (cqe.user_data() - self.pending.as_ptr() as u64) as usize;
                    if idx < self.pending.len() {
                        let op = &self.pending[idx];
                        let res = cqe.result();
                        let result = if res < 0 {
                            Err(anyhow!("io-uring operation failed: {}", res))
                        } else {
                            Ok(res)
                        };
                        let _ = op.tx.send(result);
                    }
                }

                // Remove completed operations
                self.pending.clear();
            } else {
                // No pending operations, sleep a bit
                std::thread::sleep(std::time::Duration::from_millis(1));
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
        let std_listener = StdTcpListener::bind(addr)?;
        std_listener.set_nonblocking(true)?;
        let fd = std_listener.as_raw_fd();

        Ok(UringListener {
            fd,
            _listener: std_listener,
            engine: get_engine()?,
        })
    }

    async fn connect(addr: SocketAddr) -> Result<Self::Stream> {
        let engine = get_engine()?;

        // Create socket
        let domain = match addr {
            SocketAddr::V4(_) => libc::AF_INET,
            SocketAddr::V6(_) => libc::AF_INET6,
        };
        let fd = unsafe { libc::socket(domain, libc::SOCK_STREAM, 0) };
        if fd < 0 {
            return Err(anyhow!("Failed to create socket"));
        }

        // Set non-blocking
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL, 0) };
        unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };

        // Prepare address
        let (addr_storage, addr_len) = socket_addr_to_storage(addr);

        // Submit connect operation
        let (tx, rx) = oneshot::channel();
        let op = UringOp {
            op_type: UringOpType::Connect {
                fd,
                addr: Box::new(addr_storage),
                addrlen: addr_len,
            },
            tx,
        };

        engine.submit(op)?;
        let result = rx.await.map_err(|_| anyhow!("Operation cancelled"))??;

        if result < 0 {
            unsafe { libc::close(fd) };
            return Err(anyhow!("Connect failed: {}", result));
        }

        Ok(UringStream {
            fd,
            engine,
            read_buf: None,
        })
    }
}

pub struct UringListener {
    fd: RawFd,
    _listener: StdTcpListener,
    engine: UringNetEngine,
}

#[async_trait]
impl TransportListener for UringListener {
    type Stream = UringStream;

    async fn accept(&self) -> Result<(Self::Stream, SocketAddr)> {
        let mut addr: sockaddr_storage = unsafe { std::mem::zeroed() };
        let mut addrlen: socklen_t = std::mem::size_of::<sockaddr_storage>() as socklen_t;

        let (tx, rx) = oneshot::channel();
        let op = UringOp {
            op_type: UringOpType::Accept {
                fd: self.fd,
                addr: Box::new(addr),
                addrlen: Box::new(addrlen),
            },
            tx,
        };

        self.engine.submit(op)?;
        let result = rx.await.map_err(|_| anyhow!("Operation cancelled"))??;

        if result < 0 {
            return Err(anyhow!("Accept failed: {}", result));
        }

        let client_fd = result;

        // Get peer address
        let peer_addr = unsafe {
            let mut peer_addr: sockaddr_storage = std::mem::zeroed();
            let mut peer_len = std::mem::size_of::<sockaddr_storage>() as socklen_t;
            libc::getpeername(client_fd, &mut peer_addr as *mut _ as *mut _, &mut peer_len);
            storage_to_socket_addr(&peer_addr)
        };

        Ok((
            UringStream {
                fd: client_fd,
                engine: self.engine.clone(),
                read_buf: None,
            },
            peer_addr,
        ))
    }

    fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self._listener.local_addr()?)
    }
}

pub struct UringStream {
    fd: RawFd,
    engine: UringNetEngine,
    read_buf: Option<BytesMut>,
}

impl AsRawFd for UringStream {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

#[async_trait]
impl TransportStream for UringStream {
    async fn read_buf(&mut self, buf: &mut BytesMut) -> Result<usize> {
        // Ensure we have capacity
        if buf.capacity() - buf.len() < 4096 {
            buf.reserve(4096);
        }

        let spare = buf.spare_capacity_mut();
        let ptr = spare.as_mut_ptr() as *mut u8;
        let len = spare.len();

        let (tx, rx) = oneshot::channel();
        let op = UringOp {
            op_type: UringOpType::Recv {
                fd: self.fd,
                buf: ptr,
                len,
            },
            tx,
        };

        self.engine.submit(op)?;
        let result = rx.await.map_err(|_| anyhow!("Operation cancelled"))??;

        if result < 0 {
            return Err(anyhow!("Recv failed: {}", result));
        }

        let n = result as usize;
        unsafe {
            buf.set_len(buf.len() + n);
        }

        Ok(n)
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        let mut written = 0;
        while written < buf.len() {
            let (tx, rx) = oneshot::channel();
            let op = UringOp {
                op_type: UringOpType::Send {
                    fd: self.fd,
                    buf: buf[written..].as_ptr(),
                    len: buf.len() - written,
                },
                tx,
            };

            self.engine.submit(op)?;
            let result = rx.await.map_err(|_| anyhow!("Operation cancelled"))??;

            if result < 0 {
                return Err(anyhow!("Send failed: {}", result));
            }

            written += result as usize;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        // io-uring operations are already async, no explicit flush needed
        Ok(())
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        let mut read = 0;
        while read < buf.len() {
            let (tx, rx) = oneshot::channel();
            let op = UringOp {
                op_type: UringOpType::Recv {
                    fd: self.fd,
                    buf: buf[read..].as_mut_ptr(),
                    len: buf.len() - read,
                },
                tx,
            };

            self.engine.submit(op)?;
            let result = rx.await.map_err(|_| anyhow!("Operation cancelled"))??;

            if result < 0 {
                return Err(anyhow!("Recv failed: {}", result));
            }

            if result == 0 {
                return Err(anyhow!("Connection closed"));
            }

            read += result as usize;
        }
        Ok(())
    }

    fn peer_addr(&self) -> Result<SocketAddr> {
        unsafe {
            let mut peer_addr: sockaddr_storage = std::mem::zeroed();
            let mut peer_len = std::mem::size_of::<sockaddr_storage>() as socklen_t;
            libc::getpeername(self.fd, &mut peer_addr as *mut _ as *mut _, &mut peer_len);
            Ok(storage_to_socket_addr(&peer_addr))
        }
    }

    fn set_keepalive(&self, keepalive: bool) -> Result<()> {
        let opt: libc::c_int = if keepalive { 1 } else { 0 };
        let ret = unsafe {
            libc::setsockopt(
                self.fd,
                libc::SOL_SOCKET,
                libc::SO_KEEPALIVE,
                &opt as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if ret < 0 {
            return Err(anyhow!("Failed to set keepalive"));
        }
        Ok(())
    }

    fn set_nodelay(&self, nodelay: bool) -> Result<()> {
        let opt: libc::c_int = if nodelay { 1 } else { 0 };
        let ret = unsafe {
            libc::setsockopt(
                self.fd,
                libc::IPPROTO_TCP,
                libc::TCP_NODELAY,
                &opt as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if ret < 0 {
            return Err(anyhow!("Failed to set nodelay"));
        }
        Ok(())
    }
}

impl Drop for UringStream {
    fn drop(&mut self) {
        let _ = self.engine.submit(UringOp {
            op_type: UringOpType::Close { fd: self.fd },
            tx: oneshot::channel().0,
        });
    }
}

// Helper functions
fn socket_addr_to_storage(addr: SocketAddr) -> (sockaddr_storage, socklen_t) {
    let mut storage: sockaddr_storage = unsafe { std::mem::zeroed() };
    let len = match addr {
        SocketAddr::V4(v4) => {
            let addr_in = sockaddr_in {
                sin_family: libc::AF_INET as sa_family_t,
                sin_port: v4.port().to_be(),
                sin_addr: unsafe { std::mem::transmute(v4.ip().octets()) },
                sin_zero: [0; 8],
            };
            unsafe {
                std::ptr::copy_nonoverlapping(
                    &addr_in as *const _ as *const u8,
                    &mut storage as *mut _ as *mut u8,
                    std::mem::size_of::<sockaddr_in>(),
                );
            }
            std::mem::size_of::<sockaddr_in>() as socklen_t
        }
        SocketAddr::V6(v6) => {
            let addr_in6 = sockaddr_in6 {
                sin6_family: libc::AF_INET6 as sa_family_t,
                sin6_port: v6.port().to_be(),
                sin6_flowinfo: v6.flowinfo(),
                sin6_addr: unsafe { std::mem::transmute(v6.ip().octets()) },
                sin6_scope_id: v6.scope_id(),
            };
            unsafe {
                std::ptr::copy_nonoverlapping(
                    &addr_in6 as *const _ as *const u8,
                    &mut storage as *mut _ as *mut u8,
                    std::mem::size_of::<sockaddr_in6>(),
                );
            }
            std::mem::size_of::<sockaddr_in6>() as socklen_t
        }
    };
    (storage, len)
}

unsafe fn storage_to_socket_addr(storage: &sockaddr_storage) -> SocketAddr {
    match storage.ss_family as i32 {
        libc::AF_INET => {
            let addr_in = &*(storage as *const _ as *const sockaddr_in);
            let ip = std::net::Ipv4Addr::from(u32::from_be(addr_in.sin_addr.s_addr as u32));
            let port = u16::from_be(addr_in.sin_port);
            SocketAddr::V4(std::net::SocketAddrV4::new(ip, port))
        }
        libc::AF_INET6 => {
            let addr_in6 = &*(storage as *const _ as *const sockaddr_in6);
            let ip = std::net::Ipv6Addr::from(addr_in6.sin6_addr.s6_addr);
            let port = u16::from_be(addr_in6.sin6_port);
            SocketAddr::V6(std::net::SocketAddrV6::new(
                ip,
                port,
                addr_in6.sin6_flowinfo,
                addr_in6.sin6_scope_id,
            ))
        }
        _ => panic!("Unknown address family"),
    }
}
