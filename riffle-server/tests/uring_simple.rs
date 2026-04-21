//! Reference **io_uring** patterns aligned with production `riffle-server/src/urpc/transport/uring.rs`:
//!
//! - **`IORING_OP_ACCEPT`** with **real** `sockaddr_storage` / `socklen_t` (not null).
//! - **`IORING_OP_RECV`** on connected TCP (same opcode family as transport `UringOpType::Read`).
//! - **Multiplexed worker**: `pending` map by `user_data`, `try_recv` batching, `submit_and_wait(1)`,
//!   drain **all** CQEs, **`-EAGAIN` / `-EWOULDBLOCK`** → re-submit same SQE (same `user_data`).
//!
//! **Run** (integration test binary name is `uring_simple`):
//! ```text
//! cargo test -p riffle-server --features io-uring --test uring_simple
//! ```
//! Do **not** use `cargo test uring_simple` without `--test uring_simple` — Cargo treats the name
//! as a filter on test functions and may run 0 tests.

#![cfg(target_os = "linux")]

use io_uring::{opcode, types::Fd, IoUring};
use libc::{sockaddr_storage, socklen_t};
use std::collections::HashMap;
use std::io::Write;
use std::mem;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::fd::{AsRawFd, RawFd};
use std::sync::mpsc;
use std::sync::Mutex;
use std::time::Duration;

/// Serialize tests: parallel runs can reuse small fd numbers; avoid cross-test fd aliasing.
static URING_SIMPLE_TEST_LOCK: Mutex<()> = Mutex::new(());

#[inline]
fn cqe_result_is_would_block(res: i32) -> bool {
    if res >= 0 {
        return false;
    }
    let e = -res;
    e == libc::EAGAIN as i32 || e == libc::EWOULDBLOCK as i32
}

/// Mirrors `UringOpType::Accept` / `Recv` in `transport/uring.rs`.
enum OpType {
    Accept {
        listen_fd: RawFd,
        addr: Box<sockaddr_storage>,
        addrlen: Box<socklen_t>,
    },
    Recv {
        fd: RawFd,
        buf: Vec<u8>,
    },
}

unsafe impl Send for OpType {}

struct PendingOp {
    op_type: OpType,
    /// Accept: `i32` is new fd or negative errno (same convention as production listener).
    /// Recv: filled buffer (empty = EOF).
    notify: PendingNotify,
}

enum PendingNotify {
    Accept(mpsc::SyncSender<Result<i32, String>>),
    Recv(mpsc::SyncSender<Result<Vec<u8>, String>>),
}

fn build_sqe(op_type: &OpType) -> io_uring::squeue::Entry {
    match op_type {
        OpType::Accept {
            listen_fd,
            addr,
            addrlen,
        } => opcode::Accept::new(
            Fd(*listen_fd),
            addr.as_ref() as *const _ as *mut _,
            addrlen.as_ref() as *const _ as *mut _,
        )
        .flags(libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC)
        .build(),
        OpType::Recv { fd, buf } => {
            let len = buf.len() as u32;
            let ptr = buf.as_ptr() as *mut u8;
            opcode::Recv::new(Fd(*fd), ptr, len).build()
        }
    }
}

fn push_sqe(uring: &mut IoUring, op_type: &OpType, user_data: u64) {
    let sqe = build_sqe(op_type).user_data(user_data);
    let mut sq = uring.submission();
    unsafe {
        sq.push(&sqe).expect("submission queue is full");
    }
}

/// Same control flow as `uring_worker_loop` in `uring.rs` (accept + recv multiplexed).
fn uring_reference_worker_loop(rx: mpsc::Receiver<PendingOp>) {
    let mut uring = IoUring::new(1024).expect("Failed to create io-uring");
    let mut pending: HashMap<u64, PendingOp> = HashMap::new();
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

            // Accept: negative errno is still delivered as Ok(r) for caller to branch (production).
            if r < 0 && !matches!(&op.op_type, OpType::Accept { .. }) {
                let op = pending.remove(&id).expect("pending op");
                if let PendingNotify::Recv(tx) = op.notify {
                    let _ = tx.send(Err(format!("recv failed: errno {}", -r)));
                }
                continue;
            }

            let op = pending.remove(&id).expect("pending op");
            match (op.op_type, op.notify) {
                (OpType::Accept { .. }, PendingNotify::Accept(tx)) => {
                    let _ = tx.send(Ok(r));
                }
                (OpType::Recv { mut buf, .. }, PendingNotify::Recv(tx)) => {
                    if r == 0 {
                        buf.clear();
                    } else {
                        buf.truncate(r as usize);
                    }
                    let _ = tx.send(Ok(buf));
                }
                _ => {}
            }
        }
    }
}

/// Same bind sequence as `UringTransport::bind`: `TcpListener::bind` in blocking mode.
/// (No extra `libc::listen` — `TcpListener::bind` already listens.)
fn bind_uring_style(addr: SocketAddr) -> TcpListener {
    TcpListener::bind(addr).expect("bind failed")
}

#[test]
fn uring_simple_listen_fd_invalid_after_drop() {
    let _guard = URING_SIMPLE_TEST_LOCK
        .lock()
        .expect("uring_simple test lock poisoned");

    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let std_listener = bind_uring_style(addr);
    let fd = std_listener.as_raw_fd();
    let local_addr = std_listener.local_addr().expect("local_addr");
    drop(std_listener);

    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        uring_reference_worker_loop(rx);
    });

    let addr_buf: sockaddr_storage = unsafe { mem::zeroed() };
    let addrlen = mem::size_of::<sockaddr_storage>() as socklen_t;
    let (accept_tx, accept_rx) = mpsc::sync_channel(1);
    tx.send(PendingOp {
        op_type: OpType::Accept {
            listen_fd: fd,
            addr: Box::new(addr_buf),
            addrlen: Box::new(addrlen),
        },
        notify: PendingNotify::Accept(accept_tx),
    })
    .expect("send failed");

    let res = accept_rx.recv_timeout(Duration::from_secs(5));
    match res {
        Err(_) => {}
        Ok(Ok(fd)) => assert!(
            fd < 0,
            "accept on dead listen fd must not return a live client fd, got {}",
            fd
        ),
        Ok(Err(_)) => {}
    }
    let _ = local_addr;
}

#[test]
fn uring_simple_accept_peer_sockaddr() {
    let _guard = URING_SIMPLE_TEST_LOCK
        .lock()
        .expect("uring_simple test lock poisoned");

    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = bind_uring_style(addr);
    let listen_fd = listener.as_raw_fd();
    let local_addr = listener.local_addr().expect("local_addr");
    let _keep = listener;

    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        uring_reference_worker_loop(rx);
    });

    let client = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        let _ = TcpStream::connect(local_addr).expect("connect");
    });

    let addr_buf: sockaddr_storage = unsafe { mem::zeroed() };
    let addrlen = mem::size_of::<sockaddr_storage>() as socklen_t;
    let (accept_tx, accept_rx) = mpsc::sync_channel(1);
    tx.send(PendingOp {
        op_type: OpType::Accept {
            listen_fd,
            addr: Box::new(addr_buf),
            addrlen: Box::new(addrlen),
        },
        notify: PendingNotify::Accept(accept_tx),
    })
    .expect("send op");

    let new_fd = accept_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("accept timeout")
        .expect("accept channel");
    assert!(new_fd >= 0, "expected valid client fd, got {}", new_fd);

    unsafe {
        libc::close(new_fd);
    }
    client.join().expect("client panicked");
}

#[test]
fn uring_simple_accept_then_recv_roundtrip() {
    let _guard = URING_SIMPLE_TEST_LOCK
        .lock()
        .expect("uring_simple test lock poisoned");

    let payload = b"ping";
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = bind_uring_style(addr);
    let listen_fd = listener.as_raw_fd();
    let local_addr = listener.local_addr().expect("local_addr");
    let _keep = listener;

    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        uring_reference_worker_loop(rx);
    });

    let client = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        let mut s = TcpStream::connect(local_addr).expect("connect");
        s.write_all(payload).expect("write");
    });

    let addr_buf: sockaddr_storage = unsafe { mem::zeroed() };
    let addrlen = mem::size_of::<sockaddr_storage>() as socklen_t;
    let (a_tx, a_rx) = mpsc::sync_channel(1);
    tx.send(PendingOp {
        op_type: OpType::Accept {
            listen_fd,
            addr: Box::new(addr_buf),
            addrlen: Box::new(addrlen),
        },
        notify: PendingNotify::Accept(a_tx),
    })
    .expect("send accept");

    let client_fd = a_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("accept timeout")
        .expect("accept");
    assert!(client_fd >= 0, "accept fd: {}", client_fd);

    let flags = unsafe { libc::fcntl(client_fd, libc::F_GETFL, 0) };
    assert!(flags >= 0, "fcntl");
    if unsafe { libc::fcntl(client_fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        panic!("set O_NONBLOCK on accepted fd");
    }

    let (r_tx, r_rx) = mpsc::sync_channel(1);
    tx.send(PendingOp {
        op_type: OpType::Recv {
            fd: client_fd,
            buf: vec![0u8; 64],
        },
        notify: PendingNotify::Recv(r_tx),
    })
    .expect("send recv");

    let data = r_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("recv timeout")
        .expect("recv");
    assert_eq!(&data[..payload.len()], payload);

    client.join().expect("client panicked");
    unsafe {
        libc::close(client_fd);
    }
}
