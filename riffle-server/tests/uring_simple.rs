//! Minimal io_uring accept sanity checks (listen fd lifetime).
//!
//! **How to run** (integration test binary name is `uring_simple`):
//! - `cargo test -p riffle-server --features io-uring --test uring_simple`
//! - Do **not** rely on `cargo test uring_simple` alone: without `--test uring_simple`, Cargo treats
//!   `uring_simple` as a filter on **test function names**, and these tests would be filtered out
//!   (0 tests run). You can also filter by name: `cargo test -p riffle-server --features io-uring uring_simple_fd`.

use std::io::Write;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::ptr;
use std::sync::mpsc;
use std::sync::Mutex;
use std::time::Duration;

/// Serialize tests: parallel runs can reuse the same small fd numbers across threads, so one test's
/// stale fd may alias another test's live listener (false accept success / stolen connection).
static URING_SIMPLE_TEST_LOCK: Mutex<()> = Mutex::new(());

use io_uring::{opcode, types::Fd, IoUring};
use libc::{c_int, EAGAIN, EINTR, SOCK_CLOEXEC, SOCK_NONBLOCK};

fn would_block(res: i32) -> bool {
    if res >= 0 {
        return false;
    }
    -res == EAGAIN as i32 || -res == libc::EWOULDBLOCK as i32
}

enum OpType {
    Accept { fd: c_int },
}

unsafe impl Send for OpType {}

struct Op {
    op_type: OpType,
    tx: Option<mpsc::Sender<std::io::Result<i32>>>,
}

fn uring_worker_loop(rx: mpsc::Receiver<Op>) {
    let mut uring = IoUring::new(1024).expect("Failed to create io-uring");

    while let Ok(mut op) = rx.recv() {
        let OpType::Accept { fd } = &op.op_type;
        let sqe = opcode::Accept::new(Fd(*fd), ptr::null_mut(), ptr::null_mut())
            .flags(SOCK_NONBLOCK | SOCK_CLOEXEC)
            .build()
            .user_data(0);

        {
            let mut sq = uring.submission();
            unsafe {
                sq.push(&sqe).expect("SQ full");
            }
        }

        let mut res = None;
        while res.is_none() {
            match uring.submit_and_wait(1) {
                Err(e) if e.raw_os_error() == Some(EINTR as i32) => continue,
                Err(e) => panic!("submit_and_wait failed: {e}"),
                Ok(_) => {}
            }

            let cqes: Vec<_> = uring.completion().collect();
            for cqe in cqes {
                if cqe.user_data() != 0 {
                    continue;
                }
                let r = cqe.result();
                if would_block(r) {
                    let sqe = opcode::Accept::new(Fd(*fd), ptr::null_mut(), ptr::null_mut())
                        .flags(SOCK_NONBLOCK | SOCK_CLOEXEC)
                        .build()
                        .user_data(0);
                    let mut sq = uring.submission();
                    unsafe {
                        sq.push(&sqe).expect("SQ full");
                    }
                    break;
                } else {
                    res = Some(r);
                }
            }
        }

        if let Some(r) = res {
            if let Some(tx) = op.tx.take() {
                let _ = tx.send(Ok(r));
            }
        }
    }
}

/// Reproduce EXACTLY what UringTransport::bind does.
/// Test 1: WITHOUT keeping listener alive (fd becomes invalid).
#[test]
fn uring_simple_fd_invalid_after_drop() {
    let _guard = URING_SIMPLE_TEST_LOCK
        .lock()
        .expect("uring_simple test lock poisoned");

    println!("=== uring_simple_fd_invalid_after_drop ===");

    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let std_listener = TcpListener::bind(addr).expect("bind failed");
    let fd = std_listener.as_raw_fd();
    let local_addr = std_listener.local_addr().expect("local_addr");
    std_listener
        .set_nonblocking(true)
        .expect("set_nonblocking failed");
    let ret = unsafe { libc::listen(fd, 128) };
    assert_eq!(ret, 0, "listen failed");
    println!("listener fd={} on {}", fd, local_addr);

    // BUG: dropping listener makes fd invalid!
    drop(std_listener);
    println!("std_listener dropped - fd {} is now INVALID", fd);

    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        uring_worker_loop(rx);
    });

    let (accept_tx, accept_rx) = mpsc::channel();
    tx.send(Op {
        op_type: OpType::Accept { fd },
        tx: Some(accept_tx),
    })
    .expect("send failed");

    // Accept must not return a live client fd; kernel returns negative errno in CQE (e.g. EBADF,
    // ENOTSOCK) or we may block until timeout if the worker misbehaves.
    let res = accept_rx.recv_timeout(Duration::from_secs(5));
    println!("accept result: {:?}", res);
    match res {
        Err(_) => {}
        Ok(Ok(fd)) => assert!(
            fd < 0,
            "accept on invalid listen fd must not return a non-negative client fd, got {}",
            fd
        ),
        Ok(Err(_)) => {}
    }
    println!("CONFIRMED: fd becomes invalid after TcpListener is dropped!");
}

/// Test 2: WITH listener kept alive (fd stays valid).
#[test]
fn uring_simple_fd_valid_with_listener() {
    let _guard = URING_SIMPLE_TEST_LOCK
        .lock()
        .expect("uring_simple test lock poisoned");

    println!("=== uring_simple_fd_valid_with_listener ===");

    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let std_listener = TcpListener::bind(addr).expect("bind failed");
    let fd = std_listener.as_raw_fd();
    let local_addr = std_listener.local_addr().expect("local_addr");
    std_listener
        .set_nonblocking(true)
        .expect("set_nonblocking failed");
    let ret = unsafe { libc::listen(fd, 128) };
    assert_eq!(ret, 0);
    println!("listener fd={} on {}", fd, local_addr);

    // KEEP listener alive so fd stays valid!
    let _listener_kept = std_listener; // <-- THE FIX!
    println!("std_listener kept alive - fd {} stays VALID", fd);

    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        uring_worker_loop(rx);
    });

    // Client connects.
    let client_handle = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(50));
        let mut c = TcpStream::connect(local_addr).expect("client connect failed");
        println!("client connected to {}", local_addr);
        let n = c.write(b"hello\n").expect("client write failed");
        println!("client sent {} bytes", n);
    });

    let (accept_tx, accept_rx) = mpsc::channel();
    tx.send(Op {
        op_type: OpType::Accept { fd },
        tx: Some(accept_tx),
    })
    .expect("send failed");

    let res = accept_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("accept timeout")
        .expect("accept failed");
    println!("accept succeeded! fd={}", res);
    assert!(res >= 0, "accept failed: {}", res);

    client_handle.join().expect("client panicked");
    println!("CONFIRMED: fd stays valid while TcpListener is kept alive!");
}
