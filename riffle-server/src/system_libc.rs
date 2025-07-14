use crate::urpc::frame::Frame;
use anyhow::{anyhow, Result};
use log::warn;
use std::os::fd::AsRawFd;
use tokio::io::Interest;
use tokio::net::TcpStream;

pub type CInt = std::ffi::c_int;

#[cfg(target_os = "linux")]
fn _send_file_linux(fd_in: i32, fd_out: i32, off: Option<&mut i64>, len: usize) -> Result<CInt> {
    let off = match off {
        Some(v) => v as *mut _,
        None => std::ptr::null_mut(),
    };
    let res = unsafe { libc::sendfile(fd_out, fd_in, off, len as libc::size_t) };
    Ok(res.try_into().unwrap())
}

#[cfg(not(target_os = "linux"))]
fn _send_file_linux(
    _fd_in: i32,
    _fd_out: i32,
    _off: Option<&mut i64>,
    _len: usize,
) -> Result<CInt> {
    Err(anyhow!("sendfile is only supported on Linux"))
}

#[cfg(target_os = "macos")]
fn _send_file_macos(fd_in: i32, fd_out: i32, off: Option<&mut i64>, len: usize) -> Result<CInt> {
    use libc::{c_int, off_t};
    use std::ptr;
    let mut offset: off_t = off.map(|v| *v as off_t).unwrap_or(0);
    let mut len64: off_t = len as off_t;
    // macOS sendfile: int sendfile(int fd, int s, off_t offset, off_t *len, struct sf_hdtr *hdtr, int flags);
    let ret = unsafe {
        libc::sendfile(
            fd_in,
            fd_out,
            offset,
            &mut len64 as *mut off_t,
            ptr::null_mut(),
            0,
        )
    };
    if ret == -1 {
        let err = std::io::Error::last_os_error();
        return Err(anyhow!("sendfile error: {}", err));
    }
    Ok(len64 as CInt)
}

pub fn send_file(fd_in: i32, fd_out: i32, off: Option<&mut i64>, len: usize) -> Result<CInt> {
    #[cfg(target_os = "linux")]
    {
        _send_file_linux(fd_in, fd_out, off, len)
    }
    #[cfg(target_os = "macos")]
    {
        _send_file_macos(fd_in, fd_out, off, len)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        Err(anyhow!("sendfile is only supported on Linux and macOS"))
    }
}

pub async fn send_file_full(
    io_out: &mut TcpStream,
    fd_in: i32,
    mut off: Option<i64>,
    len: usize,
) -> Result<()> {
    let fd_out = io_out.as_raw_fd();
    let mut remaining = len;
    while remaining > 0 {
        let res = Frame::async_io(io_out, Interest::WRITABLE, || {
            send_file(fd_in, fd_out, off.as_mut(), remaining)
        })
        .await;

        match res {
            Ok(transferred) => {
                if transferred == 0 {
                    return Err(anyhow!("send_file returned 0 bytes (possibly EOF)"));
                }
                remaining -= transferred as usize;
            }

            Err(e)
                if e.downcast_ref::<std::io::Error>()
                    .map_or(false, |ioe| ioe.kind() == std::io::ErrorKind::WouldBlock) =>
            {
                warn!("sendfile error: {}", e);
                continue;
            }

            Err(e) => return Err(e),
        }
    }

    Ok(())
}

#[cfg(test)]
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod tests {
    use super::*;
    use libc::{shutdown, SHUT_WR};
    use std::fs::{File, OpenOptions};
    use std::io::Seek;
    use std::io::Write;
    use std::os::fd::AsRawFd;
    use tempfile::tempfile;
    use tokio::io::AsyncReadExt;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::task;

    #[tokio::test]
    async fn test_send_file_full_linux() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = task::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            socket.read_to_end(&mut buf).await.unwrap();
            buf
        });

        let mut input = tempfile().expect("failed to create temp input");
        let data = b"Async sendfile data!";
        input.write_all(data).expect("write failed");
        input.flush().unwrap();
        input.rewind().unwrap();

        let mut stream = TcpStream::connect(addr).await.unwrap();
        send_file_full(&mut stream, input.as_raw_fd(), None, data.len())
            .await
            .expect("send_file_full failed");

        let ret = unsafe { shutdown(stream.as_raw_fd(), SHUT_WR) };
        assert_eq!(ret, 0, "shutdown failed");

        let received = server.await.unwrap();
        assert_eq!(&received, data);
    }
}
