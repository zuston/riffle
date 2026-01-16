use crate::raw_pipe::RawPipe;
use crate::urpc::frame::Frame;
use anyhow::{anyhow, Result};
use log::warn;
use std::os::fd::AsRawFd;
use tokio::io::Interest;
use tokio::net::TcpStream;

pub type CInt = std::ffi::c_int;

#[cfg(target_os = "linux")]
fn _send_file_linux(
    fd_in: i32,
    fd_out: i32,
    off: Option<&mut i64>,
    len: usize,
) -> Result<CInt, std::io::Error> {
    let off = match off {
        Some(v) => v as *mut _,
        None => std::ptr::null_mut(),
    };
    let res = unsafe { libc::sendfile(fd_out, fd_in, off, len as libc::size_t) };
    if res == -1 {
        let err = std::io::Error::last_os_error();
        return Err(err);
    }
    Ok(res as CInt)
}

#[cfg(not(target_os = "linux"))]
fn _send_file_linux(
    _fd_in: i32,
    _fd_out: i32,
    _off: Option<&mut i64>,
    _len: usize,
) -> Result<CInt, std::io::Error> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Other,
        "sendfile is only supported on Linux",
    ))
}

#[cfg(target_os = "macos")]
fn _send_file_macos(
    fd_in: i32,
    fd_out: i32,
    off: Option<&mut i64>,
    len: usize,
) -> Result<CInt, std::io::Error> {
    use libc::{c_int, off_t};
    use std::ptr;
    let mut offset: off_t = off.map(|v| *v as off_t).unwrap_or(0);
    let mut len64: off_t = len as off_t;
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
        if let Some(raw) = err.raw_os_error() {
            if raw == libc::EAGAIN {
                return Err(err);
            }
        }
        return Err(err);
    }
    Ok(len64 as CInt)
}

pub fn send_file(
    fd_in: i32,
    fd_out: i32,
    off: Option<&mut i64>,
    len: usize,
) -> Result<CInt, std::io::Error> {
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
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "sendfile is only supported on Linux and macOS",
        ))
    }
}

#[cfg(not(target_os = "linux"))]
pub async fn splice(io_out: &mut TcpStream, pipe: &RawPipe) -> Result<()> {
    Err(anyhow!("splice is only supported on Linux"))
}

#[cfg(target_os = "linux")]
pub async fn splice(io_out: &mut TcpStream, pipe: &RawPipe) -> Result<()> {
    use libc::{splice, SPLICE_F_MORE, SPLICE_F_MOVE};
    use std::io;

    let pipe_out_fd = pipe.pipe_out_fd.as_raw_fd();
    let sock_fd = io_out.as_raw_fd();

    let mut remaining = pipe.length;

    while remaining > 0 {
        let res = io_out
            .async_io(Interest::WRITABLE, || {
                let ret = unsafe {
                    splice(
                        pipe_out_fd,
                        std::ptr::null_mut(),
                        sock_fd,
                        std::ptr::null_mut(),
                        remaining,
                        SPLICE_F_MOVE | SPLICE_F_MORE,
                    )
                };

                if ret == -1 {
                    let err = io::Error::last_os_error();
                    return Err(err);
                }

                Ok(ret as usize)
            })
            .await;

        match res {
            Ok(0) => {
                return Err(anyhow!("splice returned 0 bytes (pipe EOF)"));
            }
            Ok(n) => {
                remaining -= n;
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                continue;
            }
            Err(e) => {
                return Err(anyhow!(e));
            }
        }
    }
    Ok(())
}

pub async fn send_file_full(
    io_out: &mut TcpStream,
    fd_in: i32,
    mut off: Option<i64>,
    len: usize,
) -> Result<()> {
    use std::io;
    let fd_out = io_out.as_raw_fd();
    let mut remaining = len;
    while remaining > 0 {
        let res = io_out
            .async_io(Interest::WRITABLE, || {
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
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                warn!("sendfile error: {}", e);
                continue;
            }
            Err(e) => return Err(anyhow!(e)),
        }
    }

    Ok(())
}

pub fn read_ahead(file: &std::fs::File, off: i64, len: i64) -> Result<()> {
    #[cfg(not(target_os = "linux"))]
    {
        Ok(())
    }

    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        unsafe {
            let fd = file.as_raw_fd();
            let res = libc::posix_fadvise(
                fd,
                off as libc::off_t,
                len as libc::off_t,
                libc::POSIX_FADV_WILLNEED,
            );
            if res != 0 {
                return Err(std::io::Error::from_raw_os_error(res).into());
            }
            Ok(())
        }
    }
}

#[cfg(test)]
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod tests {
    use crate::runtime::manager::create_runtime;
    use crate::store::DataBytes;

    use super::*;
    use bytes::buf;
    use libc::{shutdown, SHUT_WR};
    use std::fs::{File, OpenOptions};
    use std::io::Read;
    use std::io::Seek;
    use std::io::Write;
    use std::os::fd::AsRawFd;
    use tempfile::tempfile;
    use tokio::io::AsyncReadExt;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::task;

    fn generate_14mb_string() -> String {
        const MB14: usize = 14 * 1024 * 1024;
        let pattern = "ABCD";
        let repeat_count = MB14 / pattern.len();
        let remainder = MB14 % pattern.len();

        let mut s = pattern.repeat(repeat_count);
        s.push_str(&pattern[..remainder]);
        s
    }

    // only test splice on linux
    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    #[test]
    fn test_splice() -> anyhow::Result<()> {
        use rand::Rng;

        const FILE_SIZE: usize = 150 * 1024 * 1024;
        const CHUNK_SIZE: usize = 15 * 1024 * 1024;
        const NUM_CHUNKS: usize = FILE_SIZE / CHUNK_SIZE;

        let w_runtime = create_runtime(2, "w");

        let listener =
            w_runtime.block_on(async { TcpListener::bind("127.0.0.1:0").await.unwrap() });
        let addr = listener.local_addr().unwrap();

        let server = w_runtime.spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            socket.read_to_end(&mut buf).await.unwrap();
            buf
        });

        let mut stream = w_runtime.block_on(async { TcpStream::connect(addr).await.unwrap() });

        use crate::store::local::uring_io::tests::read_with_splice;

        let write_data = generate_14mb_string();

        println!("validating with direct read...");
        let result = read_with_splice(write_data.to_owned())?;
        match result {
            DataBytes::RawPipe(raw_pipe) => {
                assert!(raw_pipe.length == write_data.len());
                println!("Have read into the pipe");
                w_runtime.block_on(async { splice(&mut stream, &raw_pipe).await })?;
                let ret = unsafe { shutdown(stream.as_raw_fd(), SHUT_WR) };
                assert_eq!(ret, 0, "shutdown failed");
            }
            _ => panic!("Expected raw pipe bytes"),
        };

        let accepted = w_runtime.block_on(async { server.await })?;
        assert_eq!(accepted.as_slice(), write_data.as_bytes());

        Ok(())
    }

    #[tokio::test]
    async fn test_send_file_full_linux() {
        use rand::Rng;

        const FILE_SIZE: usize = 150 * 1024 * 1024;
        const CHUNK_SIZE: usize = 15 * 1024 * 1024;
        const NUM_CHUNKS: usize = FILE_SIZE / CHUNK_SIZE;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = task::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            socket.read_to_end(&mut buf).await.unwrap();
            buf
        });

        let mut input = tempfile().expect("failed to create temp input");
        // Generate 150MB of random data and write to file
        let mut file_data = vec![0u8; FILE_SIZE];
        rand::thread_rng().fill(&mut file_data[..]);
        input.write_all(&file_data).expect("write failed");
        input.flush().unwrap();

        let mut stream = TcpStream::connect(addr).await.unwrap();

        // Generate random chunk offsets for both sending and verifying
        let mut rng = rand::thread_rng();
        let mut offsets = Vec::with_capacity(NUM_CHUNKS);
        for _ in 0..NUM_CHUNKS {
            let offset = rng.gen_range(0..=(FILE_SIZE - CHUNK_SIZE));
            offsets.push(offset);
        }

        // Send each chunk using send_file_full
        for &offset in &offsets {
            send_file_full(
                &mut stream,
                input.as_raw_fd(),
                Some(offset as i64),
                CHUNK_SIZE,
            )
            .await
            .expect("send_file_full failed");
        }

        let ret = unsafe { shutdown(stream.as_raw_fd(), SHUT_WR) };
        assert_eq!(ret, 0, "shutdown failed");

        let received = server.await.unwrap();

        // Construct expected data by reading the same random chunks from file_data
        let mut expected = Vec::with_capacity(CHUNK_SIZE * NUM_CHUNKS);
        for &offset in &offsets {
            expected.extend_from_slice(&file_data[offset..offset + CHUNK_SIZE]);
        }

        assert_eq!(received, expected);
    }
}