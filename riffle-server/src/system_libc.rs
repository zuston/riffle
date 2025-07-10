use crate::urpc::frame::Frame;
use anyhow::anyhow;
use anyhow::Result;
use std::os::fd::AsRawFd;
use tokio::io::Interest;
use tokio::net::TcpStream;

pub type CInt = std::ffi::c_int;

// Linux sendfile function to send the number of files to the network.
// C function prototype:
//pub unsafe extern "C" fn sendfile(
//     out_fd: c_int,
//     in_fd: c_int,
//     offset: *mut off_t,
//     count: size_t
// ) -> ssize_t
pub fn send_file(
    fd_in: i32,
    fd_out: i32,
    off: Option<&mut i64>,
    len: usize,
) -> anyhow::Result<CInt> {
    #[cfg(not(target_os = "linux"))]
    {
        Err(anyhow!("unsupported operation"))
    }

    #[cfg(target_os = "linux")]
    {
        let off = match off {
            Some(v) => v as *mut _,
            None => std::ptr::null_mut(),
        };
        let res = unsafe { libc::sendfile(fd_out, fd_in, off, len as libc::size_t) };
        Ok(res)
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
                    return Err(anyhow!("send_file return 0"));
                }
                remaining -= transferred as usize;
            }

            Err(e)
                if e.downcast_ref::<std::io::Error>()
                    .map_or(false, |ioe| ioe.kind() == std::io::ErrorKind::WouldBlock) =>
            {
                continue;
            }

            Err(e) => return Err(e),
        }
    }

    Ok(())
}
