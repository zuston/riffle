// Copyright 2025 foyer Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! io-uring based URPC connection implementation.
//!
//! This module provides a high-performance URPC connection using io-uring
//! for asynchronous I/O operations on Linux.

use crate::error::WorkerError;
use crate::metric::URPC_REQUEST_PARSING_LATENCY;
use crate::urpc::frame::Frame;
use anyhow::Result;
use bytes::{Buf, BytesMut};
use io_uring::opcode::{Read, Write};
use io_uring::squeue::Entry;
use io_uring::types::Fd;
use io_uring::{IoUring, SubmissionQueue};
use std::io::{Cursor, IoSlice};
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{io, mem};

const INITIAL_BUFFER_LENGTH: usize = 1024 * 1024;

/// io-uring based connection for URPC.
///
/// This struct wraps a socket with an io-uring instance for high-performance
/// asynchronous I/O operations.
#[derive(Debug)]
pub struct UringConnection {
    socket: OwnedFd,
    ring: IoUring,
    read_buf: BytesMut,
    write_buf: BytesMut,
    peer_addr: SocketAddr,
}

impl UringConnection {
    /// Create a new io-uring connection from a socket.
    pub fn new(socket: std::net::TcpStream) -> Result<Self> {
        let fd = socket.as_raw_fd();
        let peer_addr = socket.peer_addr()?;

        // Set socket to non-blocking for proper io-uring operation
        unsafe {
            let flags = libc::fcntl(fd, libc::F_GETFL);
            libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }

        let ring = IoUring::builder().build(64)?;

        Ok(UringConnection {
            socket: unsafe { OwnedFd::from_raw_fd(fd) },
            ring,
            read_buf: BytesMut::with_capacity(INITIAL_BUFFER_LENGTH),
            write_buf: BytesMut::with_capacity(INITIAL_BUFFER_LENGTH),
            peer_addr,
        })
    }

    /// Get the peer's socket address.
    pub fn peer_addr(&self) -> Result<SocketAddr> {
        Ok(self.peer_addr)
    }

    /// Submit a read operation to the io-uring ring.
    fn submit_read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let fd = Fd(self.socket.as_raw_fd());
        let read_op = Read::new(fd, buf.as_mut_ptr(), buf.len() as u32);

        let mut queue = self.ring.submission();
        let mut sq = unsafe { Pin::new_unchecked(&mut queue) };

        let entry = read_op.build().into();
        unsafe { sq.push(entry) }.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // Wait for completion
        self.ring.submit_and_wait(1)?;

        // Get the result
        let cq = self.ring.completion();
        let result = unsafe { cq.pop() }
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "no completion"))?;
        Ok(result.result() as usize)
    }

    /// Submit a write operation to the io-uring ring.
    fn submit_write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let fd = Fd(self.socket.as_raw_fd());
        let write_op = Write::new(fd, buf.as_ptr(), buf.len() as u32);

        let mut queue = self.ring.submission();
        let mut sq = unsafe { Pin::new_unchecked(&mut queue) };

        let entry = write_op.build().into();
        unsafe { sq.push(entry) }.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // Wait for completion
        self.ring.submit_and_wait(1)?;

        // Get the result
        let cq = self.ring.completion();
        let result = unsafe { cq.pop() }
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "no completion"))?;
        Ok(result.result() as usize)
    }

    fn parse_frame(&mut self) -> Result<Option<Frame>> {
        let mut buf = Cursor::new(&self.read_buf[..]);

        match Frame::check(&mut buf) {
            Ok(_) => {
                let timer = std::time::Instant::now();
                let len = buf.position() as usize;
                buf.set_position(0);
                let frame = Frame::parse(&mut buf)?;
                self.read_buf.advance(len);
                URPC_REQUEST_PARSING_LATENCY
                    .with_label_values(&[&format!("{}", &frame)])
                    .observe(timer.elapsed().as_secs_f64());
                Ok(Some(frame))
            }
            Err(WorkerError::STREAM_INCOMPLETE) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Read data from the socket using io-uring.
    async fn read_from_socket(&mut self) -> Result<usize> {
        let mut temp_buf = vec![0u8; INITIAL_BUFFER_LENGTH];
        let n = self.submit_read(&mut temp_buf[..])?;
        if n > 0 {
            self.read_buf.extend_from_slice(&temp_buf[..n]);
        }
        Ok(n)
    }

    /// Write data to the socket using io-uring.
    async fn write_to_socket(&mut self, data: &[u8]) -> Result<()> {
        let n = self.submit_write(data)?;
        if n != data.len() {
            return Err(anyhow::anyhow!(
                "short write: {} of {} bytes",
                n,
                data.len()
            ));
        }
        Ok(())
    }

    /// Async read frame from the connection.
    pub async fn read_frame(&mut self) -> Result<Option<Frame>, WorkerError> {
        loop {
            // Attempt to parse a frame from the buffered data.
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // Read more data from the socket.
            let n = self.read_from_socket().await?;
            if n == 0 {
                // Connection closed
                if self.read_buf.is_empty() {
                    return Ok(None);
                } else {
                    return Err(WorkerError::STREAM_ABNORMAL);
                }
            }
        }
    }

    /// Async write frame to the connection.
    pub async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        Frame::write_async(self, frame, &mut self.write_buf).await?;
        self.flush().await?;
        Ok(())
    }

    /// Flush the write buffer to the socket.
    pub async fn flush(&mut self) -> Result<()> {
        if !self.write_buf.is_empty() {
            let data = self.write_buf.split();
            self.write_to_socket(&data[..]).await?;
        }
        Ok(())
    }
}

impl Drop for UringConnection {
    fn drop(&mut self) {
        // Ensure the socket is properly closed
        let _ = self.ring.submission();
        let _ = self.ring.completion();
    }
}

#[async_trait::async_trait]
impl crate::connection::UringCompatibleConnection for UringConnection {
    async fn read_frame(&mut self) -> Result<Option<Frame>, WorkerError> {
        self.read_frame().await
    }

    async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        self.write_frame(frame).await
    }

    fn peer_addr(&self) -> Result<SocketAddr> {
        Ok(self.peer_addr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_creation() {
        // This test requires a valid socket to be meaningful
        // Skipped in unit tests
    }
}
