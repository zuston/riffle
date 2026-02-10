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

use anyhow::Result;
use async_trait::async_trait;
use bytes::BytesMut;
use std::net::SocketAddr;

pub mod epoll;

#[cfg(all(feature = "io-uring", target_os = "linux"))]
pub mod uring;

/// Transport trait for abstracting different I/O backends (epoll, io-uring)
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    type Listener: TransportListener;
    type Stream: TransportStream;

    /// Create a listener bound to the given address
    async fn bind(addr: SocketAddr) -> Result<Self::Listener>;

    /// Connect to a remote address
    async fn connect(addr: SocketAddr) -> Result<Self::Stream>;
}

/// Transport listener trait for accepting incoming connections
#[async_trait]
pub trait TransportListener: Send + Sync + 'static {
    type Stream: TransportStream;

    /// Accept an incoming connection
    async fn accept(&self) -> Result<(Self::Stream, SocketAddr)>;

    /// Get the local address
    fn local_addr(&self) -> Result<SocketAddr>;
}

/// Trait for getting raw file descriptor from a stream
/// This is needed for sendfile/splice operations
pub trait AsRawFd {
    fn as_raw_fd(&self) -> std::os::fd::RawFd;
}

/// Transport stream trait for reading/writing data
#[async_trait]
pub trait TransportStream: Send + Sync + Unpin + AsRawFd + 'static {
    /// Read data into the buffer, returning the number of bytes read
    async fn read_buf(&mut self, buf: &mut BytesMut) -> Result<usize>;

    /// Write all data from the buffer
    async fn write_all(&mut self, buf: &[u8]) -> Result<()>;

    /// Flush the stream
    async fn flush(&mut self) -> Result<()>;

    /// Read exact number of bytes into the buffer
    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<()>;

    /// Get the peer address
    fn peer_addr(&self) -> Result<SocketAddr>;

    /// Set TCP keepalive
    fn set_keepalive(&self, keepalive: bool) -> Result<()>;

    /// Set TCP nodelay
    fn set_nodelay(&self, nodelay: bool) -> Result<()>;
}
