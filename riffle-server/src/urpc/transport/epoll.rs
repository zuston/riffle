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
use anyhow::Result;
use async_trait::async_trait;
use bytes::BytesMut;
use socket2::SockRef;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd as StdAsRawFd, RawFd};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Epoll-based transport using tokio's TcpStream
pub struct EpollTransport;

#[async_trait]
impl Transport for EpollTransport {
    type Listener = EpollListener;
    type Stream = EpollStream;

    async fn bind(addr: SocketAddr) -> Result<Self::Listener> {
        let listener = TcpListener::bind(addr).await?;
        Ok(EpollListener { inner: listener })
    }

    async fn connect(addr: SocketAddr) -> Result<Self::Stream> {
        let stream = TcpStream::connect(addr).await?;
        Ok(EpollStream { inner: stream })
    }
}

pub struct EpollListener {
    inner: TcpListener,
}

impl EpollListener {
    /// Create an EpollListener from an existing TcpListener
    pub fn from_tcp_listener(listener: TcpListener) -> Self {
        Self { inner: listener }
    }

    /// Get the underlying TcpListener
    pub fn inner_ref(&self) -> &TcpListener {
        &self.inner
    }
}

#[async_trait]
impl TransportListener for EpollListener {
    type Stream = EpollStream;

    async fn accept(&self) -> Result<(Self::Stream, SocketAddr)> {
        let (stream, addr) = self.inner.accept().await?;
        Ok((EpollStream { inner: stream }, addr))
    }

    fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.inner.local_addr()?)
    }
}

pub struct EpollStream {
    inner: TcpStream,
}

impl AsRawFd for EpollStream {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

#[async_trait]
impl TransportStream for EpollStream {
    async fn read_buf(&mut self, buf: &mut BytesMut) -> Result<usize> {
        let n = self.inner.read_buf(buf).await?;
        Ok(n)
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        self.inner.write_all(buf).await?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.inner.flush().await?;
        Ok(())
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.inner.read_exact(buf).await?;
        Ok(())
    }

    fn peer_addr(&self) -> Result<SocketAddr> {
        Ok(self.inner.peer_addr()?)
    }

    fn set_keepalive(&self, keepalive: bool) -> Result<()> {
        let sock_ref = SockRef::from(&self.inner);
        sock_ref.set_keepalive(keepalive)?;
        Ok(())
    }

    fn set_nodelay(&self, nodelay: bool) -> Result<()> {
        let sock_ref = SockRef::from(&self.inner);
        sock_ref.set_nodelay(nodelay)?;
        Ok(())
    }
}

impl EpollStream {
    /// Get the underlying TcpStream (for compatibility with existing code)
    pub fn into_inner(self) -> TcpStream {
        self.inner
    }

    /// Get a reference to the underlying TcpStream
    pub fn inner_ref(&self) -> &TcpStream {
        &self.inner
    }

    /// Create an EpollStream from a TcpStream
    pub fn from_tcp_stream(stream: TcpStream) -> Self {
        Self { inner: stream }
    }
}
