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

use crate::error::WorkerError;
use crate::store::local::options::{CreateOptions, WriteOptions};
use crate::store::local::read_options::ReadOptions;
use crate::store::local::sync_io::SyncLocalIO;
use crate::store::local::{FileStat, LocalIO};
use crate::store::DataBytes;
use anyhow::anyhow;
use clap::builder::Str;
use core_affinity::CoreId;
use io_uring::types::Fd;
use io_uring::{opcode, squeue, IoUring};
use std::fs::OpenOptions;
use std::io::{Bytes, IoSlice};
use std::os::fd::AsRawFd;
use std::path::Path;
use std::{
    fmt::Debug,
    fs,
    sync::{mpsc, Arc},
};
use tokio::sync::oneshot;

/// Builder for io_uring based I/O engine.
#[derive(Debug)]
pub struct UringIoEngineBuilder {
    threads: usize,
    cpus: Vec<u32>,
    sqpoll: bool,
    sqpoll_cpus: Vec<u32>,
    sqpoll_idle: u32,
    iopoll: bool,

    io_depth: usize,
    weight: f64,
}

impl Default for UringIoEngineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl UringIoEngineBuilder {
    /// Create a new io_uring based I/O engine builder with default configurations.
    pub fn new() -> Self {
        Self {
            threads: 1,
            cpus: vec![],
            sqpoll: false,
            sqpoll_cpus: vec![],
            sqpoll_idle: 10,
            iopoll: false,
            io_depth: 64,
            weight: 1.0,
        }
    }

    /// Set the number of threads to use for the I/O engine.
    pub fn with_threads(mut self, threads: usize) -> Self {
        self.threads = threads;
        self
    }

    /// Bind the engine threads to specific CPUs.
    ///
    /// The length of `cpus` must be equal to the threads.
    pub fn with_cpus(mut self, cpus: Vec<u32>) -> Self {
        self.cpus = cpus;
        self
    }

    /// Enable or disable I/O polling.
    ///
    /// FYI:
    ///
    /// - [io_uring_setup(2)](https://man7.org/linux/man-pages/man2/io_uring_setup.2.html)
    /// - [crate - io-uring](https://docs.rs/io-uring/latest/io_uring/struct.Builder.html#method.setup_iopoll)
    ///
    /// Related syscall flag: `IORING_SETUP_IOPOLL`.
    ///
    /// NOTE:
    ///
    /// - If this feature is enabled, the underlying device MUST be opened with the `O_DIRECT` flag.
    /// - If this feature is enabled, the underlying device MUST support io polling.
    ///
    /// Default: `false`.
    pub fn with_iopoll(mut self, iopoll: bool) -> Self {
        self.iopoll = iopoll;
        self
    }

    /// Enable or disable SQ polling.
    ///
    /// FYI:
    ///
    /// - [io_uring_setup(2)](https://man7.org/linux/man-pages/man2/io_uring_setup.2.html)
    /// - [crate - io-uring](https://docs.rs/io-uring/latest/io_uring/struct.Builder.html#method.setup_sqpoll)
    ///
    /// Related syscall flag: `IORING_SETUP_IOPOLL`.
    ///
    /// NOTE: If this feature is enabled, the underlying device must be opened with the `O_DIRECT` flag.
    ///
    /// Default: `false`.
    pub fn with_sqpoll(mut self, sqpoll: bool) -> Self {
        self.sqpoll = sqpoll;
        self
    }

    /// Bind the kernel’s SQ poll thread to the specified cpu.
    ///
    /// This flag is only meaningful when [`Self::with_sqpoll`] is enabled.
    ///
    /// The length of `cpus` must be equal to the number of threads.
    pub fn with_sqpoll_cpus(mut self, cpus: Vec<u32>) -> Self {
        self.sqpoll_cpus = cpus;
        self
    }

    /// After idle milliseconds, the kernel thread will go to sleep and you will have to wake it up again with a system
    /// call.
    ///
    /// This flag is only meaningful when [`Self::with_sqpoll`] is enabled.
    pub fn with_sqpoll_idle(mut self, idle: u32) -> Self {
        self.sqpoll_idle = idle;
        self
    }

    pub fn build(self, sync_io: SyncLocalIO) -> anyhow::Result<UringIo, WorkerError> {
        if self.threads == 0 {
            return Err(WorkerError::Other(anyhow!(
                "io uring thread number must be greater than 0"
            )));
        }

        let (read_txs, read_rxs): (Vec<mpsc::SyncSender<_>>, Vec<mpsc::Receiver<_>>) = (0..self
            .threads)
            .map(|_| {
                let (tx, rx) = mpsc::sync_channel(4096);
                (tx, rx)
            })
            .unzip();

        let (write_txs, write_rxs): (Vec<mpsc::SyncSender<_>>, Vec<mpsc::Receiver<_>>) = (0..self
            .threads)
            .map(|_| {
                let (tx, rx) = mpsc::sync_channel(4096);
                (tx, rx)
            })
            .unzip();

        for (i, (read_rx, write_rx)) in read_rxs.into_iter().zip(write_rxs.into_iter()).enumerate()
        {
            let mut builder = IoUring::builder();
            if self.iopoll {
                builder.setup_iopoll();
            }
            if self.sqpoll {
                builder.setup_sqpoll(self.sqpoll_idle);
                if !self.sqpoll_cpus.is_empty() {
                    let cpu = self.sqpoll_cpus[i];
                    builder.setup_sqpoll_cpu(cpu);
                }
            }
            let cpu = if self.cpus.is_empty() {
                None
            } else {
                Some(self.cpus[i])
            };
            let uring = builder.build(self.io_depth as _)?;
            let shard = UringIoEngineShard {
                read_rx,
                write_rx,
                uring,
                io_depth: self.io_depth,
                weight: self.weight,
                read_inflight: 0,
                write_inflight: 0,
            };

            std::thread::Builder::new()
                .name(format!("riffle-uring-{i}"))
                .spawn(move || {
                    if let Some(cpu) = cpu {
                        core_affinity::set_for_current(CoreId { id: cpu as _ });
                    }
                    shard.run();
                })?;
        }

        let engine = UringIo {
            root: sync_io.inner.root.clone(),
            read_txs: Arc::new(read_txs),
            write_txs: Arc::new(write_txs),
            sync_local_io: sync_io,
        };
        Ok(engine)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UringIoType {
    Read,
    Write,
    WriteV,
}

#[cfg(any(target_family = "unix", target_family = "wasm"))]
pub struct RawFile(pub std::os::fd::RawFd);

struct RawFileAddress {
    file: RawFile,
    offset: u64,
}

struct UringIoCtx {
    tx: oneshot::Sender<anyhow::Result<(), WorkerError>>,
    io_type: UringIoType,
    addr: RawFileAddress,
    bufs: Vec<bytes::Bytes>,
}

struct UringIoEngineShard {
    read_rx: mpsc::Receiver<UringIoCtx>,
    write_rx: mpsc::Receiver<UringIoCtx>,
    weight: f64,
    uring: IoUring,
    io_depth: usize,
    read_inflight: usize,
    write_inflight: usize,
}

impl UringIoEngineShard {
    fn run(mut self) {
        loop {
            'prepare: loop {
                if self.read_inflight + self.write_inflight >= self.io_depth {
                    break 'prepare;
                }

                let ctx = if (self.read_inflight as f64) < self.write_inflight as f64 * self.weight
                {
                    match self.read_rx.try_recv() {
                        Err(mpsc::TryRecvError::Disconnected) => return,
                        Ok(ctx) => Some(ctx),
                        Err(mpsc::TryRecvError::Empty) => match self.write_rx.try_recv() {
                            Err(mpsc::TryRecvError::Disconnected) => return,
                            Ok(ctx) => Some(ctx),
                            Err(mpsc::TryRecvError::Empty) => None,
                        },
                    }
                } else {
                    match self.write_rx.try_recv() {
                        Err(mpsc::TryRecvError::Disconnected) => return,
                        Ok(ctx) => Some(ctx),
                        Err(mpsc::TryRecvError::Empty) => match self.read_rx.try_recv() {
                            Err(mpsc::TryRecvError::Disconnected) => return,
                            Ok(ctx) => Some(ctx),
                            Err(mpsc::TryRecvError::Empty) => None,
                        },
                    }
                };

                let ctx = match ctx {
                    Some(ctx) => ctx,
                    None => break 'prepare,
                };

                let ctx = Box::new(ctx);

                let fd = Fd(ctx.addr.file.0);
                let sqe = match ctx.io_type {
                    UringIoType::Read => {
                        self.read_inflight += 1;
                        // ensure only one buf
                        opcode::Read::new(fd, ctx.bufs[0].as_mut_ptr(), ctx.bufs[0].len() as _)
                            .offset(ctx.addr.offset)
                            .build()
                    }
                    UringIoType::Write => {
                        self.write_inflight += 1;
                        // ensure only one buf
                        opcode::Write::new(fd, ctx.bufs[0].as_mut_ptr(), ctx.bufs[0].len() as _)
                            .offset(ctx.addr.offset)
                            .build()
                    }
                    UringIoType::WriteV => {
                        // https://github.com/tokio-rs/io-uring/blob/master/io-uring-test/src/utils.rs#L95
                        let slices = ctx.bufs.iter().map(|x| IoSlice::new(x)).collect();
                        opcode::Writev::new(fd, slices.as_ptr(), slices.len() as _)
                            .offset(ctx.addr.offset)
                            .build()
                            .flags(squeue::Flags::IO_LINK)
                    }
                };
                let data = Box::into_raw(ctx) as u64;
                let sqe = sqe.user_data(data);
                unsafe { self.uring.submission().push(&sqe).unwrap() }
            }

            if self.read_inflight + self.write_inflight > 0 {
                self.uring.submit().unwrap();
            }

            for cqe in self.uring.completion() {
                let data = cqe.user_data();
                let ctx = unsafe { Box::from_raw(data as *mut UringIoCtx) };

                match ctx.io_type {
                    UringIoType::Read => self.read_inflight -= 1,
                    UringIoType::Write | UringIoType::WriteV => self.write_inflight -= 1,
                }

                let res = cqe.result();
                if res < 0 {
                    let _ = ctx.tx.send(Err(WorkerError::RAW_IO_ERR(res)));
                } else {
                    let _ = ctx.tx.send(Ok(()));
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct UringIo {
    root: String,

    read_txs: Arc<Vec<mpsc::SyncSender<UringIoCtx>>>,
    write_txs: Arc<Vec<mpsc::SyncSender<UringIoCtx>>>,

    sync_local_io: SyncLocalIO,
}

impl UringIo {
    fn with_root(&self, path: &str) -> String {
        format!("{}/{}", self.root, path)
    }
}

#[async_trait::async_trait]
impl LocalIO for UringIo {
    async fn create(&self, path: &str, options: CreateOptions) -> anyhow::Result<(), WorkerError> {
        self.sync_local_io.create(path, options).await
    }

    async fn write(&self, path: &str, options: WriteOptions) -> anyhow::Result<(), WorkerError> {
        let (tx, rx) = oneshot::channel();
        let tag = options.data.len();
        let shard = &self.write_txs[tag % self.write_txs.len()];
        let bufs = options.data.always_bytes();

        let path = self.with_root(path);
        let path = Path::new(&path);
        let mut file = OpenOptions::new().append(true).create(true).open(path)?;
        let raw_fd = file.as_raw_fd();

        let ctx = UringIoCtx {
            tx,
            io_type: UringIoType::WriteV,
            addr: RawFileAddress {
                file: RawFile(raw_fd),
                offset: options.offset.unwrap_or(0),
            },
            bufs,
        };
        let _ = shard.send(ctx);
        let res = match rx.await {
            Ok(res) => res,
            Err(e) => Err(WorkerError::Other(anyhow::Error::from(e))),
        }?;
        Ok(())
    }

    async fn read(
        &self,
        path: &str,
        options: ReadOptions,
    ) -> anyhow::Result<DataBytes, WorkerError> {
        todo!()
    }

    async fn delete(&self, path: &str) -> anyhow::Result<(), WorkerError> {
        self.sync_local_io.delete(path).await
    }

    async fn file_stat(&self, path: &str) -> anyhow::Result<FileStat, WorkerError> {
        self.sync_local_io.file_stat(path).await
    }
}
