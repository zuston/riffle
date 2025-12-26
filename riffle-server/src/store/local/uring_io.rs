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
use crate::raw_pipe::RawPipe;
use crate::store::local::options::{CreateOptions, WriteOptions};
use crate::store::local::read_options::{IoMode, ReadOptions, ReadRange};
use crate::store::local::sync_io::SyncLocalIO;
use crate::store::local::{FileStat, LocalIO};
use crate::store::DataBytes;
use anyhow::anyhow;
use bytes::BytesMut;
use clap::builder::Str;
use core_affinity::CoreId;
use io_uring::types::Fd;
use io_uring::{opcode, squeue, IoUring};
use libc::{fcntl, iovec, F_SETPIPE_SZ};
use std::fs::OpenOptions;
use std::io::{Bytes, IoSlice};
use std::os::fd::{AsRawFd, FromRawFd};
use std::path::Path;
use std::thread::sleep;
use std::time::Duration;
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

    pub fn with_io_depth(mut self, depth: usize) -> Self {
        self.io_depth = depth;
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
            root: sync_io.root().to_owned(),
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
    WriteV,
    // data zero-copy to socket for read
    Splice,
}

#[cfg(any(target_family = "unix", target_family = "wasm"))]
pub struct RawFile(pub std::os::fd::RawFd);

struct RawFileAddress {
    file: RawFile,
    offset: u64,
}

struct RawBuf {
    ptr: *mut u8,
    len: usize,
}

unsafe impl Send for RawBuf {}
unsafe impl Sync for RawBuf {}

struct UringIoCtx {
    tx: oneshot::Sender<anyhow::Result<usize, WorkerError>>,
    io_type: UringIoType,
    addr: RawFileAddress,

    w_bufs: Vec<bytes::Bytes>,
    w_iovecs: Vec<iovec>,

    r_bufs: Vec<RawBuf>,

    splice_pipe: Option<SplicePipe>,
}

struct SplicePipe {
    sin: i32,
    len: usize,
}

unsafe impl Send for UringIoCtx {}
unsafe impl Sync for UringIoCtx {}

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
                    None => {
                        // todo: we must sleep to avoid cpu busy, but maybe recv is better rather than try_recv
                        sleep(Duration::from_millis(5));
                        break 'prepare;
                    }
                };

                let mut ctx = Box::new(ctx);

                let fd = Fd(ctx.addr.file.0);
                let sqe = match ctx.io_type {
                    UringIoType::Read => {
                        self.read_inflight += 1;
                        // ensure only one buf
                        opcode::Read::new(fd, ctx.r_bufs[0].ptr, ctx.r_bufs[0].len as _)
                            .offset(ctx.addr.offset)
                            .build()
                    }
                    UringIoType::WriteV => {
                        self.write_inflight += 1;
                        // https://github.com/tokio-rs/io-uring/blob/master/io-uring-test/src/utils.rs#L95
                        opcode::Writev::new(
                            fd,
                            ctx.w_iovecs.as_ptr().cast(),
                            ctx.w_iovecs.len() as _,
                        )
                        .offset(ctx.addr.offset)
                        .build()
                        .into()
                    }
                    UringIoType::Splice => {
                        // refer: https://github.com/tokio-rs/io-uring/blob/7ec7ae909f7eabcf03450e6b858919449f135ac3/io-uring-test/src/tests/fs.rs#L1084
                        self.read_inflight += 1;
                        let pipe = ctx.splice_pipe.as_ref().unwrap();
                        let pipe_in = pipe.sin;
                        let len: u32 = pipe.len as u32;
                        opcode::Splice::new(
                            fd,
                            ctx.addr.offset as i64,
                            Fd(pipe_in),
                            -1, // pipe/socket destination must use -1
                            len,
                        )
                        .build()
                        .into()
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
                    UringIoType::WriteV => self.write_inflight -= 1,
                    UringIoType::Splice => self.read_inflight -= 1,
                }

                let res = cqe.result();
                if res < 0 {
                    let _ = ctx.tx.send(Err(WorkerError::RAW_IO_ERR(res)));
                } else {
                    let _ = ctx.tx.send(Ok(res as usize));
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
        let byte_size = options.data.len();
        let bufs = options.data.always_bytes();
        let buf_len = bufs.len();
        let slices = bufs
            .iter()
            .map(|x| IoSlice::new(x.as_ref()))
            .collect::<Vec<_>>();

        let path = self.with_root(path);
        let path = Path::new(&path);
        let mut file = OpenOptions::new().append(true).create(true).open(path)?;
        let raw_fd = file.as_raw_fd();

        let mut ctx = UringIoCtx {
            tx,
            io_type: UringIoType::WriteV,
            addr: RawFileAddress {
                file: RawFile(raw_fd),
                offset: options.offset.unwrap_or(0),
            },
            w_bufs: bufs,
            w_iovecs: Vec::with_capacity(buf_len),
            r_bufs: vec![],
            splice_pipe: None,
        };
        ctx.w_iovecs = ctx
            .w_bufs
            .iter()
            .map(|b| iovec {
                iov_base: b.as_ptr() as *mut _,
                iov_len: b.len(),
            })
            .collect();

        let _ = shard.send(ctx);
        let written_bytes = match rx.await {
            Ok(res) => res,
            Err(e) => Err(WorkerError::Other(anyhow::Error::from(e))),
        }?;
        if (byte_size != written_bytes) {
            return Err(WorkerError::Other(anyhow!(
                "Unexpected io write. expected/written: {}/{}",
                byte_size,
                written_bytes
            )));
        }
        Ok(())
    }

    async fn read(
        &self,
        path: &str,
        options: ReadOptions,
    ) -> anyhow::Result<DataBytes, WorkerError> {
        // if the io_mode is sendfile, we fallback to sync io read
        if matches!(options.io_mode, IoMode::SENDFILE) {
            // use sendfile via sync io
            return self.sync_local_io.read(path, options).await;
        }

        let (offset, length) = match &options.read_range {
            ReadRange::ALL => {
                let fs_sts = self.file_stat(path).await?;
                (0, fs_sts.content_length)
            }
            ReadRange::RANGE(x, y) => (*x, *y),
        };

        let (tx, rx) = oneshot::channel();
        let shard = &self.read_txs[options.task_id as usize % self.read_txs.len()];

        // open the file
        let path = self.with_root(path);
        let path = Path::new(&path);
        // must ensure the file lifecycle > raw_fd
        let file = OpenOptions::new().read(true).open(path)?;
        let raw_fd = file.as_raw_fd();

        // todo: make the size as the optional config option in io-uring
        if matches!(options.io_mode, IoMode::SPLICE) && length < 16 * 1024 * 1024 {
            // init the pipe
            let (pipe_in, mut pipe_out) = {
                let mut pipes = [0, 0];
                let ret = unsafe { libc::pipe(pipes.as_mut_ptr()) };
                if (ret != 0) {
                    return Err(WorkerError::Other(anyhow!(
                        "Failed to create pipe for splice: {}",
                        std::io::Error::last_os_error()
                    )));
                }
                let pipe_out = unsafe { std::fs::File::from_raw_fd(pipes[0]) };
                let pipe_in = unsafe { fs::File::from_raw_fd(pipes[1]) };
                (pipe_in, pipe_out)
            };

            use libc::fcntl;
            use libc::F_SETPIPE_SZ;
            unsafe {
                let pipe_size = 16 * 1024 * 1024;
                fcntl(pipe_in.as_raw_fd(), F_SETPIPE_SZ, pipe_size);
            }

            let ctx = UringIoCtx {
                tx,
                io_type: UringIoType::Splice,
                addr: RawFileAddress {
                    file: RawFile(raw_fd),
                    offset,
                },
                w_bufs: vec![],
                w_iovecs: vec![],
                r_bufs: vec![],
                splice_pipe: Some(SplicePipe {
                    sin: pipe_in.as_raw_fd(),
                    len: length as _,
                }),
            };

            let _ = shard.send(ctx);
            let _result = match rx.await {
                Ok(res) => res,
                Err(e) => {
                    return Err(WorkerError::Other(anyhow::Error::from(e)));
                }
            }?;

            return Ok(DataBytes::RawPipe(RawPipe::from(
                pipe_in,
                pipe_out,
                length as usize,
            )));
        }

        // init buf with BytesMut for io_uring to write into
        let mut buf = BytesMut::zeroed(length as _);

        let ctx = UringIoCtx {
            tx,
            io_type: UringIoType::Read,
            addr: RawFileAddress {
                file: RawFile(raw_fd),
                offset,
            },
            w_bufs: vec![],
            w_iovecs: vec![],
            r_bufs: vec![RawBuf {
                ptr: buf.as_mut_ptr(),
                len: length as usize,
            }],
            splice_pipe: None,
        };

        let _ = shard.send(ctx);
        let _result = match rx.await {
            Ok(res) => res,
            Err(e) => {
                return Err(WorkerError::Other(anyhow::Error::from(e)));
            }
        }?;

        Ok(DataBytes::Direct(buf.freeze()))
    }

    async fn delete(&self, path: &str) -> anyhow::Result<(), WorkerError> {
        self.sync_local_io.delete(path).await
    }

    async fn file_stat(&self, path: &str) -> anyhow::Result<FileStat, WorkerError> {
        self.sync_local_io.file_stat(path).await
    }
}

#[cfg(test)]
pub mod tests {
    use crate::runtime::manager::create_runtime;
    use crate::runtime::RuntimeRef;
    use crate::store::local::read_options::IoMode;
    use crate::store::local::sync_io::SyncLocalIO;
    use crate::store::local::uring_io::UringIoEngineBuilder;
    use crate::store::local::LocalIO;
    use crate::store::DataBytes;
    use crate::{composed_bytes::ComposedBytes, raw_pipe::RawPipe};
    use bytes::{BufMut, BytesMut};
    use log::info;

    #[test]
    fn test_uring_write_read() -> anyhow::Result<()> {
        use crate::store::DataBytes;
        use bytes::Bytes;
        use tempdir::TempDir;
        use tokio::runtime::Runtime;

        let temp_dir = TempDir::new("test_write_read")?;
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("init local file path: {}", temp_path);

        let r_runtime = create_runtime(1, "r");
        let w_runtime = create_runtime(2, "w");

        let sync_io_engine =
            SyncLocalIO::new(&r_runtime, &w_runtime, temp_path.as_str(), None, None);
        let uring_io_engine = UringIoEngineBuilder::new().build(sync_io_engine)?;

        // 1. write
        println!("writing...");
        let write_data_1 = b"hello io_uring test";
        let write_options = crate::store::local::options::WriteOptions {
            append: true,
            offset: None,
            data: DataBytes::Direct(Bytes::from(&write_data_1[..])),
        };

        w_runtime.block_on(async {
            uring_io_engine
                .write("test_file", write_options)
                .await
                .unwrap();
        });

        // 2. append with multi bytes
        println!("appending...");
        let write_data_2 = b"hello io_uring test";
        let write_data_3 = b"hello world";
        let write_options = crate::store::local::options::WriteOptions {
            append: true,
            offset: Some(write_data_1.len() as _),
            data: DataBytes::Composed(ComposedBytes::from(
                vec![
                    Bytes::from(&write_data_2[..]),
                    Bytes::from(&write_data_3[..]),
                ],
                write_data_2.len() + write_data_3.len(),
            )),
        };

        w_runtime.block_on(async {
            uring_io_engine
                .write("test_file", write_options)
                .await
                .unwrap();
        });

        // 2. read
        println!("reading...");
        let read_options = crate::store::local::read_options::ReadOptions {
            io_mode: IoMode::SENDFILE,
            task_id: 0,
            read_range: crate::store::local::read_options::ReadRange::ALL,
            ahead_options: None,
        };

        let result = r_runtime.block_on(async {
            uring_io_engine
                .read("test_file", read_options)
                .await
                .unwrap()
        });

        // 3. validation
        println!("validating...");

        let mut expected = BytesMut::new();
        expected.put(write_data_1.as_ref());
        expected.put(write_data_2.as_ref());
        expected.put(write_data_3.as_ref());

        match result {
            DataBytes::Direct(bytes) => {
                assert_eq!(bytes.as_ref(), expected.as_ref());
            }
            _ => panic!("Expected direct bytes"),
        }

        Ok(())
    }

    pub fn read_with_splice(content: String) -> anyhow::Result<DataBytes> {
        // create the data and then read with splice
        use crate::store::DataBytes;
        use bytes::Bytes;
        use std::io::Read;
        use tempdir::TempDir;
        let temp_dir = TempDir::new("test_read_splice")?;
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("init local file path: {}", temp_path);
        let r_runtime = create_runtime(1, "r");
        let w_runtime = create_runtime(2, "w");
        let sync_io_engine =
            SyncLocalIO::new(&r_runtime, &w_runtime, temp_path.as_str(), None, None);
        let uring_io_engine = UringIoEngineBuilder::new().build(sync_io_engine)?;
        // 1. write
        println!("writing...");
        let write_options = crate::store::local::options::WriteOptions {
            append: true,
            offset: Some(0),
            data: DataBytes::Direct(Bytes::from(content)),
        };
        w_runtime.block_on(async {
            uring_io_engine
                .write("test_file_splice", write_options)
                .await
                .unwrap();
        });
        // 2. read with splice
        println!("reading with splice...");
        let read_options = crate::store::local::read_options::ReadOptions {
            io_mode: IoMode::SPLICE,
            task_id: 0,
            read_range: crate::store::local::read_options::ReadRange::ALL,
            ahead_options: None,
        };
        let result = r_runtime.block_on(async {
            uring_io_engine
                .read("test_file_splice", read_options)
                .await
                .unwrap()
        });
        Ok(result)
    }

    #[test]
    fn test_read_with_splice() -> anyhow::Result<()> {
        use std::io::Read;

        // case1: check the raw-pipe read
        let write_data = "helloworld!!!!!!";
        println!("validating with direct read...");
        let result = read_with_splice(write_data.to_owned())?;
        match result {
            DataBytes::RawPipe(raw_pipe) => {
                assert!(raw_pipe.length == write_data.len());
                let mut read_buf = vec![0u8; raw_pipe.length];
                let mut fd = raw_pipe.pipe_out_fd;
                fd.read_exact(&mut read_buf)?;
                // to compare
                assert_eq!(read_buf.as_slice(), write_data.as_bytes());
            }
            _ => panic!("Expected raw pipe bytes"),
        }

        Ok(())
    }
}
