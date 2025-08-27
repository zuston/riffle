use crate::config::ReadAheadConfig;
use crate::error::WorkerError;
use crate::metric::{
    READ_AHEAD_ACTIVE_TASKS, READ_AHEAD_BYTES, READ_AHEAD_HITS, READ_AHEAD_MISSES,
    READ_AHEAD_OPERATIONS, READ_AHEAD_OPERATION_DURATION, READ_AHEAD_WASTED_BYTES,
    READ_WITHOUT_AHEAD_DURATION, READ_WITH_AHEAD_HIT_DURATION, READ_WITH_AHEAD_MISS_DURATION,
};
use crate::store::local::layers::{Handler, Layer};
use crate::store::local::options::{CreateOptions, WriteOptions};
use crate::store::local::read_options::{ReadOptions, ReadRange};
use crate::store::local::{FileStat, LocalIO};
use crate::store::DataBytes;
use crate::system_libc::read_ahead;
use crate::util;
use async_trait::async_trait;
use clap::builder::Str;
use dashmap::DashMap;
use libc::abs;
use log::{debug, info, warn};
use parking_lot::Mutex;
use std::fs::File;
use std::sync::Arc;
use tokio::time::Instant;

pub struct ReadAheadLayer {
    root: String,
    options: ReadAheadConfig,
}

impl ReadAheadLayer {
    pub fn new(root: &str, options: &ReadAheadConfig) -> Self {
        Self {
            root: root.to_owned(),
            options: options.clone(),
        }
    }
}

impl Layer for ReadAheadLayer {
    fn wrap(&self, handler: Handler) -> Handler {
        let options = &self.options;
        Arc::new(Box::new(ReadAheadLayerWrapper {
            handler,
            root: self.root.to_owned(),
            load_tasks: Default::default(),
            ahead_batch_size: util::parse_raw_to_bytesize(options.batch_size.as_str()) as usize,
            ahead_batch_number: options.batch_number,
        }))
    }
}

#[derive(Clone)]
struct ReadAheadLayerWrapper {
    handler: Handler,
    root: String,

    load_tasks: DashMap<String, Option<ReadAheadTask>>,

    ahead_batch_size: usize,
    ahead_batch_number: usize,
}

unsafe impl Send for ReadAheadLayerWrapper {}
unsafe impl Sync for ReadAheadLayerWrapper {}

#[async_trait]
impl LocalIO for ReadAheadLayerWrapper {
    async fn create(&self, path: &str, options: CreateOptions) -> anyhow::Result<(), WorkerError> {
        self.handler.create(path, options).await
    }

    async fn write(&self, path: &str, options: WriteOptions) -> anyhow::Result<(), WorkerError> {
        self.handler.write(path, options).await
    }

    async fn read(
        &self,
        path: &str,
        options: ReadOptions,
    ) -> anyhow::Result<DataBytes, WorkerError> {
        match options.read_range {
            ReadRange::ALL => self.handler.read(&path, options).await,
            ReadRange::RANGE(off, len) => {
                let timer = Instant::now();
                if options.sequential {
                    let abs_path = format!("{}/{}", &self.root, path);
                    let load_task = self
                        .load_tasks
                        .entry(path.to_owned())
                        .or_insert_with(|| {
                            match ReadAheadTask::new(
                                &abs_path,
                                self.ahead_batch_size,
                                self.ahead_batch_number,
                            ) {
                                Ok(task) => {
                                    READ_AHEAD_ACTIVE_TASKS.inc();
                                    Some(task)
                                }
                                Err(_) => None,
                            }
                        })
                        .clone();

                    let mut hit = false;
                    if let Some(task) = load_task {
                        hit = task.load(off, len).await?;
                    }

                    let result = self.handler.read(&path, options).await;
                    // only record non-raw io duration
                    if let Ok(data) = &result {
                        match data {
                            DataBytes::RawIO(_) => {
                                // ignore
                            }
                            _ => {
                                let duration = timer.elapsed().as_secs_f64();
                                if hit {
                                    READ_AHEAD_HITS.inc();
                                    READ_WITH_AHEAD_HIT_DURATION.observe(duration);
                                } else {
                                    READ_AHEAD_MISSES.inc();
                                    READ_WITH_AHEAD_MISS_DURATION.observe(duration);
                                }
                            }
                        }
                    }
                    result
                } else {
                    let result = self.handler.read(&path, options).await;
                    if let Ok(data) = &result {
                        match data {
                            DataBytes::RawIO(_) => {}
                            _ => {
                                let duration = timer.elapsed().as_secs_f64();
                                READ_WITHOUT_AHEAD_DURATION.observe(duration);
                            }
                        }
                    }
                    result
                }
            }
        }
    }

    async fn delete(&self, path: &str) -> anyhow::Result<(), WorkerError> {
        let timer = Instant::now();
        let normalize_path = if !path.ends_with("/") {
            format!("{}/", path)
        } else {
            path.to_owned()
        };
        let mut deletion_keys = vec![];
        let view = self.load_tasks.clone().into_read_only();
        for (k, v) in view.iter() {
            if k.starts_with(normalize_path.as_str()) {
                deletion_keys.push(k.clone());
            }
        }
        let deleted_count = deletion_keys.len();
        for deletion_key in deletion_keys {
            self.load_tasks.remove(&deletion_key);
        }

        READ_AHEAD_ACTIVE_TASKS.sub(deleted_count as i64);

        info!(
            "Deletion cache with prefix:{} cost {} ms",
            normalize_path,
            timer.elapsed().as_millis()
        );

        self.handler.delete(path).await
    }

    async fn file_stat(&self, path: &str) -> anyhow::Result<FileStat, WorkerError> {
        self.handler.file_stat(path).await
    }
}

#[derive(Clone)]
struct ReadAheadTask {
    inner: Arc<tokio::sync::Mutex<Inner>>,
}

struct Inner {
    absolute_path: String,
    file: File,

    is_initialized: bool,
    load_start_offset: u64,
    load_length: u64,

    batch_size: usize,
    batch_number: usize,
}

impl ReadAheadTask {
    fn new(abs_path: &str, batch_size: usize, batch_number: usize) -> anyhow::Result<Self> {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(abs_path)?;
        Ok(Self {
            inner: Arc::new(tokio::sync::Mutex::new(Inner {
                absolute_path: abs_path.to_string(),
                file,
                is_initialized: false,
                load_start_offset: 0,
                load_length: 0,
                batch_size,
                batch_number,
            })),
        })
    }

    async fn do_read_ahead(&self, inner: &Inner, off: u64, len: u64) {
        let _timer = READ_AHEAD_OPERATION_DURATION.start_timer();

        debug!(
            "Read ahead: {} with offset: {}, length: {}",
            inner.absolute_path, off, len
        );

        READ_AHEAD_OPERATIONS.inc();
        READ_AHEAD_BYTES.inc_by(len);

        if let Err(e) = read_ahead(&inner.file, off as i64, len as i64) {
            warn!(
                "Errors on reading ahead: {} with offset: {}, length: {}",
                &inner.absolute_path, off, len
            );
        }
    }

    async fn load(&self, off: u64, len: u64) -> anyhow::Result<bool> {
        let mut inner = self.inner.lock().await;
        if !inner.is_initialized && off == 0 {
            let load_len = (inner.batch_number * inner.batch_size) as u64;
            self.do_read_ahead(&inner, 0, load_len).await;
            inner.is_initialized = true;
            inner.load_length = load_len;
            return Ok(true);
        }

        let diff = inner.load_length - off;
        let next_load_bytes = 2 * inner.batch_size as u64;
        if diff > 0 && diff < next_load_bytes {
            let load_len = next_load_bytes;
            self.do_read_ahead(
                &inner,
                inner.load_start_offset + inner.load_length,
                load_len,
            )
            .await;
            inner.load_length += load_len;
            return Ok(true);
        }

        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::WorkerError;
    use crate::store::local::read_options::{ReadOptions, ReadRange};
    use crate::store::local::FileStat;
    use crate::store::local::{CreateOptions, LocalIO, WriteOptions};
    use crate::store::DataBytes;
    use async_trait::async_trait;
    use bytes::Bytes;
    use log::SetLoggerError;
    use std::io::Write;
    use std::path::Path;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    fn init_logger() -> Result<(), SetLoggerError> {
        env_logger::builder().is_test(true).try_init()
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn test_read_ahead() {
        let _logger = init_logger();

        // Prepare temp file path and content
        let mut temp_file = NamedTempFile::new().expect("create temp file");
        let content = b"hello riffle read ahead";
        temp_file.write_all(content).unwrap();
        let temp_path = temp_file.path().to_str().unwrap().to_string();
        let path = Path::new(&temp_path);
        let root = path.parent().unwrap().to_str().unwrap().to_string();
        let file_name = path.file_name().unwrap().to_str().unwrap().to_string();

        let option = ReadAheadConfig::default();
        let layer = ReadAheadLayer::new(root.as_str(), &option);
        let inner_handler: Arc<Box<dyn LocalIO>> = Arc::new(Box::new(MockHandler::new()));
        let wrapped = ReadAheadLayerWrapper {
            handler: inner_handler,
            root: root.to_owned(),
            load_tasks: Default::default(),
            ahead_batch_size: util::parse_raw_to_bytesize(option.batch_size.as_str()) as usize,
            ahead_batch_number: option.batch_number,
        };

        // 1st read ahead
        let options = ReadOptions::default()
            .with_buffer_io()
            .with_read_range(ReadRange::RANGE(0, 5));
        let result = wrapped.read(file_name.as_str(), options).await;
        assert!(result.is_ok());

        wrapped.delete(root.as_str()).await.unwrap();
        assert_eq!(0, wrapped.load_tasks.len());
    }

    struct MockHandler;

    impl MockHandler {
        fn new() -> Self {
            MockHandler
        }
    }

    #[async_trait]
    impl LocalIO for MockHandler {
        async fn create(
            &self,
            _path: &str,
            _options: CreateOptions,
        ) -> anyhow::Result<(), WorkerError> {
            Ok(())
        }

        async fn write(
            &self,
            _path: &str,
            _options: WriteOptions,
        ) -> anyhow::Result<(), WorkerError> {
            Ok(())
        }

        async fn read(
            &self,
            _path: &str,
            _options: ReadOptions,
        ) -> anyhow::Result<DataBytes, WorkerError> {
            Ok(DataBytes::Direct(Bytes::from(b"mock".to_vec())))
        }

        async fn delete(&self, _path: &str) -> anyhow::Result<(), WorkerError> {
            Ok(())
        }

        async fn file_stat(&self, _path: &str) -> anyhow::Result<FileStat, WorkerError> {
            Ok(FileStat { content_length: 0 })
        }
    }
}
