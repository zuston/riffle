use crate::config::ReadAheadConfig;
use crate::error::WorkerError;
use crate::metric::{
    READ_AHEAD_ACTIVE_TASKS, READ_AHEAD_BYTES, READ_AHEAD_HITS, READ_AHEAD_MISSES,
    READ_AHEAD_OPERATIONS, READ_AHEAD_OPERATION_DURATION, READ_AHEAD_OPERATION_FAILURE_COUNT,
    READ_AHEAD_WASTED_BYTES, READ_WITHOUT_AHEAD_DURATION, READ_WITH_AHEAD_DURATION,
    READ_WITH_AHEAD_HIT_DURATION, READ_WITH_AHEAD_MISS_DURATION,
};
use crate::runtime::RuntimeRef;
use crate::store::local::layers::{Handler, Layer};
use crate::store::local::options::{CreateOptions, WriteOptions};
use crate::store::local::read_options::{AheadOptions, ReadOptions, ReadRange};
use crate::store::local::{FileStat, LocalIO};
use crate::store::DataBytes;
use crate::system_libc::read_ahead;
use crate::urpc::command::ReadSegment;
use crate::util;
use async_trait::async_trait;
use clap::builder::Str;
use dashmap::DashMap;
use libc::abs;
use log::{debug, info, warn};
use parking_lot::Mutex;
use std::fs::File;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::Semaphore;
use tokio::time::Instant;

pub struct ReadAheadLayer {
    root: String,
    options: ReadAheadConfig,
    rt: RuntimeRef,
}

impl ReadAheadLayer {
    pub fn new(root: &str, options: &ReadAheadConfig, runtime_ref: RuntimeRef) -> Self {
        Self {
            root: root.to_owned(),
            options: options.clone(),
            rt: runtime_ref,
        }
    }
}

impl Layer for ReadAheadLayer {
    fn wrap(&self, handler: Handler) -> Handler {
        let options = &self.options;
        Arc::new(Box::new(ReadAheadLayerWrapper {
            handler,
            root: self.root.to_owned(),
            sequential_load_tasks: Default::default(),
            read_plan_load_tasks: Default::default(),
            read_plan_semaphore: Arc::new(Semaphore::new(options.read_plan_concurrency)),
            ahead_batch_size: util::parse_raw_to_bytesize(options.batch_size.as_str()) as usize,
            ahead_batch_number: options.batch_number,
            read_plan_runtime: self.rt.clone(),
        }))
    }
}

#[derive(Clone)]
struct ReadAheadLayerWrapper {
    handler: Handler,
    root: String,

    sequential_load_tasks: DashMap<String, Option<SequentialReadAheadTask>>,

    ahead_batch_size: usize,
    ahead_batch_number: usize,

    // key: (path, spark_task_attempt_id), value: ahead_task
    read_plan_load_tasks: DashMap<(String, i64), Option<ReadPlanReadAheadTask>>,
    read_plan_semaphore: Arc<Semaphore>,
    read_plan_runtime: RuntimeRef,
}

unsafe impl Send for ReadAheadLayerWrapper {}
unsafe impl Sync for ReadAheadLayerWrapper {}

impl ReadAheadLayerWrapper {
    fn is_sequential(ahead_options: &Option<AheadOptions>) -> bool {
        let mut seq = false;
        if let Some(ahead_options) = ahead_options {
            if ahead_options.sequential {
                seq = true;
            }
        };
        seq
    }

    fn is_read_plan(ahead_options: &Option<AheadOptions>) -> bool {
        let mut read_plan_mode = false;
        if let Some(ahead_options) = ahead_options {
            if !ahead_options.next_read_segments.is_empty() {
                read_plan_mode = true;
            }
        }
        read_plan_mode
    }

    async fn read_ahead_with_read_plan(
        &self,
        path: &str,
        options: ReadOptions,
    ) -> anyhow::Result<DataBytes, WorkerError> {
        let timer = Instant::now();

        let (off, len) = options.read_range.get_range();
        let ahead_options = options.ahead_options.as_ref().unwrap();
        let task_id = options.task_id;
        let load_task = self
            .read_plan_load_tasks
            .entry((path.to_owned(), task_id))
            .or_insert_with(|| {
                match ReadPlanReadAheadTask::new(
                    path,
                    &self.read_plan_runtime,
                    &self.read_plan_semaphore,
                ) {
                    Ok(task) => Some(task),
                    Err(_) => None,
                }
            })
            .clone();

        if let Some(load_task) = load_task {
            load_task.load(&ahead_options.next_read_segments, off);
        }

        let result = self.handler.read(&path, options).await;
        result
    }

    async fn read_ahead_with_sequential_mode(
        &self,
        path: &str,
        options: ReadOptions,
    ) -> anyhow::Result<DataBytes, WorkerError> {
        let timer = Instant::now();

        let (off, len) = options.read_range.get_range();
        let ahead_options = options.ahead_options.as_ref().unwrap();
        let abs_path = format!("{}/{}", &self.root, path);

        // if there is no user side specifying, fallback to system default setting.
        let batch_number = ahead_options
            .read_batch_number
            .unwrap_or(self.ahead_batch_number);
        let batch_size = ahead_options
            .read_batch_size
            .unwrap_or(self.ahead_batch_size);

        let load_task = self
            .sequential_load_tasks
            .entry(path.to_owned())
            .or_insert_with(|| {
                match SequentialReadAheadTask::new(&abs_path, batch_size, batch_number) {
                    Ok(task) => {
                        READ_AHEAD_ACTIVE_TASKS.inc();
                        Some(task)
                    }
                    Err(_) => None,
                }
            })
            .clone();

        let mut read_ahead_op_triggered = false;
        if let Some(task) = load_task {
            read_ahead_op_triggered = task.load(off, len).await?;
        }

        let result = self.handler.read(&path, options).await;

        let duration = timer.elapsed().as_secs_f64();
        READ_WITH_AHEAD_DURATION.observe(duration);

        // only record non-raw io duration
        if let Ok(data) = &result {
            if !matches!(data, DataBytes::RawIO(_)) {
                if read_ahead_op_triggered {
                    READ_AHEAD_MISSES.inc();
                    READ_WITH_AHEAD_MISS_DURATION.observe(duration);
                } else {
                    READ_AHEAD_HITS.inc();
                    READ_WITH_AHEAD_HIT_DURATION.observe(duration);
                }
            }
        }
        result
    }
}

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
                if Self::is_sequential(&options.ahead_options) {
                    self.read_ahead_with_sequential_mode(path, options).await
                } else {
                    if Self::is_read_plan(&options.ahead_options) {
                        self.read_ahead_with_read_plan(path, options).await
                    } else {
                        let timer = Instant::now();
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
    }

    async fn delete(&self, path: &str) -> anyhow::Result<(), WorkerError> {
        let timer = Instant::now();
        let normalize_path = if !path.ends_with("/") {
            format!("{}/", path)
        } else {
            path.to_owned()
        };
        let mut deletion_keys = vec![];
        let view = self.sequential_load_tasks.clone().into_read_only();
        for (k, v) in view.iter() {
            if k.starts_with(normalize_path.as_str()) {
                deletion_keys.push(k.clone());
            }
        }

        let deleted_count = deletion_keys.len();
        if deleted_count > 0 {
            for deletion_key in deletion_keys {
                self.sequential_load_tasks.remove(&deletion_key);
            }

            READ_AHEAD_ACTIVE_TASKS.sub(deleted_count as i64);

            info!(
                "Deleted {} load active tasks with prefix:{} cost {} ms",
                deleted_count,
                normalize_path,
                timer.elapsed().as_millis()
            );
        }

        self.handler.delete(path).await
    }

    async fn file_stat(&self, path: &str) -> anyhow::Result<FileStat, WorkerError> {
        self.handler.file_stat(path).await
    }
}

#[derive(Clone)]
struct ReadPlanReadAheadTask {
    file: Arc<Mutex<File>>,
    read_offset: Arc<AtomicU64>,
    sender: Arc<tokio::sync::mpsc::UnboundedSender<ReadSegment>>,
    path: Arc<String>,
}

impl ReadPlanReadAheadTask {
    fn new(abs_path: &str, rt: &RuntimeRef, semphore: &Arc<Semaphore>) -> anyhow::Result<Self> {
        let (send, recv) = tokio::sync::mpsc::unbounded_channel();
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(abs_path)?;
        let task = Self {
            file: Arc::new(Mutex::new(file)),
            read_offset: Arc::new(Default::default()),
            sender: Arc::new(send),
            path: Arc::new(abs_path.to_string()),
        };
        Self::loop_load(rt.clone(), task.clone(), recv, semphore.clone());
        Ok(task)
    }

    fn loop_load(
        rt: RuntimeRef,
        task: ReadPlanReadAheadTask,
        mut recv: UnboundedReceiver<ReadSegment>,
        limit: Arc<Semaphore>,
    ) {
        rt.spawn(async move {
            let limit = limit;
            while let Some(segment) = recv.recv().await {
                let off = segment.offset;
                let len = segment.length;

                if off < task.read_offset.load(Ordering::Relaxed) as i64 {
                    continue;
                }
                let _limit = limit.acquire().await.unwrap();
                let file = task.file.lock();
                do_read_ahead(&file, task.path.as_str(), off as u64, len as u64);
            }
        });
    }

    fn load(&self, next_segments: &Vec<ReadSegment>, now_read_off: u64) -> anyhow::Result<()> {
        self.read_offset.store(now_read_off, Ordering::SeqCst);
        for segment in next_segments {
            self.sender.send(segment.clone())?;
        }
        Ok(())
    }
}

fn do_read_ahead(file: &File, path: &str, off: u64, len: u64) {
    let _timer = READ_AHEAD_OPERATION_DURATION.start_timer();

    debug!("Read ahead: {} with offset: {}, length: {}", path, off, len);

    READ_AHEAD_OPERATIONS.inc();
    READ_AHEAD_BYTES.inc_by(len);

    if let Err(e) = read_ahead(file, off as i64, len as i64) {
        warn!(
            "Errors on reading ahead: {} with offset: {}, length: {}",
            path, off, len
        );
        READ_AHEAD_OPERATION_FAILURE_COUNT.inc();
    }
}

#[derive(Clone)]
struct SequentialReadAheadTask {
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

impl SequentialReadAheadTask {
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

    // the return value shows whether the load operation happens
    async fn load(&self, off: u64, len: u64) -> anyhow::Result<bool> {
        let mut inner = self.inner.lock().await;
        if !inner.is_initialized && off == 0 {
            let load_len = (inner.batch_number * inner.batch_size) as u64;
            do_read_ahead(&inner.file, inner.absolute_path.as_str(), 0, load_len);
            inner.is_initialized = true;
            inner.load_length = load_len;
            return Ok(true);
        }

        let diff = inner.load_length - off;
        let next_load_bytes = 2 * inner.batch_size as u64;
        if diff > 0 && diff < next_load_bytes {
            let load_len = next_load_bytes;
            do_read_ahead(
                &inner.file,
                inner.absolute_path.as_str(),
                inner.load_start_offset + inner.load_length,
                load_len,
            );
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
    use crate::runtime::manager::create_runtime;
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

        let runtime = create_runtime(1, "");
        let option = ReadAheadConfig::default();
        let inner_handler: Arc<Box<dyn LocalIO>> = Arc::new(Box::new(MockHandler::new()));
        let wrapped = ReadAheadLayerWrapper {
            handler: inner_handler,
            root: root.to_owned(),
            sequential_load_tasks: Default::default(),
            read_plan_load_tasks: Default::default(),
            read_plan_semaphore: Arc::new(Semaphore::new(10)),
            ahead_batch_size: util::parse_raw_to_bytesize(option.batch_size.as_str()) as usize,
            ahead_batch_number: option.batch_number,
            read_plan_runtime: runtime,
        };

        // 1st read ahead
        let options = ReadOptions::default()
            .with_buffer_io()
            .with_read_range(ReadRange::RANGE(0, 5));
        let result = wrapped.read(file_name.as_str(), options).await;
        assert!(result.is_ok());

        wrapped.delete(root.as_str()).await.unwrap();
        assert_eq!(0, wrapped.sequential_load_tasks.len());
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
