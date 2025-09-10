mod processor;

use crate::config::ReadAheadConfig;
use crate::error::WorkerError;
use crate::metric::{
    READ_AHEAD_ACTIVE_TASKS, READ_AHEAD_ACTIVE_TASKS_OF_READ_PLAN,
    READ_AHEAD_ACTIVE_TASKS_OF_SEQUENTIAL, READ_AHEAD_BYTES, READ_AHEAD_HITS, READ_AHEAD_MISSES,
    READ_AHEAD_OPERATIONS, READ_AHEAD_OPERATION_DURATION,
    READ_AHEAD_OPERATION_DURATION_OF_READ_PLAN, READ_AHEAD_OPERATION_DURATION_OF_SEQUENTIAL,
    READ_AHEAD_OPERATION_FAILURE_COUNT, READ_AHEAD_WASTED_BYTES, READ_WITHOUT_AHEAD_DURATION,
    READ_WITH_AHEAD_DURATION, READ_WITH_AHEAD_DURATION_OF_READ_PLAN,
    READ_WITH_AHEAD_DURATION_OF_SEQUENTIAL, READ_WITH_AHEAD_HIT_DURATION,
    READ_WITH_AHEAD_MISS_DURATION, TOTAL_READ_AHEAD_ACTIVE_TASKS,
};
use crate::runtime::manager::RuntimeManager;
use crate::runtime::RuntimeRef;
use crate::store::local::io_layer_read_ahead::processor::ReadPlanReadAheadTaskProcessor;
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
use futures::channel::oneshot::channel;
use libc::abs;
use log::{debug, error, info, warn};
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
    runtime_manager: RuntimeManager,
}

impl ReadAheadLayer {
    pub fn new(root: &str, options: &ReadAheadConfig, runtime_manager: RuntimeManager) -> Self {
        Self {
            root: root.to_owned(),
            options: options.clone(),
            runtime_manager,
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
            ahead_batch_size: util::parse_raw_to_bytesize(options.batch_size.as_str()) as usize,
            ahead_batch_number: options.batch_number,
            read_plan_task_id_inc: Default::default(),
            read_plan_enabled: options.read_plan_enable,
            read_plan_load_processor: ReadPlanReadAheadTaskProcessor::new(
                &self.runtime_manager,
                Arc::new(Semaphore::new(options.read_plan_concurrency)),
            ),
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
    read_plan_task_id_inc: Arc<AtomicU64>,
    read_plan_load_processor: ReadPlanReadAheadTaskProcessor,
    read_plan_load_tasks: DashMap<(String, i64), Option<ReadPlanReadAheadTask>>,
    read_plan_enabled: bool,
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

        let abs_path = format!("{}/{}", &self.root, path);
        let (off, len) = options.read_range.get_range();
        let ahead_options = options.ahead_options.as_ref().unwrap();
        let task_id = options.task_id;
        let load_task = self
            .read_plan_load_tasks
            .entry((path.to_owned(), task_id))
            .or_insert_with(|| {
                let uid = self.read_plan_task_id_inc.fetch_add(1, Ordering::SeqCst);
                let processor = &self.read_plan_load_processor;
                match ReadPlanReadAheadTask::new(abs_path.as_str(), uid, processor) {
                    Ok(task) => {
                        TOTAL_READ_AHEAD_ACTIVE_TASKS.inc();
                        READ_AHEAD_ACTIVE_TASKS.inc();
                        READ_AHEAD_ACTIVE_TASKS_OF_READ_PLAN.inc();
                        Some(task)
                    }
                    Err(e) => {
                        error!("Errors on initializing read-plan task. err: {}", e);
                        None
                    }
                }
            })
            .clone();

        if let Some(load_task) = load_task {
            load_task
                .load(&ahead_options.next_read_segments, off)
                .await?;
        }

        let result = self.handler.read(&path, options).await;

        let elapsed = timer.elapsed().as_secs_f64();
        READ_WITH_AHEAD_DURATION.observe(elapsed);
        READ_WITH_AHEAD_DURATION_OF_READ_PLAN.observe(elapsed);

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
                        TOTAL_READ_AHEAD_ACTIVE_TASKS.inc();
                        READ_AHEAD_ACTIVE_TASKS.inc();
                        READ_AHEAD_ACTIVE_TASKS_OF_SEQUENTIAL.inc();
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
        READ_WITH_AHEAD_DURATION_OF_SEQUENTIAL.observe(duration);

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

    fn delete_with_prefix<K, V, F, G>(
        map: &DashMap<K, Option<V>>,
        prefix: &str,
        key_to_str: F,
        on_deleted: G,
    ) -> (u128, usize)
    where
        K: Clone + Eq + std::hash::Hash,
        V: Clone,
        F: Fn(&K) -> &str,
        G: Fn(&V),
    {
        let timer = Instant::now();
        let mut deletion_keys = vec![];
        let view = map.clone().into_read_only();
        for (k, _) in view.iter() {
            if key_to_str(k).starts_with(prefix) {
                deletion_keys.push(k.clone());
            }
        }
        let deleted_count = deletion_keys.len();
        if deleted_count > 0 {
            for deletion_key in deletion_keys {
                if let Some((_, v)) = map.remove(&deletion_key) {
                    if let Some(v) = v {
                        on_deleted(&v);
                    }
                }
            }
            READ_AHEAD_ACTIVE_TASKS.sub(deleted_count as i64);
        }
        (timer.elapsed().as_millis(), deleted_count)
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
                    if self.read_plan_enabled && Self::is_read_plan(&options.ahead_options) {
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
        let prefix = if !path.ends_with("/") {
            format!("{}/", path)
        } else {
            path.to_owned()
        };

        let (seq_deletion_millis, seq_deletion_tasks) =
            Self::delete_with_prefix(&self.sequential_load_tasks, prefix.as_str(), |k| k, |_| {});
        let (millis, tasks) = if seq_deletion_tasks <= 0 {
            let (millis, tasks) = Self::delete_with_prefix(
                &self.read_plan_load_tasks,
                prefix.as_str(),
                |(left, _)| left,
                |v| {
                    let uid = v.uid;
                    self.read_plan_load_processor.remove_task(uid);
                },
            );
            if tasks > 0 {
                READ_AHEAD_ACTIVE_TASKS_OF_READ_PLAN.sub(tasks as i64);
            }
            (millis, tasks)
        } else {
            READ_AHEAD_ACTIVE_TASKS_OF_SEQUENTIAL.sub(seq_deletion_tasks as i64);
            (seq_deletion_millis, seq_deletion_tasks)
        };

        info!(
            "Deleted ahead cache for prefix: {} with {} load_tasks that costs {} millis",
            prefix, tasks, millis
        );

        self.handler.delete(path).await
    }

    async fn file_stat(&self, path: &str) -> anyhow::Result<FileStat, WorkerError> {
        self.handler.file_stat(path).await
    }
}

#[derive(Clone)]
struct ReadPlanReadAheadTask {
    uid: u64,
    file: Arc<Mutex<File>>,
    read_offset: Arc<AtomicU64>,
    plan_offset: Arc<AtomicU64>,
    sender: Arc<async_channel::Sender<ReadSegment>>,
    recv: Arc<async_channel::Receiver<ReadSegment>>,
    path: Arc<String>,

    processor: ReadPlanReadAheadTaskProcessor,
}

impl ReadPlanReadAheadTask {
    fn new(
        abs_path: &str,
        uid: u64,
        processor: &ReadPlanReadAheadTaskProcessor,
    ) -> anyhow::Result<Self> {
        let (send, recv) = async_channel::unbounded();
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(abs_path)?;
        let task = Self {
            uid,
            file: Arc::new(Mutex::new(file)),
            read_offset: Arc::new(Default::default()),
            plan_offset: Arc::new(Default::default()),
            sender: Arc::new(send),
            recv: Arc::new(recv),
            path: Arc::new(abs_path.to_string()),
            processor: processor.clone(),
        };
        processor.add_task(&task);
        Ok(task)
    }

    async fn do_load(&self, segment: ReadSegment) -> anyhow::Result<()> {
        let off = segment.offset;
        let len = segment.length;

        if off < self.read_offset.load(Ordering::Relaxed) as i64 {
            return Ok(());
        }
        let _timer = READ_AHEAD_OPERATION_DURATION_OF_READ_PLAN.start_timer();
        let file = self.file.lock();
        do_read_ahead(&file, self.path.as_str(), off as u64, len as u64);
        Ok(())
    }

    async fn load(
        &self,
        next_segments: &Vec<ReadSegment>,
        now_read_off: u64,
    ) -> anyhow::Result<()> {
        self.read_offset.store(now_read_off, Ordering::SeqCst);

        // for plan offset
        let now_plan_offset = self.plan_offset.load(Ordering::SeqCst);
        let mut max = now_plan_offset;
        for next_segment in next_segments {
            let off = next_segment.offset as u64;
            if off > now_plan_offset {
                self.sender.send(next_segment.clone()).await?;
                max = off;
            }
        }
        self.plan_offset.store(max, Ordering::SeqCst);

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
            let _timer = READ_AHEAD_OPERATION_DURATION_OF_SEQUENTIAL.start_timer();
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
    use tempfile::{NamedTempFile, TempDir};

    fn init_logger() -> Result<(), SetLoggerError> {
        env_logger::builder().is_test(true).try_init()
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn test_read_ahead() {
        let _logger = init_logger();

        // Prepare temp file path and content
        let temp_dir = TempDir::new().expect("create temp dir");
        let (root, sub_dirs, file_name) = create_sub_dirs(&temp_dir, "hello riffle read ahead");
        let relative_file_name = format!("{}/{}", &sub_dirs, file_name);

        let runtime_manager = RuntimeManager::default();
        let option = ReadAheadConfig::default();
        let inner_handler: Arc<Box<dyn LocalIO>> = Arc::new(Box::new(MockHandler::new()));
        let layer = ReadAheadLayerWrapper {
            handler: inner_handler,
            root: root.to_owned(),
            sequential_load_tasks: Default::default(),
            read_plan_load_tasks: Default::default(),
            ahead_batch_size: util::parse_raw_to_bytesize(option.batch_size.as_str()) as usize,
            ahead_batch_number: option.batch_number,
            read_plan_task_id_inc: Arc::new(Default::default()),
            read_plan_enabled: false,
            read_plan_load_processor: ReadPlanReadAheadTaskProcessor::new(
                &runtime_manager,
                Arc::new(Semaphore::new(1)),
            ),
        };

        // 1st read ahead
        let options = ReadOptions::default()
            .with_buffer_io()
            .with_read_range(ReadRange::RANGE(0, 5))
            .with_ahead_options(AheadOptions {
                sequential: true,
                read_batch_number: None,
                read_batch_size: None,
                next_read_segments: vec![],
            });
        let result = runtime_manager
            .default_runtime
            .block_on(layer.read(relative_file_name.as_str(), options));
        assert!(result.is_ok());
        assert_eq!(1, layer.sequential_load_tasks.len());

        runtime_manager
            .default_runtime
            .block_on(layer.delete(sub_dirs.as_str()))
            .unwrap();
        assert_eq!(0, layer.sequential_load_tasks.len());
    }

    // (root, sub_dirs, file_name)
    fn create_sub_dirs(temp_dir: &TempDir, content: &str) -> (String, String, String) {
        let root = temp_dir.path().to_str().unwrap().to_string();
        let sub_dir_path = temp_dir.path().join("a").join("b").join("c");
        std::fs::create_dir_all(&sub_dir_path).unwrap();
        let file_name = "a.data".to_string();
        let file_path = sub_dir_path.join(&file_name);
        let mut file = File::create(&file_path).unwrap();
        use std::io::Write;
        file.write_all(content.as_bytes()).unwrap();
        let sub_dirs = sub_dir_path
            .strip_prefix(&root)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        (root, sub_dirs, file_name)
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn test_read_ahead_with_read_plan() {
        let _logger = init_logger();

        // Prepare temp file path and content
        let temp_dir = TempDir::new().expect("create temp dir");
        let (root, sub_dirs, file_name) = create_sub_dirs(&temp_dir, "hello riffle read ahead");
        let relative_file_name = format!("{}/{}", &sub_dirs, file_name);

        let runtime_manager = RuntimeManager::default();
        let option = ReadAheadConfig::default();
        let inner_handler: Arc<Box<dyn LocalIO>> = Arc::new(Box::new(MockHandler::new()));
        let layer = ReadAheadLayerWrapper {
            handler: inner_handler,
            root: root.to_owned(),
            sequential_load_tasks: Default::default(),
            read_plan_load_tasks: Default::default(),
            ahead_batch_size: util::parse_raw_to_bytesize(option.batch_size.as_str()) as usize,
            ahead_batch_number: option.batch_number,
            read_plan_task_id_inc: Arc::new(Default::default()),
            read_plan_enabled: true,
            read_plan_load_processor: ReadPlanReadAheadTaskProcessor::new(
                &runtime_manager,
                Arc::new(Semaphore::new(1)),
            ),
        };

        let options = ReadOptions::default()
            .with_buffer_io()
            .with_read_range(ReadRange::RANGE(0, 5))
            .with_ahead_options(AheadOptions {
                sequential: false,
                read_batch_number: None,
                read_batch_size: None,
                next_read_segments: vec![
                    ReadSegment {
                        offset: 1,
                        length: 1,
                    },
                    ReadSegment {
                        offset: 2,
                        length: 1,
                    },
                ],
            });
        let result = runtime_manager
            .default_runtime
            .block_on(layer.read(relative_file_name.as_str(), options));
        assert!(result.is_ok());
        assert_eq!(1, layer.read_plan_load_tasks.len());
        assert_eq!(1, layer.read_plan_load_processor.task_size());

        runtime_manager
            .default_runtime
            .block_on(layer.delete(sub_dirs.as_str()))
            .unwrap();
        assert_eq!(0, layer.read_plan_load_tasks.len());
        assert_eq!(0, layer.read_plan_load_processor.task_size());
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
