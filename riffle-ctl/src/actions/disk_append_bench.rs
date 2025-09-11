use crate::actions::Action;
use bytes::Bytes;
use bytesize;
use clap::builder::Str;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::info;
use riffle_server::config::{Config, IoLimiterConfig, RuntimeConfig};
use riffle_server::http::HttpMonitorService;
use riffle_server::runtime::manager::{create_runtime, RuntimeManager};
use riffle_server::runtime::{Runtime, RuntimeRef};
use riffle_server::store::local::options::WriteOptions;
use riffle_server::store::local::sync_io::SyncLocalIO;
use riffle_server::store::DataBytes;
use riffle_server::util;
use std::sync::Arc;
use std::time::Duration;

const NUMBER_PER_THREAD_POOL: usize = 10;
pub const FILE_PREFIX: &str = "disk-bench-";

pub struct DiskAppendBenchAction {
    dir: String,
    concurrency: usize,
    write_size: u64,
    batch_number: usize,

    disk_throughput: String,

    w_runtimes: Vec<RuntimeRef>,

    throttle_enabled: bool,
    throttle_runtime: RuntimeRef,

    http_server: Box<riffle_server::http::http_service::PoemHTTPServer>,
    runtime_manager: RuntimeManager,
}

impl DiskAppendBenchAction {
    pub fn new(
        dir: String,
        concurrency: usize,
        write_size: String,
        batch_number: usize,
        disk_throughput: String,
        throttle_enabled: bool,
    ) -> Self {
        // Creating the http service to inspect the await tree stack
        let mut config = Config::create_simple_config();
        let port = util::find_available_port().unwrap();
        config.http_port = port;
        info!("Expose http service with port: {}", port);

        let runtime_manager = RuntimeManager::from(RuntimeConfig {
            read_thread_num: 512,
            localfile_write_thread_num: 512,
            hdfs_write_thread_num: 1,
            http_thread_num: 4,
            default_thread_num: 4,
            dispatch_thread_num: 4,
            read_ahead_thread_number: 1,
        });
        let http = HttpMonitorService::init(&config, runtime_manager.clone());

        let throttle_runtime = create_runtime(10, "throttle layer pool");

        let mut w_runtimes = Vec::new();
        for _ in 0..concurrency {
            w_runtimes.push(create_runtime(NUMBER_PER_THREAD_POOL, "writing pool"));
        }

        Self {
            dir,
            concurrency,
            write_size: util::parse_raw_to_bytesize(write_size.as_str()),
            batch_number,
            disk_throughput,
            throttle_enabled,
            throttle_runtime,
            w_runtimes,
            http_server: http,
            runtime_manager,
        }
    }
}

#[async_trait::async_trait]
impl Action for DiskAppendBenchAction {
    async fn act(&self) -> anyhow::Result<()> {
        let t_runtime = tokio::runtime::Handle::current();
        let underlying_io_handler = SyncLocalIO::new(
            &self.runtime_manager.read_runtime,
            &self.runtime_manager.localfile_write_runtime,
            self.dir.as_str(),
            None,
            None,
        );

        let mut builder = riffle_server::store::local::layers::OperatorBuilder::new(Arc::new(
            Box::new(underlying_io_handler),
        ));
        if self.throttle_enabled {
            println!("Throttle is enabled.");
            let config = IoLimiterConfig {
                capacity: self.disk_throughput.clone(),
                read_enable: true,
                write_enable: true,
            };
            builder = builder.layer(
                riffle_server::store::local::io_layer_throttle::ThrottleLayer::new(
                    &self.throttle_runtime,
                    &config,
                ),
            );
        }

        let io_handler = Arc::new(builder.build());

        let test_data = vec![0u8; self.write_size as usize];
        let batch_number = self.batch_number;
        let write_size = self.write_size as usize;

        let total_bytes = (self.concurrency * batch_number * write_size) as u64;
        info!(
            "Total data to write: {}",
            bytesize::to_string(total_bytes, true)
        );

        let progress = ProgressBar::new((self.concurrency * batch_number) as u64);
        progress.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
                .unwrap()
                .progress_chars("##-"),
        );
        progress.set_message("Writing files...");
        let progress = Arc::new(progress);

        let start_time = std::time::Instant::now();
        let written_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));

        let mut handles = Vec::with_capacity(self.concurrency);
        for i in 0..self.concurrency {
            let file_path = format!("{}{}", FILE_PREFIX, i);
            let data = test_data.clone();
            let io_handler = io_handler.clone();
            let progress = progress.clone();
            let written_bytes = written_bytes.clone();
            let total_batches = (self.concurrency * batch_number) as u64;
            let start_time = start_time.clone();

            let runtime = self.w_runtimes.get(i).unwrap();
            let handle = runtime.spawn_with_await_tree(format!("w-{}", i).as_str(), async move {
                let mut file_written_bytes = 0;
                for batch in 0..batch_number {
                    let bytes = Bytes::copy_from_slice(&data);
                    match io_handler
                        .write(
                            file_path.as_str(),
                            WriteOptions::with_append_of_direct_io(
                                bytes.into(),
                                file_written_bytes,
                            ),
                        )
                        .await
                    {
                        Ok(_) => {
                            file_written_bytes += write_size as u64;
                            written_bytes
                                .fetch_add(write_size as u64, std::sync::atomic::Ordering::Relaxed);
                            progress.inc(1);

                            let elapsed = start_time.elapsed().as_secs_f64();
                            let current_written =
                                written_bytes.load(std::sync::atomic::Ordering::Relaxed);
                            let current_speed = current_written as f64 / elapsed;

                            let remaining_batches = total_batches - progress.position();
                            let est_secs = if current_speed > 0.0 {
                                (remaining_batches * write_size as u64) as f64 / current_speed
                            } else {
                                0.0
                            };

                            progress.set_message(format!(
                                "Writing files... Speed: {}/s, ETA: {:.1}s",
                                bytesize::to_string(current_speed as u64, true),
                                est_secs
                            ));
                        }
                        Err(e) => eprintln!(
                            "Error writing file {} batch {}: {:?}",
                            file_path,
                            batch + 1,
                            e
                        ),
                    }
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await?;
        }

        let elapsed = start_time.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();
        let write_speed = total_bytes as f64 / elapsed_secs;

        let log = format!(
            "Total: {}, Time: {:.2}s, Speed: {}/s",
            bytesize::to_string(total_bytes, true),
            elapsed_secs,
            bytesize::to_string(write_speed as u64, true)
        );
        println!("{}", log);
        Ok(())
    }
}
