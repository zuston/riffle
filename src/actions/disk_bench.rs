use crate::actions::Action;
use crate::runtime::manager::create_runtime;
use crate::runtime::{Runtime, RuntimeRef};
use crate::store::local::sync_io::SyncLocalIO;
use crate::store::BytesWrapper;
use crate::util;
use bytes::Bytes;
use bytesize;
use clap::builder::Str;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::sync::Arc;
use std::time::Duration;

pub struct DiskBenchAction {
    dir: String,
    concurrency: usize,
    write_size: u64,
    batch_number: usize,

    disk_throughput: u64,

    w_runtime: RuntimeRef,
    r_runtime: RuntimeRef,

    throttle_enabled: bool,
    throttle_runtime: RuntimeRef,
}

impl DiskBenchAction {
    pub fn new(
        dir: String,
        concurrency: usize,
        write_size: String,
        batch_number: usize,
        disk_throughput: String,
        throttle_enabled: bool,
    ) -> Self {
        let write_runtime = create_runtime(concurrency, "write pool");
        let read_runtime = create_runtime(concurrency, "read pool");
        let throttle_runtime = create_runtime(10, "throttle layer pool");
        Self {
            dir,
            concurrency,
            write_size: util::parse_raw_to_bytesize(write_size.as_str()),
            batch_number,
            w_runtime: write_runtime,
            r_runtime: read_runtime,
            disk_throughput: util::parse_raw_to_bytesize(disk_throughput.as_str()),
            throttle_enabled,
            throttle_runtime,
        }
    }
}

#[async_trait::async_trait]
impl Action for DiskBenchAction {
    async fn act(&self) -> anyhow::Result<()> {
        let t_runtime = tokio::runtime::Handle::current();
        let underlying_io_handler = SyncLocalIO::new(
            &self.r_runtime,
            &self.w_runtime,
            self.dir.as_str(),
            None,
            None,
        );

        let mut builder = crate::store::local::layers::OperatorBuilder::new(Arc::new(Box::new(
            underlying_io_handler,
        )));
        if self.throttle_enabled {
            println!("Throttle is enabled.");
            builder = builder.layer(crate::store::local::io_layer_throttle::ThrottleLayer::new(
                &self.throttle_runtime,
                (self.disk_throughput * 2) as usize,
                self.disk_throughput as usize,
                Duration::from_millis(10),
            ));
        }

        let io_handler = Arc::new(builder.build());

        let test_data = vec![0u8; self.write_size as usize];
        let batch_number = self.batch_number;
        let write_size = self.write_size as usize;

        let total_bytes = (self.concurrency * batch_number * write_size) as u64;
        println!(
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
            let file_path = format!("test_file_{}", i);
            let data = test_data.clone();
            let io_handler = io_handler.clone();
            let progress = progress.clone();
            let written_bytes = written_bytes.clone();

            let runtime = create_runtime(4, "write pool");
            let handle = runtime.spawn(async move {
                let mut file_written_bytes = 0;
                for batch in 0..batch_number {
                    let bytes = Bytes::copy_from_slice(&data);
                    match io_handler
                        .direct_append(
                            file_path.as_str(),
                            file_written_bytes,
                            BytesWrapper::Direct(bytes),
                        )
                        .await
                    {
                        Ok(_) => {
                            file_written_bytes += write_size;
                            written_bytes
                                .fetch_add(write_size as u64, std::sync::atomic::Ordering::Relaxed);
                            progress.inc(1);

                            let elapsed = start_time.elapsed().as_secs_f64();
                            let current_written =
                                written_bytes.load(std::sync::atomic::Ordering::Relaxed);
                            let current_speed = current_written as f64 / elapsed;

                            progress.set_message(format!(
                                "Writing files... Speed: {}/s",
                                bytesize::to_string(current_speed as u64, true)
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
