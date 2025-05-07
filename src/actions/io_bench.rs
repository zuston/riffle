use crate::actions::Action;
use crate::runtime::manager::create_runtime;
use crate::runtime::{Runtime, RuntimeRef};
use crate::store::local::sync_io::SyncLocalIO;
use crate::store::BytesWrapper;
use crate::util;
use bytes::Bytes;
use clap::builder::Str;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::sync::Arc;
use std::time::Duration;

pub struct IoBenchAction {
    dir: String,
    concurrency: usize,
    write_size: u64,
    batch_number: usize,

    disk_throughput: u64,

    w_runtime: RuntimeRef,
    r_runtime: RuntimeRef,
}

impl IoBenchAction {
    pub fn new(
        dir: String,
        concurrency: usize,
        write_size: String,
        batch_number: usize,
        disk_throughput: String,
    ) -> Self {
        let write_runtime = create_runtime(concurrency, "write pool");
        let read_runtime = create_runtime(concurrency, "read pool");
        Self {
            dir,
            concurrency,
            write_size: util::parse_raw_to_bytesize(write_size.as_str()),
            batch_number,
            w_runtime: write_runtime,
            r_runtime: read_runtime,
            disk_throughput: util::parse_raw_to_bytesize(disk_throughput.as_str()),
        }
    }
}

#[async_trait::async_trait]
impl Action for IoBenchAction {
    async fn act(&self) -> anyhow::Result<()> {
        let t_runtime = tokio::runtime::Handle::current();
        let underlying_io_handler = SyncLocalIO::new(
            &self.r_runtime,
            &self.w_runtime,
            self.dir.as_str(),
            None,
            None,
        );

        let io_handler = crate::store::local::layers::OperatorBuilder::new(Arc::new(Box::new(
            underlying_io_handler,
        )))
        .layer(crate::store::local::io_layer_throttle::ThrottleLayer::new(
            &self.w_runtime,
            (self.disk_throughput * 2) as usize,
            self.disk_throughput as usize,
            Duration::from_millis(10),
        ))
        .build();
        let io_handler = Arc::new(io_handler);

        let test_data = vec![0u8; self.write_size as usize];
        let batch_number = self.batch_number;
        let write_size = self.write_size as usize;

        // 创建总进度条
        let progress = ProgressBar::new((self.concurrency * batch_number) as u64);
        progress.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
                .unwrap()
                .progress_chars("##-"),
        );
        progress.set_message("Writing files...");
        let progress = Arc::new(progress);

        let mut handles = Vec::with_capacity(self.concurrency);
        for i in 0..self.concurrency {
            let file_path = format!("test_file_{}", i);
            let data = test_data.clone();
            let io_handler = io_handler.clone();
            let progress = progress.clone();

            let handle = self.w_runtime.spawn(async move {
                let mut written_bytes = 0;
                for batch in 0..batch_number {
                    let bytes = Bytes::copy_from_slice(&data);
                    match io_handler
                        .direct_append(
                            file_path.as_str(),
                            written_bytes,
                            BytesWrapper::Direct(bytes),
                        )
                        .await
                    {
                        Ok(_) => {
                            written_bytes += write_size;
                            progress.inc(1);
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

        progress.finish_with_message("All files completed");
        Ok(())
    }
}
