use crate::actions::disk_append_bench::{DiskAppendBenchAction, FILE_PREFIX};
use crate::actions::Action;
use crate::Commands::DiskAppendBench;
use riffle_server::config::IoLimiterConfig;
use riffle_server::runtime::manager::create_runtime;
use riffle_server::runtime::RuntimeRef;
use riffle_server::store::local::options::ReadOptions;
use riffle_server::store::local::sync_io::SyncLocalIO;
use riffle_server::store::local::LocalIO;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

pub struct DiskReadBenchAction {
    dir: String,
    read_size: u64,
    batch_number: u64,
    concurrency: usize,

    io_handler: SyncLocalIO,

    append_action: DiskAppendBenchAction,

    read_runtimes: Vec<RuntimeRef>,
}

impl DiskReadBenchAction {
    pub(crate) fn new(
        dir: String,
        read_size: String,
        batch_num: usize,
        concurrency: usize,
    ) -> Self {
        let read_runtime = create_runtime(concurrency, "pool");
        let write_runtime = create_runtime(1, "pool");
        let underlying_io_handler =
            SyncLocalIO::new(&read_runtime, &write_runtime, dir.as_str(), None, None);

        let mut r_runtimes = vec![];
        for idx in 0..concurrency {
            r_runtimes.push(create_runtime(10, ""));
        }

        let append_action = DiskAppendBenchAction::new(
            dir.to_string(),
            concurrency,
            read_size.to_string(),
            batch_num,
            "100G".to_string(),
            false,
        );

        let action = Self {
            dir,
            read_size: riffle_server::util::parse_raw_to_bytesize(&read_size),
            batch_number: batch_num as u64,
            concurrency,
            io_handler: underlying_io_handler,
            append_action,
            read_runtimes: r_runtimes,
        };
        action
    }
}

#[async_trait::async_trait]
impl Action for DiskReadBenchAction {
    async fn act(&self) -> anyhow::Result<()> {
        let dir_path = Path::new(&self.dir);
        if (!dir_path.exists()) {
            fs::create_dir_all(dir_path)?;
        }

        let files: Vec<_> = fs::read_dir(dir_path)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry.file_type().map(|ft| ft.is_file()).unwrap_or(false)
                    && entry.file_name().to_string_lossy().starts_with(FILE_PREFIX)
            })
            .collect();

        if files.is_empty() || files.len() < self.concurrency {
            println!("Creating read_bench files to read...");
            self.append_action.act().await?;
            println!("Done!");
        }

        let mut futures = vec![];
        let batch_number = self.batch_number;
        let read_size = self.read_size;
        for idx in 0..self.concurrency {
            let file_name = format!("{}{}", FILE_PREFIX, idx);
            let handler: SyncLocalIO = self.io_handler.clone();
            let batch_number = batch_number;
            let read_size = read_size;
            let rt = self.read_runtimes.get(idx).unwrap();
            let f = rt.spawn(async move {
                let mut offset = 0;
                let mut latencies = Vec::with_capacity(batch_number as usize);
                let start = Instant::now();
                for _batch_idx in 0..batch_number {
                    let batch_start = Instant::now();
                    let _data = handler
                        .read(file_name.as_str(), ReadOptions::with_read_of_buffer_io(offset, read_size))
                        .await;
                    let batch_elapsed = batch_start.elapsed();
                    latencies.push(batch_elapsed.as_secs_f64());
                    offset += read_size as u64;
                }
                let elapsed = start.elapsed();
                latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
                let min = latencies.first().cloned().unwrap_or(0.0);
                let max = latencies.last().cloned().unwrap_or(0.0);
                let median = if latencies.is_empty() {
                    0.0
                } else if latencies.len() % 2 == 1 {
                    latencies[latencies.len() / 2]
                } else {
                    let mid = latencies.len() / 2;
                    (latencies[mid - 1] + latencies[mid]) / 2.0
                };
                println!(
                    "[concurrency {}] total read time: {:.3} secs, batch latency min/median/max: {:.6}/{:.6}/{:.6} secs",
                    idx,
                    elapsed.as_secs_f64(),
                    min,
                    median,
                    max
                );
            });
            futures.push(f);
        }

        for handle in futures {
            handle.await?;
        }

        Ok(())
    }
}
