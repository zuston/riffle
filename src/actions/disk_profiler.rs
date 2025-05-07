use crate::actions::Action;
use crate::runtime::{Runtime, RuntimeRef};
use crate::store::local::sync_io::SyncLocalIO;
use crate::store::local::LocalIO;
use crate::store::BytesWrapper;
use crate::util;
use bytes::Bytes;
use bytesize;
use indicatif::{ProgressBar, ProgressStyle};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct DiskProfileResult {
    pub best_block_size: usize,
    pub best_concurrency: usize,
    pub best_throughput: f64,
    pub detailed_results: Vec<BlockSizeResult>,
}

#[derive(Clone)]
pub struct BlockSizeResult {
    pub block_size: usize,
    pub best_concurrency: usize,
    pub best_throughput: f64,
    pub concurrency_results: Vec<(usize, f64)>,
}

pub struct DiskProfiler {
    dir: String,
    min_block_size: usize,
    max_block_size: usize,
    min_concurrency: usize,
    max_concurrency: usize,
    test_duration: Duration,
    runtime: RuntimeRef,
    samples_per_test: usize,
    throughput_threshold: f64,
}

impl DiskProfiler {
    pub fn new(
        dir: String,
        min_block_size: String,
        max_block_size: String,
        min_concurrency: usize,
        max_concurrency: usize,
        test_duration_secs: u64,
    ) -> Self {
        let runtime = crate::runtime::manager::create_runtime(max_concurrency, "profiler pool");
        Self {
            dir,
            min_block_size: util::parse_raw_to_bytesize(min_block_size.as_str()) as usize,
            max_block_size: util::parse_raw_to_bytesize(max_block_size.as_str()) as usize,
            min_concurrency,
            max_concurrency,
            test_duration: Duration::from_secs(test_duration_secs),
            runtime,
            samples_per_test: 3,        // 每个配置测试3次取平均值
            throughput_threshold: 0.95, // 当性能下降到最佳值的95%时停止测试
        }
    }

    async fn run_single_test(
        &self,
        block_size: usize,
        concurrency: usize,
        io_handler: Arc<SyncLocalIO>,
    ) -> f64 {
        let mut total_throughput = 0.0;

        // 运行多次测试取平均值
        for _ in 0..self.samples_per_test {
            let test_data = vec![0u8; block_size];
            let total_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let start_time = Instant::now();
            let end_time = start_time + self.test_duration;

            let mut handles = Vec::with_capacity(concurrency);
            for i in 0..concurrency {
                let file_path = format!("profile_test_{}", i);
                let data = test_data.clone();
                let io_handler = io_handler.clone();
                let total_bytes = total_bytes.clone();

                let handle = self.runtime.spawn(async move {
                    let mut file_written_bytes = 0;
                    while Instant::now() < end_time {
                        let bytes = Bytes::copy_from_slice(&data);
                        if let Ok(_) = io_handler
                            .direct_append(
                                file_path.as_str(),
                                file_written_bytes,
                                BytesWrapper::Direct(bytes),
                            )
                            .await
                        {
                            file_written_bytes += block_size;
                            total_bytes
                                .fetch_add(block_size as u64, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                });
                handles.push(handle);
            }

            for handle in handles {
                let _ = handle.await;
            }

            let elapsed = start_time.elapsed().as_secs_f64();
            let total = total_bytes.load(std::sync::atomic::Ordering::Relaxed);
            total_throughput += total as f64 / elapsed;
        }

        total_throughput / self.samples_per_test as f64
    }

    async fn find_best_concurrency(
        &self,
        block_size: usize,
        io_handler: Arc<SyncLocalIO>,
        progress: &ProgressBar,
    ) -> BlockSizeResult {
        let mut results = Vec::new();
        let mut best_throughput = 0.0;
        let mut best_concurrency = self.min_concurrency;

        // 使用二分查找来快速定位最佳并行度
        let mut left = self.min_concurrency;
        let mut right = self.max_concurrency;

        while left <= right {
            let mid = (left + right) / 2;
            let mid_throughput = self
                .run_single_test(block_size, mid, io_handler.clone())
                .await;
            results.push((mid, mid_throughput));

            progress.set_message(format!(
                "Testing block_size={}, concurrency={}, throughput={}/s",
                bytesize::to_string(block_size as u64, true),
                mid,
                bytesize::to_string(mid_throughput as u64, true)
            ));

            if mid_throughput > best_throughput {
                best_throughput = mid_throughput;
                best_concurrency = mid;
            }

            // 如果性能开始下降，停止增加并行度
            if mid > self.min_concurrency {
                let prev_throughput = results
                    .iter()
                    .find(|(c, _)| *c == mid - 1)
                    .map(|(_, t)| *t)
                    .unwrap_or(0.0);

                if mid_throughput < prev_throughput * self.throughput_threshold {
                    break;
                }
            }

            // 如果当前并行度的性能比之前的好，继续增加
            if mid > self.min_concurrency {
                let prev_throughput = results
                    .iter()
                    .find(|(c, _)| *c == mid - 1)
                    .map(|(_, t)| *t)
                    .unwrap_or(0.0);

                if mid_throughput > prev_throughput {
                    left = mid + 1;
                } else {
                    right = mid - 1;
                }
            } else {
                left = mid + 1;
            }
        }

        BlockSizeResult {
            block_size,
            best_concurrency,
            best_throughput,
            concurrency_results: results,
        }
    }

    pub async fn profile(&self) -> DiskProfileResult {
        let io_handler =
            SyncLocalIO::new(&self.runtime, &self.runtime, self.dir.as_str(), None, None);
        let io_handler = Arc::new(io_handler);

        let progress = ProgressBar::new(
            ((self.max_block_size - self.min_block_size) / self.min_block_size + 1) as u64,
        );
        progress.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
                .unwrap()
                .progress_chars("##-"),
        );

        let mut detailed_results = Vec::new();
        let mut best_result = DiskProfileResult {
            best_block_size: self.min_block_size,
            best_concurrency: self.min_concurrency,
            best_throughput: 0.0,
            detailed_results: Vec::new(),
        };

        let mut block_size = self.min_block_size;
        while block_size <= self.max_block_size {
            let block_result = self
                .find_best_concurrency(block_size, io_handler.clone(), &progress)
                .await;
            detailed_results.push(block_result.clone());

            if block_result.best_throughput > best_result.best_throughput {
                best_result.best_block_size = block_size;
                best_result.best_concurrency = block_result.best_concurrency;
                best_result.best_throughput = block_result.best_throughput;
            }

            progress.inc(1);
            block_size *= 2;
        }

        best_result.detailed_results = detailed_results;
        progress.finish_with_message("Profiling completed");

        // 打印详细结果
        println!("\nDetailed profiling results:");
        for result in &best_result.detailed_results {
            println!(
                "\nBlock size: {}",
                bytesize::to_string(result.block_size as u64, true)
            );
            println!("Best concurrency: {}", result.best_concurrency);
            println!(
                "Best throughput: {}/s",
                bytesize::to_string(result.best_throughput as u64, true)
            );
            println!("Concurrency vs Throughput:");
            for (concurrency, throughput) in &result.concurrency_results {
                println!(
                    "  {}: {}/s",
                    concurrency,
                    bytesize::to_string(*throughput as u64, true)
                );
            }
        }

        best_result
    }
}

#[async_trait::async_trait]
impl Action for DiskProfiler {
    async fn act(&self) -> anyhow::Result<()> {
        let result = self.profile().await;
        Ok(())
    }
}
