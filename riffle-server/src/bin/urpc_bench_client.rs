// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use anyhow::Result;
use bytes::Bytes;
use clap::Parser;
use hdrhistogram::Histogram;
use riffle_server::app_manager::application_identifier::ApplicationId;
use riffle_server::grpc::protobuf::uniffle::shuffle_server_client::ShuffleServerClient;
use riffle_server::grpc::protobuf::uniffle::{
    PartitionToBlockIds, ReportShuffleResultRequest, RequireBufferRequest, SendShuffleDataRequest,
    ShuffleBlock, ShuffleData, ShuffleRegisterRequest,
};
use riffle_server::id_layout::DEFAULT_BLOCK_ID_LAYOUT;
use riffle_server::urpc::client::EpollUrpcClient;
use riffle_server::urpc::command::GetLocalDataRequestCommand;
use serde::Serialize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Parser, Debug, Clone)]
#[command(version, about = "URPC micro benchmark client")]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    urpc_host: String,
    #[arg(long, default_value_t = 19999)]
    urpc_port: usize,
    #[arg(long, default_value = "127.0.0.1")]
    grpc_host: String,
    #[arg(long, default_value_t = 19997)]
    grpc_port: usize,
    #[arg(long, default_value_t = 16384)]
    payload_bytes: usize,
    #[arg(long, default_value_t = 32)]
    concurrency: usize,
    #[arg(long, default_value_t = 30)]
    warmup_secs: u64,
    #[arg(long, default_value_t = 120)]
    measure_secs: u64,
    #[arg(long, default_value_t = 3000)]
    timeout_ms: u64,
    #[arg(long, default_value_t = 1)]
    repeats: usize,
    #[arg(long, default_value = "epoll")]
    mode_tag: String,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value_t = false)]
    skip_prepare: bool,
}

#[derive(Debug, Clone)]
struct BenchTarget {
    app_id: String,
    shuffle_id: i32,
    partition_id: i32,
    partition_num_per_range: i32,
    partition_num: i32,
    payload_len: i32,
}

#[derive(Debug, Clone)]
struct WorkerStats {
    success: u64,
    errors: u64,
    latency_hist: Histogram<u64>,
}

#[derive(Debug, Serialize)]
struct RunResult {
    mode_tag: String,
    payload_bytes: usize,
    concurrency: usize,
    warmup_secs: u64,
    measure_secs: u64,
    run_id: usize,
    success: u64,
    errors: u64,
    qps: f64,
    error_rate: f64,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    max_us: u64,
}

#[derive(Debug, Serialize)]
struct BenchOutput {
    urpc_host: String,
    urpc_port: usize,
    grpc_host: String,
    grpc_port: usize,
    mode_tag: String,
    payload_bytes: usize,
    concurrency: usize,
    warmup_secs: u64,
    measure_secs: u64,
    timeout_ms: u64,
    repeats: usize,
    skip_prepare: bool,
    prepared_app_id: String,
    results: Vec<RunResult>,
}

static REQUEST_ID: AtomicI64 = AtomicI64::new(1);

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    if args.payload_bytes == 0 {
        anyhow::bail!("payload_bytes must be greater than 0");
    }
    if args.concurrency == 0 {
        anyhow::bail!("concurrency must be greater than 0");
    }

    let target = if args.skip_prepare {
        BenchTarget {
            app_id: ApplicationId::mock().to_string(),
            shuffle_id: 0,
            partition_id: 0,
            partition_num_per_range: 1,
            partition_num: 1,
            payload_len: args.payload_bytes as i32,
        }
    } else {
        prepare_bench_data(&args).await?
    };

    let mut results = Vec::with_capacity(args.repeats);
    for run_id in 0..args.repeats {
        warmup_run(&args, &target).await?;
        let run_result = measure_run(&args, &target, run_id).await?;
        results.push(run_result);
    }

    let output = BenchOutput {
        urpc_host: args.urpc_host.clone(),
        urpc_port: args.urpc_port,
        grpc_host: args.grpc_host.clone(),
        grpc_port: args.grpc_port,
        mode_tag: args.mode_tag.clone(),
        payload_bytes: args.payload_bytes,
        concurrency: args.concurrency,
        warmup_secs: args.warmup_secs,
        measure_secs: args.measure_secs,
        timeout_ms: args.timeout_ms,
        repeats: args.repeats,
        skip_prepare: args.skip_prepare,
        prepared_app_id: target.app_id.clone(),
        results,
    };

    if let Some(parent) = args.output.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&args.output, serde_json::to_vec_pretty(&output)?)?;

    println!("benchmark result written to {}", args.output.display());
    Ok(())
}

async fn prepare_bench_data(args: &Args) -> Result<BenchTarget> {
    let grpc_addr = format!("http://{}:{}", args.grpc_host, args.grpc_port);
    let mut grpc_client = ShuffleServerClient::connect(grpc_addr).await?;

    let app_id = format!(
        "bench-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis()
    );
    let shuffle_id = 0;
    let partition_id = 0;
    let payload = vec![b'x'; args.payload_bytes];

    grpc_client
        .register_shuffle(ShuffleRegisterRequest {
            app_id: app_id.clone(),
            shuffle_id,
            partition_ranges: vec![],
            remote_storage: None,
            user: "bench".to_string(),
            shuffle_data_distribution: 1,
            max_concurrency_per_partition_to_write: 10,
            merge_context: None,
            properties: Default::default(),
        })
        .await?;

    let buffer_resp = grpc_client
        .require_buffer(RequireBufferRequest {
            require_size: payload.len() as i32,
            app_id: app_id.clone(),
            shuffle_id,
            partition_ids: vec![partition_id],
        })
        .await?
        .into_inner();

    let block_id = DEFAULT_BLOCK_ID_LAYOUT.get_block_id(1, partition_id as i64, 1);
    grpc_client
        .send_shuffle_data(SendShuffleDataRequest {
            app_id: app_id.clone(),
            shuffle_id,
            require_buffer_id: buffer_resp.require_buffer_id,
            shuffle_data: vec![ShuffleData {
                partition_id,
                block: vec![ShuffleBlock {
                    block_id,
                    length: payload.len() as i32,
                    uncompress_length: payload.len() as i32,
                    crc: 0,
                    data: Bytes::from(payload),
                    task_attempt_id: 0,
                }],
            }],
            timestamp: 0,
            stage_attempt_number: 0,
            combined_shuffle_data: None,
        })
        .await?;

    grpc_client
        .report_shuffle_result(ReportShuffleResultRequest {
            app_id: app_id.clone(),
            shuffle_id,
            task_attempt_id: 0,
            bitmap_num: 0,
            partition_to_block_ids: vec![PartitionToBlockIds {
                partition_id,
                block_ids: vec![block_id],
            }],
            partition_stats: vec![],
        })
        .await?;

    Ok(BenchTarget {
        app_id,
        shuffle_id,
        partition_id,
        partition_num_per_range: 1,
        partition_num: 1,
        payload_len: args.payload_bytes as i32,
    })
}

async fn warmup_run(args: &Args, target: &BenchTarget) -> Result<()> {
    run_phase(args, target, Duration::from_secs(args.warmup_secs), false)
        .await
        .map(|_| ())
}

async fn measure_run(args: &Args, target: &BenchTarget, run_id: usize) -> Result<RunResult> {
    let stats = run_phase(args, target, Duration::from_secs(args.measure_secs), true).await?;

    let total = stats.success + stats.errors;
    let qps = stats.success as f64 / args.measure_secs as f64;
    let error_rate = if total == 0 {
        0.0
    } else {
        stats.errors as f64 / total as f64
    };

    let p50_us = if stats.latency_hist.len() > 0 {
        stats.latency_hist.value_at_quantile(0.50)
    } else {
        0
    };
    let p95_us = if stats.latency_hist.len() > 0 {
        stats.latency_hist.value_at_quantile(0.95)
    } else {
        0
    };
    let p99_us = if stats.latency_hist.len() > 0 {
        stats.latency_hist.value_at_quantile(0.99)
    } else {
        0
    };
    let max_us = if stats.latency_hist.len() > 0 {
        stats.latency_hist.max()
    } else {
        0
    };

    Ok(RunResult {
        mode_tag: args.mode_tag.clone(),
        payload_bytes: args.payload_bytes,
        concurrency: args.concurrency,
        warmup_secs: args.warmup_secs,
        measure_secs: args.measure_secs,
        run_id,
        success: stats.success,
        errors: stats.errors,
        qps,
        error_rate,
        p50_us,
        p95_us,
        p99_us,
        max_us,
    })
}

async fn run_phase(
    args: &Args,
    target: &BenchTarget,
    phase_duration: Duration,
    collect_latency: bool,
) -> Result<WorkerStats> {
    let end = Instant::now() + phase_duration;
    let mut handles = Vec::with_capacity(args.concurrency);

    for _ in 0..args.concurrency {
        let bench_target = target.clone();
        let host = args.urpc_host.clone();
        let port = args.urpc_port;
        let timeout = Duration::from_millis(args.timeout_ms);
        let worker_end = end;
        let collect = collect_latency;

        handles.push(tokio::spawn(async move {
            let mut client = EpollUrpcClient::connect(&host, port).await?;
            let mut worker_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)?;
            let mut success = 0u64;
            let mut errors = 0u64;

            while Instant::now() < worker_end {
                let request = GetLocalDataRequestCommand::new(
                    REQUEST_ID.fetch_add(1, Ordering::Relaxed),
                    bench_target.app_id.clone(),
                    bench_target.shuffle_id,
                    bench_target.partition_id,
                    bench_target.partition_num_per_range,
                    bench_target.partition_num,
                    0,
                    bench_target.payload_len,
                    0,
                );

                let started = Instant::now();
                match tokio::time::timeout(timeout, client.get_local_shuffle_data(request)).await {
                    Ok(Ok(_)) => {
                        success += 1;
                        if collect {
                            let elapsed_us = started.elapsed().as_micros() as u64;
                            let _ = worker_hist.record(elapsed_us.max(1));
                        }
                    }
                    _ => {
                        errors += 1;
                    }
                }
            }

            Ok::<WorkerStats, anyhow::Error>(WorkerStats {
                success,
                errors,
                latency_hist: worker_hist,
            })
        }));
    }

    let mut merged_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)?;
    let mut merged = WorkerStats {
        success: 0,
        errors: 0,
        latency_hist: merged_hist.clone(),
    };

    for handle in handles {
        let stats = handle.await??;
        merged.success += stats.success;
        merged.errors += stats.errors;
        merged_hist.add(&stats.latency_hist)?;
    }
    merged.latency_hist = merged_hist;

    Ok(merged)
}
