#![allow(dead_code, unused)]
#![feature(impl_trait_in_assoc_type)]

mod actions;

use crate::actions::disk_bench::DiskBenchAction;
use crate::actions::disk_profiler::DiskProfiler;
use crate::actions::postgres_server::PostgresServerAction;
use crate::actions::disk_read_bench::DiskReadBenchAction;
use crate::actions::{Action, NodeUpdateAction, OutputFormat, QueryAction, ValidateAction};
use clap::{Parser, Subcommand};
use tokio::runtime::Runtime;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    #[command(
        about = "Expose postgres protocol server to query instances/active_apps/historical_apps table"
    )]
    PostgresServer {
        #[arg(short, long)]
        coordinator_http_url: String,
        #[arg(long, default_value = "0.0.0.0")]
        host: String,
        #[arg(long, default_value = "29999")]
        port: usize,
    },
    DiskReadBench {
        #[arg(short, long)]
        dir: String,
        #[arg(short, long)]
        read_size: String,
        #[arg(short, long)]
        batch_number: usize,
        #[arg(short, long)]
        concurrency: usize,
    },
    #[command(about = "Using the riffle IO scheduler to test local disk IO")]
    DiskBench {
        #[arg(short, long)]
        dir: String,
        #[arg(short, long)]
        batch_number: usize,
        #[arg(short, long)]
        concurrency: usize,
        #[arg(short, long)]
        write_size: String,
        #[arg(short, long)]
        disk_throughput: String,
        #[arg(short, long)]
        throttle_enabled: bool,
    },

    #[command(
        about = "Profile disk performance with different block sizes and concurrency levels"
    )]
    DiskProfiler {
        #[arg(short, long)]
        dir: String,
        #[arg(short, long, default_value = "4KB")]
        min_block_size: String,
        #[arg(short, long, default_value = "64MB")]
        max_block_size: String,
        #[arg(short, long, default_value = "1")]
        min_concurrency: usize,
        #[arg(short, long, default_value = "16")]
        max_concurrency: usize,
        #[arg(short, long, default_value = "10")]
        test_duration_secs: u64,
    },

    #[command(about = "Validate internal index/data file")]
    Validate {
        #[arg(short, long)]
        index_file_path: String,
        #[arg(short, long)]
        data_file_path: String,
    },
    #[command(about = "Use sql to query instances/active_apps/historical_apps table")]
    Query {
        #[arg(short, long)]
        sql: String,
        #[arg(short, long)]
        coordinator_http_url: String,
        #[arg(short)]
        pipeline: bool,
    },
    #[command(about = "Update server status to make it decommission (pipeline mode supported)")]
    Update {
        #[arg(short, long)]
        instance: Option<String>,
        #[arg(short, long)]
        status: String,
    },
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let command = args.command;

    let action: Box<dyn Action> = match command {
        Commands::PostgresServer {
            coordinator_http_url,
            host,
            port,
        } => Box::new(PostgresServerAction::new(coordinator_http_url, host, port)),
        Commands::DiskReadBench {
            dir,
            read_size,
            batch_number,
            concurrency,
        } => Box::new(DiskReadBenchAction::new(
            dir,
            read_size,
            batch_number,
            concurrency,
        )),
        Commands::DiskBench {
            dir,
            batch_number,
            concurrency,
            write_size,
            disk_throughput,
            throttle_enabled,
        } => Box::new(DiskBenchAction::new(
            dir,
            concurrency,
            write_size,
            batch_number,
            disk_throughput,
            throttle_enabled,
        )),

        Commands::DiskProfiler {
            dir,
            min_block_size,
            max_block_size,
            min_concurrency,
            max_concurrency,
            test_duration_secs,
        } => {
            let profiler = DiskProfiler::new(
                dir,
                min_block_size,
                max_block_size,
                min_concurrency,
                max_concurrency,
                test_duration_secs,
            );
            Box::new(profiler)
        }

        Commands::Validate {
            index_file_path,
            data_file_path,
        } => Box::new(ValidateAction::new(index_file_path, data_file_path)),

        Commands::Query {
            sql,
            coordinator_http_url,
            pipeline,
        } => {
            let table_format = if pipeline {
                OutputFormat::JSON
            } else {
                OutputFormat::TABLE
            };
            Box::new(QueryAction::new(sql, table_format, coordinator_http_url))
        }

        Commands::Update { instance, status } => Box::new(NodeUpdateAction::new(instance, status)),

        _ => panic!("Unknown command"),
    };

    let rt = Runtime::new()?;
    rt.block_on(async {
        let _ = action.act().await;
    });

    Ok(())
}
