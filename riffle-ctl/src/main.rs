#![allow(dead_code, unused)]
#![feature(impl_trait_in_assoc_type)]

mod actions;

use crate::actions::disk_append_bench::DiskAppendBenchAction;
use crate::actions::disk_profiler::DiskProfiler;
use crate::actions::disk_read_bench::DiskReadBenchAction;
use crate::actions::hdfs_append::HdfsAppendAction;
use crate::actions::kill_action::KillAction;
use crate::actions::postgres_server::PostgresServerAction;
use crate::actions::query::{OutputFormat, QueryAction};
use crate::actions::update_action::NodeUpdateAction;
use crate::actions::{Action, ValidateAction};
use crate::Commands::Kill;
use clap::{Parser, Subcommand};
use log::LevelFilter;
use logforth::append;
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
        #[arg(short, long)]
        read_ahead_enable: bool,
    },
    #[command(about = "Using the riffle IO scheduler to test local disk IO")]
    DiskAppendBench {
        #[arg(long)]
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
        #[arg(short, long)]
        decommission_grpc_mode: bool,
    },
    #[command(about = "Kill riffle server")]
    Kill {
        #[arg(short, long, default_value = "false")]
        force: bool,
        #[arg(short, long)]
        instance: Option<String>,
    },
    #[command(about = "Hdfs append test")]
    HdfsAppend {
        #[arg(short, long)]
        file_path: String,
        #[arg(short, long)]
        total_size: String,
        #[arg(short, long)]
        batch_size: String,
    },
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let command = args.command;

    logforth::builder()
        .dispatch(|d| {
            d.filter(LevelFilter::Info)
                .append(append::Stdout::default())
        })
        .apply();

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
            read_ahead_enable,
        } => Box::new(DiskReadBenchAction::new(
            dir,
            read_size,
            batch_number,
            concurrency,
            read_ahead_enable,
        )),
        Commands::DiskAppendBench {
            dir,
            batch_number,
            concurrency,
            write_size,
            disk_throughput,
            throttle_enabled,
        } => Box::new(DiskAppendBenchAction::new(
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

        Commands::Update {
            instance,
            status,
            decommission_grpc_mode,
        } => Box::new(NodeUpdateAction::new(
            instance,
            status,
            decommission_grpc_mode,
        )),
        Commands::Kill { force, instance } => Box::new(KillAction::new(force, instance)),
        Commands::HdfsAppend {
            file_path,
            total_size,
            batch_size,
        } => Box::new(HdfsAppendAction::new(
            file_path.as_str(),
            total_size.as_str(),
            batch_size.as_str(),
        )),
        _ => panic!("Unknown command"),
    };

    let rt = Runtime::new()?;
    rt.block_on(async {
        if let Err(e) = action.act().await {
            println!("Error: {}", e);
        }
    });

    Ok(())
}
