#![allow(dead_code, unused)]
#![feature(impl_trait_in_assoc_type)]
extern crate core;

mod actions;
mod meta;

use crate::actions::disk_append_bench::DiskAppendBenchAction;
use crate::actions::disk_profiler::DiskProfiler;
use crate::actions::disk_read_bench::DiskReadBenchAction;
use crate::actions::hdfs_append::HdfsAppendAction;
use crate::actions::kill_action::KillAction;
use crate::actions::query::{OutputFormat, QueryAction};
use crate::actions::tag_action::{TagAction, TagOperation};
use crate::actions::update_action::NodeUpdateAction;
use crate::actions::{Action, ValidateAction};
use clap::{Parser, Subcommand};
use log::{info, LevelFilter};
use logforth::append;
use riffle_server::server_state_manager::ServerState;
use tokio::runtime::Runtime;

#[derive(Parser)]
#[command(
    version,
    about = "Riffle cluster ops, benchmarks, and local data inspection"
)]
#[command(arg_required_else_help = true)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Query instances/active_apps/historical_apps
    Query {
        #[arg(short, long)]
        sql: String,
        #[arg(short, long)]
        coordinator_http_url: String,
        /// Output format: table for humans, json for scripts/pipelines
        #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
        format: OutputFormat,
    },
    /// Manage riffle-server instances
    Instance {
        #[command(subcommand)]
        command: InstanceCommands,
    },
    /// IO benchmarks and profilers
    Bench {
        #[command(subcommand)]
        command: BenchCommands,
    },
    /// Inspect local shuffle data
    Inspect {
        #[command(subcommand)]
        command: InspectCommands,
    },
    /// Cluster metadata config for riffle-ctl
    Config {
        #[command(subcommand)]
        command: ConfigCommands,
    },
}

#[derive(Subcommand)]
enum InstanceCommands {
    /// Kill a riffle-server instance
    Kill {
        #[arg(short, long, default_value = "false")]
        force: bool,
        #[arg(short, long)]
        instance: Option<String>,
        /// Interval between kill attempts (seconds). 0 means disabled.
        #[arg(long, default_value = "0")]
        interval_secs: u64,
        /// Total timeout for periodic kill (seconds). 0 means no timeout (single kill).
        #[arg(long, default_value = "0")]
        timeout_secs: u64,
    },
    /// Manage instance status
    Status {
        #[command(subcommand)]
        command: StatusCommands,
    },
    /// Manage instance tags
    Tag {
        #[command(subcommand)]
        command: TagCommands,
    },
}

#[derive(Subcommand)]
enum StatusCommands {
    /// Set instance status (pipeline mode supported)
    Set {
        #[arg(short, long)]
        instance: Option<String>,
        #[arg(short, long)]
        status: ServerState,
    },
}

#[derive(Subcommand)]
enum TagCommands {
    /// Replace instance tags (pipeline mode supported)
    Set {
        #[arg(short, long)]
        instance: Option<String>,
        #[arg(long, required = true, value_delimiter = ',')]
        tags: Vec<String>,
    },
    /// Add a tag (pipeline mode supported)
    Add {
        #[arg(short, long)]
        instance: Option<String>,
        #[arg(long)]
        tag: String,
    },
    /// Delete a tag (pipeline mode supported)
    Delete {
        #[arg(short, long)]
        instance: Option<String>,
        #[arg(long)]
        tag: String,
    },
}

#[derive(Subcommand)]
enum BenchCommands {
    /// Local disk IO benchmarks
    Disk {
        #[command(subcommand)]
        command: DiskBenchCommands,
    },
    /// HDFS IO benchmarks
    Hdfs {
        #[command(subcommand)]
        command: HdfsBenchCommands,
    },
}

#[derive(Subcommand)]
enum DiskBenchCommands {
    /// Benchmark disk append via the riffle IO scheduler
    Append {
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
        /// Enable disk IO throttle layer
        #[arg(short = 't', long)]
        throttle: bool,
    },
    /// Benchmark disk reads via the riffle IO stack
    Read {
        #[arg(short, long)]
        dir: String,
        #[arg(short, long)]
        read_size: String,
        #[arg(short, long)]
        batch_number: usize,
        #[arg(short, long)]
        concurrency: usize,
        /// Enable read-ahead layer
        #[arg(long)]
        read_ahead: bool,
        #[arg(short, long)]
        sleep_millis_per_batch: usize,
    },
    /// Profile disk throughput across block sizes and concurrency
    Profile {
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
}

#[derive(Subcommand)]
enum HdfsBenchCommands {
    /// HDFS append write test
    Append {
        #[arg(short, long)]
        file_path: String,
        #[arg(short, long)]
        total_size: String,
        #[arg(short, long)]
        batch_size: String,
    },
}

#[derive(Subcommand)]
enum InspectCommands {
    /// Check index/data file length consistency and per-block CRC
    FileCheck {
        #[arg(short, long)]
        index_file_path: String,
        #[arg(short, long)]
        data_file_path: String,
    },
}

#[derive(Subcommand)]
enum ConfigCommands {
    /// Initialize cluster-id to coordinator-url mapping
    Init {
        #[arg(short, long)]
        config: String,
    },
}

fn main() -> anyhow::Result<()> {
    logforth::builder()
        .dispatch(|d| {
            d.filter(LevelFilter::Info)
                .append(append::Stdout::default())
        })
        .apply();

    let args = Args::parse();
    if let Commands::Config {
        command: ConfigCommands::Init { config },
    } = &args.command
    {
        meta::Metadata::create(None, config.as_str());
        info!("Succeed to setup config path");
        return Ok(());
    }

    let action = build_action(args.command)?;
    let rt = Runtime::new()?;
    rt.block_on(async {
        if let Err(e) = action.act().await {
            println!("Error: {}", e);
        }
    });

    Ok(())
}

fn build_action(command: Commands) -> anyhow::Result<Box<dyn Action>> {
    let action: Box<dyn Action> = match command {
        Commands::Query {
            sql,
            coordinator_http_url,
            format,
        } => Box::new(QueryAction::new(sql, format, coordinator_http_url)),

        Commands::Instance {
            command:
                InstanceCommands::Kill {
                    force,
                    instance,
                    interval_secs,
                    timeout_secs,
                },
        } => Box::new(KillAction::new(
            force,
            instance,
            interval_secs,
            timeout_secs,
        )),

        Commands::Instance {
            command:
                InstanceCommands::Status {
                    command: StatusCommands::Set { instance, status },
                },
        } => Box::new(NodeUpdateAction::new(instance, Some(status), None)),

        Commands::Instance {
            command:
                InstanceCommands::Tag {
                    command: TagCommands::Set { instance, tags },
                },
        } => {
            if tags.is_empty() {
                anyhow::bail!("--tags must not be empty");
            }
            Box::new(TagAction::new(instance, TagOperation::Update(tags)))
        }

        Commands::Instance {
            command:
                InstanceCommands::Tag {
                    command: TagCommands::Add { instance, tag },
                },
        } => Box::new(TagAction::new(instance, TagOperation::Add(tag))),

        Commands::Instance {
            command:
                InstanceCommands::Tag {
                    command: TagCommands::Delete { instance, tag },
                },
        } => Box::new(TagAction::new(instance, TagOperation::Delete(tag))),

        Commands::Bench {
            command:
                BenchCommands::Disk {
                    command:
                        DiskBenchCommands::Append {
                            dir,
                            batch_number,
                            concurrency,
                            write_size,
                            disk_throughput,
                            throttle,
                        },
                },
        } => Box::new(DiskAppendBenchAction::new(
            dir,
            concurrency,
            write_size,
            batch_number,
            disk_throughput,
            throttle,
        )),

        Commands::Bench {
            command:
                BenchCommands::Disk {
                    command:
                        DiskBenchCommands::Read {
                            dir,
                            read_size,
                            batch_number,
                            concurrency,
                            read_ahead,
                            sleep_millis_per_batch,
                        },
                },
        } => Box::new(DiskReadBenchAction::new(
            dir,
            read_size,
            batch_number,
            concurrency,
            read_ahead,
            sleep_millis_per_batch,
        )),

        Commands::Bench {
            command:
                BenchCommands::Disk {
                    command:
                        DiskBenchCommands::Profile {
                            dir,
                            min_block_size,
                            max_block_size,
                            min_concurrency,
                            max_concurrency,
                            test_duration_secs,
                        },
                },
        } => Box::new(DiskProfiler::new(
            dir,
            min_block_size,
            max_block_size,
            min_concurrency,
            max_concurrency,
            test_duration_secs,
        )),

        Commands::Bench {
            command:
                BenchCommands::Hdfs {
                    command:
                        HdfsBenchCommands::Append {
                            file_path,
                            total_size,
                            batch_size,
                        },
                },
        } => Box::new(HdfsAppendAction::new(
            file_path.as_str(),
            total_size.as_str(),
            batch_size.as_str(),
        )),

        Commands::Inspect {
            command:
                InspectCommands::FileCheck {
                    index_file_path,
                    data_file_path,
                },
        } => Box::new(ValidateAction::new(index_file_path, data_file_path)),

        Commands::Config { .. } => unreachable!("config commands are handled before build_action"),
    };

    Ok(action)
}
