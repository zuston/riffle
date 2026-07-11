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
    about = "Riffle cluster ops, benchmarks, and local data inspection",
    after_help = "Examples:\n  riffle-ctl query --coordinator http://127.0.0.1:21001 --sql \"select * from instances\"\n  riffle-ctl instance set --instance 192.168.1.1:19998 --status unhealthy\n  riffle-ctl bench disk profile --dir /data1/bench"
)]
#[command(arg_required_else_help = true)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Query instances/active_apps/historical_apps
    #[command(
        after_help = "Examples:\n  riffle-ctl query --coordinator http://127.0.0.1:21001 --sql \"select * from active_apps\"\n  riffle-ctl query --coordinator production --sql \"select * from instances\" --json"
    )]
    Query {
        #[arg(short, long)]
        sql: String,
        /// Coordinator URL or configured cluster ID
        #[arg(short, long)]
        coordinator: String,
        /// Emit JSON for scripts and pipelines
        #[arg(long)]
        json: bool,
    },
    /// Manage riffle-server instances
    #[command(
        after_help = "Examples:\n  riffle-ctl instance set --instance 192.168.1.1:19998 --status unhealthy\n  riffle-ctl instance add --instance 192.168.1.1:19998 --tag production\n  riffle-ctl instance kill --instance 192.168.1.1:19998 --force"
    )]
    Instance {
        /// Target instance. Omit to read targets as JSON from stdin.
        #[arg(short, long, global = true, value_name = "IP:HTTP_PORT")]
        instance: Option<String>,
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
    /// Set instance status and/or replace tags
    #[command(
        after_help = "Examples:\n  riffle-ctl instance set --instance 192.168.1.1:19998 --status unhealthy\n  riffle-ctl instance set --instance 192.168.1.1:19998 --status unhealthy --tags sata,maintenance\n  riffle-ctl query --coordinator production --sql \"select * from instances\" --json | riffle-ctl instance set --status unhealthy"
    )]
    Set(SetInstanceArgs),
    /// Add a tag to an instance
    #[command(
        after_help = "Examples:\n  riffle-ctl instance add --instance 192.168.1.1:19998 --tag production\n  riffle-ctl query --coordinator production --sql \"select * from instances\" --json | riffle-ctl instance add --tag production"
    )]
    Add {
        #[arg(long)]
        tag: String,
    },
    /// Remove a tag from an instance
    #[command(
        after_help = "Examples:\n  riffle-ctl instance remove --instance 192.168.1.1:19998 --tag deprecated\n  riffle-ctl query --coordinator production --sql \"select * from instances\" --json | riffle-ctl instance remove --tag deprecated"
    )]
    Remove {
        #[arg(long)]
        tag: String,
    },
    /// Kill a riffle-server instance
    #[command(
        after_help = "Examples:\n  riffle-ctl instance kill --instance 192.168.1.1:19998\n  riffle-ctl instance kill --instance 192.168.1.1:19998 --force\n  riffle-ctl instance kill --instance 192.168.1.1:19998 --interval-secs 5 --timeout-secs 120"
    )]
    Kill {
        #[arg(short, long, default_value = "false")]
        force: bool,
        /// Interval between kill attempts (seconds). 0 means disabled.
        #[arg(long, default_value = "0")]
        interval_secs: u64,
        /// Total timeout for periodic kill (seconds). 0 means no timeout (single kill).
        #[arg(long, default_value = "0")]
        timeout_secs: u64,
    },
}

#[derive(clap::Args)]
#[group(required = true, multiple = true)]
struct SetInstanceArgs {
    #[arg(short, long)]
    status: Option<ServerState>,
    #[arg(long, value_delimiter = ',')]
    tags: Option<Vec<String>>,
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
    #[command(
        after_help = "Example:\n  riffle-ctl bench disk append --dir /data1/bench --batch-number 100 --concurrency 200 --write-size 10M --disk-throughput 100M --throttle"
    )]
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
    #[command(
        after_help = "Example:\n  riffle-ctl bench disk read --dir /data1/bench --read-size 1M --batch-number 100 --concurrency 32 --read-ahead"
    )]
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
        #[arg(short, long, default_value = "0")]
        sleep_millis_per_batch: usize,
    },
    /// Profile disk throughput across block sizes and concurrency
    #[command(
        after_help = "Example:\n  riffle-ctl bench disk profile --dir /data1/bench --min-block-size 4KB --max-block-size 64MB --min-concurrency 1 --max-concurrency 16 --test-duration-secs 10"
    )]
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
    #[command(
        after_help = "Example:\n  riffle-ctl bench hdfs append --file-path hdfs://namenode/riffle/bench.data --total-size 10G --batch-size 4M"
    )]
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
    #[command(
        after_help = "Example:\n  riffle-ctl inspect file --index-file /data/shuffle/index --data-file /data/shuffle/data"
    )]
    File {
        #[arg(short, long)]
        index_file: String,
        #[arg(short, long)]
        data_file: String,
    },
}

#[derive(Subcommand)]
enum ConfigCommands {
    /// Initialize cluster-id to coordinator-url mapping
    #[command(after_help = "Example:\n  riffle-ctl config init --file ./clusters.toml")]
    Init {
        #[arg(short, long)]
        file: String,
    },
}

fn main() -> anyhow::Result<()> {
    logforth::builder()
        .dispatch(|d| {
            d.filter(LevelFilter::Info)
                .append(append::Stderr::default())
        })
        .apply();

    let args = Args::parse();
    if let Commands::Config {
        command: ConfigCommands::Init { file },
    } = &args.command
    {
        meta::Metadata::create(None, file.as_str());
        info!("Succeed to setup config path");
        return Ok(());
    }

    let action = build_action(args.command)?;
    let rt = Runtime::new()?;
    rt.block_on(action.act())
}

fn build_action(command: Commands) -> anyhow::Result<Box<dyn Action>> {
    let action: Box<dyn Action> = match command {
        Commands::Query {
            sql,
            coordinator,
            json,
        } => Box::new(QueryAction::new(
            sql,
            if json {
                OutputFormat::Json
            } else {
                OutputFormat::Table
            },
            coordinator,
        )),

        Commands::Instance {
            instance,
            command:
                InstanceCommands::Kill {
                    force,
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
            instance,
            command: InstanceCommands::Set(SetInstanceArgs { status, tags }),
        } => {
            if tags.as_ref().is_some_and(Vec::is_empty) {
                anyhow::bail!("--tags must not be empty");
            }
            Box::new(NodeUpdateAction::new(instance, status, tags))
        }

        Commands::Instance {
            instance,
            command: InstanceCommands::Add { tag },
        } => Box::new(TagAction::new(instance, TagOperation::Add(tag))),

        Commands::Instance {
            instance,
            command: InstanceCommands::Remove { tag },
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
                InspectCommands::File {
                    index_file,
                    data_file,
                },
        } => Box::new(ValidateAction::new(index_file, data_file)),

        Commands::Config { .. } => unreachable!("config commands are handled before build_action"),
    };

    Ok(action)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn parse_query_json_command() {
        let args = Args::try_parse_from([
            "riffle-ctl",
            "query",
            "--coordinator",
            "production",
            "--sql",
            "select * from instances",
            "--json",
        ])
        .unwrap();

        assert!(matches!(
            args.command,
            Commands::Query {
                coordinator,
                json: true,
                ..
            } if coordinator == "production"
        ));
    }

    #[test]
    fn parse_instance_selector_before_or_after_subcommand() {
        for argv in [
            vec![
                "riffle-ctl",
                "instance",
                "--instance",
                "192.168.1.1:19998",
                "set",
                "--status",
                "unhealthy",
            ],
            vec![
                "riffle-ctl",
                "instance",
                "set",
                "--status",
                "unhealthy",
                "--instance",
                "192.168.1.1:19998",
            ],
        ] {
            let args = Args::try_parse_from(argv).unwrap();
            assert!(matches!(
                args.command,
                Commands::Instance {
                    instance: Some(instance),
                    command: InstanceCommands::Set(SetInstanceArgs {
                        status: Some(ServerState::UNHEALTHY),
                        ..
                    }),
                } if instance == "192.168.1.1:19998"
            ));
        }
    }

    #[test]
    fn parse_instance_pipeline_commands() {
        let set = Args::try_parse_from([
            "riffle-ctl",
            "instance",
            "set",
            "--status",
            "unhealthy",
            "--tags",
            "sata,maintenance",
        ])
        .unwrap();
        assert!(matches!(
            set.command,
            Commands::Instance {
                instance: None,
                command: InstanceCommands::Set(SetInstanceArgs {
                    status: Some(ServerState::UNHEALTHY),
                    tags: Some(tags),
                }),
            } if tags == vec!["sata".to_string(), "maintenance".to_string()]
        ));

        let remove =
            Args::try_parse_from(["riffle-ctl", "instance", "remove", "--tag", "deprecated"])
                .unwrap();
        assert!(matches!(
            remove.command,
            Commands::Instance {
                instance: None,
                command: InstanceCommands::Remove { tag },
            } if tag == "deprecated"
        ));
    }

    #[test]
    fn instance_set_requires_a_change() {
        assert!(Args::try_parse_from(["riffle-ctl", "instance", "set"]).is_err());
    }

    #[test]
    fn parse_simplified_inspect_and_config_commands() {
        let inspect = Args::try_parse_from([
            "riffle-ctl",
            "inspect",
            "file",
            "--index-file",
            "/data/index",
            "--data-file",
            "/data/data",
        ])
        .unwrap();
        assert!(matches!(
            inspect.command,
            Commands::Inspect {
                command: InspectCommands::File { .. },
            }
        ));

        let config =
            Args::try_parse_from(["riffle-ctl", "config", "init", "--file", "clusters.toml"])
                .unwrap();
        assert!(matches!(
            config.command,
            Commands::Config {
                command: ConfigCommands::Init { file },
            } if file == "clusters.toml"
        ));
    }

    #[test]
    fn help_includes_examples() {
        let command = Args::command();
        let mut set_command = command
            .find_subcommand("instance")
            .unwrap()
            .find_subcommand("set")
            .unwrap()
            .clone();
        let help = set_command.render_long_help().to_string();

        assert!(help.contains("Examples:"));
        assert!(help.contains("riffle-ctl instance set"));
    }
}
