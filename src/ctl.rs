#![allow(dead_code, unused)]

use clap::{Parser, Subcommand};
use tokio::runtime::Runtime;
use uniffle_worker::actions::io_bench::IoBenchAction;
use uniffle_worker::actions::{
    Action, NodeUpdateAction, OutputFormat, QueryAction, ValidateAction,
};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    #[command(about = "Using the riffle IO scheduler to test local disk IO")]
    Iobench {
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
        Commands::Iobench {
            dir,
            batch_number,
            concurrency,
            write_size,
            disk_throughput,
        } => Box::new(IoBenchAction::new(
            dir,
            batch_number,
            write_size,
            concurrency,
            disk_throughput,
        )),

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
