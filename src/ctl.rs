#![allow(dead_code, unused)]

use clap::{Parser, Subcommand};
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
    Validate {
        #[arg(short, long)]
        index_file_path: String,
        #[arg(short, long)]
        data_file_path: String,
    },
    Query {
        #[arg(short, long)]
        sql: String,
        #[arg(short, long)]
        coordinator_http_url: String,
        #[arg(short)]
        pipeline: bool,
    },
    UpdateServerStatus {
        #[arg(short, long)]
        instance: Option<String>,
        #[arg(short, long)]
        status: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let command = args.command;

    let action: Box<dyn Action> = match command {
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

        Commands::UpdateServerStatus { instance, status } => {
            Box::new(NodeUpdateAction::new(instance, status))
        }

        _ => panic!("Unknown command"),
    };

    action.act().await?;

    Ok(())
}
