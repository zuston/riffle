#![allow(dead_code, unused)]

use bytes::{Buf, Bytes};
use clap::builder::Str;
use clap::{Parser, Subcommand};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Use::Default;
use std::fs;
use tonic::Status;
use uniffle_worker::actions::discovery::ServerStatus;
use uniffle_worker::actions::{
    Action, NodeUpdateAction, OutputFormat, QueryAction, ValidateAction,
};
use uniffle_worker::util::get_crc;

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
    },
    UPDATE {
        #[arg(short, long)]
        instance: String,
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
        } => Box::new(QueryAction::new(
            sql,
            OutputFormat::TABLE,
            coordinator_http_url,
        )),

        Commands::UPDATE { instance, status } => Box::new(NodeUpdateAction::new(instance, status)),

        _ => panic!("Unknown command"),
    };

    action.act().await?;

    Ok(())
}
