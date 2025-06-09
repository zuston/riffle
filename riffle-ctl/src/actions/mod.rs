use crate::actions::query::SessionContextExtend;
use bytes::{Buf, Bytes};
use clap::builder::Str;
use dashmap::DashMap;
use datafusion::dataframe::DataFrameWriteOptions;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use riffle_server::grpc::protobuf::uniffle::shuffle_server_client::ShuffleServerClient;
use riffle_server::grpc::protobuf::uniffle::shuffle_server_internal_client::ShuffleServerInternalClient;
use riffle_server::grpc::protobuf::uniffle::{CancelDecommissionRequest, DecommissionRequest};
use riffle_server::server_state_manager::ServerState;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::{fs, io, thread};
use url::Url;

pub mod discovery;
pub mod disk_append_bench;
pub mod disk_profiler;
pub mod disk_read_bench;
pub mod kill_action;
pub mod postgres_server;
pub mod query;
pub mod update_action;

#[async_trait::async_trait]
pub trait Action {
    async fn act(&self) -> anyhow::Result<()>;
}

pub enum OutputFormat {
    TABLE,
    JSON,
}

pub struct QueryAction {
    sql: String,
    format: OutputFormat,
    coordinator_url: String,
}

impl QueryAction {
    pub fn new(sql: String, format: OutputFormat, coordinator_url: String) -> Self {
        Self {
            sql,
            format,
            coordinator_url,
        }
    }
}

#[async_trait::async_trait]
impl Action for QueryAction {
    async fn act(&self) -> anyhow::Result<()> {
        let context = SessionContextExtend::new(self.coordinator_url.as_str()).await?;
        let df = context.sql(self.sql.as_str()).await?;
        match &self.format {
            OutputFormat::TABLE => {
                df.show().await?;
            }
            OutputFormat::JSON => {
                let temp_dir = tempfile::tempdir().unwrap();
                let file_path = temp_dir.path().join("output.json");
                let absolute_file_path = file_path.to_str().unwrap();
                df.write_json(absolute_file_path, DataFrameWriteOptions::new(), None)
                    .await?;
                let contents = fs::read_to_string(absolute_file_path)?;
                println!("{}", contents);
            }
        }
        Ok(())
    }
}

pub struct ValidateAction {
    index_path: String,
    data_path: String,
}
impl ValidateAction {
    pub fn new(index_path: String, data_path: String) -> Self {
        Self {
            index_path,
            data_path,
        }
    }
}

#[async_trait::async_trait]
impl Action for ValidateAction {
    async fn act(&self) -> anyhow::Result<()> {
        let index_data = fs::read(&self.index_path)?;
        let mut index_data = Bytes::copy_from_slice(&index_data);

        let data = fs::read(&self.data_path)?;
        let data = Bytes::copy_from_slice(&data);

        // check the length
        let mut index_clone = index_data.clone();
        let mut expected_len = 0;
        let batch = index_clone.len() / 40;
        for idx in 0..batch {
            let offset = index_clone.get_i64();
            let length = index_clone.get_i32();
            let uncompress_len = index_clone.get_i32();
            let crc = index_clone.get_i64();
            let block_id = index_clone.get_i64();
            let task_id = index_clone.get_i64();

            expected_len += length as usize;
        }

        if expected_len != data.len() {
            println!(
                "index recorded data len: {}. real: {}",
                expected_len,
                data.len()
            );
            return Ok(());
        }

        for idx in 0..batch {
            let offset = index_data.get_i64();
            let length = index_data.get_i32();
            let uncompress_len = index_data.get_i32();
            let crc = index_data.get_i64();
            let block_id = index_data.get_i64();
            let task_id = index_data.get_i64();

            let partial = data.slice((offset as usize..(offset + (length as i64)) as usize));
            let data_crc = riffle_server::util::get_crc(&partial);
            if crc != data_crc {
                println!(
                    "blockId: {}, crc is not correct. expected: {}, real: {}. total batch: {}. batch index: {}",
                    block_id, crc, data_crc, batch, idx
                );
            }
        }
        Ok(())
    }
}
