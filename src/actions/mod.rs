use crate::actions::query::SessionContextExtend;
use crate::grpc::protobuf::uniffle::shuffle_server_client::ShuffleServerClient;
use crate::grpc::protobuf::uniffle::shuffle_server_internal_client::ShuffleServerInternalClient;
use crate::grpc::protobuf::uniffle::{
    shuffle_server_internal_client, CancelDecommissionRequest, DecommissionRequest, ServerStatus,
};
use crate::util::get_crc;
use bytes::{Buf, Bytes};
use clap::builder::Str;
use dashmap::DashMap;
use datafusion::dataframe::DataFrameWriteOptions;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use std::{fs, io, thread};
use url::Url;

pub mod discovery;
pub mod query;

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
            let data_crc = get_crc(&partial);
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

pub struct NodeUpdateAction {
    ip_and_port: Option<String>,
    target_status: String,
}

impl NodeUpdateAction {
    pub fn new(ip_and_port: Option<String>, target_status: String) -> Self {
        Self {
            ip_and_port,
            target_status,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct NodeUpdateInfo {
    ip: String,
    grpc_port: i32,
}

enum NodeUpdateOperation {
    DECOMMISSION,
    CANCEL_DECOMMISSION,
}

async fn do_decommission_operation(
    ip_and_port: String,
    grpc_port: i32,
    op: &NodeUpdateOperation,
) -> anyhow::Result<()> {
    // thread::sleep(Duration::from_millis(5));
    // return Ok(());

    let mut client =
        ShuffleServerInternalClient::connect(format!("http://{}:{}", ip_and_port, grpc_port))
            .await?;
    match op {
        NodeUpdateOperation::DECOMMISSION => {
            client.decommission(DecommissionRequest::default()).await?;
        }
        NodeUpdateOperation::CANCEL_DECOMMISSION => {
            client
                .cancel_decommission(CancelDecommissionRequest::default())
                .await?;
        }
    };

    Ok(())
}

#[async_trait::async_trait]
impl Action for NodeUpdateAction {
    async fn act(&self) -> anyhow::Result<()> {
        let operation = match self.target_status.as_str() {
            "decommission" => NodeUpdateOperation::DECOMMISSION,
            "cancel_decommission" => NodeUpdateOperation::CANCEL_DECOMMISSION,
            _ => panic!("invalid target status: {}", self.target_status),
        };

        // use pipeline mode
        if self.ip_and_port.is_none() {
            let stdin = io::stdin();
            let mut infos = vec![];
            for line in stdin.lines() {
                let line = line?;
                if line.is_empty() {
                    break;
                }
                let info: NodeUpdateInfo = serde_json::from_str(line.as_str())?;
                infos.push(info);
            }

            let multi_progress = MultiProgress::new();

            let total_pb = multi_progress.add(ProgressBar::new(infos.len() as u64));
            total_pb.set_style(
                ProgressStyle::default_bar().template("{prefix:.bold} {wide_bar} {pos}/{len}")?,
            );
            total_pb.set_prefix("Total  ");

            let fail_pb = multi_progress.add(ProgressBar::new(infos.len() as u64));
            fail_pb.set_style(
                ProgressStyle::default_bar().template("{prefix:.bold} {wide_bar} {pos}/{len}")?,
            );
            fail_pb.set_prefix("Fail   ");

            let success_pb = multi_progress.add(ProgressBar::new(infos.len() as u64));
            success_pb.set_style(
                ProgressStyle::default_bar().template("{prefix:.bold} {wide_bar} {pos}/{len}")?,
            );
            success_pb.set_prefix("Success");

            let mut failed_ips = vec![];
            for info in &infos {
                match do_decommission_operation(info.ip.clone(), info.grpc_port, &operation).await {
                    Ok(_) => success_pb.inc(1),
                    Err(_) => {
                        fail_pb.inc(1);
                        failed_ips.push(info.ip.clone());
                    }
                }
                total_pb.inc(1);
            }

            total_pb.finish();
            success_pb.finish();
            fail_pb.finish();

            println!("\nTotal: {}. Failed: {}", infos.len(), failed_ips.len());
            if failed_ips.len() > 0 {
                println!("Failed list: {:?}", failed_ips);
            }

            return Ok(());
        }

        let url = self.ip_and_port.clone().unwrap();
        let splits: Vec<_> = url.split(":").collect();
        if splits.len() != 2 {
            panic!("Illegal id_and_port: {:?}", self.ip_and_port);
        }
        do_decommission_operation(
            splits.get(0).unwrap().to_string(),
            splits.get(1).unwrap().parse()?,
            &operation,
        )
        .await?;

        Ok(())
    }
}
