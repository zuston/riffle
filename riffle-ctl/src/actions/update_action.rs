use crate::actions::kill_action::{
    read_batch_items, run_batch_action, BatchActionItem, KillTarget,
};
use crate::actions::Action;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use riffle_server::grpc::protobuf::uniffle::shuffle_server_internal_client::ShuffleServerInternalClient;
use riffle_server::grpc::protobuf::uniffle::{CancelDecommissionRequest, DecommissionRequest};
use riffle_server::server_state_manager::ServerState;
use serde::{Deserialize, Serialize};
use std::io;
use std::str::FromStr;

pub struct NodeUpdateAction {
    ip_and_port: Option<String>,
    target_status: ServerState,
}

impl NodeUpdateAction {
    pub fn new(instance: Option<String>, target_status: ServerState) -> Self {
        Self {
            ip_and_port: instance,
            target_status,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct NodeUpdateInfo {
    ip: String,
    http_port: i32,
    grpc_port: i32,
}

impl From<Vec<&str>> for NodeUpdateInfo {
    fn from(parts: Vec<&str>) -> Self {
        if parts.len() != 2 {
            panic!("Illegal vars.")
        }
        NodeUpdateInfo {
            ip: parts.get(0).unwrap().to_string(),
            http_port: parts.get(1).unwrap().parse::<i32>().unwrap(),
            grpc_port: 0,
        }
    }
}

async fn update_remote_server_status(
    update_info: &NodeUpdateInfo,
    target_status: ServerState,
) -> anyhow::Result<()> {
    let ip = update_info.ip.clone();
    let http_port = update_info.http_port;
    let grpc_port = update_info.grpc_port;

    let url = format!(
        "http://{}:{}/admin?update_state={}",
        ip.as_str(),
        http_port,
        target_status
    );
    let resp = reqwest::get(url).await?;
    if !resp.status().is_success() {
        Err(anyhow::anyhow!(
            "Failed to update remote server status: {}",
            resp.text().await?
        ))
    } else {
        Ok(())
    }
}

impl BatchActionItem for NodeUpdateInfo {
    fn key(&self) -> String {
        format!("{}:{}", self.ip, self.http_port)
    }
}

#[async_trait::async_trait]
impl Action for NodeUpdateAction {
    async fn act(&self) -> anyhow::Result<()> {
        if self.ip_and_port.is_none() {
            let items = read_batch_items::<NodeUpdateInfo>()?;
            run_batch_action("Kill", items, |item| {
                let status = self.target_status.clone();
                async move { update_remote_server_status(&item, status).await }
            })
            .await?;
            return Ok(());
        }

        let url = self.ip_and_port.clone().unwrap();
        let splits: Vec<_> = url.split(":").collect();
        if splits.len() != 2 {
            panic!("Illegal [id:http_port]={:?}", self.ip_and_port);
        }
        update_remote_server_status(&NodeUpdateInfo::from(splits), self.target_status.clone())
            .await?;

        Ok(())
    }
}
