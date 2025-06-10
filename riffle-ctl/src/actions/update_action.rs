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
    target_status: String,
    is_decommission_grpc_mode: bool,
}

impl NodeUpdateAction {
    pub fn new(
        instance: Option<String>,
        target_status: String,
        is_decommission_grpc_mode: bool,
    ) -> Self {
        Self {
            ip_and_port: instance,
            target_status,
            is_decommission_grpc_mode,
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
    target_status: String,
    decommission_is_grpc_mode: bool,
) -> anyhow::Result<()> {
    let target_status = ServerState::from_str(target_status.as_str())?;

    let ip = update_info.ip.clone();
    let http_port = update_info.http_port;
    let grpc_port = update_info.grpc_port;

    if decommission_is_grpc_mode {
        match &target_status {
            ServerState::DECOMMISSIONING => {
                let mut client =
                    ShuffleServerInternalClient::connect(format!("http://{}:{}", ip, grpc_port))
                        .await?;
                client.decommission(DecommissionRequest::default()).await?;
                return Ok(());
            }
            ServerState::CANCEL_DECOMMISSION => {
                let mut client =
                    ShuffleServerInternalClient::connect(format!("http://{}:{}", ip, grpc_port))
                        .await?;
                client
                    .cancel_decommission(CancelDecommissionRequest::default())
                    .await?;
                return Ok(());
            }
            _ => {}
        }
    }

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
                let grpc_mode = self.is_decommission_grpc_mode;
                async move { update_remote_server_status(&item, status, grpc_mode).await }
            })
            .await?;
            return Ok(());
        }

        let url = self.ip_and_port.clone().unwrap();
        let splits: Vec<_> = url.split(":").collect();
        if splits.len() != 2 {
            panic!("Illegal [id:http_port]={:?}", self.ip_and_port);
        }
        update_remote_server_status(
            &NodeUpdateInfo::from(splits),
            self.target_status.to_string(),
            false,
        )
        .await?;

        Ok(())
    }
}
