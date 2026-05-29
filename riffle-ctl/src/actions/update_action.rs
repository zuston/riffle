use crate::actions::kill_action::{
    read_batch_items, run_batch_action, BatchActionItem, KillTarget,
};
use crate::actions::Action;
use riffle_server::server_state_manager::ServerState;
use serde::{Deserialize, Serialize};

pub struct NodeUpdateAction {
    ip_and_port: Option<String>,
    target_status: Option<ServerState>,
    target_tags: Option<Vec<String>>,
}

impl NodeUpdateAction {
    pub fn new(
        instance: Option<String>,
        target_status: Option<ServerState>,
        target_tags: Option<Vec<String>>,
    ) -> Self {
        Self {
            ip_and_port: instance,
            target_status,
            target_tags,
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

async fn update_remote_server(
    update_info: &NodeUpdateInfo,
    target_status: Option<ServerState>,
    target_tags: Option<Vec<String>>,
) -> anyhow::Result<()> {
    let mut query_parts = vec![];
    if let Some(status) = target_status {
        query_parts.push(format!("update_state={}", status));
    }
    if let Some(tags) = target_tags {
        query_parts.push(format!("update_tags={}", tags.join(",")));
    }
    if query_parts.is_empty() {
        return Err(anyhow::anyhow!("No update operation specified"));
    }

    let url = format!(
        "http://{}:{}/admin?{}",
        update_info.ip.as_str(),
        update_info.http_port,
        query_parts.join("&")
    );
    let resp = reqwest::get(url).await?;
    if !resp.status().is_success() {
        Err(anyhow::anyhow!(
            "Failed to update remote server: {}",
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
            let status = self.target_status.clone();
            let tags = self.target_tags.clone();
            run_batch_action("Update", items, |item| {
                let status = status.clone();
                let tags = tags.clone();
                async move { update_remote_server(&item, status, tags).await }
            })
            .await?;
            return Ok(());
        }

        let url = self.ip_and_port.clone().unwrap();
        let splits: Vec<_> = url.split(":").collect();
        if splits.len() != 2 {
            panic!("Illegal [id:http_port]={:?}", self.ip_and_port);
        }
        update_remote_server(
            &NodeUpdateInfo::from(splits),
            self.target_status.clone(),
            self.target_tags.clone(),
        )
        .await?;

        Ok(())
    }
}
