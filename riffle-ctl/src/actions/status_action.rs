use crate::actions::kill_action::{read_batch_items, run_batch_action, BatchActionItem};
use crate::actions::Action;
use anyhow::{anyhow, Result};
use riffle_server::server_state_manager::ServerState;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct StatusTarget {
    ip: String,
    http_port: i32,
}

impl BatchActionItem for StatusTarget {
    fn key(&self) -> String {
        format!("{}:{}", self.ip, self.http_port)
    }
}

impl TryFrom<&str> for StatusTarget {
    type Error = anyhow::Error;

    fn try_from(instance: &str) -> Result<Self> {
        let (ip, http_port) = instance
            .split_once(':')
            .ok_or_else(|| anyhow!("Illegal instance target: {instance}"))?;
        Ok(Self {
            ip: ip.to_string(),
            http_port: http_port.parse()?,
        })
    }
}

pub struct StatusAction {
    instance: Option<String>,
    status: ServerState,
}

impl StatusAction {
    pub fn new(instance: Option<String>, status: ServerState) -> Self {
        Self { instance, status }
    }
}

async fn set_status(target: &StatusTarget, status: ServerState) -> Result<()> {
    let url = format!(
        "http://{}:{}/admin?update_state={}",
        target.ip, target.http_port, status
    );
    let response = reqwest::get(url).await?;
    if response.status().is_success() {
        Ok(())
    } else {
        Err(anyhow!(
            "Failed to update remote server status: {}",
            response.text().await?
        ))
    }
}

#[async_trait::async_trait]
impl Action for StatusAction {
    async fn act(&self) -> Result<()> {
        if let Some(instance) = &self.instance {
            return set_status(
                &StatusTarget::try_from(instance.as_str())?,
                self.status.clone(),
            )
            .await;
        }

        let items = read_batch_items::<StatusTarget>()?;
        let status = self.status.clone();
        run_batch_action("Status", items, |item| {
            let status = status.clone();
            async move { set_status(&item, status).await }
        })
        .await
    }
}
