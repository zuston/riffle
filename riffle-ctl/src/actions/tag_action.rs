use crate::actions::kill_action::{read_batch_items, run_batch_action, BatchActionItem};
use crate::actions::Action;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct TagTarget {
    ip: String,
    http_port: i32,
}

impl BatchActionItem for TagTarget {
    fn key(&self) -> String {
        format!("{}:{}", self.ip, self.http_port)
    }
}

impl From<Vec<&str>> for TagTarget {
    fn from(parts: Vec<&str>) -> Self {
        if parts.len() != 2 {
            panic!("Illegal vars.")
        }
        TagTarget {
            ip: parts.get(0).unwrap().to_string(),
            http_port: parts.get(1).unwrap().parse::<i32>().unwrap(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum TagOperation {
    Update(Vec<String>),
    Add(String),
    Remove(String),
}

pub struct TagAction {
    instance: Option<String>,
    operation: TagOperation,
}

impl TagAction {
    pub fn new(instance: Option<String>, operation: TagOperation) -> Self {
        Self {
            instance,
            operation,
        }
    }
}

async fn apply_tag_operation(target: &TagTarget, operation: &TagOperation) -> anyhow::Result<()> {
    let query = match operation {
        TagOperation::Update(tags) => format!("update_tags={}", tags.join(",")),
        TagOperation::Add(tag) => format!("add_tag={}", tag),
        TagOperation::Remove(tag) => format!("delete_tag={}", tag),
    };
    let url = format!(
        "http://{}:{}/admin?{}",
        target.ip.as_str(),
        target.http_port,
        query
    );
    let resp = reqwest::get(url).await?;
    if !resp.status().is_success() {
        Err(anyhow::anyhow!(
            "Failed to update remote server tags: {}",
            resp.text().await?
        ))
    } else {
        Ok(())
    }
}

#[async_trait::async_trait]
impl Action for TagAction {
    async fn act(&self) -> anyhow::Result<()> {
        if self.instance.is_none() {
            let items = read_batch_items::<TagTarget>()?;
            let operation = self.operation.clone();
            run_batch_action("Tag", items, |item| {
                let op = operation.clone();
                async move { apply_tag_operation(&item, &op).await }
            })
            .await?;
            return Ok(());
        }

        let url = self.instance.clone().unwrap();
        let splits: Vec<_> = url.split(':').collect();
        if splits.len() != 2 {
            panic!("Illegal [id:http_port]={:?}", self.instance);
        }
        apply_tag_operation(&TagTarget::from(splits), &self.operation).await?;

        Ok(())
    }
}

pub fn parse_tags(raw_tags: &str) -> Vec<String> {
    if raw_tags.is_empty() {
        return vec![];
    }
    raw_tags
        .split(',')
        .map(|tag| tag.trim().to_string())
        .filter(|tag| !tag.is_empty())
        .collect()
}
