use crate::actions::Action;
use anyhow::Result;
use async_trait::async_trait;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::io::{self, BufRead};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct KillTarget {
    pub ip: String,
    pub http_port: i32,
}

impl BatchActionItem for KillTarget {
    fn key(&self) -> String {
        format!("{}:{}", self.ip, self.http_port)
    }
}

pub struct KillAction {
    force: bool,
    instance: Option<String>,
}

impl KillAction {
    pub fn new(force: bool, instance: Option<String>) -> Self {
        Self { force, instance }
    }
}

async fn kill_request(ip: &str, http_port: usize, force: bool) -> Result<()> {
    let operation_name = if force { "FORCE_KILL" } else { "KILL" };
    let url = format!(
        "http://{}:{}/admin?operation={}",
        ip, http_port, operation_name
    );
    let resp = reqwest::get(url).await?;
    if !resp.status().is_success() {
        Err(anyhow::anyhow!(
            "Failed to request kill operation: {}",
            resp.text().await?
        ))
    } else {
        Ok(())
    }
}

#[async_trait::async_trait]
impl Action for KillAction {
    async fn act(&self) -> Result<()> {
        if self.instance.is_none() {
            let items = read_batch_items::<KillTarget>()?;
            let force = self.force;
            run_batch_action("Kill", items, |item| {
                let value = force.clone();
                async move { kill_request(item.ip.as_str(), item.http_port as usize, value).await }
            })
            .await?;
            return Ok(());
        } else {
            let url = self.instance.clone().unwrap();
            let splits: Vec<_> = url.split(":").collect();
            if splits.len() != 2 {
                panic!("Illegal [id:http_port]={:?}", self.instance);
            }
            let ip = splits.get(0).unwrap();
            let http_port = splits.get(1).unwrap().parse::<usize>().unwrap();
            kill_request(ip, http_port, self.force).await?;
        }
        Ok(())
    }
}

pub trait BatchActionItem: Sized + DeserializeOwned {
    fn key(&self) -> String;
}

pub async fn run_batch_action<T, F, Fut>(
    action_name: &str,
    items: Vec<T>,
    mut action_fn: F,
) -> anyhow::Result<()>
where
    T: BatchActionItem + Clone,
    F: FnMut(T) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    let multi_progress = MultiProgress::new();

    let total_pb = multi_progress.add(ProgressBar::new(items.len() as u64));
    total_pb
        .set_style(ProgressStyle::default_bar().template("{prefix:.bold} {wide_bar} {pos}/{len}")?);
    total_pb.set_prefix("Total  ");

    let fail_pb = multi_progress.add(ProgressBar::new(items.len() as u64));
    fail_pb
        .set_style(ProgressStyle::default_bar().template("{prefix:.bold} {wide_bar} {pos}/{len}")?);
    fail_pb.set_prefix("Fail   ");

    let success_pb = multi_progress.add(ProgressBar::new(items.len() as u64));
    success_pb
        .set_style(ProgressStyle::default_bar().template("{prefix:.bold} {wide_bar} {pos}/{len}")?);
    success_pb.set_prefix("Success");

    let mut failed_keys = vec![];
    for item in &items {
        match action_fn(item.clone()).await {
            Ok(_) => success_pb.inc(1),
            Err(_) => {
                fail_pb.inc(1);
                failed_keys.push(item.key());
            }
        }
        total_pb.inc(1);
    }

    total_pb.finish();
    success_pb.finish();
    fail_pb.finish();

    println!("\nTotal: {}. Failed: {}", items.len(), failed_keys.len());
    if !failed_keys.is_empty() {
        println!("Failed list: {:?}", failed_keys);
    }

    Ok(())
}

pub fn read_batch_items<T: BatchActionItem>() -> Result<Vec<T>> {
    let stdin = io::stdin();
    let mut items = vec![];
    for line in stdin.lock().lines() {
        let line = line?;
        if line.is_empty() {
            break;
        }
        let item: T = serde_json::from_str(&line)?;
        items.push(item);
    }
    Ok(items)
}
