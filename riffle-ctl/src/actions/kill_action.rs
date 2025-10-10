use crate::actions::Action;
use anyhow::Result;
use async_trait::async_trait;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::info;
use riffle_server::server_state_manager::ServerState;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::io::{self, BufRead};
use std::str::FromStr;

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

    // params for blocking wait to riffle-server exit.
    // but this is only valid for single instance rather than batch mode
    interval_secs: u64,
    timeout_secs: u64,
}

impl KillAction {
    pub fn new(
        force: bool,
        instance: Option<String>,
        interval_secs: u64,
        timeout_secs: u64,
    ) -> Self {
        Self {
            force,
            instance,
            interval_secs,
            timeout_secs,
        }
    }
}

async fn do_kill(ip: &str, http_port: usize, force: bool) -> Result<()> {
    let url = format!(
        "http://{}:{}/admin?kill{}",
        ip,
        http_port,
        if force { "=force" } else { "" },
    );
    info!("Executing http request with url: {}", &url);
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

/// the returned value is the tag to indicate whether to continue wait
async fn block_wait(ip: &str, http_port: usize) -> Result<bool> {
    let url = format!("http://{}:{}/admin?get_state", ip, http_port);
    let resp = reqwest::get(url).await?;
    if resp.status().is_success() {
        let result = resp.text().await?;
        match ServerState::from_str(&result.to_ascii_uppercase()).ok() {
            None => Ok(true),
            Some(state) => {
                if state == ServerState::DECOMMISSIONED {
                    info!("now target status is DECOMMISSIONED");
                    Ok(false)
                } else {
                    Ok(true)
                }
            }
        }
    } else {
        info!("unreachable request with url: {}", &url);
        Ok(false)
    }
}

#[async_trait::async_trait]
impl Action for KillAction {
    async fn act(&self) -> Result<()> {
        if self.instance.is_none() {
            // batch mode
            let items = read_batch_items::<KillTarget>()?;
            let force = self.force;

            run_batch_action("Kill", items, |item| {
                let value = force;
                async move { do_kill(item.ip.as_str(), item.http_port as usize, value).await }
            })
            .await?;

            return Ok(());
        } else {
            // single instance mode
            let url = self.instance.clone().unwrap();
            let splits: Vec<_> = url.split(':').collect();
            if splits.len() != 2 {
                panic!("Illegal [id:http_port]={:?}", self.instance);
            }
            let ip = splits.get(0).unwrap();
            let http_port = splits.get(1).unwrap().parse::<usize>().unwrap();

            do_kill(ip, http_port, self.force).await?;

            if self.interval_secs > 0 && self.timeout_secs > 0 {
                use tokio::time::{sleep, Duration, Instant};
                let start = Instant::now();
                while start.elapsed().as_secs() < self.timeout_secs {
                    if block_wait(ip, http_port).await? {
                        sleep(Duration::from_secs(self.interval_secs)).await;
                    } else {
                        info!("the kill operation with blocking wait has been finished that costs {} secs", start.elapsed().as_secs());
                        break;
                    }
                }
            }
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
