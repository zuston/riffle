use crate::actions::Action;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use console::Term;
use log::{debug, info};
use riffle_server::server_state_manager::ServerState;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::io::{self, BufRead, IsTerminal};
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
    debug!("Executing http request with url: {}", &url);
    let resp = reqwest::get(url).await?;
    let status = resp.status();
    let body = resp.text().await?;
    if !status.is_success() {
        return Err(anyhow::anyhow!(
            "Failed to request kill operation: status={status}, body={body}"
        ));
    }
    if body.trim() != "OK" {
        return Err(anyhow::anyhow!(
            "Failed to request kill operation: status={status}, body={body}"
        ));
    }
    Ok(())
}

/// the returned value is the tag to indicate whether to continue wait
async fn block_wait(ip: &str, http_port: usize) -> Result<bool> {
    let url = format!("http://{}:{}/admin?get_state", ip, http_port);
    let resp = reqwest::get(&url).await?;
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

fn batch_progress_bar_width() -> usize {
    80_usize.saturating_sub(18)
}

fn format_batch_bar_line(prefix: &str, pos: u64, len: u64, width: usize) -> String {
    let filled = if len == 0 {
        0
    } else {
        ((pos as u128 * width as u128) / len as u128) as usize
    };
    let filled = filled.min(width);
    format!(
        "{prefix} {bar} {pos}/{len}",
        prefix = prefix,
        bar = format!("{}{}", "█".repeat(filled), "░".repeat(width - filled)),
        pos = pos,
        len = len,
    )
}

fn batch_progress_message(total: u64, fail: u64, success: u64, len: u64, width: usize) -> String {
    format!(
        "{}\n{}\n{}",
        format_batch_bar_line("Total  ", total, len, width),
        format_batch_bar_line("Fail   ", fail, len, width),
        format_batch_bar_line("Success", success, len, width),
    )
}

const BATCH_PROGRESS_LINES: usize = 3;

struct BatchProgressDisplay {
    term: Term,
    len: u64,
    bar_width: usize,
    last_lines: usize,
}

impl BatchProgressDisplay {
    fn new(len: u64) -> Self {
        Self {
            term: Term::stderr(),
            len,
            bar_width: batch_progress_bar_width(),
            last_lines: 0,
        }
    }

    fn update(&mut self, done: u64, fail: u64, success: u64) -> io::Result<()> {
        if !self.term.is_term() {
            return Ok(());
        }
        debug_assert_eq!(done, fail + success);

        let msg = batch_progress_message(done, fail, success, self.len, self.bar_width);
        let lines: Vec<&str> = msg.lines().collect();
        debug_assert_eq!(lines.len(), BATCH_PROGRESS_LINES);
        let line_count = lines.len();

        if self.last_lines > 0 {
            self.term.clear_last_lines(self.last_lines)?;
        }
        for line in lines {
            self.term.write_line(line)?;
        }
        self.last_lines = line_count;
        Ok(())
    }

    fn finish(&mut self) -> io::Result<()> {
        if self.last_lines > 0 && self.term.is_term() {
            self.term.write_line("")?;
            self.last_lines = 0;
        }
        Ok(())
    }
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
    let len = items.len() as u64;
    let mut progress = BatchProgressDisplay::new(len);
    progress.update(0, 0, 0)?;

    let mut failed_keys = vec![];
    let mut fail = 0_u64;
    let mut success = 0_u64;

    for (idx, item) in items.iter().enumerate() {
        match action_fn(item.clone()).await {
            Ok(_) => success += 1,
            Err(_) => {
                fail += 1;
                failed_keys.push(item.key());
            }
        }
        let done = (idx + 1) as u64;
        progress.update(done, fail, success)?;
    }

    progress.finish()?;

    println!(
        "Total: {}. Success: {}. Failed: {}",
        items.len(),
        success,
        failed_keys.len()
    );
    if !failed_keys.is_empty() {
        println!("Failed list: {:?}", failed_keys);
    }

    Ok(())
}

pub fn read_batch_items<T: BatchActionItem>() -> Result<Vec<T>> {
    let stdin = io::stdin();
    if stdin.is_terminal() {
        return Err(anyhow!(
            "No instance specified. Pass --instance <IP:HTTP_PORT> or pipe JSON targets to stdin"
        ));
    }

    let mut items = vec![];
    for line in stdin.lock().lines() {
        let line = line?.trim().to_string();
        if line.is_empty() || line == "[" || line == "]" {
            continue;
        }
        let line = line.trim_end_matches(',');
        let item: T = serde_json::from_str(line)?;
        items.push(item);
    }
    Ok(items)
}
