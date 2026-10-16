// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use clap::Parser;
use log::info;
use logforth::record::{Level, LevelFilter};
use riffle_coordinator::config::Config;
use riffle_coordinator::coordinator::Coordinator;
use riffle_server::log_service::LogService;
use tokio::signal::unix::{signal, SignalKind};

#[derive(Parser, Debug)]
#[command(name = "riffle-coordinator")]
#[command(author = "Apache Software Foundation")]
#[command(version)]
#[command(about = "Riffle Coordinator - Uniffle Shuffle Service Coordinator", long_about = None)]
struct Args {
    /// Configuration file path
    #[arg[short, long]]
    config: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config = Config::load(args.config.as_deref())?;

    // Initialize logging
    let _log_guard = match &config.log {
        None => {
            logforth::starter_log::stdout()
                .filter(LevelFilter::MoreSevereEqual(Level::Info))
                .apply();
            None
        }
        Some(log_config) => {
            let _guard = LogService::init(log_config);
            Some(_guard)
        }
    };

    let mut coordinator = Coordinator::start(config).await?;
    let mut sigterm = signal(SignalKind::terminate())?;
    tokio::select! {
        result = coordinator.wait() => result,
        _ = sigterm.recv() => {
            info!("Received SIGTERM, shutting down coordinator");
            coordinator.shutdown().await
        }
    }
}
