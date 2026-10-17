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

use anyhow::Result;
use clap::Parser;
use log::info;
use riffle_server::config::Config;
#[cfg(not(feature = "logforth"))]
use riffle_server::log_service::LogService;
#[cfg(feature = "logforth")]
use riffle_server::logforth_service::LogService;
use riffle_server::server::Server;
use tokio::signal::unix::{signal, SignalKind};

#[cfg(all(unix, feature = "allocator-analysis"))]
const MAX_MEMORY_ALLOCATION_SIZE_ENV_KEY: &str = "MAX_MEMORY_ALLOCATION_LIMIT_SIZE";

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg[short, long]]
    config: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    setup_max_memory_allocation();
    let args = Args::parse();
    let config = Config::from(&args.config);

    #[cfg(feature = "logforth")]
    let _guard = LogService::init(config.log.as_ref());
    #[cfg(not(feature = "logforth"))]
    let _guard = config.log.as_ref().map(LogService::init);

    info!(
        "Riffle is built on the git commit hash: {}",
        env!("GIT_COMMIT_HASH")
    );
    #[cfg(feature = "deadlock-detection")]
    riffle_server::deadlock::detect_deadlock();
    riffle_server::panic_hook::set_panic_hook();
    info!("The specified config show as follows: \n {:#?}", config);

    let mut server = Server::start(config).await?;
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;
    tokio::select! {
        result = server.wait() => return result,
        _ = sigterm.recv() => info!("Received SIGTERM, shutting down server"),
        _ = sigint.recv() => info!("Received SIGINT, shutting down server"),
    }
    server.shutdown().await
}

fn setup_max_memory_allocation() {
    #[cfg(all(unix, feature = "allocator-analysis"))]
    {
        use bytesize::ByteSize;
        use riffle_server::mem_allocator::ALLOCATOR;
        use std::str::FromStr;

        let _ = std::env::var(MAX_MEMORY_ALLOCATION_SIZE_ENV_KEY).map(|v| {
            let readable_size = ByteSize::from_str(v.as_str()).unwrap();
            ALLOCATOR.set_limit(readable_size.as_u64() as usize)
        });
    }
}
