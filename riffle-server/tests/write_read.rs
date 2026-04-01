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

#[cfg(test)]
mod tests {
    use anyhow::{anyhow, Result};
    use log::info;
    use riffle_server::config::Config;
    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    use riffle_server::config::{RpcVersion, UrpcConfig};
    use riffle_server::metric::GAUGE_MEMORY_ALLOCATED;
    use riffle_server::mini_riffle;
    use riffle_server::mini_riffle::shuffle_testing;
    use riffle_server::urpc::client::EpollUrpcClient;
    use riffle_server::urpc::command::GetLocalDataRequestCommand;
    use riffle_server::util;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Semaphore;

    fn init_logger() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    /// Probe the urpc service at `urpc_port` with an EpollUrpcClient until it responds.
    async fn wait_for_urpc_epoll_ready(urpc_port: u16) -> Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let mut last_err = None;

        while tokio::time::Instant::now() < deadline {
            match EpollUrpcClient::connect("0.0.0.0", urpc_port as usize).await {
                Ok(mut client) => {
                    let probe_req = GetLocalDataRequestCommand::new(
                        0,
                        "__urpc_probe_app__".to_string(),
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                    );
                    match tokio::time::timeout(
                        Duration::from_millis(800),
                        client.get_local_shuffle_data(probe_req),
                    )
                    .await
                    {
                        Ok(Ok(_)) | Ok(Err(_)) => return Ok(()),
                        Err(timeout) => {
                            last_err = Some(anyhow!("probe timed out: {timeout}"));
                        }
                    }
                }
                Err(err) => {
                    last_err = Some(err);
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Err(anyhow!(
            "urpc epoll service not ready within 10s, last error: {:?}",
            last_err
        ))
    }

    /// Probe the urpc service at `urpc_port` until it responds.
    /// Uses EpollUrpcClient (epoll) to test server, regardless of server transport.
    /// This isolates server-side io-uring issues from client-side issues.
    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    async fn wait_for_urpc_uring_ready(urpc_port: u16) -> Result<()> {
        // Use epoll client to probe server - only testing server-side io-uring
        wait_for_urpc_epoll_ready(urpc_port).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shuffle_write_read_testing() -> Result<()> {
        init_logger();
        let temp_dir = tempdir::TempDir::new("test_write_read").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        info!("temp file path: {} created", &temp_path);

        let grpc_port = 21101;
        let urpc_port = 21102;
        let mut config =
            Config::create_mem_localfile_config(grpc_port, "1G".to_string(), temp_path);
        config.urpc_port = Some(urpc_port);
        config.hybrid_store.memory_single_buffer_max_spill_size = Some("1B".to_string());
        config.localfile_store.as_mut().unwrap().disk_high_watermark = 1.0;
        config.http_port = util::find_available_port().expect("free http port");

        let _app_ref = mini_riffle::start(&config).await?;
        wait_for_urpc_epoll_ready(urpc_port).await?;

        assert_eq!(0, GAUGE_MEMORY_ALLOCATED.get());
        shuffle_testing(&config, _app_ref).await
    }

    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shuffle_write_read_testing_with_io_uring() -> Result<()> {
        init_logger();
        let temp_dir = tempdir::TempDir::new("test_write_read_io_uring").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        info!("temp file path: {} created", &temp_path);

        let grpc_port = util::find_available_port().expect("free grpc port") as i32;
        let urpc_port = util::find_available_port().expect("free urpc port");
        let mut config =
            Config::create_mem_localfile_config(grpc_port, "1G".to_string(), temp_path);
        config.urpc_port = Some(urpc_port);
        config.hybrid_store.memory_single_buffer_max_spill_size = Some("1B".to_string());
        config.localfile_store.as_mut().unwrap().disk_high_watermark = 1.0;
        config.http_port = util::find_available_port().expect("free http port");
        config.urpc_config = Some(UrpcConfig {
            get_index_rpc_version: RpcVersion::V1,
            io_uring_enable: true,
            io_uring_threads: 0,
        });

        let _app_ref = mini_riffle::start(&config).await?;
        wait_for_urpc_uring_ready(urpc_port).await?;

        assert_eq!(0, GAUGE_MEMORY_ALLOCATED.get());
        shuffle_testing(&config, _app_ref).await
    }

    /// Stress probe for server-side io-uring: high concurrency requests with bounded timeout.
    /// This test is ignored by default because it is load-sensitive and slow in CI.
    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    async fn setup_uring_test_server(case_name: &str) -> Result<(Config, u16)> {
        let temp_dir = tempdir::TempDir::new(case_name).unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        let grpc_port = util::find_available_port().expect("free grpc port") as i32;
        let urpc_port = util::find_available_port().expect("free urpc port");

        let mut config =
            Config::create_mem_localfile_config(grpc_port, "2G".to_string(), temp_path);
        config.urpc_port = Some(urpc_port);
        config.http_port = util::find_available_port().expect("free http port");
        config.urpc_config = Some(UrpcConfig {
            get_index_rpc_version: RpcVersion::V1,
            io_uring_enable: true,
            io_uring_threads: 0,
        });

        let _app_ref = mini_riffle::start(&config).await?;
        wait_for_urpc_uring_ready(urpc_port).await?;
        Ok((config, urpc_port))
    }

    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    async fn run_multi_client_rounds(
        urpc_port: u16,
        rounds: usize,
        client_count: usize,
        req_per_client_per_round: usize,
        request_timeout_secs: u64,
    ) -> Result<()> {
        let gate = Arc::new(Semaphore::new(client_count));
        for round in 0..rounds {
            let mut tasks = Vec::with_capacity(client_count);
            for client_idx in 0..client_count {
                let gate = gate.clone();
                tasks.push(tokio::spawn(async move {
                    let _permit = gate.acquire_owned().await?;
                    let mut client = EpollUrpcClient::connect("0.0.0.0", urpc_port as usize).await?;
                    for req_idx in 0..req_per_client_per_round {
                        let request_id =
                            ((round as i64) << 32) | ((client_idx as i64) << 16) | req_idx as i64;
                        let req = GetLocalDataRequestCommand::new(
                            request_id,
                            format!("functional-r{}-c{}", round, client_idx % 64),
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                        );
                        let _ = tokio::time::timeout(
                            Duration::from_secs(request_timeout_secs),
                            client.get_local_shuffle_data(req),
                        )
                        .await
                        .map_err(|e| {
                            anyhow!(
                                "functional suite timeout: round={round}, client={client_idx}, req={req_idx}, err={e}"
                            )
                        })?;
                    }
                    Ok::<(), anyhow::Error>(())
                }));
            }

            for task in tasks {
                task.await??;
            }
            info!("functional suite round done: {}/{}", round + 1, rounds);
        }
        Ok(())
    }

    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn urpc_uring_functional_completeness_suite() -> Result<()> {
        init_logger();
        let (_config, urpc_port) = setup_uring_test_server("test_uring_functional_suite").await?;

        // Case 1: single epoll client, repeated requests on one persistent connection.
        {
            let mut client = EpollUrpcClient::connect("0.0.0.0", urpc_port as usize).await?;
            for i in 0..32usize {
                let req = GetLocalDataRequestCommand::new(
                    i as i64,
                    "single-client-seq".to_string(),
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                );
                let _ = tokio::time::timeout(
                    Duration::from_secs(3),
                    client.get_local_shuffle_data(req),
                )
                .await
                .map_err(|e| anyhow!("single client seq timeout at req={i}: {e}"))?;
            }
        }

        // Case 2: multi-client + multi-round requests, each client reuses a persistent connection.
        run_multi_client_rounds(urpc_port, 3, 48, 3, 3).await?;

        // Case 3: timeout path should return quickly and never hang forever.
        // We only assert bounded completion time because server may also return a fast business error.
        {
            let mut client = EpollUrpcClient::connect("0.0.0.0", urpc_port as usize)
                .await?
                .with_request_timeout(Duration::from_millis(1));
            let req = GetLocalDataRequestCommand::new(
                9_999_999,
                "timeout-check".to_string(),
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            );
            let _ =
                tokio::time::timeout(Duration::from_secs(1), client.get_local_shuffle_data(req))
                    .await
                    .map_err(|e| anyhow!("timeout path hangs unexpectedly: {e}"))?;
        }

        Ok(())
    }

    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore]
    async fn urpc_uring_concurrency_stress_probe() -> Result<()> {
        init_logger();
        let (_config, urpc_port) = setup_uring_test_server("test_uring_stress_probe").await?;

        let round_count = std::env::var("URPC_STRESS_ROUNDS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(5);
        let client_count = std::env::var("URPC_STRESS_CLIENTS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(400);
        let req_per_client_per_round = std::env::var("URPC_STRESS_REQ_PER_CLIENT")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(4);
        let request_timeout_secs = std::env::var("URPC_STRESS_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(3);
        let round_cooldown_ms = std::env::var("URPC_STRESS_ROUND_COOLDOWN_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(200);

        for round in 0..round_count {
            info!(
                "stress round start: round={}/{}, clients={}, req_per_client={}, timeout_secs={}",
                round + 1,
                round_count,
                client_count,
                req_per_client_per_round,
                request_timeout_secs
            );
            run_multi_client_rounds(
                urpc_port,
                1,
                client_count,
                req_per_client_per_round,
                request_timeout_secs,
            )
            .await?;
            tokio::time::sleep(Duration::from_millis(round_cooldown_ms)).await;
        }
        Ok(())
    }
}
