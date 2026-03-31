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
    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    use riffle_server::urpc::client::UringUrpcClient;
    use riffle_server::urpc::command::GetLocalDataRequestCommand;
    use riffle_server::util;
    use std::time::Duration;

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

    /// Probe the urpc service at `urpc_port` with a UringUrpcClient until it responds.
    /// Only available when the io-uring feature is enabled.
    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    async fn wait_for_urpc_uring_ready(urpc_port: u16) -> Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let mut last_err = None;

        while tokio::time::Instant::now() < deadline {
            // Bound connect so an unbounded `await` cannot stall the deadline loop.
            match tokio::time::timeout(
                Duration::from_secs(2),
                UringUrpcClient::connect("0.0.0.0", urpc_port as usize),
            )
            .await
            {
                Ok(Ok(mut client)) => {
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
                Ok(Err(err)) => {
                    last_err = Some(err);
                }
                Err(_) => {
                    last_err = Some(anyhow!("urpc connect timed out after 2s"));
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Err(anyhow!(
            "urpc io-uring service not ready within 10s, last error: {:?}",
            last_err
        ))
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
}
