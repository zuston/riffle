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
    use anyhow::Result;
    use log::info;
    use riffle_server::config::Config;
    use riffle_server::metric::GAUGE_MEMORY_ALLOCATED;
    use riffle_server::mini_riffle;
    use riffle_server::mini_riffle::shuffle_testing;
    use std::time::Duration;

    fn init_logger() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shuffle_write_read_testing() -> Result<()> {
        init_logger();
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        info!("temp file path: {} created", &temp_path);

        let grpc_port = 21101;
        let urpc_port = 21102;
        let mut config =
            Config::create_mem_localfile_config(grpc_port, "1G".to_string(), temp_path);
        config.urpc_port = Some(urpc_port);
        config.hybrid_store.memory_single_buffer_max_spill_size = Some("1B".to_string());
        config.localfile_store.as_mut().unwrap().disk_high_watermark = 1.0;

        let _app_ref = mini_riffle::start(&config).await?;
        // wait all setup
        tokio::time::sleep(Duration::from_secs(1)).await;

        // after one batch write/read process, the allocated memory size should be 0
        assert_eq!(0, GAUGE_MEMORY_ALLOCATED.get());

        shuffle_testing(&config, _app_ref).await
    }

    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shuffle_write_read_with_uring_net_engine_testing() -> Result<()> {
        use riffle_server::config::{RpcVersion, UrpcConfig, UrpcNetEngine};

        init_logger();
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        info!("temp file path: {} created", &temp_path);

        let grpc_port = 21203;
        let urpc_port = 21204;
        let mut config =
            Config::create_mem_localfile_config(grpc_port, "1G".to_string(), temp_path);
        config.urpc_port = Some(urpc_port);
        config.urpc_config = Some(UrpcConfig {
            get_index_rpc_version: RpcVersion::V1,
            streaming_parse_enabled: true,
            write_mode: Default::default(),
            net_engine: UrpcNetEngine::URING,
        });
        config.hybrid_store.memory_single_buffer_max_spill_size = Some("1B".to_string());
        config.localfile_store.as_mut().unwrap().disk_high_watermark = 1.0;

        let _app_ref = mini_riffle::start(&config).await?;
        // wait all setup
        tokio::time::sleep(Duration::from_secs(1)).await;

        shuffle_testing(&config, _app_ref).await
    }
}
