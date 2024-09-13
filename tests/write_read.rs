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
    use uniffle_worker::config::{Config, LocalfileStoreConfig, StorageType};
    use uniffle_worker::{start_uniffle_worker, write_read_for_one_time};

    use std::time::Duration;
    use tonic::transport::Channel;
    use uniffle_worker::grpc::protobuf::uniffle::shuffle_server_client::ShuffleServerClient;
    use uniffle_worker::metric::GAUGE_MEMORY_ALLOCATED;

    async fn get_data_from_remote(
        _client: &ShuffleServerClient<Channel>,
        _app_id: &str,
        _shuffle_id: i32,
        _partitions: Vec<i32>,
    ) {
    }

    async fn start_embedded_worker(path: String, port: i32) {
        let config = Config::create_mem_localfile_config(port, "1G".to_string(), path);
        let _ = start_uniffle_worker(config).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn write_read_test_with_embedded_worker() -> Result<()> {
        let temp_dir = tempdir::TempDir::new("test_write_read").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("created the temp file path: {}", &temp_path);

        let port = 21101;
        let _ = start_embedded_worker(temp_path, port).await;

        let client = ShuffleServerClient::connect(format!("http://{}:{}", "0.0.0.0", port)).await?;

        // after one batch write/read process, the allocated memory size should be 0
        assert_eq!(0, GAUGE_MEMORY_ALLOCATED.get());

        write_read_for_one_time(client).await
    }
}
