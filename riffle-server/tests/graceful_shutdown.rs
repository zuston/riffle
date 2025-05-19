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
mod test {
    use std::time::Duration;

    use signal_hook::consts::SIGTERM;
    use signal_hook::low_level::raise;
    use tonic::transport::Channel;

    use riffle_server::config::Config;
    use riffle_server::grpc::protobuf::uniffle::shuffle_server_client::ShuffleServerClient;
    use riffle_server::{start_uniffle_worker, write_read_for_one_time};

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

    #[tokio::test]
    async fn graceful_shutdown_test_with_embedded_worker_successfully_shutdown() {
        let temp_dir = tempdir::TempDir::new("test_write_read").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("created the temp file path: {}", &temp_path);

        let port = 21101;
        let _ = start_embedded_worker(temp_path, port).await;

        let client =
            match ShuffleServerClient::connect(format!("http://{}:{}", "0.0.0.0", port)).await {
                Ok(client) => client,
                Err(e) => {
                    // Handle the error, e.g., by panicking or logging it.
                    panic!("Failed to connect: {}", e);
                }
            };

        let jh = tokio::spawn(async move { write_read_for_one_time(client).await });

        // raise shutdown signal
        tokio::spawn(async {
            raise(SIGTERM).expect("failed to raise shutdown signal");
            eprintln!("successfully raised shutdown signal");
        });

        let _ = jh.await.expect("Task panicked or failed.");
    }
}
