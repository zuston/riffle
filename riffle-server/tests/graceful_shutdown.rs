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
    use signal_hook::consts::SIGTERM;
    use signal_hook::low_level::raise;
    use std::time::Duration;
    use tonic::transport::Channel;

    use riffle_server::config::Config;
    use riffle_server::grpc::protobuf::uniffle::shuffle_server_client::ShuffleServerClient;
    use riffle_server::mini_riffle::shuffle_testing;
    use riffle_server::{mini_riffle, util};

    async fn get_data_from_remote(
        _client: &ShuffleServerClient<Channel>,
        _app_id: &str,
        _shuffle_id: i32,
        _partitions: Vec<i32>,
    ) {
    }

    #[tokio::test]
    async fn graceful_shutdown_test() -> anyhow::Result<()> {
        let temp_dir = tempdir::TempDir::new("test_write_read").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("temp file path: {} created", &temp_path);

        let grpc_port = 20011;
        let config = Config::create_mem_localfile_config(grpc_port, "1G".to_string(), temp_path);
        let _ = mini_riffle::start(&config).await?;

        let jh = tokio::spawn(async move { shuffle_testing(&config).await });

        tokio::time::sleep(Duration::from_secs(1)).await;

        // raise shutdown signal
        tokio::spawn(async {
            raise(SIGTERM).expect("failed to raise shutdown signal");
            eprintln!("successfully raised shutdown signal");
        });

        let _ = jh.await.expect("Task panicked or failed.");

        Ok(())
    }
}
