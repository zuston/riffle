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
    use anyhow::{Context, Result};
    use riffle_server::config::Config;
    use riffle_server::server::Server;
    use std::io::ErrorKind;
    use std::net::TcpListener;
    use std::time::Duration;

    #[tokio::test]
    async fn graceful_shutdown() -> Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();
        let mut config = Config::create_simple_config();
        config.grpc_port = 0;
        config.urpc_port = Some(0);
        config.http_port = 0;
        config.coordinator_quorum.clear();

        let server = Server::start(config).await?;
        let rpc_ports = [
            ("gRPC", server.config().grpc_port),
            ("URPC", server.config().urpc_port.unwrap()),
        ];
        let http_port = server.config().http_port;

        for (service, port) in rpc_ports.into_iter().chain([("HTTP", http_port)]) {
            let error = TcpListener::bind(("0.0.0.0", port)).expect_err(&format!(
                "{service} port {port} was not bound after startup"
            ));
            assert_eq!(
                error.kind(),
                ErrorKind::AddrInUse,
                "unexpected error probing {service} port {port}: {error}"
            );
        }

        tokio::time::timeout(Duration::from_secs(5), server.shutdown())
            .await
            .context("server shutdown timed out")??;

        for (service, port) in rpc_ports {
            let _listener = TcpListener::bind(("0.0.0.0", port))
                .with_context(|| format!("{service} port {port} is still bound after shutdown"))?;
        }

        // HTTP handles the shutdown signal independently of RPC completion.
        awaitility::at_most(Duration::from_secs(1))
            .poll_interval(Duration::from_millis(10))
            .describe("HTTP port should be released after shutdown")
            .until(|| TcpListener::bind(("0.0.0.0", http_port)).is_ok());

        Ok(())
    }
}
