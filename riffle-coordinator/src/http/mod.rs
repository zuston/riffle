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

mod application;

use crate::application::ApplicationManager;
use application::ApplicationHandler;
use log::info;
use poem::listener::TcpAcceptor;
use poem::{get, EndpointExt, Route, Server};
use riffle_server::http::Handler;
use std::future::Future;
use tokio::net::TcpListener;

fn routes(application_manager: ApplicationManager) -> Route {
    let handler = ApplicationHandler::new(application_manager.clone());
    Route::new()
        .at(handler.get_route_path(), handler.get_route_method())
        .at(
            "/apps/:app_id",
            get(application::application).data(application_manager),
        )
}

pub async fn serve(
    application_manager: ApplicationManager,
    listener: TcpListener,
    shutdown: impl Future<Output = ()>,
) -> std::io::Result<()> {
    info!("Starting HTTP server on {}", listener.local_addr()?);
    Server::new_with_acceptor(TcpAcceptor::from_tokio(listener)?)
        .run_with_graceful_shutdown(routes(application_manager), shutdown, None)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::ClusterManager;
    use crate::config::Config;
    use crate::grpc::protobuf::uniffle::coordinator_server_server::CoordinatorServer;
    use crate::grpc::service::DefaultCoordinatorServer;
    use poem::{Endpoint, Request};
    use riffle_server::grpc::protobuf::uniffle::{
        ApplicationInfo, ShuffleServerHeartBeatRequest, ShuffleServerId,
    };
    use serde_json::json;

    #[tokio::test]
    async fn apps_endpoint_returns_aggregated_usage() {
        let manager = ApplicationManager::default();
        let endpoint = routes(manager.clone());
        let coordinator =
            DefaultCoordinatorServer::new(&ClusterManager::new(&Config::default()), &manager);
        let response = endpoint
            .call(Request::builder().uri("/apps".parse().unwrap()).finish())
            .await
            .unwrap();
        assert_eq!(response.status(), poem::http::StatusCode::OK);
        assert_eq!(response.into_body().into_string().await.unwrap(), "[]");

        manager.register("app-b".to_string(), "user-b".to_string());
        manager.register("app-a".to_string(), "user-a".to_string());
        for (server_id, total_size) in [("server-b", 100), ("server-a", 200)] {
            coordinator
                .heartbeat(tonic::Request::new(ShuffleServerHeartBeatRequest {
                    server_id: Some(ShuffleServerId {
                        id: server_id.to_string(),
                        ip: "127.0.0.1".to_string(),
                        port: 1,
                        ..Default::default()
                    }),
                    application_info: vec![ApplicationInfo {
                        app_id: "app-a".to_string(),
                        total_size,
                        memory_size: 10,
                        local_total_size: 20,
                        hadoop_total_size: 30,
                        ..Default::default()
                    }],
                    ..Default::default()
                }))
                .await
                .unwrap();
        }

        let response = endpoint
            .call(Request::builder().uri("/apps".parse().unwrap()).finish())
            .await
            .unwrap();
        assert_eq!(response.status(), poem::http::StatusCode::OK);
        assert_eq!(
            response.content_type(),
            Some("application/json; charset=utf-8")
        );
        let apps: serde_json::Value = response.into_body().into_json().await.unwrap();
        assert_eq!(apps.as_array().unwrap().len(), 2);
        assert_eq!(apps[0]["app_id"], "app-a");
        assert_eq!(apps[0]["user"], "user-a");
        assert!(apps[0]["start_time"].as_str().is_some());
        assert!(apps[0]["last_heartbeat_time"].as_str().is_some());
        assert!(apps[0].get("server_ids").is_none());
        assert_eq!(
            apps[0]["stats"],
            json!({
                "received_bytes": 300,
                "resident_bytes": 120,
                "memory_bytes": 20,
                "localfile_bytes": 40,
                "hdfs_bytes": 60,
            })
        );
        assert_eq!(apps[1]["app_id"], "app-b");
        assert_eq!(apps[1]["stats"]["received_bytes"], 0);

        let response = endpoint
            .call(
                Request::builder()
                    .uri("/apps/app-a".parse().unwrap())
                    .finish(),
            )
            .await
            .unwrap();
        let app: serde_json::Value = response.into_body().into_json().await.unwrap();
        assert_eq!(app, apps[0]);

        let error = endpoint
            .call(
                Request::builder()
                    .uri("/apps/unknown".parse().unwrap())
                    .finish(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.status(), poem::http::StatusCode::NOT_FOUND);
    }
}
