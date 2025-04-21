use crate::runtime::manager::create_runtime;
use crate::runtime::{Runtime, RuntimeRef};
use anyhow::{anyhow, Result};
use futures::future::try_join_all;
use libc::iovec;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use std::str::FromStr;
use strum_macros::Display;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ActiveAppInfo {
    pub app_id: String,
    pub registry_timestamp: u128,
    pub duration_minutes: f64,
    pub resident_bytes: u64,
    pub partition_number: usize,
    pub huge_partition_number: u64,
    pub reported_block_id_number: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct HistoricalAppInfo {
    pub app_id: String,
    pub partition_num: usize,
    pub huge_partition_num: usize,

    #[serde(skip_serializing)]
    pub avg_huge_partition_bytes: usize,

    pub max_huge_partition_bytes: usize,

    #[serde(skip_serializing)]
    pub min_huge_partition_bytes: usize,

    #[serde(skip_serializing)]
    pub record_timestamp: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct NodesBody {
    code: i32,
    data: Vec<ServerInfo>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Display)]
pub enum ServerStatus {
    ACTIVE,
    DECOMMISSIONING,
    DECOMMISSIONED,
    LOST,
    UNHEALTHY,
    EXCLUDED,
    UNKNOWN,
}

impl FromStr for ServerStatus {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ACTIVE" => Ok(ServerStatus::ACTIVE),
            "DECOMMISSIONING" => Ok(ServerStatus::DECOMMISSIONING),
            "DECOMMISSIONED" => Ok(ServerStatus::DECOMMISSIONED),
            "LOST" => Ok(ServerStatus::LOST),
            "UNHEALTHY" => Ok(ServerStatus::UNHEALTHY),
            "EXCLUDED" => Ok(ServerStatus::EXCLUDED),
            "UNKNOWN" => Ok(ServerStatus::UNKNOWN),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ServerInfo {
    #[serde(skip_serializing)]
    pub id: String,
    pub ip: String,

    #[serde(rename = "grpcPort")]
    pub grpc_port: u16,
    #[serde(rename = "nettyPort")]
    pub netty_port: i32,

    #[serde(rename = "totalMemory")]
    pub total_memory: usize,

    #[serde(rename = "usedMemory")]
    pub used_memory: usize,

    #[serde(rename = "availableMemory")]
    #[serde(skip_serializing)]
    pub available_memory: usize,

    #[serde(rename = "preAllocatedMemory")]
    #[serde(skip_serializing)]
    pub pre_allocated_memory: usize,

    #[serde(rename = "eventNumInFlush")]
    pub event_num_in_flush: usize,

    #[serde(skip_serializing)]
    pub timestamp: u64,

    #[serde(serialize_with = "raw_tags")]
    pub tags: Vec<String>,

    pub status: ServerStatus,

    #[serde(rename = "jettyPort")]
    #[serde(default = "default_jetty_port")]
    pub jetty_port: usize,
}

fn default_jetty_port() -> usize {
    0
}

fn raw_tags<S>(values: &Vec<String>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let values_string = values
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(",");
    serializer.serialize_str(&values_string)
}

pub struct Discovery {
    coordinator_quorum: Vec<String>,
}

impl Discovery {
    pub fn new(coordinator_quorum: &[&str]) -> Discovery {
        Self {
            coordinator_quorum: coordinator_quorum.iter().map(|s| s.to_string()).collect(),
        }
    }
}

impl Discovery {
    pub async fn list_nodes(&self) -> Result<Vec<ServerInfo>> {
        let url = format!(
            "{}/api/server/nodes",
            self.coordinator_quorum.get(0).as_ref().unwrap().as_str()
        );
        let resp = reqwest::get(url).await?.json::<NodesBody>().await?;
        Ok(resp.data.clone())
    }

    pub async fn list_active_apps(&self) -> Result<Vec<ActiveAppInfo>> {
        self.fetch_from_node_api("/apps?format=Json").await
    }

    pub async fn list_historical_apps(&self) -> Result<Vec<HistoricalAppInfo>> {
        self.fetch_from_node_api("/apps/history").await
    }

    async fn fetch_from_node_api<T>(&self, url_prefix: &str) -> Result<Vec<T>>
    where
        T: DeserializeOwned + Send + Sync + 'static,
    {
        let server_infos = self.list_nodes().await?;
        let ips = server_infos
            .into_iter()
            .map(|x| (x.ip.to_string(), x.jetty_port))
            .collect::<Vec<_>>();

        let mut future_list = vec![];
        for (ip, http_port) in ips.into_iter() {
            let ip = ip.to_string();
            let prefix = url_prefix.to_string();
            let future = async move {
                let url = format!("http://{}:{}{}", &ip, http_port, prefix);
                let response = reqwest::get(&url).await?;
                let apps = response.json::<Vec<T>>().await?;
                Result::<_, reqwest::Error>::Ok(apps)
            };
            future_list.push(tokio::spawn(future));
        }
        let results = try_join_all(future_list)
            .await
            .map_err(|x| anyhow!("Error happened. err: {}", x))?;
        let all_apps = results
            .into_iter()
            .filter_map(Result::ok)
            .flatten()
            .collect::<Vec<T>>();
        Ok(all_apps)
    }
}

#[cfg(test)]
pub mod tests {
    use crate::actions::discovery::{
        ActiveAppInfo, Discovery, NodesBody, ServerInfo, ServerStatus,
    };
    use crate::http::Handler;
    use crate::mem_allocator::dump_heap_flamegraph;
    use anyhow::{anyhow, Result};
    use futures::future::try_join_all;
    use hyper::StatusCode;
    use poem::listener::TcpListener;
    use poem::web::Json;
    use poem::{IntoResponse, Request, Route, RouteMethod, Server};
    use std::thread;

    pub struct FakeCoordinator;
    #[poem::handler]
    async fn nodes_handler(req: &Request) -> String {
        let body = r#"
{
    "code": 0,
    "data": [
        {
            "id": "10.71.128.191-21100",
            "ip": "10.71.128.191",
            "grpcPort": 21100,
            "usedMemory": 11689967306,
            "preAllocatedMemory": 816219,
            "availableMemory": 9784052955,
            "eventNumInFlush": 0,
            "timestamp": 1744273660946,
            "tags": [
                "riffle",
                "ss_v5",
                "ss_v4",
                "v0.9.0-rc1",
                "GRPC"
            ],
            "status": "ACTIVE",
            "storageInfo": {

            },
            "nettyPort": -1,
            "totalMemory": 21474020261,
            "jettyPort": 1000
        }
    ]
}
        "#;
        body.into()
    }
    impl FakeCoordinator {
        pub async fn new(port: i32) -> Self {
            let route = Route::new().at("/api/server/nodes", RouteMethod::new().get(nodes_handler));
            tokio::spawn(async move {
                let server = Server::new(TcpListener::bind(format!("0.0.0.0:{}", port)))
                    .run(route)
                    .await
                    .unwrap();
            });
            Self
        }
    }

    #[tokio::test]
    async fn test_discovery() -> Result<()> {
        let coordinator = FakeCoordinator::new(20010).await;
        let discovery = Discovery {
            coordinator_quorum: vec!["http://localhost:20010".to_string()],
        };
        let nodes = discovery.list_nodes().await?;
        assert_ne!(nodes.len(), 0);

        Ok(())
    }
}
