use crate::actions::discovery::{Discovery, ServerInfo, ServerStatus};
use anyhow::Result;
use csv::Writer;
use datafusion::arrow;
use datafusion::arrow::csv::reader::Format;
use datafusion::arrow::csv::ReaderBuilder;
use datafusion::datasource::MemTable;
use datafusion::prelude::{CsvReadOptions, SessionConfig, SessionContext};
use indicatif::HumanBytes;
use riffle_server::util;
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::{Cursor, Seek, Write};
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;

const INSTANCES_TABLE_NAME: &str = "instances";
const HISTORICAL_APPS_TABLE_NAME: &str = "historical_apps";
const ACTIVE_APPS_TABLE_NAME: &str = "active_apps";

pub struct SessionContextExtend {
    ctx: SessionContext,
    discovery: Discovery,
}

impl Deref for SessionContextExtend {
    type Target = SessionContext;

    fn deref(&self) -> &Self::Target {
        &self.ctx
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TableInstance {
    pub ip: String,
    pub grpc_port: u16,
    pub urpc_port: i32,
    pub http_port: i32,

    pub total_memory: String,
    pub used_memory: String,
    pub event_num_in_flush: usize,
    pub tags: String,
    pub status: ServerStatus,
}

impl From<&ServerInfo> for TableInstance {
    fn from(info: &ServerInfo) -> Self {
        Self {
            ip: info.ip.to_string(),
            grpc_port: info.grpc_port,
            urpc_port: info.netty_port,
            http_port: info.http_port as i32,
            total_memory: HumanBytes(info.total_memory as u64).to_string(),
            used_memory: HumanBytes(info.used_memory as u64).to_string(),
            event_num_in_flush: info.event_num_in_flush,
            tags: info
                .tags
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(","),
            status: info.status.clone(),
        }
    }
}

impl SessionContextExtend {
    pub async fn new(coordinator_url: &str) -> Result<Self> {
        let ctx =
            SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
        let self_me = Self {
            ctx,
            discovery: Discovery::new(&[coordinator_url]),
        };
        self_me.register_instances().await?;
        self_me.register_active_apps().await?;
        self_me.register_historical_apps().await?;
        Ok(self_me)
    }

    async fn register_active_apps(&self) -> Result<()> {
        let apps = self.discovery.list_active_apps().await?;
        self.register(apps, ACTIVE_APPS_TABLE_NAME)?;
        Ok(())
    }

    async fn register_historical_apps(&self) -> Result<()> {
        let apps = self.discovery.list_historical_apps().await?;
        self.register(apps, HISTORICAL_APPS_TABLE_NAME)?;
        Ok(())
    }

    async fn register_instances(&self) -> Result<()> {
        let instances = self.discovery.list_nodes().await?;
        let instances = instances
            .iter()
            .map(|x| x.into())
            .collect::<Vec<TableInstance>>();
        self.register(instances, INSTANCES_TABLE_NAME)?;
        Ok(())
    }

    fn register<T>(&self, raw_vec: Vec<T>, table_name: &str) -> Result<()>
    where
        T: Serialize,
    {
        let mut wtr = Writer::from_writer(vec![]);
        for raw in raw_vec {
            wtr.serialize(raw)?;
        }
        let raw_bytes = String::from_utf8(wtr.into_inner()?)?;

        let mut cursor = Cursor::new(raw_bytes);
        let (schema, _) = Format::default()
            .with_header(true)
            .infer_schema(&mut cursor, None)
            .unwrap();
        cursor.rewind()?;
        let schema = Arc::new(schema);

        let mut reader = ReaderBuilder::new(schema.clone())
            .with_header(true)
            .build(cursor)?;
        let batches = reader.collect::<arrow::error::Result<Vec<_>>>()?;

        let table = MemTable::try_new(schema, vec![batches])?;
        self.ctx.register_table(table_name, Arc::new(table))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::actions::discovery::tests::FakeCoordinator;
    use crate::actions::query::SessionContextExtend;

    #[tokio::test]
    #[ignore]
    async fn test_connect_with_real_coordinator() {
        let coordinator_url = "http://xxx:21001";
        let context = SessionContextExtend::new(coordinator_url).await.unwrap();
        let df = context.sql("select * from active_apps").await.unwrap();
        df.show().await.unwrap();
    }

    #[tokio::test]
    async fn test_register_table_instances() {
        let port = riffle_server::util::find_available_port().unwrap();
        let coordinator = FakeCoordinator::new(port as i32).await;
        let coordinator_url = format!("http://localhost:{}", port);

        let context = SessionContextExtend::new(&coordinator_url).await.unwrap();

        let sql = "SELECT * FROM instances";
        let df = context.sql(sql).await.unwrap();
        // df.show().await.unwrap();

        context
            .sql("select * from instances where grpc_port > -1")
            .await
            .unwrap()
            .show()
            .await
            .unwrap();

        assert_eq!(1, df.count().await.unwrap());
    }
}
