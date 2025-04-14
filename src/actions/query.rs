use crate::actions::discovery::{Discovery, ServerInfo, ServerStatus};
use crate::util;
use anyhow::Result;
use csv::Writer;
use datafusion::prelude::{CsvReadOptions, NdJsonReadOptions, SessionConfig, SessionContext};
use human_size::Any::{Byte, Gigabyte};
use human_size::{Size, SpecificSize};
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::Write;
use std::ops::Deref;
use std::path::PathBuf;
use tempfile::TempDir;

const INSTANCES_TABLE_NAME: &str = "instances";
const HISTORICAL_APPS_TABLE_NAME: &str = "historical_apps";
const ACTIVE_APPS_TABLE_NAME: &str = "active_apps";

pub struct SessionContextExtend {
    ctx: SessionContext,
    tmp_dir: TempDir,
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
    pub netty_port: i32,

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
            netty_port: info.netty_port,
            total_memory: SpecificSize::new(info.total_memory as f64, Byte)
                .unwrap()
                .to_string(),
            used_memory: SpecificSize::new(info.used_memory as f64, Byte)
                .unwrap()
                .to_string(),
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
            tmp_dir: tempfile::tempdir().unwrap(),
            discovery: Discovery::new(&[coordinator_url]),
        };
        self_me.register_table_instances().await.unwrap();
        self_me.register_table_active_apps().await.unwrap();
        self_me.register_table_historical_apps().await.unwrap();
        Ok(self_me)
    }

    fn write_csv<T: serde::Serialize>(
        &self,
        instances: Vec<T>,
        file_name: &str,
    ) -> Result<PathBuf> {
        let mut wtr = Writer::from_writer(vec![]);

        for instance in instances {
            wtr.serialize(instance)?;
        }

        let raw_data = String::from_utf8(wtr.into_inner()?)?;
        let file_path = self.tmp_dir.path().join(file_name);

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&file_path)?;

        file.write_all(raw_data.as_bytes())?;
        file.sync_all()?;

        Ok(file_path)
    }

    async fn register_table_active_apps(&self) -> Result<()> {
        let apps = self.discovery.list_active_apps().await?;
        const CSV_FILE_NAME: &str = "riffle.active.app.csv";
        let path = self.write_csv(apps, CSV_FILE_NAME)?;
        self.ctx
            .register_csv(
                ACTIVE_APPS_TABLE_NAME,
                path.to_str().unwrap(),
                CsvReadOptions::default().has_header(true),
            )
            .await?;

        Ok(())
    }

    async fn register_table_historical_apps(&self) -> Result<()> {
        let apps = self.discovery.list_historical_apps().await?;
        const CSV_FILE_NAME: &str = "riffle.history.app.csv";

        let path = self.write_csv(apps, CSV_FILE_NAME)?;

        self.ctx
            .register_csv(
                HISTORICAL_APPS_TABLE_NAME,
                path.to_str().unwrap(),
                CsvReadOptions::default().has_header(true),
            )
            .await?;

        Ok(())
    }

    async fn register_table_instances(&self) -> Result<()> {
        let instances = self.discovery.list_nodes().await?;
        let instances = instances
            .iter()
            .map(|x| x.into())
            .collect::<Vec<TableInstance>>();
        const CSV_FILE_NAME: &str = "riffle.instance.csv";

        let mut wtr = Writer::from_writer(vec![]);
        for instance_info in instances {
            wtr.serialize(instance_info)?;
        }
        let raw_data = String::from_utf8(wtr.into_inner()?)?;
        let file_path = self.tmp_dir.path().join(CSV_FILE_NAME);

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&file_path)?;
        file.write_all(raw_data.as_bytes())?;
        file.sync_all()?;

        self.ctx
            .register_csv(
                INSTANCES_TABLE_NAME,
                file_path.to_str().unwrap(),
                CsvReadOptions::default().has_header(true),
            )
            .await?;

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
        let coordinator = FakeCoordinator::new(20010).await;
        let coordinator_url = "http://localhost:20010";

        let context = SessionContextExtend::new(coordinator_url).await.unwrap();

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
