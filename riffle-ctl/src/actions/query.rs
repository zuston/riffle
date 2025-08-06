use crate::actions::discovery::{Discovery, ServerInfo, ServerStatus};
use crate::actions::Action;
use crate::meta;
use anyhow::Result;
use clap::builder::Str;
use csv::Writer;
use datafusion::arrow;
use datafusion::arrow::csv::reader::Format;
use datafusion::arrow::csv::ReaderBuilder;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::MemTable;
use datafusion::prelude::{CsvReadOptions, SessionConfig, SessionContext};
use indicatif::HumanBytes;
use riffle_server::util;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{Query, SetExpr, Statement, TableFactor, TableWithJoins};
use std::fs;
use std::fs::OpenOptions;
use std::io::{Cursor, Seek, Write};
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use url::{ParseError, Url};

const INSTANCES_TABLE_NAME: &str = "instances";
const HISTORICAL_APPS_TABLE_NAME: &str = "historical_apps";
const ACTIVE_APPS_TABLE_NAME: &str = "active_apps";

pub struct SessionContextExtend {
    ctx: Arc<SessionContext>,
    discovery: Discovery,
}

impl Deref for SessionContextExtend {
    type Target = Arc<SessionContext>;

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
    pub async fn new(coordinator_url: &str, tables: Option<Vec<&str>>) -> Result<Self> {
        let ctx =
            SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
        let self_me = Self {
            ctx: Arc::new(ctx),
            discovery: Discovery::new(&[coordinator_url]),
        };
        self_me.reload(tables).await?;
        Ok(self_me)
    }

    pub fn get_context(&self) -> Arc<SessionContext> {
        self.ctx.clone()
    }

    pub async fn reload(&self, tables: Option<Vec<&str>>) -> Result<()> {
        if tables.is_none() || tables.as_ref().unwrap().contains(&INSTANCES_TABLE_NAME) {
            self.register_instances().await?;
        }
        if tables.is_none() || tables.as_ref().unwrap().contains(&ACTIVE_APPS_TABLE_NAME) {
            self.register_active_apps().await?;
        }
        if tables.is_none()
            || tables
                .as_ref()
                .unwrap()
                .contains(&HISTORICAL_APPS_TABLE_NAME)
        {
            self.register_historical_apps().await?;
        }
        Ok(())
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
        let _ = self.ctx.deregister_table(table_name);
        self.ctx.register_table(table_name, Arc::new(table))?;

        Ok(())
    }
}

pub enum OutputFormat {
    TABLE,
    JSON,
}

pub struct QueryAction {
    sql: String,
    format: OutputFormat,
    coordinator_url: String,
}

impl QueryAction {
    pub fn new(sql: String, format: OutputFormat, coordinator_name: String) -> Self {
        let url = match Url::parse(coordinator_name.as_str()) {
            Ok(url) => coordinator_name,
            Err(_) => {
                let meta = meta::Metadata::load(None);
                let mapping = meta.get_cluster_ids();
                let url = mapping.get(&coordinator_name);
                if url.is_none() {
                    panic!("Illegal coordinator url/cluster id. {}", &coordinator_name);
                }
                url.unwrap().into()
            }
        };

        Self {
            sql,
            format,
            coordinator_url: url,
        }
    }
}

#[async_trait::async_trait]
impl Action for QueryAction {
    async fn act(&self) -> anyhow::Result<()> {
        let sql = self.sql.as_str();
        let tables = extract_tables(sql);
        let retrieved_tables = tables.iter().map(|x| x.as_str()).collect::<Vec<_>>();
        let context =
            SessionContextExtend::new(self.coordinator_url.as_str(), Some(retrieved_tables))
                .await?;
        let df = context.sql(sql).await?;
        match &self.format {
            OutputFormat::TABLE => {
                df.show().await?;
            }
            OutputFormat::JSON => {
                let temp_dir = tempfile::tempdir().unwrap();
                let file_path = temp_dir.path().join("output.json");
                let absolute_file_path = file_path.to_str().unwrap();
                df.write_json(absolute_file_path, DataFrameWriteOptions::new(), None)
                    .await?;
                let contents = fs::read_to_string(absolute_file_path)?;
                println!("{}", contents);
            }
        }
        Ok(())
    }
}

fn extract_tables(sql: &str) -> Vec<String> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql).expect("SQL parsing failed");

    let mut tables = Vec::new();
    for stmt in ast {
        extract_table_names_from_statement(&stmt, &mut tables);
    }
    tables
}

fn extract_table_names_from_statement(stmt: &Statement, tables: &mut Vec<String>) {
    match stmt {
        Statement::Query(query) => extract_from_query(query, tables),
        _ => {}
    }
}

fn extract_from_query(query: &Box<Query>, tables: &mut Vec<String>) {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            extract_from_query(&cte.query, tables);
        }
    }

    match &*query.body {
        SetExpr::Select(select) => {
            for table_with_joins in &select.from {
                extract_table_from_table_with_joins(table_with_joins, tables);
            }
        }
        SetExpr::Query(subquery) => {
            extract_from_query(subquery, tables);
        }
        _ => {}
    }
}

fn extract_table_from_table_with_joins(twj: &TableWithJoins, tables: &mut Vec<String>) {
    extract_table_factor(&twj.relation, tables);
    for join in &twj.joins {
        extract_table_factor(&join.relation, tables);
    }
}

fn extract_table_factor(factor: &TableFactor, tables: &mut Vec<String>) {
    match factor {
        TableFactor::Table { name, .. } => {
            tables.push(name.to_string());
        }
        TableFactor::Derived { subquery, .. } => {
            extract_from_query(subquery, tables);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use crate::actions::discovery::tests::FakeCoordinator;
    use crate::actions::query::{extract_tables, SessionContextExtend};

    #[test]
    fn test_extract_table_names() {
        let sql = "select * from instances";
        let tables = extract_tables(sql);
        assert_eq!(tables.len(), 1);
        assert!(tables.contains(&"instances".to_string()));
    }

    #[tokio::test]
    #[ignore]
    async fn test_connect_with_real_coordinator() {
        let coordinator_url = "http://xxx:21001";
        let context = SessionContextExtend::new(coordinator_url, None)
            .await
            .unwrap();
        let df = context.sql("select * from active_apps").await.unwrap();
        df.show().await.unwrap();
    }

    #[tokio::test]
    async fn test_register_table_instances() {
        let port = riffle_server::util::find_available_port().unwrap();
        let coordinator = FakeCoordinator::new(port as i32).await;
        let coordinator_url = format!("http://localhost:{}", port);

        let context = SessionContextExtend::new(&coordinator_url, None)
            .await
            .unwrap();

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
