use crate::admin::query::SessionContextExtend;
use dashmap::DashMap;
use datafusion::prelude::{SQLOptions, SessionContext};
use std::sync::Arc;

pub mod discovery;
pub mod query;

pub struct CtlContext {
    pub coordinator_http_url: String,
}

#[async_trait::async_trait]
pub trait Action {
    async fn act(&self) -> anyhow::Result<()>;
}

pub enum OutputFormat {
    TABLE,
    JSON,
}

pub struct QueryAction {
    ctx: CtlContext,
    sql: String,
    format: OutputFormat,
}

impl QueryAction {
    pub fn new(ctx: CtlContext, sql: String, format: OutputFormat) -> Self {
        Self { ctx, sql, format }
    }
}

#[async_trait::async_trait]
impl Action for QueryAction {
    async fn act(&self) -> anyhow::Result<()> {
        let context = SessionContextExtend::new(self.ctx.coordinator_http_url.as_str()).await?;
        let df = context.sql(self.sql.as_str()).await?;
        match &self.format {
            OutputFormat::TABLE => {
                df.show().await?;
            }
            OutputFormat::JSON => {
                todo!()
            }
        }
        Ok(())
    }
}
