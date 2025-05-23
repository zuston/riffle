use crate::actions::query::SessionContextExtend;
use crate::actions::Action;
use async_trait::async_trait;
use datafusion::common::context;
use datafusion_postgres::{DfSessionService, HandlerFactory};
use pgwire::tokio::process_socket;
use std::sync::Arc;
use tokio::net::TcpListener;

pub struct PostgresServerAction {
    coordinator_server_url: String,
    host: String,
    port: usize,
}

impl PostgresServerAction {
    pub fn new(coordinator_server_url: String, host: String, port: usize) -> PostgresServerAction {
        Self {
            coordinator_server_url,
            host,
            port,
        }
    }
}

#[async_trait]
impl Action for PostgresServerAction {
    async fn act(&self) -> anyhow::Result<()> {
        let context_manager =
            SessionContextExtend::new(self.coordinator_server_url.as_str()).await?;
        let context = context_manager.get_context();

        tokio::spawn(async move {
            loop {
                // refresh table
                tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
                if let Err(e) = context_manager.reload().await {
                    println!("Error reloading session: {}", e);
                }
            }
        });

        // Get the first catalog name from the session context
        let catalog_name = context
            .catalog_names() // Fixed: Removed .catalog_list()
            .first()
            .cloned();

        let factory = Arc::new(HandlerFactory(Arc::new(DfSessionService::new(
            context,
            catalog_name,
        ))));

        // Bind to the specified host and port
        let server_addr = format!("{}:{}", &self.host, self.port);
        let listener = TcpListener::bind(&server_addr).await?;
        println!("Listening on {}", server_addr);

        // Accept incoming connections
        loop {
            let (socket, addr) = listener.accept().await?;
            let factory_ref = factory.clone();
            println!("Accepted connection from {}", addr);

            tokio::spawn(async move {
                if let Err(e) = process_socket(socket, None, factory_ref).await {
                    eprintln!("Error processing socket: {}", e);
                }
            });
        }

        Ok(())
    }
}
