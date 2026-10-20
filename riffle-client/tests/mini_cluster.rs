use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::TryStreamExt;
use riffle_client::{
    Endpoint, ManagerRpcService, MiniCluster, MiniClusterConfig, ReadTaskInfo,
    ShuffleManagerServiceServer, ShuffleReader, ShuffleWriter, WriteTaskInfo,
};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mini_cluster_client_write_read() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut cluster = MiniCluster::new(MiniClusterConfig { num_servers: 2 });
        cluster.start().await?;
        assert_eq!(cluster.get_servers().len(), 2);

        let coordinator = cluster.coordinator_endpoint()?;
        let app_id = "mini-cluster-client-write-read";
        let service = ManagerRpcService::new();
        let manager = service
            .create_shuffle_manager(
                app_id,
                "test",
                HashMap::from([
                    (
                        "riffle.client.coordinator.quorum".to_owned(),
                        format!("{}:{}", coordinator.host, coordinator.grpc_port),
                    ),
                    (
                        "riffle.client.assignment.serverNumber".to_owned(),
                        "2".to_owned(),
                    ),
                ]),
            )
            .await?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let manager_endpoint = Endpoint {
            host: "127.0.0.1".to_owned(),
            grpc_port: listener.local_addr()?.port(),
        };
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let manager_task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(ShuffleManagerServiceServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        let mut expected = Vec::new();
        for map_id in 0..2 {
            let mut writer = ShuffleWriter::new(
                app_id.to_owned(),
                manager_endpoint.clone(),
                WriteTaskInfo {
                    job_id: 1,
                    stage_id: 1,
                    map_id,
                    attempt_id: 0,
                    num_mappers: 2,
                    num_partitions: 4,
                },
            )
            .await?;
            // Leave partition 3 empty and preserve duplicate records across mappers.
            for partition_id in 0..3 {
                for data in [
                    Bytes::new(),
                    Bytes::from(format!("mapper={map_id}, partition={partition_id}")),
                    Bytes::from_static(b"\x00\xff\x01\x80"),
                ] {
                    writer.write(partition_id, data.clone()).await?;
                    expected.push((partition_id, data));
                }
            }
            writer.finish().await?;
        }

        for server in cluster.get_servers() {
            assert!(
                server.app_manager().store_memory_snapshot().await?.used() > 0,
                "server on port {} received no shuffle data",
                server.config().grpc_port
            );
        }

        let reader = ShuffleReader::new(
            app_id.to_owned(),
            manager_endpoint,
            ReadTaskInfo {
                job_id: 1,
                upstream_stage_id: 1,
                partitions: vec![0, 1, 2, 3],
            },
        )
        .await?;
        let mut actual: Vec<_> = reader
            .stream()
            .map_ok(|record| (record.partition_id, record.data))
            .try_collect()
            .await?;

        manager.close().await?;
        drop(manager);
        let _ = shutdown_tx.send(());
        manager_task.await??;
        cluster.shutdown().await?;

        actual.sort_unstable();
        expected.sort_unstable();
        assert_eq!(actual, expected);
        Ok(())
    })
    .await
    .context("mini cluster client write/read timed out")?
}
