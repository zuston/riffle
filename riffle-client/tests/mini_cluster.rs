use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::TryStreamExt;
use riffle_client::rpc_client::manager_rpc_client::ManagerRpcClient;
use riffle_client::rpc_client::{ServerConnOptions, ServerCoonPool};
use riffle_client::{
    CompressionCodec, Endpoint, ManagerRpcService, MiniCluster, MiniClusterConfig, ReadTaskInfo,
    ShuffleManagerServiceServer, ShuffleReader, ShuffleWriter, WriteTaskInfo,
};
use riffle_server::grpc::protobuf::uniffle::{
    PartitionStats, ReportShuffleResultRequest, StatusCode, TaskAttemptIdToRecords,
};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mini_cluster_client_write_read() -> Result<()> {
    client_write_read(CompressionCodec::Lz4).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mini_cluster_client_write_read_zstd() -> Result<()> {
    client_write_read(CompressionCodec::Zstd).await
}

async fn client_write_read(compression_codec: CompressionCodec) -> Result<()> {
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
                    (
                        "riffle.client.compression.codec".to_owned(),
                        compression_codec.as_str().to_owned(),
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
                    num_partitions: 5,
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
            // An empty record becomes a block that grows under either codec.
            writer.write(4, Bytes::new()).await?;
            expected.push((4, Bytes::new()));
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
            manager_endpoint.clone(),
            ReadTaskInfo {
                job_id: 1,
                upstream_stage_id: 1,
                partitions: vec![0, 1, 2, 3, 4],
            },
        )
        .await?;
        let mut actual: Vec<_> = reader
            .stream()
            .map_ok(|record| (record.partition_id, record.data))
            .try_collect()
            .await?;

        // Verify subset reads aggregate both mappers and deduplicate requested partition IDs.
        // Empty partitions contribute zero records; empty payloads still count as records.
        let client = ManagerRpcClient::new(app_id.to_owned(), manager_endpoint.clone())?;
        let subset = ReadTaskInfo {
            job_id: 1,
            upstream_stage_id: 1,
            partitions: vec![0, 3, 3, 4],
        };
        let subset_records: Vec<_> =
            ShuffleReader::new(app_id.to_owned(), manager_endpoint.clone(), subset)
                .await?
                .stream()
                .try_collect()
                .await?;
        assert_eq!(subset_records.len(), 8);

        // Report one record without any server blocks: EOF must not silently succeed.
        let (assignment, task_id, _) = client
            .get_write_shuffle_handle(&WriteTaskInfo {
                job_id: 1,
                stage_id: 1,
                map_id: 2,
                attempt_id: 0,
                num_mappers: 3,
                num_partitions: 5,
            })
            .await?;
        let response = ServerCoonPool::new()
            .get_or_create_client(ServerConnOptions::new(assignment.get_endpoint(3)?))?
            .report_shuffle_result(ReportShuffleResultRequest {
                app_id: app_id.to_owned(),
                shuffle_id: assignment.shuffle_id as i32,
                task_attempt_id: task_id as i64,
                bitmap_num: 1,
                partition_to_block_ids: vec![],
                partition_stats: vec![PartitionStats {
                    partition_id: 3,
                    task_attempt_id_to_records: vec![TaskAttemptIdToRecords {
                        task_attempt_id: task_id as i64,
                        record_number: 1,
                    }],
                }],
            })
            .await?
            .into_inner();
        assert_eq!(response.status, StatusCode::Success as i32);
        // Statistics from an unfinished upstream task must not affect the read snapshot.
        let mut unfinished = ShuffleReader::new(
            app_id.to_owned(),
            manager_endpoint.clone(),
            ReadTaskInfo {
                job_id: 1,
                upstream_stage_id: 1,
                partitions: vec![3],
            },
        )
        .await?
        .stream();
        assert!(unfinished.try_next().await?.is_none());
        client
            .report_shuffle_result(assignment.shuffle_id, task_id)
            .await?;
        let mut missing = ShuffleReader::new(
            app_id.to_owned(),
            manager_endpoint,
            ReadTaskInfo {
                job_id: 1,
                upstream_stage_id: 1,
                partitions: vec![3],
            },
        )
        .await?
        .stream();
        let error = missing
            .try_next()
            .await
            .err()
            .context("missing records were not detected")?;
        assert!(error
            .to_string()
            .contains("partition 3: expected 1 records, read 0"));

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
