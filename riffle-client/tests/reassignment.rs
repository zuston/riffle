use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::{BufMut, Bytes, BytesMut};
use futures::TryStreamExt;
use riffle_client::rpc_client::manager_rpc_client::ManagerRpcClient;
use riffle_client::rpc_client::{ServerConnOptions, ServerCoonPool};
use riffle_client::{
    CompressionCodec, Endpoint, ManagerRpcService, MiniCluster, MiniClusterConfig, ReadTaskInfo,
    ShuffleManagerServiceServer, ShuffleReader, ShuffleWriter, WriteTaskInfo,
};
use riffle_server::app_manager::application_identifier::ApplicationId;
use riffle_server::grpc::protobuf::uniffle::{RequireBufferRequest, StatusCode};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reassignment_preserves_data_across_two_tasks_and_servers() -> Result<()> {
    reassignment_preserves_data(CompressionCodec::Lz4).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reassignment_preserves_zstd_data_across_two_tasks_and_servers() -> Result<()> {
    reassignment_preserves_data(CompressionCodec::Zstd).await
}

async fn reassignment_preserves_data(compression_codec: CompressionCodec) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(45), async {
        let mut cluster = MiniCluster::new(MiniClusterConfig { num_servers: 2 });
        cluster.start().await?;
        let coordinator = cluster.coordinator_endpoint()?;
        let app_id = "mini-cluster-reassignment";
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
        let client = ManagerRpcClient::new(app_id.to_owned(), manager_endpoint.clone())?;
        let (initial, task0_id, _) = client.get_write_shuffle_handle(&task(0)).await?;
        let failed_endpoint = initial.get_endpoint(0)?;
        let healthy_endpoint = initial.get_endpoint(1)?;
        assert_ne!(failed_endpoint, healthy_endpoint);
        let shuffle_id = initial.shuffle_id;
        let first_records = records(0);
        let second_records = records(1);
        write_task(app_id, &manager_endpoint, 0, &first_records).await?;

        let failed_server = cluster
            .get_servers()
            .iter()
            .find(|server| server.config().grpc_port == failed_endpoint.grpc_port)
            .context("failed server is not in the mini cluster")?;
        let healthy_server = cluster
            .get_servers()
            .iter()
            .find(|server| server.config().grpc_port == healthy_endpoint.grpc_port)
            .context("healthy server is not in the mini cluster")?;
        let server_app_id = ApplicationId::from(app_id);
        let failed_app = failed_server
            .app_manager()
            .get_app(&server_app_id)
            .context("task 0 did not register the first server")?;
        let healthy_app = healthy_server
            .app_manager()
            .get_app(&server_app_id)
            .context("task 0 did not register the second server")?;
        assert_eq!(failed_app.reported_block_id_number(), 1);
        assert_eq!(healthy_app.reported_block_id_number(), 1);
        let failed_bytes = failed_app.total_received_data_size();
        let healthy_bytes = healthy_app.total_received_data_size();
        assert!(failed_bytes > 0 && healthy_bytes > 0);

        // Exhaust only A's write budget. Its task-0 blocks must remain readable.
        let available = failed_server.storage().mem_snapshot()?.available();
        let pool = ServerCoonPool::new();
        let reservation = pool
            .get_or_create_client(ServerConnOptions::new(failed_endpoint.clone()))?
            .require_buffer(RequireBufferRequest {
                app_id: app_id.to_owned(),
                shuffle_id: shuffle_id as i32,
                require_size: i32::try_from(available)?,
                partition_ids: vec![0],
            })
            .await?
            .into_inner();
        assert_eq!(reservation.status, StatusCode::Success as i32);
        assert_eq!(failed_server.storage().mem_snapshot()?.available(), 0);

        let (_, task1_id, _) = client.get_write_shuffle_handle(&task(1)).await?;
        write_task(app_id, &manager_endpoint, 1, &second_records).await?;
        let (updated, _, _) = client.get_write_shuffle_handle(&task(1)).await?;
        assert_eq!(
            updated.assignments.get(&0).unwrap().as_slice(),
            &[failed_endpoint.clone(), healthy_endpoint.clone()]
        );
        assert_eq!(
            updated.assignments.get(&1).unwrap().as_slice(),
            std::slice::from_ref(&healthy_endpoint)
        );
        assert_eq!(failed_app.total_received_data_size(), failed_bytes);
        assert_eq!(failed_app.reported_block_id_number(), 1);
        assert_eq!(healthy_app.reported_block_id_number(), 3);
        let mut second_blocks: HashMap<usize, BytesMut> = HashMap::new();
        for (partition, data) in &second_records {
            let block = second_blocks.entry(*partition).or_default();
            block.put_u32(data.len() as u32);
            block.extend_from_slice(data);
        }
        let mut second_bytes = 0;
        for block in second_blocks.values() {
            second_bytes += match compression_codec {
                CompressionCodec::Lz4 => lz4_flex::block::compress(block).len(),
                CompressionCodec::Zstd => zstd::bulk::compress(block, 3)?.len(),
            } as u64;
        }
        assert_eq!(
            healthy_app.total_received_data_size(),
            healthy_bytes + second_bytes,
            "healthy-server blocks must not be sent again during reassignment"
        );

        // Two writers reporting the old failure must reuse the same replacement.
        let (repeated, concurrent) = tokio::join!(
            client.reassign_partitions(
                shuffle_id,
                &[0],
                &failed_endpoint,
                StatusCode::NoBuffer as i32
            ),
            client.reassign_partitions(
                shuffle_id,
                &[0],
                &failed_endpoint,
                StatusCode::NoBuffer as i32
            ),
        );
        let expected_history = vec![failed_endpoint.clone(), healthy_endpoint.clone()];
        assert_eq!(repeated?[&0], expected_history);
        assert_eq!(concurrent?[&0], expected_history);

        let read_task = ReadTaskInfo {
            job_id: 1,
            upstream_stage_id: 1,
            partitions: vec![0, 1],
        };
        let (_, mut finished_tasks) = client.get_read_shuffle_handle(&read_task).await?;
        finished_tasks.sort_unstable();
        let mut expected_tasks = vec![task0_id, task1_id];
        expected_tasks.sort_unstable();
        assert_eq!(finished_tasks, expected_tasks);
        let reader = ShuffleReader::new(app_id.to_owned(), manager_endpoint, read_task).await?;
        let mut actual: Vec<_> = reader
            .stream()
            .map_ok(|record| (record.partition_id, record.data))
            .try_collect()
            .await?;
        let mut expected = first_records;
        expected.extend(second_records);
        actual.sort_unstable();
        expected.sort_unstable();
        assert_eq!(actual, expected);

        for (partitions, endpoint, status) in [
            (vec![2], failed_endpoint.clone(), StatusCode::NoBuffer),
            (vec![0, 0], failed_endpoint.clone(), StatusCode::NoBuffer),
            (vec![0], failed_endpoint.clone(), StatusCode::Success),
            (vec![0], "127.0.0.1:1".parse()?, StatusCode::NoBuffer),
        ] {
            let error = client
                .reassign_partitions(shuffle_id, &partitions, &endpoint, status as i32)
                .await
                .unwrap_err();
            assert_eq!(
                error
                    .chain()
                    .find_map(|cause| cause.downcast_ref::<tonic::Status>())
                    .context("missing RPC status")?
                    .code(),
                tonic::Code::InvalidArgument
            );
        }

        let reserved_bytes = failed_app
            .release_ticket(reservation.require_buffer_id)
            .await?;
        failed_app.dec_allocated_from_budget(reserved_bytes)?;
        // A is healthy again, but must not be chosen from this shuffle's failed history.
        let cluster_manager = cluster.get_coordinator().unwrap().cluster_manager();
        tokio::time::timeout(Duration::from_secs(5), async {
            while cluster_manager.list_available(&[]).len() != 2 {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .context("servers did not become available again")?;
        let error = client
            .reassign_partitions(
                shuffle_id,
                &[0],
                &healthy_endpoint,
                StatusCode::NoBuffer as i32,
            )
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("select replacement servers"));
        let (unchanged, _, _) = client.get_write_shuffle_handle(&task(1)).await?;
        assert_eq!(
            unchanged.assignments.get(&0).unwrap().as_slice(),
            expected_history.as_slice()
        );
        assert_eq!(
            unchanged.assignments.get(&1).unwrap().as_slice(),
            std::slice::from_ref(&healthy_endpoint)
        );

        manager.close().await?;
        drop(manager);
        let _ = shutdown_tx.send(());
        manager_task.await??;
        cluster.shutdown().await?;
        Ok(())
    })
    .await
    .context("reassignment integration test timed out")?
}

fn task(map_id: usize) -> WriteTaskInfo {
    WriteTaskInfo {
        job_id: 1,
        stage_id: 1,
        map_id,
        attempt_id: 0,
        num_mappers: 2,
        num_partitions: 2,
    }
}

fn records(map_id: usize) -> Vec<(usize, Bytes)> {
    (0..2)
        .flat_map(|partition| {
            [
                Bytes::new(),
                Bytes::from(format!("task={map_id}, partition={partition}")),
                Bytes::from_static(b"duplicate record"),
                Bytes::from_static(b"duplicate record"),
            ]
            .into_iter()
            .map(move |data| (partition, data))
        })
        .collect()
}

async fn write_task(
    app_id: &str,
    manager_endpoint: &Endpoint,
    map_id: usize,
    records: &[(usize, Bytes)],
) -> Result<()> {
    let mut writer =
        ShuffleWriter::new(app_id.to_owned(), manager_endpoint.clone(), task(map_id)).await?;
    for (partition, data) in records {
        writer.write(*partition, data.clone()).await?;
    }
    writer.finish().await
}
