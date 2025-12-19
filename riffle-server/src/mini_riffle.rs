use crate::app_manager::application_identifier::ApplicationId;
use crate::app_manager::partition_identifier::PartitionUId;
use crate::app_manager::request_context::{ReadingOptions, ReadingViewContext, RpcType};
use crate::app_manager::{AppManager, AppManagerRef};
use crate::common::init_global_variable;
use crate::config;
use crate::config::Config;
use crate::config_reconfigure::ReconfigurableConfManager;
use crate::grpc::protobuf::uniffle::shuffle_server_client::ShuffleServerClient;
use crate::grpc::protobuf::uniffle::{
    GetLocalShuffleDataRequest, GetLocalShuffleIndexRequest, GetMemoryShuffleDataRequest,
    GetShuffleResultRequest, PartitionToBlockIds, ReportShuffleResultRequest, RequireBufferRequest,
    SendShuffleDataRequest, ShuffleBlock, ShuffleData, ShuffleRegisterRequest,
};
use crate::http::HttpMonitorService;
use crate::id_layout::DEFAULT_BLOCK_ID_LAYOUT;
use crate::metric::MetricService;
use crate::runtime::manager::RuntimeManager;
use crate::server_state_manager::ServerStateManager;
use crate::storage::StorageService;
use crate::urpc::client::UrpcClient;
use crate::urpc::command::GetLocalDataRequestCommand;
use bytes::{Buf, Bytes, BytesMut};
use croaring::{JvmLegacy, Treemap};
use std::collections::HashSet;
use std::time::Duration;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::oneshot;
use tonic::transport::Channel;

/// The entrypoint to start the mini riffle server.
pub async fn start(config: &Config) -> anyhow::Result<AppManagerRef> {
    let config = config.clone();
    init_global_variable(&config);
    let runtime_manager = RuntimeManager::from(config.runtime_config.clone());

    MetricService::init(&config, runtime_manager.clone());

    let (tx, rx) = oneshot::channel::<()>();

    let reconf_manager = ReconfigurableConfManager::new(&config, None)?;
    let storage = StorageService::init(&runtime_manager, &config, &reconf_manager);
    let app_manager_ref = AppManager::get_ref(
        runtime_manager.clone(),
        config.clone(),
        &storage,
        &reconf_manager,
    );

    HttpMonitorService::init(&config, runtime_manager.clone());

    let app_manager_ref_cloned = app_manager_ref.clone();
    let rm = runtime_manager.clone();
    let server_state_manager = ServerStateManager::new(&app_manager_ref, &config);
    runtime_manager.default_runtime.spawn(async move {
        crate::rpc::DefaultRpcService {}.start(
            &config,
            rm,
            app_manager_ref_cloned,
            &server_state_manager,
        )
    });

    runtime_manager.default_runtime.spawn(async move {
        let _ = signal(SignalKind::terminate())
            .expect("Failed to register signal handlers")
            .recv()
            .await;

        let _ = tx.send(());
    });

    Ok(app_manager_ref)
}

pub async fn shuffle_testing(config: &Config, app_ref: AppManagerRef) -> anyhow::Result<()> {
    let grpc_port = config.grpc_port;
    let urpc_port = config.urpc_port;

    let mut grpc_client =
        match ShuffleServerClient::connect(format!("http://{}:{}", "0.0.0.0", grpc_port)).await {
            Ok(client) => client,
            Err(e) => {
                // Handle the error, e.g., by panicking or logging it.
                panic!("Failed to connect: {}", e);
            }
        };
    let mut urpc_client = match urpc_port {
        None => None,
        Some(port) => Some(UrpcClient::connect("0.0.0.0", port as usize).await?),
    };

    let app_id = ApplicationId::mock();
    let raw_app_id = app_id.to_string();
    let register_response = grpc_client
        .register_shuffle(ShuffleRegisterRequest {
            app_id: raw_app_id.clone(),
            shuffle_id: 0,
            partition_ranges: vec![],
            remote_storage: None,
            user: "".to_string(),
            shuffle_data_distribution: 1,
            max_concurrency_per_partition_to_write: 10,
            merge_context: None,
            properties: Default::default(),
        })
        .await?
        .into_inner();
    assert_eq!(register_response.status, 0);

    let mut all_bytes_data = BytesMut::new();
    let mut block_ids = vec![];

    let batch_size = 30;
    let data = b"hello world";
    for idx in 0..batch_size {
        block_ids.push(idx as i64);

        let len = data.len();

        all_bytes_data.extend_from_slice(data);

        let buffer_required_resp = grpc_client
            .require_buffer(RequireBufferRequest {
                require_size: len as i32,
                app_id: raw_app_id.clone(),
                shuffle_id: 0,
                partition_ids: vec![],
            })
            .await?
            .into_inner();

        assert_eq!(0, buffer_required_resp.status);

        let response = grpc_client
            .send_shuffle_data(SendShuffleDataRequest {
                app_id: raw_app_id.clone(),
                shuffle_id: 0,
                require_buffer_id: buffer_required_resp.require_buffer_id,
                shuffle_data: vec![ShuffleData {
                    partition_id: idx,
                    block: vec![ShuffleBlock {
                        block_id: idx as i64,
                        length: len as i32,
                        uncompress_length: 0,
                        crc: 0,
                        data: Bytes::copy_from_slice(data),
                        task_attempt_id: 0,
                    }],
                }],
                timestamp: 0,
                stage_attempt_number: 0,
                combined_shuffle_data: None,
            })
            .await?;

        let response = response.into_inner();
        assert_eq!(0, response.status);

        // report the finished block ids
        let partition_id = idx;
        let block_id = DEFAULT_BLOCK_ID_LAYOUT.get_block_id(1, partition_id as i64, 1);
        grpc_client
            .report_shuffle_result(ReportShuffleResultRequest {
                app_id: raw_app_id.clone(),
                shuffle_id: 0,
                task_attempt_id: 0,
                bitmap_num: 0,
                partition_to_block_ids: vec![PartitionToBlockIds {
                    partition_id: idx,
                    block_ids: vec![block_id],
                }],
                partition_stats: vec![],
            })
            .await?;
    }

    tokio::time::sleep(Duration::from_secs(1)).await;

    // check from the riffle-server point
    assert_eq!(1, app_ref.apps.len());
    let app = app_ref.get_app(&app_id).unwrap();
    let uid = PartitionUId::new(&app_id, 0, 0);
    let response = app
        .select(ReadingViewContext {
            uid: uid.clone(),
            reading_options: ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(-1, 10000000),
            task_ids_filter: None,
            rpc_source: RpcType::GRPC,
            read_ahead_client_enabled: false,
            sequential: false,
            read_ahead_batch_number: None,
            read_ahead_batch_size: None,
            localfile_next_read_segments: vec![],
            task_id: 0,
            urpc_read_io_mode: Default::default(),
        })
        .await?;
    let mut total_partition_len = 0;
    let xdata = response.from_memory();
    total_partition_len += xdata.data.len();
    let response = app
        .select(ReadingViewContext {
            uid: uid.clone(),
            reading_options: ReadingOptions::FILE_OFFSET_AND_LEN(0, data.len() as i64),
            task_ids_filter: None,
            rpc_source: RpcType::GRPC,
            read_ahead_client_enabled: false,
            sequential: false,
            read_ahead_batch_number: None,
            read_ahead_batch_size: None,
            localfile_next_read_segments: vec![],
            task_id: 0,
            urpc_read_io_mode: Default::default(),
        })
        .await?;
    let xdata = response.from_local();
    total_partition_len += xdata.len();
    assert_eq!(data.len(), total_partition_len);

    let mut accepted_block_ids = HashSet::new();
    let mut accepted_data_bytes = BytesMut::new();

    let mut grpc_accepted_data_bytes_from_localfile = BytesMut::new();
    let mut urpc_accepted_data_bytes_from_localfile = BytesMut::new();

    // firstly. read from the memory
    for idx in 0..batch_size {
        let block_id_result = grpc_client
            .get_shuffle_result(GetShuffleResultRequest {
                app_id: raw_app_id.clone(),
                shuffle_id: 0,
                partition_id: idx,
                block_id_layout: None,
            })
            .await?
            .into_inner();

        assert_eq!(0, block_id_result.status);

        let block_id_bitmap =
            Treemap::deserialize::<JvmLegacy>(&*block_id_result.serialized_bitmap);
        assert_eq!(1, block_id_bitmap.iter().count());
        for entry in block_id_bitmap.iter() {
            assert_eq!(
                idx as i64,
                DEFAULT_BLOCK_ID_LAYOUT.get_partition_id(entry as i64)
            );
        }

        let response_data = grpc_client
            .get_memory_shuffle_data(GetMemoryShuffleDataRequest {
                app_id: raw_app_id.clone(),
                shuffle_id: 0,
                partition_id: idx,
                last_block_id: -1,
                read_buffer_size: 10000000,
                timestamp: 0,
                serialized_expected_task_ids_bitmap: Default::default(),
            })
            .await?;
        let response = response_data.into_inner();
        let segments = response.shuffle_data_block_segments;
        for segment in segments {
            accepted_block_ids.insert(segment.block_id);
        }
        let data = response.data;
        accepted_data_bytes.extend_from_slice(&data);
    }

    // secondly, read from the localfile
    for idx in 0..batch_size {
        let local_index_data = grpc_client
            .get_local_shuffle_index(GetLocalShuffleIndexRequest {
                app_id: raw_app_id.clone(),
                shuffle_id: 0,
                partition_id: idx,
                partition_num_per_range: 1,
                partition_num: 0,
            })
            .await?;

        let mut bytes = local_index_data.into_inner().index_data;
        if bytes.is_empty() {
            continue;
        }
        // index_bytes_holder.put_i64(next_offset);
        // index_bytes_holder.put_i32(length);
        // index_bytes_holder.put_i32(uncompress_len);
        // index_bytes_holder.put_i64(crc);
        // index_bytes_holder.put_i64(block_id);
        // index_bytes_holder.put_i64(task_attempt_id);
        bytes.get_i64();
        let len = bytes.get_i32();
        bytes.get_i32();
        bytes.get_i64();
        let id = bytes.get_i64();
        accepted_block_ids.insert(id);

        // getting the localfile data from the grpc
        let partitioned_local_data = grpc_client
            .get_local_shuffle_data(GetLocalShuffleDataRequest {
                app_id: raw_app_id.clone(),
                shuffle_id: 0,
                partition_id: idx,
                partition_num_per_range: 0,
                partition_num: 0,
                offset: 0,
                length: len,
                timestamp: 0,
                storage_id: 0,
            })
            .await?;
        let data = partitioned_local_data.into_inner().data;
        accepted_data_bytes.extend_from_slice(&data);
        grpc_accepted_data_bytes_from_localfile.extend_from_slice(&data);

        if let Some(u_client) = &mut urpc_client {
            let data = u_client
                .get_local_shuffle_data(GetLocalDataRequestCommand {
                    request_id: 0,
                    app_id: raw_app_id.clone(),
                    shuffle_id: 0,
                    partition_id: idx,
                    partition_num_per_range: 0,
                    partition_num: 0,
                    offset: 0,
                    length: len,
                    timestamp: 0,
                })
                .await?;

            // split the getting request into 2 requests
            let data_1 = u_client
                .get_local_shuffle_data(GetLocalDataRequestCommand {
                    request_id: 0,
                    app_id: raw_app_id.clone(),
                    shuffle_id: 0,
                    partition_id: idx,
                    partition_num_per_range: 0,
                    partition_num: 0,
                    offset: 0,
                    length: len - 1,
                    timestamp: 0,
                })
                .await?;
            let data_2 = u_client
                .get_local_shuffle_data(GetLocalDataRequestCommand {
                    request_id: 0,
                    app_id: raw_app_id.clone(),
                    shuffle_id: 0,
                    partition_id: idx,
                    partition_num_per_range: 0,
                    partition_num: 0,
                    offset: (len - 1) as i64,
                    length: 1,
                    timestamp: 0,
                })
                .await?;
            assert_eq!(len - 1, data_1.len() as i32);
            assert_eq!(1, data_2.len() as i32);
            urpc_accepted_data_bytes_from_localfile.extend_from_slice(&data);
        }
    }

    // check the block ids
    assert_eq!(batch_size as usize, accepted_block_ids.len());
    // assert_eq!(block_ids, accepted_block_ids.iter().collect());

    // check the shuffle data
    assert_eq!(all_bytes_data.freeze(), accepted_data_bytes.freeze());

    // check the grpc and urpc data from the localfile
    if !urpc_accepted_data_bytes_from_localfile.is_empty() {
        assert_eq!(
            grpc_accepted_data_bytes_from_localfile.freeze(),
            urpc_accepted_data_bytes_from_localfile.freeze()
        );
    }

    Ok(())
}
