use crate::app_manager::application_identifier::ApplicationId;
use crate::app_manager::partition_identifier::PartitionUId;
use crate::app_manager::request_context::{
    ReadingIndexViewContext, ReadingOptions, ReadingViewContext, RpcType, WritingViewContext,
};
use crate::app_manager::AppManagerRef;
use crate::config::RpcVersion;
use crate::constant::StatusCode;
use crate::metric::URPC_SEND_DATA_TRANSPORT_TIME;
use crate::store::ResponseDataIndex::Local;
use crate::store::{Block, DataBytes, LocalDataIndex, ResponseData};
use crate::urpc::command::ReadSegment;
use crate::urpc_uring::connection::Connection;
use crate::urpc_uring::frame::Frame;
use crate::util;
use anyhow::Result;
use await_tree::InstrumentAwait;
use bytes::Bytes;
use croaring::{JvmLegacy, Treemap};
use log::{debug, error, info};
use std::collections::HashMap;
use std::time::Instant;

#[derive(Debug)]
pub enum Command {
    Send(SendDataRequestCommand),
    GetMem(GetMemoryDataRequestCommand),
    GetLocalIndex(GetLocalDataIndexRequestCommand),
    GetLocalData(GetLocalDataRequestCommand),
    GetLocalDataV2(GetLocalDataRequestV2Command),
    GetLocalDataV3(GetLocalDataRequestV3Command),
}

impl Command {
    pub fn from_frame(frame: Frame) -> Result<Command> {
        match frame {
            Frame::SendShuffleData(req) => Ok(Command::Send(req)),
            Frame::GetMemoryData(req) => Ok(Command::GetMem(req)),
            Frame::GetLocalDataIndex(req) => Ok(Command::GetLocalIndex(req)),
            Frame::GetLocalData(req) => Ok(Command::GetLocalData(req)),
            Frame::GetLocalDataV2(req) => Ok(Command::GetLocalDataV2(req)),
            Frame::GetLocalDataV3(req) => Ok(Command::GetLocalDataV3(req)),
            _ => todo!(),
        }
    }

    pub async fn apply(self, app_manager_ref: AppManagerRef, conn: &mut Connection) -> Result<()> {
        match self {
            Command::Send(req) => req.apply(app_manager_ref, conn).await?,
            Command::GetMem(req) => req.apply(app_manager_ref, conn).await?,
            Command::GetLocalIndex(req) => req.apply(app_manager_ref, conn).await?,
            Command::GetLocalData(req) => req.apply(app_manager_ref, conn).await?,
            Command::GetLocalDataV2(req) => req.apply(app_manager_ref, conn).await?,
            Command::GetLocalDataV3(req) => req.apply(app_manager_ref, conn).await?,
            _ => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct GetMemoryDataRequestCommand {
    pub(crate) request_id: i64,
    pub(crate) app_id: String,
    pub(crate) shuffle_id: i32,
    pub(crate) partition_id: i32,
    pub(crate) last_block_id: i64,
    pub(crate) read_buffer_size: i32,
    pub(crate) expected_tasks_bitmap_raw: Option<Bytes>,
    pub(crate) timestamp: i64,
}

impl GetMemoryDataRequestCommand {
    pub(crate) async fn apply(
        &self,
        app_manager_ref: AppManagerRef,
        conn: &mut Connection,
    ) -> Result<()> {
        let timer = Instant::now();

        let request_id = self.request_id;
        let app_id = self.app_id.as_str();
        let shuffle_id = self.shuffle_id;
        let partition_id = self.partition_id;
        let last_block_id = self.last_block_id;
        let read_buffer_size = self.read_buffer_size;
        let application_id = ApplicationId::from(app_id);

        let app = app_manager_ref.get_app(&application_id);
        if app.is_none() {
            let response = RpcResponseCommand {
                request_id,
                status_code: StatusCode::NO_REGISTER.into(),
                ret_msg: "No such app in server side".to_string(),
            };
            write_response(conn, response).await?;
            return Ok(());
        }

        let app = app.unwrap();
        let uid = PartitionUId::new(&application_id, shuffle_id, partition_id);

        let ctx = ReadingViewContext::new(
            uid,
            ReadingOptions::MEMORY_LAST_BLOCK_ID_AND_MAX_SIZE(
                last_block_id,
                read_buffer_size as i64,
            ),
            RpcType::URPC,
        );
        let ctx = match &self.expected_tasks_bitmap_raw {
            Some(raw) => {
                let bitmap = Treemap::deserialize::<JvmLegacy>(raw);
                ctx.with_task_ids_filter(bitmap)
            }
            _ => ctx,
        };

        let result = app.select(ctx).await;
        let mut read_length = 0;

        let rpc_version = &app.app_config_options.get_memory_data_urpc_version;
        match rpc_version {
            RpcVersion::V1 => {
                let response = match result {
                    Err(e) => GetMemoryDataResponseCommand {
                        request_id,
                        status_code: StatusCode::INTERNAL_ERROR.into(),
                        ret_msg: format!("Errors on getting memory data. err: {:#?}", e),
                        data: ResponseData::Mem(Default::default()),
                    },
                    Ok(res) => {
                        read_length = res.len();
                        GetMemoryDataResponseCommand {
                            request_id,
                            status_code: StatusCode::SUCCESS.into(),
                            ret_msg: "".to_string(),
                            data: res,
                        }
                    }
                };
                let frame = Frame::GetMemoryDataResponse(response);
                conn.write_frame(&frame).await?;
            }
            _ => {
                let response = match result {
                    Err(e) => GetMemoryDataResponseV2Command {
                        request_id,
                        status_code: StatusCode::INTERNAL_ERROR.into(),
                        ret_msg: format!("Errors on getting memory data. err: {:#?}", e),
                        data: ResponseData::Mem(Default::default()),
                    },
                    Ok(res) => {
                        read_length = res.len();
                        GetMemoryDataResponseV2Command {
                            request_id,
                            status_code: StatusCode::SUCCESS.into(),
                            ret_msg: "".to_string(),
                            data: res,
                        }
                    }
                };
                let frame = Frame::GetMemoryDataV2Response(response);
                conn.write_frame(&frame).await?;
            }
        }

        info!(
            "[get_memory_data][{:?}] duration {}(ms) with {} bytes. app_id: {}, shuffle_id: {}, partition_id: {}.",
            rpc_version,
            timer.elapsed().as_millis(),
            read_length,
            app_id,
            shuffle_id,
            partition_id,
        );
        Ok(())
    }
}

#[derive(Debug)]
pub struct GetLocalDataResponseCommand {
    pub(crate) request_id: i64,
    pub(crate) status_code: i32,
    pub(crate) ret_msg: String,
    pub(crate) data: DataBytes,
}

#[derive(Debug, Default)]
pub struct GetLocalDataRequestCommand {
    pub(crate) request_id: i64,
    pub(crate) app_id: String,
    pub(crate) shuffle_id: i32,
    pub(crate) partition_id: i32,
    pub(crate) partition_num_per_range: i32,
    pub(crate) partition_num: i32,
    pub(crate) offset: i64,
    pub(crate) length: i32,
    pub(crate) timestamp: i64,
}

#[derive(Debug, Default)]
pub struct GetLocalDataRequestV2Command {
    pub(crate) request_id: i64,
    pub(crate) app_id: String,
    pub(crate) shuffle_id: i32,
    pub(crate) partition_id: i32,
    pub(crate) partition_num_per_range: i32,
    pub(crate) partition_num: i32,
    pub(crate) offset: i64,
    pub(crate) length: i32,
    pub(crate) timestamp: i64,
    pub(crate) storage_id: i32,
}

#[derive(Debug, Default)]
pub struct GetLocalDataRequestV3Command {
    pub(crate) request_id: i64,
    pub(crate) app_id: String,
    pub(crate) shuffle_id: i32,
    pub(crate) partition_id: i32,
    pub(crate) partition_num_per_range: i32,
    pub(crate) partition_num: i32,
    pub(crate) offset: i64,
    pub(crate) length: i32,
    pub(crate) timestamp: i64,
    pub(crate) storage_id: i32,
    pub(crate) next_read_segments: Vec<ReadSegment>,
    pub(crate) task_id: i64,
}

impl GetLocalDataRequestV2Command {
    pub(crate) async fn apply(
        &self,
        app_manager_ref: AppManagerRef,
        conn: &mut Connection,
    ) -> Result<()> {
        let timer = Instant::now();

        let request_id = self.request_id;
        let app_id = self.app_id.as_str();
        let shuffle_id = self.shuffle_id;
        let partition_id = self.partition_id;
        let offset = self.offset;
        let length = self.length;
        let application_id = ApplicationId::from(app_id);
        // ignore the storage id.
        let storage_id = self.storage_id;

        let app = app_manager_ref.get_app(&application_id);
        if app.is_none() {
            let command = GetLocalDataResponseCommand {
                request_id,
                status_code: StatusCode::NO_REGISTER.into(),
                ret_msg: "No such app in server side".to_string(),
                data: Default::default(),
            };
            let frame = Frame::GetLocalDataResponse(command);
            conn.write_frame(&frame)
                .instrument_await("No such app and then fast return")
                .await?;
            return Ok(());
        }

        let app = app.unwrap();
        let uid = PartitionUId::new(&application_id, shuffle_id, partition_id);
        let ctx = ReadingViewContext::new(
            uid,
            ReadingOptions::FILE_OFFSET_AND_LEN(offset, length as i64),
            RpcType::URPC,
        );
        let mut len = 0;
        let command = match app
            .select(ctx)
            .instrument_await(format!("getting local shuffle data for app:{}", &app_id))
            .await
        {
            Err(e) => GetLocalDataResponseCommand {
                request_id,
                status_code: StatusCode::INTERNAL_ERROR.into(),
                ret_msg: format!("Errors on getting file data. err: {:#?}", e),
                data: Default::default(),
            },
            Ok(result) => {
                if let ResponseData::Local(data) = result {
                    len = data.data.len();
                    GetLocalDataResponseCommand {
                        request_id,
                        status_code: StatusCode::SUCCESS.into(),
                        ret_msg: "".to_string(),
                        data: data.data,
                    }
                } else {
                    GetLocalDataResponseCommand {
                        request_id,
                        status_code: StatusCode::INTERNAL_ERROR.into(),
                        ret_msg: "Incorrect local data type.".to_string(),
                        data: Default::default(),
                    }
                }
            }
        };

        let frame = Frame::GetLocalDataResponse(command);
        conn.write_frame(&frame).await?;
        info!(
            "[get_local_data_v2] duration {}(ms) with {} bytes. app_id: {}, shuffle_id: {}, partition_id: {}",
            timer.elapsed().as_millis(),
            len,
            app_id,
            shuffle_id,
            partition_id,
        );
        Ok(())
    }
}

impl GetLocalDataRequestV3Command {
    pub(crate) async fn apply(
        &self,
        app_manager_ref: AppManagerRef,
        conn: &mut Connection,
    ) -> Result<()> {
        let timer = Instant::now();

        let request_id = self.request_id;
        let app_id = self.app_id.as_str();
        let shuffle_id = self.shuffle_id;
        let partition_id = self.partition_id;
        let offset = self.offset;
        let length = self.length;
        let application_id = ApplicationId::from(app_id);
        // todo: haven't support multi disk for one partition.
        let storage_id = self.storage_id;

        let app = app_manager_ref.get_app(&application_id);
        if app.is_none() {
            let command = GetLocalDataResponseCommand {
                request_id,
                status_code: StatusCode::NO_REGISTER.into(),
                ret_msg: "No such app in server side".to_string(),
                data: Default::default(),
            };
            let frame = Frame::GetLocalDataResponse(command);
            conn.write_frame(&frame)
                .instrument_await("No such app and then fast return")
                .await?;
            return Ok(());
        }

        let app = app.unwrap();
        let uid = PartitionUId::new(&application_id, shuffle_id, partition_id);
        let ctx = ReadingViewContext::new(
            uid,
            ReadingOptions::FILE_OFFSET_AND_LEN(offset, length as i64),
            RpcType::URPC,
        )
        .with_localfile_next_read_segments(self.next_read_segments.clone())
        .with_task_id(self.task_id);
        let mut len = 0;
        let command = match app
            .select(ctx)
            .instrument_await(format!("getting local shuffle data for app:{}", &app_id))
            .await
        {
            Err(e) => GetLocalDataResponseCommand {
                request_id,
                status_code: StatusCode::INTERNAL_ERROR.into(),
                ret_msg: format!("Errors on getting file data. err: {:#?}", e),
                data: Default::default(),
            },
            Ok(result) => {
                if let ResponseData::Local(data) = result {
                    len = data.data.len();
                    GetLocalDataResponseCommand {
                        request_id,
                        status_code: StatusCode::SUCCESS.into(),
                        ret_msg: "".to_string(),
                        data: data.data,
                    }
                } else {
                    GetLocalDataResponseCommand {
                        request_id,
                        status_code: StatusCode::INTERNAL_ERROR.into(),
                        ret_msg: "Incorrect local data type.".to_string(),
                        data: Default::default(),
                    }
                }
            }
        };

        let frame = Frame::GetLocalDataResponse(command);
        conn.write_frame(&frame).await?;
        info!(
            "[get_local_data_v3] duration {}(ms) with {} bytes. app_id: {}, shuffle_id: {}, partition_id: {}",
            timer.elapsed().as_millis(),
            len,
            app_id,
            shuffle_id,
            partition_id,
        );
        Ok(())
    }
}

impl GetLocalDataRequestCommand {
    pub(crate) async fn apply(
        &self,
        app_manager_ref: AppManagerRef,
        conn: &mut Connection,
    ) -> Result<()> {
        let timer = Instant::now();

        let request_id = self.request_id;
        let app_id = self.app_id.as_str();
        let shuffle_id = self.shuffle_id;
        let partition_id = self.partition_id;
        let offset = self.offset;
        let length = self.length;
        let application_id = ApplicationId::from(app_id);

        let app = app_manager_ref.get_app(&application_id);
        if app.is_none() {
            let command = GetLocalDataResponseCommand {
                request_id,
                status_code: StatusCode::NO_REGISTER.into(),
                ret_msg: "No such app in server side".to_string(),
                data: Default::default(),
            };
            let frame = Frame::GetLocalDataResponse(command);
            conn.write_frame(&frame)
                .instrument_await("No such app and then fast return")
                .await?;
            return Ok(());
        }

        let app = app.unwrap();
        let uid = PartitionUId::new(&application_id, shuffle_id, partition_id);
        let ctx = ReadingViewContext::new(
            uid,
            ReadingOptions::FILE_OFFSET_AND_LEN(offset, length as i64),
            RpcType::URPC,
        );
        let mut len = 0;
        let command = match app
            .select(ctx)
            .instrument_await(format!("getting local shuffle data for app:{}", &app_id))
            .await
        {
            Err(e) => GetLocalDataResponseCommand {
                request_id,
                status_code: StatusCode::INTERNAL_ERROR.into(),
                ret_msg: format!("Errors on getting file data. err: {:#?}", e),
                data: Default::default(),
            },
            Ok(result) => {
                if let ResponseData::Local(data) = result {
                    len = data.data.len();
                    GetLocalDataResponseCommand {
                        request_id,
                        status_code: StatusCode::SUCCESS.into(),
                        ret_msg: "".to_string(),
                        data: data.data,
                    }
                } else {
                    GetLocalDataResponseCommand {
                        request_id,
                        status_code: StatusCode::INTERNAL_ERROR.into(),
                        ret_msg: "Incorrect local data type.".to_string(),
                        data: Default::default(),
                    }
                }
            }
        };

        let frame = Frame::GetLocalDataResponse(command);
        conn.write_frame(&frame).await?;
        info!(
            "[get_local_data] duration {}(ms) with {} bytes. app_id: {}, shuffle_id: {}, partition_id: {}",
            timer.elapsed().as_millis(),
            len,
            app_id,
            shuffle_id,
            partition_id,
        );
        Ok(())
    }
}

#[derive(Debug)]
pub struct GetLocalDataIndexResponseCommand {
    pub(crate) request_id: i64,
    pub(crate) status_code: i32,
    pub(crate) ret_msg: String,
    pub(crate) data_index: LocalDataIndex,
}

#[derive(Debug)]
pub struct GetLocalDataIndexV2ResponseCommand {
    pub(crate) request_id: i64,
    pub(crate) status_code: i32,
    pub(crate) ret_msg: String,
    pub(crate) data_index: LocalDataIndex,
    pub(crate) storage_ids: Vec<usize>,
}

#[derive(Debug)]
pub struct GetLocalDataIndexRequestCommand {
    pub(crate) request_id: i64,
    pub(crate) app_id: String,
    pub(crate) shuffle_id: i32,
    pub(crate) partition_id: i32,
    pub(crate) partition_num_per_range: i32,
    pub(crate) partition_num: i32,
}

impl GetLocalDataIndexRequestCommand {
    pub(crate) async fn apply(
        &self,
        app_manager_ref: AppManagerRef,
        conn: &mut Connection,
    ) -> Result<()> {
        let timer = Instant::now();

        let request_id = self.request_id;
        let app_id = self.app_id.as_str();
        let shuffle_id = self.shuffle_id;
        let partition_id = self.partition_id;
        let application_id = ApplicationId::from(app_id);

        let app = app_manager_ref.get_app(&application_id);
        if app.is_none() {
            let command = GetLocalDataIndexResponseCommand {
                request_id,
                status_code: StatusCode::NO_REGISTER.into(),
                ret_msg: "No such app in server side".to_string(),
                data_index: Default::default(),
            };
            let frame = Frame::GetLocalDataIndexResponse(command);
            conn.write_frame(&frame).await?;
            return Ok(());
        }

        let app = app.unwrap();
        let application_id = ApplicationId::from(app_id);
        let uid = PartitionUId::new(&application_id, shuffle_id, partition_id);
        let ctx = ReadingIndexViewContext { partition_id: uid };

        let config = app_manager_ref.get_config();
        let rpc_version = if let Some(c) = &config.urpc_config {
            c.get_index_rpc_version.clone()
        } else {
            RpcVersion::V1
        };

        let mut len = 0;
        let result = app
            .list_index(ctx)
            .instrument_await(format!("listing localfile index for app:{}", &app_id))
            .await;
        let frame = match rpc_version {
            RpcVersion::V1 => {
                let command = match result {
                    Ok(index) => {
                        let Local(result) = index;
                        len = result.index_data.len();
                        GetLocalDataIndexResponseCommand {
                            request_id,
                            status_code: StatusCode::SUCCESS.into(),
                            ret_msg: "".to_string(),
                            data_index: result,
                        }
                    }
                    Err(e) => GetLocalDataIndexResponseCommand {
                        request_id,
                        status_code: StatusCode::INTERNAL_ERROR.into(),
                        ret_msg: format!("Errors on listing local index. err: {:#?}", e),
                        data_index: Default::default(),
                    },
                };
                Frame::GetLocalDataIndexResponse(command)
            }
            _ => {
                let command = match result {
                    Ok(index) => {
                        let Local(result) = index;
                        len = result.index_data.len();
                        GetLocalDataIndexV2ResponseCommand {
                            request_id,
                            status_code: StatusCode::SUCCESS.into(),
                            ret_msg: "".to_string(),
                            data_index: result,
                            // todo: haven't support multi disk for one partition.
                            storage_ids: vec![0],
                        }
                    }
                    Err(e) => GetLocalDataIndexV2ResponseCommand {
                        request_id,
                        status_code: StatusCode::INTERNAL_ERROR.into(),
                        ret_msg: format!("Errors on listing local index. err: {:#?}", e),
                        data_index: Default::default(),
                        storage_ids: vec![],
                    },
                };
                Frame::GetLocalDataIndexV2Response(command)
            }
        };
        conn.write_frame(&frame).await?;
        info!(
            "[get_local_index] duration {}(ms) with {} bytes. app_id: {}, shuffle_id: {}, partition_id: {}",
            timer.elapsed().as_millis(),
            len,
            app_id,
            shuffle_id,
            partition_id,
        );
        Ok(())
    }
}

#[derive(Debug)]
pub struct GetMemoryDataResponseCommand {
    pub(crate) request_id: i64,
    pub(crate) status_code: i32,
    pub(crate) ret_msg: String,
    pub(crate) data: ResponseData,
}

#[derive(Debug)]
pub struct GetMemoryDataResponseV2Command {
    pub(crate) request_id: i64,
    pub(crate) status_code: i32,
    pub(crate) ret_msg: String,
    pub(crate) data: ResponseData,
}

#[derive(Debug, Clone)]
pub struct SendDataRequestCommand {
    pub(crate) request_id: i64,
    pub(crate) app_id: String,
    pub(crate) shuffle_id: i32,
    pub(crate) blocks: HashMap<i32, Vec<Block>>,
    pub(crate) ticket_id: i64,
    pub(crate) timestamp: i64,
}

#[derive(Debug, Clone)]
pub struct RpcResponseCommand {
    pub(crate) request_id: i64,
    pub(crate) status_code: i32,
    pub(crate) ret_msg: String,
}

async fn write_response(conn: &mut Connection, command: RpcResponseCommand) -> Result<()> {
    let frame = Frame::RpcResponse(command);
    conn.write_frame(&frame).await
}

impl SendDataRequestCommand {
    async fn apply(self, app_manager_ref: AppManagerRef, conn: &mut Connection) -> Result<()> {
        let timer = Instant::now();

        let request_id = self.request_id;
        let ticket_id = self.ticket_id;
        let shuffle_id = self.shuffle_id;
        let app_id = self.app_id.as_str();
        let timestamp = self.timestamp;
        let application_id = ApplicationId::from(app_id);

        URPC_SEND_DATA_TRANSPORT_TIME
            .observe(((util::now_timestamp_as_millis() - timestamp as u128) / 1000) as f64);
        let app = app_manager_ref.get_app(&application_id);
        if app.is_none() {
            let response = RpcResponseCommand {
                request_id,
                status_code: StatusCode::NO_REGISTER.into(),
                ret_msg: "No such app in server side".to_string(),
            };
            write_response(conn, response).await?;
            return Ok(());
        }

        let app = app.unwrap();
        let ticket_len = match app
            .release_ticket(ticket_id)
            .instrument_await(format!("releasing allocated ticket for app:{}", &app_id))
            .await
        {
            Err(e) => {
                let response = RpcResponseCommand {
                    request_id,
                    status_code: StatusCode::INTERNAL_ERROR.into(),
                    ret_msg: "No such ticket id. Maybe it has been out of date".to_string(),
                };
                write_response(conn, response).await?;
                return Ok(());
            }
            Ok(len) => len,
        };

        let mut insert_failure_occur = false;
        let mut insert_failure_message = None;

        let mut insert_len = 0;
        let blocks = self.blocks;
        for block in blocks {
            let partition_id = block.0;
            let partition_blocks = block.1;
            let uid = PartitionUId::new(&application_id, shuffle_id, partition_id);
            let ctx = WritingViewContext::new(uid, partition_blocks);
            match app.insert(ctx).await {
                Ok(size) => insert_len += size as i64,
                Err(e) => {
                    let msg = format!(
                        "Errors on inserting data for app: {:?}. error:{:#?}",
                        &app_id, e
                    );
                    error!("{}", &msg);
                    insert_failure_occur = true;
                    insert_failure_message = Some(msg);
                }
            }
        }
        let _ = app.move_allocated_used_from_budget(insert_len);
        let unused = ticket_len - insert_len;
        if unused > 0 {
            debug!("Has remaining {} allocated buffer.", &unused);
            let _ = app.dec_allocated_from_budget(unused);
        }

        let response = match insert_failure_occur {
            true => RpcResponseCommand {
                request_id,
                status_code: StatusCode::INTERNAL_ERROR.into(),
                ret_msg: insert_failure_message.unwrap(),
            },
            _ => RpcResponseCommand {
                request_id,
                status_code: StatusCode::SUCCESS.into(),
                ret_msg: "".to_string(),
            },
        };
        write_response(conn, response).await?;
        debug!(
            "[send_data] duration {}(ms) with {} bytes. app_id: {}, shuffle_id: {}",
            timer.elapsed().as_millis(),
            insert_len,
            app_id,
            shuffle_id,
        );
        Ok(())
    }
}
