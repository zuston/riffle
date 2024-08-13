use crate::app::{AppManagerRef, PartitionedUId, WritingViewContext};
use crate::constant::StatusCode;
use crate::store::Block;
use crate::urpc::connection::Connection;
use crate::urpc::frame::Frame;
use crate::urpc::shutdown::Shutdown;
use anyhow::Result;
use log::{error, warn};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum Command {
    Send(SendDataRequestCommand),
}

impl Command {
    pub fn from_frame(frame: Frame) -> Result<Command> {
        match frame {
            Frame::SendShuffleData(req) => {
                let request = req;
                Ok(Command::Send(request))
            }
            _ => todo!(),
        }
    }

    pub async fn apply(
        self,
        app_manager_ref: AppManagerRef,
        conn: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> Result<()> {
        match self {
            Command::Send(req) => req.apply(app_manager_ref, conn, shutdown).await?,
            _ => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SendDataRequestCommand {
    pub(crate) request_id: i64,
    pub(crate) app_id: String,
    pub(crate) shuffle_id: i32,
    pub(crate) blocks: HashMap<i32, Vec<Block>>,
    pub(crate) ticket_id: i64,
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
    async fn apply(
        self,
        app_manager_ref: AppManagerRef,
        conn: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> Result<()> {
        let request_id = self.request_id;
        let ticket_id = self.ticket_id;
        let shuffle_id = self.shuffle_id;
        let app_id = self.app_id.as_str();

        let app = app_manager_ref.get_app(&app_id);
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
        let ticket_len = match app.release_ticket(ticket_id).await {
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
            let uid = PartitionedUId::from(app_id.to_string(), shuffle_id, partition_id);
            let ctx = WritingViewContext::from(uid, partition_blocks);
            match app.insert(ctx).await {
                Ok(size) => insert_len += size as i64,
                Err(e) => {
                    let msg = format!(
                        "Errors on inserting data for app: {:?}. error:{:#?}",
                        app_id, e
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
            warn!("Has remaining {} allocated buffer.", &unused);
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
        Ok(())
    }
}
