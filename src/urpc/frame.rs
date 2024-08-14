use crate::error::WorkerError;
use crate::error::WorkerError::{STREAM_INCOMPLETE, STREAM_INCORRECT};
use crate::store::Block;
use crate::urpc::command::{
    GetMemoryDataRequestCommand, GetMemoryDataResponseCommand, RpcResponseCommand,
    SendDataRequestCommand,
};
use anyhow::{Error, Result};
use bytes::{Buf, Bytes};
use log::warn;
use num_enum::{TryFromPrimitive, TryFromPrimitiveError};
use std::collections::HashMap;
use std::hash::Hash;
use std::io::Cursor;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

///
/// The encode urpc:
///
/// HEADER
/// 1. content_length   (i32, 4 bytes)
/// 2. message_type     (u8, 1 byte)
/// 3. body_length      (i32, 4 bytes)
///
/// CONTENT
/// 4. data
///

impl From<TryFromPrimitiveError<MessageType>> for WorkerError {
    fn from(value: TryFromPrimitiveError<MessageType>) -> Self {
        WorkerError::Other(Error::new(value))
    }
}

#[allow(non_camel_case_types)]
#[derive(Debug, Eq, PartialEq, TryFromPrimitive)]
#[repr(u8)]
enum MessageType {
    SendShuffleData = 3,
    GetMemoryData = 6,
    GetMemoryDataResponse = 16,

    GetLocalDataIndex = 4,
    GetLocalDataIndexResponse = 14,

    GetLocalData = 5,
    GetLocalDataResponse = 15,

    RpcResponse = 0,
}

const HEADER_LEN: usize = 4 + 1 + 4;

#[derive(Debug)]
pub enum Frame {
    SendShuffleData(SendDataRequestCommand),

    GetMemoryData(GetMemoryDataRequestCommand),
    GetMemoryDataResponse(GetMemoryDataResponseCommand),

    // GetLocalDataIndex(GetLocalDataIndexRequestCommand),
    // GetLocalDataIndexResponse(GetLocalDataIndexResponseCommand),
    //
    // GetLocalData(GetLocalDataRequestCommand),
    // GetLocalDataResponse(GetLocalDataResponseCommand),
    RpcResponse(RpcResponseCommand),
}

impl Frame {
    pub async fn write(stream: &mut BufWriter<TcpStream>, frame: &Frame) -> Result<()> {
        match frame {
            Frame::GetMemoryDataResponse(resp) => {
                let request_id = resp.request_id;
                let status_code = resp.status_code;

                let msg = &resp.ret_msg;
                let msg_bytes = msg.as_bytes();

                let mem_data = resp.data.from_memory();
                let data_bytes = mem_data.data;
                let data_bytes_len = data_bytes.len() as i32;

                let segments = mem_data.shuffle_data_block_segments;
                let segments_encode_len = (4 + segments.len() * (3 * 8 + 3 * 4)) as i32;

                // header
                stream
                    .write_i32(msg_bytes.len() as i32 + 8 + 4 + 4 + segments_encode_len)
                    .await?;
                stream
                    .write_u8(MessageType::GetMemoryDataResponse as u8)
                    .await?;
                stream.write_i32(data_bytes_len).await?;

                // partial content with general response info
                stream.write_i64(request_id).await?;
                stream.write_i32(status_code).await?;

                stream.write_i32(msg_bytes.len() as i32).await?;
                stream.write_all(msg_bytes).await?;

                // write segment
                stream.write_i32(segments.len() as i32).await?;
                for segment in segments {
                    stream.write_i64(segment.block_id).await?;
                    stream.write_i32(segment.offset as i32).await?;
                    stream.write_i32(segment.length).await?;
                    stream.write_i32(segment.uncompress_length).await?;
                    stream.write_i64(segment.crc).await?;
                    stream.write_i64(segment.task_attempt_id).await?;
                }

                // data_bytes
                stream.write_all(&*data_bytes).await?;

                return Ok(());
            }
            Frame::RpcResponse(resp) => {
                let request_id = resp.request_id;
                let status_code = resp.status_code;

                let msg = &resp.ret_msg;
                let msg_bytes = msg.as_bytes();

                // header
                stream.write_i32(msg_bytes.len() as i32 + 8 + 4 + 4).await?;
                stream.write_u8(MessageType::RpcResponse as u8).await?;
                stream.write_i32(0).await?;

                // content
                stream.write_i64(request_id).await?;
                stream.write_i32(status_code).await?;

                stream.write_i32(msg_bytes.len() as i32).await?;
                stream.write_all(msg_bytes).await?;
                return Ok(());
            }
            _ => todo!(),
        };
    }

    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), WorkerError> {
        if Buf::remaining(src) < HEADER_LEN {
            return Err(STREAM_INCOMPLETE);
        }

        let msg_len = get_i32(src)?;
        let msg_type = get_u8(src)?;
        let body_len = get_i32(src)?;

        if Buf::remaining(src) < (msg_len + body_len) as usize {
            return Err(STREAM_INCOMPLETE);
        }
        skip(src, msg_len as usize)?;

        Ok(())
    }

    fn parse_to_get_memory_data_command(
        src: &mut Cursor<&[u8]>,
    ) -> Result<GetMemoryDataRequestCommand> {
        let request_id = get_i64(src)?;
        let app_id = get_string(src)?;
        let shuffle_id = get_i32(src)?;
        let partition_id = get_i32(src)?;
        let last_block_id = get_i64(src)?;
        let read_buffer_size = get_i32(src)?;
        let timestamp = get_i64(src)?;

        let expected_task_bitmap_raw_option = get_bytes(src)?;
        Ok(GetMemoryDataRequestCommand {
            request_id,
            app_id,
            shuffle_id,
            partition_id,
            last_block_id,
            read_buffer_size,
            expected_tasks_bitmap_raw: expected_task_bitmap_raw_option,
        })
    }

    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, WorkerError> {
        let encode_msg_len = get_i32(src)?;
        let msg_type = get_u8(src)?;
        let body_len = get_i32(src)?;

        if Buf::remaining(src) < (encode_msg_len + body_len) as usize {
            warn!("This should not happen that the frame has been passed in check logic, but not have enough buffer to parse.");
            return Err(WorkerError::STREAM_ABNORMAL);
        }

        let msg_type = MessageType::try_from(msg_type);
        match msg_type {
            Err(e) => return Err(WorkerError::STREAM_MESSAGE_TYPE_NOT_FOUND),
            _ => {}
        }

        match msg_type? {
            MessageType::GetMemoryData => {
                let command = Frame::parse_to_get_memory_data_command(src)?;
                return Ok(Frame::GetMemoryData(command));
            }
            MessageType::SendShuffleData => {
                let request_id = get_i64(src)?;
                let app_id = get_string(src)?;
                let shuffle_id = get_i32(src)?;
                let require_id = get_i64(src)?;

                let mut blocks_map: HashMap<i32, Vec<Block>> = HashMap::new();

                let partition_batch_size = get_i32(src)?;
                for idx in 0..partition_batch_size {
                    let partition_id = get_i32(src)?;
                    let block_batch_size = get_i32(src)?;
                    let mut blocks = Vec::with_capacity(block_batch_size as usize);
                    for block_idx in 0..block_batch_size {
                        let pid = get_i32(src)?;
                        let blockId = get_i64(src)?;
                        let length = get_i32(src)?;
                        let shuffle_id = get_i32(src)?;
                        let crc = get_i64(src)?;
                        let task_attempt_id = get_i64(src)?;
                        // todo: make this allocated with the contiguous memory buffer.
                        let buffer = get_bytes(src)?.unwrap_or(Bytes::new());

                        /// skip the shuffle-servers data?
                        let length_of_shuffle_servers = get_i32(src)?;
                        for idx in 0..length_of_shuffle_servers {
                            let _ = get_string(src)?;
                            let _ = get_string(src)?;
                            let _ = get_i32(src)?;
                            let _ = get_i32(src)?;
                        }

                        let uncompress_len = get_i32(src)?;
                        let free_mem = get_i64(src)?;

                        let block = Block {
                            block_id: blockId,
                            length,
                            uncompress_length: uncompress_len,
                            crc,
                            data: buffer,
                            task_attempt_id,
                        };
                        blocks.push(block);
                    }

                    blocks_map.insert(partition_id, blocks);
                }
                let req = SendDataRequestCommand {
                    request_id,
                    app_id,
                    shuffle_id,
                    blocks: blocks_map,
                    ticket_id: require_id,
                };
                return Ok(Frame::SendShuffleData(req));
            }
            MessageType::RpcResponse => {
                let request_id = get_i64(src)?;
                let status_code = get_i32(src)?;
                let ret_msg = get_string(src)?;
                return Ok(Frame::RpcResponse(RpcResponseCommand {
                    request_id,
                    status_code,
                    ret_msg,
                }));
            }
            _ => {
                todo!()
            }
        }
        todo!()
    }
}

fn get_bytes(src: &mut Cursor<&[u8]>) -> Result<Option<Bytes>, WorkerError> {
    if !Buf::has_remaining(src) {
        return Err(STREAM_INCORRECT("get_bytes".into()));
    }
    let bytes_data_len = get_i32(src)? as usize;
    if bytes_data_len < 0 {
        return Ok(None);
    }

    if Buf::remaining(src) < bytes_data_len {
        return Err(STREAM_INCORRECT(
            "get_bytes but not have enough remaining bytes".into(),
        ));
    }

    let data = Bytes::copy_from_slice(&Buf::chunk(src)[..bytes_data_len]);
    skip(src, bytes_data_len)?;
    Ok(Some(data))
}

fn get_i64(src: &mut Cursor<&[u8]>) -> Result<i64, WorkerError> {
    if !Buf::has_remaining(src) {
        return Err(STREAM_INCORRECT("get_i64".into()));
    }

    Ok(src.get_i64())
}

fn get_i32(src: &mut Cursor<&[u8]>) -> Result<i32, WorkerError> {
    if !Buf::has_remaining(src) {
        return Err(STREAM_INCORRECT("get_i32".into()));
    }
    Ok(src.get_i32())
}

fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), WorkerError> {
    if Buf::remaining(src) < n {
        return Err(STREAM_INCORRECT("skip".into()));
    }

    Buf::advance(src, n);
    Ok(())
}

fn get_string(src: &mut Cursor<&[u8]>) -> Result<String, WorkerError> {
    if !Buf::has_remaining(src) {
        return Err(STREAM_INCORRECT("get_string 1".into()));
    }
    let len = get_i32(src)? as usize;
    if len <= 0 {
        return Ok("".into());
    }

    if Buf::remaining(src) < len {
        return Err(STREAM_INCORRECT(format!(
            "get string. src remaining: {}. len: {}",
            Buf::remaining(src),
            len
        )));
    }

    let msg = Bytes::copy_from_slice(&Buf::chunk(src)[..len]);
    skip(src, len)?;

    Ok(String::from_utf8(msg.to_vec())?)
}

fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, WorkerError> {
    if !Buf::has_remaining(src) {
        return Err(STREAM_INCORRECT("get_u8".into()));
    }
    Ok(src.get_u8())
}

#[cfg(test)]
mod test {
    use crate::error::WorkerError;
    use crate::urpc::frame::Frame;
    use anyhow::Result;
    use bytes::{BufMut, Bytes, BytesMut};
    use std::io::Cursor;

    ///
    /// The encode urpc:
    ///
    /// 1. encoded_length(i32, 4 bytes)
    /// 2. message_type(u8, 1 byte)
    /// 3. body_length (i32, 4 bytes)
    /// 4. content.
    ///

    #[test]
    fn frame_parse() -> Result<()> {
        Ok(())
    }

    #[test]
    fn frame_check() -> Result<()> {
        /// case1: something lack, and then check will fast fail
        let mut send_data_request = BytesMut::new();
        // encoded_length
        send_data_request.put_i32(128);
        // message_type
        send_data_request.put_u8(b'1');
        // body_length(only for some read request to transfer file data)
        send_data_request.put_i32(0);

        let cursor = &mut Cursor::new(&send_data_request[..]);
        match Frame::check(cursor) {
            Ok(_) => panic!(),
            Err(WorkerError::STREAM_INCOMPLETE) => {}
            _ => panic!(),
        }

        /// case2: check will pass
        // data bytes
        send_data_request.put(Bytes::from(vec![0; 128]));
        let cursor = &mut Cursor::new(&send_data_request[..]);
        Frame::check(cursor).unwrap();

        Ok(())
    }
}
