use crate::error::WorkerError;
use crate::error::WorkerError::{STREAM_INCOMPLETE, STREAM_INCORRECT};
use crate::store::ResponseData::Mem;
use crate::store::{Block, DataBytes};
use crate::system_libc::send_file_full;
use crate::urpc::command::{
    GetLocalDataIndexRequestCommand, GetLocalDataIndexResponseCommand,
    GetLocalDataIndexV2ResponseCommand, GetLocalDataRequestCommand, GetLocalDataRequestV2Command,
    GetLocalDataRequestV3Command, GetLocalDataResponseCommand, GetMemoryDataRequestCommand,
    GetMemoryDataResponseCommand, ReadSegment, RpcResponseCommand, SendDataRequestCommand,
};
use anyhow::{Error, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::{error, warn};
use num_enum::{TryFromPrimitive, TryFromPrimitiveError};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::io;
use std::io::{Cursor, IoSlice};
use strum_macros::EnumVariantNames;
use tokio::io::{AsyncWriteExt, BufWriter, Interest};
use tokio::net::TcpStream;
use tracing::{debug, info};

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
pub enum MessageType {
    SendShuffleData = 3,
    GetMemoryData = 6,
    GetMemoryDataResponse = 16,

    GetLocalDataIndex = 4,
    GetLocalDataIndexResponse = 14,
    GetLocalDataIndexV2Response = 23,

    GetLocalData = 5,
    GetLocalDataV2 = 24,
    GetLocalDataV3 = 25,
    GetLocalDataResponse = 15,

    RpcResponse = 0,
}

const HEADER_LEN: usize = 4 + 1 + 4;

#[derive(Debug, strum_macros::Display)]
pub enum Frame {
    #[strum(serialize = "SendShuffleData")]
    SendShuffleData(SendDataRequestCommand),

    #[strum(serialize = "GetMemoryData")]
    GetMemoryData(GetMemoryDataRequestCommand),
    #[strum(serialize = "GetMemoryDataResponse")]
    GetMemoryDataResponse(GetMemoryDataResponseCommand),

    #[strum(serialize = "GetLocalDataIndex")]
    GetLocalDataIndex(GetLocalDataIndexRequestCommand),
    #[strum(serialize = "GetLocalDataIndexResponse")]
    GetLocalDataIndexResponse(GetLocalDataIndexResponseCommand),

    #[strum(serialize = "GetLocalDataIndexResponseV2")]
    GetLocalDataIndexV2Response(GetLocalDataIndexV2ResponseCommand),

    #[strum(serialize = "GetLocalData")]
    GetLocalData(GetLocalDataRequestCommand),

    #[strum(serialize = "GetLocalDataV2")]
    GetLocalDataV2(GetLocalDataRequestV2Command),

    #[strum(serialize = "GetLocalDataV3")]
    GetLocalDataV3(GetLocalDataRequestV3Command),

    #[strum(serialize = "GetLocalDataResponse")]
    GetLocalDataResponse(GetLocalDataResponseCommand),

    #[strum(serialize = "RpcResponse")]
    RpcResponse(RpcResponseCommand),
}

impl Frame {
    pub async fn write(
        stream: &mut TcpStream,
        frame: &Frame,
        write_buf: &mut BytesMut,
    ) -> Result<()> {
        match frame {
            Frame::GetLocalDataResponse(resp) => {
                debug!("gotten the localfile data response");

                let request_id = resp.request_id;
                let status_code = resp.status_code;

                let msg = &resp.ret_msg;
                let msg_bytes = msg.as_bytes();

                let data = &resp.data;

                // header
                write_buf.put_i32(msg_bytes.len() as i32 + 8 + 4 + 4);
                write_buf.put_u8(MessageType::GetLocalDataResponse as u8);
                write_buf.put_i32(data.len() as i32);

                // partial content with general response info
                write_buf.put_i64(request_id);
                write_buf.put_i32(status_code);

                write_buf.put_i32(msg_bytes.len() as i32);
                write_buf.put(msg_bytes);

                stream.write_all(&write_buf.split()).await?;

                // write all data
                match data {
                    DataBytes::Direct(val) => {
                        stream.write_all(val).await?;
                    }
                    DataBytes::Composed(val) => {
                        stream.write_all(val.freeze().as_ref()).await?;
                    }
                    DataBytes::RawIO(raw) => {
                        send_file_full(
                            stream,
                            raw.raw_fd,
                            Some(raw.offset as i64),
                            raw.length as usize,
                        )
                        .await.map_err(|e| {
                            error!("Errors on getting localfile data by sendfile. off:{}. length:{}. e: {}", raw.offset, raw.length, &e);
                            e
                        })?;
                    }
                }

                Ok(())
            }
            Frame::GetLocalDataIndexV2Response(resp) => {
                let request_id = resp.request_id;
                let status_code = resp.status_code;

                let msg = &resp.ret_msg;
                let msg_bytes = msg.as_bytes();

                let index_bytes = &resp.data_index.index_data;
                let data_file_len = resp.data_index.data_file_len;

                // header
                write_buf.put_i32(msg_bytes.len() as i32 + 8 + 4 + 4 + 8);
                write_buf.put_u8(MessageType::GetLocalDataIndexV2Response as u8);
                write_buf.put_i32(index_bytes.len() as i32);

                // partial content with general response info
                write_buf.put_i64(request_id);
                write_buf.put_i32(status_code);

                write_buf.put_i32(msg_bytes.len() as i32);
                write_buf.put(msg_bytes);

                // write the data length
                write_buf.put_i64(data_file_len);

                stream.write_all(&write_buf.split()).await?;

                // write the all bytes
                let data = index_bytes.freeze();
                stream.write_all(&data).await?;

                // write the storage ids
                write_buf.put_i32(resp.storage_ids.len() as i32);
                for storage_id in &resp.storage_ids {
                    write_buf.put_i32(*storage_id as i32);
                }
                stream.write_all(&write_buf.split()).await?;

                Ok(())
            }
            Frame::GetLocalDataIndexResponse(resp) => {
                debug!("gotten the localfile index response");

                let request_id = resp.request_id;
                let status_code = resp.status_code;

                let msg = &resp.ret_msg;
                let msg_bytes = msg.as_bytes();

                let index_bytes = &resp.data_index.index_data;
                let data_file_len = resp.data_index.data_file_len;

                // header
                write_buf.put_i32(msg_bytes.len() as i32 + 8 + 4 + 4 + 8);
                write_buf.put_u8(MessageType::GetLocalDataIndexResponse as u8);
                write_buf.put_i32(index_bytes.len() as i32);

                // partial content with general response info
                write_buf.put_i64(request_id);
                write_buf.put_i32(status_code);

                write_buf.put_i32(msg_bytes.len() as i32);
                write_buf.put(msg_bytes);

                // write the data length
                write_buf.put_i64(data_file_len);

                stream.write_all(&write_buf.split()).await?;
                // write the all bytes
                let data = index_bytes.freeze();
                stream.write_all(&data).await?;

                Ok(())
            }
            Frame::GetMemoryDataResponse(resp) => {
                let request_id = resp.request_id;
                let status_code = resp.status_code;

                let msg = &resp.ret_msg;
                let msg_bytes = msg.as_bytes();

                let read_result_data = &resp.data;
                let mem_data = match read_result_data {
                    Mem(mem_data) => mem_data,
                    _ => panic!("This should not happen that the result data is not mem type."),
                };

                let data_bytes_wrapper = &mem_data.data;
                let data_bytes_len = data_bytes_wrapper.len() as i32;

                let segments = &mem_data.shuffle_data_block_segments;
                let segments_encode_len = (4 + segments.len() * (3 * 8 + 3 * 4)) as i32;

                // header
                write_buf.put_i32(msg_bytes.len() as i32 + 8 + 4 + 4 + segments_encode_len);
                write_buf.put_u8(MessageType::GetMemoryDataResponse as u8);
                write_buf.put_i32(data_bytes_len);

                // partial content with general response info
                write_buf.put_i64(request_id);
                write_buf.put_i32(status_code);

                write_buf.put_i32(msg_bytes.len() as i32);
                write_buf.put(msg_bytes);

                // write segment
                write_buf.put_i32(segments.len() as i32);
                for segment in segments {
                    write_buf.put_i64(segment.block_id);
                    write_buf.put_i32(segment.offset as i32);
                    write_buf.put_i32(segment.length);
                    write_buf.put_i32(segment.uncompress_length);
                    write_buf.put_i64(segment.crc);
                    write_buf.put_i64(segment.task_attempt_id);
                }
                stream.write_all(&write_buf.split()).await?;

                // data_bytes
                for composed_byte in data_bytes_wrapper.always_composed().iter() {
                    stream.write_all(&composed_byte).await?;
                }
                Ok(())
            }
            Frame::RpcResponse(resp) => {
                let request_id = resp.request_id;
                let status_code = resp.status_code;

                let msg = &resp.ret_msg;
                let msg_bytes = msg.as_bytes();

                // header
                write_buf.put_i32(msg_bytes.len() as i32 + 8 + 4 + 4);
                write_buf.put_u8(MessageType::RpcResponse as u8);
                write_buf.put_i32(0);

                // content
                write_buf.put_i64(request_id);
                write_buf.put_i32(status_code);

                write_buf.put_i32(msg_bytes.len() as i32);
                write_buf.put(msg_bytes);

                stream.write_all(&write_buf.split()).await?;
                Ok(())
            }
            _ => todo!(),
        }
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
        skip(src, (msg_len + body_len) as usize)?;

        Ok(())
    }

    fn parse_to_get_localfile_data_v2_command(
        src: &mut Cursor<&[u8]>,
    ) -> Result<GetLocalDataRequestV2Command> {
        debug!("Gotten the localfile data v2 request");

        let request_id = get_i64(src)?;
        let app_id = get_string(src)?;
        let shuffle_id = get_i32(src)?;
        let partition_id = get_i32(src)?;
        let partition_num_per_range = get_i32(src)?;
        let partition_num = get_i32(src)?;
        let offset = get_i64(src)?;
        let length = get_i32(src)?;
        let timestamp = get_i64(src)?;

        // for the v2 version
        let storage_id = get_i32(src)?;

        Ok(GetLocalDataRequestV2Command {
            request_id,
            app_id,
            shuffle_id,
            partition_id,
            partition_num_per_range,
            partition_num,
            offset,
            length,
            timestamp,
            storage_id,
        })
    }

    fn parse_to_get_localfile_data_v3_command(
        src: &mut Cursor<&[u8]>,
    ) -> Result<GetLocalDataRequestV3Command> {
        debug!("Gotten the localfile data v3 request");

        let request_id = get_i64(src)?;
        let app_id = get_string(src)?;
        let shuffle_id = get_i32(src)?;
        let partition_id = get_i32(src)?;
        let partition_num_per_range = get_i32(src)?;
        let partition_num = get_i32(src)?;
        let offset = get_i64(src)?;
        let length = get_i32(src)?;
        let timestamp = get_i64(src)?;

        // for the v2 version
        let storage_id = get_i32(src)?;

        // for the v3 version.
        let segment_len = get_i32(src)?;
        let mut segments = Vec::with_capacity(segment_len as usize);
        for _ in 0..segment_len {
            segments.push(ReadSegment {
                offset: get_i64(src)?,
                length: get_i64(src)?,
            });
        }
        let task_id = get_i64(src)?;

        Ok(GetLocalDataRequestV3Command {
            request_id,
            app_id,
            shuffle_id,
            partition_id,
            partition_num_per_range,
            partition_num,
            offset,
            length,
            timestamp,
            storage_id,
            next_read_segments: segments,
            task_id,
        })
    }

    fn parse_to_get_localfile_data_command(
        src: &mut Cursor<&[u8]>,
    ) -> Result<GetLocalDataRequestCommand> {
        debug!("Gotten the localfile data request");

        let request_id = get_i64(src)?;
        let app_id = get_string(src)?;
        let shuffle_id = get_i32(src)?;
        let partition_id = get_i32(src)?;
        let partition_num_per_range = get_i32(src)?;
        let partition_num = get_i32(src)?;
        let offset = get_i64(src)?;
        let length = get_i32(src)?;
        let timestamp = get_i64(src)?;

        Ok(GetLocalDataRequestCommand {
            request_id,
            app_id,
            shuffle_id,
            partition_id,
            partition_num_per_range,
            partition_num,
            offset,
            length,
            timestamp,
        })
    }

    fn parse_to_send_shuffle_data_command(
        src: &mut Cursor<&[u8]>,
    ) -> Result<SendDataRequestCommand> {
        let request_id = get_i64(src)?;
        let app_id = get_string(src)?;
        let shuffle_id = get_i32(src)?;
        let require_id = get_i64(src)?;

        let partition_batch_size = get_i32(src)?;
        let mut blocks_map: HashMap<i32, Vec<Block>> =
            HashMap::with_capacity(partition_batch_size as usize);
        for idx in 0..partition_batch_size {
            let partition_id = get_i32(src)?;
            let block_batch_size = get_i32(src)?;
            let mut blocks = Vec::with_capacity(block_batch_size as usize);
            for block_idx in 0..block_batch_size {
                let pid = get_i32(src)?;
                let block_id = get_i64(src)?;
                let length = get_i32(src)?;
                let shuffle_id = get_i32(src)?;
                let crc = get_i64(src)?;
                let task_attempt_id = get_i64(src)?;
                // todo: make this allocated with the contiguous memory buffer.
                let buffer = get_bytes(src)?.unwrap_or(Bytes::new());

                /// skip the shuffle-servers data?
                let length_of_shuffle_servers = get_i32(src)?;
                for idx in 0..length_of_shuffle_servers {
                    let _ = skip_string(src)?;
                    let _ = skip_string(src)?;
                    let _ = get_i32(src)?;
                    let _ = get_i32(src)?;
                }

                let uncompress_len = get_i32(src)?;
                let free_mem = get_i64(src)?;

                let block = Block {
                    block_id,
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
        let timestamp = get_i64(src)?;
        let req = SendDataRequestCommand {
            request_id,
            app_id,
            shuffle_id,
            blocks: blocks_map,
            ticket_id: require_id,
            timestamp,
        };
        return Ok(req);
    }

    fn parse_to_get_localfile_index_command(
        src: &mut Cursor<&[u8]>,
    ) -> Result<GetLocalDataIndexRequestCommand> {
        debug!("Gotten the localfile index request");
        let request_id = get_i64(src)?;
        let app_id = get_string(src)?;
        let shuffle_id = get_i32(src)?;
        let partition_id = get_i32(src)?;
        let partition_num_per_range = get_i32(src)?;
        let partition_num = get_i32(src)?;

        Ok(GetLocalDataIndexRequestCommand {
            request_id,
            app_id,
            shuffle_id,
            partition_id,
            partition_num_per_range,
            partition_num,
        })
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
            timestamp,
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
            MessageType::GetLocalData => {
                let command = Frame::parse_to_get_localfile_data_command(src)?;
                return Ok(Frame::GetLocalData(command));
            }
            MessageType::GetLocalDataV2 => {
                let command = Frame::parse_to_get_localfile_data_v2_command(src)?;
                return Ok(Frame::GetLocalDataV2(command));
            }
            MessageType::GetLocalDataV3 => {
                let command = Frame::parse_to_get_localfile_data_v3_command(src)?;
                return Ok(Frame::GetLocalDataV3(command));
            }
            MessageType::GetLocalDataIndex => {
                let command = Frame::parse_to_get_localfile_index_command(src)?;
                return Ok(Frame::GetLocalDataIndex(command));
            }
            MessageType::GetMemoryData => {
                let command = Frame::parse_to_get_memory_data_command(src)?;
                return Ok(Frame::GetMemoryData(command));
            }
            MessageType::SendShuffleData => {
                let command = Frame::parse_to_send_shuffle_data_command(src)?;
                return Ok(Frame::SendShuffleData(command));
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

pub fn get_bytes(src: &mut Cursor<&[u8]>) -> Result<Option<Bytes>, WorkerError> {
    if !Buf::has_remaining(src) {
        return Err(STREAM_INCORRECT("get_bytes".into()));
    }
    let bytes_data_len = get_i32(src)?;
    if bytes_data_len <= 0 {
        return Ok(None);
    }

    if Buf::remaining(src) < bytes_data_len as usize {
        return Err(STREAM_INCORRECT(format!(
            "get_bytes but not have enough remaining bytes. expected: {}, real: {}",
            bytes_data_len,
            Buf::remaining(src)
        )));
    }

    let data = Bytes::copy_from_slice(&Buf::chunk(src)[..bytes_data_len as usize]);
    skip(src, bytes_data_len as usize)?;
    Ok(Some(data))
}

pub fn get_i64(src: &mut Cursor<&[u8]>) -> Result<i64, WorkerError> {
    if !Buf::has_remaining(src) {
        return Err(STREAM_INCORRECT("get_i64".into()));
    }

    Ok(src.get_i64())
}

pub fn get_i32(src: &mut Cursor<&[u8]>) -> Result<i32, WorkerError> {
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

fn skip_string(src: &mut Cursor<&[u8]>) -> Result<(), WorkerError> {
    if !Buf::has_remaining(src) {
        return Err(STREAM_INCORRECT("get_string 1".into()));
    }
    let len = get_i32(src)? as usize;
    if len <= 0 {
        return Ok(());
    }
    if Buf::remaining(src) < len {
        return Err(STREAM_INCORRECT(format!(
            "get string. src remaining: {}. len: {}",
            Buf::remaining(src),
            len
        )));
    }
    skip(src, len)?;
    Ok(())
}

pub fn get_string(src: &mut Cursor<&[u8]>) -> Result<String, WorkerError> {
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

pub fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, WorkerError> {
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
