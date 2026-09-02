use crate::config::UrpcWriteMode;
use crate::error::WorkerError;
use crate::error::WorkerError::{STREAM_INCOMPLETE, STREAM_INCORRECT};
use crate::store::ResponseData::Mem;
use crate::store::{Block, DataBytes};
use crate::system_libc::{self, send_file_full};
use crate::urpc::command::{
    GetLocalDataIndexRequestCommand, GetLocalDataIndexResponseCommand,
    GetLocalDataIndexV2ResponseCommand, GetLocalDataRequestCommand, GetLocalDataRequestV2Command,
    GetLocalDataRequestV3Command, GetLocalDataResponseCommand, GetMemoryDataRequestCommand,
    GetMemoryDataResponseCommand, GetMemoryDataResponseV2Command, ReadSegment, RpcResponseCommand,
    SendDataRequestCommand,
};
use anyhow::{Error, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::{error, warn};
use num_enum::{TryFromPrimitive, TryFromPrimitiveError};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::io::{Cursor, IoSlice};
use strum_macros::EnumVariantNames;
use tokio::io::AsyncWriteExt;
use tokio::net::unix::pipe;
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
    GetMemoryDataV2Response = 26,

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
const MAX_WRITE_VECTORS: usize = 64;

#[derive(Debug, strum_macros::Display)]
pub enum Frame {
    #[strum(serialize = "SendShuffleData")]
    SendShuffleData(SendDataRequestCommand),

    #[strum(serialize = "GetMemoryData")]
    GetMemoryData(GetMemoryDataRequestCommand),
    #[strum(serialize = "GetMemoryDataResponse")]
    GetMemoryDataResponse(GetMemoryDataResponseCommand),

    #[strum(serialize = "GetMemoryDataV2Response")]
    GetMemoryDataV2Response(GetMemoryDataResponseV2Command),

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
        Self::write_with_mode(stream, frame, write_buf, UrpcWriteMode::default()).await
    }

    pub async fn write_with_mode(
        stream: &mut TcpStream,
        frame: &Frame,
        write_buf: &mut BytesMut,
        write_mode: UrpcWriteMode,
    ) -> Result<()> {
        let data = frame.encode_head(write_buf)?;
        stream.write_all(&write_buf.split()).await?;
        if let Some(data) = data {
            let write_mode = match frame {
                Frame::GetLocalDataIndexResponse(_) | Frame::GetLocalDataIndexV2Response(_) => {
                    UrpcWriteMode::FREEZE
                }
                _ => write_mode,
            };
            write_data_bytes(stream, data, write_mode).await?;
        }
        Ok(())
    }

    pub(crate) fn encode_head<'a>(&'a self, head: &mut BytesMut) -> Result<Option<&'a DataBytes>> {
        match self {
            Frame::GetLocalDataResponse(resp) => {
                debug!("gotten the localfile data response");
                let msg_bytes = resp.ret_msg.as_bytes();
                let data = &resp.data;
                head.reserve(HEADER_LEN + 16 + msg_bytes.len());
                head.put_i32(msg_bytes.len() as i32 + 8 + 4 + 4);
                head.put_u8(MessageType::GetLocalDataResponse as u8);
                head.put_i32(data.len() as i32);
                head.put_i64(resp.request_id);
                head.put_i32(resp.status_code);
                head.put_i32(msg_bytes.len() as i32);
                head.put(msg_bytes);
                Ok(Some(data))
            }
            Frame::GetLocalDataIndexV2Response(resp) => {
                let msg_bytes = resp.ret_msg.as_bytes();
                let index_bytes = &resp.data_index.index_data;
                head.reserve(HEADER_LEN + 28 + msg_bytes.len() + 4 * resp.storage_ids.len());
                head.put_i32(
                    msg_bytes.len() as i32 + 8 + 4 + 4 + 8 + 4 + 4 * resp.storage_ids.len() as i32,
                );
                head.put_u8(MessageType::GetLocalDataIndexV2Response as u8);
                head.put_i32(index_bytes.len() as i32);
                head.put_i64(resp.request_id);
                head.put_i32(resp.status_code);
                head.put_i32(msg_bytes.len() as i32);
                head.put(msg_bytes);
                head.put_i64(resp.data_index.data_file_len);
                head.put_i32(resp.storage_ids.len() as i32);
                for storage_id in &resp.storage_ids {
                    head.put_i32(*storage_id as i32);
                }
                Ok(Some(index_bytes))
            }
            Frame::GetLocalDataIndexResponse(resp) => {
                debug!("gotten the localfile index response");
                let msg_bytes = resp.ret_msg.as_bytes();
                let index_bytes = &resp.data_index.index_data;
                head.reserve(HEADER_LEN + 24 + msg_bytes.len());
                head.put_i32(msg_bytes.len() as i32 + 8 + 4 + 4 + 8);
                head.put_u8(MessageType::GetLocalDataIndexResponse as u8);
                head.put_i32(index_bytes.len() as i32);
                head.put_i64(resp.request_id);
                head.put_i32(resp.status_code);
                head.put_i32(msg_bytes.len() as i32);
                head.put(msg_bytes);
                head.put_i64(resp.data_index.data_file_len);
                Ok(Some(index_bytes))
            }
            Frame::GetMemoryDataResponse(resp) => {
                let mem_data = match &resp.data {
                    Mem(mem_data) => mem_data,
                    _ => return Err(Error::msg("GetMemoryDataResponse requires mem typed data")),
                };
                let msg_bytes = resp.ret_msg.as_bytes();
                let segments = &mem_data.shuffle_data_block_segments;
                let segments_encode_len = (4 + segments.len() * (3 * 8 + 3 * 4)) as i32;
                head.reserve(HEADER_LEN + 16 + msg_bytes.len() + segments_encode_len as usize);
                head.put_i32(msg_bytes.len() as i32 + 8 + 4 + 4 + segments_encode_len);
                head.put_u8(MessageType::GetMemoryDataResponse as u8);
                head.put_i32(mem_data.data.len() as i32);
                head.put_i64(resp.request_id);
                head.put_i32(resp.status_code);
                head.put_i32(msg_bytes.len() as i32);
                head.put(msg_bytes);
                head.put_i32(segments.len() as i32);
                for segment in segments {
                    head.put_i64(segment.block_id);
                    head.put_i32(segment.offset as i32);
                    head.put_i32(segment.length);
                    head.put_i32(segment.uncompress_length);
                    head.put_i64(segment.crc);
                    head.put_i64(segment.task_attempt_id);
                }
                Ok(Some(&mem_data.data))
            }
            Frame::GetMemoryDataV2Response(resp) => {
                let mem_data = match &resp.data {
                    Mem(mem_data) => mem_data,
                    _ => {
                        return Err(Error::msg(
                            "GetMemoryDataV2Response requires mem typed data",
                        ))
                    }
                };
                let msg_bytes = resp.ret_msg.as_bytes();
                let segments = &mem_data.shuffle_data_block_segments;
                let segments_encode_len = (4 + segments.len() * (3 * 8 + 3 * 4)) as i32;
                head.reserve(HEADER_LEN + 17 + msg_bytes.len() + segments_encode_len as usize);
                head.put_i32(msg_bytes.len() as i32 + 8 + 4 + 4 + segments_encode_len + 1);
                head.put_u8(MessageType::GetMemoryDataV2Response as u8);
                head.put_i32(mem_data.data.len() as i32);
                head.put_i64(resp.request_id);
                head.put_i32(resp.status_code);
                head.put_i32(msg_bytes.len() as i32);
                head.put(msg_bytes);
                head.put_i32(segments.len() as i32);
                for segment in segments {
                    head.put_i64(segment.block_id);
                    head.put_i32(segment.offset as i32);
                    head.put_i32(segment.length);
                    head.put_i32(segment.uncompress_length);
                    head.put_i64(segment.crc);
                    head.put_i64(segment.task_attempt_id);
                }
                head.put_u8(mem_data.is_end as u8);
                Ok(Some(&mem_data.data))
            }
            Frame::RpcResponse(resp) => {
                let msg_bytes = resp.ret_msg.as_bytes();
                head.reserve(HEADER_LEN + 16 + msg_bytes.len());
                head.put_i32(msg_bytes.len() as i32 + 8 + 4 + 4);
                head.put_u8(MessageType::RpcResponse as u8);
                head.put_i32(0);
                head.put_i64(resp.request_id);
                head.put_i32(resp.status_code);
                head.put_i32(msg_bytes.len() as i32);
                head.put(msg_bytes);
                Ok(None)
            }
            other => Err(Error::msg(format!("Cannot encode frame type: {}", other))),
        }
    }

    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), WorkerError> {
        if Buf::remaining(src) < HEADER_LEN {
            return Err(STREAM_INCOMPLETE);
        }

        let msg_len = get_len(src, "frame.content_len")?;
        let _msg_type = get_u8(src)?;
        let body_len = get_len(src, "frame.body_len")?;
        let payload_len = msg_len
            .checked_add(body_len)
            .ok_or_else(|| STREAM_INCORRECT("frame payload length overflow".into()))?;

        if src.remaining() < payload_len {
            return Err(STREAM_INCOMPLETE);
        }
        skip(src, payload_len)?;

        Ok(())
    }

    fn parse_to_get_localfile_data_v2_command(
        src: &mut impl Buf,
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
        src: &mut impl Buf,
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
        src: &mut impl Buf,
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

    fn parse_to_send_shuffle_data_command(src: &mut impl Buf) -> Result<SendDataRequestCommand> {
        let request_id = get_i64(src)?;
        let app_id = get_string(src)?;
        let shuffle_id = get_i32(src)?;
        let require_id = get_i64(src)?;

        let partition_batch_size = get_len(src, "send.partition_batch_size")?;
        let mut blocks_map: HashMap<i32, Vec<Block>> = HashMap::with_capacity(partition_batch_size);
        for _ in 0..partition_batch_size {
            let partition_id = get_i32(src)?;
            let block_batch_size = get_len(src, "send.block_batch_size")?;
            let mut blocks = Vec::with_capacity(block_batch_size);
            for _ in 0..block_batch_size {
                let _pid = get_i32(src)?;
                let block_id = get_i64(src)?;
                let length = get_i32(src)?;
                let _shuffle_id = get_i32(src)?;
                let crc = get_i64(src)?;
                let task_attempt_id = get_i64(src)?;
                let buffer = get_bytes(src)?.unwrap_or(Bytes::new());

                let shuffle_server_len = get_len(src, "send.block.shuffle_server_len")?;
                for _ in 0..shuffle_server_len {
                    skip_string(src)?;
                    skip_string(src)?;
                    skip(src, 8)?;
                }

                let uncompress_len = get_i32(src)?;
                let _free_mem = get_i64(src)?;

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
        Ok(SendDataRequestCommand {
            request_id,
            app_id,
            shuffle_id,
            blocks: blocks_map,
            ticket_id: require_id,
            timestamp,
        })
    }

    fn parse_to_get_localfile_index_command(
        src: &mut impl Buf,
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

    fn parse_to_get_memory_data_command(src: &mut impl Buf) -> Result<GetMemoryDataRequestCommand> {
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

    /// Parses a complete frame. Passing `Bytes` keeps payload extraction zero-copy.
    pub fn parse(mut src: impl Buf) -> Result<Frame, WorkerError> {
        let encode_msg_len = get_len(&mut src, "frame.content_len")?;
        let msg_type = get_u8(&mut src)?;
        let body_len = get_len(&mut src, "frame.body_len")?;
        let payload_len = encode_msg_len
            .checked_add(body_len)
            .ok_or_else(|| STREAM_INCORRECT("frame payload length overflow".into()))?;

        if src.remaining() < payload_len {
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
                let command = Frame::parse_to_get_localfile_data_command(&mut src)?;
                return Ok(Frame::GetLocalData(command));
            }
            MessageType::GetLocalDataV2 => {
                let command = Frame::parse_to_get_localfile_data_v2_command(&mut src)?;
                return Ok(Frame::GetLocalDataV2(command));
            }
            MessageType::GetLocalDataV3 => {
                let command = Frame::parse_to_get_localfile_data_v3_command(&mut src)?;
                return Ok(Frame::GetLocalDataV3(command));
            }
            MessageType::GetLocalDataIndex => {
                let command = Frame::parse_to_get_localfile_index_command(&mut src)?;
                return Ok(Frame::GetLocalDataIndex(command));
            }
            MessageType::GetMemoryData => {
                let command = Frame::parse_to_get_memory_data_command(&mut src)?;
                return Ok(Frame::GetMemoryData(command));
            }
            MessageType::SendShuffleData => {
                let command = Frame::parse_to_send_shuffle_data_command(&mut src)?;
                return Ok(Frame::SendShuffleData(command));
            }
            MessageType::RpcResponse => {
                let request_id = get_i64(&mut src)?;
                let status_code = get_i32(&mut src)?;
                let ret_msg = get_string(&mut src)?;
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

async fn write_data_bytes(
    stream: &mut TcpStream,
    data: &DataBytes,
    write_mode: UrpcWriteMode,
) -> Result<()> {
    match data {
        DataBytes::Direct(bytes) => stream.write_all(bytes).await?,
        DataBytes::Composed(composed) => match write_mode {
            UrpcWriteMode::VECTORED => write_composed_bytes(stream, composed.iter()).await?,
            UrpcWriteMode::FREEZE => stream.write_all(&composed.freeze()).await?,
            UrpcWriteMode::CHUNKED => {
                for chunk in composed.iter() {
                    stream.write_all(chunk).await?;
                }
            }
        },
        DataBytes::RawIO(raw) => {
            send_file_full(
                stream,
                raw.raw_fd,
                Some(raw.offset as i64),
                raw.length as usize,
            )
            .await
            .map_err(|e| {
                error!(
                    "Errors on getting localfile data by sendfile. off:{}. length:{}. e: {}",
                    raw.offset, raw.length, &e
                );
                e
            })?;
        }
        DataBytes::RawPipe(pipe) => {
            system_libc::splice(stream, pipe).await.map_err(|e| {
                error!(
                    "Errors on getting localfile data by splice from pipe. length:{}. e: {}",
                    pipe.length, &e
                );
                e
            })?;
        }
    }

    Ok(())
}

#[cfg(feature = "bench")]
#[doc(hidden)]
pub async fn write_composed_bytes_for_bench(
    stream: &mut TcpStream,
    chunks: &[Bytes],
) -> Result<()> {
    write_composed_bytes(stream, chunks.iter()).await
}

async fn write_composed_bytes<'a, I>(stream: &mut TcpStream, chunks: I) -> Result<()>
where
    I: Iterator<Item = &'a Bytes>,
{
    let chunks: Vec<&[u8]> = chunks
        .filter(|chunk| !chunk.is_empty())
        .map(|chunk| chunk.as_ref())
        .collect();

    if chunks.is_empty() {
        return Ok(());
    }

    if chunks.len() == 1 {
        stream.write_all(chunks[0]).await?;
        return Ok(());
    }

    let mut chunk_index = 0;
    let mut chunk_offset = 0;
    let mut slices = Vec::with_capacity(MAX_WRITE_VECTORS);

    while chunk_index < chunks.len() {
        slices.clear();
        for (index, chunk) in chunks[chunk_index..]
            .iter()
            .take(MAX_WRITE_VECTORS)
            .enumerate()
        {
            let chunk = if index == 0 {
                &chunk[chunk_offset..]
            } else {
                chunk
            };
            if !chunk.is_empty() {
                slices.push(IoSlice::new(chunk));
            }
        }

        if slices.is_empty() {
            return Err(Error::msg("composed data contained no writable bytes"));
        }

        let written = stream.write_vectored(&slices).await?;
        if written == 0 {
            return Err(Error::msg("socket write returned zero bytes"));
        }

        let mut remaining = written;
        while remaining > 0 {
            let available = chunks[chunk_index].len() - chunk_offset;
            if remaining < available {
                chunk_offset += remaining;
                remaining = 0;
            } else {
                remaining -= available;
                chunk_index += 1;
                chunk_offset = 0;
            }
        }
    }

    Ok(())
}

fn get_bytes(src: &mut impl Buf) -> Result<Option<Bytes>, WorkerError> {
    if !src.has_remaining() {
        return Err(STREAM_INCORRECT("get_bytes".into()));
    }
    let bytes_data_len = get_i32(src)?;
    if bytes_data_len <= 0 {
        return Ok(None);
    }

    if src.remaining() < bytes_data_len as usize {
        return Err(STREAM_INCORRECT(format!(
            "get_bytes but not have enough remaining bytes. expected: {}, real: {}",
            bytes_data_len,
            src.remaining()
        )));
    }

    Ok(Some(src.copy_to_bytes(bytes_data_len as usize)))
}

pub fn get_i64(src: &mut impl Buf) -> Result<i64, WorkerError> {
    if src.remaining() < 8 {
        return Err(STREAM_INCORRECT("get_i64".into()));
    }

    Ok(src.get_i64())
}

pub fn get_i32(src: &mut impl Buf) -> Result<i32, WorkerError> {
    if src.remaining() < 4 {
        return Err(STREAM_INCORRECT("get_i32".into()));
    }
    Ok(src.get_i32())
}

fn get_len(src: &mut impl Buf, field: &'static str) -> Result<usize, WorkerError> {
    let len = get_i32(src)?;
    if len < 0 {
        return Err(STREAM_INCORRECT(format!(
            "{field} should not be negative: {len}"
        )));
    }
    Ok(len as usize)
}

fn skip(src: &mut impl Buf, n: usize) -> Result<(), WorkerError> {
    if src.remaining() < n {
        return Err(STREAM_INCORRECT("skip".into()));
    }

    src.advance(n);
    Ok(())
}

fn skip_string(src: &mut impl Buf) -> Result<(), WorkerError> {
    if !src.has_remaining() {
        return Err(STREAM_INCORRECT("get_string 1".into()));
    }
    let len = get_i32(src)? as usize;
    if len <= 0 {
        return Ok(());
    }
    if src.remaining() < len {
        return Err(STREAM_INCORRECT(format!(
            "get string. src remaining: {}. len: {}",
            src.remaining(),
            len
        )));
    }
    skip(src, len)?;
    Ok(())
}

pub fn get_string(src: &mut impl Buf) -> Result<String, WorkerError> {
    if !src.has_remaining() {
        return Err(STREAM_INCORRECT("get_string 1".into()));
    }
    let len = get_i32(src)? as usize;
    if len <= 0 {
        return Ok("".into());
    }

    if src.remaining() < len {
        return Err(STREAM_INCORRECT(format!(
            "get string. src remaining: {}. len: {}",
            src.remaining(),
            len
        )));
    }

    let msg = if src.chunk().len() >= len {
        let msg = src.chunk()[..len].to_vec();
        src.advance(len);
        msg
    } else {
        let mut msg = vec![0; len];
        src.copy_to_slice(&mut msg);
        msg
    };

    Ok(String::from_utf8(msg)?)
}

pub fn get_u8(src: &mut impl Buf) -> Result<u8, WorkerError> {
    if !src.has_remaining() {
        return Err(STREAM_INCORRECT("get_u8".into()));
    }
    Ok(src.get_u8())
}

#[cfg(test)]
mod test {
    use crate::composed_bytes::ComposedBytes;
    use crate::config::UrpcWriteMode;
    use crate::error::WorkerError;
    use crate::store::DataBytes;
    use crate::urpc::command::GetLocalDataResponseCommand;
    use crate::urpc::frame::{
        get_string, write_composed_bytes, write_data_bytes, Frame, MessageType,
    };
    use anyhow::Result;
    use bytes::{Buf, BufMut, Bytes, BytesMut};
    use std::io::Cursor;
    use tokio::io::AsyncReadExt;
    use tokio::net::{TcpListener, TcpStream};

    ///
    /// The encode urpc:
    ///
    /// 1. encoded_length(i32, 4 bytes)
    /// 2. message_type(u8, 1 byte)
    /// 3. body_length (i32, 4 bytes)
    /// 4. content.
    ///

    #[test]
    fn get_string_reads_fragmented_buf() -> Result<()> {
        let mut src = Bytes::from_static(b"\0\0\0\x03a").chain(Bytes::from_static(b"bc"));
        assert_eq!("abc", get_string(&mut src)?);
        Ok(())
    }

    #[test]
    fn response_encoding_preserves_wire_layout() -> Result<()> {
        let chunks = vec![Bytes::from_static(b"first"), Bytes::from_static(b"second")];
        let frame = Frame::GetLocalDataResponse(GetLocalDataResponseCommand {
            request_id: 42,
            status_code: 7,
            ret_msg: "ok".into(),
            data: DataBytes::Composed(ComposedBytes::from(chunks.clone(), 11)),
        });

        let mut encoded = BytesMut::new();
        let data = frame.encode_head(&mut encoded)?.expect("response payload");
        match data {
            DataBytes::Direct(bytes) => encoded.extend_from_slice(bytes),
            DataBytes::Composed(composed) => {
                for chunk in composed.iter() {
                    encoded.extend_from_slice(chunk);
                }
            }
            _ => panic!("expected in-memory payload"),
        }

        let mut expected = BytesMut::new();
        expected.put_i32(18);
        expected.put_u8(MessageType::GetLocalDataResponse as u8);
        expected.put_i32(11);
        expected.put_i64(42);
        expected.put_i32(7);
        expected.put_i32(2);
        expected.put_slice(b"okfirstsecond");
        assert_eq!(expected, encoded);
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

    #[tokio::test]
    async fn composed_bytes_are_written_without_compaction() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let mut client = TcpStream::connect(address).await?;
        let (mut server, _) = listener.accept().await?;

        let chunks: Vec<Bytes> = (0..(super::MAX_WRITE_VECTORS + 1))
            .map(|index| Bytes::from(vec![index as u8; 3]))
            .collect();
        let expected: Vec<u8> = chunks
            .iter()
            .flat_map(|chunk| chunk.iter().copied())
            .collect();
        let composed = ComposedBytes::from(chunks, expected.len());

        write_composed_bytes(&mut server, composed.iter()).await?;

        let mut actual = vec![0; expected.len()];
        client.read_exact(&mut actual).await?;
        assert_eq!(expected, actual);
        Ok(())
    }

    #[tokio::test]
    async fn composed_data_write_modes_preserve_payload() -> Result<()> {
        let chunks = vec![Bytes::from_static(b"first"), Bytes::from_static(b"second")];
        let expected = b"firstsecond";

        for write_mode in [
            UrpcWriteMode::VECTORED,
            UrpcWriteMode::FREEZE,
            UrpcWriteMode::CHUNKED,
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?;
            let mut client = TcpStream::connect(address).await?;
            let (mut server, _) = listener.accept().await?;
            let data = DataBytes::Composed(ComposedBytes::from(chunks.clone(), expected.len()));

            write_data_bytes(&mut server, &data, write_mode).await?;

            let mut actual = vec![0; expected.len()];
            client.read_exact(&mut actual).await?;
            assert_eq!(expected, actual.as_slice());
        }

        Ok(())
    }
}
