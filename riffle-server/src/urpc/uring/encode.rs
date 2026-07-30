//! Wire-level encode/decode helpers for the io_uring based urpc net engine.
//!
//! The encoded layout MUST stay byte-identical with [`Frame::write_with_mode`]
//! (see `urpc/frame.rs`), so that clients cannot distinguish which net engine
//! serves them.

use crate::error::WorkerError;
use crate::store::{Block, DataBytes, ResponseData};
use crate::urpc::command::SendDataRequestCommand;
use crate::urpc::frame::{get_i32, get_i64, get_string, get_u8, Frame, MessageType};
use anyhow::{anyhow, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::HashMap;
use std::io::Cursor;

/// content_length(i32) + message_type(u8) + body_length(i32)
pub const FRAME_HEADER_LEN: usize = 4 + 1 + 4;

/// Parsed frame header of an inbound request.
#[derive(Debug, Clone, Copy)]
pub struct RequestHeader {
    pub content_len: usize,
    pub body_len: usize,
}

impl RequestHeader {
    pub fn total_len(&self) -> usize {
        FRAME_HEADER_LEN + self.content_len + self.body_len
    }
}

/// Peeks the frame header from the buffered bytes without consuming them.
/// Returns `None` if fewer than [`FRAME_HEADER_LEN`] bytes are available.
pub fn peek_request_header(buf: &[u8]) -> Result<Option<RequestHeader>, WorkerError> {
    if buf.len() < FRAME_HEADER_LEN {
        return Ok(None);
    }
    let mut cursor = Cursor::new(buf);
    let content_len = get_i32(&mut cursor)?;
    let _message_type = get_u8(&mut cursor)?;
    let body_len = get_i32(&mut cursor)?;
    if content_len < 0 || body_len < 0 {
        return Err(WorkerError::STREAM_INCORRECT(format!(
            "negative frame length. content_len: {}, body_len: {}",
            content_len, body_len
        )));
    }
    Ok(Some(RequestHeader {
        content_len: content_len as usize,
        body_len: body_len as usize,
    }))
}

/// Parses one complete request frame. `frame_bytes` must contain exactly one
/// frame (header included), as sliced out by the engine according to
/// [`peek_request_header`].
///
/// For `SendShuffleData` the block payloads are zero-copy slices of
/// `frame_bytes` instead of the copying path in [`Frame::parse`].
pub fn parse_request_frame(frame_bytes: Bytes) -> Result<Frame, WorkerError> {
    let mut cursor = Cursor::new(&frame_bytes[..]);
    let _content_len = get_i32(&mut cursor)?;
    let message_type = get_u8(&mut cursor)?;
    let _body_len = get_i32(&mut cursor)?;

    if message_type == MessageType::SendShuffleData as u8 {
        return parse_send_shuffle_data_zero_copy(&frame_bytes, &mut cursor);
    }

    cursor.set_position(0);
    Frame::parse(&mut cursor)
}

fn ensure_remaining(
    cursor: &Cursor<&[u8]>,
    len: usize,
    field: &'static str,
) -> Result<(), WorkerError> {
    if cursor.remaining() < len {
        return Err(WorkerError::STREAM_INCORRECT(format!(
            "{field} requires {len} bytes, but only {} remaining",
            cursor.remaining()
        )));
    }
    Ok(())
}

fn read_len(cursor: &mut Cursor<&[u8]>, field: &'static str) -> Result<usize, WorkerError> {
    let len = get_i32(cursor)?;
    if len < 0 {
        return Err(WorkerError::STREAM_INCORRECT(format!(
            "{field} should not be negative: {len}"
        )));
    }
    Ok(len as usize)
}

fn skip_string(cursor: &mut Cursor<&[u8]>, field: &'static str) -> Result<(), WorkerError> {
    let len = read_len(cursor, field)?;
    ensure_remaining(cursor, len, field)?;
    cursor.advance(len);
    Ok(())
}

fn parse_send_shuffle_data_zero_copy(
    frame_bytes: &Bytes,
    cursor: &mut Cursor<&[u8]>,
) -> Result<Frame, WorkerError> {
    let request_id = get_i64(cursor)?;
    let app_id = get_string(cursor)?;
    let shuffle_id = get_i32(cursor)?;
    let ticket_id = get_i64(cursor)?;

    let partition_batch_size = read_len(cursor, "send.partition_batch_size")?;
    let mut blocks_map: HashMap<i32, Vec<Block>> = HashMap::with_capacity(partition_batch_size);
    for _ in 0..partition_batch_size {
        let partition_id = get_i32(cursor)?;
        let block_batch_size = read_len(cursor, "send.block_batch_size")?;
        let mut blocks = Vec::with_capacity(block_batch_size);
        for _ in 0..block_batch_size {
            let _pid = get_i32(cursor)?;
            let block_id = get_i64(cursor)?;
            let length = get_i32(cursor)?;
            let _shuffle_id = get_i32(cursor)?;
            let crc = get_i64(cursor)?;
            let task_attempt_id = get_i64(cursor)?;

            let data_len = get_i32(cursor)?;
            let data = if data_len <= 0 {
                Bytes::new()
            } else {
                let data_len = data_len as usize;
                ensure_remaining(cursor, data_len, "send.block.data")?;
                let pos = cursor.position() as usize;
                // Zero-copy: the block payload shares the receive buffer allocation.
                let data = frame_bytes.slice(pos..pos + data_len);
                cursor.advance(data_len);
                data
            };

            let shuffle_server_len = read_len(cursor, "send.block.shuffle_server_len")?;
            for _ in 0..shuffle_server_len {
                skip_string(cursor, "send.block.shuffle_server.id")?;
                skip_string(cursor, "send.block.shuffle_server.host")?;
                ensure_remaining(cursor, 8, "send.block.shuffle_server.ports")?;
                cursor.advance(8);
            }

            let uncompress_length = get_i32(cursor)?;
            let _free_mem = get_i64(cursor)?;

            blocks.push(Block {
                block_id,
                length,
                uncompress_length,
                crc,
                data,
                task_attempt_id,
            });
        }
        blocks_map.insert(partition_id, blocks);
    }
    let timestamp = get_i64(cursor)?;

    Ok(Frame::SendShuffleData(SendDataRequestCommand::new(
        request_id, app_id, shuffle_id, blocks_map, ticket_id, timestamp,
    )))
}

/// Appends the data payload of a response as separate zero-copy chunks.
fn push_data_chunks(data: &DataBytes, chunks: &mut Vec<Bytes>) -> Result<()> {
    match data {
        DataBytes::Direct(bytes) => {
            if !bytes.is_empty() {
                chunks.push(bytes.clone());
            }
        }
        DataBytes::Composed(composed) => {
            for chunk in composed.iter() {
                if !chunk.is_empty() {
                    chunks.push(chunk.clone());
                }
            }
        }
        DataBytes::RawIO(_) | DataBytes::RawPipe(_) => {
            return Err(anyhow!(
                "The io_uring net engine requires in-memory response data, \
                 but got a raw fd/pipe based payload. Materialize it first."
            ));
        }
    }
    Ok(())
}

/// Encodes a response frame: the fixed meta part is appended into `head`,
/// while large data payloads are pushed into `chunks` (in wire order, to be
/// written right after `head`). The byte layout replicates
/// [`Frame::write_with_mode`].
pub fn encode_frame_into(
    frame: &Frame,
    head: &mut BytesMut,
    chunks: &mut Vec<Bytes>,
) -> Result<()> {
    match frame {
        Frame::RpcResponse(resp) => {
            let msg_bytes = resp.ret_msg.as_bytes();
            head.reserve(FRAME_HEADER_LEN + 16 + msg_bytes.len());
            head.put_i32(msg_bytes.len() as i32 + 8 + 4 + 4);
            head.put_u8(MessageType::RpcResponse as u8);
            head.put_i32(0);
            head.put_i64(resp.request_id);
            head.put_i32(resp.status_code);
            head.put_i32(msg_bytes.len() as i32);
            head.put(msg_bytes);
        }
        Frame::GetLocalDataResponse(resp) => {
            let msg_bytes = resp.ret_msg.as_bytes();
            let data = &resp.data;
            head.reserve(FRAME_HEADER_LEN + 16 + msg_bytes.len());
            head.put_i32(msg_bytes.len() as i32 + 8 + 4 + 4);
            head.put_u8(MessageType::GetLocalDataResponse as u8);
            head.put_i32(data.len() as i32);
            head.put_i64(resp.request_id);
            head.put_i32(resp.status_code);
            head.put_i32(msg_bytes.len() as i32);
            head.put(msg_bytes);
            push_data_chunks(data, chunks)?;
        }
        Frame::GetLocalDataIndexResponse(resp) => {
            let msg_bytes = resp.ret_msg.as_bytes();
            let index_bytes = &resp.data_index.index_data;
            head.reserve(FRAME_HEADER_LEN + 24 + msg_bytes.len());
            head.put_i32(msg_bytes.len() as i32 + 8 + 4 + 4 + 8);
            head.put_u8(MessageType::GetLocalDataIndexResponse as u8);
            head.put_i32(index_bytes.len() as i32);
            head.put_i64(resp.request_id);
            head.put_i32(resp.status_code);
            head.put_i32(msg_bytes.len() as i32);
            head.put(msg_bytes);
            head.put_i64(resp.data_index.data_file_len);
            push_data_chunks(index_bytes, chunks)?;
        }
        Frame::GetLocalDataIndexV2Response(resp) => {
            let msg_bytes = resp.ret_msg.as_bytes();
            let index_bytes = &resp.data_index.index_data;
            head.reserve(FRAME_HEADER_LEN + 28 + msg_bytes.len() + 4 * resp.storage_ids.len());
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
            push_data_chunks(index_bytes, chunks)?;
        }
        Frame::GetMemoryDataResponse(resp) => {
            let mem_data = match &resp.data {
                ResponseData::Mem(mem_data) => mem_data,
                _ => return Err(anyhow!("GetMemoryDataResponse requires mem typed data")),
            };
            let msg_bytes = resp.ret_msg.as_bytes();
            let segments = &mem_data.shuffle_data_block_segments;
            let segments_encode_len = (4 + segments.len() * (3 * 8 + 3 * 4)) as i32;
            head.reserve(FRAME_HEADER_LEN + 16 + msg_bytes.len() + segments_encode_len as usize);
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
            push_data_chunks(&mem_data.data, chunks)?;
        }
        Frame::GetMemoryDataV2Response(resp) => {
            let mem_data = match &resp.data {
                ResponseData::Mem(mem_data) => mem_data,
                _ => return Err(anyhow!("GetMemoryDataV2Response requires mem typed data")),
            };
            let msg_bytes = resp.ret_msg.as_bytes();
            let segments = &mem_data.shuffle_data_block_segments;
            let segments_encode_len = (4 + segments.len() * (3 * 8 + 3 * 4)) as i32;
            head.reserve(FRAME_HEADER_LEN + 17 + msg_bytes.len() + segments_encode_len as usize);
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
            push_data_chunks(&mem_data.data, chunks)?;
        }
        other => {
            return Err(anyhow!(
                "The io_uring net engine cannot encode frame type: {}",
                other
            ));
        }
    }
    Ok(())
}
