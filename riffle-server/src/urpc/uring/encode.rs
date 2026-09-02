//! Wire-level encode/decode helpers for the io_uring based urpc net engine.
//!
//! The encoded layout MUST stay byte-identical with [`Frame::write_with_mode`]
//! (see `urpc/frame.rs`), so that clients cannot distinguish which net engine
//! serves them.

use crate::error::WorkerError;
use crate::store::{DataBytes, ResponseData};
use crate::urpc::frame::{get_i32, get_u8, Frame, MessageType};
use anyhow::{anyhow, Result};
use bytes::{BufMut, Bytes, BytesMut};
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
