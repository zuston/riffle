use bytes::{Buf, Bytes, BytesMut};
use std::io::Cursor;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::error::WorkerError;
use crate::error::WorkerError::STREAM_INCORRECT;
use crate::metric::{URPC_CONNECTION_BUFFER_CAPACITY_BYTES, URPC_REQUEST_PARSING_LATENCY};
use crate::store::Block;
use crate::urpc::command::SendDataRequestCommand;
use crate::urpc::frame::Frame;
use crate::urpc::frame::MessageType;
use anyhow::Result;
use await_tree::InstrumentAwait;
use num_enum::TryFromPrimitive;
use std::collections::HashMap;

const HEADER_LEN: usize = 4 + 1 + 4;
const INITIAL_READ_BUFFER_LENGTH: usize = 32 * 1024;
const INITIAL_WRITE_BUFFER_LENGTH: usize = 8 * 1024;
const READ_BUFFER_SHRINK_THRESHOLD: usize = 256 * 1024;
const WRITE_BUFFER_SHRINK_THRESHOLD: usize = 64 * 1024;

#[derive(Debug)]
struct FrameHeader {
    content_len: usize,
    body_len: usize,
    message_type: MessageType,
}

impl FrameHeader {
    fn payload_len(self) -> usize {
        self.content_len + self.body_len
    }
}

#[derive(Debug)]
pub struct Connection {
    stream: TcpStream,
    read_buf: BytesMut,
    write_buf: BytesMut,
    streaming_parse_enabled: bool,
}

impl Connection {
    pub fn new(socket: TcpStream, streaming_parse_enabled: bool) -> Self {
        Connection {
            stream: socket,
            read_buf: BytesMut::with_capacity(INITIAL_READ_BUFFER_LENGTH),
            write_buf: BytesMut::with_capacity(INITIAL_WRITE_BUFFER_LENGTH),
            streaming_parse_enabled,
        }
    }

    fn maybe_reset_read_buffer(&mut self) {
        if self.read_buf.is_empty() && self.read_buf.capacity() > READ_BUFFER_SHRINK_THRESHOLD {
            self.read_buf = BytesMut::with_capacity(INITIAL_READ_BUFFER_LENGTH);
        }
    }

    fn maybe_reset_write_buffer(&mut self) {
        if self.write_buf.is_empty() && self.write_buf.capacity() > WRITE_BUFFER_SHRINK_THRESHOLD {
            self.write_buf = BytesMut::with_capacity(INITIAL_WRITE_BUFFER_LENGTH);
        }
    }

    fn observe_read_buffer_capacity(&self) {
        URPC_CONNECTION_BUFFER_CAPACITY_BYTES
            .with_label_values(&["read"])
            .observe(self.read_buf.capacity() as f64);
    }

    fn observe_write_buffer_capacity(&self) {
        URPC_CONNECTION_BUFFER_CAPACITY_BYTES
            .with_label_values(&["write"])
            .observe(self.write_buf.capacity() as f64);
    }

    fn parse_frame_header(&self) -> Result<Option<FrameHeader>, WorkerError> {
        if self.read_buf.len() < HEADER_LEN {
            return Ok(None);
        }

        let mut buf = Cursor::new(&self.read_buf[..]);
        let content_len = read_usize_i32(&mut buf, "frame.content_len")?;
        let message_type = MessageType::try_from(read_u8(&mut buf)?)?;
        let body_len = read_usize_i32(&mut buf, "frame.body_len")?;

        Ok(Some(FrameHeader {
            content_len,
            body_len,
            message_type,
        }))
    }

    async fn read_more_into_buffer(&mut self) -> Result<usize, WorkerError> {
        let n = self.stream.read_buf(&mut self.read_buf).await?;
        if n == 0 {
            if self.read_buf.is_empty() {
                return Ok(0);
            }
            return Err(WorkerError::STREAM_ABNORMAL);
        }
        Ok(n)
    }

    async fn ensure_buffered(&mut self, expected_len: usize) -> Result<(), WorkerError> {
        if expected_len > self.read_buf.capacity() {
            self.read_buf.reserve(expected_len - self.read_buf.len());
        }

        while self.read_buf.len() < expected_len {
            self.read_more_into_buffer().await?;
        }
        Ok(())
    }

    fn consume_bytes(&mut self, len: usize, field: &'static str) -> Result<Bytes, WorkerError> {
        if self.read_buf.len() < len {
            return Err(STREAM_INCORRECT(format!(
                "{field} requires {len} bytes, but only {} buffered",
                self.read_buf.len()
            )));
        }
        Ok(self.read_buf.split_to(len).freeze())
    }

    fn consume_u8(&mut self, field: &'static str) -> Result<u8, WorkerError> {
        Ok(self.consume_bytes(1, field)?[0])
    }

    fn consume_i32(&mut self, field: &'static str) -> Result<i32, WorkerError> {
        let bytes = self.consume_bytes(4, field)?;
        let mut buf = Cursor::new(&bytes[..]);
        Ok(buf.get_i32())
    }

    fn consume_i64(&mut self, field: &'static str) -> Result<i64, WorkerError> {
        let bytes = self.consume_bytes(8, field)?;
        let mut buf = Cursor::new(&bytes[..]);
        Ok(buf.get_i64())
    }

    fn consume_usize_i32(&mut self, field: &'static str) -> Result<usize, WorkerError> {
        let len = self.consume_i32(field)?;
        to_usize_len(len, field)
    }

    fn consume_string(&mut self, len: usize, field: &'static str) -> Result<String, WorkerError> {
        let bytes = self.consume_bytes(len, field)?;
        Ok(String::from_utf8(bytes.to_vec())?)
    }

    fn skip_bytes(&mut self, len: usize, field: &'static str) -> Result<(), WorkerError> {
        let _ = self.consume_bytes(len, field)?;
        Ok(())
    }

    async fn read_send_shuffle_data_frame(
        &mut self,
        header: FrameHeader,
    ) -> Result<Frame, WorkerError> {
        self.ensure_buffered(HEADER_LEN).await?;
        self.read_buf.advance(HEADER_LEN);

        let mut reader = StreamingFrameReader::new(self, header.payload_len());
        let request_id = reader.read_i64("send.request_id").await?;
        let app_id = reader.read_string("send.app_id").await?;
        let shuffle_id = reader.read_i32("send.shuffle_id").await?;
        let ticket_id = reader.read_i64("send.ticket_id").await?;

        let partition_batch_size = reader.read_len("send.partition_batch_size").await?;
        let mut blocks_map = HashMap::with_capacity(partition_batch_size);
        for _ in 0..partition_batch_size {
            let partition_id = reader.read_i32("send.partition_id").await?;
            let block_batch_size = reader.read_len("send.block_batch_size").await?;
            let mut blocks = Vec::with_capacity(block_batch_size);

            for _ in 0..block_batch_size {
                let _pid = reader.read_i32("send.block.pid").await?;
                let block_id = reader.read_i64("send.block.block_id").await?;
                let length = reader.read_i32("send.block.length").await?;
                let _shuffle_id = reader.read_i32("send.block.shuffle_id").await?;
                let crc = reader.read_i64("send.block.crc").await?;
                let task_attempt_id = reader.read_i64("send.block.task_attempt_id").await?;
                let data = reader.read_len_prefixed_bytes("send.block.data").await?;

                let shuffle_server_len = reader.read_len("send.block.shuffle_server_len").await?;
                for _ in 0..shuffle_server_len {
                    reader.skip_string("send.block.shuffle_server.host").await?;
                    reader.skip_string("send.block.shuffle_server.id").await?;
                    let _ = reader
                        .read_i32("send.block.shuffle_server.grpc_port")
                        .await?;
                    let _ = reader
                        .read_i32("send.block.shuffle_server.netty_port")
                        .await?;
                }

                let uncompress_length = reader.read_i32("send.block.uncompress_length").await?;
                let _free_mem = reader.read_i64("send.block.free_mem").await?;

                blocks.push(Block {
                    block_id,
                    length,
                    uncompress_length,
                    crc,
                    data: data.unwrap_or_else(Bytes::new),
                    task_attempt_id,
                });
            }

            blocks_map.insert(partition_id, blocks);
        }

        let timestamp = reader.read_i64("send.timestamp").await?;
        reader.finish()?;
        self.maybe_reset_read_buffer();

        Ok(Frame::SendShuffleData(SendDataRequestCommand {
            request_id,
            app_id,
            shuffle_id,
            blocks: blocks_map,
            ticket_id,
            timestamp,
        }))
    }

    fn parse_frame(&mut self) -> Result<Option<Frame>> {
        let mut buf = Cursor::new(&self.read_buf[..]);

        match Frame::check(&mut buf) {
            Ok(_) => {
                let timer = std::time::Instant::now();
                let len = buf.position() as usize;
                buf.set_position(0);
                let frame = Frame::parse(&mut buf)?;
                self.read_buf.advance(len);
                self.maybe_reset_read_buffer();
                URPC_REQUEST_PARSING_LATENCY
                    .with_label_values(&[&format!("{}", &frame)])
                    .observe(timer.elapsed().as_secs_f64());
                Ok(Some(frame))
            }
            Err(WorkerError::STREAM_INCOMPLETE) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        Frame::write(&mut self.stream, frame, &mut self.write_buf)
            .instrument_await("writing frame...")
            .await?;
        self.stream
            .flush()
            .instrument_await("flushing frame...")
            .await?;
        self.maybe_reset_write_buffer();
        self.observe_write_buffer_capacity();
        Ok(())
    }

    pub async fn read_frame(&mut self) -> Result<Option<Frame>, WorkerError> {
        loop {
            if self.streaming_parse_enabled {
                if let Some(header) = self.parse_frame_header()? {
                    if header.message_type == MessageType::SendShuffleData {
                        let timer = Instant::now();
                        let frame = self.read_send_shuffle_data_frame(header).await?;
                        URPC_REQUEST_PARSING_LATENCY
                            .with_label_values(&[&format!("{}", &frame)])
                            .observe(timer.elapsed().as_secs_f64());
                        self.observe_read_buffer_capacity();
                        return Ok(Some(frame));
                    }
                }
            }

            // Attempt to parse a frame from the buffered data. If enough data
            // has been buffered, the frame is returned.
            if let Some(frame) = self.parse_frame()? {
                self.observe_read_buffer_capacity();
                return Ok(Some(frame));
            }

            // There is not enough buffered data to read a frame. Attempt to
            // read more data from the socket.
            //
            // On success, the number of bytes is returned. `0` indicates "end
            // of stream".
            if 0 == self.read_more_into_buffer().await? {
                // The remote closed the connection. For this to be a clean
                // shutdown, there should be no data in the read buffer. If
                // there is, this means that the peer closed the socket while
                // sending a frame.
                if self.read_buf.is_empty() {
                    self.observe_read_buffer_capacity();
                    return Ok(None);
                } else {
                    return Err(WorkerError::STREAM_ABNORMAL);
                }
            }
        }
    }
}

struct StreamingFrameReader<'a> {
    conn: &'a mut Connection,
    remaining: usize,
}

impl<'a> StreamingFrameReader<'a> {
    fn new(conn: &'a mut Connection, remaining: usize) -> Self {
        Self { conn, remaining }
    }

    async fn ensure(&mut self, len: usize, field: &'static str) -> Result<(), WorkerError> {
        if self.remaining < len {
            return Err(STREAM_INCORRECT(format!(
                "{field} exceeds frame payload, remaining={}, need={len}",
                self.remaining
            )));
        }

        self.conn.ensure_buffered(len).await?;
        Ok(())
    }

    async fn read_i32(&mut self, field: &'static str) -> Result<i32, WorkerError> {
        self.ensure(4, field).await?;
        self.remaining -= 4;
        self.conn.consume_i32(field)
    }

    async fn read_i64(&mut self, field: &'static str) -> Result<i64, WorkerError> {
        self.ensure(8, field).await?;
        self.remaining -= 8;
        self.conn.consume_i64(field)
    }

    async fn read_len(&mut self, field: &'static str) -> Result<usize, WorkerError> {
        let len = self.read_i32(field).await?;
        to_usize_len(len, field)
    }

    async fn read_string(&mut self, field: &'static str) -> Result<String, WorkerError> {
        let len = self.read_len(field).await?;
        self.ensure(len, field).await?;
        self.remaining -= len;
        self.conn.consume_string(len, field)
    }

    async fn skip_string(&mut self, field: &'static str) -> Result<(), WorkerError> {
        let len = self.read_len(field).await?;
        self.ensure(len, field).await?;
        self.remaining -= len;
        self.conn.skip_bytes(len, field)
    }

    async fn read_len_prefixed_bytes(
        &mut self,
        field: &'static str,
    ) -> Result<Option<Bytes>, WorkerError> {
        let len = self.read_i32(field).await?;
        if len <= 0 {
            return Ok(None);
        }

        let len = to_usize_len(len, field)?;
        self.ensure(len, field).await?;
        self.remaining -= len;
        Ok(Some(self.conn.consume_bytes(len, field)?))
    }

    fn finish(self) -> Result<(), WorkerError> {
        if self.remaining == 0 {
            return Ok(());
        }

        Err(STREAM_INCORRECT(format!(
            "streaming frame parser left {} bytes unread",
            self.remaining
        )))
    }
}

fn to_usize_len(value: i32, field: &'static str) -> Result<usize, WorkerError> {
    if value < 0 {
        return Err(STREAM_INCORRECT(format!(
            "{field} should not be negative: {value}"
        )));
    }
    Ok(value as usize)
}

fn read_usize_i32(src: &mut Cursor<&[u8]>, field: &'static str) -> Result<usize, WorkerError> {
    let len = read_i32(src)?;
    to_usize_len(len, field)
}

fn read_i32(src: &mut Cursor<&[u8]>) -> Result<i32, WorkerError> {
    if src.remaining() < 4 {
        return Err(WorkerError::STREAM_INCOMPLETE);
    }
    Ok(src.get_i32())
}

fn read_u8(src: &mut Cursor<&[u8]>) -> Result<u8, WorkerError> {
    if src.remaining() < 1 {
        return Err(WorkerError::STREAM_INCOMPLETE);
    }
    Ok(src.get_u8())
}

#[cfg(test)]
mod tests {
    use super::{Connection, HEADER_LEN, INITIAL_READ_BUFFER_LENGTH, READ_BUFFER_SHRINK_THRESHOLD};
    use crate::metric::REGISTRY;
    use crate::urpc::frame::{Frame, MessageType};
    use anyhow::Result;
    use bytes::{BufMut, BytesMut};
    use prometheus::{Encoder, TextEncoder};
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};

    fn put_string(buf: &mut BytesMut, value: &str) {
        buf.put_i32(value.len() as i32);
        buf.put_slice(value.as_bytes());
    }

    fn build_send_shuffle_data_frame(payload: &[u8]) -> BytesMut {
        let mut body = BytesMut::new();
        body.put_i64(42);
        put_string(&mut body, "app-streaming");
        body.put_i32(7);
        body.put_i64(99);
        body.put_i32(1);
        body.put_i32(11);
        body.put_i32(1);
        body.put_i32(11);
        body.put_i64(1234);
        body.put_i32(payload.len() as i32);
        body.put_i32(7);
        body.put_i64(88);
        body.put_i64(9001);
        body.put_i32(payload.len() as i32);
        body.put_slice(payload);
        body.put_i32(0);
        body.put_i32(payload.len() as i32);
        body.put_i64(0);
        body.put_i64(123456);

        let mut frame = BytesMut::with_capacity(HEADER_LEN + body.len());
        frame.put_i32(0);
        frame.put_u8(MessageType::SendShuffleData as u8);
        frame.put_i32(body.len() as i32);
        frame.extend_from_slice(&body);
        frame
    }

    fn build_get_local_data_frame() -> BytesMut {
        let mut body = BytesMut::new();
        body.put_i64(77);
        put_string(&mut body, "app-small");
        body.put_i32(3);
        body.put_i32(4);
        body.put_i32(5);
        body.put_i32(6);
        body.put_i64(7);
        body.put_i32(8);
        body.put_i64(9);

        let mut frame = BytesMut::with_capacity(HEADER_LEN + body.len());
        frame.put_i32(0);
        frame.put_u8(MessageType::GetLocalData as u8);
        frame.put_i32(body.len() as i32);
        frame.extend_from_slice(&body);
        frame
    }

    async fn connected_streams() -> Result<(TcpStream, TcpStream)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let client = TcpStream::connect(addr).await?;
        let (server, _) = listener.accept().await?;
        Ok((client, server))
    }

    #[tokio::test]
    async fn send_shuffle_data_uses_streaming_parse_and_preserves_next_frame() -> Result<()> {
        let payload = vec![7u8; READ_BUFFER_SHRINK_THRESHOLD + 16 * 1024];
        let send_frame = build_send_shuffle_data_frame(&payload);
        let local_frame = build_get_local_data_frame();

        let (mut client, server) = connected_streams().await?;
        client.write_all(&send_frame).await?;
        client.write_all(&local_frame).await?;

        let mut conn = Connection::new(server, true);

        let frame = conn.read_frame().await?.expect("send frame");
        match frame {
            Frame::SendShuffleData(req) => {
                assert_eq!(42, req.request_id);
                assert_eq!("app-streaming", req.app_id);
                assert_eq!(1, req.blocks.len());
                let blocks = req.blocks.get(&11).expect("partition");
                assert_eq!(1, blocks.len());
                assert_eq!(payload.len(), blocks[0].data.len());
                assert_eq!(payload.as_slice(), blocks[0].data.as_ref());
            }
            other => panic!("unexpected frame: {other:?}"),
        }

        let frame = conn.read_frame().await?.expect("next frame");
        match frame {
            Frame::GetLocalData(req) => {
                assert_eq!(77, req.request_id);
                assert_eq!("app-small", req.app_id);
            }
            other => panic!("unexpected frame: {other:?}"),
        }

        assert!(conn.read_buf.capacity() <= INITIAL_READ_BUFFER_LENGTH);
        Ok(())
    }

    #[tokio::test]
    async fn send_shuffle_data_streaming_parse_can_be_disabled() -> Result<()> {
        let payload = vec![3u8; 8 * 1024];
        let send_frame = build_send_shuffle_data_frame(&payload);

        let (mut client, server) = connected_streams().await?;
        client.write_all(&send_frame).await?;

        let mut conn = Connection::new(server, false);
        let frame = conn.read_frame().await?.expect("send frame");

        match frame {
            Frame::SendShuffleData(req) => {
                let blocks = req.blocks.get(&11).expect("partition");
                assert_eq!(payload.as_slice(), blocks[0].data.as_ref());
            }
            other => panic!("unexpected frame: {other:?}"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn buffer_capacity_histogram_registered_after_read_frame() -> Result<()> {
        let payload = vec![3u8; 8 * 1024];
        let send_frame = build_send_shuffle_data_frame(&payload);
        let (mut client, server) = connected_streams().await?;
        client.write_all(&send_frame).await?;
        drop(client);

        let mut conn = Connection::new(server, false);
        let _ = conn.read_frame().await?;

        let metric_families = REGISTRY.gather();
        let mut encoded = Vec::new();
        TextEncoder::new()
            .encode(&metric_families, &mut encoded)
            .expect("encode metrics");
        let text = String::from_utf8(encoded).expect("utf8 metrics");
        assert!(text.contains("urpc_connection_buffer_capacity_bytes"));
        assert!(text.contains("buffer=\"read\""));
        Ok(())
    }
}
