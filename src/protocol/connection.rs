use bytes::{Buf, BytesMut};
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

use crate::error::WorkerError;
use crate::protocol::frame::Frame;
use anyhow::Result;

#[derive(Debug)]
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(socket: TcpStream) -> Self {
        Connection {
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(1024 * 1024),
        }
    }

    fn parse_frame(&mut self) -> Result<Option<Frame>> {
        let mut buf = Cursor::new(&self.buffer[..]);

        match Frame::check(&mut buf) {
            Ok(_) => {
                let len = buf.position() as usize;
                buf.set_position(0);
                let frame = Frame::parse(&mut buf)?;
                self.buffer.advance(len);
                Ok(Some(frame))
            }
            Err(WorkerError::STREAM_INCOMPLETE) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        Frame::write(&mut self.stream, frame).await?;
        self.stream.flush().await?;
        Ok(())
    }

    pub async fn read_frame(&mut self) -> Result<Option<Frame>, WorkerError> {
        loop {
            // Attempt to parse a frame from the buffered data. If enough data
            // has been buffered, the frame is returned.
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // There is not enough buffered data to read a frame. Attempt to
            // read more data from the socket.
            //
            // On success, the number of bytes is returned. `0` indicates "end
            // of stream".
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // The remote closed the connection. For this to be a clean
                // shutdown, there should be no data in the read buffer. If
                // there is, this means that the peer closed the socket while
                // sending a frame.
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err(WorkerError::STREAM_ABNORMAL);
                }
            }
        }
    }
}
