use bytes::{Buf, BytesMut};
use std::io::Cursor;

use crate::error::WorkerError;
use crate::metric::URPC_REQUEST_PARSING_LATENCY;
use crate::urpc::frame::Frame;
use crate::urpc::transport::TransportStream;
use anyhow::Result;

const INITIAL_BUFFER_LENGTH: usize = 1024 * 1024;

#[derive(Debug)]
pub struct Connection<S: TransportStream> {
    stream: S,
    read_buf: BytesMut,
    write_buf: BytesMut,
}

impl<S: TransportStream> Connection<S> {
    pub fn new(stream: S) -> Self {
        Connection {
            stream,
            read_buf: BytesMut::with_capacity(INITIAL_BUFFER_LENGTH),
            write_buf: BytesMut::with_capacity(INITIAL_BUFFER_LENGTH),
        }
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
        Frame::write(&mut self.stream, frame, &mut self.write_buf).await?;
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
            if 0 == self.stream.read_buf(&mut self.read_buf).await? {
                // The remote closed the connection. For this to be a clean
                // shutdown, there should be no data in the read buffer. If
                // there is, this means that the peer closed the socket while
                // sending a frame.
                if self.read_buf.is_empty() {
                    return Ok(None);
                } else {
                    return Err(WorkerError::STREAM_ABNORMAL);
                }
            }
        }
    }

    /// Get a reference to the underlying stream
    pub fn stream_ref(&self) -> &S {
        &self.stream
    }

    /// Get a mutable reference to the underlying stream
    pub fn stream_mut(&mut self) -> &mut S {
        &mut self.stream
    }
}
