//! Bridge between the io_uring net engine and the existing urpc command
//! processing logic.
//!
//! The engine thread only parses frames; the actual command execution
//! (memory/localfile store access) is spawned onto a tokio runtime and the
//! response is pushed back to the engine through a [`RemoteResponder`].

use crate::app_manager::AppManagerRef;
use crate::runtime::RuntimeRef;
use crate::store::DataBytes;
use crate::urpc::command::Command;
use crate::urpc::frame::Frame;
use crate::urpc::metrics::RequestMetricTracker;
use crate::urpc::uring::engine::{FrameHandler, Responder};
use anyhow::Result;
use bytes::Bytes;
use log::error;

pub struct AppCommandBridgeHandler {
    app_manager: AppManagerRef,
    runtime: RuntimeRef,
}

impl AppCommandBridgeHandler {
    pub fn new(app_manager: AppManagerRef, runtime: RuntimeRef) -> Self {
        Self {
            app_manager,
            runtime,
        }
    }
}

impl FrameHandler for AppCommandBridgeHandler {
    fn on_frame(&mut self, frame: Frame, responder: &mut Responder<'_>) {
        let tracker = RequestMetricTracker::new(&frame);
        tracker.start();

        let command = match Command::from_frame(frame) {
            Ok(command) => command,
            Err(e) => {
                error!("Errors on decoding the urpc frame to command. {:#?}", e);
                return;
            }
        };

        let remote = responder.remote();
        let app_manager = self.app_manager.clone();
        self.runtime.spawn(async move {
            let _tracker = tracker;
            match command.process(app_manager).await {
                Ok(frame) => match materialize_frame_data(frame) {
                    Ok(frame) => {
                        if let Err(e) = remote.respond(&frame) {
                            error!("Errors on encoding the urpc response frame. {:#?}", e);
                        }
                    }
                    Err(e) => {
                        error!("Errors on materializing the response data. {:#?}", e);
                    }
                },
                Err(e) => {
                    error!("Errors on handling the urpc request. {:#?}", e);
                }
            }
        });
    }
}

/// The uring engine only writes in-memory payloads. Raw fd/pipe based
/// payloads (produced by sendfile/splice read modes) are materialized into
/// memory here.
fn materialize_frame_data(frame: Frame) -> Result<Frame> {
    match frame {
        Frame::GetLocalDataResponse(mut resp) => {
            resp.data = materialize_data_bytes(resp.data)?;
            Ok(Frame::GetLocalDataResponse(resp))
        }
        Frame::GetLocalDataIndexResponse(mut resp) => {
            resp.data_index.index_data = materialize_data_bytes(resp.data_index.index_data)?;
            Ok(Frame::GetLocalDataIndexResponse(resp))
        }
        Frame::GetLocalDataIndexV2Response(mut resp) => {
            resp.data_index.index_data = materialize_data_bytes(resp.data_index.index_data)?;
            Ok(Frame::GetLocalDataIndexV2Response(resp))
        }
        other => Ok(other),
    }
}

fn materialize_data_bytes(data: DataBytes) -> Result<DataBytes> {
    match data {
        DataBytes::RawIO(raw) => {
            use std::os::unix::fs::FileExt;
            let mut buf = vec![0u8; raw.length as usize];
            raw.file.read_exact_at(&mut buf, raw.offset)?;
            Ok(DataBytes::Direct(Bytes::from(buf)))
        }
        DataBytes::RawPipe(mut pipe) => {
            use std::io::Read;
            let mut buf = vec![0u8; pipe.length];
            pipe.pipe_out_fd.read_exact(&mut buf)?;
            Ok(DataBytes::Direct(Bytes::from(buf)))
        }
        other => Ok(other),
    }
}
