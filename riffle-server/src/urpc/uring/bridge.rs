//! Bridge between the io_uring net engine and the existing urpc command
//! processing logic.

use crate::app_manager::AppManagerRef;
use crate::config::UrpcNetEngine;
use crate::store::DataBytes;
use crate::urpc::command::Command;
use crate::urpc::frame::Frame;
use crate::urpc::metrics::RequestMetricTracker;
use crate::urpc::uring::engine::{FrameHandler, Responder};
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use core_affinity::CoreId;
use log::error;
use std::sync::{mpsc, Arc};
use tokio::runtime::Runtime;

pub(crate) fn create_handler_runtime(core: CoreId) -> Result<Arc<Runtime>> {
    let (affinity_tx, affinity_rx) = mpsc::sync_channel(1);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name(format!("urpc-handler-{}", core.id))
        .on_thread_start(move || {
            let _ = affinity_tx.send(core_affinity::set_for_current(core));
        })
        .enable_all()
        .build()
        .with_context(|| {
            format!(
                "failed to create urpc handler runtime on logical CPU {}",
                core.id
            )
        })?;

    if !affinity_rx
        .recv()
        .context("urpc handler runtime exited before reporting CPU affinity")?
    {
        return Err(anyhow!(
            "failed to bind urpc handler runtime to logical CPU {}",
            core.id
        ));
    }

    Ok(Arc::new(runtime))
}

pub struct AppCommandBridgeHandler {
    app_manager: AppManagerRef,
    runtime: Arc<Runtime>,
}

impl AppCommandBridgeHandler {
    pub fn new(app_manager: AppManagerRef, runtime: Arc<Runtime>) -> Self {
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
            match command.process(app_manager, UrpcNetEngine::URING).await {
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

#[cfg(test)]
mod tests {
    use super::create_handler_runtime;

    #[test]
    fn handler_runtime_is_pinned() {
        let core = core_affinity::get_core_ids()
            .and_then(|cores| cores.into_iter().next())
            .expect("at least one logical CPU should be available");
        let runtime = create_handler_runtime(core).unwrap();

        let allowed_cores = runtime.block_on(runtime.spawn(async {
            core_affinity::get_core_ids().expect("runtime CPU affinity should be readable")
        }));
        assert_eq!(vec![core], allowed_cores.unwrap());
    }
}
