use crate::error::WorkerError;
use crate::metric::{
    LOCALFILE_DISK_APPEND_OPERATION_DURATION, LOCALFILE_DISK_DELETE_OPERATION_DURATION,
    LOCALFILE_DISK_DIRECT_APPEND_OPERATION_DURATION, LOCALFILE_DISK_DIRECT_READ_OPERATION_DURATION,
    LOCALFILE_DISK_READ_OPERATION_DURATION, TOTAL_LOCAL_DISK_APPEND_OPERATION_BYTES_COUNTER,
    TOTAL_LOCAL_DISK_APPEND_OPERATION_COUNTER, TOTAL_LOCAL_DISK_READ_OPERATION_BYTES_COUNTER,
    TOTAL_LOCAL_DISK_READ_OPERATION_COUNTER,
};
use crate::store::local::io_layer_timeout::TimeoutLayer;
use crate::store::local::layers::{Handler, Layer};
use crate::store::local::options::{CreateOptions, WriteOptions};
use crate::store::local::read_options::ReadOptions;
use crate::store::local::{FileStat, LocalIO};
use crate::store::DataBytes;
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;

pub struct MetricsLayer {
    root: String,
}

impl MetricsLayer {
    pub fn new(root: &str) -> Self {
        Self {
            root: root.to_string(),
        }
    }
}

impl Layer for MetricsLayer {
    fn wrap(&self, handler: Handler) -> Handler {
        Arc::new(Box::new(MetricsLayerWrapper {
            handler,
            root: self.root.clone(),
        }))
    }
}

#[derive(Clone)]
struct MetricsLayerWrapper {
    handler: Handler,
    root: String,
}

unsafe impl Send for MetricsLayerWrapper {}
unsafe impl Sync for MetricsLayerWrapper {}

#[async_trait]
impl LocalIO for MetricsLayerWrapper {
    async fn create(&self, dir: &str, options: CreateOptions) -> anyhow::Result<(), WorkerError> {
        self.handler.create(dir, options).await
    }

    async fn write(&self, path: &str, options: WriteOptions) -> anyhow::Result<(), WorkerError> {
        let len = options.data.len();
        let is_append = options.append;
        // only record for the append mode
        let _timer = if is_append {
            // for buffer io
            Some(if options.offset.is_none() {
                LOCALFILE_DISK_APPEND_OPERATION_DURATION
                    .with_label_values(&[&self.root])
                    .start_timer()
            } else {
                // for direct io
                LOCALFILE_DISK_DIRECT_APPEND_OPERATION_DURATION
                    .with_label_values(&[&self.root])
                    .start_timer()
            })
        } else {
            None
        };

        self.handler.write(path, options).await?;

        if is_append {
            TOTAL_LOCAL_DISK_APPEND_OPERATION_BYTES_COUNTER
                .with_label_values(&[&self.root])
                .inc_by(len as u64);
            TOTAL_LOCAL_DISK_APPEND_OPERATION_COUNTER
                .with_label_values(&[&self.root])
                .inc();
        }

        Ok(())
    }

    async fn read(
        &self,
        path: &str,
        options: ReadOptions,
    ) -> anyhow::Result<DataBytes, WorkerError> {
        let timer = LOCALFILE_DISK_READ_OPERATION_DURATION
            .with_label_values(&[&self.root])
            .start_timer();

        let bytes = self.handler.read(path, options).await?;

        TOTAL_LOCAL_DISK_READ_OPERATION_BYTES_COUNTER
            .with_label_values(&[&self.root])
            .inc_by(bytes.len() as u64);
        TOTAL_LOCAL_DISK_READ_OPERATION_COUNTER
            .with_label_values(&[&self.root])
            .inc();

        Ok(bytes)
    }

    async fn delete(&self, path: &str) -> anyhow::Result<(), WorkerError> {
        let timer = LOCALFILE_DISK_DELETE_OPERATION_DURATION
            .with_label_values(&[&self.root])
            .start_timer();
        self.handler.delete(path).await?;
        Ok(())
    }

    async fn file_stat(&self, path: &str) -> anyhow::Result<FileStat, WorkerError> {
        self.handler.file_stat(path).await
    }
}
