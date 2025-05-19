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
use crate::store::local::{FileStat, LocalIO};
use crate::store::BytesWrapper;
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
    async fn create_dir(&self, dir: &str) -> anyhow::Result<(), WorkerError> {
        self.handler.create_dir(dir).await
    }

    async fn append(&self, path: &str, data: BytesWrapper) -> anyhow::Result<(), WorkerError> {
        let len = data.len();
        let timer = LOCALFILE_DISK_APPEND_OPERATION_DURATION
            .with_label_values(&[&self.root])
            .start_timer();
        self.handler.append(path, data).await?;
        timer.observe_duration();

        TOTAL_LOCAL_DISK_APPEND_OPERATION_BYTES_COUNTER
            .with_label_values(&[&self.root])
            .inc_by(len as u64);
        TOTAL_LOCAL_DISK_APPEND_OPERATION_COUNTER
            .with_label_values(&[&self.root])
            .inc();
        Ok(())
    }

    async fn read(
        &self,
        path: &str,
        offset: i64,
        length: Option<i64>,
    ) -> anyhow::Result<Bytes, WorkerError> {
        let timer = LOCALFILE_DISK_READ_OPERATION_DURATION
            .with_label_values(&[&self.root])
            .start_timer();

        let bytes = self.handler.read(path, offset, length).await?;

        timer.observe_duration();
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

    async fn write(&self, path: &str, data: Bytes) -> anyhow::Result<(), WorkerError> {
        self.handler.write(path, data).await
    }

    async fn file_stat(&self, path: &str) -> anyhow::Result<FileStat, WorkerError> {
        self.handler.file_stat(path).await
    }

    async fn direct_append(
        &self,
        path: &str,
        written_bytes: usize,
        data: BytesWrapper,
    ) -> anyhow::Result<(), WorkerError> {
        let len = data.len();

        let timer = LOCALFILE_DISK_DIRECT_APPEND_OPERATION_DURATION
            .with_label_values(&[&self.root])
            .start_timer();

        self.handler
            .direct_append(path, written_bytes, data)
            .await?;

        TOTAL_LOCAL_DISK_APPEND_OPERATION_BYTES_COUNTER
            .with_label_values(&[&self.root])
            .inc_by(len as u64);
        TOTAL_LOCAL_DISK_APPEND_OPERATION_COUNTER
            .with_label_values(&[&self.root])
            .inc();

        Ok(())
    }

    async fn direct_read(
        &self,
        path: &str,
        offset: i64,
        length: i64,
    ) -> anyhow::Result<Bytes, WorkerError> {
        let timer = LOCALFILE_DISK_DIRECT_READ_OPERATION_DURATION
            .with_label_values(&[&self.root])
            .start_timer();

        let data = self.handler.direct_read(path, offset, length).await?;

        TOTAL_LOCAL_DISK_READ_OPERATION_BYTES_COUNTER
            .with_label_values(&[&self.root])
            .inc_by(data.len() as u64);
        TOTAL_LOCAL_DISK_READ_OPERATION_COUNTER
            .with_label_values(&[&self.root])
            .inc();

        Ok(data)
    }
}
