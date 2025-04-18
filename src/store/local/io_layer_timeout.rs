// use std::sync::Arc;
// use crate::error::WorkerError;
// use crate::store::local::layers::{Handler, Layer};
// use crate::store::local::{FileStat, LocalIO};
// use crate::store::BytesWrapper;
// use anyhow::Context;
// use async_trait::async_trait;
// use await_tree::InstrumentAwait;
// use bytes::Bytes;
// use std::time::Duration;
// use tokio::time::timeout;
//
// pub struct TimeoutLayer {
//     threshold_seconds: usize,
// }
//
// impl Layer for TimeoutLayer {
//     fn wrap(&self, handler: Handler) -> Handler {
//         let layer = TimeoutLayerWrapper {
//             handler,
//             threshold_seconds: self.threshold_seconds as u64,
//         };
//         Arc::new(layer)
//     }
// }
//
// #[derive(Clone)]
// struct TimeoutLayerWrapper {
//     handler: Handler,
//     threshold_seconds: u64,
// }
//
// #[async_trait]
// impl LocalIO for TimeoutLayerWrapper {
//     async fn create_dir(&self, dir: &str) -> anyhow::Result<(), WorkerError> {
//         let f = self.handler.create_dir(dir);
//         timeout(Duration::from_secs(self.threshold_seconds), f).await??;
//         Ok(())
//     }
//
//     async fn append(&self, path: &str, data: BytesWrapper) -> anyhow::Result<(), WorkerError> {
//         let f = self.handler.append(path, data);
//         timeout(Duration::from_secs(self.threshold_seconds), f).await??;
//         Ok(())
//     }
//
//     async fn read(
//         &self,
//         path: &str,
//         offset: i64,
//         length: Option<i64>,
//     ) -> anyhow::Result<Bytes, WorkerError> {
//         let f = self.handler.read(path, offset, length);
//         let bytes = timeout(Duration::from_secs(self.threshold_seconds), f).await??;
//         Ok(bytes)
//     }
//
//     async fn delete(&self, path: &str) -> anyhow::Result<(), WorkerError> {
//         let f = self.handler.delete(path);
//         timeout(Duration::from_secs(self.threshold_seconds), f).await??;
//         Ok(())
//     }
//
//     async fn write(&self, path: &str, data: Bytes) -> anyhow::Result<(), WorkerError> {
//         let f = self.handler.write(path, data);
//         timeout(Duration::from_secs(self.threshold_seconds), f).await??;
//         Ok(())
//     }
//
//     async fn file_stat(&self, path: &str) -> anyhow::Result<FileStat, WorkerError> {
//         let f = self.handler.file_stat(path);
//         let stat = timeout(Duration::from_secs(self.threshold_seconds), f).await??;
//         Ok(stat)
//     }
//
//     async fn direct_append(
//         &self,
//         path: &str,
//         written_bytes: usize,
//         data: BytesWrapper,
//     ) -> anyhow::Result<(), WorkerError> {
//         let f = self.handler.direct_append(path, written_bytes, data);
//         timeout(Duration::from_secs(self.threshold_seconds), f).await??;
//         Ok(())
//     }
//
//     async fn direct_read(
//         &self,
//         path: &str,
//         offset: i64,
//         length: i64,
//     ) -> anyhow::Result<Bytes, WorkerError> {
//         let f = self.handler.direct_read(path, offset, length);
//         let bytes = timeout(Duration::from_secs(self.threshold_seconds), f).await??;
//         Ok(bytes)
//     }
// }
