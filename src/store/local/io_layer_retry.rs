// use crate::error::WorkerError;
// use crate::store::local::layers::{Handler, Layer};
// use crate::store::local::{FileStat, LocalIO};
// use crate::store::BytesWrapper;
// use async_trait::async_trait;
// use backon::Retryable;
// use backon::{BackoffBuilder, ExponentialBuilder};
// use bytes::Bytes;
// use std::sync::Arc;
//
// /// Now only the delete will be retried on failure
// struct RetryIoLayer {
//     retry_times: u32,
// }
//
// impl Layer for RetryIoLayer {
//     fn wrap(&self, handler: Handler) -> Handler {
//         let layer = RetryIoLayerWrapper {
//             inner: Arc::new(handler),
//             retry_times: self.retry_times,
//         };
//         Arc::new(Box::new(layer))
//     }
// }
//
// #[derive(Clone)]
// struct RetryIoLayerWrapper {
//     inner: Arc<Box<dyn LocalIO>>,
//     retry_times: u32,
// }
//
// #[async_trait]
// impl LocalIO for RetryIoLayerWrapper {
//     async fn create_dir(&self, dir: &str) -> anyhow::Result<(), WorkerError> {
//         self.inner.create_dir(dir).await
//     }
//
//     async fn append(&self, path: &str, data: BytesWrapper) -> anyhow::Result<(), WorkerError> {
//         self.inner.append(path, data).await
//     }
//
//     async fn read(
//         &self,
//         path: &str,
//         offset: i64,
//         length: Option<i64>,
//     ) -> anyhow::Result<Bytes, WorkerError> {
//         self.inner.read(path, offset, length).await
//     }
//
//     async fn delete(&self, path: &str) -> anyhow::Result<(), WorkerError> {
//         self.inner
//             .delete(path)
//             .retry(
//                 ExponentialBuilder::new()
//                     .with_max_times(self.retry_times)
//                     .build(),
//             )
//             .await
//     }
//
//     async fn write(&self, path: &str, data: Bytes) -> anyhow::Result<(), WorkerError> {
//         self.inner.write(path, data).await
//     }
//
//     async fn file_stat(&self, path: &str) -> anyhow::Result<FileStat, WorkerError> {
//         self.inner.file_stat(path).await
//     }
//
//     async fn direct_append(
//         &self,
//         path: &str,
//         written_bytes: usize,
//         data: BytesWrapper,
//     ) -> anyhow::Result<(), WorkerError> {
//         self.inner.direct_append(path, written_bytes, data).await
//     }
//
//     async fn direct_read(
//         &self,
//         path: &str,
//         offset: i64,
//         length: i64,
//     ) -> anyhow::Result<Bytes, WorkerError> {
//         self.inner.direct_read(path, offset, length).await
//     }
// }
