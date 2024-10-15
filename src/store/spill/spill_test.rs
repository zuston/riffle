#[cfg(feature = "hdfs")]
#[cfg(test)]
mod tests {
    use crate::app::test::mock_writing_context;
    use crate::config::Config;
    use crate::config::StorageType::{HDFS, LOCALFILE};
    use crate::runtime::manager::RuntimeManager;
    use crate::store::hybrid::{HybridStore, PersistentStore};
    use crate::store::spill::event_handler::SpillEventHandler;
    use crate::store::spill::spill_test::mock::MockStore;
    use crate::store::Store;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::Arc;
    use std::time::Duration;

    fn create_3_level_config(grpc_port: i32, capacity: String, local_data_path: String) -> Config {
        let toml_str = format!(
            r#"
        store_type = "MEMORY_LOCALFILE_HDFS"
        coordinator_quorum = [""]
        grpc_port = {:?}

        [memory_store]
        capacity = {:?}

        [localfile_store]
        data_paths = [{:?}]

        [hdfs_store]
        max_concurrency = 10
        "#,
            grpc_port, capacity, local_data_path
        );

        toml::from_str(toml_str.as_str()).unwrap()
    }

    fn create_hybrid_store(config: Config, warm: &MockStore, cold: &MockStore) -> Arc<HybridStore> {
        let runtime_manager = RuntimeManager::default();
        let mut hybrid_store = HybridStore::from(config, runtime_manager);

        let warm_wrapper: Option<Box<dyn PersistentStore>> = Some(Box::new(warm.clone()));
        let cold_wrapper: Option<Box<dyn PersistentStore>> = Some(Box::new(cold.clone()));
        let _ = std::mem::replace(&mut hybrid_store.warm_store, warm_wrapper);
        let _ = std::mem::replace(&mut hybrid_store.cold_store, cold_wrapper);

        let threshold = 10u64;
        let _ = std::mem::replace(
            &mut hybrid_store.memory_spill_partition_max_threshold,
            Some(10),
        );

        let store = Arc::new(hybrid_store);
        store.event_bus.subscribe(SpillEventHandler {
            store: store.clone(),
        });

        store.clone()
    }

    #[tokio::test]
    async fn test_single_buffer_spill() {
        let warm_healthy = Arc::new(AtomicBool::new(true));
        let warm = MockStore::new(LOCALFILE, &warm_healthy);
        let cold_healthy = Arc::new(AtomicBool::new(true));
        let cold = MockStore::new(HDFS, &cold_healthy);

        let temp_dir = tempdir::TempDir::new("test_local_store").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("init local file path: {}", &temp_path);

        let mut config = create_3_level_config(1, "1M".to_string(), temp_path);
        config.hybrid_store.memory_spill_high_watermark = 1.0;

        let store = create_hybrid_store(config, &warm, &cold);

        let app_id = "single_buffer_spill-app";
        let ctx = mock_writing_context(app_id, 1, 0, 1, 20);
        let _ = store.insert(ctx).await;

        awaitility::at_most(Duration::from_secs(1))
            .until(|| warm.inner.spill_insert_ops.load(SeqCst) == 1);
    }

    #[tokio::test]
    async fn test_localfile_disk_unhealthy() {
        // when the local disk is unhealthy, the data should be flushed
        // to the cold store(like hdfs). If not having cold, it will retry again
        // then again.
        let warm_healthy = Arc::new(AtomicBool::new(true));
        let warm = MockStore::new(LOCALFILE, &warm_healthy);
        let cold_healthy = Arc::new(AtomicBool::new(true));
        let cold = MockStore::new(HDFS, &cold_healthy);

        let temp_dir = tempdir::TempDir::new("test_local_store").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("init local file path: {}", &temp_path);

        let mut config = create_3_level_config(1, "1M".to_string(), temp_path);
        config.hybrid_store.memory_spill_high_watermark = 1.0;

        let store = create_hybrid_store(config, &warm, &cold);

        warm_healthy.store(false, SeqCst);
        let app_id = "test_localfile_disk_unhealthy-app";
        let ctx = mock_writing_context(app_id, 1, 0, 1, 20);
        let _ = store.insert(ctx).await;

        awaitility::at_most(Duration::from_secs(1))
            .until(|| cold.inner.spill_insert_ops.load(SeqCst) == 1);
    }
}

mod mock {
    use crate::app::{
        PurgeDataContext, ReadingIndexViewContext, ReadingViewContext, RegisterAppContext,
        ReleaseTicketContext, RequireBufferContext, WritingViewContext,
    };
    use crate::config::StorageType;
    use crate::error::WorkerError;
    use crate::store::hybrid::PersistentStore;
    use crate::store::spill::SpillWritingViewContext;
    use crate::store::{Persistent, RequireBufferResponse, ResponseData, ResponseDataIndex, Store};
    use async_trait::async_trait;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::sync::Arc;

    #[derive(Clone)]
    pub(crate) struct MockStore {
        pub(crate) inner: Arc<Inner>,
    }

    pub struct Inner {
        pub(crate) spill_insert_ops: AtomicU64,
        pub(crate) store_type: StorageType,
        pub(crate) is_healthy: Arc<AtomicBool>,
    }

    impl MockStore {
        pub fn new(stype: StorageType, is_healthy: &Arc<AtomicBool>) -> Self {
            Self {
                inner: Arc::new(Inner {
                    spill_insert_ops: Default::default(),
                    store_type: stype,
                    is_healthy: is_healthy.clone(),
                }),
            }
        }
    }
    impl Persistent for MockStore {}
    impl PersistentStore for MockStore {}
    #[async_trait]
    impl Store for MockStore {
        fn start(self: Arc<Self>) {
            todo!()
        }

        async fn insert(&self, ctx: WritingViewContext) -> anyhow::Result<(), WorkerError> {
            todo!()
        }

        async fn get(&self, ctx: ReadingViewContext) -> anyhow::Result<ResponseData, WorkerError> {
            todo!()
        }

        async fn get_index(
            &self,
            ctx: ReadingIndexViewContext,
        ) -> anyhow::Result<ResponseDataIndex, WorkerError> {
            todo!()
        }

        async fn purge(&self, ctx: PurgeDataContext) -> anyhow::Result<i64> {
            todo!()
        }

        async fn is_healthy(&self) -> anyhow::Result<bool> {
            Ok(self.inner.is_healthy.load(SeqCst))
        }

        async fn require_buffer(
            &self,
            ctx: RequireBufferContext,
        ) -> anyhow::Result<RequireBufferResponse, WorkerError> {
            todo!()
        }

        async fn release_ticket(
            &self,
            ctx: ReleaseTicketContext,
        ) -> anyhow::Result<i64, WorkerError> {
            todo!()
        }

        async fn register_app(&self, ctx: RegisterAppContext) -> anyhow::Result<()> {
            todo!()
        }

        async fn name(&self) -> StorageType {
            self.inner.store_type
        }

        async fn spill_insert(
            &self,
            ctx: SpillWritingViewContext,
        ) -> anyhow::Result<(), WorkerError> {
            self.inner.spill_insert_ops.fetch_add(1, SeqCst);
            Ok(())
        }
    }
}
