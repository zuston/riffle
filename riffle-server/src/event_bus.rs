use crate::await_tree::AWAIT_TREE_REGISTRY;
use crate::config_ref::ConfigOption;
use crate::metric::{
    EVENT_BUS_HANDLE_DURATION, GAUGE_EVENT_BUS_QUEUE_HANDLING_SIZE,
    GAUGE_EVENT_BUS_QUEUE_PENDING_SIZE, TOTAL_EVENT_BUS_EVENT_HANDLED_SIZE,
    TOTAL_EVENT_BUS_EVENT_PUBLISHED_SIZE,
};
use crate::runtime::RuntimeRef;
use async_trait::async_trait;
use await_tree::{InstrumentAwait, SpanExt};
use log::info;
use once_cell::sync::OnceCell;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::Instrument;

pub(crate) mod queue;
use queue::{EventQueue, FifoQueue, QueueCompletion};

#[async_trait]
pub trait Subscriber: Send + Sync {
    type Input;

    async fn on_event(&self, event: Event<Self::Input>) -> bool;
}

#[derive(Debug, Clone)]
pub struct Event<T> {
    pub data: T,
}

impl<T: Send + Sync + Clone> Event<T> {
    pub fn new(data: T) -> Event<T> {
        Event { data }
    }

    pub fn get_data(&self) -> &T {
        &self.data
    }
}

impl<T: Send + Sync + Clone> From<T> for Event<T> {
    fn from(data: T) -> Self {
        Event::new(data)
    }
}

#[derive(Clone)]
pub struct EventBus<T> {
    inner: Arc<Inner<T>>,
}

struct Inner<T> {
    subscriber: OnceCell<Arc<Box<dyn Subscriber<Input = T> + 'static>>>,

    queue: Arc<dyn EventQueue<T>>,

    name: String,
    runtime: RuntimeRef,

    concurrency_num: ConfigOption<usize>,
    concurrency_limit: Arc<Semaphore>,
}

unsafe impl<T: Send + Sync + 'static> Send for EventBus<T> {}
unsafe impl<T: Send + Sync + 'static> Sync for EventBus<T> {}

impl<T: Send + Sync + Clone + 'static> EventBus<T> {
    pub fn new(
        runtime: &RuntimeRef,
        name: String,
        concurrency_ref: ConfigOption<usize>,
    ) -> EventBus<T> {
        Self::new_with_queue(runtime, name, concurrency_ref, Arc::new(FifoQueue::new()))
    }

    pub(crate) fn new_with_queue(
        runtime: &RuntimeRef,
        name: String,
        concurrency_ref: ConfigOption<usize>,
        queue: Arc<dyn EventQueue<T>>,
    ) -> EventBus<T> {
        let concurrency_limiter = Arc::new(Semaphore::new(concurrency_ref.get()));

        // attach the callback into the dynamic concurrency ref
        let limiter = concurrency_limiter.clone();
        concurrency_ref.with_callback(Box::new(move |legacy, new| {
            if new > legacy {
                limiter.add_permits(new - legacy);
            } else {
                limiter.forget_permits(legacy - new);
            }
            info!(
                "The concurrency limiter permits change from {} to {}",
                legacy, new
            );
        }));

        let event_bus = EventBus {
            inner: Arc::new(Inner {
                subscriber: OnceCell::new(),
                queue,
                name: name.to_string(),
                runtime: runtime.clone(),
                concurrency_num: concurrency_ref,
                concurrency_limit: concurrency_limiter,
            }),
        };
        let cloned = event_bus.clone();
        runtime.spawn_with_await_tree(
            format!("EventBus - [{}]", &event_bus.inner.name).as_str(),
            async move {
                EventBus::handle(cloned).await;
            },
        );
        event_bus
    }

    async fn handle(event_bus: EventBus<T>) {
        while let Ok((message, completion)) = event_bus
            .inner
            .queue
            .pop()
            .instrument_await("receiving event".long_running())
            .await
        {
            let permit = event_bus
                .inner
                .concurrency_limit
                .clone()
                .acquire_owned()
                .instrument_await("waiting for the spill concurrent reject.")
                .await
                .unwrap();
            event_bus.spawn_handler(message, permit, completion);
        }
    }

    fn spawn_handler(
        &self,
        message: Event<T>,
        concurrency_guarder: OwnedSemaphorePermit,
        completion: QueueCompletion,
    ) {
        let bus = self.clone();
        self.inner.runtime.spawn_with_await_tree(
            format!("EventBus - [{}] - Handler", &self.inner.name).as_str(),
            async move {
                let timer = EVENT_BUS_HANDLE_DURATION
                    .with_label_values(&[&bus.inner.name])
                    .start_timer();
                GAUGE_EVENT_BUS_QUEUE_HANDLING_SIZE
                    .with_label_values(&[&bus.inner.name])
                    .inc();
                GAUGE_EVENT_BUS_QUEUE_PENDING_SIZE
                    .with_label_values(&[&bus.inner.name])
                    .dec();

                let binding = bus.inner.subscriber.get();
                let subscriber = binding.as_ref().unwrap();
                let _ = subscriber.on_event(message).await;

                timer.observe_duration();
                GAUGE_EVENT_BUS_QUEUE_HANDLING_SIZE
                    .with_label_values(&[&bus.inner.name])
                    .dec();
                TOTAL_EVENT_BUS_EVENT_HANDLED_SIZE
                    .with_label_values(&[&bus.inner.name])
                    .inc();

                drop(completion);
                drop(concurrency_guarder);
            },
        );
    }

    pub fn subscribe<R: Subscriber<Input = T> + 'static + Send + Sync>(&self, listener: R) {
        let _ = self.inner.subscriber.set(Arc::new(Box::new(listener)));
    }

    pub async fn publish(&self, event: Event<T>) -> anyhow::Result<()> {
        self.sync_publish(event)
    }

    pub fn sync_publish(&self, event: Event<T>) -> anyhow::Result<()> {
        self.inner.queue.push(event)?;

        GAUGE_EVENT_BUS_QUEUE_PENDING_SIZE
            .with_label_values(&[&self.inner.name])
            .inc();
        TOTAL_EVENT_BUS_EVENT_PUBLISHED_SIZE
            .with_label_values(&[&self.inner.name])
            .inc();
        Ok(())
    }

    pub fn concurrency_limit(&self) -> usize {
        self.inner.concurrency_num.get()
    }
}

#[cfg(test)]
mod test {
    use crate::app_manager::partition_identifier::PartitionUId;
    use crate::config_ref::{ConfRef, ConfigOption, DynamicConfRef, StaticConfRef};
    use crate::event_bus::queue::PartitionPriorityQueue;
    use crate::event_bus::{Event, EventBus, Subscriber};
    use crate::metric::{
        GAUGE_EVENT_BUS_QUEUE_HANDLING_SIZE, GAUGE_EVENT_BUS_QUEUE_PENDING_SIZE,
        TOTAL_EVENT_BUS_EVENT_HANDLED_SIZE, TOTAL_EVENT_BUS_EVENT_PUBLISHED_SIZE,
    };
    use crate::runtime::manager::create_runtime;
    use async_trait::async_trait;
    use std::sync::atomic::Ordering::{Relaxed, SeqCst};
    use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;
    use tokio::sync::Semaphore;

    #[test]
    fn test_keyed_bus_prioritizes_idle_partitions() -> anyhow::Result<()> {
        const NAME: &str = "test_keyed_bus_prioritizes_idle_partitions";

        struct BlockingCallback {
            started: async_channel::Sender<(PartitionUId, u64)>,
            gates: Arc<Vec<Arc<Semaphore>>>,
        }

        #[async_trait]
        impl Subscriber for BlockingCallback {
            type Input = (PartitionUId, u64);

            async fn on_event(&self, event: Event<Self::Input>) -> bool {
                self.started.send(event.data.clone()).await.unwrap();
                self.gates[event.data.0.shuffle_id as usize]
                    .acquire()
                    .await
                    .unwrap()
                    .forget();
                true
            }
        }

        let runtime = create_runtime(2, NAME);
        let bus = EventBus::new_with_queue(
            &runtime,
            NAME.to_string(),
            StaticConfRef::new(2usize).into(),
            Arc::new(PartitionPriorityQueue::new(
                |message: &(PartitionUId, u64)| message.0.clone(),
            )),
        );
        let (started, received) = async_channel::unbounded();
        let gates: Arc<Vec<_>> = Arc::new((0..3).map(|_| Arc::new(Semaphore::new(0))).collect());
        bus.subscribe(BlockingCallback {
            started,
            gates: gates.clone(),
        });
        let message = |shuffle_id, sequence| {
            (
                PartitionUId {
                    shuffle_id,
                    ..Default::default()
                },
                sequence,
            )
        };
        let next_started = || async {
            tokio::time::timeout(Duration::from_secs(2), received.recv())
                .await
                .unwrap()
                .unwrap()
        };

        runtime.block_on(async {
            bus.publish(message(0, 0).into()).await?;
            assert_eq!(message(0, 0), next_started().await);
            bus.publish(message(0, 1).into()).await?;
            bus.publish(message(0, 2).into()).await?;
            bus.publish(message(1, 0).into()).await?;
            bus.publish(message(1, 1).into()).await?;

            // Another full UID must fill the second slot despite a hot partition backlog.
            assert_eq!(message(1, 0), next_started().await);
            bus.publish(message(2, 0).into()).await?;
            assert!(received.try_recv().is_err());

            // Queued events for active partitions do not occupy execution slots.
            gates[1].add_permits(1);
            assert_eq!(message(2, 0), next_started().await);
            gates[2].add_permits(1);
            assert_eq!(message(1, 1), next_started().await);
            gates[1].add_permits(1);

            // The hot partition's queued events remain FIFO.
            gates[0].add_permits(3);
            assert_eq!(message(0, 1), next_started().await);
            assert_eq!(message(0, 2), next_started().await);
            tokio::time::timeout(Duration::from_secs(2), async {
                while TOTAL_EVENT_BUS_EVENT_HANDLED_SIZE
                    .with_label_values(&[NAME])
                    .get()
                    != 6
                {
                    tokio::task::yield_now().await;
                }
            })
            .await?;
            assert_eq!(
                0,
                GAUGE_EVENT_BUS_QUEUE_PENDING_SIZE
                    .with_label_values(&[NAME])
                    .get()
            );
            assert_eq!(
                0,
                GAUGE_EVENT_BUS_QUEUE_HANDLING_SIZE
                    .with_label_values(&[NAME])
                    .get()
            );

            // A drained key can be scheduled again.
            bus.publish(message(0, 3).into()).await?;
            assert_eq!(message(0, 3), next_started().await);
            gates[0].add_permits(1);
            Ok(())
        })
    }

    #[test]
    fn test_key_released_after_handler_panics() -> anyhow::Result<()> {
        struct Callback {
            handled: async_channel::Sender<u32>,
        }

        #[async_trait]
        impl Subscriber for Callback {
            type Input = u32;

            async fn on_event(&self, event: Event<u32>) -> bool {
                if event.data == 0 {
                    panic!("handler failed");
                }
                self.handled.send(event.data).await.unwrap();
                true
            }
        }

        let runtime = create_runtime(2, "test_key_released_after_handler_panics");
        let bus = EventBus::new_with_queue(
            &runtime,
            "test_key_released_after_handler_panics".to_string(),
            StaticConfRef::new(1usize).into(),
            Arc::new(PartitionPriorityQueue::new(|_: &u32| 0)),
        );
        let (handled, received) = async_channel::unbounded();
        bus.subscribe(Callback { handled });

        runtime.block_on(async {
            bus.publish(0.into()).await?;
            bus.publish(1.into()).await?;
            assert_eq!(
                1,
                tokio::time::timeout(Duration::from_secs(1), received.recv()).await??
            );
            Ok(())
        })
    }

    #[test]
    fn test_dynamic_limiter() -> anyhow::Result<()> {
        use awaitility::at_most;
        use std::time::Duration;

        const EVENT_BUS_NAME: &str = "test_dynamic_limiter";

        let dyn_conf = DynamicConfRef::new("", 1usize);
        let dyn_conf: ConfigOption<usize> = dyn_conf.into();

        let runtime = create_runtime(5, "test");
        let event_bus = EventBus::new(&runtime, EVENT_BUS_NAME.to_string(), dyn_conf.clone());

        let flag = Arc::new(AtomicI64::new(0));

        struct SimpleCallback {
            flag: Arc<AtomicI64>,
        }

        #[async_trait]
        impl Subscriber for SimpleCallback {
            type Input = String;

            async fn on_event(&self, event: Event<Self::Input>) -> bool {
                println!("SimpleCallback received event: {:?}", event.get_data());
                self.flag.fetch_add(1, Ordering::SeqCst);
                sleep(Duration::from_millis(400));
                true
            }
        }

        let flag_cloned = flag.clone();
        event_bus.subscribe(SimpleCallback { flag: flag_cloned });

        let bus = event_bus.clone();
        runtime.block_on(async move {
            bus.publish(Event::new("test_event".to_string()))
                .await
                .unwrap();
        });
        at_most(Duration::from_secs(1)).until(|| flag.load(Ordering::SeqCst) == 1);

        dyn_conf.on_change(&serde_json::json!(5));
        for i in 0..5 {
            let bus = event_bus.clone();
            runtime.block_on(async move {
                bus.publish(Event::new(format!("event_{}", i)))
                    .await
                    .unwrap();
            });
        }
        // the updated dynamic concurrency will accept all tasks to run. the total
        // duration will be less than 1 sec.
        at_most(Duration::from_secs(1)).until(|| flag.load(SeqCst) == 6);
        Ok(())
    }

    #[test]
    fn test_event_bus() -> anyhow::Result<()> {
        const EVENT_BUS_NAME: &str = "test";

        TOTAL_EVENT_BUS_EVENT_HANDLED_SIZE.remove_label_values(&[EVENT_BUS_NAME]);
        TOTAL_EVENT_BUS_EVENT_PUBLISHED_SIZE.remove_label_values(&[EVENT_BUS_NAME]);

        let runtime = create_runtime(4, EVENT_BUS_NAME);
        let mut event_bus = EventBus::new(
            &runtime,
            "test".to_string(),
            StaticConfRef::new(1usize).into(),
        );
        let flag = Arc::new(AtomicI64::new(0));

        struct SimpleCallback {
            flag: Arc<AtomicI64>,
        }

        #[async_trait]
        impl Subscriber for SimpleCallback {
            type Input = String;

            async fn on_event(&self, event: Event<Self::Input>) -> bool {
                println!("SimpleCallback has accepted event: {:?}", event.get_data());
                self.flag.fetch_add(1, Ordering::SeqCst);
                true
            }
        }

        let flag_cloned = flag.clone();
        event_bus.subscribe(SimpleCallback { flag: flag_cloned });

        let bus = event_bus.clone();
        let _ =
            runtime.block_on(async move { bus.publish("singleEvent".to_string().into()).await });

        // case1: check the handle logic
        awaitility::at_most(Duration::from_secs(1)).until(|| flag.load(Ordering::SeqCst) == 1);

        // case2: check the metrics
        assert_eq!(
            1,
            TOTAL_EVENT_BUS_EVENT_HANDLED_SIZE
                .with_label_values(&[EVENT_BUS_NAME])
                .get()
        );
        assert_eq!(
            1,
            TOTAL_EVENT_BUS_EVENT_PUBLISHED_SIZE
                .with_label_values(&[EVENT_BUS_NAME])
                .get()
        );

        Ok(())
    }
}
