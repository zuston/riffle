use crate::await_tree::AWAIT_TREE_REGISTRY;
use crate::metric::{
    GAUGE_EVENT_BUS_QUEUE_PENDING_SIZE, TOTAL_EVENT_BUS_EVENT_HANDLED_SIZE,
    TOTAL_EVENT_BUS_EVENT_PUBLISHED_SIZE,
};
use crate::runtime::RuntimeRef;
use parking_lot::Mutex;
use std::sync::Arc;
use tracing::Instrument;

pub trait Subscriber {
    type Input;

    fn on_event(&self, event: &Event<Self::Input>);
}

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
    subscribers: Arc<Mutex<Vec<Box<dyn Subscriber<Input = T> + 'static>>>>,

    queue_recv: async_channel::Receiver<Event<T>>,
    queue_send: async_channel::Sender<Event<T>>,

    name: String,
}

unsafe impl<T: Send + Sync + 'static> Send for EventBus<T> {}
unsafe impl<T: Send + Sync + 'static> Sync for EventBus<T> {}

impl<T: Send + Sync + Clone + 'static> EventBus<T> {
    pub fn new(runtime: RuntimeRef, name: String) -> EventBus<T> {
        let (send, recv) = async_channel::unbounded();
        let event_bus = EventBus {
            subscribers: Arc::new(Mutex::new(vec![])),
            queue_recv: recv,
            queue_send: send,
            name: name.to_string(),
        };

        let cloned = event_bus.clone();
        runtime.spawn(async move {
            let await_root = AWAIT_TREE_REGISTRY
                .clone()
                .register(format!("EventBus - [{:?}]", name))
                .await;
            await_root
                .instrument(async move {
                    EventBus::handle(cloned).await;
                })
                .await;
        });

        event_bus
    }

    async fn handle(event_bus: EventBus<T>) {
        while let Ok(message) = event_bus.queue_recv.recv().await {
            let subscribers = event_bus.subscribers.lock();
            for subscriber in subscribers.iter() {
                subscriber.on_event(&message);
            }
            GAUGE_EVENT_BUS_QUEUE_PENDING_SIZE
                .with_label_values(&[&event_bus.name])
                .dec();
            TOTAL_EVENT_BUS_EVENT_HANDLED_SIZE
                .with_label_values(&[&event_bus.name])
                .inc();
        }
    }

    pub fn subscribe<R: Subscriber<Input = T> + 'static>(&mut self, listener: R) {
        let mut subscribers = self.subscribers.lock();
        subscribers.push(Box::new(listener));
    }

    pub async fn publish(&self, event: Event<T>) -> anyhow::Result<()> {
        self.queue_send.send(event).await?;
        GAUGE_EVENT_BUS_QUEUE_PENDING_SIZE
            .with_label_values(&[&self.name])
            .inc();
        TOTAL_EVENT_BUS_EVENT_PUBLISHED_SIZE
            .with_label_values(&[&self.name])
            .inc();
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::event_bus::{Event, EventBus, Subscriber};
    use crate::metric::{TOTAL_EVENT_BUS_EVENT_HANDLED_SIZE, TOTAL_EVENT_BUS_EVENT_PUBLISHED_SIZE};
    use crate::runtime::manager::create_runtime;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn test_event_bus() -> anyhow::Result<()> {
        let runtime = create_runtime(1, "test");
        let mut event_bus = EventBus::new(runtime.clone(), "test".to_string());

        let flag = Arc::new(AtomicI64::new(0));

        struct SimpleCallback {
            flag: Arc<AtomicI64>,
        }
        impl Subscriber for SimpleCallback {
            type Input = String;

            fn on_event(&self, event: &Event<Self::Input>) {
                println!("SimpleCallback has accepted event: {:?}", event.get_data());
                self.flag.fetch_add(1, Ordering::SeqCst);
            }
        }
        event_bus.subscribe(SimpleCallback { flag: flag.clone() });

        let _ = runtime
            .block_on(async move { event_bus.publish("singleEvent".to_string().into()).await });

        // case1: check the handle logic
        awaitility::at_most(Duration::from_secs(1)).until(|| flag.load(Ordering::SeqCst) == 1);

        // case2: check the metrics
        assert_eq!(
            1,
            TOTAL_EVENT_BUS_EVENT_HANDLED_SIZE
                .with_label_values(&["test"])
                .get()
        );
        assert_eq!(
            1,
            TOTAL_EVENT_BUS_EVENT_PUBLISHED_SIZE
                .with_label_values(&["test"])
                .get()
        );

        Ok(())
    }
}
