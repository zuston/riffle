use super::Event;
use anyhow::Result;
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::{HashSet, VecDeque};
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::Notify;

#[async_trait]
pub(crate) trait EventQueue<T>: Send + Sync {
    fn push(&self, event: Event<T>) -> Result<()>;
    async fn pop(&self) -> Result<(Event<T>, QueueCompletion)>;
}

/// Release a queue reservation when a handler finishes, panics, or is cancelled.
#[derive(Default)]
pub(crate) struct QueueCompletion(Option<Box<dyn FnOnce() + Send>>);

impl QueueCompletion {
    fn new(complete: impl FnOnce() + Send + 'static) -> Self {
        Self(Some(Box::new(complete)))
    }
}

impl Drop for QueueCompletion {
    fn drop(&mut self) {
        if let Some(complete) = self.0.take() {
            complete();
        }
    }
}

pub(crate) struct FifoQueue<T> {
    sender: async_channel::Sender<Event<T>>,
    receiver: async_channel::Receiver<Event<T>>,
}

impl<T> FifoQueue<T> {
    pub(crate) fn new() -> Self {
        let (sender, receiver) = async_channel::unbounded();
        Self { sender, receiver }
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> EventQueue<T> for FifoQueue<T> {
    fn push(&self, event: Event<T>) -> Result<()> {
        self.sender.try_send(event)?;
        Ok(())
    }

    async fn pop(&self) -> Result<(Event<T>, QueueCompletion)> {
        Ok((self.receiver.recv().await?, QueueCompletion::default()))
    }
}

struct PartitionQueueState<T, K> {
    pending: VecDeque<(K, Event<T>)>,
    active: HashSet<K>,
}

pub(crate) struct PartitionPriorityQueue<T, K, F> {
    key: F,
    state: Arc<Mutex<PartitionQueueState<T, K>>>,
    available: Arc<Notify>,
}

impl<T, K, F> PartitionPriorityQueue<T, K, F> {
    pub(crate) fn new(key: F) -> Self {
        Self {
            key,
            state: Arc::new(Mutex::new(PartitionQueueState {
                pending: VecDeque::new(),
                active: HashSet::new(),
            })),
            available: Arc::new(Notify::new()),
        }
    }
}

#[async_trait]
impl<T, K, F> EventQueue<T> for PartitionPriorityQueue<T, K, F>
where
    T: Send + 'static,
    K: Eq + Hash + Clone + Send + 'static,
    F: Fn(&T) -> K + Send + Sync,
{
    fn push(&self, event: Event<T>) -> Result<()> {
        let key = (self.key)(&event.data);
        self.state.lock().pending.push_back((key, event));
        self.available.notify_one();
        Ok(())
    }

    async fn pop(&self) -> Result<(Event<T>, QueueCompletion)> {
        loop {
            let notified = self.available.notified();
            tokio::pin!(notified);
            // Register before checking state so concurrent pushes and completions cannot be missed.
            notified.as_mut().enable();
            let next = {
                let mut state = self.state.lock();
                // shortcut: scanning is linear in backlog size, index ready events if profiling justifies it.
                let index = state
                    .pending
                    .iter()
                    .position(|(key, _)| !state.active.contains(key));
                index.map(|index| {
                    let (key, event) = state.pending.remove(index).unwrap();
                    state.active.insert(key.clone());
                    (key, event)
                })
            };
            if let Some((key, event)) = next {
                let state = self.state.clone();
                let available = self.available.clone();
                let completion = QueueCompletion::new(move || {
                    state.lock().active.remove(&key);
                    available.notify_one();
                });
                return Ok((event, completion));
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{EventQueue, FifoQueue, PartitionPriorityQueue};
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn fifo_keeps_arrival_order_without_waiting_for_completion() -> anyhow::Result<()> {
        let queue = FifoQueue::new();
        for value in [1, 1, 2] {
            queue.push(value.into())?;
        }
        let mut completions = Vec::new();
        for expected in [1, 1, 2] {
            let (event, completion) = queue.pop().await?;
            assert_eq!(expected, event.data);
            completions.push(completion);
        }
        Ok(())
    }

    #[tokio::test]
    async fn priority_keeps_fifo_among_available_partitions() -> anyhow::Result<()> {
        let queue = PartitionPriorityQueue::new(|event: &(u32, u32)| event.0);
        for event in [(0, 0), (1, 0), (0, 1), (2, 0)] {
            queue.push(event.into())?;
        }
        let (first, completion) = queue.pop().await?;
        assert_eq!((0, 0), first.data);
        drop(completion);

        // Completing partition 0 must not move its second event ahead of older partition 1.
        let (second, completion) = queue.pop().await?;
        assert_eq!((1, 0), second.data);
        drop(completion);
        assert_eq!((0, 1), queue.pop().await?.0.data);
        assert_eq!((2, 0), queue.pop().await?.0.data);
        Ok(())
    }

    #[tokio::test]
    async fn priority_skips_active_partitions_and_wakes_after_completion() -> anyhow::Result<()> {
        let queue = PartitionPriorityQueue::new(|event: &(u32, u32)| event.0);
        for event in [(0, 0), (0, 1), (1, 0)] {
            queue.push(event.into())?;
        }
        let (_, active) = queue.pop().await?;
        assert_eq!((1, 0), queue.pop().await?.0.data);
        let pending = queue.pop();
        tokio::pin!(pending);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut pending)
                .await
                .is_err()
        );
        drop(active);
        let (event, _) = tokio::time::timeout(Duration::from_secs(1), pending).await??;
        assert_eq!((0, 1), event.data);
        Ok(())
    }

    #[tokio::test]
    async fn priority_wakes_waiting_consumers_on_push() -> anyhow::Result<()> {
        let queue = Arc::new(PartitionPriorityQueue::new(|event: &u32| *event));
        let mut consumers = Vec::new();
        for _ in 0..2 {
            let queue = queue.clone();
            consumers.push(tokio::spawn(async move { queue.pop().await.unwrap() }));
        }
        tokio::task::yield_now().await;
        queue.push(0.into())?;
        queue.push(1.into())?;
        let mut received = Vec::new();
        for consumer in consumers {
            received.push(
                tokio::time::timeout(Duration::from_secs(1), consumer)
                    .await??
                    .0
                    .data,
            );
        }
        received.sort_unstable();
        assert_eq!(vec![0, 1], received);
        Ok(())
    }

    #[tokio::test]
    async fn priority_releases_partition_when_consumer_is_cancelled() -> anyhow::Result<()> {
        let queue = Arc::new(PartitionPriorityQueue::new(|event: &u32| *event));
        queue.push(0.into())?;
        queue.push(0.into())?;
        let (started, received) = tokio::sync::oneshot::channel();
        let consumer_queue = queue.clone();
        let consumer = tokio::spawn(async move {
            let (_, completion) = consumer_queue.pop().await.unwrap();
            started.send(()).unwrap();
            std::future::pending::<()>().await;
            drop(completion);
        });
        received.await?;
        consumer.abort();
        assert!(consumer.await.unwrap_err().is_cancelled());
        assert_eq!(
            0,
            tokio::time::timeout(Duration::from_secs(1), queue.pop())
                .await??
                .0
                .data
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn priority_handles_concurrent_producers_and_consumers() -> anyhow::Result<()> {
        use parking_lot::Mutex;
        use std::collections::HashSet;

        let queue = Arc::new(PartitionPriorityQueue::new(|event: &(u32, u32)| event.0));
        let active = Arc::new(Mutex::new(HashSet::new()));
        let mut producers = Vec::new();
        let mut consumers = Vec::new();
        for partition in 0..4 {
            let queue = queue.clone();
            producers.push(tokio::spawn(async move {
                for sequence in 0..64 {
                    queue.push((partition, sequence).into()).unwrap();
                    tokio::task::yield_now().await;
                }
            }));
        }
        for _ in 0..4 {
            let queue = queue.clone();
            let active = active.clone();
            consumers.push(tokio::spawn(async move {
                let mut received = Vec::new();
                for _ in 0..64 {
                    let (event, completion) = queue.pop().await.unwrap();
                    assert!(active.lock().insert(event.data.0));
                    tokio::task::yield_now().await;
                    assert!(active.lock().remove(&event.data.0));
                    received.push(event.data);
                    drop(completion);
                }
                received
            }));
        }
        for producer in producers {
            producer.await?;
        }
        let mut received = Vec::new();
        for consumer in consumers {
            received.extend(tokio::time::timeout(Duration::from_secs(2), consumer).await??);
        }
        received.sort_unstable();
        let expected: Vec<_> = (0..4)
            .flat_map(|partition| (0..64).map(move |sequence| (partition, sequence)))
            .collect();
        assert_eq!(expected, received);
        Ok(())
    }
}
