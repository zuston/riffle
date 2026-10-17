use std::sync::atomic::AtomicUsize;
use riffle_server::ddashmap::DDashMap;
use crate::ShuffleId;

#[derive(Clone, Hash, PartialEq, Eq)]
pub enum ShuffleKey {
    JobIdAndStageId(String),
    ShuffleId(ShuffleId),
}

pub struct ShuffleIdManager {
    pub key_to_id: DDashMap<ShuffleKey, usize>,
    pub counter: AtomicUsize,
}

impl ShuffleIdManager {
    pub fn new() -> Self {
        Self {
            key_to_id: Default::default(),
            counter: Default::default(),
        }
    }

    pub fn get(&self, key: &ShuffleKey) -> ShuffleId {
        self.key_to_id
            .entry(key.clone())
            .or_insert_with(|| match key {
                ShuffleKey::JobIdAndStageId(_) => self
                    .counter
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                ShuffleKey::ShuffleId(x) => *x,
            })
            .value()
            .clone()
    }
}