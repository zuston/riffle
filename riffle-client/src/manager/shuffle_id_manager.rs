use crate::ShuffleId;
use riffle_server::ddashmap::DDashMap;
use std::sync::atomic::AtomicUsize;

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

    pub fn get_or_create(&self, key: &ShuffleKey) -> ShuffleId {
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

    pub fn find(&self, key: &ShuffleKey) -> Option<ShuffleId> {
        match key {
            ShuffleKey::ShuffleId(id) => Some(*id),
            ShuffleKey::JobIdAndStageId(_) => self.key_to_id.get(key).map(|entry| *entry),
        }
    }
}
