use dashmap::DashMap;
use fxhash::{FxBuildHasher, FxHasher};
use std::hash::{BuildHasher, BuildHasherDefault, RandomState};
use std::ops::Deref;

pub struct DashMapExtend<K, V, S = RandomState>(DashMap<K, V, S>)
where
    K: Eq + std::hash::Hash,
    S: std::hash::BuildHasher + Clone + Default;

impl<K, V, S> Deref for DashMapExtend<K, V, S>
where
    K: Eq + std::hash::Hash,
    S: std::hash::BuildHasher + Clone + Default,
{
    type Target = DashMap<K, V, S>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K, V, S> DashMapExtend<K, V, S>
where
    K: Eq + std::hash::Hash,
    S: std::hash::BuildHasher + Clone + Default,
{
    pub fn new() -> DashMapExtend<K, V, BuildHasherDefault<FxHasher>> {
        let map = DashMap::with_hasher_and_shard_amount(FxBuildHasher::default(), 128);
        DashMapExtend(map)
    }

    pub fn compute_if_absent<F>(&self, key: K, f: F) -> V
    where
        F: FnOnce() -> V,
        V: Clone,
    {
        if let Some(value) = self.0.get(&key) {
            return value.clone();
        }

        match self.0.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(entry) => entry.get().clone(),
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let value = f();
                entry.insert(value.clone());
                value
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dashmap::DashMap;
    use std::sync::Arc;

    #[test]
    fn test_new() {
        let map = DashMapExtend::<i32, i32, BuildHasherDefault<FxHasher>>::new();
        map.insert(0, 1);
        map.compute_if_absent(1, || 2);
        assert_eq!(2, map.len());
    }

    #[test]
    fn test_deref_access() {
        let map = DashMapExtend(DashMap::new());
        map.insert("key4", 88);
        assert_eq!(*map.get("key4").unwrap(), 88);
    }

    #[test]
    fn test_insert_new_key() {
        let map = DashMapExtend(DashMap::new());
        let value = map.compute_if_absent("key1", || 42);
        assert_eq!(value, 42);
    }

    #[test]
    fn test_return_existing_key() {
        let map = DashMapExtend(DashMap::new());
        map.0.insert("key2", 100);
        let value = map.compute_if_absent("key2", || 999);
        assert_eq!(value, 100);
    }

    #[test]
    fn test_insert_only_once() {
        let map = DashMapExtend(DashMap::new());
        let first = map.compute_if_absent("key3", || 7);
        let second = map.compute_if_absent("key3", || panic!("Should not be called"));
        assert_eq!(first, 7);
        assert_eq!(second, 7);
    }
}
