// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use dashmap::DashMap;
use fxhash::FxHasher;
use std::hash::{BuildHasherDefault, Hash};
use std::ops::Deref;

pub struct DDashMap<K, V>(DashMap<K, V, BuildHasherDefault<FxHasher>>);

impl<K: Eq + Hash, V> DDashMap<K, V> {
    pub fn new(capacity: usize, shard_amount: usize) -> Self {
        let inner = DashMap::with_capacity_and_hasher_and_shard_amount(
            capacity,
            Default::default(),
            shard_amount,
        );
        DDashMap(inner)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let inner = DashMap::with_capacity_and_hasher(capacity, Default::default());
        DDashMap(inner)
    }

    pub fn into_inner(self) -> DashMap<K, V, BuildHasherDefault<FxHasher>> {
        self.0
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

impl<K: Eq + Hash, V> Deref for DDashMap<K, V> {
    type Target = DashMap<K, V, BuildHasherDefault<FxHasher>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K: Eq + Hash, V> Default for DDashMap<K, V> {
    fn default() -> Self {
        Self::with_capacity(128)
    }
}

#[cfg(test)]
mod tests {
    use crate::ddashmap::DDashMap;

    #[test]
    fn test_map() {
        let mut map = DDashMap::default();
        map.insert(1, 2);
        assert_eq!(2, *(map.get(&1).unwrap().value()));
        let v = map.compute_if_absent(2, || 3);
        assert_eq!(v, 3);
        let v = map.compute_if_absent(2, || 4);
        assert_eq!(v, 3);
    }
}
