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

use await_tree::{init_global_registry, span, AnyKey, Config, Registry, Span, Tree, TreeRoot};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub static AWAIT_TREE_REGISTRY: Lazy<AwaitTreeDelegator> = Lazy::new(|| AwaitTreeDelegator::new());

#[derive(Clone)]
pub struct AwaitTreeDelegator {
    registry: Registry,
    next_id: Arc<AtomicU64>,
}

impl AwaitTreeDelegator {
    fn new() -> Self {
        init_global_registry(Config::default());
        let registry = Registry::current();
        Self {
            registry,
            next_id: Arc::new(Default::default()),
        }
    }

    pub async fn register(&self, msg: impl Into<Span>) -> TreeRoot {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        self.registry.register(id, msg)
    }

    pub fn collect_all(&self) -> Vec<(u64, Tree)> {
        self.registry
            .collect_all()
            .iter()
            .map(|(key, tree)| {
                let key: u64 = *key.downcast_ref().unwrap();
                (key, tree.clone())
            })
            .collect()
    }
}
