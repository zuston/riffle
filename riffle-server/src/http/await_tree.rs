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

use crate::await_tree::AWAIT_TREE_REGISTRY;
use crate::http::Handler;
use await_tree::AnyKey;
use poem::endpoint::make;
use poem::{get, RouteMethod};

pub struct AwaitTreeHandler {}

impl Default for AwaitTreeHandler {
    fn default() -> Self {
        Self {}
    }
}

impl Handler for AwaitTreeHandler {
    fn get_route_method(&self) -> RouteMethod {
        get(make(|_| async {
            let registry = AWAIT_TREE_REGISTRY.clone();
            let mut sorted_list = registry.collect_all();
            sorted_list.sort_by_key(|kv| kv.0);

            let mut dynamic_string = String::new();
            for (idx, raw_tree) in sorted_list {
                dynamic_string.push_str(format!("actor={idx} ").as_str());
                dynamic_string.push_str(raw_tree.to_string().as_str());
                dynamic_string.push('\n');
            }
            dynamic_string
        }))
    }

    fn get_route_path(&self) -> String {
        "/await-tree".to_string()
    }
}

#[cfg(test)]
mod tests {
    use crate::await_tree::AWAIT_TREE_REGISTRY;
    use crate::http::await_tree::AwaitTreeHandler;
    use crate::http::Handler;
    use await_tree::{span, InstrumentAwait};
    use poem::test::TestClient;
    use poem::Route;
    use tracing::Instrument;

    #[tokio::test]
    async fn test_router() -> anyhow::Result<()> {
        let await_registry = AWAIT_TREE_REGISTRY.clone();
        let await_root = await_registry.register("test".to_string()).await;

        tokio::spawn(await_root.instrument(async move {
            tokio::time::sleep(tokio::time::Duration::from_secs(5))
                .instrument_await(span!("sleeping..."))
                .await;
        }));

        let handler = AwaitTreeHandler::default();
        let app = Route::new().at(handler.get_route_path(), handler.get_route_method());
        let cli = TestClient::new(app);
        let resp = cli.get("/await-tree").send().await;
        resp.assert_status_is_ok();
        println!("{}", resp.0.into_body().into_string().await.unwrap());

        Ok(())
    }
}
