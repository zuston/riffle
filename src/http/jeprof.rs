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

use log::error;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::time::Duration;

use poem::error::ResponseError;
use poem::http::StatusCode;
use poem::{IntoResponse, Request, RouteMethod};
use tempfile::Builder;
use tokio::time::sleep as delay_for;

use super::Handler;
use crate::mem_allocator::error::ProfError;
use crate::mem_allocator::error::ProfError::IoError;
use crate::mem_allocator::*;

#[derive(Deserialize, Serialize)]
#[serde(default)]
pub struct JeProfRequest {
    pub(crate) duration: u64,
    pub(crate) keep_profiling: bool,
}

impl Default for JeProfRequest {
    fn default() -> Self {
        JeProfRequest {
            duration: 15,
            keep_profiling: false,
        }
    }
}

// converts profiling error to http error
impl ResponseError for ProfError {
    fn status(&self) -> StatusCode {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

#[poem::handler]
async fn jemalloc_pprof_handler(req: &Request) -> poem::Result<impl IntoResponse> {
    let pprof = dump_prof("").await?;
    Ok(pprof)
}

#[derive(Default)]
pub struct HeapProfHandler;

impl Handler for HeapProfHandler {
    fn get_route_method(&self) -> RouteMethod {
        RouteMethod::new().get(jemalloc_pprof_handler)
    }

    fn get_route_path(&self) -> String {
        "/debug/heap/profile".to_string()
    }
}

#[derive(Default)]
pub struct HeapProfFlameGraphHandler;
impl Handler for HeapProfFlameGraphHandler {
    fn get_route_method(&self) -> RouteMethod {
        RouteMethod::new().get(handle_get_heap_flamegraph)
    }

    fn get_route_path(&self) -> String {
        "/debug/heap/profile/flamegraph".to_string()
    }
}
#[poem::handler]
async fn handle_get_heap_flamegraph(req: &Request) -> poem::Result<impl IntoResponse> {
    let svg = dump_heap_flamegraph().await?;
    let response = poem::Response::builder()
        .status(StatusCode::OK)
        .content_type("image/svg+xml")
        .body(svg);
    Ok(response)
}

#[cfg(test)]
mod test {
    use crate::http::jeprof::HeapProfHandler;
    use crate::http::Handler;
    use poem::test::TestClient;
    use poem::Route;
    use tonic::codegen::http::StatusCode;

    #[tokio::test]
    #[ignore]
    async fn test_router() {
        let handler = HeapProfHandler::default();
        let app = Route::new().at(handler.get_route_path(), handler.get_route_method());
        let cli = TestClient::new(app);
        let resp = cli
            .get("/debug/heap/profile")
            .query("seconds", &10)
            .send()
            .await;

        #[cfg(feature = "mem-profiling")]
        resp.assert_status_is_ok();
        #[cfg(not(feature = "mem-profiling"))]
        resp.assert_status(StatusCode::INTERNAL_SERVER_ERROR);
    }
}
