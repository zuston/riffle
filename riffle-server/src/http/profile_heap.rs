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
use std::num::NonZeroI32;
use std::time::Duration;

use poem::error::ResponseError;
use poem::http::header::{CONTENT_DISPOSITION, CONTENT_TYPE};
use poem::http::StatusCode;
use poem::{Body, IntoResponse, Request, Response, RouteMethod};
use tempfile::Builder;
use tokio::time::sleep as delay_for;

use super::{Format, Handler};
use crate::mem_allocator::error::ProfError;
use crate::mem_allocator::error::ProfError::IoError;
use crate::mem_allocator::*;

#[derive(Deserialize, Serialize)]
#[serde(default)]
struct ProfileHeapRequest {
    format: Format,
}

impl Default for ProfileHeapRequest {
    fn default() -> Self {
        ProfileHeapRequest {
            format: Format::Svg,
        }
    }
}

// converts profiling error to http error
impl ResponseError for ProfError {
    fn status(&self) -> StatusCode {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

#[derive(Default)]
pub struct ProfileHeapHandler;
impl Handler for ProfileHeapHandler {
    fn get_route_method(&self) -> RouteMethod {
        RouteMethod::new().get(heap_profile)
    }

    fn get_route_path(&self) -> String {
        "/debug/heap/profile".to_string()
    }
}

#[poem::handler]
async fn heap_profile(req: &Request) -> poem::Result<impl IntoResponse> {
    let req = req.params::<ProfileHeapRequest>()?;
    let format = req.format;

    let response = match format {
        Format::Pprof => {
            let pprof = dump_prof("").await?;
            Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/octet-stream")
                .header(CONTENT_DISPOSITION, "attachment; filename=\"heap.pb.gz\"")
                .body(Body::from(pprof))
        }
        Format::Svg => {
            let svg = dump_heap_flamegraph().await?;
            Response::builder()
                .status(StatusCode::OK)
                .content_type("image/svg+xml")
                .body(svg)
        }
    };

    Ok(response)
}
