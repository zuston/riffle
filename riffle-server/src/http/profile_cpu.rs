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

use crate::error::WorkerError;
use crate::http::{Format, Handler};
use log::error;

use anyhow::anyhow;
use hyper::StatusCode;
use once_cell::sync::Lazy;
use poem::error::{BadRequest, InternalServerError};
use poem::http::header::{CONTENT_DISPOSITION, CONTENT_TYPE};
use poem::{handler, Body, IntoResponse, Request, Response, RouteMethod};
use pprof::protos::Message;
use pprof::{ProfilerGuard, ProfilerGuardBuilder};
use serde::{Deserialize, Serialize};
use std::num::{NonZero, NonZeroI32};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep as delay_for;

#[derive(Deserialize, Serialize)]
#[serde(default)]
struct PProfRequest {
    seconds: u64,
    frequency: NonZeroI32,
    format: Format,
}

impl Default for PProfRequest {
    fn default() -> Self {
        PProfRequest {
            seconds: 5,
            frequency: NonZeroI32::new(100).unwrap(),
            format: Format::Svg,
        }
    }
}

#[handler]
async fn pprof_handler(req: &Request) -> poem::Result<Response> {
    let req = req.params::<PProfRequest>()?;
    let seconds = req.seconds;
    let frequency = req.frequency;
    let format = req.format;

    if seconds > 60 {
        return Err(anyhow!("Bad request. seconds must < 60").into());
    }
    if frequency > NonZero::try_from(1000).unwrap() {
        return Err(anyhow!("Bad request. frequency must < 1000").into());
    }

    static PROFILE_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

    // Copied from the neon. refer link:
    // https://github.com/neondatabase/neon/blob/main/libs/http-utils/src/endpoint.rs

    let report = {
        // Only allow one profiler at a time. If force is true, cancel a running profile (e.g. a
        // Grafana continuous profile). We use a try_lock() loop when cancelling instead of waiting
        // for a lock(), to avoid races where the notify isn't currently awaited.
        let _lock = loop {
            match PROFILE_LOCK.try_lock() {
                Ok(lock) => break lock,
                Err(_) => {
                    return Err(anyhow!("profiler already running").into());
                }
            }
        };

        let guard = ProfilerGuardBuilder::default()
            .frequency(frequency.into())
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .map_err(|err| InternalServerError(err))?;

        tokio::time::sleep(Duration::from_secs(seconds)).await;

        guard
            .report()
            .build()
            .map_err(|err| InternalServerError(err))?
    };

    let mut body = Vec::new();
    let response = match format {
        Format::Pprof => {
            let raw_pprof = report.pprof().map_err(|err| InternalServerError(err))?;
            raw_pprof.write_to_vec(&mut body);
            Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/octet-stream")
                .header(CONTENT_DISPOSITION, "attachment; filename=\"profile.pb\"")
                .body(Body::from(body))
        }
        Format::Svg => {
            report
                .flamegraph(&mut body)
                .map_err(|err| InternalServerError(err))?;
            Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "image/svg+xml")
                .body(Body::from(body))
        }
    };

    Ok(response)
}

#[derive(Default)]
pub struct ProfileCpuHandler {}
impl Handler for ProfileCpuHandler {
    fn get_route_method(&self) -> RouteMethod {
        RouteMethod::new().get(pprof_handler)
    }

    fn get_route_path(&self) -> String {
        "/debug/pprof/profile".to_string()
    }
}

#[cfg(test)]
mod tests {
    use crate::http::profile_cpu::ProfileCpuHandler;
    use crate::http::Handler;
    use poem::test::TestClient;
    use poem::Route;

    #[tokio::test]
    #[ignore]
    async fn test_router() {
        let handler = ProfileCpuHandler::default();
        let app = Route::new().at(handler.get_route_path(), handler.get_route_method());
        let cli = TestClient::new(app);
        let resp = cli.get("/debug/pprof/profile").send().await;
        resp.assert_status_is_ok();
    }
}
