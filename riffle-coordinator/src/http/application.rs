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

use crate::application::{ApplicationManager, ApplicationSnapshot};
use poem::web::{Data, Json, Path};
use poem::{handler, EndpointExt, RouteMethod};
use riffle_server::http::Handler;

pub struct ApplicationHandler {
    application_manager: ApplicationManager,
}

impl ApplicationHandler {
    pub fn new(application_manager: ApplicationManager) -> Self {
        Self {
            application_manager,
        }
    }
}

impl Handler for ApplicationHandler {
    fn get_route_method(&self) -> RouteMethod {
        RouteMethod::new().get(applications.data(self.application_manager.clone()))
    }

    fn get_route_path(&self) -> String {
        "/apps".to_string()
    }
}

#[handler]
async fn applications(Data(manager): Data<&ApplicationManager>) -> Json<Vec<ApplicationSnapshot>> {
    Json(manager.snapshots())
}

#[handler]
pub(super) async fn application(
    Path(app_id): Path<String>,
    Data(manager): Data<&ApplicationManager>,
) -> poem::Result<Json<ApplicationSnapshot>> {
    manager
        .snapshot(&app_id)
        .map(Json)
        .ok_or_else(|| poem::Error::from_status(poem::http::StatusCode::NOT_FOUND))
}
