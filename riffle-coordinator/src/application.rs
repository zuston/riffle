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

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use log::info;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApplicationInfo {
    pub app_id: String,
    pub user: String,
    pub client_version: Option<String>,
    pub client_git_commit_id: Option<String>,
    pub registration_time: DateTime<Utc>,
}

impl ApplicationInfo {
    pub fn new(
        app_id: String,
        user: String,
        version: Option<String>,
        git_commit_id: Option<String>,
    ) -> Self {
        Self {
            app_id,
            user,
            client_version: version,
            client_git_commit_id: git_commit_id,
            registration_time: Utc::now(),
        }
    }
}

#[derive(Clone, Default)]
pub struct ApplicationManager {
    apps: Arc<DashMap<String, ApplicationInfo>>,
}

impl ApplicationManager {
    pub fn register(
        &self,
        app_id: String,
        user: String,
        version: Option<String>,
        git_commit_id: Option<String>,
    ) {
        self.apps
            .entry(app_id.clone())
            .and_modify(|application| {
                application.user = user.clone();
                application.client_version = version.clone();
                application.client_git_commit_id = git_commit_id.clone();
            })
            .or_insert_with(|| {
                info!("Application registered: {app_id} (user: {user})");
                ApplicationInfo::new(app_id, user, version, git_commit_id)
            });
    }

    pub fn heartbeat(&self, app_id: &str) {
        if !self.apps.contains_key(app_id) {
            self.register(app_id.to_string(), String::new(), None, None);
        }
    }
}
